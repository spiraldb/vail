//! Scalar entry points for the exact test used by the call aggregator.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, StructArray,
    builder::{Float64Builder, ListBuilder},
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, expr_fn::create_udf};
use std::sync::Arc;

pub(crate) fn result_type(both: bool) -> DataType {
    if both {
        DataType::Struct(
            ["het_freq_hwe", "p_value", "p_value_excess_het"]
                .map(|name| Arc::new(Field::new(name, DataType::Float64, true)))
                .to_vec()
                .into(),
        )
    } else {
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
    }
}

pub(crate) fn udf(both: bool) -> Arc<ScalarUDF> {
    Arc::new(create_udf(
        if both {
            "vail_hwe_both_counts"
        } else {
            "vail_hardy_weinberg_counts"
        },
        if both {
            vec![DataType::Int64; 3]
        } else {
            vec![
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Boolean,
            ]
        },
        result_type(both),
        Volatility::Immutable,
        Arc::new(move |values| {
            let arrays = ColumnarValue::values_to_arrays(values)?;
            let counts = arrays[..3]
                .iter()
                .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap())
                .collect::<Vec<_>>();
            let modes = if both {
                None
            } else {
                Some(arrays[3].as_any().downcast_ref::<BooleanArray>().unwrap())
            };
            let mut list = ListBuilder::new(Float64Builder::new());
            let (mut frequency, mut two, mut one) = (
                Float64Builder::new(),
                Float64Builder::new(),
                Float64Builder::new(),
            );
            let mut valid = Vec::with_capacity(arrays[0].len());
            for row in 0..arrays[0].len() {
                if arrays.iter().any(|a| a.is_null(row)) {
                    if both {
                        frequency.append_null();
                        two.append_null();
                        one.append_null();
                        valid.push(false);
                    } else {
                        list.append(false);
                    }
                    continue;
                }
                if counts.iter().any(|a| a.value(row) < 0) {
                    return Err(DataFusionError::Execution(
                        "hardy_weinberg_test requires nonnegative counts".into(),
                    ));
                }
                let (freq, p, excess) = crate::advanced_genomic_aggregates::hwe_both(
                    counts[0].value(row),
                    counts[1].value(row),
                    counts[2].value(row),
                );
                if both {
                    frequency.append_value(freq);
                    two.append_value(p);
                    one.append_value(excess);
                    valid.push(true);
                } else {
                    list.values().append_value(freq);
                    list.values()
                        .append_value(if modes.is_some_and(|m| m.value(row)) {
                            excess
                        } else {
                            p
                        });
                    list.append(true);
                }
            }
            let result: ArrayRef = if both {
                let DataType::Struct(fields) = result_type(true) else {
                    unreachable!()
                };
                Arc::new(StructArray::try_new(
                    fields,
                    vec![
                        Arc::new(frequency.finish()),
                        Arc::new(two.finish()),
                        Arc::new(one.finish()),
                    ],
                    Some(NullBuffer::from(valid)),
                )?)
            } else {
                Arc::new(list.finish())
            };
            if values.iter().all(|v| matches!(v, ColumnarValue::Scalar(_))) {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            }
        }),
    ))
}
