//! Scalar entry point for the same exact test used by the call aggregator.
use arrow_array::{
    Array, BooleanArray, Int32Array,
    builder::{Float64Builder, ListBuilder},
};
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, expr_fn::create_udf};
use std::sync::Arc;

pub(crate) fn udf() -> Arc<ScalarUDF> {
    Arc::new(create_udf(
        "vail_hardy_weinberg_counts",
        vec![
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Boolean,
        ],
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        Volatility::Immutable,
        Arc::new(|values| {
            let arrays = ColumnarValue::values_to_arrays(values)?;
            let counts = arrays[..3]
                .iter()
                .map(|a| a.as_any().downcast_ref::<Int32Array>().unwrap())
                .collect::<Vec<_>>();
            let modes = arrays[3].as_any().downcast_ref::<BooleanArray>().unwrap();
            let mut result = ListBuilder::new(Float64Builder::new());
            for i in 0..arrays[0].len() {
                if arrays.iter().any(|a| a.is_null(i)) {
                    result.append(false);
                    continue;
                }
                if counts.iter().any(|a| a.value(i) < 0) {
                    return Err(DataFusionError::Execution(
                        "hardy_weinberg_test requires nonnegative counts".into(),
                    ));
                }
                let (freq, p) = crate::advanced_genomic_aggregates::hwe(
                    counts[0].value(i) as i64,
                    counts[1].value(i) as i64,
                    counts[2].value(i) as i64,
                    modes.value(i),
                );
                result.values().append_value(freq);
                result.values().append_value(p);
                result.append(true);
            }
            let result: arrow_array::ArrayRef = Arc::new(result.finish());
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
