//! Run shared aggregate kernels over each row's Arrow list, without unnesting.
//!
//! Primitive reductions use Arrow segmented kernels; correlation, call stats,
//! and regression use grouped accumulators. Other reducers borrow each list's
//! child buffers and use the shared aggregate adapter, including empty identities.
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, StructArray, new_empty_array,
    new_null_array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, GroupsAccumulator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_physical_expr::{
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
    expressions::Column,
};

mod direct;

const PREFIX: &str = "__vail_list_reduce_";
const GROUP_INDEX_CHUNK: usize = 64 * 1024;

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListAggregate {
    name: String,
    function: String,
    direct_call_stats: bool,
    signature: Signature,
}

/// Adapter descriptor shared by capability inquiry, construction and validation.
/// Looking up arity allocates no UDF or configured accumulator.
pub(crate) fn arity(function: &str) -> Option<usize> {
    if !matches!(
        function,
        "collect"
            | "collect_as_set"
            | "sum"
            | "product"
            | "min"
            | "max"
            | "mean"
            | "corr"
            | "stddev_pop"
            | "count_where"
            | "fraction"
            | "linreg"
            | "any"
            | "all"
            | "call_stats"
            | "take"
            | "take_ordered"
    ) {
        return None;
    }
    Some(
        if matches!(function, "call_stats" | "take" | "take_ordered" | "linreg") {
            2
        } else {
            1
        },
    )
}

pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    let direct_call_stats = name == "__vail_list_call_stats";
    let function = if direct_call_stats {
        "call_stats"
    } else {
        let function = name.strip_prefix(PREFIX)?;
        if function == "call_stats" {
            return None;
        }
        function
    };
    let arity = if direct_call_stats {
        2
    } else {
        arity(function)?
    };
    Some(Arc::new(ScalarUDF::from(ListAggregate {
        name: name.to_owned(),
        function: function.to_owned(),
        direct_call_stats,
        signature: Signature::any(arity, Volatility::Immutable),
    })))
}

fn element_type(args: &[DataType], function: &str) -> Result<DataType> {
    let expected = arity(function)
        .ok_or_else(|| plan_datafusion_err!("unsupported list reducer {function}"))?;
    if args.len() != expected {
        return Err(plan_datafusion_err!(
            "list {function} requires {expected} arguments"
        ));
    }
    let DataType::List(field) = &args[0] else {
        return Err(plan_datafusion_err!("list {function} requires a list"));
    };
    if function == "count_where" && field.data_type() != &DataType::Boolean {
        return Err(plan_datafusion_err!("count_where requires Boolean input"));
    }
    if function == "call_stats" && args[1] != DataType::Int32 {
        return Err(plan_datafusion_err!(
            "call_stats initializer requires int32"
        ));
    }
    if matches!(function, "take" | "take_ordered") && args[1] != DataType::Int64 {
        return Err(plan_datafusion_err!("take initializer requires int64"));
    }
    if function == "linreg" && args[1] != DataType::Int64 {
        return Err(plan_datafusion_err!("linreg nested_dim requires int64"));
    }
    Ok(field.data_type().clone())
}

fn configured_linreg(
    input: &DataType,
    nested_dim: usize,
) -> Result<Arc<datafusion_expr::AggregateUDF>> {
    let DataType::Struct(fields) = input else {
        return Err(plan_datafusion_err!("linreg list input requires a struct"));
    };
    let x = fields
        .iter()
        .find(|field| field.name() == "x")
        .ok_or_else(|| plan_datafusion_err!("linreg list input requires x predictors"))?;
    let DataType::Struct(predictors) = x.data_type() else {
        return Err(plan_datafusion_err!(
            "linreg list input requires struct predictors"
        ));
    };
    let n_predictors = predictors.len();
    let options = serde_json::json!({
        "nested_dim": nested_dim,
        "n_predictors": n_predictors,
    });
    crate::reducer::configured_udaf("linreg", Some(input), &options)
        .map_err(|error| plan_datafusion_err!("{error}"))
}

fn configured(
    function: &str,
    input: &DataType,
    n_alleles: usize,
) -> Result<Arc<datafusion_expr::AggregateUDF>> {
    // This object stays in Rust memory; it is the existing reducer's ordinary
    // configuration, not serialized expression or FFI transport.
    let options = if function == "call_stats" {
        serde_json::json!({"n_alleles": n_alleles})
    } else if matches!(function, "take" | "take_ordered") {
        serde_json::json!({"n": n_alleles, "ordered": function == "take_ordered"})
    } else {
        serde_json::Value::Null
    };
    let function = if function == "take_ordered" {
        "take"
    } else if function == "count_where" {
        "count"
    } else {
        function
    };
    crate::reducer::configured_udaf(function, Some(input), &options)
        .map_err(|error| plan_datafusion_err!("{error}"))
}

fn packed_call_stats_input(call_type: &DataType) -> DataType {
    DataType::Struct(
        vec![
            Arc::new(Field::new("__vail_genomic_call", call_type.clone(), true)),
            Arc::new(Field::new("__vail_genomic_alleles", DataType::Int32, true)),
        ]
        .into(),
    )
}

fn kernel(
    function: &str,
    input: &DataType,
    n_alleles: usize,
) -> Result<Arc<AggregateFunctionExpr>> {
    Ok(Arc::new(
        AggregateExprBuilder::new(
            configured(function, input, n_alleles)?,
            vec![Arc::new(Column::new("element", 0))],
        )
        .schema(Arc::new(Schema::new(vec![Field::new(
            "element",
            input.clone(),
            true,
        )])))
        .alias("list_reducer")
        .build()?,
    ))
}

fn normalize_collect_empty(value: ScalarValue, input: &DataType) -> ScalarValue {
    if value.is_null() {
        ScalarValue::List(ScalarValue::new_list(&[], input, true))
    } else {
        value
    }
}

fn update_grouped_lists(
    groups: &mut dyn GroupsAccumulator,
    lists: &ListArray,
    row_indices: impl Iterator<Item = usize>,
    total_groups: usize,
    input_type: &DataType,
) -> Result<()> {
    let empty = new_empty_array(input_type);
    groups.update_batch(&[empty], &[], None, total_groups)?;
    let mut indices = vec![0; GROUP_INDEX_CHUNK];
    for (group, row) in row_indices.enumerate() {
        if lists.is_null(row) {
            continue;
        }
        let values = lists.value(row);
        let mut offset = 0;
        while offset < values.len() {
            let len = (values.len() - offset).min(indices.len());
            indices[..len].fill(group);
            groups.update_batch(
                &[values.slice(offset, len)],
                &indices[..len],
                None,
                total_groups,
            )?;
            offset += len;
        }
    }
    Ok(())
}

fn grouped_corr_lists(lists: &ListArray, input_type: &DataType) -> Result<ArrayRef> {
    let rows = lists.len();
    if rows == 0 {
        return Ok(new_empty_array(&DataType::Float64));
    }
    let aggregate = kernel("corr", input_type, 0)?;
    if !aggregate.groups_accumulator_supported() {
        return Err(exec_datafusion_err!(
            "corr list reducer requires grouped accumulator support"
        ));
    }
    let mut groups = aggregate.create_groups_accumulator()?;
    update_grouped_lists(groups.as_mut(), lists, 0..rows, rows, input_type)?;
    Ok(groups.evaluate(datafusion_expr::EmitTo::All)?)
}

fn initializer_n(initializers: &dyn Array, row: usize) -> Result<usize> {
    let value = match initializers.data_type() {
        DataType::Int32 => initializers
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row) as i64,
        DataType::Int64 => initializers
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row),
        _ => return Err(exec_datafusion_err!("invalid reducer initializer type")),
    };
    if initializers.is_null(row) || value < 0 {
        return Err(exec_datafusion_err!(
            "call_stats allele count requires a defined nonnegative initializer"
        ));
    }
    usize::try_from(value)
        .map_err(|_| exec_datafusion_err!("reducer initializer exceeds addressable memory"))
}

fn grouped_call_stats_lists(
    lists: &ListArray,
    initializers: &dyn Array,
    input_type: &DataType,
    output_type: &DataType,
) -> Result<ArrayRef> {
    let mut buckets = std::collections::BTreeMap::<usize, Vec<usize>>::new();
    let mut selected = (0..lists.len()).map(|row| (0, row)).collect::<Vec<_>>();
    let mut bucket_results = vec![new_null_array(output_type, lists.len())];
    for row in 0..lists.len() {
        // Validate initializer values even for null lists, matching the scalar fallback.
        let n = initializer_n(initializers, row)?;
        if !lists.is_null(row) {
            buckets.entry(n).or_default().push(row);
        }
    }
    for (n, rows) in buckets {
        let aggregate = kernel("call_stats", input_type, n)?;
        if !aggregate.groups_accumulator_supported() {
            return Err(exec_datafusion_err!(
                "call_stats list reducer requires grouped accumulator support"
            ));
        }
        let mut groups = aggregate.create_groups_accumulator()?;
        update_grouped_lists(
            groups.as_mut(),
            lists,
            rows.iter().copied(),
            rows.len(),
            input_type,
        )?;
        let batch = groups.evaluate(datafusion_expr::EmitTo::All)?;
        let array_index = bucket_results.len();
        bucket_results.push(batch);
        for (group, row) in rows.into_iter().enumerate() {
            selected[row] = (array_index, group);
        }
    }
    if selected.is_empty() {
        Ok(new_empty_array(output_type))
    } else {
        let arrays = bucket_results
            .iter()
            .map(|array| array.as_ref() as &dyn Array)
            .collect::<Vec<_>>();
        Ok(arrow_select::interleave::interleave(&arrays, &selected)?)
    }
}

impl ScalarUDFImpl for ListAggregate {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let logical_args = if self.direct_call_stats {
            if args.len() != 2 || args[0] != DataType::Int32 {
                return Err(plan_datafusion_err!(
                    "direct call_stats requires int32 n_alleles followed by list<Call>"
                ));
            }
            let DataType::List(field) = &args[1] else {
                return Err(plan_datafusion_err!(
                    "direct call_stats requires int32 n_alleles followed by list<Call>"
                ));
            };
            if !vail_ordering::same_layout(field.data_type(), &crate::calls::storage_type()) {
                return Err(plan_datafusion_err!(
                    "direct call_stats requires int32 n_alleles followed by list<Call>"
                ));
            }
            vec![args[1].clone(), args[0].clone()]
        } else {
            args.to_vec()
        };
        let input = element_type(&logical_args, &self.function)?;
        let input = if self.function == "call_stats"
            && vail_ordering::same_layout(&input, &crate::calls::storage_type())
        {
            packed_call_stats_input(&input)
        } else {
            input
        };
        if self.function == "linreg" {
            configured_linreg(&input, 0)?.return_type(&[input])
        } else {
            configured(&self.function, &input, 0)?.return_type(&[input])
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let mut arrays = args
            .args
            .iter()
            .cloned()
            .map(|a| a.into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        if self.direct_call_stats {
            if arrays.len() != 2 || arrays[0].data_type() != &DataType::Int32 {
                return Err(exec_datafusion_err!(
                    "direct call_stats requires int32 n_alleles followed by list<Call>"
                ));
            }
            arrays.swap(0, 1);
        }
        let input_type = element_type(
            &arrays
                .iter()
                .map(|a| a.data_type().clone())
                .collect::<Vec<_>>(),
            &self.function,
        )?;
        let lists = arrays[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| exec_datafusion_err!("list reducer requires Arrow lists"))?;
        if self.function == "linreg" {
            return invoke_linreg_lists(lists, &arrays[1], rows, scalar);
        }
        if let Some(result) = direct::reduce(lists, &self.function)? {
            return if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            };
        }
        let initializers = arrays.get(1);
        let output_input_type = if self.function == "call_stats"
            && vail_ordering::same_layout(&input_type, &crate::calls::storage_type())
        {
            packed_call_stats_input(&input_type)
        } else {
            input_type.clone()
        };
        let output_type = configured(&self.function, &output_input_type, 0)?
            .return_type(&[output_input_type.clone()])?;
        if self.function == "corr" {
            let result = grouped_corr_lists(lists, &input_type)?;
            return if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            };
        }
        if self.function == "stddev_pop" {
            let valid_count = (0..rows).filter(|&row| !lists.is_null(row)).count();
            if valid_count == 0 {
                let result = new_null_array(&output_type, rows);
                return if scalar {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &result, 0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(result))
                };
            }
            let aggregate = kernel("stddev_pop", &input_type, 0)?;
            if !aggregate.groups_accumulator_supported() {
                return Err(exec_datafusion_err!(
                    "stddev_pop list reducer requires grouped accumulator support"
                ));
            }
            let mut groups = aggregate.create_groups_accumulator()?;
            update_grouped_lists(
                groups.as_mut(),
                lists,
                (0..rows).filter(|&row| !lists.is_null(row)),
                valid_count,
                &input_type,
            )?;
            let batch = groups.evaluate(datafusion_expr::EmitTo::All)?;
            if valid_count == rows {
                return if scalar {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &batch, 0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(batch))
                };
            }
            let nulls = new_null_array(&output_type, 1);
            let mut group = 0;
            let mapping = (0..rows)
                .map(|row| {
                    if lists.is_null(row) {
                        (0, 0)
                    } else {
                        let current = group;
                        group += 1;
                        (1, current)
                    }
                })
                .collect::<Vec<_>>();
            let arrays = [nulls.as_ref(), batch.as_ref()];
            let result = arrow_select::interleave::interleave(&arrays, &mapping)?;
            return if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            };
        }
        if self.function == "call_stats" {
            let initializer = initializers.ok_or_else(|| {
                exec_datafusion_err!("call_stats requires an allele-count initializer")
            })?;
            // Keep each row's allele count in its grouped accumulator. The
            // calls child buffer is already aligned with list offsets.
            let calls_input_type = packed_call_stats_input(&input_type);
            let result = grouped_call_stats_lists(
                lists,
                initializer.as_ref(),
                &calls_input_type,
                &output_type,
            )?;
            return if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            };
        }
        // Reuse consecutive configurations with bounded adapter memory. A new
        // accumulator still owns each row's counts and empty identity.
        let mut previous_kernel: Option<(usize, Arc<AggregateFunctionExpr>)> = None;
        let mut output = Vec::with_capacity(rows);
        for row in 0..rows {
            let n = if let Some(init) = initializers {
                let value = match init.data_type() {
                    DataType::Int32 => init
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(row) as i64,
                    DataType::Int64 => init
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                    _ => return Err(exec_datafusion_err!("invalid reducer initializer type")),
                };
                if init.is_null(row) || value < 0 {
                    let parameter = if self.function == "call_stats" {
                        "call_stats allele count"
                    } else {
                        &self.function
                    };
                    return Err(exec_datafusion_err!(
                        "{parameter} requires a defined nonnegative initializer"
                    ));
                }
                usize::try_from(value).map_err(|_| {
                    exec_datafusion_err!("reducer initializer exceeds addressable memory")
                })?
            } else {
                0
            };
            if lists.is_null(row) {
                output.push(ScalarValue::try_from(&output_type)?);
                continue;
            }
            let aggregate = match &previous_kernel {
                Some((previous_n, aggregate)) if *previous_n == n => Arc::clone(aggregate),
                _ => {
                    let aggregate = kernel(&self.function, &input_type, n)?;
                    previous_kernel = Some((n, Arc::clone(&aggregate)));
                    aggregate
                }
            };
            let mut accumulator = aggregate.create_accumulator()?;
            let values = lists.value(row);
            if self.function == "count_where" {
                let predicates = values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| exec_datafusion_err!("count_where requires Boolean input"))?;
                // Reuse the predicate payload bitmap. Native COUNT only needs
                // true values to be valid, so false bits become nulls in a
                // lightweight validity view; only nullable inputs need a new
                // bitmap combining predicate and original validity.
                let valid = match predicates.nulls() {
                    Some(nulls) => predicates.values() & nulls.inner(),
                    None => predicates.values().clone(),
                };
                let true_only =
                    BooleanArray::new(predicates.values().clone(), Some(NullBuffer::new(valid)));
                accumulator.update_batch(&[Arc::new(true_only)])?;
            } else {
                accumulator.update_batch(&[values])?;
            }
            let value = accumulator.evaluate()?;
            output.push(if self.function == "collect" {
                normalize_collect_empty(value, &input_type)
            } else {
                value
            });
        }
        let result: ArrayRef = if output.is_empty() {
            new_empty_array(&output_type)
        } else {
            ScalarValue::iter_to_array(output)?
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn invoke_linreg_lists(
    lists: &ListArray,
    nested_values: &ArrayRef,
    rows: usize,
    scalar: bool,
) -> Result<ColumnarValue> {
    let nested_values = nested_values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| exec_datafusion_err!("linreg nested_dim must be int64"))?;
    if nested_values.len() != rows {
        return Err(exec_datafusion_err!("linreg nested_dim length mismatch"));
    }
    let nested_dim = if rows == 0 {
        0
    } else {
        if nested_values.is_null(0) || nested_values.value(0) < 0 {
            return Err(exec_datafusion_err!(
                "linreg nested_dim requires a defined nonnegative integer"
            ));
        }
        let nested = nested_values.value(0);
        if (0..rows).any(|row| nested_values.is_null(row) || nested_values.value(row) != nested) {
            return Err(exec_datafusion_err!(
                "linreg nested_dim must be constant across a list batch"
            ));
        }
        usize::try_from(nested)
            .map_err(|_| exec_datafusion_err!("linreg nested_dim exceeds addressable memory"))?
    };
    let input_type = lists.values().data_type().clone();
    let udf = configured_linreg(&input_type, nested_dim)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "element",
        input_type.clone(),
        true,
    )]));
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("element", 0))])
        .schema(schema)
        .alias("list_linreg")
        .build()?;
    if !aggregate.groups_accumulator_supported() {
        return Err(exec_datafusion_err!(
            "linreg list reduction requires a native grouped accumulator"
        ));
    }
    let mut grouped = aggregate.create_groups_accumulator()?;
    update_grouped_lists(grouped.as_mut(), lists, 0..rows, rows, &input_type)?;
    let output = grouped.evaluate(datafusion_expr::EmitTo::All)?;
    let output = output
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| exec_datafusion_err!("linreg must return struct results"))?;
    let result: ArrayRef = Arc::new(StructArray::new(
        output.fields().clone(),
        output.columns().to_vec(),
        lists.nulls().cloned(),
    ));
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}
