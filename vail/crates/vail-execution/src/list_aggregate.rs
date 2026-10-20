//! Run shared aggregate kernels over each row's Arrow list, without unnesting.
//!
//! A list slice borrows the existing child buffers. Each row gets one fresh
//! accumulator and one update_batch call, including for an empty list. Only the
//! initializer's value varies across rows; no aggregate semantics live here.
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, new_empty_array,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_expr::{
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
    expressions::Column,
};

const PREFIX: &str = "__vail_list_reduce_";

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListAggregate {
    name: String,
    function: String,
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
            | "any"
            | "all"
            | "call_stats"
            | "take"
            | "take_ordered"
    ) {
        return None;
    }
    Some(
        if matches!(function, "call_stats" | "take" | "take_ordered") {
            2
        } else {
            1
        },
    )
}

pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    let function = name.strip_prefix(PREFIX)?;
    let arity = arity(function)?;
    Some(Arc::new(ScalarUDF::from(ListAggregate {
        name: name.to_owned(),
        function: function.to_owned(),
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
    if function == "call_stats" && args[1] != DataType::Int32 {
        return Err(plan_datafusion_err!(
            "call_stats initializer requires int32"
        ));
    }
    if matches!(function, "take" | "take_ordered") && args[1] != DataType::Int64 {
        return Err(plan_datafusion_err!("take initializer requires int64"));
    }
    Ok(field.data_type().clone())
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

impl ScalarUDFImpl for ListAggregate {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let input = element_type(args, &self.function)?;
        configured(&self.function, &input, 0)?.return_type(&[input])
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .cloned()
            .map(|a| a.into_array(rows))
            .collect::<Result<Vec<_>>>()?;
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
        let initializers = arrays.get(1);
        let output_type =
            configured(&self.function, &input_type, 0)?.return_type(&[input_type.clone()])?;
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
                let true_only = BooleanArray::from_iter(
                    predicates.iter().map(|value| value.filter(|value| *value)),
                );
                accumulator.update_batch(&[Arc::new(true_only)])?;
            } else {
                accumulator.update_batch(&[values])?;
            }
            output.push(accumulator.evaluate()?);
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
