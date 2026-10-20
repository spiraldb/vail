//! Exclusive list scans reuse the same accumulator implementations as tables.
//! Each selected input row is consumed once; growing collection outputs retain
//! their inherent output-size cost, without replaying prior input rows.
use crate::expressions::ExprError;
use arrow_array::{Array, BooleanArray, ListArray, new_empty_array};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility};
use std::sync::Arc;

pub fn make_udf(
    function: &str,
    input: &DataType,
    options: &serde_json::Value,
) -> Result<Arc<ScalarUDF>, ExprError> {
    let DataType::List(element) = input else {
        return Err(ExprError::Invalid(
            "aggregate scan requires list input".into(),
        ));
    };
    let count_where = function == "count_where";
    if count_where {
        if element.data_type() != &DataType::Boolean {
            return Err(ExprError::Invalid(
                "count_where requires Boolean input".into(),
            ));
        }
        let mut contract_options = options
            .as_object()
            .cloned()
            .ok_or_else(|| ExprError::Invalid("aggregate options must be an object".into()))?;
        contract_options.remove("__vail_result_type");
        let input_type = crate::expressions::Type::from_arrow(element.data_type())?;
        crate::aggregate_contract::result_type(
            function,
            Some(&input_type),
            &serde_json::Value::Object(contract_options),
            &[],
        )?;
    }
    let aggregate = if count_where {
        datafusion_functions_aggregate::count::count_udaf()
    } else {
        crate::reducer::configured_udaf(function, Some(element.data_type()), options)?
    };
    let descriptor =
        crate::scan_state::ScanState::new(aggregate, vec![element.data_type().clone()])?;
    let output_element = descriptor.output_type();
    let output = DataType::List(Arc::new(Field::new_list_field(
        output_element.clone(),
        true,
    )));
    let predicate = DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true)));
    let identity = (function.to_owned(), input.clone(), options.clone());
    Ok(Arc::new(crate::kernel_identity::scalar(
        "vail_list_scan",
        identity,
        Signature::exact(vec![input.clone(), predicate], Volatility::Volatile),
        output.clone(),
        Arc::new(move |args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let values = arrays[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("aggregate scan input is not a list".into())
                })?;
            let predicates = arrays[1]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("aggregate scan predicate is not a list".into())
                })?;
            let result = scan_lists(values, predicates, &output_element, count_where, || {
                descriptor.accumulator()
            })?;
            Ok(ColumnarValue::Array(result))
        }),
    )))
}

fn scan_lists(
    values: &ListArray,
    predicates: &ListArray,
    output_element: &DataType,
    count_where: bool,
    accumulator: impl Fn() -> Result<Box<dyn datafusion_expr::Accumulator>>,
) -> Result<Arc<ListArray>> {
    let mut outputs = Vec::new();
    let mut lengths = Vec::with_capacity(values.len());
    let mut valid = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        valid.push(values.is_valid(row));
        if values.is_null(row) {
            lengths.push(0);
            continue;
        }
        let input = values.value(row);
        let flags = predicates.value(row);
        let flags = flags
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "aggregate scan predicate elements must be boolean".into(),
                )
            })?;
        if predicates.is_null(row) || input.len() != flags.len() {
            return Err(DataFusionError::Execution(
                "aggregate scan input and predicate lengths differ".into(),
            ));
        }
        let mut accumulator = accumulator()?;
        lengths.push(input.len());
        for index in 0..input.len() {
            outputs.push(accumulator.evaluate()?.to_array_of_size(1)?);
            let count_selected = !count_where
                || (input.data_type() == &DataType::Boolean
                    && input
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .is_some_and(|values| values.is_valid(index) && values.value(index)));
            if flags.is_valid(index) && flags.value(index) && count_selected {
                accumulator.update_batch(&[input.slice(index, 1)])?;
            }
        }
    }
    let flat = if outputs.is_empty() {
        new_empty_array(output_element)
    } else {
        concat(&outputs.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
    };
    let field = Arc::new(Field::new_list_field(output_element.clone(), true));
    Ok(Arc::new(ListArray::try_new(
        field,
        OffsetBuffer::from_lengths(lengths),
        flat,
        Some(NullBuffer::from(valid)),
    )?))
}
