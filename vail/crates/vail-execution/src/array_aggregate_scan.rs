//! Exclusive list scans reuse the same accumulator implementations as tables.
//! Each selected input row is consumed once; growing collection outputs retain
//! their inherent output-size cost, without replaying prior input rows.
use std::sync::Arc;
use arrow_array::{Array, BooleanArray, ListArray, new_empty_array};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, Signature};
use crate::expressions::ExprError;

pub fn make_udf(function: &str, input: &DataType, options: &serde_json::Value) -> Result<Arc<ScalarUDF>, ExprError> {
    let DataType::List(element) = input else {
        return Err(ExprError::Invalid("aggregate scan requires list input".into()));
    };
    let aggregate = crate::reducer::configured_udaf(function, Some(element.data_type()), options)?;
    let descriptor = crate::scan_state::ScanState::new(aggregate, vec![element.data_type().clone()])?;
    let output_element = descriptor.output_type();
    let output = DataType::List(Arc::new(Field::new_list_field(output_element.clone(), true)));
    let predicate = DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true)));
    let identity = (function.to_owned(), input.clone(), options.clone());
    Ok(Arc::new(crate::kernel_identity::scalar("vail_list_scan", identity,
        Signature::exact(vec![input.clone(), predicate], Volatility::Volatile), output.clone(),
        Arc::new(move |args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let values = arrays[0].as_any().downcast_ref::<ListArray>().ok_or_else(|| DataFusionError::Execution("aggregate scan input is not a list".into()))?;
            let predicates = arrays[1].as_any().downcast_ref::<ListArray>().ok_or_else(|| DataFusionError::Execution("aggregate scan predicate is not a list".into()))?;
            let result = scan_lists(values, predicates, &output_element, || descriptor.accumulator())?;
            Ok(ColumnarValue::Array(result))
        }))))
}

fn scan_lists(
    values: &ListArray,
    predicates: &ListArray,
    output_element: &DataType,
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
                let flags = flags.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| DataFusionError::Execution("aggregate scan predicate elements must be boolean".into()))?;
                if predicates.is_null(row) || input.len() != flags.len() {
                    return Err(DataFusionError::Execution("aggregate scan input and predicate lengths differ".into()));
                }
                let mut accumulator = accumulator()?;
                lengths.push(input.len());
                for index in 0..input.len() {
                    outputs.push(accumulator.evaluate()?.to_array_of_size(1)?);
                    if flags.is_valid(index) && flags.value(index) {
                        accumulator.update_batch(&[input.slice(index, 1)])?;
                    }
                }
            }
            let flat = if outputs.is_empty() { new_empty_array(output_element) }
                else { concat(&outputs.iter().map(|a| a.as_ref()).collect::<Vec<_>>())? };
    let field = Arc::new(Field::new_list_field(output_element.clone(), true));
    Ok(Arc::new(ListArray::try_new(field, OffsetBuffer::from_lengths(lengths), flat, Some(NullBuffer::from(valid)))?))
}
