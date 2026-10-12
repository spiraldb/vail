//! Hail-compatible collect aggregate. Values stay in Arrow arrays throughout
//! accumulation so nested Arrow field metadata is retained and missing values
//! remain ordinary list elements.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, new_empty_array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, Volatility, expr_fn::create_udaf};

#[derive(Debug)]
struct CollectAccumulator {
    item_type: DataType,
    chunks: Vec<ArrayRef>,
    len: usize,
}

impl CollectAccumulator {
    fn new(item_type: DataType) -> Self {
        Self {
            item_type,
            chunks: Vec::new(),
            len: 0,
        }
    }

    fn append(&mut self, values: ArrayRef) -> DfResult<()> {
        self.len = self.len.checked_add(values.len()).ok_or_else(|| {
            DataFusionError::Execution("collect result exceeds Arrow list capacity".into())
        })?;
        if self.len > i32::MAX as usize {
            return Err(DataFusionError::Execution(
                "collect result exceeds Arrow list capacity".into(),
            ));
        }
        if values.len() > 0 {
            self.chunks.push(values);
        }
        Ok(())
    }

    fn values(&self) -> DfResult<ArrayRef> {
        if self.chunks.is_empty() {
            return Ok(new_empty_array(&self.item_type));
        }
        let chunks = self
            .chunks
            .iter()
            .map(|chunk| chunk.as_ref())
            .collect::<Vec<_>>();
        concat(&chunks).map_err(Into::into)
    }

    fn list_scalar(&self) -> DfResult<ScalarValue> {
        let values = self.values()?;
        let field = Arc::new(Field::new("item", self.item_type.clone(), true));
        let list = ListArray::new(field, OffsetBuffer::from_lengths([self.len]), values, None);
        Ok(ScalarValue::List(Arc::new(list)))
    }
}

impl Accumulator for CollectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let Some(values) = values.first() else {
            return Err(DataFusionError::Execution(
                "collect expects exactly one input".into(),
            ));
        };
        if values.data_type() != &self.item_type {
            // Physical expression results may refine nested field nullability
            // (for example, mapping an array to a non-null constant). Preserve
            // the declared logical type and normalize with Arrow's cast kernel.
            if !matches!(
                (crate::expressions::Type::from_arrow(values.data_type()),
                 crate::expressions::Type::from_arrow(&self.item_type)),
                (Ok(actual), Ok(expected)) if actual == expected
            ) {
                return Err(DataFusionError::Execution(format!(
                    "collect expected {:?}, got {:?}",
                    self.item_type,
                    values.data_type()
                )));
            }
            return self.append(arrow_cast::cast(values.as_ref(), &self.item_type)?);
        }
        self.append(values.clone())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.list_scalar()
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.chunks.capacity() * std::mem::size_of::<ArrayRef>()
            + self
                .chunks
                .iter()
                .map(|chunk| chunk.get_array_memory_size())
                .sum::<usize>()
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.list_scalar()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [states] = states else {
            return Err(DataFusionError::Execution(
                "collect state must contain exactly one list".into(),
            ));
        };
        let list = states
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("collect state must be a list".into()))?;
        if list.value_type() != self.item_type {
            return Err(DataFusionError::Execution(format!(
                "collect state expected {:?}, got {:?}",
                self.item_type,
                list.value_type()
            )));
        }
        for row in 0..list.len() {
            if list.is_valid(row) {
                self.append(list.value(row))?;
            }
        }
        Ok(())
    }
}

/// Construct the Hail `collect` UDAF for an Arrow input type.
pub fn collect_udf(input_type: &DataType) -> Arc<AggregateUDF> {
    let item_type = input_type.clone();
    let return_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
    let state_type = return_type.clone();
    let input_types = vec![item_type.clone()];
    let factory: datafusion_expr::AccumulatorFactoryFunction =
        Arc::new(move |_| Ok(Box::new(CollectAccumulator::new(item_type.clone()))));
    Arc::new(create_udaf(
        "collect",
        input_types,
        Arc::new(return_type),
        Volatility::Immutable,
        factory,
        Arc::new(vec![state_type]),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, ListArray, StructArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use datafusion_expr::utils::AggregateOrderSensitivity;

    #[test]
    fn collect_declares_its_input_order_requirement() {
        assert_eq!(
            collect_udf(&DataType::Int32).order_sensitivity(),
            AggregateOrderSensitivity::HardRequirement
        );
    }

    #[test]
    fn collect_normalizes_nested_nullability_without_coercing_value_types() {
        let expected = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let input: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        ));
        let mut accumulator = CollectAccumulator::new(expected.clone());
        accumulator.update_batch(&[input]).unwrap();
        assert_eq!(accumulator.values().unwrap().data_type(), &expected);
        let mut integer_accumulator = CollectAccumulator::new(DataType::Int64);
        assert!(
            integer_accumulator
                .update_batch(&[Arc::new(Int32Array::from(vec![1]))])
                .is_err()
        );
    }

    #[test]
    fn collect_merges_ordered_partial_states_and_keeps_nested_nulls_and_fields() {
        let inner_field = Arc::new(Field::new("labels", DataType::Int32, true));
        let nested_type = DataType::List(inner_field.clone());
        let nested = ListArray::new(
            inner_field.clone(),
            OffsetBuffer::from_lengths([2, 0, 1]),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        // Use a named struct child as well as a nested list child; both are
        // part of the exact Arrow DataType that collect must retain.
        let fields = vec![Arc::new(Field::new("payload", nested_type.clone(), true))];
        let input: ArrayRef = Arc::new(StructArray::new(
            fields.clone().into(),
            vec![Arc::new(nested)],
            Some(NullBuffer::from(vec![true, false, true])),
        ));
        let input_type = DataType::Struct(fields.clone().into());

        let mut left = CollectAccumulator::new(input_type.clone());
        let mut right = CollectAccumulator::new(input_type.clone());
        left.update_batch(&[input.slice(0, 1)]).unwrap();
        right.update_batch(&[input.slice(1, 2)]).unwrap();

        let partial_state = right.state().unwrap();
        let partial_array = ScalarValue::iter_to_array(partial_state).unwrap();
        left.merge_batch(&[partial_array]).unwrap();

        let result = left.evaluate().unwrap();
        let ScalarValue::List(result) = result else {
            panic!("expected list result")
        };
        assert_eq!(result.len(), 1);
        assert_eq!(result.value_type(), input_type);
        assert_eq!(result.values().len(), 3);
        assert!(result.values().is_null(1));
        assert_eq!(
            result.values().data_type(),
            &DataType::Struct(fields.into())
        );
        let result_struct = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(result_struct.column(0).data_type(), &nested_type);
        let result_nested = result_struct
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(result_nested.value(0).is_null(1));
        assert_eq!(result_nested.value(2).data_type(), &DataType::Int32);
    }

    #[test]
    fn collect_empty_result_is_typed_and_memory_size_tracks_chunks() {
        let item_type =
            DataType::Struct(vec![Arc::new(Field::new("n", DataType::Int32, true))].into());
        let mut empty_accumulator = CollectAccumulator::new(item_type.clone());
        let empty = empty_accumulator.evaluate().unwrap();
        let ScalarValue::List(empty) = empty else {
            panic!("expected list result")
        };
        assert_eq!(empty.len(), 1);
        assert_eq!(empty.value_type(), item_type);
        assert_eq!(empty.values().len(), 0);

        let mut accumulator = CollectAccumulator::new(item_type.clone());
        let base_size = accumulator.size();
        let input: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("n", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
            None,
        ));
        accumulator.update_batch(&[input]).unwrap();
        assert!(accumulator.size() > base_size);
    }
}
