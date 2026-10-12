//! Advanced collection aggregates backed by Arrow and DataFusion's row kernels.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, StructArray, UInt32Array, new_empty_array};
use arrow_buffer::OffsetBuffer;
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, SortOptions};
use arrow_select::concat::concat;
use arrow_select::take::take;
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, Volatility, expr_fn::create_udaf};
use datafusion_functions_aggregate::array_agg::DistinctArrayAggAccumulator;

use crate::aggregate_identity::configured_udaf_name;
use crate::expressions::ExprError;

#[derive(Debug)]
struct SetAccumulator {
    inner: DistinctArrayAggAccumulator,
    item_type: DataType,
}

impl SetAccumulator {
    fn list(&mut self) -> DfResult<ScalarValue> {
        let ScalarValue::List(values) = self.inner.evaluate()? else {
            return Err(DataFusionError::Internal(
                "distinct array aggregate returned a non-list".into(),
            ));
        };
        let item_field = Arc::new(Field::new("__vail_set_item", self.item_type.clone(), true));
        Ok(ScalarValue::List(Arc::new(ListArray::new(
            item_field,
            values.offsets().clone(),
            values.values().clone(),
            None, // Hail's empty set is defined, unlike SQL array_agg's NULL.
        ))))
    }
}

impl Accumulator for SetAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        self.inner.update_batch(values)
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.list()
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.list()?])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        self.inner.merge_batch(states)
    }
}

/// Build a collection aggregate from the Hail aggregate name and Arrow input.
/// The parent planner packs value plus any auxiliary expressions into a Struct.
pub fn make_udaf(
    function: &str,
    input_type: &DataType,
    options: &serde_json::Value,
) -> Result<Option<Arc<AggregateUDF>>, ExprError> {
    match function {
        "collect_as_set" => {
            let item_type = input_type.clone();
            let result_type = DataType::List(Arc::new(Field::new(
                "__vail_set_item",
                item_type.clone(),
                true,
            )));
            let state_type = result_type.clone();
            let accumulator_type = item_type.clone();
            let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
                Ok(Box::new(SetAccumulator {
                    inner: DistinctArrayAggAccumulator::try_new(&accumulator_type, None, false)?,
                    item_type: accumulator_type.clone(),
                }))
            });
            Ok(Some(Arc::new(create_udaf(
                "collect_as_set",
                vec![item_type],
                Arc::new(result_type),
                Volatility::Immutable,
                factory,
                Arc::new(vec![state_type]),
            ))))
        }
        "take" => {
            let n = options
                .get("n")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    ExprError::Invalid("take requires a non-negative integer option n".into())
                })?;
            if n > i32::MAX as u64 {
                return Err(ExprError::Invalid(
                    "take n exceeds Arrow list capacity".into(),
                ));
            }
            let ordered = options
                .get("ordered")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let value_type = if ordered {
                let DataType::Struct(fields) = input_type else {
                    return Err(ExprError::Invalid(
                        "ordered take expects a value/ordering struct".into(),
                    ));
                };
                if fields.len() != 2 {
                    return Err(ExprError::Invalid(
                        "ordered take requires value and ordering fields".into(),
                    ));
                }
                fields[0].data_type().clone()
            } else {
                input_type.clone()
            };
            let output = DataType::List(Arc::new(Field::new("item", value_type.clone(), true)));
            let state_type = if ordered {
                DataType::List(Arc::new(Field::new("item", input_type.clone(), true)))
            } else {
                output.clone()
            };
            let input_type = input_type.clone();
            let argument_type = input_type.clone();
            let n = n as usize;
            let udaf_name = configured_udaf_name("take", &input_type, options);
            let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
                Ok(Box::new(TakeAccumulator {
                    input_type: input_type.clone(),
                    n,
                    ordered,
                    items: None,
                }))
            });
            Ok(Some(Arc::new(create_udaf(
                &udaf_name,
                vec![argument_type],
                Arc::new(output),
                Volatility::Immutable,
                factory,
                Arc::new(vec![state_type]),
            ))))
        }
        "counter" => Ok(None),
        _ => Ok(None),
    }
}

#[derive(Debug)]
struct TakeAccumulator {
    input_type: DataType,
    n: usize,
    ordered: bool,
    items: Option<ArrayRef>,
}

impl TakeAccumulator {
    fn append(&mut self, incoming: &ArrayRef) -> DfResult<()> {
        if self.n == 0 || incoming.is_empty() {
            return Ok(());
        }
        let combined = if let Some(current) = &self.items {
            concat(&[current.as_ref(), incoming.as_ref()])?
        } else {
            incoming.clone()
        };
        let result = if self.ordered {
            let rows = combined
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("ordered take expects struct rows".into())
                })?;
            if rows.num_columns() != 2 {
                return Err(DataFusionError::Execution(
                    "ordered take expects value and ordering fields".into(),
                ));
            }
            let order = SortColumn {
                values: Arc::clone(rows.column(1)),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            };
            let indices = lexsort_to_indices(&[order], Some(self.n))?;
            take(rows.column(0).as_ref(), &indices, None)?
        } else {
            let count = combined.len().min(self.n);
            take(
                combined.as_ref(),
                &UInt32Array::from((0..count).map(|i| i as u32).collect::<Vec<_>>()),
                None,
            )?
        };
        self.items = Some(if self.ordered {
            // Keep original value/order pairs so distributed states can be merged consistently.
            let rows = combined.as_any().downcast_ref::<StructArray>().unwrap();
            let indices = lexsort_to_indices(
                &[SortColumn {
                    values: Arc::clone(rows.column(1)),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                }],
                Some(self.n),
            )?;
            take(combined.as_ref(), &indices, None)?
        } else {
            result
        });
        Ok(())
    }

    fn output(&self) -> DfResult<ScalarValue> {
        let item_type = if self.ordered {
            match &self.input_type {
                DataType::Struct(fields) => fields[0].data_type().clone(),
                _ => self.input_type.clone(),
            }
        } else {
            self.input_type.clone()
        };
        let items = match &self.items {
            Some(items) if self.ordered => items
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0)
                .clone(),
            Some(items) => items.clone(),
            None => new_empty_array(&item_type),
        };
        let len = items.len();
        Ok(ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(Field::new("item", item_type, true)),
            OffsetBuffer::from_lengths([len]),
            items,
            None,
        ))))
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn take_udafs_with_different_limits_have_distinct_identities() {
        let one = make_udaf("take", &DataType::Int64, &json!({"n": 1}))
            .unwrap()
            .unwrap();
        let three = make_udaf("take", &DataType::Int64, &json!({"n": 3}))
            .unwrap()
            .unwrap();
        assert_ne!(one.name(), three.name());
    }
}

impl Accumulator for TakeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let [values] = values else {
            return Err(DataFusionError::Execution("take expects one input".into()));
        };
        if values.data_type() != &self.input_type {
            return Err(DataFusionError::Execution(
                "take input type mismatch".into(),
            ));
        }
        self.append(values)
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.output()
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.items.as_ref().map_or(0, |x| x.get_array_memory_size())
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![if let Some(items) = &self.items {
            ScalarValue::List(Arc::new(ListArray::new(
                Arc::new(Field::new("item", self.input_type.clone(), true)),
                OffsetBuffer::from_lengths([items.len()]),
                items.clone(),
                None,
            )))
        } else {
            ScalarValue::List(Arc::new(ListArray::new(
                Arc::new(Field::new("item", self.input_type.clone(), true)),
                OffsetBuffer::from_lengths([0]),
                new_empty_array(&self.input_type),
                None,
            )))
        }])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [states] = states else {
            return Err(DataFusionError::Execution(
                "take state expects one list".into(),
            ));
        };
        let list = states
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("take state must be a list".into()))?;
        for i in 0..list.len() {
            if list.is_valid(i) {
                self.append(&list.value(i))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StructArray};
    use arrow_schema::Fields;

    #[test]
    fn set_retains_one_null_and_merges_partial_states() {
        let mut left = SetAccumulator {
            inner: DistinctArrayAggAccumulator::try_new(&DataType::Int64, None, false).unwrap(),
            item_type: DataType::Int64,
        };
        let mut right = SetAccumulator {
            inner: DistinctArrayAggAccumulator::try_new(&DataType::Int64, None, false).unwrap(),
            item_type: DataType::Int64,
        };
        left.update_batch(&[Arc::new(Int64Array::from(vec![Some(2), None]))])
            .unwrap();
        right
            .update_batch(&[Arc::new(Int64Array::from(vec![Some(2), Some(3), None]))])
            .unwrap();
        let partial = ScalarValue::iter_to_array(right.state().unwrap()).unwrap();
        left.merge_batch(&[partial]).unwrap();
        let ScalarValue::List(result) = left.evaluate().unwrap() else {
            panic!("expected list")
        };
        assert_eq!(result.values().len(), 3);
        assert_eq!(result.values().null_count(), 1);
        let values = result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let observed = (0..values.len())
            .filter_map(|i| values.is_valid(i).then(|| values.value(i)))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(observed, [2, 3].into_iter().collect());
    }

    #[test]
    fn ordered_take_keeps_smallest_keys_and_nulls_last() {
        let fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int64, true)),
            Arc::new(Field::new("ordering", DataType::Int64, true)),
        ]
        .into();
        let packed: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(40),
                ])),
                Arc::new(Int64Array::from(vec![Some(3), None, Some(1), Some(2)])),
            ],
            None,
        ));
        let mut accumulator = TakeAccumulator {
            input_type: DataType::Struct(fields),
            n: 3,
            ordered: true,
            items: None,
        };
        accumulator.update_batch(&[packed]).unwrap();
        let ScalarValue::List(output) = accumulator.evaluate().unwrap() else {
            panic!("expected list")
        };
        let values = output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            (0..values.len())
                .map(|i| values.value(i))
                .collect::<Vec<_>>(),
            vec![30, 40, 10]
        );
    }
}
