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
use crate::expressions::{ExprError, Type};

#[derive(Debug)]
struct SetAccumulator {
    inner: DistinctArrayAggAccumulator,
    item_field: Arc<Field>,
}

impl SetAccumulator {
    fn list(&mut self) -> DfResult<ScalarValue> {
        let ScalarValue::List(values) = self.inner.evaluate()? else {
            return Err(DataFusionError::Internal(
                "distinct array aggregate returned a non-list".into(),
            ));
        };
        Ok(ScalarValue::List(Arc::new(ListArray::new(
            self.item_field.clone(),
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
            let result_type = Type::Set {
                set: Box::new(Type::from_arrow(&item_type)?),
            }
            .to_arrow()?;
            let state_type = result_type.clone();
            let DataType::List(item_field) = &result_type else {
                unreachable!()
            };
            let item_field = item_field.clone();
            let accumulator_type = item_type.clone();
            let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
                Ok(Box::new(SetAccumulator {
                    inner: DistinctArrayAggAccumulator::try_new(&accumulator_type, None, false)?,
                    item_field: item_field.clone(),
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
                if fields.len() != 2
                    || fields[0].name() != "value"
                    || fields[1].name() != "ordering"
                {
                    return Err(ExprError::Invalid(
                        "ordered take requires value then ordering fields".into(),
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
            let argument_type = input_type.clone();
            let config = Arc::new(TakeConfig {
                input_type: input_type.clone(),
                item_type: value_type,
                n: n as usize,
            });
            let udaf_name = configured_udaf_name("take", input_type, options);
            let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
                Ok(Box::new(TakeAccumulator {
                    config: config.clone(),
                    state: if ordered {
                        TakeState::Ordered(None)
                    } else {
                        TakeState::Unordered {
                            chunks: Vec::new(),
                            len: 0,
                        }
                    },
                }))
            });
            Ok(Some(crate::kernel_identity::aggregate_named(
                &udaf_name,
                TakeIdentity { input: input_type.clone(), n: n as usize, ordered },
                vec![argument_type],
                output,
                vec![state_type],
                factory,
            )))
        }
        "counter" => Ok(None),
        _ => Ok(None),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TakeIdentity {
    input: DataType,
    n: usize,
    ordered: bool,
}

pub(crate) fn coordinate_take(udf: &AggregateUDF) -> Option<crate::coordinate_collect::CoordinateCollect> {
    let identity = crate::kernel_identity::aggregate_key::<TakeIdentity>(udf)?;
    (!identity.ordered && bounded_take_owns_values(&identity.input)).then(|| crate::coordinate_collect::CoordinateCollect::bounded(identity.input.clone(), identity.n))
}

pub(crate) fn coordinate_ordered_take(udf: &AggregateUDF) -> Option<crate::coordinate_take::CoordinateOrderedTake> {
    let identity = crate::kernel_identity::aggregate_key::<TakeIdentity>(udf)?;
    (identity.ordered && bounded_take_owns_values(&identity.input)).then(|| crate::coordinate_take::CoordinateOrderedTake::new(identity.input.clone(), identity.n))
}

// Arrow MutableArrayData retains all variadic buffers for view storage. Keep
// these layouts on the native fallback until retained view buffers are compacted.
fn bounded_take_owns_values(dtype: &DataType) -> bool {
    match dtype {
        DataType::Utf8View | DataType::BinaryView | DataType::Dictionary(_, _) => false,
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => bounded_take_owns_values(f.data_type()),
        DataType::ListView(_) | DataType::LargeListView(_) => false,
        DataType::Struct(fields) => fields.iter().all(|f| bounded_take_owns_values(f.data_type())),
        DataType::Union(fields, _) => fields.iter().all(|(_,f)| bounded_take_owns_values(f.data_type())),
        DataType::RunEndEncoded(_, values) => bounded_take_owns_values(values.data_type()),
        _ => true,
    }
}

#[derive(Debug)]
struct TakeConfig {
    input_type: DataType,
    item_type: DataType,
    n: usize,
}
#[derive(Debug)]
enum TakeState {
    Ordered(Option<ArrayRef>),
    Unordered { chunks: Vec<ArrayRef>, len: usize },
}
#[derive(Debug)]
struct TakeAccumulator {
    config: Arc<TakeConfig>,
    state: TakeState,
}
fn chunks_value(chunks: &[ArrayRef], dtype: &DataType) -> DfResult<ArrayRef> {
    match chunks {
        [] => Ok(new_empty_array(dtype)),
        [one] => Ok(one.clone()),
        _ => concat(
            &chunks
                .iter()
                .map(|chunk| chunk.as_ref())
                .collect::<Vec<_>>(),
        )
        .map_err(Into::into),
    }
}
impl TakeAccumulator {
    fn append(&mut self, incoming: &ArrayRef) -> DfResult<()> {
        let n = self.config.n;
        if n == 0 || incoming.is_empty() {
            return Ok(());
        }
        match &mut self.state {
            TakeState::Unordered { chunks, len } => {
                if *len == n {
                    return Ok(());
                }
                let count = incoming.len().min(n - *len);
                // Own only retained rows, including when merging sliced states.
                let indices = UInt32Array::from_iter_values((0..count).map(|i| i as u32));
                chunks.push(take(incoming.as_ref(), &indices, None)?);
                *len += count;
            }
            TakeState::Ordered(current) => {
                let rows = incoming
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("ordered take expects packed records".into())
                    })?;
                if rows.null_count() != 0 {
                    return Err(DataFusionError::Execution(
                        "ordered take packed records must be defined".into(),
                    ));
                }
                let previous = current
                    .as_ref()
                    .map(|array| array.as_any().downcast_ref::<StructArray>().unwrap());
                let left_len = previous.map_or(0, |rows| rows.len());
                if left_len
                    .checked_add(rows.len())
                    .is_none_or(|len| len > u32::MAX as usize)
                {
                    return Err(DataFusionError::Execution(
                        "ordered take batch exceeds Arrow index capacity".into(),
                    ));
                }
                let keys = if let Some(previous) = previous {
                    concat(&[previous.column(1).as_ref(), rows.column(1).as_ref()])?
                } else {
                    rows.column(1).clone()
                };
                let indices = lexsort_to_indices(
                    &[SortColumn {
                        values: keys,
                        options: Some(SortOptions {
                            descending: false,
                            nulls_first: false,
                        }),
                    }],
                    Some(n),
                )?;
                let selected = if let Some(previous) = current.as_ref() {
                    let selection = indices
                        .values()
                        .iter()
                        .map(|i| {
                            let i = *i as usize;
                            if i < left_len {
                                (0, i)
                            } else {
                                (1, i - left_len)
                            }
                        })
                        .collect::<Vec<_>>();
                    arrow_select::interleave::interleave(
                        &[previous.as_ref(), incoming.as_ref()],
                        &selection,
                    )?
                } else {
                    take(incoming.as_ref(), &indices, None)?
                };
                *current = Some(selected);
            }
        }
        Ok(())
    }
    fn retained(&self) -> DfResult<ArrayRef> {
        match &self.state {
            TakeState::Ordered(Some(items)) => Ok(items.clone()),
            TakeState::Ordered(None) => Ok(new_empty_array(&self.config.input_type)),
            TakeState::Unordered { chunks, .. } => chunks_value(chunks, &self.config.input_type),
        }
    }
    fn list(items: ArrayRef) -> ScalarValue {
        ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(Field::new("item", items.data_type().clone(), true)),
            OffsetBuffer::from_lengths([items.len()]),
            items,
            None,
        )))
    }
}
impl Accumulator for TakeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let [values] = values else {
            return Err(DataFusionError::Execution("take expects one input".into()));
        };
        if values.data_type() != &self.config.input_type {
            return Err(DataFusionError::Execution(
                "take input type mismatch".into(),
            ));
        }
        self.append(values)
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let retained = self.retained()?;
        let values = match &self.state {
            TakeState::Ordered(_) => retained
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0)
                .clone(),
            _ => retained,
        };
        debug_assert_eq!(values.data_type(), &self.config.item_type);
        Ok(Self::list(values))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + match &self.state {
                TakeState::Ordered(items) => items
                    .as_ref()
                    .map_or(0, |array| array.get_array_memory_size()),
                TakeState::Unordered { chunks, .. } => {
                    chunks.capacity() * std::mem::size_of::<ArrayRef>()
                        + chunks
                            .iter()
                            .map(|chunk| chunk.get_array_memory_size())
                            .sum::<usize>()
                }
            }
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![Self::list(self.retained()?)])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [states] = states else {
            return Err(DataFusionError::Execution(
                "take state expects one list".into(),
            ));
        };
        let lists = states
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("take state must be list".into()))?;
        if lists.value_type() != self.config.input_type {
            return Err(DataFusionError::Execution(
                "take state item type mismatch".into(),
            ));
        }
        for row in 0..lists.len() {
            if lists.is_valid(row) {
                self.append(&lists.value(row))?;
            }
        }
        Ok(())
    }
}
