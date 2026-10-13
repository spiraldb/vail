//! Nested window-aggregate adapters. DataFusion owns grouping and all reducer
//! algorithms; these adapters preserve their states inside Arrow collections.
use crate::scan_state::ScanState;
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, EmitTo, GroupsAccumulator, Volatility, expr_fn::create_udaf,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::GroupsAccumulatorAdapter;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::aggregates::{
    group_values::{GroupValues, new_group_values},
    order::GroupOrdering,
};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
fn structure(array: &ArrayRef) -> Result<&StructArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("scan scope requires Arrow struct"))
}
fn list(array: &ArrayRef) -> Result<&ListArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("scan scope requires Arrow list"))
}
fn list_scalar(values: ArrayRef) -> ScalarValue {
    ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::from_lengths([values.len()]),
        values,
        None,
    )))
}
fn flatten(values: &ListArray) -> Result<ArrayRef> {
    let arrays = (0..values.len())
        .filter(|&i| values.is_valid(i))
        .map(|i| values.value(i))
        .collect::<Vec<_>>();
    if arrays.is_empty() {
        Ok(arrow_array::new_empty_array(&values.value_type()))
    } else {
        Ok(arrow_select::concat::concat(
            &arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
        )?)
    }
}
fn udf(
    name: &str,
    input: DataType,
    output: DataType,
    states: Vec<DataType>,
    factory: datafusion_expr::AccumulatorFactoryFunction,
) -> Arc<AggregateUDF> {
    Arc::new(create_udaf(
        &format!("vail_scan_{name}_{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        vec![input],
        Arc::new(output),
        Volatility::Immutable,
        factory,
        Arc::new(states),
    ))
}

#[derive(Clone)]
pub struct Composition {
    pub children: Vec<ScanState>,
    pub schema: SchemaRef,
    pub projection: Arc<dyn PhysicalExpr>,
}
pub fn composed(spec: Composition, input: DataType) -> Result<Arc<AggregateUDF>> {
    let output = spec.projection.data_type(&spec.schema)?;
    let states = spec
        .children
        .iter()
        .flat_map(|c| {
            c.fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>()
        })
        .collect();
    Ok(udf(
        "composition",
        input,
        output,
        states,
        Arc::new(move |_| {
            let children = spec
                .children
                .iter()
                .map(ScanState::accumulator)
                .collect::<Result<Vec<_>>>()?;
            Ok(Box::new(CompositionAccumulator {
                spec: spec.clone(),
                children,
            }))
        }),
    ))
}
struct CompositionAccumulator {
    spec: Composition,
    children: Vec<Box<dyn Accumulator>>,
}
impl fmt::Debug for CompositionAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScanCompositionAccumulator")
    }
}
impl Accumulator for CompositionAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let packed = structure(&values[0])?;
        for (child, input) in self.children.iter_mut().zip(packed.columns()) {
            child.update_batch(&[input.clone()])?;
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let columns = self
            .children
            .iter_mut()
            .map(|child| child.evaluate()?.to_array_of_size(1))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(self.spec.schema.clone(), columns)?;
        let value = self.spec.projection.evaluate(&batch)?.into_array(1)?;
        crate::expressions::scalar_at(&value, 0).map_err(|e| error(&e.to_string()))
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut states = Vec::new();
        for child in &mut self.children {
            states.extend(child.state()?);
        }
        Ok(states)
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let mut offset = 0;
        for (child, spec) in self.children.iter_mut().zip(&self.spec.children) {
            let end = offset + spec.fields().len();
            child.merge_batch(&states[offset..end])?;
            offset = end;
        }
        Ok(())
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.children.iter().map(|c| c.size()).sum::<usize>()
    }
}

#[derive(Clone, Copy)]
pub enum Transform {
    Filter,
    Explode,
}
pub fn transformed(child: ScanState, input: DataType, transform: Transform) -> Arc<AggregateUDF> {
    let states = child
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    udf(
        "scope",
        input,
        child.output_type(),
        states,
        Arc::new(move |_| {
            Ok(Box::new(TransformAccumulator {
                inner: child.accumulator()?,
                transform,
            }))
        }),
    )
}
#[derive(Debug)]
struct TransformAccumulator {
    inner: Box<dyn Accumulator>,
    transform: Transform,
}
impl fmt::Debug for Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Filter => "Filter",
            Self::Explode => "Explode",
        })
    }
}
impl Accumulator for TransformAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let argument = match self.transform {
            Transform::Explode => flatten(list(&values[0])?)?,
            Transform::Filter => {
                let packed = structure(&values[0])?;
                let filter = packed
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| error("scan filter requires boolean predicate"))?;
                arrow_select::filter::filter(packed.column(1).as_ref(), filter)?
            }
        };
        self.inner.update_batch(&[argument])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
    fn size(&self) -> usize {
        self.inner.size() + std::mem::size_of::<Self>()
    }
}

#[derive(Clone)]
struct IndexedSpec {
    child: ScanState,
    key: DataType,
    output: DataType,
    array: bool,
}
struct IndexedInner {
    spec: IndexedSpec,
    groups: Option<Box<dyn GroupValues>>,
    children: GroupsAccumulatorAdapter,
    keys: Vec<ScalarValue>,
    length: Option<usize>,
}
impl fmt::Debug for IndexedInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedScanAccumulator")
            .field("groups", &self.keys.len())
            .finish()
    }
}
impl IndexedSpec {
    fn accumulator(&self) -> Result<IndexedInner> {
        let child = self.child.clone();
        Ok(IndexedInner {
            spec: self.clone(),
            children: GroupsAccumulatorAdapter::new(move || child.snapshot_accumulator()),
            groups: if self.array {
                None
            } else {
                Some(new_group_values(
                    Arc::new(Schema::new(vec![Field::new("key", self.key.clone(), true)])),
                    &GroupOrdering::None,
                )?)
            },
            keys: vec![],
            length: None,
        })
    }
    fn entries_type(&self) -> DataType {
        DataType::Struct(
            vec![
                Field::new("key", self.key.clone(), true),
                Field::new("state", self.child.state_type(), true),
            ]
            .into(),
        )
    }
}
pub fn indexed(
    child: ScanState,
    input: DataType,
    key: DataType,
    output: DataType,
    array: bool,
) -> Result<Arc<AggregateUDF>> {
    let spec = IndexedSpec {
        child,
        key,
        output: output.clone(),
        array,
    };
    let states = vec![
        DataType::Int64,
        DataType::List(Arc::new(Field::new("item", spec.entries_type(), true))),
    ];
    Ok(udf(
        if array { "array" } else { "group" },
        input,
        output,
        states,
        Arc::new(move |_| {
            Ok(Box::new(IndexedAccumulator {
                inner: std::sync::Mutex::new(spec.accumulator()?),
            }))
        }),
    ))
}
impl IndexedInner {
    fn intern(&mut self, keys: &ArrayRef) -> Result<Vec<usize>> {
        let mut indices = Vec::new();
        self.groups
            .as_mut()
            .ok_or_else(|| error("array scan cannot intern keyed groups"))?
            .intern(&[keys.clone()], &mut indices)?;
        for (row, &index) in indices.iter().enumerate() {
            if index == self.keys.len() {
                self.keys.push(
                    crate::expressions::scalar_at(keys, row).map_err(|e| error(&e.to_string()))?,
                );
            }
        }
        Ok(indices)
    }
    fn shape(&mut self, length: usize) -> Result<()> {
        if let Some(previous) = self.length {
            if previous != length {
                return Err(error("scan.array_agg requires arrays of identical length"));
            }
        } else {
            self.length = Some(length);
        }
        Ok(())
    }
    fn count(&self) -> usize {
        if self.spec.array {
            self.length.unwrap_or(0)
        } else {
            self.keys.len()
        }
    }
    fn snapshot(&mut self) -> Result<ArrayRef> {
        let count = self.count();
        if count == 0 {
            return Ok(arrow_array::new_empty_array(&self.spec.child.state_type()));
        }
        // DF55's grouped adapter emits destructively. Export the state snapshots,
        // then restore them through its standard merge API so windows can grow.
        let states = self.children.evaluate(EmitTo::All)?;
        self.children.merge_batch(
            structure(&states)?.columns(),
            &(0..count).collect::<Vec<_>>(),
            count,
        )?;
        Ok(states)
    }
}
impl IndexedInner {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.spec.array {
            let arrays = list(&values[0])?;
            for row in 0..arrays.len() {
                if arrays.is_null(row) {
                    continue;
                }
                let value = arrays.value(row);
                self.shape(value.len())?;
                let indices = (0..value.len()).collect::<Vec<_>>();
                self.children
                    .update_batch(&[value], &indices, None, self.count())?;
            }
        } else {
            let packed = structure(&values[0])?;
            let indices = self.intern(packed.column(0))?;
            self.children.update_batch(
                &[packed.column(1).clone()],
                &indices,
                None,
                self.count(),
            )?;
        }
        Ok(())
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let states = self.snapshot()?;
        let count = self.count();
        let keys = if self.spec.array {
            Arc::new(Int64Array::from_iter_values((0..count).map(|i| i as i64))) as ArrayRef
        } else if self.keys.is_empty() {
            arrow_array::new_empty_array(&self.spec.key)
        } else {
            ScalarValue::iter_to_array(self.keys.clone())?
        };
        let DataType::Struct(fields) = self.spec.entries_type() else {
            unreachable!()
        };
        let entries: ArrayRef = Arc::new(StructArray::try_new(fields, vec![keys, states], None)?);
        Ok(vec![
            ScalarValue::Int64(self.length.map(|n| n as i64)),
            list_scalar(entries),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let lengths = states[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| error("invalid array scan length state"))?;
        let entries = list(&states[1])?;
        for row in 0..entries.len() {
            if entries.is_null(row) {
                continue;
            }
            if self.spec.array && lengths.is_valid(row) {
                self.shape(lengths.value(row) as usize)?;
            }
            let batch = entries.value(row);
            let batch = structure(&batch)?;
            let indices = if self.spec.array {
                (0..batch.len()).collect()
            } else {
                self.intern(batch.column(0))?
            };
            let nested = structure(batch.column(1))?;
            self.children
                .merge_batch(nested.columns(), &indices, self.count())?;
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.spec.array && self.length.is_none() {
            return ScalarValue::try_from(&self.spec.output);
        }
        let states = self.snapshot()?;
        let values = self.spec.child.finish_states(structure(&states)?)?;
        let DataType::List(item) = &self.spec.output else {
            return Err(error("indexed scan result must have list storage"));
        };
        let output = if self.spec.array {
            values
        } else {
            let keys = if self.keys.is_empty() {
                arrow_array::new_empty_array(&self.spec.key)
            } else {
                ScalarValue::iter_to_array(self.keys.clone())?
            };
            let order = arrow_ord::sort::sort_to_indices(keys.as_ref(), None, None)?;
            let DataType::Struct(fields) = item.data_type() else {
                return Err(error("dictionary scan requires entry struct"));
            };
            Arc::new(StructArray::try_new(
                fields.clone(),
                vec![
                    arrow_select::take::take(keys.as_ref(), &order, None)?,
                    arrow_select::take::take(values.as_ref(), &order, None)?,
                ],
                None,
            )?) as ArrayRef
        };
        Ok(ScalarValue::List(Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths([output.len()]),
            output,
            None,
        ))))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.children.size()
            + self.keys.iter().map(ScalarValue::size).sum::<usize>()
            + self.groups.as_ref().map_or(0, |g| g.size())
    }
}

#[derive(Debug)]
struct IndexedAccumulator {
    inner: std::sync::Mutex<IndexedInner>,
}
impl IndexedAccumulator {
    fn inner(&mut self) -> Result<&mut IndexedInner> {
        self.inner
            .get_mut()
            .map_err(|_| error("poisoned scan group adapter"))
    }
}
impl Accumulator for IndexedAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner()?.update_batch(values)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner()?.evaluate()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner()?.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner()?.merge_batch(states)
    }
    fn size(&self) -> usize {
        self.inner
            .lock()
            .map_or(std::mem::size_of::<Self>(), |inner| inner.size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{aggregates::aggregate_udf, scan_state::ScanState};

    fn grouped_output() -> DataType {
        let entry = DataType::Struct(
            vec![
                Field::new("key", DataType::Int64, true),
                Field::new("value", DataType::Int64, true),
            ]
            .into(),
        );
        DataType::List(Arc::new(Field::new("item", entry, true)))
    }
    use arrow_array::Int64Array;

    fn group_input(child: &ScanState, keys: &[i64], values: &[i64]) -> Result<ArrayRef> {
        let fields = vec![
            Arc::new(Field::new("key", DataType::Int64, true)),
            Arc::new(Field::new("input", child.input_type(), true)),
        ]
        .into();
        Ok(Arc::new(StructArray::try_new(
            fields,
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
            None,
        )?))
    }

    #[test]
    fn grouped_snapshots_can_be_repeated_and_merged() -> Result<()> {
        let child = ScanState::new(
            aggregate_udf("sum", Some(&DataType::Int64)).unwrap(),
            vec![DataType::Int64],
        )?;
        let mut indexed = IndexedSpec {
            child: child.clone(),
            key: DataType::Int64,
            output: grouped_output(),
            array: false,
        }
        .accumulator()?;
        indexed.update_batch(&[group_input(&child, &[9, 4, 9], &[2, 7, 3])?])?;

        let first = indexed.snapshot()?;
        let second = indexed.snapshot()?;
        assert_eq!(
            first.to_data(),
            second.to_data(),
            "snapshot must restore the grouped adapter state"
        );
        let state = indexed.state()?;

        let mut restored = IndexedSpec {
            child,
            key: DataType::Int64,
            output: grouped_output(),
            array: false,
        }
        .accumulator()?;
        let arrays = state
            .iter()
            .map(|value| value.to_array_of_size(1))
            .collect::<Result<Vec<_>>>()?;
        restored.merge_batch(&arrays)?;
        let result = restored.evaluate()?;
        let ScalarValue::List(result) = result else {
            panic!("grouped result must be a list")
        };
        let entries = result.value(0);
        let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            entries
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![4, 9])
        );
        assert_eq!(
            entries
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![7, 5])
        );
        Ok(())
    }

    #[test]
    fn array_scope_rejects_different_lengths() -> Result<()> {
        let child = ScanState::new(
            aggregate_udf("sum", Some(&DataType::Int64)).unwrap(),
            vec![DataType::Int64],
        )?;
        let mut indexed = IndexedSpec {
            child,
            key: DataType::Int64,
            output: DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            array: true,
        }
        .accumulator()?;
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let lists: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::from_lengths([2, 1]),
            values,
            None,
        ));
        assert!(
            indexed
                .update_batch(&[lists])
                .unwrap_err()
                .to_string()
                .contains("identical length")
        );
        Ok(())
    }
}
