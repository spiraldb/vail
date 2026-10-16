//! Nested window-aggregate adapters. DataFusion owns grouping and all reducer
//! algorithms; these adapters preserve their states inside Arrow collections.
use crate::scan_state::ScanState;
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, EmitTo, GroupsAccumulator};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::GroupsAccumulatorAdapter;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::aggregates::{
    group_values::{GroupValues, new_group_values},
    order::GroupOrdering,
};
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Clone, Debug)]
struct PhysicalIdentity(Arc<dyn PhysicalExpr>);
impl PartialEq for PhysicalIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for PhysicalIdentity {}
impl Hash for PhysicalIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ScopeIdentity {
    Composition {
        children: Vec<ScanState>,
        schema: SchemaRef,
        projection: PhysicalIdentity,
    },
    Transform {
        child: ScanState,
        transform: Transform,
    },
    Indexed {
        child: ScanState,
        key: DataType,
        array: bool,
    },
}
fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
fn structure(array: &ArrayRef) -> Result<&StructArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| error("scan scope requires Arrow struct"))
}
fn packed(array: &ArrayRef) -> Result<&StructArray> {
    let value = structure(array)?;
    if value.null_count() != 0 {
        return Err(error("scan packed parents must be defined"));
    }
    Ok(value)
}
fn input<'a>(values: &'a [ArrayRef], expected: &DataType) -> Result<&'a ArrayRef> {
    let [value] = values else {
        return Err(error("scan scope requires one packed input"));
    };
    if value.data_type() != expected {
        return Err(error("scan scope input type mismatch"));
    }
    Ok(value)
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
    if values.null_count() == 0 {
        let offsets = values.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[values.len()] as usize;
        return Ok(values.values().slice(start, end - start));
    }
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
    identity: ScopeIdentity,
    input: DataType,
    output: DataType,
    states: Vec<DataType>,
    factory: datafusion_expr::AccumulatorFactoryFunction,
) -> Arc<AggregateUDF> {
    crate::kernel_identity::aggregate(
        &format!("vail_scan_{name}"),
        identity,
        vec![input],
        output,
        states,
        factory,
    )
}

#[derive(Clone)]
pub struct Composition {
    pub children: Vec<ScanState>,
    pub schema: SchemaRef,
    pub projection: Arc<dyn PhysicalExpr>,
}
pub fn composed(spec: Composition, input: DataType) -> Result<Arc<AggregateUDF>> {
    let DataType::Struct(inputs) = &input else {
        return Err(error("scan composition input must be struct"));
    };
    if inputs.len() != spec.children.len()
        || spec.schema.fields().len() != spec.children.len()
        || spec
            .children
            .iter()
            .zip(inputs)
            .any(|(child, field)| child.input_type() != *field.data_type())
        || spec
            .children
            .iter()
            .zip(spec.schema.fields())
            .any(|(child, field)| child.output_type() != *field.data_type())
    {
        return Err(error("scan composition child schema mismatch"));
    }
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
        ScopeIdentity::Composition {
            children: spec.children.clone(),
            schema: spec.schema.clone(),
            projection: PhysicalIdentity(spec.projection.clone()),
        },
        input.clone(),
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
                input: input.clone(),
            }))
        }),
    ))
}
struct CompositionAccumulator {
    spec: Composition,
    children: Vec<Box<dyn Accumulator>>,
    input: DataType,
}
impl fmt::Debug for CompositionAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScanCompositionAccumulator")
    }
}
impl Accumulator for CompositionAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let packed = packed(input(values, &self.input)?)?;
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
        if states.len()
            != self
                .spec
                .children
                .iter()
                .map(|child| child.fields().len())
                .sum::<usize>()
        {
            return Err(error("scan composition state arity mismatch"));
        }
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transform {
    Filter,
    Explode,
}
pub fn transformed(
    child: ScanState,
    input: DataType,
    transform: Transform,
) -> Result<Arc<AggregateUDF>> {
    let valid = match (&transform, &input) {
        (Transform::Explode, DataType::List(item)) => *item.data_type() == child.input_type(),
        (Transform::Filter, DataType::Struct(fields)) => {
            fields.len() == 2
                && fields[0].name() == "predicate"
                && *fields[0].data_type() == DataType::Boolean
                && fields[1].name() == "value"
                && *fields[1].data_type() == child.input_type()
        }
        _ => false,
    };
    if !valid {
        return Err(error("scan transform input schema mismatch"));
    }
    let states = child
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    Ok(udf(
        "scope",
        ScopeIdentity::Transform {
            child: child.clone(),
            transform,
        },
        input.clone(),
        child.output_type(),
        states,
        Arc::new(move |_| {
            Ok(Box::new(TransformAccumulator {
                inner: child.accumulator()?,
                transform,
                input: input.clone(),
            }))
        }),
    ))
}
#[derive(Debug)]
struct TransformAccumulator {
    inner: Box<dyn Accumulator>,
    transform: Transform,
    input: DataType,
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
        let value = input(values, &self.input)?;
        let argument = match self.transform {
            Transform::Explode => flatten(list(value)?)?,
            Transform::Filter => {
                let packed = packed(value)?;
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
    input: DataType,
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
    let valid = if array {
        key == DataType::Int64
            && matches!(&input,DataType::List(item) if *item.data_type()==child.input_type())
            && matches!(&output,DataType::List(item) if *item.data_type()==child.output_type())
    } else {
        matches!(&input,DataType::Struct(fields) if fields.len()==2 && fields[0].name()=="key" && *fields[0].data_type()==key && fields[1].name()=="value" && *fields[1].data_type()==child.input_type())
            && matches!(&output,DataType::List(item) if matches!(item.data_type(),DataType::Struct(fields) if fields.len()==2 && fields[0].name()=="key" && *fields[0].data_type()==key && fields[1].name()=="value" && *fields[1].data_type()==child.output_type()))
    };
    if !valid {
        return Err(error("indexed scan schema mismatch"));
    }
    let spec = IndexedSpec {
        input: input.clone(),
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
        ScopeIdentity::Indexed {
            child: spec.child.clone(),
            key: spec.key.clone(),
            array,
        },
        input.clone(),
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
        let value = input(values, &self.spec.input)?;
        if self.spec.array {
            let arrays = list(value)?;
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
            let packed = packed(value)?;
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
        let [lengths, entries] = states else {
            return Err(error("indexed scan state arity mismatch"));
        };
        let lengths = lengths
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| error("invalid array scan length state"))?;
        let entries = list(entries)?;
        if lengths.len() != entries.len() {
            return Err(error("indexed scan state row counts differ"));
        }
        for row in 0..entries.len() {
            if entries.is_null(row) {
                continue;
            }
            if self.spec.array && lengths.is_valid(row) {
                self.shape(
                    usize::try_from(lengths.value(row))
                        .map_err(|_| error("negative array scan length"))?,
                )?;
            }
            let batch = entries.value(row);
            let batch = packed(&batch)?;
            if batch.num_columns() != 2 || batch.column(0).data_type() != &self.spec.key {
                return Err(error("indexed scan entry schema mismatch"));
            }
            let indices = if self.spec.array {
                if batch.len() != self.count() {
                    return Err(error("array scan state length mismatch"));
                }
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| error("array scan keys must be int64"))?;
                if keys.null_count() != 0
                    || keys
                        .values()
                        .iter()
                        .enumerate()
                        .any(|(index, key)| *key != index as i64)
                {
                    return Err(error("array scan state keys must be coordinate positions"));
                }
                (0..batch.len()).collect()
            } else {
                self.intern(batch.column(0))?
            };
            if batch.num_columns() != 2 || batch.column(0).data_type() != &self.spec.key {
                return Err(error("indexed scan entry schema mismatch"));
            }
            let nested = structure(batch.column(1))?;
            if nested.fields() != &self.spec.child.fields() || nested.null_count() != 0 {
                return Err(error("indexed scan child state schema mismatch"));
            }
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
