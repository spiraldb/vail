//! Prepared block execution of a list map containing an independent inner map.
//! Input owners stay in their original domains; only narrow coordinates expand.
use crate::indexed_batch::{self, Input, Source};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StructArray, new_empty_array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Schema};
use arrow_select::{concat::concat, take::take};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    collections::{HashMap,HashSet},
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub(crate) struct ProductMap {
    pub choices: Arc<dyn PhysicalExpr>,
    pub entries: Arc<dyn PhysicalExpr>,
    pub captures: Vec<Arc<dyn PhysicalExpr>>,
    pub inputs: Vec<Input>,
    pub capture_slots: Vec<Option<usize>>,
    pub body: Arc<dyn PhysicalExpr>,
    pub dtype: DataType,
    pub inner_field: usize,
    pub outer_paths: Vec<Vec<usize>>,
    pub frame: Arc<Schema>,
}
impl PartialEq for ProductMap {
    fn eq(&self, other: &Self) -> bool {
        self.choices.eq(&other.choices)
            && self.entries.eq(&other.entries)
            && self.captures == other.captures
            && self.body.eq(&other.body)
            && self.dtype == other.dtype
            && self.inner_field == other.inner_field
            && self.outer_paths == other.outer_paths
            && self.inputs == other.inputs
            && self.capture_slots == other.capture_slots
            && self.frame == other.frame
    }
}
impl Eq for ProductMap {}
impl Hash for ProductMap {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.choices.hash(h);
        self.entries.hash(h);
        self.captures.hash(h);
        self.body.hash(h);
        self.dtype.hash(h);
        self.inner_field.hash(h);
        self.outer_paths.hash(h);
        self.inputs.hash(h);
        self.capture_slots.hash(h);
        self.frame.hash(h);
    }
}
impl fmt::Display for ProductMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ListProductMap")
    }
}
impl PhysicalExpr for ProductMap {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut out = vec![&self.choices, &self.entries];
        out.extend(self.captures.iter());
        out
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 2 + self.captures.len() {
            return Err(DataFusionError::Internal(
                "product-map inputs changed".into(),
            ));
        }
        Ok(Arc::new(Self {
            choices: children[0].clone(),
            entries: children[1].clone(),
            captures: children[2..].to_vec(),
            ..self.as_ref().clone()
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.execute(batch).map(ColumnarValue::Array)
    }
}
fn list(a: &ArrayRef) -> Result<&ListArray> {
    a.as_any()
        .downcast_ref()
        .ok_or_else(|| DataFusionError::Execution("product-map expects List32".into()))
}
fn offset(n: usize) -> Result<i32> {
    i32::try_from(n)
        .map_err(|_| DataFusionError::Execution("product-map output exceeds List32".into()))
}
impl ProductMap {
    fn execute(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let choices = self.choices.evaluate(batch)?.into_array(batch.num_rows())?;
        let choices = list(&choices)?;
        let DataType::List(item) = &self.dtype else {
            unreachable!()
        };
        let DataType::Struct(fields) = item.data_type() else {
            unreachable!()
        };
        let DataType::List(inner_item) = fields[self.inner_field].data_type() else {
            unreachable!()
        };
        // An empty outer map never demands its inner producer or captures.
        if (0..choices.len()).all(|r| {
            choices.is_null(r) || choices.value_offsets()[r] == choices.value_offsets()[r + 1]
        }) {
            return Ok(Arc::new(ListArray::try_new(
                item.clone(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0; choices.len() + 1])),
                new_empty_array(item.data_type()),
                choices.nulls().cloned(),
            )?));
        }
        let entries = self.entries.evaluate(batch)?.into_array(batch.num_rows())?;
        let entries = list(&entries)?;
        let captures = self
            .captures
            .iter()
            .map(|p| p.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        let owners=captures.iter().map(|value| match value {
            ColumnarValue::Array(array)=>Ok((array.clone(),false)),
            ColumnarValue::Scalar(value)=>Ok((value.to_array()?,true)),
        }).collect::<Result<Vec<_>>>()?;
        let mut validated=HashSet::new();
        let sources = self
            .inputs
            .iter()
            .enumerate()
            .map(|(slot, input)| {
                let (owner, scalar) = match input.axis.as_str() {
                    "entry" => (entries.values().clone(), false),
                    "row" => (choices.values().clone(), false),
                    "col" => {
                        let i = self.capture_slots[slot].unwrap();
                        owners[i].clone()
                    }
                    _ => return Err(DataFusionError::Internal("invalid product domain".into())),
                };
                let descriptor=input.root_type.as_ref().map_or(0,|d|Arc::as_ptr(d) as usize);
                let owner_id=Arc::as_ptr(&owner) as *const () as usize;
                if validated.insert((descriptor,owner_id)) {Source::validate_root(&owner,input)?;}
                Source::bind_validated(owner, input, scalar)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut expected = 0usize;
        let mut expanded = 0usize;
        for r in 0..choices.len() {
            if choices.is_valid(r) {
                let copies = (choices.value_offsets()[r + 1] - choices.value_offsets()[r]) as usize;
                expanded = expanded.checked_add(copies).ok_or_else(|| {
                    DataFusionError::Execution("product expansion overflow".into())
                })?;
                let width = if entries.is_valid(r) {
                    (entries.value_offsets()[r + 1] - entries.value_offsets()[r]) as usize
                } else {
                    0
                };
                expected = expected
                    .checked_add(copies.checked_mul(width).ok_or_else(|| {
                        DataFusionError::Execution("product child overflow".into())
                    })?)
                    .ok_or_else(|| DataFusionError::Execution("product child overflow".into()))?;
            }
        }
        offset(expected)?;
        offset(expanded)?;
        let body = indexed_batch::bind(&self.body, &sources, &mut HashMap::new())?;
        let mut outer_offsets = vec![0i32];
        let mut inner_offsets = vec![0i32];
        let mut inner_valid = vec![];
        let mut choice_ids = vec![];
        let mut chunks = vec![];
        let block = 65536usize;
        let mut entry_ids = Vec::with_capacity(block);
        let mut outer_ids = Vec::with_capacity(block);
        let mut parent_ids = Vec::with_capacity(block);
        let flush = |entry_ids: &mut Vec<i32>,
                     outer_ids: &mut Vec<i32>,
                     parent_ids: &mut Vec<i32>,
                     chunks: &mut Vec<ArrayRef>|
         -> Result<()> {
            if entry_ids.is_empty() {
                return Ok(());
            }
            let rows = entry_ids.len();
            let frame = RecordBatch::try_new(
                self.frame.clone(),
                vec![
                    Arc::new(Int32Array::from(std::mem::replace(
                        entry_ids,
                        Vec::with_capacity(block),
                    ))),
                    Arc::new(Int32Array::from(std::mem::replace(
                        parent_ids,
                        Vec::with_capacity(block),
                    ))),
                    Arc::new(Int32Array::from(std::mem::replace(
                        outer_ids,
                        Vec::with_capacity(block),
                    ))),
                    Arc::new(BooleanArray::new(BooleanBuffer::new_set(rows), None)),
                ],
            )?;
            let output = body.evaluate(&frame)?.into_array(rows)?;
            if output.len() != rows {
                return Err(DataFusionError::Execution(
                    "product-map body cardinality changed".into(),
                ));
            }
            chunks.push(output);
            Ok(())
        };
        let mut total = 0usize;
        for r in 0..choices.len() {
            if choices.is_valid(r) {
                for c in choices.value_offsets()[r]..choices.value_offsets()[r + 1] {
                    choice_ids.push(c);
                    inner_valid.push(entries.is_valid(r));
                    if entries.is_valid(r) {
                        for e in entries.value_offsets()[r]..entries.value_offsets()[r + 1] {
                            entry_ids.push(e);
                            outer_ids.push(c);
                            parent_ids.push(offset(r)?);
                            total = total.checked_add(1).ok_or_else(|| {
                                DataFusionError::Execution("product-map length overflow".into())
                            })?;
                            if entry_ids.len() == block {
                                flush(
                                    &mut entry_ids,
                                    &mut outer_ids,
                                    &mut parent_ids,
                                    &mut chunks,
                                )?;
                            }
                        }
                    }
                    inner_offsets.push(offset(total)?);
                }
            }
            outer_offsets.push(offset(choice_ids.len())?);
        }
        flush(&mut entry_ids, &mut outer_ids, &mut parent_ids, &mut chunks)?;
        let values = match chunks.len() {
            0 => new_empty_array(inner_item.data_type()),
            1 => chunks.pop().unwrap(),
            _ => concat(&chunks.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?,
        };
        let inner = Arc::new(ListArray::try_new(
            inner_item.clone(),
            OffsetBuffer::new(ScalarBuffer::from(inner_offsets)),
            values,
            Some(NullBuffer::from(inner_valid)),
        )?) as ArrayRef;
        let indices = Int32Array::from(choice_ids);
        let mut columns = Vec::with_capacity(fields.len());
        let mut path = 0;
        for i in 0..fields.len() {
            if i == self.inner_field {
                columns.push(inner.clone());
                continue;
            }
            let mut owner = choices.values().clone();
            let mut ancestors = vec![];
            for index in &self.outer_paths[path] {
                let a = owner
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("product outer storage changed".into())
                    })?;
                if let Some(nulls) = a.nulls() {
                    ancestors.push(nulls.clone());
                }
                owner = a.column(*index).clone();
            }
            path += 1;
            let mut selected_indices = indices.clone();
            if !ancestors.is_empty() {
                let mut mask = NullBufferBuilder::new(indices.len());
                for index in indices.values() {
                    mask.append(ancestors.iter().all(|n| n.is_valid(*index as usize)));
                }
                selected_indices = Int32Array::new(indices.values().clone(), mask.finish());
            }
            let selected_indices = arrow_array::UInt32Array::from(
                selected_indices
                    .iter()
                    .map(|i| i.map(|i| i as u32))
                    .collect::<Vec<_>>(),
            );
            let selected = if let Some(lists) = owner
                .as_any()
                .downcast_ref::<ListArray>()
                .filter(|a| a.values().data_type().is_primitive())
            {
                indexed_batch::take_primitive_lists(lists, &selected_indices)?
            } else {
                take(owner.as_ref(), &selected_indices, None)?
            };
            columns.push(selected);
        }
        let values = Arc::new(StructArray::try_new_with_length(
            fields.clone(),
            columns,
            None,
            indices.len(),
        )?) as ArrayRef;
        Ok(Arc::new(ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(ScalarBuffer::from(outer_offsets)),
            values,
            choices.nulls().cloned(),
        )?))
    }
}
