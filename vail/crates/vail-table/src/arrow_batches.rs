//! Immutable Arrow input batches with indexed, zero-copy row windows.
use crate::{Result, TableError};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::Arc;

/// Every batch has the declared schema. Cumulative row ends are nondecreasing
/// (empty batches are allowed), with checked total length. A view owns the same
/// buffers and restricts its absolute row range without copying batch contents.
#[derive(Debug, Clone)]
pub struct ArrowBatches {
    schema: SchemaRef,
    batches: Arc<[RecordBatch]>,
    ends: Arc<[usize]>,
    offset: usize,
    len: usize,
}
impl ArrowBatches {
    pub(crate) fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        let mut ends = Vec::with_capacity(batches.len());
        let mut len = 0usize;
        for batch in &batches {
            if batch.schema() != schema {
                return Err(TableError::Invalid(
                    "Arrow input batch schemas must match".into(),
                ));
            }
            len = len
                .checked_add(batch.num_rows())
                .ok_or_else(|| TableError::Invalid("Arrow input row count exceeds usize".into()))?;
            ends.push(len);
        }
        Ok(Self {
            schema,
            batches: batches.into(),
            ends: ends.into(),
            offset: 0,
            len,
        })
    }
    pub fn num_rows(&self) -> usize {
        self.len
    }
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    /// Proportional partition bounds with a widened product: even a zero-column
    /// source may have usize::MAX rows. Callers supply a valid partition index.
    pub(crate) fn partition_bounds(
        &self,
        part: usize,
        partitions: usize,
    ) -> std::ops::Range<usize> {
        assert!(
            partitions > 0 && part < partitions,
            "invalid Arrow partition index"
        );
        let bound =
            |index: usize| ((self.len as u128 * index as u128) / partitions as u128) as usize;
        bound(part)..bound(part + 1)
    }
    /// The requested window must lie within this view, as for RecordBatch::slice.
    pub(crate) fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset <= self.len && len <= self.len - offset,
            "Arrow batch view slice out of bounds"
        );
        Self {
            offset: self.offset + offset,
            len,
            ..self.clone()
        }
    }
    pub(crate) fn batches(&self) -> impl Iterator<Item = RecordBatch> + '_ {
        let end = self.offset + self.len;
        let first = self.ends.partition_point(|&at| at <= self.offset);
        self.batches
            .iter()
            .enumerate()
            .skip(first)
            .take_while(move |(i, _)| if *i == 0 { 0 } else { self.ends[i - 1] } < end)
            .filter_map(move |(i, batch)| {
                let base = if i == 0 { 0 } else { self.ends[i - 1] };
                let start = self.offset.saturating_sub(base);
                let len = (end - base).min(batch.num_rows()) - start;
                (len != 0).then(|| batch.slice(start, len))
            })
    }
}
