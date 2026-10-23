//! Coordinate layout adapter for native ARRAY_AGG's ListArray state protocol.
//! Retain Arrow inputs with implicit row-major coordinates; Arrow performs the
//! necessary coordinate-major output copy once, without per-entry group IDs,
//! counting-sort metadata, or an interleave index array.
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, make_array, new_empty_array};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result};
use std::sync::Arc;

fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Debug)]
enum Source {
    Rows {
        values: ArrayRef,
        filter: Option<BooleanArray>,
    },
    State(ArrayRef),
}
impl Source {
    fn values(&self) -> &ArrayRef {
        match self {
            Self::Rows { values, .. } => values,
            Self::State(state) => state.as_any().downcast_ref::<ListArray>().unwrap().values(),
        }
    }
    fn size(&self) -> usize {
        match self {
            Self::Rows { values, filter } => {
                values.get_array_memory_size()
                    + filter.as_ref().map_or(0, |f| f.get_array_memory_size())
            }
            Self::State(state) => state.get_array_memory_size(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct CoordinateCollect {
    dtype: DataType,
    sources: Vec<Source>,
    snapshot: Option<ArrayRef>,
}
impl CoordinateCollect {
    pub(crate) fn new(dtype: DataType) -> Self {
        Self {
            dtype,
            sources: Vec::new(),
            snapshot: None,
        }
    }
    fn extend_snapshot(&mut self) {
        if let Some(state) = self.snapshot.take() {
            // state() may run before the first input establishes width.
            if !state.is_empty() {
                self.sources.push(Source::State(state));
            }
        }
    }
    pub(crate) fn update(
        &mut self,
        values: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        if values.data_type() != &self.dtype {
            return Err(error("coordinate collect input type mismatch"));
        }
        if width == 0 || values.len() % width != 0 {
            return Err(error("coordinate collect input width mismatch"));
        }
        if filter.is_some_and(|filter| filter.len() != values.len()) {
            return Err(error("coordinate collect filter length mismatch"));
        }
        self.extend_snapshot();
        self.sources.push(Source::Rows {
            values: Arc::clone(values),
            filter: filter.cloned(),
        });
        Ok(())
    }
    pub(crate) fn merge(&mut self, states: &[ArrayRef], width: usize) -> Result<()> {
        let [state] = states else {
            return Err(error(
                "coordinate collect requires one native partial state",
            ));
        };
        let list = state
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| error("coordinate collect partial state must be a list"))?;
        if list.len() != width || list.values().data_type() != &self.dtype {
            return Err(error(
                "coordinate collect partial state type/width mismatch",
            ));
        }
        self.extend_snapshot();
        self.sources.push(Source::State(Arc::clone(state)));
        Ok(())
    }
    pub(crate) fn array(&mut self, width: usize) -> Result<ArrayRef> {
        if let Some(snapshot) = &self.snapshot {
            return Ok(Arc::clone(snapshot));
        }
        let mut counts = vec![0usize; width];
        for source in &self.sources {
            match source {
                Source::Rows {
                    values,
                    filter: None,
                } => {
                    let rows = values.len() / width;
                    for count in &mut counts {
                        *count = count
                            .checked_add(rows)
                            .ok_or_else(|| error("coordinate collect length overflow"))?;
                    }
                }
                Source::Rows {
                    values,
                    filter: Some(filter),
                } => {
                    for row in 0..values.len() / width {
                        for (column, count) in counts.iter_mut().enumerate() {
                            let index = row * width + column;
                            if filter.is_valid(index) && filter.value(index) {
                                *count = count
                                    .checked_add(1)
                                    .ok_or_else(|| error("coordinate collect length overflow"))?;
                            }
                        }
                    }
                }
                Source::State(state) => {
                    let lists = state.as_any().downcast_ref::<ListArray>().unwrap();
                    for (column, count) in counts.iter_mut().enumerate() {
                        if lists.is_valid(column) {
                            *count = count
                                .checked_add(lists.value_length(column) as usize)
                                .ok_or_else(|| error("coordinate collect length overflow"))?;
                        }
                    }
                }
            }
        }
        let mut offsets = Vec::with_capacity(width + 1);
        offsets.push(0i32);
        let mut total = 0usize;
        for count in &counts {
            total = total
                .checked_add(*count)
                .ok_or_else(|| error("coordinate collect length overflow"))?;
            offsets.push(
                i32::try_from(total)
                    .map_err(|_| error("coordinate collect exceeds Arrow List offset capacity"))?,
            );
        }
        let values = if total == 0 {
            new_empty_array(&self.dtype)
        } else {
            // Compile native Arrow extension functions once per source batch,
            // outside the coordinate/value loop. No scalar arrays are created.
            let data = self
                .sources
                .iter()
                .map(|source| source.values().to_data())
                .collect::<Vec<_>>();
            let mut output = MutableArrayData::new(data.iter().collect(), false, total);
            for column in 0..width {
                for (source_index, source) in self.sources.iter().enumerate() {
                    match source {
                        Source::Rows { values, filter } => {
                            for index in (column..values.len()).step_by(width) {
                                if filter.as_ref().is_none_or(|filter| {
                                    filter.is_valid(index) && filter.value(index)
                                }) {
                                    output.extend(source_index, index, index + 1);
                                }
                            }
                        }
                        Source::State(state) => {
                            let lists = state.as_any().downcast_ref::<ListArray>().unwrap();
                            if lists.is_valid(column) {
                                let offsets = lists.value_offsets();
                                output.extend(
                                    source_index,
                                    offsets[column] as usize,
                                    offsets[column + 1] as usize,
                                );
                            }
                        }
                    }
                }
            }
            make_array(output.freeze())
        };
        let nulls = counts
            .iter()
            .any(|count| *count == 0)
            .then(|| NullBuffer::from(counts.iter().map(|count| *count != 0).collect::<Vec<_>>()));
        let result: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(self.dtype.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            nulls,
        )?);
        self.sources.clear();
        self.snapshot = Some(Arc::clone(&result));
        Ok(result)
    }
    pub(crate) fn size(&self) -> usize {
        self.sources.capacity() * std::mem::size_of::<Source>()
            + self.sources.iter().map(Source::size).sum::<usize>()
            + self
                .snapshot
                .as_ref()
                .map_or(0, |state| state.get_array_memory_size())
    }
}
