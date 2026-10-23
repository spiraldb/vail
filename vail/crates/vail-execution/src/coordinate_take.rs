//! Ordered coordinate TAKE using prepared native Arrow comparison/selection.
//! Only selected owned records persist; no per-entry coordinate group IDs or
//! full coordinate-value gathers are materialized.
use arrow_array::{
    Array, ArrayRef, BooleanArray, ListArray, StructArray, make_array, new_empty_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_ord::sort::{LexicographicalComparator, SortColumn, partial_sort};
use arrow_schema::{DataType, Field, SortOptions};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result};
use std::sync::Arc;

fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
#[derive(Debug)]
pub(crate) struct CoordinateOrderedTake {
    input: DataType,
    n: usize,
    state: Option<Arc<ListArray>>,
    candidates: Vec<usize>,
    selected: Vec<usize>,
}
impl CoordinateOrderedTake {
    pub(crate) fn new(input: DataType, n: usize) -> Self {
        Self {
            input,
            n,
            state: None,
            candidates: Vec::new(),
            selected: Vec::new(),
        }
    }
    fn ensure_width(&mut self, width: usize) -> Result<()> {
        // state() may be requested before input establishes coordinate width.
        if width > 0 && self.state.as_ref().is_some_and(|state| state.is_empty()) {
            self.state = None;
        }
        if let Some(state) = &self.state {
            if state.len() != width {
                return Err(error("coordinate ordered take width mismatch"));
            }
        } else {
            self.state = Some(Arc::new(ListArray::try_new(
                Arc::new(Field::new_list_field(self.input.clone(), true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![
                    0i32;
                    width.checked_add(1).ok_or_else(
                        || error("coordinate take width overflow")
                    )?
                ])),
                new_empty_array(&self.input),
                None,
            )?));
        }
        Ok(())
    }
    pub(crate) fn update(
        &mut self,
        values: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        if width == 0
            || values.len() % width != 0
            || filter.is_some_and(|f| f.len() != values.len())
            || values.data_type() != &self.input
        {
            return Err(error("coordinate ordered take input type/shape mismatch"));
        }
        self.select(values, None, filter, width)
    }
    pub(crate) fn merge(&mut self, states: &[ArrayRef], width: usize) -> Result<()> {
        let [state] = states else {
            return Err(error(
                "coordinate ordered take requires one native list state",
            ));
        };
        let state = state
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| error("coordinate ordered take state must be a list"))?;
        if state.len() != width || state.value_type() != self.input {
            return Err(error("coordinate ordered take state type/width mismatch"));
        }
        self.select(state.values(), Some(state), None, width)
    }
    fn select(
        &mut self,
        input: &ArrayRef,
        ranges: Option<&ListArray>,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        self.ensure_width(width)?;
        if self.n == 0 {
            return Ok(());
        }
        let previous = Arc::clone(self.state.as_ref().unwrap());
        let old = previous
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error("ordered take requires packed value/ordering records"))?;
        let incoming = input
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error("ordered take requires packed value/ordering records"))?;
        if old.num_columns() != 2 || incoming.num_columns() != 2 {
            return Err(error("ordered take requires value and ordering fields"));
        }
        let old_len = old.len();
        let keys = if old_len == 0 {
            Arc::clone(incoming.column(1))
        } else {
            concat(&[old.column(1).as_ref(), incoming.column(1).as_ref()])?
        };
        // Type dispatch/comparator construction happens once for the batch,
        // not once for each coordinate or candidate record.
        let comparator = LexicographicalComparator::try_new(&[SortColumn {
            values: keys,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        }])?;
        let max_incoming = if let Some(ranges) = ranges {
            (0..width)
                .filter(|&i| ranges.is_valid(i))
                .map(|i| ranges.value_length(i) as usize)
                .max()
                .unwrap_or(0)
        } else {
            input.len() / width
        };
        let max_previous = (0..width)
            .map(|column| previous.value_length(column) as usize)
            .max()
            .unwrap_or(0);
        let capacity = max_previous
            .checked_add(max_incoming)
            .ok_or_else(|| error("ordered take candidate length overflow"))?;
        if self.candidates.capacity() < capacity {
            self.candidates.reserve(capacity - self.candidates.len());
        }
        self.selected.clear();
        let mut offsets = Vec::with_capacity(width + 1);
        offsets.push(0i32);
        for column in 0..width {
            self.candidates.clear();
            let old_offsets = previous.value_offsets();
            if previous.is_valid(column) {
                self.candidates
                    .extend(old_offsets[column] as usize..old_offsets[column + 1] as usize);
            }
            match ranges {
                Some(ranges) if ranges.is_valid(column) => {
                    let offsets = ranges.value_offsets();
                    for index in offsets[column] as usize..offsets[column + 1] as usize {
                        if incoming.is_null(index) {
                            return Err(error("ordered take packed records must be defined"));
                        }
                        self.candidates.push(
                            old_len
                                .checked_add(index)
                                .ok_or_else(|| error("ordered take candidate index overflow"))?,
                        );
                    }
                }
                Some(_) => {}
                None => {
                    for index in (column..input.len()).step_by(width) {
                        if filter.is_none_or(|f| f.is_valid(index) && f.value(index)) {
                            if incoming.is_null(index) {
                                return Err(error("ordered take packed records must be defined"));
                            }
                            self.candidates.push(
                                old_len.checked_add(index).ok_or_else(|| {
                                    error("ordered take candidate index overflow")
                                })?,
                            );
                        }
                    }
                }
            }
            let count = self.n.min(self.candidates.len());
            // Native Arrow's selection is unstable for equal ordering values,
            // as is the existing native ordered TAKE implementation.
            partial_sort(&mut self.candidates, count, |a, b| {
                comparator.compare(*a, *b)
            });
            let length = self
                .selected
                .len()
                .checked_add(count)
                .ok_or_else(|| error("ordered take output length overflow"))?;
            offsets.push(i32::try_from(length).map_err(|_| {
                error("coordinate ordered take exceeds Arrow List offset capacity")
            })?);
            self.selected.extend_from_slice(&self.candidates[..count]);
        }
        let old_data = old.to_data();
        let new_data = incoming.to_data();
        let mut output =
            MutableArrayData::new(vec![&old_data, &new_data], false, self.selected.len());
        for &index in &self.selected {
            if index < old_len {
                output.try_extend(0, index, index + 1)?;
            } else {
                let index = index - old_len;
                output.try_extend(1, index, index + 1)?;
            }
        }
        let values = make_array(output.freeze());
        self.state = Some(Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(self.input.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            None,
        )?));
        self.candidates.clear();
        self.selected.clear();
        Ok(())
    }
    pub(crate) fn states(&mut self, width: usize) -> Result<Vec<ArrayRef>> {
        self.ensure_width(width)?;
        Ok(vec![self.state.as_ref().unwrap().clone()])
    }
    pub(crate) fn array(&mut self, width: usize) -> Result<ArrayRef> {
        self.ensure_width(width)?;
        let state = self.state.as_ref().unwrap();
        let packed = state
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error("ordered take requires packed state"))?;
        let values = packed.column(0).clone();
        Ok(Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            state.offsets().clone(),
            values,
            None,
        )?))
    }
    pub(crate) fn size(&self) -> usize {
        self.state.as_ref().map_or(0, |s| s.get_array_memory_size())
            + (self.candidates.capacity() + self.selected.capacity()) * std::mem::size_of::<usize>()
    }
}
