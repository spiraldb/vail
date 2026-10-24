// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use arrow_buffer::ArrowNativeType;
use vortex_buffer::Buffer;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_mask::Mask;

use crate::ArrayRef;
use crate::Canonical;
use crate::Columnar;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::aggregate_fn::Accumulator;
use crate::aggregate_fn::AggregateFn;
use crate::aggregate_fn::AggregateFnRef;
use crate::aggregate_fn::AggregateFnVTable;
use crate::aggregate_fn::DynAccumulator;
use crate::aggregate_fn::session::AggregateFnSessionExt;
use crate::arrays::ChunkedArray;
use crate::arrays::FixedSizeListArray;
use crate::arrays::ListViewArray;
use crate::arrays::fixed_size_list::FixedSizeListArrayExt;
use crate::arrays::fixed_size_list::FixedSizeListArraySlotsExt;
use crate::arrays::listview::ListViewArraySlotsExt;
use crate::builders::builder_with_capacity_in;
use crate::builtins::ArrayBuiltins;
use crate::columnar::AnyColumnar;
use crate::dtype::DType;
use crate::executor::max_iterations;
use crate::match_each_integer_ptype;

/// Reference-counted type-erased grouped accumulator.
pub type GroupedAccumulatorRef = Box<dyn DynGroupedAccumulator>;

/// A batch of grouped values to aggregate.
///
/// Each outer list value is one group, and the inner element array is shared by all groups.
/// Aggregate implementations can inspect the concrete grouped representation directly, or ask for
/// derived ranges when their algorithm is expressed in terms of `(offset, size)` pairs.
pub enum GroupedArray {
    /// Groups represented as a list-view array with per-group offsets and sizes.
    ListView(ListViewArray),
    /// Groups represented as a fixed-size list array.
    FixedSizeList(FixedSizeListArray),
}

impl From<ListViewArray> for GroupedArray {
    fn from(groups: ListViewArray) -> Self {
        Self::ListView(groups)
    }
}

impl From<FixedSizeListArray> for GroupedArray {
    fn from(groups: FixedSizeListArray) -> Self {
        Self::FixedSizeList(groups)
    }
}

impl GroupedArray {
    /// The inner element array shared by all groups.
    pub fn elements(&self) -> &ArrayRef {
        match self {
            Self::ListView(groups) => groups.elements(),
            Self::FixedSizeList(groups) => groups.elements(),
        }
    }

    /// Return the `(offset, size)` ranges describing each group in `elements`.
    pub fn group_ranges(&self, ctx: &mut ExecutionCtx) -> VortexResult<GroupRanges> {
        match self {
            Self::ListView(groups) => list_view_group_ranges(groups, ctx),
            Self::FixedSizeList(groups) => Ok(fixed_size_list_group_ranges(groups)),
        }
    }

    /// Return the per-group validity mask.
    pub fn group_validity(&self, ctx: &mut ExecutionCtx) -> VortexResult<Mask> {
        match self {
            Self::ListView(groups) => groups.validity()?.execute_mask(groups.len(), ctx),
            Self::FixedSizeList(groups) => groups.validity()?.execute_mask(groups.len(), ctx),
        }
    }

    /// The number of groups in this batch.
    pub fn len(&self) -> usize {
        match self {
            Self::ListView(groups) => groups.len(),
            Self::FixedSizeList(groups) => groups.len(),
        }
    }

    /// Returns true when this batch contains no groups.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true when every group is valid.
    pub fn all_groups_valid(&self, ctx: &mut ExecutionCtx) -> VortexResult<bool> {
        Ok(self.group_validity(ctx)?.all_true())
    }

    unsafe fn with_elements_unchecked(&self, elements: ArrayRef) -> VortexResult<Self> {
        Ok(match self {
            Self::ListView(groups) => unsafe {
                ListViewArray::new_unchecked(
                    elements,
                    groups.offsets().clone(),
                    groups.sizes().clone(),
                    groups.validity()?,
                )
            }
            .into(),
            Self::FixedSizeList(groups) => unsafe {
                FixedSizeListArray::new_unchecked(
                    elements,
                    groups.list_size(),
                    groups.validity()?,
                    groups.len(),
                )
            }
            .into(),
        })
    }
}

/// The physical ranges of a grouped array.
pub enum GroupRanges {
    /// Explicit ranges extracted from a list-view array.
    ListView {
        /// The `(offset, size)` ranges.
        ranges: Vec<(usize, usize)>,
    },
    /// Uniform ranges derived from a fixed-size list array.
    FixedSizeList {
        /// The number of groups.
        len: usize,
        /// The number of elements in each group.
        size: usize,
    },
}

impl GroupRanges {
    /// The number of groups described by these ranges.
    pub fn len(&self) -> usize {
        match self {
            Self::ListView { ranges } => ranges.len(),
            Self::FixedSizeList { len, .. } => *len,
        }
    }

    /// Returns true when there are no groups.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the `(offset, size)` range for the group at `index`.
    fn range(&self, index: usize) -> (usize, usize) {
        match self {
            Self::ListView { ranges } => ranges[index],
            Self::FixedSizeList { len, size } => {
                assert!(index < *len, "range index out of bounds");
                (index * size, *size)
            }
        }
    }

    /// Iterate over all `(offset, size)` group ranges.
    pub fn iter(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        (0..self.len()).map(|index| self.range(index))
    }
}

/// An accumulator used for computing grouped aggregates.
///
/// Note that the groups must be processed in order, and the accumulator does not support random
/// access to groups.
pub struct GroupedAccumulator<V: AggregateFnVTable> {
    /// The vtable of the aggregate function.
    vtable: V,
    /// The options of the aggregate function.
    options: V::Options,
    /// Type-erased aggregate function used for kernel dispatch.
    aggregate_fn: AggregateFnRef,
    /// The DType of the input.
    dtype: DType,
    /// The DType of the aggregate.
    return_dtype: DType,
    /// The DType of the partial accumulator state.
    partial_dtype: DType,
    /// The accumulated state for prior batches of groups.
    partials: Vec<ArrayRef>,
}

impl<V: AggregateFnVTable> GroupedAccumulator<V> {
    pub fn try_new(vtable: V, options: V::Options, dtype: DType) -> VortexResult<Self> {
        let aggregate_fn = AggregateFn::new(vtable.clone(), options.clone()).erased();
        let return_dtype = vtable.return_dtype(&options, &dtype).ok_or_else(|| {
            vortex_err!(
                "Aggregate function {} cannot be applied to dtype {}",
                vtable.id(),
                dtype
            )
        })?;
        let partial_dtype = vtable.partial_dtype(&options, &dtype).ok_or_else(|| {
            vortex_err!(
                "Aggregate function {} cannot be applied to dtype {}",
                vtable.id(),
                dtype
            )
        })?;

        Ok(Self {
            vtable,
            options,
            aggregate_fn,
            dtype,
            return_dtype,
            partial_dtype,
            partials: vec![],
        })
    }
}

/// A trait object for type-erased grouped accumulators, used for dynamic dispatch when the aggregate
/// function is not known at compile time.
pub trait DynGroupedAccumulator: 'static + Send {
    /// Accumulate a list of groups into the accumulator.
    fn accumulate_list(&mut self, groups: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<()>;

    /// Finish the accumulation and return the partial aggregate results for all groups.
    /// Resets the accumulator state for the next round of accumulation.
    fn flush(&mut self) -> VortexResult<ArrayRef>;

    /// Finish the accumulation and return the final aggregate results for all groups.
    /// Resets the accumulator state for the next round of accumulation.
    fn finish(&mut self) -> VortexResult<ArrayRef>;
}

impl<V: AggregateFnVTable> DynGroupedAccumulator for GroupedAccumulator<V> {
    fn accumulate_list(&mut self, groups: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<()> {
        let elements_dtype = match groups.dtype() {
            DType::List(elem, _) => elem,
            DType::FixedSizeList(elem, ..) => elem,
            _ => vortex_bail!(
                "Input DType mismatch: expected List or FixedSizeList, got {}",
                groups.dtype()
            ),
        };
        vortex_ensure!(
            elements_dtype.as_ref() == &self.dtype,
            "Input DType mismatch: expected {}, got {}",
            self.dtype,
            elements_dtype
        );

        // We first execute the groups until it is a ListView or FixedSizeList, since we only
        // dispatch the aggregate kernel over the elements of these arrays.
        let canonical = match groups.clone().execute::<Columnar>(ctx)? {
            Columnar::Canonical(c) => c,
            Columnar::Constant(c) => c.into_array().execute::<Canonical>(ctx)?,
        };
        match canonical {
            Canonical::List(groups) => self.accumulate_grouped_array(groups.into(), ctx),
            Canonical::FixedSizeList(groups) => self.accumulate_grouped_array(groups.into(), ctx),
            _ => vortex_panic!("We checked the DType above, so this should never happen"),
        }
    }

    fn flush(&mut self) -> VortexResult<ArrayRef> {
        let mut states = std::mem::take(&mut self.partials);
        if states.len() == 1 {
            return Ok(states.pop().vortex_expect("checked one partial"));
        }
        Ok(ChunkedArray::try_new(states, self.partial_dtype.clone())?.into_array())
    }

    fn finish(&mut self) -> VortexResult<ArrayRef> {
        let states = self.flush()?;
        let results = self.vtable.finalize(states)?;

        vortex_ensure!(
            results.dtype() == &self.return_dtype,
            "Return DType mismatch: expected {}, got {}",
            self.return_dtype,
            results.dtype()
        );

        Ok(results)
    }
}

impl<V: AggregateFnVTable> GroupedAccumulator<V> {
    fn accumulate_grouped_array(
        &mut self,
        groups: GroupedArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        let mut elements = groups.elements().clone();
        let session = ctx.session().clone();

        for _ in 0..max_iterations() {
            // Try a registered grouped kernel for the current element encoding.
            if let Some(kernel) = session
                .aggregate_fns()
                .find_grouped_encoding_kernel(elements.encoding_id(), self.aggregate_fn.id())
            {
                // SAFETY: we assume that elements execution is safe
                let kernel_groups = unsafe { groups.with_elements_unchecked(elements.clone())? };
                if let Some(result) =
                    kernel.grouped_aggregate(&self.aggregate_fn, &kernel_groups, ctx)?
                {
                    return self.push_result(result);
                }
            }

            // Try a grouped kernel for the current aggregate regardless of element encoding.
            if let Some(kernel) = session
                .aggregate_fns()
                .find_grouped_kernel(self.aggregate_fn.id())
            {
                // SAFETY: we preserve the grouped shape and validity while replacing the
                // elements with another representation of the same logical array.
                let kernel_groups = unsafe { groups.with_elements_unchecked(elements.clone())? };
                if let Some(result) =
                    kernel.grouped_aggregate(&self.aggregate_fn, &kernel_groups, ctx)?
                {
                    return self.push_result(result);
                }
            }

            if elements.is::<AnyColumnar>() {
                break;
            }

            // Execute one step and try again
            elements = elements.execute(ctx)?;
        }

        let elements = elements.execute::<Columnar>(ctx)?.into_array();
        // SAFETY: we preserve the grouped shape and validity while replacing the elements with an
        // executed form of the same logical array.
        let grouped = unsafe { groups.with_elements_unchecked(elements)? };

        // Otherwise, we iterate the offsets and sizes and accumulate each group one by one.
        self.accumulate_grouped_fallback(&grouped, ctx)
    }

    fn accumulate_grouped_fallback(
        &mut self,
        grouped: &GroupedArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        let mut accumulator = Accumulator::try_new(
            self.vtable.clone(),
            self.options.clone(),
            self.dtype.clone(),
        )?;
        let mut states =
            builder_with_capacity_in(&self.partial_dtype, grouped.len(), ctx.allocator());
        let group_ranges = grouped.group_ranges(ctx)?;
        let group_validity = grouped.group_validity(ctx)?;

        for ((offset, size), valid) in group_ranges.iter().zip(group_validity.iter()) {
            if valid {
                let group = grouped.elements().slice(offset..offset + size)?;
                accumulator.accumulate(&group, ctx)?;
                states.append_scalar(&accumulator.flush()?)?;
            } else {
                states.append_null()
            }
        }

        self.push_result(states.finish())
    }

    fn push_result(&mut self, state: ArrayRef) -> VortexResult<()> {
        vortex_ensure!(
            state.dtype() == &self.partial_dtype,
            "State DType mismatch: expected {}, got {}",
            self.partial_dtype,
            state.dtype()
        );
        self.partials.push(state);
        Ok(())
    }
}
fn list_view_group_ranges(
    groups: &ListViewArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<GroupRanges> {
    let offsets = groups.offsets();
    let sizes = groups.sizes().cast(offsets.dtype().clone())?;

    let ranges = match_each_integer_ptype!(offsets.dtype().as_ptype(), |O| {
        let offsets = offsets.clone().execute::<Buffer<O>>(ctx)?;
        let sizes = sizes.execute::<Buffer<O>>(ctx)?;
        offsets
            .as_ref()
            .iter()
            .zip(sizes.as_ref().iter())
            .map(|(offset, size)| {
                (
                    offset.to_usize().vortex_expect("Offset value is not usize"),
                    size.to_usize().vortex_expect("Size value is not usize"),
                )
            })
            .collect::<Vec<_>>()
    });

    Ok(GroupRanges::ListView { ranges })
}

fn fixed_size_list_group_ranges(groups: &FixedSizeListArray) -> GroupRanges {
    GroupRanges::FixedSizeList {
        len: groups.len(),
        size: groups.list_size() as usize,
    }
}

/// Type-erased list accumulator whose schema includes declared outer nullability.
pub type ListGroupedAccumulatorRef = Box<dyn DynListGroupedAccumulator>;

/// An accumulator prepared from a complete List or FixedSizeList dtype.
///
/// Scalar aggregate states contain only real, present groups. Missing outer groups
/// are carried separately as nullable restoration indices, never null scalar states.
/// This is a correctness contract; it does not guarantee selective child decoding.
pub struct ListGroupedAccumulator<V: AggregateFnVTable> {
    present: GroupedAccumulator<V>,
    list_dtype: DType,
    result_dtype: DType,
    mappings: Vec<GroupRestore>,
    present_count: usize,
    group_count: usize,
}

/// A batch mapping. The all-valid path borrows a contiguous result span without
/// allocating expanded rank indices, even for a declared-nullable list dtype.
enum GroupRestore {
    Contiguous(usize),
    Compact {
        indices: ArrayRef,
        present_count: usize,
    },
}

impl GroupRestore {
    fn present_count(&self) -> usize {
        match self {
            Self::Contiguous(len) => *len,
            Self::Compact { present_count, .. } => *present_count,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Contiguous(len) => *len,
            Self::Compact { indices, .. } => indices.len(),
        }
    }
}

/// Opaque paired scalar states and group mapping. Only the validated accumulator
/// can create it; flushing moves states and mappings together and resets both.
pub struct ListGroupPartials {
    states: ArrayRef,
    mappings: Vec<GroupRestore>,
    present_count: usize,
    group_count: usize,
    scalar_result_dtype: DType,
    result_dtype: DType,
    finalize: Box<dyn FnOnce(ArrayRef) -> VortexResult<ArrayRef> + Send>,
}

impl ListGroupPartials {
    /// Finalize actual present groups, then restore missing groups using Vortex's
    /// nullable-index Take. Null scalar results remain null independently.
    pub fn finish(self) -> VortexResult<ArrayRef> {
        let results = (self.finalize)(self.states)?;
        vortex_ensure!(
            results.len() == self.present_count,
            "Grouped finalizer changed cardinality"
        );
        vortex_ensure!(
            results.dtype() == &self.scalar_result_dtype,
            "Grouped finalizer changed scalar dtype"
        );
        let mut offset = 0usize;
        let mut restored = Vec::with_capacity(self.mappings.len());
        let mut output_len = 0usize;
        for mapping in self.mappings {
            let end = offset
                .checked_add(mapping.present_count())
                .ok_or_else(|| vortex_err!("Grouped state offset overflow"))?;
            let present = results.slice(offset..end)?;
            let part = match &mapping {
                GroupRestore::Contiguous(_) => present.cast(self.result_dtype.clone())?,
                GroupRestore::Compact { indices, .. } => present.take(indices.clone())?,
            };
            vortex_ensure!(
                part.dtype() == &self.result_dtype,
                "Grouped restoration dtype mismatch"
            );
            vortex_ensure!(
                part.len() == mapping.len(),
                "Grouped restoration changed cardinality"
            );
            output_len = output_len
                .checked_add(part.len())
                .ok_or_else(|| vortex_err!("Grouped output length overflow"))?;
            restored.push(part);
            offset = end;
        }
        vortex_ensure!(
            offset == self.present_count && output_len == self.group_count,
            "Grouped restoration did not consume paired state"
        );
        if restored.len() == 1 {
            return Ok(restored.pop().vortex_expect("checked one result"));
        }
        Ok(ChunkedArray::try_new(restored, self.result_dtype)?.into_array())
    }
}

/// Object-safe complete-list aggregation interface. Its flush result preserves
/// declared schema, scalar states, and missing-group correspondence together.
pub trait DynListGroupedAccumulator: Send {
    /// Accumulate groups with exactly the prepared declared list dtype.
    fn accumulate_list(&mut self, groups: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<()>;
    /// Move paired states and mappings out, resetting both for reuse.
    fn flush_groups(&mut self) -> VortexResult<ListGroupPartials>;
    /// Finalize paired groups, preserving scalar nulls and missing outer lists.
    fn finish(&mut self) -> VortexResult<ArrayRef> {
        self.flush_groups()?.finish()
    }
}

impl<V: AggregateFnVTable> ListGroupedAccumulator<V> {
    /// Prepare once from the full declared input type. Result nullability is
    /// scalar-result nullability union declared outer-list nullability.
    pub fn try_new(vtable: V, options: V::Options, list_dtype: DType) -> VortexResult<Self> {
        let element = match &list_dtype {
            DType::List(element, _) | DType::FixedSizeList(element, ..) => element.as_ref().clone(),
            _ => vortex_bail!("Expected complete List or FixedSizeList dtype, got {list_dtype}"),
        };
        let present = GroupedAccumulator::try_new(vtable, options, element)?;
        let result_dtype = present
            .return_dtype
            .with_nullability(present.return_dtype.nullability() | list_dtype.nullability());
        Ok(Self {
            present,
            list_dtype,
            result_dtype,
            mappings: Vec::new(),
            present_count: 0,
            group_count: 0,
        })
    }
}

impl<V: AggregateFnVTable> DynListGroupedAccumulator for ListGroupedAccumulator<V> {
    fn accumulate_list(&mut self, input: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<()> {
        vortex_ensure!(
            input.dtype() == &self.list_dtype,
            "Declared grouped input dtype mismatch: expected {}, got {}",
            self.list_dtype,
            input.dtype()
        );
        let columnar = input.clone().execute::<Columnar>(ctx)?;
        let canonical = match columnar {
            Columnar::Canonical(c) => c,
            Columnar::Constant(c) => c.into_array().execute::<Canonical>(ctx)?,
        };
        let groups: GroupedArray = match canonical {
            Canonical::List(groups) => groups.into(),
            Canonical::FixedSizeList(groups) => groups.into(),
            _ => vortex_bail!("Canonical grouped input is not a list"),
        };
        let len = groups.len();
        let validity = groups.group_validity(ctx)?;
        let present_count = validity.true_count();
        let new_present_count = self
            .present_count
            .checked_add(present_count)
            .ok_or_else(|| vortex_err!("Grouped present count overflow"))?;
        let new_group_count = self
            .group_count
            .checked_add(len)
            .ok_or_else(|| vortex_err!("Grouped output count overflow"))?;
        let mapping = if validity.all_true() {
            // Preserve the direct grouped-kernel path without rank expansion.
            self.present.accumulate_grouped_array(groups, ctx)?;
            GroupRestore::Contiguous(len)
        } else {
            let ranges = groups.group_ranges(ctx)?;
            let mut offsets = Vec::with_capacity(present_count);
            let mut sizes = Vec::with_capacity(present_count);
            let mut ranks = Vec::with_capacity(len);
            let mut rank = 0u64;
            for ((offset, size), valid) in ranges.iter().zip(validity.iter()) {
                if valid {
                    offsets.push(
                        u64::try_from(offset)
                            .map_err(|_| vortex_err!("Group offset does not fit u64"))?,
                    );
                    sizes.push(
                        u64::try_from(size)
                            .map_err(|_| vortex_err!("Group size does not fit u64"))?,
                    );
                    ranks.push(Some(rank));
                    rank = rank
                        .checked_add(1)
                        .ok_or_else(|| vortex_err!("Group rank overflow"))?;
                } else {
                    ranks.push(None);
                }
            }
            // This construction establishes a dense one-to-one mapping in original
            // group order, not merely a maximum-rank bound.
            vortex_ensure!(
                usize::try_from(rank).ok() == Some(present_count),
                "Group rank count mismatch"
            );
            if present_count != 0 {
                let compact = ListViewArray::try_new(
                    groups.elements().clone(),
                    crate::arrays::PrimitiveArray::from_iter(offsets).into_array(),
                    crate::arrays::PrimitiveArray::from_iter(sizes).into_array(),
                    crate::validity::Validity::NonNullable,
                )?;
                self.present.accumulate_grouped_array(compact.into(), ctx)?;
            }
            GroupRestore::Compact {
                indices: crate::arrays::PrimitiveArray::from_option_iter(ranks).into_array(),
                present_count,
            }
        };
        self.mappings.push(mapping);
        self.present_count = new_present_count;
        self.group_count = new_group_count;
        Ok(())
    }

    fn flush_groups(&mut self) -> VortexResult<ListGroupPartials> {
        let states = self.present.flush()?;
        vortex_ensure!(
            states.dtype() == &self.present.partial_dtype,
            "Grouped state dtype mismatch"
        );
        vortex_ensure!(
            states.len() == self.present_count,
            "Grouped state count mismatch"
        );
        let mapped_present = self
            .mappings
            .iter()
            .try_fold(0usize, |n, m| n.checked_add(m.present_count()))
            .ok_or_else(|| vortex_err!("Grouped mapping count overflow"))?;
        let mapped_groups = self
            .mappings
            .iter()
            .try_fold(0usize, |n, m| n.checked_add(m.len()))
            .ok_or_else(|| vortex_err!("Grouped mapping length overflow"))?;
        vortex_ensure!(
            mapped_present == self.present_count && mapped_groups == self.group_count,
            "Grouped states and mappings are not paired"
        );
        let vtable = self.present.vtable.clone();
        Ok(ListGroupPartials {
            states,
            mappings: std::mem::take(&mut self.mappings),
            present_count: std::mem::take(&mut self.present_count),
            group_count: std::mem::take(&mut self.group_count),
            scalar_result_dtype: self.present.return_dtype.clone(),
            result_dtype: self.result_dtype.clone(),
            finalize: Box::new(move |states| vtable.finalize(states)),
        })
    }
}

#[cfg(test)]
mod tests {
    use vortex_error::VortexResult;

    use crate::ArrayRef;
    use crate::IntoArray;
    use crate::aggregate_fn::DynGroupedAccumulator;
    use crate::aggregate_fn::GroupedAccumulator;
    use crate::aggregate_fn::NumericalAggregateOpts;
    use crate::aggregate_fn::fns::count::Count;
    use crate::arrays::Chunked;
    use crate::arrays::PrimitiveArray;
    use crate::dtype::DType;
    use crate::dtype::Nullability::NonNullable;
    use crate::dtype::PType;

    fn accumulator() -> VortexResult<GroupedAccumulator<Count>> {
        GroupedAccumulator::try_new(
            Count,
            NumericalAggregateOpts::default(),
            DType::Primitive(PType::I32, NonNullable),
        )
    }

    fn state(values: impl IntoIterator<Item = u64>) -> ArrayRef {
        PrimitiveArray::from_iter(values).into_array()
    }

    #[test]
    fn test_flush_single_partial_returns_original_array() -> VortexResult<()> {
        let mut accumulator = accumulator()?;
        let state = state([1, 2, 3]);
        accumulator.push_result(state.clone())?;

        let flushed = accumulator.flush()?;

        assert!(ArrayRef::ptr_eq(&flushed, &state));
        Ok(())
    }

    #[test]
    fn test_flush_multiple_partials_returns_chunked_array() -> VortexResult<()> {
        let mut accumulator = accumulator()?;
        accumulator.push_result(state([1, 2]))?;
        accumulator.push_result(state([3, 4]))?;

        let flushed = accumulator.flush()?;

        assert!(flushed.is::<Chunked>());
        assert_eq!(flushed.len(), 4);
        Ok(())
    }

    #[test]
    fn test_flush_without_partials_returns_empty_chunked_array() -> VortexResult<()> {
        let mut accumulator = accumulator()?;

        let flushed = accumulator.flush()?;

        assert!(flushed.is::<Chunked>());
        assert!(flushed.is_empty());
        Ok(())
    }
}
