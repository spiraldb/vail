// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reduce dictionary codes without materializing the expanded numeric child.
//!
//! The initial capability is signed i32 widened to i64. A group with at most
//! u32::MAX i32 values cannot overflow i64, so it needs neither per-value
//! overflow checks nor an intermediate widened array. Other widths retain the
//! existing canonical implementation and its overflow semantics.

use num_traits::AsPrimitive;
use vortex_buffer::{BitBuffer, BitBufferMut};
use vortex_error::VortexResult;
use vortex_mask::Mask;

use super::{SumV2, sum_v2_partial_fields};
use crate::aggregate_fn::kernels::DynGroupedAggregateKernel;
use crate::aggregate_fn::{AggregateFnRef, GroupRanges, GroupedArray};
use crate::arrays::dict::DictArraySlotsExt;
use crate::arrays::{BoolArray, Dict, PrimitiveArray, StructArray};
use crate::dtype::{DType, NativePType, Nullability, PType};
use crate::match_each_integer_ptype;
use crate::validity::Validity;
use crate::{ArrayRef, ExecutionCtx, IntoArray};

#[derive(Debug)]
pub(crate) struct DictGroupedSumV2EncodingKernel;

impl DynGroupedAggregateKernel for DictGroupedSumV2EncodingKernel {
    fn grouped_aggregate(
        &self,
        aggregate: &AggregateFnRef,
        groups: &GroupedArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        if aggregate.as_opt::<SumV2>().is_none()
            || !matches!(groups.elements().dtype(), DType::Primitive(PType::I32, _))
        {
            return Ok(None);
        }
        let Some(dict) = groups.elements().as_opt::<Dict>() else {
            return Ok(None);
        };
        let ranges = groups.group_ranges(ctx)?;
        if ranges.iter().any(|(_, len)| len > u32::MAX as usize) {
            return Ok(None);
        }
        let group_mask = groups.group_validity(ctx)?;
        // ListView slices can retain a much larger child owner. Decode only
        // the contiguous envelope used by visible valid groups, then rebase.
        let mut start = usize::MAX;
        let mut end = 0;
        for (group, (offset, len)) in ranges.iter().enumerate() {
            if group_mask.value(group) && len != 0 {
                start = start.min(offset);
                end = end.max(offset + len);
            }
        }
        if start == usize::MAX {
            return Ok(Some(partial(
                PrimitiveArray::from_iter(std::iter::repeat_n(0_i64, groups.len())),
                BitBuffer::new_set(groups.len()),
                group_mask,
            )));
        }
        // A tiny visible slice must not force decoding an arbitrarily large
        // retained dictionary. Let the existing representation-aware fallback
        // handle dictionaries with more distinct slots than visible codes.
        if dict.values().len() > end - start {
            return Ok(None);
        }
        let ranges = GroupRanges::ListView {
            ranges: ranges
                .iter()
                .enumerate()
                .map(|(group, (offset, len))| {
                    if group_mask.value(group) && len != 0 {
                        (offset - start, len)
                    } else {
                        (0, 0)
                    }
                })
                .collect(),
        };
        // Decode only the codes and dictionary, never the expanded child.
        let codes = dict
            .codes()
            .slice(start..end)?
            .execute::<PrimitiveArray>(ctx)?;
        let values = dict.values().clone().execute::<PrimitiveArray>(ctx)?;
        let code_mask = codes.as_ref().validity()?.execute_mask(codes.len(), ctx)?;
        let value_mask = values
            .as_ref()
            .validity()?
            .execute_mask(values.len(), ctx)?;
        let (sums, empty) = match_each_integer_ptype!(codes.ptype(), |C| {
            let codes = codes.as_slice::<C>();
            let values = values.as_slice::<i32>();
            // Nullability strategy is selected once for this physical batch.
            match (code_mask.all_true(), value_mask.all_true()) {
                (true, true) => reduce::<C, true, true>(
                    codes,
                    values,
                    &ranges,
                    &group_mask,
                    &code_mask,
                    &value_mask,
                ),
                (true, false) => reduce::<C, true, false>(
                    codes,
                    values,
                    &ranges,
                    &group_mask,
                    &code_mask,
                    &value_mask,
                ),
                (false, true) => reduce::<C, false, true>(
                    codes,
                    values,
                    &ranges,
                    &group_mask,
                    &code_mask,
                    &value_mask,
                ),
                (false, false) => reduce::<C, false, false>(
                    codes,
                    values,
                    &ranges,
                    &group_mask,
                    &code_mask,
                    &value_mask,
                ),
            }
        });
        Ok(Some(partial(sums, empty, group_mask)))
    }
}

fn partial(sums: PrimitiveArray, empty: BitBuffer, group_mask: Mask) -> ArrayRef {
    let len = group_mask.len();
    let fields = sum_v2_partial_fields(sums.dtype().clone());
    // SAFETY: all children contain exactly one partial state per group.
    unsafe {
        StructArray::new_unchecked(
            vec![
                sums.into_array(),
                BoolArray::new(BitBuffer::new_unset(len), Validity::NonNullable).into_array(),
                BoolArray::new(empty, Validity::NonNullable).into_array(),
            ],
            fields,
            len,
            Validity::from_mask(group_mask, Nullability::Nullable),
        )
    }
    .into_array()
}

fn reduce<
    C: NativePType + AsPrimitive<usize>,
    const CODES_VALID: bool,
    const VALUES_VALID: bool,
>(
    codes: &[C],
    values: &[i32],
    ranges: &GroupRanges,
    groups: &Mask,
    code_mask: &Mask,
    value_mask: &Mask,
) -> (PrimitiveArray, BitBuffer) {
    let mut empty = BitBufferMut::new_unset(groups.len());
    let sums = ranges.iter().enumerate().map(|(group, (offset, len))| {
        if !groups.value(group) {
            return 0_i64;
        }
        let mut sum = 0_i64;
        let mut any = false;
        for (relative, code) in codes[offset..offset + len].iter().enumerate() {
            if !CODES_VALID && !code_mask.value(offset + relative) {
                continue;
            }
            let index = (*code).as_();
            if !VALUES_VALID && !value_mask.value(index) {
                continue;
            }
            sum += i64::from(values[index]);
            any = true;
        }
        if !any {
            // SAFETY: enumerate index is within the group-sized bitmap.
            unsafe { empty.set_unchecked(group) };
        }
        sum
    });
    (PrimitiveArray::from_iter(sums), empty.freeze())
}
