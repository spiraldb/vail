// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reduce dictionary codes without materializing the expanded numeric child.
//!
//! The initial capability is signed i32 widened to i64. A group with at most
//! u32::MAX i32 values cannot overflow i64, so it needs neither per-value
//! overflow checks nor an intermediate widened array. Other widths retain the
//! existing canonical implementation and its overflow semantics.

use num_traits::AsPrimitive;
use vortex_buffer::{BitBuffer, BitBufferMut, BitBufferView};
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
        // Decode only the codes and dictionary, never the expanded child.
        let codes = dict.codes().slice(start..end)?;
        let code_mask = codes.validity()?.execute_mask(codes.len(), ctx)?;
        let value_mask = dict
            .values()
            .validity()?
            .execute_mask(dict.values().len(), ctx)?;
        if code_mask.all_false() || value_mask.all_false() {
            return Ok(Some(partial(
                PrimitiveArray::from_iter(std::iter::repeat_n(0_i64, groups.len())),
                BitBuffer::new_set(groups.len()),
                group_mask,
            )));
        }
        let codes = codes.execute::<PrimitiveArray>(ctx)?;
        let values = dict.values().clone().execute::<PrimitiveArray>(ctx)?;
        // Resolve Mask enum representations once, before entry traversal.
        // All-valid specializations never access their empty placeholder view.
        let empty_bits = BitBuffer::new_unset(0);
        let code_bits = code_mask.values().map_or(&empty_bits, |v| v.bit_buffer());
        let value_bits = value_mask.values().map_or(&empty_bits, |v| v.bit_buffer());
        let code_bits = BitBufferView::new_with_offset(code_bits.inner().as_slice(), code_bits.len(), code_bits.offset());
        let value_bits = BitBufferView::new_with_offset(value_bits.inner().as_slice(), value_bits.len(), value_bits.offset());
        // Small, reused dictionaries amortize a cardinality-sized normalization
        // and keep the value/validity pair in cache. Large dictionaries retain
        // the borrowed bitmap path rather than doubling their lookup footprint.
        const NORMALIZED_DICTIONARY_BYTES: usize = 16 * 1024;
        let normalized = (!value_mask.all_true()
            && values.len() <= NORMALIZED_DICTIONARY_BYTES / size_of::<LookupValue>()
            && code_mask.true_count() >= values.len() * 4)
            .then(|| values.as_slice::<i32>().iter().enumerate().map(|(index, &value)| {
                let valid = value_bits.value(index);
                LookupValue { value: if valid { value } else { 0 }, valid: i32::from(valid) }
            }).collect::<Vec<_>>());
        let (sums, empty) = match_each_integer_ptype!(codes.ptype(), |C| {
            let codes = codes.as_slice::<C>();
            let values = values.as_slice::<i32>();
            // Lookup and code-validity strategies are selected once per batch.
            if let Some(normalized) = &normalized {
                dispatch_codes(codes, &PreparedValues(normalized), &ranges, start, &group_mask, code_bits, code_mask.all_true(), code_mask.density() >= 0.9)
            } else if value_mask.all_true() {
                dispatch_codes(codes, &PlainValues::<true> { values, validity: value_bits }, &ranges, start, &group_mask, code_bits, code_mask.all_true(), code_mask.density() >= 0.9)
            } else {
                dispatch_codes(codes, &PlainValues::<false> { values, validity: value_bits }, &ranges, start, &group_mask, code_bits, code_mask.all_true(), code_mask.density() >= 0.9)
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

/// One cache-sized normalized dictionary slot, never one record per input code.
#[derive(Clone, Copy)]
#[repr(C)]
struct LookupValue { value: i32, valid: i32 }

trait ValueLookup { fn get(&self, index: usize) -> LookupValue; }
struct PreparedValues<'a>(&'a [LookupValue]);
impl ValueLookup for PreparedValues<'_> {
    #[inline]
    fn get(&self, index: usize) -> LookupValue { self.0[index] }
}
struct PlainValues<'a, const VALID: bool> { values: &'a [i32], validity: BitBufferView<'a> }
impl<const VALID: bool> ValueLookup for PlainValues<'_, VALID> {
    #[inline]
    fn get(&self, index: usize) -> LookupValue {
        // A checked lookup also proves the index lies in the equal-length mask.
        let value = self.values[index];
        // SAFETY: this mask was executed for exactly values.len() slots.
        let valid = VALID || unsafe { self.validity.value_unchecked(index) };
        LookupValue { value: if valid { value } else { 0 }, valid: i32::from(valid) }
    }
}

fn dispatch_codes<C: NativePType + AsPrimitive<usize>, L: ValueLookup>(
    codes: &[C], lookup: &L, ranges: &GroupRanges, base: usize, groups: &Mask,
    code_bits: BitBufferView<'_>, codes_valid: bool, dense_codes: bool,
) -> (PrimitiveArray, BitBuffer) {
    if codes_valid { reduce::<C, L, true, false>(codes, lookup, ranges, base, groups, code_bits) }
    else if dense_codes { reduce::<C, L, false, true>(codes, lookup, ranges, base, groups, code_bits) }
    else { reduce::<C, L, false, false>(codes, lookup, ranges, base, groups, code_bits) }
}

fn reduce<C: NativePType + AsPrimitive<usize>, L: ValueLookup, const CODES_VALID: bool, const CODE_RUNS: bool>(
    codes: &[C], lookup: &L, ranges: &GroupRanges, base: usize, groups: &Mask,
    code_bits: BitBufferView<'_>,
) -> (PrimitiveArray, BitBuffer) {
    debug_assert!(CODES_VALID || code_bits.len() == codes.len());
    let mut empty = BitBufferMut::new_unset(groups.len());
    let sums = ranges.iter().enumerate().map(|(group, (offset, len))| {
        if !groups.value(group) { return 0_i64; }
        let offset = if len == 0 { 0 } else { offset - base };
        let mut sum = 0_i64;
        let mut any = 0_i32;
        let codes = &codes[offset..offset + len];
        let mut add = |code: C| {
            let slot = lookup.get(code.as_());
            sum += i64::from(slot.value);
            any |= slot.valid;
        };
        if CODES_VALID {
            for &code in codes { add(code); }
        } else if CODE_RUNS {
            // Dense masks expose contiguous runs: keep bitmap work outside the
            // dictionary lookup loop and let the compiler optimize that loop.
            for (start, end) in code_bits.slice(offset..offset + len).set_slices() {
                // SAFETY: library run bounds lie in the bitmap slice, which has
                // exactly the same length as this already-checked code slice.
                for &code in unsafe { codes.get_unchecked(start..end) } { add(code); }
            }
        } else {
            // The library iterator visits validity words without allocating indices.
            for index in code_bits.slice(offset..offset + len).set_indices() {
                // SAFETY: the bitmap/code slices have identical len, and the
                // library iterator only yields positions inside that bitmap.
                add(unsafe { *codes.get_unchecked(index) });
            }
        }
        if any == 0 {
            // SAFETY: enumerate index is within the group-sized bitmap.
            unsafe { empty.set_unchecked(group) };
        }
        sum
    });
    (PrimitiveArray::from_iter(sums), empty.freeze())
}
