//! Reader-local LEB128 Int32 values. Numeric expansion belongs to consumers.
use arrow_array::{
    Array as ArrowArray, ArrayRef as ArrowArrayRef, Int32Array, UInt8Array, UInt64Array,
};
use arrow_buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field};
use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    ops::Range,
    sync::{Arc, OnceLock},
};
use vortex::{
    array::{
        Array, ArrayEq, ArrayHash, ArrayId, ArrayParts, ArrayRef, ArrayView, EqMode, ExecutionCtx,
        ExecutionResult, IntoArray, OperationsVTable, VTable, ValidityVTable,
        arrays::{Chunked, Slice, chunked::ChunkedArrayExt},
        buffer::BufferHandle,
        serde::ArrayChildren,
        validity::Validity,
        with_empty_buffers,
    },
    arrow::ArrowSessionExt,
    dtype::{DType, Nullability, PType},
    error::{VortexResult, vortex_ensure, vortex_err},
    scalar::Scalar,
    session::{VortexSession, registry::CachedId},
};
const STRIDE: usize = 128;

/// Validate framing once and retain one sparse byte-offset checkpoint per 128 values.
/// Payload width is intentionally not validated; expansion keeps its low 32 bits.
pub fn array(
    bytes: Vec<u8>,
    count: usize,
    field: &Field,
    session: &VortexSession,
) -> VortexResult<ArrayRef> {
    vortex_ensure!(
        field.data_type() == &DataType::Int32,
        "LEB array requires Int32 field"
    );
    vortex_ensure!(count <= bytes.len(), "LEB logical count exceeds byte count");
    let mut checkpoints = Vec::with_capacity(count.div_ceil(STRIDE));
    let mut values = 0usize;
    let mut beginning = 0usize;
    for (offset, byte) in bytes.iter().enumerate() {
        if byte & 128 == 0 {
            vortex_ensure!(values < count, "LEB array has trailing encoded values");
            if values % STRIDE == 0 {
                checkpoints.push(beginning as u64);
            }
            values += 1;
            beginning = offset + 1;
        }
    }
    vortex_ensure!(
        values == count && beginning == bytes.len(),
        "truncated LEB array framing"
    );
    let raw = session
        .arrow()
        .from_arrow_array(Arc::new(UInt8Array::from(bytes)), false)?;
    let checkpoints = session
        .arrow()
        .from_arrow_array(Arc::new(UInt64Array::from(checkpoints)), false)?;
    from_parts(
        session.arrow().from_arrow_field(field)?,
        Data {
            start: 0,
            total: count,
        },
        count,
        vec![Some(raw), Some(checkpoints)],
    )
}

/// Ordinary Arrow scans materialize the same wire values directly.
pub fn to_arrow(bytes: &[u8], count: usize) -> VortexResult<ArrowArrayRef> {
    vortex_ensure!(count <= bytes.len(), "LEB logical count exceeds byte count");
    let mut position = 0;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(
            decode_one(bytes, &mut position).ok_or_else(|| vortex_err!("truncated LEB value"))?,
        );
    }
    vortex_ensure!(position == bytes.len(), "LEB array has trailing bytes");
    Ok(Arc::new(Int32Array::from(values)))
}

#[derive(Clone, Debug)]
struct Data {
    start: usize,
    total: usize,
}
impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "start={}, total={}", self.start, self.total)
    }
}
impl ArrayHash for Data {
    fn array_hash<H: Hasher>(&self, state: &mut H, _: EqMode) {
        self.start.hash(state);
        self.total.hash(state);
    }
}
impl ArrayEq for Data {
    fn array_eq(&self, other: &Self, _: EqMode) -> bool {
        self.start == other.start && self.total == other.total
    }
}
fn from_parts(
    dtype: DType,
    data: Data,
    len: usize,
    slots: Vec<Option<ArrayRef>>,
) -> VortexResult<ArrayRef> {
    Ok(Array::try_from_parts(
        ArrayParts::new(EncodedI32, dtype, len, data).with_slots(slots.into()),
    )?
    .into_array())
}
#[derive(Clone, Debug)]
struct EncodedI32;
impl VTable for EncodedI32 {
    type TypedArrayData = Data;
    type OperationsVTable = Self;
    type ValidityVTable = Self;
    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vail.legacy-leb-int32");
        *ID
    }
    fn validate(
        &self,
        data: &Data,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        vortex_ensure!(
            matches!(dtype, DType::Primitive(PType::I32, _)),
            "LEB array requires Int32 dtype"
        );
        vortex_ensure!(
            data.start <= data.total && len <= data.total - data.start,
            "LEB logical view is out of bounds"
        );
        vortex_ensure!(
            slots.len() == 2,
            "LEB array requires wire and checkpoint children"
        );
        let raw = slots[0]
            .as_ref()
            .ok_or_else(|| vortex_err!("missing LEB wire bytes"))?;
        let checkpoints = slots[1]
            .as_ref()
            .ok_or_else(|| vortex_err!("missing LEB checkpoints"))?;
        vortex_ensure!(
            raw.dtype() == &DType::Primitive(PType::U8, Nullability::NonNullable),
            "LEB wire child requires nonnull UInt8"
        );
        vortex_ensure!(
            checkpoints.dtype() == &DType::Primitive(PType::U64, Nullability::NonNullable)
                && checkpoints.len() == data.total.div_ceil(STRIDE),
            "LEB checkpoint shape differs"
        );
        Ok(())
    }
    fn nbuffers(_: ArrayView<'_, Self>) -> usize {
        0
    }
    fn buffer(_: ArrayView<'_, Self>, _: usize) -> BufferHandle {
        unreachable!("LEB buffers belong to children")
    }
    fn buffer_name(_: ArrayView<'_, Self>, _: usize) -> Option<String> {
        None
    }
    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        with_empty_buffers(self, array, buffers)
    }
    fn serialize(_: ArrayView<'_, Self>, _: &VortexSession) -> VortexResult<Option<Vec<u8>>> {
        Err(vortex_err!(
            "expand reader-local LEB values before persistence"
        ))
    }
    fn deserialize(
        &self,
        _: &DType,
        _: usize,
        _: &[u8],
        _: &[BufferHandle],
        _: &dyn ArrayChildren,
        _: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        Err(vortex_err!("LEB Int32 arrays are reader-local"))
    }
    fn slot_name(_: ArrayView<'_, Self>, index: usize) -> String {
        ["wire bytes", "value checkpoints"][index].into()
    }
    fn execute(array: Array<Self>, ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        let view = wire_view(array.as_ref(), ctx)?;
        let expanded = LazyArrowI32::new(vec![view]);
        let canonical = ctx.session().clone().arrow().from_arrow_array(
            Arc::new(expanded.expanded().clone()),
            array.dtype().is_nullable(),
        )?;
        Ok(ExecutionResult::done(canonical))
    }
    fn reduce_parent(
        array: ArrayView<'_, Self>,
        parent: &ArrayRef,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        if child_idx == 0 {
            if let Some(slice) = parent.as_opt::<Slice>() {
                let range = slice.data().slice_range();
                return from_parts(
                    parent.dtype().clone(),
                    Data {
                        start: array.data().start + range.start,
                        total: array.data().total,
                    },
                    range.end - range.start,
                    array.slots().to_vec(),
                )
                .map(Some);
            }
        }
        Ok(None)
    }
}
impl ValidityVTable<EncodedI32> for EncodedI32 {
    fn validity(array: ArrayView<'_, Self>) -> VortexResult<Validity> {
        Ok(if array.dtype().is_nullable() {
            Validity::AllValid
        } else {
            Validity::NonNullable
        })
    }
}
impl OperationsVTable<EncodedI32> for EncodedI32 {
    fn scalar_at(
        array: ArrayView<'_, Self>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        vortex_ensure!(index < array.len(), "LEB scalar index is out of bounds");
        let view = wire_view(array.array(), ctx)?;
        let value = view
            .value(index)
            .ok_or_else(|| vortex_err!("truncated LEB value"))?;
        Ok(if array.dtype().is_nullable() {
            Scalar::from(Some(value))
        } else {
            Scalar::from(value)
        })
    }
}

#[derive(Debug)]
struct Wire {
    raw: Buffer,
    checkpoints: ScalarBuffer<u64>,
}
#[derive(Clone, Debug)]
struct View {
    wire: Arc<Wire>,
    start: usize,
    len: usize,
}
impl View {
    fn byte_position(&self, index: usize) -> Option<usize> {
        if index >= self.len {
            return None;
        }
        let absolute = self.start.checked_add(index)?;
        let checkpoint = absolute / STRIDE;
        let mut position = usize::try_from(*self.wire.checkpoints.get(checkpoint)?).ok()?;
        for _ in checkpoint * STRIDE..absolute {
            loop {
                let byte = *self.wire.raw.get(position)?;
                position += 1;
                if byte & 128 == 0 {
                    break;
                }
            }
        }
        Some(position)
    }
    fn value(&self, index: usize) -> Option<i32> {
        let mut position = self.byte_position(index)?;
        decode_one(&self.wire.raw, &mut position)
    }
}
fn decode_one(bytes: &[u8], position: &mut usize) -> Option<i32> {
    let mut value = 0u32;
    let mut shift = 0;
    loop {
        let byte = *bytes.get(*position)?;
        *position += 1;
        if shift < 32 {
            value |= u32::from(byte & 127) << shift;
        }
        shift = (shift + 7).min(32);
        if byte & 128 == 0 {
            return Some(value as i32);
        }
    }
}
fn wire_view(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<View> {
    let encoded = array
        .as_opt::<EncodedI32>()
        .ok_or_else(|| vortex_err!("expected LEB array"))?;
    let session = ctx.session().clone();
    let raw_field = Field::new("", DataType::UInt8, false);
    let raw = session.arrow().execute_arrow(
        encoded.slots()[0].as_ref().unwrap().clone(),
        Some(&raw_field),
        ctx,
    )?;
    let raw = raw
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| vortex_err!("invalid LEB byte child"))?;
    let checkpoint_field = Field::new("", DataType::UInt64, false);
    let checkpoints = session.arrow().execute_arrow(
        encoded.slots()[1].as_ref().unwrap().clone(),
        Some(&checkpoint_field),
        ctx,
    )?;
    let checkpoints = checkpoints
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| vortex_err!("invalid LEB checkpoint child"))?;
    Ok(View {
        wire: Arc::new(Wire {
            raw: raw.values().inner().clone(),
            checkpoints: checkpoints.values().clone(),
        }),
        start: encoded.data().start,
        len: array.len(),
    })
}
fn slice_views(views: &[View], range: Range<usize>) -> Vec<View> {
    let mut result = Vec::new();
    let mut base = 0;
    for view in views {
        let start = range.start.saturating_sub(base).min(view.len);
        let end = range.end.saturating_sub(base).min(view.len);
        if start < end {
            result.push(View {
                wire: view.wire.clone(),
                start: view.start + start,
                len: end - start,
            });
        }
        base += view.len;
        if base >= range.end {
            break;
        }
    }
    result
}
fn encoded_views(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Option<Vec<View>>> {
    if array.is::<EncodedI32>() {
        return wire_view(array, ctx).map(|view| Some(vec![view]));
    }
    if let Some(slice) = array.as_opt::<Slice>() {
        let Some(views) = encoded_views(slice.slots()[0].as_ref().unwrap(), ctx)? else {
            return Ok(None);
        };
        return Ok(Some(slice_views(
            &views,
            slice.data().slice_range().clone(),
        )));
    }
    if let Some(chunked) = array.as_opt::<Chunked>() {
        let mut result = Vec::new();
        for chunk in chunked.chunks() {
            let Some(views) = encoded_views(&chunk, ctx)? else {
                return Ok(None);
            };
            result.extend(views);
        }
        return Ok(Some(result));
    }
    Ok(None)
}
/// Call before generic primitive Arrow export. Other encodings return None.
pub fn try_export(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Option<ArrowArrayRef>> {
    if !matches!(array.dtype(), DType::Primitive(PType::I32, _)) {
        return Ok(None);
    }
    encoded_views(array, ctx)
        .map(|views| views.map(|views| Arc::new(LazyArrowI32::new(views)) as ArrowArrayRef))
}

struct LazyArrowI32 {
    views: Vec<View>,
    len: usize,
    expanded: OnceLock<Int32Array>,
}
impl LazyArrowI32 {
    fn new(views: Vec<View>) -> Self {
        let len = views
            .iter()
            .try_fold(0usize, |len, view| len.checked_add(view.len))
            .expect("LEB chunked logical length overflow");
        Self {
            views,
            len,
            expanded: OnceLock::new(),
        }
    }
    fn expanded(&self) -> &Int32Array {
        self.expanded.get_or_init(|| {
            let mut values = Vec::with_capacity(self.len);
            for view in &self.views {
                if view.len == 0 {
                    continue;
                }
                let mut position = view.byte_position(0).unwrap_or(view.wire.raw.len());
                for _ in 0..view.len {
                    // Published reader arrays are frame-checked. Retaining a
                    // deterministic value even for rewritten malformed slots
                    // keeps this safe Arrow proxy's shape invariant intact.
                    values.push(decode_one(&view.wire.raw, &mut position).unwrap_or(0));
                }
            }
            Int32Array::from(values)
        })
    }
}
impl fmt::Debug for LazyArrowI32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyArrowI32")
            .field("len", &self.len)
            .field("materialized", &self.expanded.get().is_some())
            .finish()
    }
}
// SAFETY: reports exactly Int32, and every physical accessor/downcast returns
// the matching canonical Int32Array. Logical length, slices and validity agree.
unsafe impl ArrowArray for LazyArrowI32 {
    fn as_any(&self) -> &dyn Any {
        self.expanded()
    }
    fn data_type(&self) -> &DataType {
        &DataType::Int32
    }
    fn len(&self) -> usize {
        self.len
    }
    fn is_empty(&self) -> bool {
        self.len == 0
    }
    fn offset(&self) -> usize {
        0
    }
    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }
    fn to_data(&self) -> ArrayData {
        self.expanded().to_data()
    }
    fn into_data(self) -> ArrayData {
        self.expanded().to_data()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrowArrayRef {
        assert!(offset <= self.len && length <= self.len - offset);
        Arc::new(Self::new(slice_views(&self.views, offset..offset + length)))
    }
    fn visit_buffers(&self, visitor: &mut dyn FnMut(&Buffer)) {
        for view in &self.views {
            visitor(&view.wire.raw);
            visitor(view.wire.checkpoints.inner());
        }
        if let Some(expanded) = self.expanded.get() {
            expanded.visit_buffers(visitor);
        }
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.views
            .iter()
            .map(|v| v.wire.raw.capacity() + v.wire.checkpoints.inner().capacity())
            .sum::<usize>()
            + self
                .expanded
                .get()
                .map_or(0, ArrowArray::get_buffer_memory_size)
    }
    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.views.capacity() * std::mem::size_of::<View>()
            + self.get_buffer_memory_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::UInt32Array;
    use vortex::{
        VortexSessionDefault,
        array::{VortexSessionExecute, builtins::ArrayBuiltins},
        mask::Mask,
    };

    fn bytes(values: &[i32]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            let mut value = *value as u32 as u64;
            loop {
                let byte = (value & 127) as u8;
                value >>= 7;
                bytes.push(byte | if value == 0 { 0 } else { 128 });
                if value == 0 {
                    break;
                }
            }
        }
        bytes
    }
    #[test]
    fn lazy_shape_memory_slices_and_int32_materialization() -> VortexResult<()> {
        let session = VortexSession::default();
        let values = (0..600)
            .map(|i| if i % 3 == 0 { -i } else { i * 129 })
            .collect::<Vec<_>>();
        for nullable in [false, true] {
            let field = Field::new("value", DataType::Int32, nullable);
            let encoded = array(bytes(&values), values.len(), &field, &session)?;
            assert_eq!(encoded.dtype().is_nullable(), nullable);
            let mut ctx = session.create_execution_ctx();
            let arrow = try_export(&encoded, &mut ctx)?.unwrap();
            assert_eq!(arrow.data_type(), &DataType::Int32);
            assert_eq!(arrow.len(), 600);
            assert_eq!(arrow.offset(), 0);
            assert!(arrow.nulls().is_none());
            let mut visited = Vec::new();
            arrow.visit_buffers(&mut |buffer| visited.push(buffer.clone()));
            assert_eq!(visited.len(), 2);
            assert!(
                arrow.get_buffer_memory_size() >= visited.iter().map(Buffer::len).sum::<usize>()
            );
            assert!(format!("{arrow:?}").contains("materialized: false"));
            let slice = arrow.slice(247, 267);
            assert!(format!("{slice:?}").contains("materialized: false"));
            assert_eq!(
                slice.to_data(),
                Int32Array::from(values[247..514].to_vec()).to_data()
            );
            assert!(format!("{arrow:?}").contains("materialized: false"));
            let first = arrow.as_any().downcast_ref::<Int32Array>().unwrap() as *const Int32Array;
            let second = arrow.as_any().downcast_ref::<Int32Array>().unwrap() as *const Int32Array;
            assert_eq!(first, second);
            assert_eq!(arrow.to_data(), Int32Array::from(values.clone()).to_data());
            let sliced = encoded.slice(127..259)?;
            let sliced = try_export(&sliced, &mut ctx)?.unwrap();
            assert_eq!(
                sliced.to_data(),
                Int32Array::from(values[127..259].to_vec()).to_data()
            );
        }
        Ok(())
    }
    #[test]
    fn chunks_stay_lazy_and_filter_take_use_canonical_fallback() -> VortexResult<()> {
        let session = VortexSession::default();
        let field = Field::new("value", DataType::Int32, true);
        let values = vec![1, -1, 128, 7, 8];
        let encoded = array(bytes(&values), values.len(), &field, &session)?;
        let chunks = vortex::array::arrays::ChunkedArray::try_new(
            vec![encoded.slice(0..2)?, encoded.slice(2..5)?],
            encoded.dtype().clone(),
        )?
        .into_array();
        let mut ctx = session.create_execution_ctx();
        let arrow = try_export(&chunks, &mut ctx)?.unwrap();
        assert!(format!("{arrow:?}").contains("materialized: false"));
        assert_eq!(
            arrow.slice(1, 3).to_data(),
            Int32Array::from(vec![-1, 128, 7]).to_data()
        );
        let filtered = encoded.filter(Mask::from_iter([false, true, true, false, true]))?;
        let indices = session
            .arrow()
            .from_arrow_array(Arc::new(UInt32Array::from(vec![4, 0, 2])), false)?;
        let taken = encoded.take(indices)?;
        for (selection, expected) in [(filtered, vec![-1, 128, 8]), (taken, vec![8, 1, 128])] {
            let actual = session
                .arrow()
                .execute_arrow(selection, Some(&field), &mut ctx)?;
            assert_eq!(actual.to_data(), Int32Array::from(expected).to_data());
        }
        Ok(())
    }
    #[test]
    fn framing_is_checked_but_payload_width_is_not() -> VortexResult<()> {
        let session = VortexSession::default();
        let field = Field::new("value", DataType::Int32, false);
        for (bytes, count) in [(vec![128], 1), (vec![1, 2], 1), (vec![], 1), (vec![0], 0)] {
            assert!(array(bytes, count, &field, &session).is_err());
        }
        let mut long = vec![255u8; 100];
        long.push(0);
        let encoded = array(long, 1, &field, &session)?;
        let mut ctx = session.create_execution_ctx();
        let arrow = try_export(&encoded, &mut ctx)?.unwrap();
        assert_eq!(arrow.to_data(), Int32Array::from(vec![-1]).to_data());
        assert_eq!(
            to_arrow(&bytes(&[1, -1, 128]), 3)?.to_data(),
            Int32Array::from(vec![1, -1, 128]).to_data()
        );
        let empty = array(vec![], 0, &field, &session)?;
        assert_eq!(try_export(&empty, &mut ctx)?.unwrap().len(), 0);
        Ok(())
    }
}
