//! Execution-local packed Hail Calls. Logical and Arrow schemas remain the
//! ordinary four-child Call struct. Only consumers inspecting those children
//! pay to expand the validated wire words.
use arrow_array::{
    Array as ArrowArray, ArrayRef as ArrowArrayRef, BooleanArray, LargeListArray, ListArray,
    StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field};
use std::{
    any::Any,
    fmt,
    sync::{Arc, OnceLock},
};
use vortex::{
    array::{
        Array, ArrayId, ArrayParts, ArrayRef, ArrayView, EmptyArrayData, ExecutionCtx,
        ExecutionResult, IntoArray, OperationsVTable, VTable, ValidityVTable,
        arrays::{
            Chunked, Dict, Filter, List, ListViewArray, ScalarFn, Slice,
            StructArray as VxStructArray, chunked::ChunkedArrayExt, list::ListArraySlotsExt,
            listview::ListViewRebuildMode, scalar_fn::ScalarFnArrayExt,
            struct_::StructArraySlotsExt,
        },
        buffer::BufferHandle,
        builtins::ArrayBuiltins,
        serde::ArrayChildren,
        validity::Validity,
        with_empty_buffers,
    },
    arrow::{ArrowSessionExt, to_arrow_null_buffer},
    dtype::{DType, Nullability, PType},
    error::{VortexResult, vortex_ensure, vortex_err},
    scalar::Scalar,
    scalar_fn::fns::pack::Pack,
    session::{VortexSession, registry::CachedId},
};

const NAMES: [&str; 4] = [
    "__vail_call_allele0",
    "__vail_call_allele1",
    "__vail_call_ploidy",
    "__vail_call_phased",
];

/// `packed` must contain Calls already validated by the native decoder. This
/// boundary changes representation only; wire errors are reported on append.
pub fn array(
    packed: UInt32Array,
    field: &Field,
    session: &VortexSession,
) -> VortexResult<ArrayRef> {
    let dtype = session.arrow().from_arrow_field(field)?;
    let child = session
        .arrow()
        .from_arrow_array(Arc::new(packed), field.is_nullable())?;
    from_child(dtype, child)
}
fn from_child(dtype: DType, child: ArrayRef) -> VortexResult<ArrayRef> {
    Ok(Array::try_from_parts(
        ArrayParts::new(PackedCall, dtype, child.len(), EmptyArrayData)
            .with_slots(vec![Some(child)].into()),
    )?
    .into_array())
}

#[derive(Clone, Debug)]
struct PackedCall;
impl VTable for PackedCall {
    type TypedArrayData = EmptyArrayData;
    type OperationsVTable = Self;
    type ValidityVTable = Self;
    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vail.legacy-packed-call");
        *ID
    }
    fn validate(
        &self,
        _: &EmptyArrayData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        let DType::Struct(fields, _) = dtype else {
            return Err(vortex_err!("packed Call requires Struct dtype"));
        };
        vortex_ensure!(fields.nfields() == 4, "packed Call requires four fields");
        let expected = [
            DType::Primitive(PType::U16, Nullability::Nullable),
            DType::Primitive(PType::U16, Nullability::Nullable),
            DType::Primitive(PType::U8, Nullability::Nullable),
            DType::Bool(Nullability::Nullable),
        ];
        for i in 0..4 {
            vortex_ensure!(
                fields.field_name(i).is_some_and(|n| n.as_ref() == NAMES[i])
                    && fields.field_by_index(i).as_ref() == Some(&expected[i]),
                "invalid packed Call field {i}"
            );
        }
        vortex_ensure!(slots.len() == 1, "packed Call requires one wire child");
        let child = slots[0]
            .as_ref()
            .ok_or_else(|| vortex_err!("packed Call wire child is absent"))?;
        vortex_ensure!(
            child.len() == len
                && child.dtype() == &DType::Primitive(PType::U32, dtype.nullability()),
            "packed Call wire child length or dtype differs"
        );
        Ok(())
    }
    fn nbuffers(_: ArrayView<'_, Self>) -> usize {
        0
    }
    fn buffer(_: ArrayView<'_, Self>, _: usize) -> BufferHandle {
        unreachable!("packed Call buffers belong to its child")
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
            "expand execution-local packed Hail Calls before persistence"
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
        Err(vortex_err!("packed Hail Calls are execution-local"))
    }
    fn slot_name(_: ArrayView<'_, Self>, _: usize) -> String {
        "validated wire calls".into()
    }
    fn execute(array: Array<Self>, ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        let session = ctx.session().clone();
        let child = packed_arrow(array.slots()[0].as_ref().unwrap().clone(), ctx)?;
        let field = session.arrow().to_arrow_field("", array.dtype())?;
        let expanded = LazyArrowCall::new(child, field.data_type().clone());
        let canonical = session
            .arrow()
            .from_arrow_array(Arc::new(expanded.expanded().clone()), field.is_nullable())?;
        Ok(ExecutionResult::done(canonical))
    }
    fn reduce_parent(
        array: ArrayView<'_, Self>,
        parent: &ArrayRef,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        let child = array.slots()[0].as_ref().unwrap();
        let selected = if child_idx == 0 {
            if let Some(parent) = parent.as_opt::<Slice>() {
                Some(child.slice(parent.data().slice_range().clone())?)
            } else if let Some(parent) = parent.as_opt::<Filter>() {
                Some(child.filter(parent.data().filter_mask().clone())?)
            } else {
                None
            }
        } else if child_idx == 1 && parent.is::<Dict>() {
            Some(child.take(parent.slots()[0].as_ref().unwrap().clone())?)
        } else {
            None
        };
        selected
            .map(|selected| from_child(parent.dtype().clone(), selected))
            .transpose()
    }
}
impl ValidityVTable<PackedCall> for PackedCall {
    fn validity(array: ArrayView<'_, Self>) -> VortexResult<Validity> {
        array.slots()[0].as_ref().unwrap().validity()
    }
}
impl OperationsVTable<PackedCall> for PackedCall {
    fn scalar_at(
        array: ArrayView<'_, Self>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let word = array.slots()[0]
            .as_ref()
            .unwrap()
            .execute_scalar(index, ctx)?
            .as_primitive()
            .as_::<u32>()
            .ok_or_else(|| vortex_err!("missing packed Call word"))?;
        let (first, second, ploidy, phased) = decode(word);
        Ok(Scalar::struct_(
            array.dtype().clone(),
            [
                Scalar::from(Some(first)),
                Scalar::from(Some(second)),
                Scalar::from(Some(ploidy)),
                Scalar::from(Some(phased)),
            ],
        ))
    }
}

fn decode(word: u32) -> (u16, u16, u8, bool) {
    let ploidy = ((word >> 1) & 3) as u8;
    let phased = word & 1 != 0;
    let allele = u64::from(word >> 3);
    let (first, second) = match ploidy {
        0 => (0, 0),
        1 => (allele, 0),
        2 if !phased && allele <= 2 => match allele {
            0 => (0, 0),
            1 => (0, 1),
            _ => (1, 1),
        },
        2 => {
            let k = ((8 * allele + 1).isqrt() - 1) / 2;
            let j = allele - k * (k + 1) / 2;
            (j, if phased { k - j } else { k })
        }
        _ => unreachable!("the native decoder validates Call ploidy before publication"),
    };
    (first as u16, second as u16, ploidy, phased)
}

/// Proxy for the exact standard Arrow StructArray. Every operation exposing
/// physical child storage delegates to one cached StructArray, so Arrow casts
/// receive the concrete type matching `data_type`. Shape/validity remain cheap.
struct LazyArrowCall {
    packed: UInt32Array,
    data_type: DataType,
    expanded: OnceLock<StructArray>,
}
impl LazyArrowCall {
    fn new(packed: UInt32Array, data_type: DataType) -> Self {
        Self {
            packed,
            data_type,
            expanded: OnceLock::new(),
        }
    }
    fn expanded(&self) -> &StructArray {
        self.expanded.get_or_init(|| {
            let DataType::Struct(fields) = &self.data_type else {
                unreachable!("validated Call Struct dtype")
            };
            let mut first = Vec::with_capacity(self.packed.len());
            let mut second = Vec::with_capacity(self.packed.len());
            let mut ploidy = Vec::with_capacity(self.packed.len());
            let mut phased = Vec::with_capacity(self.packed.len());
            let nulls = self.packed.logical_nulls();
            for i in 0..self.packed.len() {
                let value = if nulls.as_ref().is_some_and(|nulls| !nulls.is_valid(i)) {
                    (0, 0, 0, false)
                } else {
                    decode(self.packed.value(i))
                };
                first.push(value.0);
                second.push(value.1);
                ploidy.push(value.2);
                phased.push(value.3);
            }
            StructArray::try_new_with_length(
                fields.clone(),
                vec![
                    Arc::new(UInt16Array::new(first.into(), nulls.clone())),
                    Arc::new(UInt16Array::new(second.into(), nulls.clone())),
                    Arc::new(UInt8Array::new(ploidy.into(), nulls.clone())),
                    Arc::new(BooleanArray::new(
                        BooleanBuffer::from(phased),
                        nulls.clone(),
                    )),
                ],
                nulls,
                self.packed.len(),
            )
            .expect("validated Call fields and aligned buffers")
        })
    }
}
impl fmt::Debug for LazyArrowCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyArrowCall")
            .field("len", &self.packed.len())
            .field("materialized", &self.expanded.get().is_some())
            .finish()
    }
}
// SAFETY: the proxy reports the exact standard Struct datatype and delegates
// all physical representation access/downcasting to a matching StructArray.
// Its shape, slices and validity agree with that array before materialization.
unsafe impl ArrowArray for LazyArrowCall {
    fn as_any(&self) -> &dyn Any {
        self.expanded()
    }
    fn visit_buffers(&self, visitor: &mut dyn FnMut(&arrow_buffer::Buffer)) {
        self.packed.visit_buffers(visitor);
        if let Some(expanded) = self.expanded.get() {
            expanded.visit_buffers(visitor);
        }
    }
    fn to_data(&self) -> ArrayData {
        self.expanded().to_data()
    }
    fn into_data(self) -> ArrayData {
        self.expanded().to_data()
    }
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
    fn slice(&self, offset: usize, length: usize) -> ArrowArrayRef {
        Arc::new(Self::new(
            self.packed.slice(offset, length),
            self.data_type.clone(),
        ))
    }
    fn len(&self) -> usize {
        self.packed.len()
    }
    fn is_empty(&self) -> bool {
        self.packed.is_empty()
    }
    fn offset(&self) -> usize {
        0
    }
    fn nulls(&self) -> Option<&NullBuffer> {
        self.packed.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.packed.get_buffer_memory_size()
            + self
                .expanded
                .get()
                .map_or(0, ArrowArray::get_buffer_memory_size)
    }
    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.packed.get_array_memory_size()
            + self
                .expanded
                .get()
                .map_or(0, ArrowArray::get_array_memory_size)
    }
}

fn packed_arrow(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<UInt32Array> {
    let field = Field::new("", DataType::UInt32, array.dtype().is_nullable());
    let arrow = ctx
        .session()
        .clone()
        .arrow()
        .execute_arrow(array, Some(&field), ctx)?;
    Ok(arrow
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| vortex_err!("packed Call child is not UInt32"))?
        .clone())
}

fn try_packed(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Option<UInt32Array>> {
    if let Some(call) = array.as_opt::<PackedCall>() {
        return packed_arrow(call.slots()[0].as_ref().unwrap().clone(), ctx).map(Some);
    }
    if let Some(chunks) = array.as_opt::<Chunked>() {
        let mut arrays = Vec::<ArrowArrayRef>::with_capacity(chunks.nchunks());
        for chunk in chunks.chunks() {
            let Some(packed) = try_packed(&chunk, ctx)? else {
                return Ok(None);
            };
            arrays.push(Arc::new(packed));
        }
        if !arrays.is_empty() {
            let refs = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            let combined = arrow_select::concat::concat(&refs).map_err(|e| vortex_err!("{e}"))?;
            return Ok(Some(
                combined
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .clone(),
            ));
        }
    }
    Ok(None)
}

/// Reader-local recursive Arrow export. Ordinary leaves retain the standard
/// Vortex exporter; packed Calls are intercepted before Struct canonicalization.
pub fn export(
    array: ArrayRef,
    target: Option<&Field>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    let session = ctx.session().clone();
    let inferred;
    let field = match target {
        Some(field) => field,
        None => {
            inferred = session.arrow().to_arrow_field("", array.dtype())?;
            &inferred
        }
    };
    if let Some(packed) = try_packed(&array, ctx)? {
        return Ok(Arc::new(LazyArrowCall::new(
            packed,
            field.data_type().clone(),
        )));
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            // A pack already owns lazy field expressions; avoid executing its
            // children merely to assemble the parent Arrow struct.
            let (children, validity) = if let Some(pack) = array.as_opt::<ScalarFn>()
                && pack.scalar_fn().as_opt::<Pack>().is_some()
            {
                (pack.children(), None)
            } else {
                let structure = array.clone().execute::<VxStructArray>(ctx)?;
                (
                    structure.fields().to_vec(),
                    to_arrow_null_buffer(structure.validity()?, structure.len(), ctx)?,
                )
            };
            vortex_ensure!(
                children.len() == fields.len(),
                "legacy Arrow struct field count differs"
            );
            let children = children
                .into_iter()
                .zip(fields.iter())
                .map(|(child, field)| export(child, Some(field.as_ref()), ctx))
                .collect::<VortexResult<Vec<_>>>()?;
            Ok(Arc::new(
                StructArray::try_new_with_length(fields.clone(), children, validity, array.len())
                    .map_err(|e| vortex_err!("{e}"))?,
            ))
        }
        DataType::List(element) | DataType::LargeList(element) => {
            let (elements, mut offsets, validity) = if let Some(list) = array.as_opt::<List>() {
                (
                    list.elements().clone(),
                    offset_values(list.offsets().clone(), ctx)?,
                    to_arrow_null_buffer(list.validity()?, list.len(), ctx)?,
                )
            } else {
                let view = array.clone().execute::<ListViewArray>(ctx)?;
                let view = if view.is_zero_copy_to_list() {
                    view
                } else {
                    view.rebuild(ListViewRebuildMode::MakeZeroCopyToList, ctx)?
                };
                let parts = view.into_data_parts();
                let mut offsets = offset_values(parts.offsets, ctx)?;
                let last = if array.is_empty() {
                    0
                } else {
                    let size = parts
                        .sizes
                        .execute_scalar(array.len() - 1, ctx)?
                        .as_primitive()
                        .as_::<u64>()
                        .ok_or_else(|| vortex_err!("legacy list size does not fit UInt64"))?;
                    offsets
                        .last()
                        .copied()
                        .unwrap_or(0)
                        .checked_add(size)
                        .ok_or_else(|| vortex_err!("legacy list offset overflow"))?
                };
                offsets.push(last);
                (
                    parts.elements,
                    offsets,
                    to_arrow_null_buffer(parts.validity, array.len(), ctx)?,
                )
            };
            if offsets.is_empty() {
                offsets.push(0);
            }
            let elements = export(elements, Some(element.as_ref()), ctx)?;
            match field.data_type() {
                DataType::List(_) => {
                    let offsets = offsets
                        .into_iter()
                        .map(i32::try_from)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| vortex_err!("{e}"))?;
                    Ok(Arc::new(
                        ListArray::try_new(
                            element.clone(),
                            OffsetBuffer::new(ScalarBuffer::from(offsets)),
                            elements,
                            validity,
                        )
                        .map_err(|e| vortex_err!("{e}"))?,
                    ))
                }
                _ => {
                    let offsets = offsets
                        .into_iter()
                        .map(i64::try_from)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| vortex_err!("{e}"))?;
                    Ok(Arc::new(
                        LargeListArray::try_new(
                            element.clone(),
                            OffsetBuffer::new(ScalarBuffer::from(offsets)),
                            elements,
                            validity,
                        )
                        .map_err(|e| vortex_err!("{e}"))?,
                    ))
                }
            }
        }
        _ => session.arrow().execute_arrow(array, Some(field), ctx),
    }
}
fn offset_values(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Vec<u64>> {
    let field = Field::new("", DataType::UInt64, false);
    let array = array.cast(DType::Primitive(PType::U64, Nullability::NonNullable))?;
    let arrow = ctx
        .session()
        .clone()
        .arrow()
        .execute_arrow(array, Some(&field), ctx)?;
    let offsets = arrow
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| vortex_err!("invalid legacy list offsets"))?;
    Ok(offsets.values().to_vec())
}
