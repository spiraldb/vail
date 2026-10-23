use super::input::Input;
use super::*;
use arrow_array::{builder::*, *};
use arrow_buffer::{BooleanBufferBuilder, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef, Fields};
use vail_execution::expressions::PrimitiveType as P;

// Keep Hail's packed Call until an Arrow consumer requests its public struct.
struct CallColumn {
    fields: Fields,
    packed: Vec<u32>,
    valid: NullBufferBuilder,
}
impl CallColumn {
    fn new(fields: Fields) -> Self {
        Self {
            fields,
            packed: Vec::with_capacity(8192),
            valid: NullBufferBuilder::new(8192),
        }
    }
    fn null(&mut self) {
        self.packed.push(0);
        self.valid.append_null();
    }
    fn take_packed(&mut self) -> UInt32Array {
        UInt32Array::new(
            ScalarBuffer::from(std::mem::take(&mut self.packed)),
            self.valid.finish(),
        )
    }
    fn finish(&mut self) -> Result<ArrayRef> {
        let packed = self.take_packed();
        let nulls = packed.nulls().cloned();
        let mut first = Vec::with_capacity(packed.len());
        let mut second = Vec::with_capacity(packed.len());
        let mut ploidies = Vec::with_capacity(packed.len());
        let mut phased = BooleanBufferBuilder::new(packed.len());
        for i in 0..packed.len() {
            let (a, b, p, phase) = if packed.is_null(i) {
                (0, 0, 0, false)
            } else {
                call_parts(packed.value(i))?
            };
            first.push(a);
            second.push(b);
            ploidies.push(p);
            phased.append(phase);
        }
        Ok(Arc::new(StructArray::try_new_with_length(
            self.fields.clone(),
            vec![
                Arc::new(UInt16Array::new(ScalarBuffer::from(first), nulls.clone())),
                Arc::new(UInt16Array::new(ScalarBuffer::from(second), nulls.clone())),
                Arc::new(UInt8Array::new(ScalarBuffer::from(ploidies), nulls.clone())),
                Arc::new(BooleanArray::new(phased.finish(), nulls.clone())),
            ],
            nulls,
            packed.len(),
        )?))
    }
}

fn validate_packed_call(v: u32) -> Result<()> {
    match (v >> 1) & 3 {
        3 => Err("invalid Hail call ploidy".into()),
        1 => {
            u16::try_from(v >> 3)?;
            Ok(())
        }
        // The diploid payload is at most 2^29-1, so its triangular
        // inverse k is at most 32767. Both j and k-j lie in 0..=k;
        // both phased and unphased alleles therefore fit UInt16.
        // Ploidy zero intentionally ignores its payload, like call_parts.
        _ => Ok(()),
    }
}

fn call_parts(v: u32) -> Result<(u16, u16, u8, bool)> {
    let ploidy = ((v >> 1) & 3) as u8;
    let phased = v & 1 != 0;
    let allele = (v >> 3) as u64;
    let (a, b) = match ploidy {
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
        _ => return Err("invalid Hail call ploidy".into()),
    };
    Ok((u16::try_from(a)?, u16::try_from(b)?, ploidy, phased))
}

// Column builders own values directly. No intermediate row maps, JSON values,
// Python objects, or per-cell foreign-language calls are used by the decoder.
enum Column {
    Skipped(DataType, usize, Option<ArrayRef>),
    Bool(BooleanBuilder),
    Int32(Int32Builder),
    EncodedI32(super::encoded_int::EncodedI32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    UInt16(UInt16Builder),
    UInt8(UInt8Builder),
    Call(Box<CallColumn>),
    Struct(Fields, Vec<Column>, NullBufferBuilder),
    List(FieldRef, Box<Column>, Vec<i32>, NullBufferBuilder),
}
impl Column {
    fn new(t: &DataType) -> Self {
        match t {
            DataType::Struct(fs) => Self::Struct(
                fs.clone(),
                fs.iter().map(|f| Self::new(f.data_type())).collect(),
                NullBufferBuilder::new(8192),
            ),
            DataType::List(f) => Self::List(
                f.clone(),
                Box::new(Self::new(f.data_type())),
                vec![0],
                NullBufferBuilder::new(8192),
            ),
            DataType::Boolean => Self::Bool(BooleanBuilder::with_capacity(8192)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(8192)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(8192)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(8192)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(8192)),
            DataType::Utf8 => Self::String(StringBuilder::with_capacity(8192, 65536)),
            DataType::UInt16 => Self::UInt16(UInt16Builder::with_capacity(8192)),
            DataType::UInt8 => Self::UInt8(UInt8Builder::with_capacity(8192)),
            _ => unreachable!("validated Hail Arrow type"),
        }
    }
    fn projected(node: &Node, ty: &DataType) -> Self {
        Self::projected_encoding(node, ty, false)
    }
    fn projected_encoding(node: &Node, ty: &DataType, retain_required_i32: bool) -> Self {
        match ty {
            DataType::Struct(fields) if matches!(node.ty, Type::Primitive(P::Call)) => {
                Self::Call(Box::new(CallColumn::new(fields.clone())))
            }
            DataType::Struct(fields) if node.e.kind == "EBaseStruct" => {
                let children = fields
                    .iter()
                    .enumerate()
                    .map(
                        |(i, f)| match node.fields.iter().position(|field| field.dest == i) {
                            Some(j) if !node.fields[j].project => {
                                Self::Skipped(f.data_type().clone(), 0, None)
                            }
                            Some(j) => Self::projected_encoding(
                                &node.children[j],
                                f.data_type(),
                                retain_required_i32,
                            ),
                            None => Self::new(f.data_type()),
                        },
                    )
                    .collect();
                Self::Struct(fields.clone(), children, NullBufferBuilder::new(8192))
            }
            DataType::List(field) if node.e.kind.starts_with("EArray") => Self::List(
                field.clone(),
                Box::new(
                    if retain_required_i32
                        && node.children[0].e.required
                        && matches!(node.children[0].wire, Wire::Varint)
                        && matches!(node.children[0].append_kind, AppendKind::Int32)
                    {
                        Self::EncodedI32(super::encoded_int::EncodedI32Builder::new())
                    } else {
                        Self::projected_encoding(
                            &node.children[0],
                            field.data_type(),
                            retain_required_i32,
                        )
                    },
                ),
                vec![0],
                NullBufferBuilder::new(8192),
            ),
            _ => Self::new(ty),
        }
    }
    #[inline]
    fn bool_builder(&mut self) -> &mut BooleanBuilder {
        let Self::Bool(builder) = self else {
            unreachable!("validated Hail builder")
        };
        builder
    }
    #[inline]
    fn int32(&mut self) -> &mut Int32Builder {
        let Self::Int32(builder) = self else {
            unreachable!("validated Hail builder")
        };
        builder
    }
    #[inline]
    fn int64(&mut self) -> &mut Int64Builder {
        let Self::Int64(builder) = self else {
            unreachable!("validated Hail builder")
        };
        builder
    }
    #[inline]
    fn float32(&mut self) -> &mut Float32Builder {
        let Self::Float32(builder) = self else {
            unreachable!("validated Hail builder")
        };
        builder
    }
    #[inline]
    fn float64(&mut self) -> &mut Float64Builder {
        let Self::Float64(builder) = self else {
            unreachable!("validated Hail builder")
        };
        builder
    }
    #[inline]
    fn string(&mut self) -> &mut StringBuilder {
        let Self::String(builder) = self else {
            unreachable!("validated Hail builder")
        };
        builder
    }
    #[inline]
    fn uint16(&mut self) -> &mut UInt16Builder {
        let Self::UInt16(builder) = self else {
            unreachable!("validated Hail builder")
        };
        builder
    }
    #[inline]
    fn uint8(&mut self) -> &mut UInt8Builder {
        let Self::UInt8(builder) = self else {
            unreachable!("validated Hail builder")
        };
        builder
    }
    #[inline(always)]
    fn children(&mut self) -> &mut Vec<Column> {
        let Self::Struct(_, c, _) = self else {
            unreachable!()
        };
        c
    }
    #[inline(always)]
    fn valid(&mut self, v: bool) {
        let Self::Struct(_, _, valid) = self else {
            unreachable!()
        };
        valid.append(v)
    }
    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            Self::Skipped(_, n, _) => *n,
            Self::Call(c) => c.valid.len(),
            Self::Bool(b) => b.len(),
            Self::Int32(b) => b.len(),
            Self::EncodedI32(builder) => builder.len(),
            Self::Int64(b) => b.len(),
            Self::Float32(b) => b.len(),
            Self::Float64(b) => b.len(),
            Self::String(b) => b.len(),
            Self::UInt16(b) => b.len(),
            Self::UInt8(b) => b.len(),
            Self::Struct(_, _, v) | Self::List(_, _, _, v) => v.len(),
        }
    }
    fn null(&mut self) -> Result<()> {
        match self {
            Self::Skipped(_, n, _) => *n += 1,
            Self::Call(c) => c.null(),
            Self::Struct(_, c, v) => {
                for c in c {
                    // A skipped child takes its full length from this owning
                    // struct at finish, including missing parent positions.
                    if !matches!(c, Self::Skipped(..)) {
                        c.null()?;
                    }
                }
                v.append_null()
            }
            Self::List(_, _, o, v) => {
                o.push(*o.last().unwrap());
                v.append_null()
            }
            Self::Bool(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::EncodedI32(_) => return Err("null in required Hail Int32 elements".into()),
            Self::Int64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::String(b) => b.append_null(),
            Self::UInt16(b) => b.append_null(),
            Self::UInt8(b) => b.append_null(),
        }
        Ok(())
    }
    #[inline(always)]
    fn end_list(&mut self) -> Result<()> {
        let Self::List(_, c, o, v) = self else {
            unreachable!()
        };
        o.push(i32::try_from(c.len())?);
        v.append_non_null();
        Ok(())
    }
    fn finish_vortex(
        &mut self,
        field: &arrow_schema::Field,
        session: &vortex::session::VortexSession,
    ) -> Result<vortex::array::ArrayRef> {
        use vortex::array::IntoArray;
        use vortex::array::validity::Validity;
        use vortex::arrow::ArrowSessionExt;
        fn validity(
            v: &mut NullBufferBuilder,
            nullable: bool,
            session: &vortex::session::VortexSession,
        ) -> Result<Validity> {
            Ok(match v.finish() {
                Some(nulls) => Validity::Array(session.arrow().from_arrow_array(
                    Arc::new(BooleanArray::new(nulls.inner().clone(), None)),
                    false,
                )?),
                None if nullable => Validity::AllValid,
                None => Validity::NonNullable,
            })
        }
        Ok(match self {
            Self::EncodedI32(builder) => builder.finish_vortex(field, session)?,
            Self::Call(c) => super::packed_call::array(c.take_packed(), field, session)?,
            Self::Struct(fs, children, valid) => {
                let n = valid.len();
                let arrays = children
                    .iter_mut()
                    .zip(fs.iter())
                    .map(|(child, f)| {
                        if let Self::Skipped(_, len, _) = child {
                            *len = n;
                        }
                        child.finish_vortex(f, session)
                    })
                    .collect::<Result<Vec<_>>>()?;
                vortex::array::arrays::StructArray::try_new_with_dtype(
                    arrays,
                    session.arrow().from_arrow_fields(fs)?,
                    n,
                    validity(valid, field.is_nullable(), session)?,
                )?
                .into_array()
            }
            Self::List(element, child, offsets, valid) => {
                let values = child.finish_vortex(element, session)?;
                let offsets = Int32Array::new(
                    ScalarBuffer::from(std::mem::replace(offsets, vec![0])),
                    None,
                );
                let offsets = session.arrow().from_arrow_array(Arc::new(offsets), false)?;
                vortex::array::arrays::ListArray::try_new(
                    values,
                    offsets,
                    validity(valid, field.is_nullable(), session)?,
                )?
                .into_array()
            }
            _ => session.arrow().from_arrow_array(self.finish()?, field)?,
        })
    }
    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(match self {
            Self::EncodedI32(builder) => builder.finish_arrow()?,
            Self::Call(c) => c.finish()?,
            Self::Skipped(ty, n, cached) => {
                let length = std::mem::take(n);
                if cached.as_ref().is_none_or(|array| array.len() < length) {
                    *cached = Some(new_null_array(ty, length));
                }
                cached.as_ref().unwrap().slice(0, length)
            }
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::String(b) => Arc::new(b.finish()),
            Self::UInt16(b) => Arc::new(b.finish()),
            Self::UInt8(b) => Arc::new(b.finish()),
            Self::Struct(fs, c, v) => {
                let n = v.len();
                let arrays = c
                    .iter_mut()
                    .map(|child| {
                        if let Self::Skipped(_, len, _) = child {
                            *len = n;
                        }
                        child.finish()
                    })
                    .collect::<Result<_>>()?;
                Arc::new(StructArray::try_new_with_length(
                    fs.clone(),
                    arrays,
                    v.finish(),
                    n,
                )?)
            }
            Self::List(f, c, o, v) => {
                let child = c.finish()?;
                let offsets = std::mem::replace(o, vec![0]);
                Arc::new(ListArray::try_new(
                    f.clone(),
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    child,
                    v.finish(),
                )?)
            }
        })
    }
}
fn logical_fields(t: &Type) -> Vec<(String, Type)> {
    match t {
        Type::Struct { r#struct: f } => f.clone(),
        Type::Tuple { tuple } => tuple
            .iter()
            .enumerate()
            .map(|(i, t)| (i.to_string(), t.clone()))
            .collect(),
        Type::Locus { .. } => vec![
            ("contig".into(), Type::Primitive(P::Str)),
            ("position".into(), Type::Primitive(P::Int32)),
        ],
        Type::Interval { interval } => vec![
            ("start".into(), *interval.clone()),
            ("end".into(), *interval.clone()),
            ("includesStart".into(), Type::Primitive(P::Bool)),
            ("includesEnd".into(), Type::Primitive(P::Bool)),
        ],
        _ => vec![],
    }
}
pub fn check(e: &Encoding, t: &Type) -> Result<()> {
    let valid = match (e.kind.as_str(), t) {
        ("EBoolean", Type::Primitive(P::Bool))
        | ("EInt32", Type::Primitive(P::Int32 | P::Call))
        | ("EInt64", Type::Primitive(P::Int64))
        | ("EVarint", Type::Primitive(P::Int32 | P::Int64 | P::Call))
        | ("EFloat32", Type::Primitive(P::Float32))
        | ("EFloat64", Type::Primitive(P::Float64)) => true,
        (k, Type::Primitive(P::Str)) if k.starts_with("EBinary") => true,
        (
            "EBaseStruct",
            Type::Struct { .. } | Type::Tuple { .. } | Type::Locus { .. } | Type::Interval { .. },
        ) => {
            let fs = logical_fields(t);
            if fs.len() != e.fields.len() {
                false
            } else {
                for (n, child) in &e.fields {
                    let ty = fs
                        .iter()
                        .find(|(name, _)| name == n)
                        .ok_or("incompatible Hail struct field")?;
                    check(child, &ty.1)?
                }
                true
            }
        }
        (k, Type::Array { array } | Type::Set { set: array }) if k.starts_with("EArray") => {
            check(&e.fields[0].1, array)?;
            true
        }
        (k, Type::Dict { dict }) if k.starts_with("EArray") => {
            check(
                &e.fields[0].1,
                &Type::Struct {
                    r#struct: vec![
                        ("key".into(), dict.0.clone()),
                        ("value".into(), dict.1.clone()),
                    ],
                },
            )?;
            true
        }
        ("ENDArrayColumnMajor", Type::Ndarray { ndarray }) => {
            check(&e.fields[0].1, &ndarray.0)?;
            e.rank == ndarray.1
        }
        _ => false,
    };
    if !valid {
        return Err(format!("Hail encoding {} is incompatible with {t:?}", e.kind).into());
    }
    Ok(())
}
fn boolean(input: &mut Input) -> Result<bool> {
    match input.byte()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err("invalid Hail Boolean".into()),
    }
}
enum Bitmap {
    Inline([u8; 8]),
    Heap(Vec<u8>),
}
#[inline(never)]
fn heap_bitmap(input: &mut Input, bytes: usize) -> Result<Bitmap> {
    Ok(Bitmap::Heap(input.bytes(bytes)?))
}
#[inline(always)]
fn bitmap(input: &mut Input, n: usize) -> Result<Bitmap> {
    let bytes = n.div_ceil(8);
    if bytes <= 8 {
        let mut bits = [0; 8];
        for byte in &mut bits[..bytes] {
            *byte = input.byte()?;
        }
        Ok(Bitmap::Inline(bits))
    } else {
        heap_bitmap(input, bytes)
    }
}
impl Bitmap {
    #[inline(always)]
    fn low_word(&self) -> u64 {
        match self {
            Self::Inline(bytes) => u64::from_le_bytes(*bytes),
            Self::Heap(bytes) => {
                let mut word = [0; 8];
                let n = bytes.len().min(8);
                word[..n].copy_from_slice(&bytes[..n]);
                u64::from_le_bytes(word)
            }
        }
    }
    fn present_count(&self, len: usize) -> usize {
        let bits: &[u8] = match self {
            Self::Inline(b) => b,
            Self::Heap(b) => b,
        };
        let whole = len / 8;
        let mut missing = bits[..whole]
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum::<usize>();
        let remainder = len % 8;
        if remainder != 0 {
            missing += (bits[whole] & ((1u8 << remainder) - 1)).count_ones() as usize;
        }
        len - missing
    }
}
#[inline(always)]
fn absent(bitmap: &Bitmap, i: usize) -> bool {
    let bits: &[u8] = match bitmap {
        Bitmap::Inline(b) => b,
        Bitmap::Heap(b) => b,
    };
    bits[i / 8] & (1 << (i % 8)) != 0
}

#[derive(Clone, Copy)]
enum Wire {
    Bool,
    Int32,
    Int64,
    Varint,
    Float32,
    Float64,
    Binary,
    Struct,
    Array,
    Ndarray,
}
impl Wire {
    fn new(kind: &str) -> Self {
        match kind {
            "EBoolean" => Self::Bool,
            "EInt32" => Self::Int32,
            "EInt64" => Self::Int64,
            "EVarint" => Self::Varint,
            "EFloat32" => Self::Float32,
            "EFloat64" => Self::Float64,
            "EBaseStruct" => Self::Struct,
            "ENDArrayColumnMajor" => Self::Ndarray,
            k if k.starts_with("EBinary") => Self::Binary,
            k if k.starts_with("EArray") => Self::Array,
            _ => unreachable!("validated Hail wire encoding"),
        }
    }
}

#[derive(Clone, Copy)]
enum AppendKind {
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Call,
    Struct,
    List,
    Ndarray,
}
impl AppendKind {
    fn new(ty: &Type) -> Self {
        match ty {
            Type::Primitive(P::Bool) => Self::Bool,
            Type::Primitive(P::Int32) => Self::Int32,
            Type::Primitive(P::Int64) => Self::Int64,
            Type::Primitive(P::Float32) => Self::Float32,
            Type::Primitive(P::Float64) => Self::Float64,
            Type::Primitive(P::Str) => Self::String,
            Type::Primitive(P::Call) => Self::Call,
            Type::Struct { .. }
            | Type::Tuple { .. }
            | Type::Interval { .. }
            | Type::Locus { .. } => Self::Struct,
            Type::Array { .. } | Type::Set { .. } | Type::Dict { .. } => Self::List,
            Type::Ndarray { .. } => Self::Ndarray,
        }
    }
}

#[derive(Clone, Copy)]
enum ArraySkip {
    Int32,
    Int64,
    Varint,
    Fixed {
        raw: Option<usize>,
        leb: Option<usize>,
    },
}

struct FieldOp {
    dest: usize,
    missing_bit: usize,
    project: bool,
    invert_boolean: bool,
    varint_run: u8,
    run_requires_leb: bool,
    run_missing_mask: u64,
    run_wide_missingness: bool,
    run_output_mask: u8,
}

#[derive(Clone, Copy)]
enum StructPostlude {
    Plain,
    Locus,
    EmptyTuple,
}

// Builder references are bound once per array in physical wire order. Taking
// each logical destination from disjoint iter_mut references needs no aliasing.
enum BoundOutput<'a> {
    Skip,
    Bool(&'a mut BooleanBuilder),
    InvertedBool(&'a mut BooleanBuilder),
    I32(&'a mut Int32Builder),
    I64(&'a mut Int64Builder),
    F32(&'a mut Float32Builder),
    F64(&'a mut Float64Builder),
    Call(&'a mut CallColumn),
    EncodedList(BoundList<'a, super::encoded_int::EncodedI32Builder>),
    ArrowList(BoundList<'a, Int32Builder>),
    Other(&'a mut Column),
}
struct BoundList<'a, T> {
    values: &'a mut T,
    offsets: &'a mut Vec<i32>,
    validity: &'a mut NullBufferBuilder,
    decode: for<'b> fn(&mut Input, &mut BoundList<'b, T>) -> Result<()>,
}
impl<T> BoundList<'_, T> {
    fn null(&mut self) {
        self.offsets.push(*self.offsets.last().unwrap());
        self.validity.append_null();
    }
}
#[inline(always)]
fn bound_list_length<const MODE: u8>(input: &mut Input) -> Result<usize> {
    let value = match MODE {
        0 => i32::from_le_bytes(input.fixed()?) as i64,
        1 => input.varint(32)? as u32 as i32 as i64,
        2 => input.varint(32)? as i64,
        _ => unreachable!(),
    };
    Ok(usize::try_from(value).map_err(|_| "negative Hail collection length")?)
}
fn decode_encoded_list<const MODE: u8>(
    input: &mut Input,
    list: &mut BoundList<'_, super::encoded_int::EncodedI32Builder>,
) -> Result<()> {
    let count = bound_list_length::<MODE>(input)?;
    list.values.capture(input, count)?;
    list.offsets.push(i32::try_from(list.values.len())?);
    list.validity.append_non_null();
    Ok(())
}
fn decode_arrow_list<const MODE: u8>(
    input: &mut Input,
    list: &mut BoundList<'_, Int32Builder>,
) -> Result<()> {
    let count = bound_list_length::<MODE>(input)?;
    for _ in 0..count {
        let value = input.varint(64)?;
        if value > u32::MAX as u64 {
            return Err("Hail int32 varint overflow".into());
        }
        list.values.append_value(value as u32 as i32);
    }
    list.offsets.push(i32::try_from(list.values.len())?);
    list.validity.append_non_null();
    Ok(())
}
struct BoundField<'n, 'a> {
    node: &'n Node,
    missing: usize,
    output: BoundOutput<'a>,
}
impl BoundField<'_, '_> {
    fn null(&mut self) -> Result<()> {
        match &mut self.output {
            BoundOutput::Skip => {}
            BoundOutput::Bool(b) | BoundOutput::InvertedBool(b) => b.append_null(),
            BoundOutput::I32(b) => b.append_null(),
            BoundOutput::I64(b) => b.append_null(),
            BoundOutput::F32(b) => b.append_null(),
            BoundOutput::F64(b) => b.append_null(),
            BoundOutput::Call(b) => b.null(),
            BoundOutput::EncodedList(list) => list.null(),
            BoundOutput::ArrowList(list) => list.null(),
            BoundOutput::Other(b) => b.null()?,
        }
        Ok(())
    }
    fn append(&mut self, input: &mut Input) -> Result<()> {
        match &mut self.output {
            BoundOutput::Skip => self.node.skip(input)?,
            BoundOutput::Bool(b) => b.append_value(boolean(input)?),
            BoundOutput::InvertedBool(b) => b.append_value(!boolean(input)?),
            BoundOutput::I32(b) => b.append_value(self.node.integer(input, 32)? as i32),
            BoundOutput::I64(b) => b.append_value(self.node.integer(input, 64)?),
            BoundOutput::F32(b) => b.append_value(f32::from_le_bytes(input.fixed()?)),
            BoundOutput::F64(b) => b.append_value(f64::from_le_bytes(input.fixed()?)),
            BoundOutput::Call(b) => {
                let value = self.node.integer(input, 32)? as u32;
                validate_packed_call(value)?;
                b.packed.push(value);
                b.valid.append_non_null();
            }
            BoundOutput::Other(b) => self.node.append(input, b)?,
            BoundOutput::EncodedList(list) => (list.decode)(input, list)?,
            BoundOutput::ArrowList(list) => (list.decode)(input, list)?,
        }
        Ok(())
    }
    fn append_small(&mut self, value: u8) -> Result<()> {
        match &mut self.output {
            BoundOutput::Skip => {}
            BoundOutput::I32(b) => b.append_value(value as i32),
            BoundOutput::I64(b) => b.append_value(value as i64),
            BoundOutput::Call(b) => {
                validate_packed_call(value as u32)?;
                b.packed.push(value as u32);
                b.valid.append_non_null();
            }
            BoundOutput::Other(b) => self.node.append_call_value(value as u32, b)?,
            _ => unreachable!("bound integer run"),
        }
        Ok(())
    }
    fn missing(&self, bits: &Bitmap) -> bool {
        self.missing != usize::MAX && absent(bits, self.missing)
    }
}
enum BoundOp<'n, 'a> {
    Field(BoundField<'n, 'a>),
    Run {
        fields: Vec<BoundField<'n, 'a>>,
        requires_leb: bool,
    },
}
impl BoundOp<'_, '_> {
    fn null(&mut self) -> Result<()> {
        match self {
            Self::Field(f) => f.null(),
            Self::Run { fields, .. } => {
                for field in fields {
                    field.null()?;
                }
                Ok(())
            }
        }
    }
    fn append(&mut self, input: &mut Input, bits: &Bitmap, all_present: bool) -> Result<()> {
        match self {
            Self::Field(field) => {
                if !all_present && field.missing(bits) {
                    field.null()
                } else {
                    field.append(input)
                }
            }
            Self::Run {
                fields,
                requires_leb,
            } => {
                if (!*requires_leb || input.leb)
                    && (all_present || !fields.iter().any(|f| f.missing(bits)))
                {
                    if let Some(values) = input.try_small_varints(fields.len())? {
                        for (field, value) in fields.iter_mut().zip(values) {
                            field.append_small(value)?;
                        }
                        return Ok(());
                    }
                }
                for field in fields {
                    if !all_present && field.missing(bits) {
                        field.null()?;
                    } else {
                        field.append(input)?;
                    }
                }
                Ok(())
            }
        }
    }
}

// Slice probing is transactional at entry boundaries. All scalar writes are
// deferred until the complete entry is present in the existing Input buffer.
#[derive(Clone, Copy)]
enum ProbeKind {
    Byte,
    Bool,
    Call,
    F32,
    F64,
    IntegerArray,
}
struct ProbeField {
    kind: ProbeKind,
    missing: u64,
    output: Option<usize>,
}
struct BufferedScalars {
    fields: Vec<ProbeField>,
    bitmap_bytes: usize,
}
// The final word can contain following fields: consume precisely the Nth
// terminator. An incomplete payload leaves the caller's cursor unchanged.
fn slice_skip_varints(bytes: &[u8], cursor: &mut usize, mut remaining: usize) -> Option<()> {
    let mut position = *cursor;
    while remaining != 0 {
        let available = bytes.get(position..)?;
        if available.len() >= 8 {
            let word = u64::from_le_bytes(available[..8].try_into().ok()?);
            let high = word & 0x8080_8080_8080_8080;
            if remaining <= 8 && high & (u64::MAX >> ((8 - remaining) * 8)) == 0 {
                position += remaining;
                break;
            }
            let mut terminators = high ^ 0x8080_8080_8080_8080;
            let count = terminators.count_ones() as usize;
            if count < remaining {
                remaining -= count;
                position += 8;
            } else {
                for _ in 1..remaining {
                    terminators &= terminators - 1;
                }
                position += terminators.trailing_zeros() as usize / 8 + 1;
                break;
            }
        } else {
            for &byte in available {
                position += 1;
                if byte < 128 {
                    remaining -= 1;
                    if remaining == 0 {
                        break;
                    }
                }
            }
            if remaining != 0 {
                return None;
            }
        }
    }
    *cursor = position;
    Some(())
}
impl BufferedScalars {
    fn new(node: &Node, leb: bool) -> Option<Self> {
        if node.optional_count > 64 || node.fields.is_empty() {
            return None;
        }
        let mut selected = 0;
        let mut fields = Vec::with_capacity(node.fields.len());
        for (op, child) in node.fields.iter().zip(&node.children) {
            let kind = match child.append_kind {
                AppendKind::Int32 | AppendKind::Int64 | AppendKind::Call
                    if leb || matches!(child.wire, Wire::Varint) =>
                {
                    if op.project && matches!(child.append_kind, AppendKind::Call) {
                        ProbeKind::Call
                    } else {
                        ProbeKind::Byte
                    }
                }
                AppendKind::Bool => ProbeKind::Bool,
                AppendKind::Float32 => ProbeKind::F32,
                AppendKind::Float64 => ProbeKind::F64,
                AppendKind::List
                    if !op.project
                        && child.children[0].e.required
                        && (child.varint_length || leb)
                        && (matches!(child.array_skip, ArraySkip::Varint)
                            || leb
                                && matches!(
                                    child.array_skip,
                                    ArraySkip::Int32 | ArraySkip::Int64
                                )) =>
                {
                    ProbeKind::IntegerArray
                }
                _ => return None,
            };
            let output = if op.project {
                let index = selected;
                selected += 1;
                Some(index)
            } else {
                None
            };
            if selected > 16 {
                return None;
            }
            fields.push(ProbeField {
                kind,
                missing: if op.missing_bit == usize::MAX {
                    0
                } else {
                    1u64 << op.missing_bit
                },
                output,
            });
        }
        Some(Self {
            fields,
            bitmap_bytes: node.optional_count.div_ceil(8),
        })
    }
    fn probe(&self, bytes: &[u8], cursor: &mut usize, values: &mut [u64; 16]) -> Option<u16> {
        let bitmap = bytes.get(*cursor..cursor.checked_add(self.bitmap_bytes)?)?;
        let mut missing = 0u64;
        for (i, byte) in bitmap.iter().enumerate() {
            missing |= (*byte as u64) << (i * 8);
        }
        *cursor += self.bitmap_bytes;
        let mut nulls = 0;
        for field in &self.fields {
            if missing & field.missing != 0 {
                if let Some(output) = field.output {
                    nulls |= 1 << output;
                }
                continue;
            }
            let value = match field.kind {
                ProbeKind::Byte | ProbeKind::Bool | ProbeKind::Call => {
                    let value = *bytes.get(*cursor)?;
                    if value >= 128
                        || matches!(field.kind, ProbeKind::Bool) && value > 1
                        || matches!(field.kind, ProbeKind::Call)
                            && validate_packed_call(value as u32).is_err()
                    {
                        return None;
                    }
                    *cursor += 1;
                    value as u64
                }
                ProbeKind::F32 => {
                    let end = cursor.checked_add(4)?;
                    let value = u32::from_le_bytes(bytes.get(*cursor..end)?.try_into().ok()?);
                    *cursor = end;
                    value as u64
                }
                ProbeKind::F64 => {
                    let end = cursor.checked_add(8)?;
                    let value = u64::from_le_bytes(bytes.get(*cursor..end)?.try_into().ok()?);
                    *cursor = end;
                    value
                }
                ProbeKind::IntegerArray => {
                    let n = *bytes.get(*cursor)?;
                    if n >= 128 {
                        return None;
                    }
                    *cursor += 1;
                    slice_skip_varints(bytes, cursor, usize::from(n))?;
                    0
                }
            };
            if let Some(output) = field.output {
                values[output] = value;
            }
        }
        Some(nulls)
    }
}
fn bind_buffered_outputs<'p, 'n, 'a>(
    ops: &'p mut [BoundOp<'n, 'a>],
) -> vortex::array::smallvec::SmallVec<[&'p mut BoundOutput<'a>; 16]> {
    let mut selected = vortex::array::smallvec::SmallVec::new();
    for op in ops {
        let fields = match op {
            BoundOp::Field(field) => std::slice::from_mut(field),
            BoundOp::Run { fields, .. } => fields.as_mut_slice(),
        };
        for field in fields {
            if !matches!(field.output, BoundOutput::Skip) {
                selected.push(&mut field.output);
            }
        }
    }
    selected
}
fn buffered_scalar_output(output: &mut BoundOutput<'_>, value: u64, missing: bool) {
    macro_rules! append {
        ($builder:expr, $value:expr) => {
            if missing {
                $builder.append_null();
            } else {
                $builder.append_value($value);
            }
        };
    }
    match output {
        BoundOutput::Skip => unreachable!("selected buffered output"),
        BoundOutput::I32(b) => append!(b, value as i32),
        BoundOutput::I64(b) => append!(b, value as i64),
        BoundOutput::F32(b) => append!(b, f32::from_bits(value as u32)),
        BoundOutput::F64(b) => append!(b, f64::from_bits(value)),
        BoundOutput::Bool(b) => append!(b, value != 0),
        BoundOutput::InvertedBool(b) => append!(b, value == 0),
        BoundOutput::Call(b) => {
            if missing {
                b.null();
            } else {
                b.packed.push(value as u32);
                b.valid.append_non_null();
            }
        }
        BoundOutput::Other(_) | BoundOutput::EncodedList(_) | BoundOutput::ArrowList(_) => {
            unreachable!("buffered scalar eligibility")
        }
    }
}
fn buffered_scalar_row(outputs: &mut [&mut BoundOutput<'_>], values: &[u64; 16], nulls: u16) {
    for (index, output) in outputs.iter_mut().enumerate() {
        buffered_scalar_output(output, values[index], nulls & (1 << index) != 0);
    }
}
fn has_buffered_outputs(ops: &[BoundOp<'_, '_>]) -> bool {
    let eligible = |f: &BoundField<'_, '_>| {
        !matches!(
            f.output,
            BoundOutput::Other(_) | BoundOutput::EncodedList(_) | BoundOutput::ArrowList(_)
        )
    };
    ops.iter().all(|op| match op {
        BoundOp::Field(f) => eligible(f),
        BoundOp::Run { fields, .. } => fields.iter().all(eligible),
    })
}

/// Compile physical destination mapping once, outside the row loop.
struct Node {
    e: Encoding,
    wire: Wire,
    append_kind: AppendKind,
    fields: Vec<FieldOp>,
    array_skip: ArraySkip,
    ty: Type,
    children: Vec<Node>,
    optional_count: usize,
    all_present_mask: Option<u64>,
    struct_postlude: StructPostlude,
    distinct_destinations: bool,
    buffered_scalars: [std::sync::OnceLock<Option<BufferedScalars>>; 2],
    varint_length: bool,
}
impl Node {
    fn new(e: &Encoding, t: &Type) -> Self {
        let logical = logical_fields(t);
        let mut children = vec![];
        let mut dest = vec![];
        if e.kind == "EBaseStruct" {
            for (n, c) in &e.fields {
                let index = logical
                    .iter()
                    .position(|(name, _)| name == n)
                    .expect("checked schema");
                children.push(Self::new(c, &logical[index].1));
                dest.push(match t {
                    Type::Interval { .. } => [0, 2, 1, 3][index],
                    Type::Locus { .. } => index + 1,
                    _ => index,
                });
            }
        } else if e.kind.starts_with("EArray") {
            let child = match t {
                Type::Array { array } | Type::Set { set: array } => *array.clone(),
                Type::Dict { dict } => Type::Struct {
                    r#struct: vec![
                        ("key".into(), dict.0.clone()),
                        ("value".into(), dict.1.clone()),
                    ],
                },
                _ => unreachable!(),
            };
            children.push(Self::new(&e.fields[0].1, &child));
        } else if let Type::Ndarray { ndarray } = t {
            children.push(Self::new(&e.fields[0].1, &ndarray.0));
        }
        let mut optional_count = 0;
        let mut fields = children
            .iter()
            .enumerate()
            .map(|(i, child)| {
                let missing_bit = if child.e.required {
                    usize::MAX
                } else {
                    let bit = optional_count;
                    optional_count += 1;
                    bit
                };
                FieldOp {
                    dest: dest.get(i).copied().unwrap_or(i),
                    missing_bit,
                    project: true,
                    invert_boolean: matches!(t, Type::Interval { .. })
                        && e.fields[i].0 == "includesStart",
                    varint_run: 0,
                    run_requires_leb: false,
                    run_missing_mask: 0,
                    run_wide_missingness: false,
                    run_output_mask: 0,
                }
            })
            .collect::<Vec<_>>();
        // Group only contiguous integer/call wire values. Optional fields
        // use the ordinary decoder unless the complete group is present.
        if e.kind == "EBaseStruct" {
            let eligible = |child: &Node| {
                matches!(
                    child.append_kind,
                    AppendKind::Int32 | AppendKind::Int64 | AppendKind::Call
                ) && matches!(child.wire, Wire::Int32 | Wire::Int64 | Wire::Varint)
            };
            let mut i = 0;
            while i < children.len() {
                if !eligible(&children[i]) {
                    i += 1;
                    continue;
                }
                let start = i;
                let mut requires_leb = false;
                while i < children.len() && i - start < 8 && eligible(&children[i]) {
                    requires_leb |= !matches!(children[i].wire, Wire::Varint);
                    i += 1;
                }
                if i - start >= 2 {
                    fields[start].varint_run = (i - start) as u8;
                    fields[start].run_requires_leb = requires_leb;
                    fields[start].run_output_mask = u8::MAX >> (8 - (i - start));
                    let mut missing_mask = 0u64;
                    let mut wide = false;
                    for field in &fields[start..i] {
                        if field.missing_bit == usize::MAX {
                            continue;
                        }
                        if field.missing_bit >= 64 {
                            wide = true;
                        } else {
                            missing_mask |= 1u64 << field.missing_bit;
                        }
                    }
                    fields[start].run_missing_mask = missing_mask;
                    fields[start].run_wide_missingness = wide;
                }
            }
        }
        let array_skip = if e.kind.starts_with("EArray") {
            let child = &children[0];
            match child.wire {
                Wire::Int32 => ArraySkip::Int32,
                Wire::Int64 => ArraySkip::Int64,
                Wire::Varint => ArraySkip::Varint,
                _ => ArraySkip::Fixed {
                    raw: child.fixed_width(false),
                    leb: child.fixed_width(true),
                },
            }
        } else {
            ArraySkip::Fixed {
                raw: None,
                leb: None,
            }
        };
        let distinct_destinations = fields.iter().enumerate().all(|(i, field)| {
            fields[..i]
                .iter()
                .all(|previous| previous.dest != field.dest)
        });
        Self {
            e: e.clone(),
            wire: Wire::new(&e.kind),
            append_kind: AppendKind::new(t),
            fields,
            array_skip,
            ty: t.clone(),
            children,
            optional_count,
            all_present_mask: (optional_count <= 64).then(|| {
                if optional_count == 64 {
                    u64::MAX
                } else {
                    (1u64 << optional_count) - 1
                }
            }),
            distinct_destinations,
            buffered_scalars: std::array::from_fn(|_| std::sync::OnceLock::new()),
            struct_postlude: match t {
                Type::Locus { .. } => StructPostlude::Locus,
                Type::Tuple { tuple } if tuple.is_empty() => StructPostlude::EmptyTuple,
                _ => StructPostlude::Plain,
            },
            varint_length: e.kind.ends_with('2'),
        }
    }
    #[inline(always)]
    fn integer(&self, input: &mut Input, bits: u32) -> Result<i64> {
        if matches!(self.wire, Wire::Varint) {
            let value = input.varint(64)?;
            if bits == 32 {
                if value > u32::MAX as u64 {
                    return Err("Hail int32 varint overflow".into());
                }
                Ok(value as u32 as i32 as i64)
            } else {
                Ok(value as i64)
            }
        } else {
            input.int(bits)
        }
    }
    #[inline(always)]
    fn length(&self, input: &mut Input) -> Result<usize> {
        let value = if self.varint_length {
            input.varint(32)? as i64
        } else {
            input.int(32)?
        };
        Ok(usize::try_from(value).map_err(|_| "negative Hail collection length")?)
    }
    // Compile child demand once, like Vortex struct layouts. Lists propagate
    // demand to their elements without materializing unrelated children.
    fn demand(&mut self, paths: &[Vec<String>]) {
        if paths.iter().any(Vec::is_empty) {
            return;
        }
        if self.e.kind == "EBaseStruct" {
            // Locus has a derived ordinal child; keep its small physical record
            // together so contig validation and ordinal construction stay valid.
            if matches!(self.ty, Type::Locus { .. }) {
                return;
            }
            for i in 0..self.children.len() {
                let name = &self.e.fields[i].0;
                let selected = paths
                    .iter()
                    .filter(|p| p.first() == Some(name))
                    .map(|p| p[1..].to_vec())
                    .collect::<Vec<_>>();
                self.fields[i].project &= !selected.is_empty();
                if self.fields[i].project {
                    self.children[i].demand(&selected);
                }
            }
        } else if self.e.kind.starts_with("EArray") {
            self.children[0].demand(paths);
        }
        self.refresh_run_projection();
    }
    fn refresh_run_projection(&mut self) {
        for cached in &mut self.buffered_scalars {
            cached.take();
        }
        for i in 0..self.fields.len() {
            let count = self.fields[i].varint_run as usize;
            if count == 0 {
                continue;
            }
            self.fields[i].run_output_mask = self.fields[i..i + count]
                .iter()
                .enumerate()
                .fold(0u8, |mask, (offset, op)| {
                    mask | (u8::from(op.project) << offset)
                });
        }
    }
    fn fixed_width(&self, leb: bool) -> Option<usize> {
        match self.wire {
            Wire::Bool => Some(1),
            Wire::Float32 => Some(4),
            Wire::Float64 => Some(8),
            Wire::Int32 if !leb => Some(4),
            Wire::Int64 if !leb => Some(8),
            Wire::Struct if self.children.iter().all(|c| c.e.required) => self
                .children
                .iter()
                .try_fold(0usize, |n, c| n.checked_add(c.fixed_width(leb)?)),
            _ => None,
        }
    }
    #[inline(always)]
    fn skip(&self, input: &mut Input) -> Result<()> {
        match self.wire {
            Wire::Bool => {
                boolean(input)?;
            }
            Wire::Int32 => {
                input.int(32)?;
            }
            Wire::Int64 => {
                input.int(64)?;
            }
            Wire::Varint => {
                input.varint(64)?;
            }
            Wire::Float32 => input.skip(4)?,
            Wire::Float64 => input.skip(8)?,
            Wire::Array => return self.skip_array(input),
            _ => return self.skip_container(input),
        }
        Ok(())
    }
    // Keep array framing out of the inlined per-field dispatch. Arrays have
    // their own compact frame rather than sharing recursive struct machinery.
    #[inline(never)]
    fn skip_array(&self, input: &mut Input) -> Result<()> {
        let n = self.length(input)?;
        let c = &self.children[0];
        let bits = if c.e.required {
            Bitmap::Inline([0; 8])
        } else {
            bitmap(input, n)?
        };
        let (varint_bits, fixed_width) = match self.array_skip {
            ArraySkip::Int32 if input.leb => (Some(32), None),
            ArraySkip::Int32 => (None, Some(4)),
            ArraySkip::Int64 if input.leb => (Some(64), None),
            ArraySkip::Int64 => (None, Some(8)),
            ArraySkip::Varint => (Some(64), None),
            ArraySkip::Fixed { raw, leb } => (None, if input.leb { leb } else { raw }),
        };
        if let Some(bits_per_value) = varint_bits {
            let count = if c.e.required {
                n
            } else {
                bits.present_count(n)
            };
            input.skip_varints(count, bits_per_value)?;
        } else if let Some(width) = fixed_width {
            let count = if c.e.required {
                n
            } else {
                bits.present_count(n)
            };
            input.skip(
                count
                    .checked_mul(width)
                    .ok_or("Hail array byte length overflow")?,
            )?
        } else {
            for i in 0..n {
                if c.e.required || !absent(&bits, i) {
                    c.skip(input)?
                }
            }
        }

        Ok(())
    }
    fn skip_container(&self, input: &mut Input) -> Result<()> {
        match self.wire {
            Wire::Binary => {
                let n = self.length(input)?;
                input.skip(n)?
            }
            Wire::Struct => {
                let bits = bitmap(input, self.optional_count)?;
                for (op, c) in self.fields.iter().zip(&self.children) {
                    let bit = op.missing_bit;
                    let missing = bit != usize::MAX && absent(&bits, bit);
                    if !missing {
                        c.skip(input)?
                    }
                }
            }

            Wire::Ndarray => {
                let mut n = 1usize;
                for _ in 0..self.e.rank {
                    n = n
                        .checked_mul(usize::try_from(input.int(64)?)?)
                        .ok_or("Hail ndarray shape overflow")?
                }
                for _ in 0..n {
                    self.children[0].skip(input)?
                }
            }
            _ => unreachable!("primitive leaves use compact skip dispatch"),
        }
        Ok(())
    }
    #[inline(always)]
    fn append(&self, input: &mut Input, out: &mut Column) -> Result<()> {
        // The recursive container loop sees only this compact leaf dispatch;
        // large struct/list/ndarray decoding stays outside its instruction path.
        match self.append_kind {
            AppendKind::Bool => out.bool_builder().append_value(boolean(input)?),
            AppendKind::Int32 => out.int32().append_value(self.integer(input, 32)? as i32),
            AppendKind::Int64 => out.int64().append_value(self.integer(input, 64)?),
            AppendKind::Float32 => out
                .float32()
                .append_value(f32::from_le_bytes(input.fixed()?)),
            AppendKind::Float64 => out
                .float64()
                .append_value(f64::from_le_bytes(input.fixed()?)),
            AppendKind::String => return self.append_string(input, out),
            AppendKind::Call => return self.append_call(input, out),
            AppendKind::Struct => return self.append_struct(input, out),
            AppendKind::List => return self.append_list(input, out),
            AppendKind::Ndarray => return self.append_ndarray(input, out),
        }
        Ok(())
    }
    fn append_string(&self, input: &mut Input, out: &mut Column) -> Result<()> {
        let n = self.length(input)?;
        let bytes = input.bytes(n)?;
        out.string().append_value(std::str::from_utf8(&bytes)?);
        Ok(())
    }
    fn append_call(&self, input: &mut Input, out: &mut Column) -> Result<()> {
        self.append_call_value(self.integer(input, 32)? as u32, out)
    }
    fn append_call_value(&self, v: u32, out: &mut Column) -> Result<()> {
        if let Column::Call(c) = out {
            validate_packed_call(v)?;
            c.packed.push(v);
            c.valid.append_non_null();
        } else {
            let (first, second, ploidy, phased) = call_parts(v)?;
            // Generic Arrow builders are retained for record helpers and
            // differential tests, without guessing Call identity from names.
            let c = out.children();
            c[0].uint16().append_value(first);
            c[1].uint16().append_value(second);
            c[2].uint8().append_value(ploidy);
            c[3].bool_builder().append_value(phased);
            out.valid(true);
        }
        Ok(())
    }
    #[inline(never)]
    fn try_append_varint_run<const ALL_PRESENT: bool>(
        &self,
        start: usize,
        input: &mut Input,
        outputs: &mut [Column],
        bits: &Bitmap,
    ) -> Result<bool> {
        let first = &self.fields[start];
        let count = first.varint_run as usize;
        if first.run_requires_leb && !input.leb {
            return Ok(false);
        }
        if !ALL_PRESENT && first.run_wide_missingness {
            if self.fields[start..start + count]
                .iter()
                .any(|op| op.missing_bit != usize::MAX && absent(bits, op.missing_bit))
            {
                return Ok(false);
            }
        } else if !ALL_PRESENT && bits.low_word() & first.run_missing_mask != 0 {
            return Ok(false);
        }
        let Some(values) = input.try_small_varints(count)? else {
            return Ok(false);
        };
        let mut selected = first.run_output_mask;
        while selected != 0 {
            let offset = selected.trailing_zeros() as usize;
            selected &= selected - 1;
            let op = &self.fields[start + offset];
            let child = &self.children[start + offset];
            let value = values[offset];
            let output = &mut outputs[op.dest];
            match child.append_kind {
                AppendKind::Int32 => output.int32().append_value(i32::from(value)),
                AppendKind::Int64 => output.int64().append_value(i64::from(value)),
                AppendKind::Call => child.append_call_value(u32::from(value), output)?,
                _ => unreachable!("compiled adjacent varint fields"),
            }
        }
        Ok(true)
    }
    fn append_struct(&self, input: &mut Input, out: &mut Column) -> Result<()> {
        let bits = bitmap(input, self.optional_count)?;
        let Column::Struct(_, outputs, validity) = out else {
            unreachable!("compiled struct output")
        };
        if self
            .all_present_mask
            .is_some_and(|mask| bits.low_word() & mask == 0)
        {
            self.append_struct_fields::<true>(input, outputs, &bits)?;
        } else {
            self.append_struct_fields::<false>(input, outputs, &bits)?;
        }
        if matches!(self.struct_postlude, StructPostlude::Locus) {
            let Type::Locus { locus } = &self.ty else {
                unreachable!()
            };
            let c = &mut *outputs;
            let builder = c[1].string();
            let bytes = builder.values_slice();
            let offsets = builder.offsets_slice();
            let n = offsets.len();
            let contig =
                std::str::from_utf8(&bytes[offsets[n - 2] as usize..offsets[n - 1] as usize])?;
            let ordinal = locus.ordinal(contig).ok_or("invalid Hail locus contig")?;
            c[0].int32().append_value(ordinal);
        }
        if matches!(self.struct_postlude, StructPostlude::EmptyTuple) {
            outputs[0].bool_builder().append_value(true)
        }
        validity.append_non_null();

        Ok(())
    }

    // Keep the two small missingness variants outlined; do not duplicate the
    // struct decoder into every primitive/container call site.
    #[inline(never)]
    fn append_struct_fields<const ALL_PRESENT: bool>(
        &self,
        input: &mut Input,
        outputs: &mut [Column],
        bits: &Bitmap,
    ) -> Result<()> {
        let mut i = 0;
        while i < self.fields.len() {
            let op = &self.fields[i];
            if op.varint_run != 0
                && self.try_append_varint_run::<ALL_PRESENT>(i, input, outputs, bits)?
            {
                i += op.varint_run as usize;
                continue;
            }
            let c = &self.children[i];
            i += 1;
            let bit = op.missing_bit;
            let missing = !ALL_PRESENT && bit != usize::MAX && absent(bits, bit);
            if !op.project {
                if !missing {
                    c.skip(input)?
                }
                // The owning struct supplies skipped child length at
                // finish; no work per omitted field per entry.
                continue;
            }
            let target = &mut outputs[op.dest];
            if missing {
                target.null()?
            } else if op.invert_boolean {
                target.bool_builder().append_value(!boolean(input)?)
            } else {
                c.append(input, target)?
            }
        }
        Ok(())
    }

    fn bind_struct<'n, 'a>(&'n self, outputs: &'a mut [Column], leb: bool) -> Vec<BoundOp<'n, 'a>> {
        let mut destinations = outputs.iter_mut().map(Some).collect::<Vec<_>>();
        let mut fields = self
            .fields
            .iter()
            .zip(&self.children)
            .map(|(op, node)| {
                let output = if !op.project {
                    BoundOutput::Skip
                } else {
                    let destination = destinations[op.dest]
                        .take()
                        .expect("unique physical field destination");
                    if matches!(node.wire, Wire::Array)
                        && node.children[0].e.required
                        && matches!(node.children[0].wire, Wire::Varint)
                        && matches!(node.children[0].append_kind, AppendKind::Int32)
                    {
                        let Column::List(_, child, offsets, validity) = destination else {
                            unreachable!("required Int32 list output")
                        };
                        let mode = if node.varint_length {
                            2
                        } else if leb {
                            1
                        } else {
                            0
                        };
                        match child.as_mut() {
                            Column::EncodedI32(values) => BoundOutput::EncodedList(BoundList {
                                values,
                                offsets,
                                validity,
                                decode: [
                                    decode_encoded_list::<0>,
                                    decode_encoded_list::<1>,
                                    decode_encoded_list::<2>,
                                ][mode],
                            }),
                            Column::Int32(values) => BoundOutput::ArrowList(BoundList {
                                values,
                                offsets,
                                validity,
                                decode: [
                                    decode_arrow_list::<0>,
                                    decode_arrow_list::<1>,
                                    decode_arrow_list::<2>,
                                ][mode],
                            }),
                            _ => unreachable!("required Int32 list builder"),
                        }
                    } else {
                        match destination {
                            Column::Bool(b) => {
                                if op.invert_boolean {
                                    BoundOutput::InvertedBool(b)
                                } else {
                                    BoundOutput::Bool(b)
                                }
                            }
                            Column::Int32(b) => BoundOutput::I32(b),
                            Column::Int64(b) => BoundOutput::I64(b),
                            Column::Float32(b) => BoundOutput::F32(b),
                            Column::Float64(b) => BoundOutput::F64(b),
                            Column::Call(b) => BoundOutput::Call(b),
                            other => BoundOutput::Other(other),
                        }
                    }
                };
                BoundField {
                    node,
                    missing: op.missing_bit,
                    output,
                }
            })
            .collect::<Vec<_>>()
            .into_iter();
        let mut ops = Vec::new();
        let mut i = 0;
        while i < self.fields.len() {
            let op = &self.fields[i];
            if op.varint_run != 0 {
                let run = fields.by_ref().take(op.varint_run as usize).collect();
                ops.push(BoundOp::Run {
                    fields: run,
                    requires_leb: op.run_requires_leb,
                });
                i += op.varint_run as usize;
            } else {
                ops.push(BoundOp::Field(fields.next().unwrap()));
                i += 1;
            }
        }
        ops
    }
    fn append_struct_elements(
        &self,
        input: &mut Input,
        child: &mut Column,
        n: usize,
        element_bits: &Bitmap,
    ) -> Result<()> {
        if self.e.required {
            self.append_struct_elements_impl::<true>(input, child, n, element_bits)
        } else {
            self.append_struct_elements_impl::<false>(input, child, n, element_bits)
        }
    }
    fn append_struct_elements_impl<const REQUIRED: bool>(
        &self,
        input: &mut Input,
        child: &mut Column,
        n: usize,
        element_bits: &Bitmap,
    ) -> Result<()> {
        if !matches!(self.struct_postlude, StructPostlude::Plain) || !self.distinct_destinations {
            for i in 0..n {
                if !REQUIRED && absent(element_bits, i) {
                    child.null()?;
                } else {
                    self.append_struct(input, child)?;
                }
            }
            return Ok(());
        }
        let Column::Struct(_, outputs, validity) = child else {
            unreachable!("struct array output")
        };
        let mut ops = self.bind_struct(outputs, input.leb);
        let buffered = self.buffered_scalars[usize::from(input.leb)]
            .get_or_init(|| BufferedScalars::new(self, input.leb))
            .as_ref()
            .filter(|_| has_buffered_outputs(&ops));
        let mut i = 0;
        while i < n {
            // Leading absent entries need no input; preserve that property
            // even if refilling the underlying reader would fail.
            if !REQUIRED && absent(element_bits, i) {
                for op in &mut ops {
                    op.null()?;
                }
                validity.append_null();
                i += 1;
                continue;
            }

            if let Some(program) = &buffered {
                let accepted = input.with_buffer(|bytes| {
                    let mut selected_outputs = bind_buffered_outputs(&mut ops);
                    let mut cursor = 0;
                    let mut accepted = 0;
                    let mut values = [0u64; 16];
                    while i + accepted < n {
                        if !REQUIRED && absent(element_bits, i + accepted) {
                            buffered_scalar_row(&mut selected_outputs, &values, u16::MAX);
                            validity.append_null();
                        } else {
                            let start = cursor;
                            let Some(nulls) = program.probe(bytes, &mut cursor, &mut values) else {
                                cursor = start;
                                break;
                            };
                            buffered_scalar_row(&mut selected_outputs, &values, nulls);
                            validity.append_non_null();
                        }
                        accepted += 1;
                    }
                    (accepted, cursor)
                })?;
                if accepted != 0 {
                    i += accepted;
                    continue;
                }
            }
            // A partial/unsupported entry is retried from its original byte
            // boundary; only the generic decoder produces wire errors.
            if !REQUIRED && absent(element_bits, i) {
                for op in &mut ops {
                    op.null()?;
                }
                validity.append_null();
            } else {
                let bits = bitmap(input, self.optional_count)?;
                let all_present = self
                    .all_present_mask
                    .is_some_and(|mask| bits.low_word() & mask == 0);
                for op in &mut ops {
                    op.append(input, &bits, all_present)?;
                }
                validity.append_non_null();
            }
            i += 1;
        }
        Ok(())
    }

    fn append_list(&self, input: &mut Input, out: &mut Column) -> Result<()> {
        let n = self.length(input)?;
        let c = &self.children[0];
        let bits = if c.e.required {
            Bitmap::Inline([0; 8])
        } else {
            bitmap(input, n)?
        };
        let Column::List(_, child, _, _) = out else {
            unreachable!()
        };
        if let Column::EncodedI32(builder) = child.as_mut() {
            builder.capture(input, n)?;
            return out.end_list();
        }
        // Bind primitive builders once per array. AD/PL-style integer
        // arrays avoid recursive Node dispatch and builder selection
        // for every value while retaining element missingness.
        macro_rules! primitive_elements {
            ($builder:expr, $value:expr) => {{
                let builder = $builder;
                if c.e.required {
                    for _ in 0..n {
                        builder.append_value($value);
                    }
                } else {
                    for i in 0..n {
                        if absent(&bits, i) {
                            builder.append_null();
                        } else {
                            builder.append_value($value);
                        }
                    }
                }
            }};
        }
        match c.ty {
            Type::Primitive(P::Int32) => {
                primitive_elements!(child.int32(), c.integer(input, 32)? as i32)
            }
            Type::Primitive(P::Int64) => {
                primitive_elements!(child.int64(), c.integer(input, 64)?)
            }
            Type::Primitive(P::Float32) => {
                primitive_elements!(child.float32(), f32::from_le_bytes(input.fixed()?))
            }
            Type::Primitive(P::Float64) => {
                primitive_elements!(child.float64(), f64::from_le_bytes(input.fixed()?))
            }
            Type::Primitive(P::Bool) => {
                primitive_elements!(child.bool_builder(), boolean(input)?)
            }
            Type::Struct { .. }
            | Type::Tuple { .. }
            | Type::Locus { .. }
            | Type::Interval { .. } => {
                c.append_struct_elements(input, child, n, &bits)?;
            }
            _ => {
                for i in 0..n {
                    if !c.e.required && absent(&bits, i) {
                        child.null()?;
                    } else {
                        c.append(input, child)?;
                    }
                }
            }
        }
        out.end_list()?;

        Ok(())
    }

    fn append_ndarray(&self, input: &mut Input, out: &mut Column) -> Result<()> {
        let Type::Ndarray { ndarray } = &self.ty else {
            unreachable!("compiled ndarray operation")
        };

        let shape = (0..ndarray.1)
            .map(|_| Ok(usize::try_from(input.int(64)?)?))
            .collect::<Result<Vec<_>>>()?;
        let n = shape.iter().try_fold(1usize, |n, x| {
            n.checked_mul(*x).ok_or("Hail ndarray shape overflow")
        })?;
        // Arrow data is C order; Hail encodes Fortran order. Decode straight
        // into a typed temporary column, then permute this one ndarray.
        let mut values = Column::new(&ndarray.0.to_arrow()?);
        for _ in 0..n {
            self.children[0].append(input, &mut values)?
        }
        let array = values.finish()?;
        let c = out.children();
        let Column::List(_, data, _, _) = &mut c[0] else {
            unreachable!()
        };
        for row in 0..n {
            let mut rest = row;
            let mut source = 0;
            let mut stride = n;
            let mut fstride = 1;
            for &dim in &shape {
                stride /= dim;
                let coord = rest / stride;
                rest %= stride;
                source += coord * fstride;
                fstride *= dim;
            }
            append_array(data, &array, source)?;
        }
        c[0].end_list()?;
        let Column::List(_, dims, _, _) = &mut c[1] else {
            unreachable!()
        };
        for d in shape {
            dims.int64().append_value(d as i64)
        }
        c[1].end_list()?;
        out.valid(true);

        Ok(())
    }
}
fn append_array(out: &mut Column, array: &ArrayRef, i: usize) -> Result<()> {
    macro_rules! primitive{($($a:ty=>$b:ident),*)=>{$(if let Some(a)=array.as_any().downcast_ref::<$a>(){out.$b().append_value(a.value(i));return Ok(())})*}}
    primitive!(Int32Array=>int32,Int64Array=>int64,Float32Array=>float32,Float64Array=>float64,BooleanArray=>bool_builder);
    Err("unsupported Hail ndarray element type".into())
}
pub(super) fn record(input: &mut Input, e: &Encoding, ty: &Type) -> Result<ArrayRef> {
    let node = Node::new(e, ty);
    let mut column = Column::projected(&node, &ty.to_arrow()?);
    node.append(input, &mut column)?;
    column.finish()
}
pub struct PartReader {
    input: Input,
    node: Node,
    schema: SchemaRef,
    path: PathBuf,
    ended: bool,
    pub rows: u64,
    projection: Vec<usize>,
    decode_columns: Option<Vec<usize>>,
    end_row: Option<u64>,
    columns: Option<Vec<Column>>,
}
impl PartReader {
    pub fn new(c: &Component, part: usize, projection: &[usize]) -> Result<Self> {
        let path = c
            .paths
            .get(part)
            .ok_or("Hail part index out of bounds")?
            .clone();
        let mut node = Node::new(&c.encoding, &c.dtype);
        for field in &mut node.fields {
            field.project = projection.contains(&field.dest);
        }
        node.refresh_run_projection();
        Ok(Self {
            input: Input::open(&path, &c.buffer)?,
            node,
            schema: Arc::new(c.schema.project(projection)?),
            path,
            ended: false,
            rows: 0,
            projection: projection.to_vec(),
            decode_columns: None,
            end_row: None,
            columns: None,
        })
    }
    pub fn seek(&mut self, component: &Component, row: u64, virtual_offset: u64) -> Result<()> {
        self.input = Input::open_virtual(&self.path, &component.buffer, virtual_offset)?;
        self.rows = row;
        self.ended = false;
        Ok(())
    }
    pub fn with_end_row(mut self, end: u64) -> Self {
        self.end_row = Some(end);
        self
    }
    pub fn with_decode_columns(mut self, columns: Option<Vec<usize>>) -> Self {
        self.decode_columns = columns;
        self
    }
    pub fn with_decode_paths(mut self, paths: Option<Vec<Vec<String>>>) -> Self {
        if let Some(paths) = paths {
            self.node.demand(&paths);
        }
        self
    }
    pub fn next_batch(&mut self, size: usize) -> Result<Option<RecordBatch>> {
        self.read_batch(size)
            .map_err(|e| format!("cannot read Hail partition {}: {e}", self.path.display()).into())
    }
    pub fn next_vortex_batch(
        &mut self,
        size: usize,
        session: &vortex::session::VortexSession,
    ) -> Result<Option<vortex::array::ArrayRef>> {
        self.read_vortex_batch(size, session)
            .map_err(|e| format!("cannot read Hail partition {}: {e}", self.path.display()).into())
    }
    fn read_vortex_batch(
        &mut self,
        size: usize,
        session: &vortex::session::VortexSession,
    ) -> Result<Option<vortex::array::ArrayRef>> {
        use vortex::{
            array::{IntoArray, validity::Validity},
            arrow::ArrowSessionExt,
        };
        let Some((n, mut columns)) = self.read_columns(size)? else {
            return Ok(None);
        };
        let arrays = columns
            .iter_mut()
            .zip(self.schema.fields())
            .map(|(column, field)| column.finish_vortex(field, session))
            .collect::<Result<Vec<_>>>()?;
        self.columns = Some(columns);
        Ok(Some(
            vortex::array::arrays::StructArray::try_new_with_dtype(
                arrays,
                session.arrow().from_arrow_fields(self.schema.fields())?,
                n,
                Validity::NonNullable,
            )?
            .into_array(),
        ))
    }
    fn read_batch(&mut self, size: usize) -> Result<Option<RecordBatch>> {
        let Some((n, mut columns)) = self.read_columns(size)? else {
            return Ok(None);
        };
        let arrays = columns
            .iter_mut()
            .map(Column::finish)
            .collect::<Result<_>>()?;
        self.columns = Some(columns);
        Ok(Some(RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )?))
    }
    fn read_columns(&mut self, size: usize) -> Result<Option<(usize, Vec<Column>)>> {
        if self.ended {
            return Ok(None);
        }
        let mut columns = self.columns.take().unwrap_or_else(|| {
            self.schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let physical = self
                        .node
                        .fields
                        .iter()
                        .position(|field| field.dest == self.projection[i])
                        .unwrap();
                    if !self.node.fields[physical].project {
                        Column::Skipped(f.data_type().clone(), 0, None)
                    } else {
                        Column::projected(&self.node.children[physical], f.data_type())
                    }
                })
                .collect::<Vec<_>>()
        });
        let mut n = 0;
        let start_bytes = self.input.bytes;
        while n < size.max(1)
            && self.end_row.is_none_or(|end| self.rows < end)
            && (n == 0 || self.input.bytes - start_bytes < 8 * 1024 * 1024)
        {
            match self.input.byte()? {
                0 => {
                    self.input.finish()?;
                    self.ended = true;
                    break;
                }
                1 => {}
                _ => return Err("invalid Hail row marker".into()),
            }
            let bits = bitmap(&mut self.input, self.node.optional_count)?;
            for (op, c) in self.node.fields.iter().zip(&self.node.children) {
                let missing = op.missing_bit != usize::MAX && absent(&bits, op.missing_bit);
                if let Some(dest) = self.projection.iter().position(|p| *p == op.dest) {
                    if missing {
                        columns[dest].null()?
                    } else if !op.project
                        || self
                            .decode_columns
                            .as_ref()
                            .is_some_and(|columns| !columns.contains(&op.dest))
                    {
                        c.skip(&mut self.input)?;
                        columns[dest].null()?;
                    } else {
                        c.append(&mut self.input, &mut columns[dest])?
                    }
                } else if !missing {
                    c.skip(&mut self.input)?
                }
            }
            n += 1;
            self.rows += 1;
        }
        if n == 0 {
            return Ok(None);
        }
        Ok(Some((n, columns)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_required_varint_lists_match_generic_all_length_modes_and_nullable_fallback()
    -> Result<()> {
        fn leb(mut value: u64, out: &mut Vec<u8>) {
            loop {
                let byte = (value & 127) as u8;
                value >>= 7;
                out.push(byte | if value == 0 { 0 } else { 128 });
                if value == 0 {
                    break;
                }
            }
        }
        for modern in [false, true] {
            for compressed in [false, true] {
                for required in [false, true] {
                    let integer = |required| Encoding {
                        kind: "EVarint".into(),
                        required,
                        fields: vec![],
                        rank: 0,
                    };
                    let list = Encoding {
                        kind: if modern { "EArray2" } else { "EArray" }.into(),
                        required: false,
                        fields: vec![("element".into(), integer(required))],
                        rank: 0,
                    };
                    let encoding = Encoding {
                        kind: "EArray2".into(),
                        required: true,
                        fields: vec![(
                            "element".into(),
                            Encoding {
                                kind: "EBaseStruct".into(),
                                required: true,
                                fields: vec![
                                    ("scalar".into(), integer(true)),
                                    ("array".into(), list),
                                ],
                                rank: 0,
                            },
                        )],
                        rank: 0,
                    };
                    let ty = Type::Array {
                        array: Box::new(Type::Struct {
                            r#struct: vec![
                                (
                                    "array".into(),
                                    Type::Array {
                                        array: Box::new(Type::Primitive(P::Int32)),
                                    },
                                ),
                                ("scalar".into(), Type::Primitive(P::Int32)),
                            ],
                        }),
                    };
                    let node = Node::new(&encoding, &ty);
                    let mut bytes = vec![3, 0, 6];
                    if modern || compressed {
                        bytes.push(2);
                    } else {
                        bytes.extend_from_slice(&2i32.to_le_bytes());
                    }
                    if !required {
                        bytes.push(2);
                    }
                    leb(u32::MAX as u64, &mut bytes);
                    if required {
                        leb(300, &mut bytes);
                    }
                    bytes.extend_from_slice(&[1, 7, 0, 8]);
                    if modern || compressed {
                        bytes.push(0);
                    } else {
                        bytes.extend_from_slice(&0i32.to_le_bytes());
                    }
                    for retained in [false, true] {
                        for truncated in [false, true] {
                            let data: Arc<[u8]> =
                                bytes[..bytes.len() - usize::from(truncated)].into();
                            let mut left = Input::from_memory(data.clone(), compressed);
                            let mut right = Input::from_memory(data, compressed);
                            let mut expected = Column::new(&ty.to_arrow()?);
                            let mut actual = if retained {
                                Column::projected_encoding(&node, &ty.to_arrow()?, true)
                            } else {
                                Column::new(&ty.to_arrow()?)
                            };
                            let generic =
                                append_unbound_struct_array(&node, &mut left, &mut expected);
                            let bound = node.append(&mut right, &mut actual);
                            assert_eq!(left.bytes, right.bytes);
                            if truncated {
                                assert_eq!(
                                    generic.unwrap_err().to_string(),
                                    bound.unwrap_err().to_string()
                                );
                            } else {
                                generic?;
                                bound?;
                                assert_eq!(expected.finish()?.as_ref(), actual.finish()?.as_ref());
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn buffered_selected_bindings_preserve_physical_order_and_null_slots() -> Result<()> {
        let fields = ["x", "y", "z"]
            .into_iter()
            .map(|name| (name.into(), Type::Primitive(P::Int32)))
            .collect();
        let ty = Type::Struct { r#struct: fields };
        let encoding = Encoding {
            kind: "EBaseStruct".into(),
            required: true,
            fields: ["y", "z", "x"]
                .into_iter()
                .map(|name| {
                    (
                        name.into(),
                        Encoding {
                            kind: "EInt32".into(),
                            required: false,
                            fields: vec![],
                            rank: 0,
                        },
                    )
                })
                .collect(),
            rank: 0,
        };
        let mut node = Node::new(&encoding, &ty);
        node.demand(&[vec!["x".into()], vec!["z".into()]]);
        let mut column = Column::projected(&node, &ty.to_arrow()?);
        {
            let Column::Struct(_, outputs, validity) = &mut column else {
                unreachable!()
            };
            let mut ops = node.bind_struct(outputs, true);
            let mut selected = bind_buffered_outputs(&mut ops);
            assert_eq!(selected.len(), 2);
            assert!(!selected.spilled());
            let mut values = [0; 16];
            values[0] = 5;
            values[1] = 9;
            buffered_scalar_row(&mut selected, &values, 2);
            validity.append_non_null();
            values[0] = 12;
            values[1] = 17;
            buffered_scalar_row(&mut selected, &values, 1);
            validity.append_non_null();
        }
        let array = column.finish()?;
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![None, Some(17)])
        );
        assert_eq!(
            array
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(5), None])
        );
        Ok(())
    }

    #[test]
    fn slice_varint_skip_matches_scalar_at_word_and_buffer_boundaries() {
        fn reference(bytes: &[u8], start: usize, mut n: usize) -> Option<usize> {
            if n == 0 {
                return Some(start);
            }
            for (i, byte) in bytes.iter().enumerate().skip(start) {
                if byte & 128 == 0 {
                    n -= 1;
                    if n == 0 {
                        return Some(i + 1);
                    }
                }
            }
            None
        }
        for pattern in 0..256u16 {
            for len in 0..=72 {
                for start in 0..=8 {
                    let mut bytes = vec![255; start];
                    bytes.extend((0..len).map(|i| {
                        (i % 127) as u8
                            | if pattern & (1 << (i % 8)) != 0 {
                                128
                            } else {
                                0
                            }
                    }));
                    for n in 0..=9 {
                        let expected = reference(&bytes, start, n);
                        let mut cursor = start;
                        let actual = slice_skip_varints(&bytes, &mut cursor, n);
                        assert_eq!(
                            actual.is_some(),
                            expected.is_some(),
                            "pattern={pattern} len={len} start={start} n={n}"
                        );
                        assert_eq!(cursor, expected.unwrap_or(start));
                    }
                }
            }
        }
        // Skipped values are framed by terminators, not numeric width. Include
        // an overwide payload crossing both8-byte and64-byte boundaries.
        let mut bytes = vec![128; 129];
        bytes.push(1);
        bytes.extend_from_slice(&[2; 72]);
        for n in 0..=74 {
            let expected = reference(&bytes, 0, n);
            let mut cursor = 0;
            assert_eq!(
                slice_skip_varints(&bytes, &mut cursor, n).is_some(),
                expected.is_some()
            );
            assert_eq!(cursor, expected.unwrap_or(0));
        }
        for end in [7, 8, 9, 63, 64, 65, 128, 129] {
            let mut cursor = 0;
            assert!(slice_skip_varints(&bytes[..end], &mut cursor, 1).is_none());
            assert_eq!(cursor, 0);
        }
    }

    #[test]
    fn required_varint_i32_lists_match_arrow_and_keep_vortex_export_lazy() -> Result<()> {
        use vortex::{VortexSessionDefault, array::VortexSessionExecute, arrow::ArrowSessionExt};
        fn leb(mut value: u64, out: &mut Vec<u8>) {
            loop {
                let byte = (value & 127) as u8;
                value >>= 7;
                out.push(byte | if value == 0 { 0 } else { 128 });
                if value == 0 {
                    break;
                }
            }
        }
        let ty = Type::Array {
            array: Box::new(Type::Primitive(P::Int32)),
        };
        let encoding = Encoding {
            kind: "EArray2".into(),
            required: false,
            fields: vec![(
                "element".into(),
                Encoding {
                    kind: "EVarint".into(),
                    required: true,
                    fields: vec![],
                    rank: 0,
                },
            )],
            rank: 0,
        };
        let node = Node::new(&encoding, &ty);
        let datatype = ty.to_arrow()?;
        let field = arrow_schema::Field::new("values", datatype.clone(), true);
        let session = vortex::session::VortexSession::default();
        let mut encoded = Column::projected_encoding(&node, &datatype, true);
        let mut generic = Column::new(&datatype);
        for values in [
            vec![-1i32, 0, 127, 128, i32::MIN, i32::MAX],
            vec![],
            vec![3, 4],
        ] {
            let mut bytes = vec![values.len() as u8];
            for value in values {
                leb(value as u32 as u64, &mut bytes);
            }
            let bytes: Arc<[u8]> = bytes.into();
            node.append(&mut Input::from_memory(bytes.clone(), false), &mut encoded)?;
            node.append(&mut Input::from_memory(bytes.clone(), false), &mut generic)?;
            encoded.null()?;
            generic.null()?;
            assert_eq!(encoded.finish()?.as_ref(), generic.finish()?.as_ref());
            let mut vortex = Column::projected_encoding(&node, &datatype, true);
            node.append(&mut Input::from_memory(bytes, false), &mut vortex)?;
            vortex.null()?;
            let array = vortex.finish_vortex(&field, &session)?;
            assert_eq!(array.dtype(), &session.arrow().from_arrow_field(&field)?);
            let mut ctx = session.create_execution_ctx();
            let arrow = super::super::packed_call::export(array, Some(&field), &mut ctx)?;
            let list = arrow.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list.null_count(), 1);
            assert_eq!(list.values().null_count(), 0);
            assert!(format!("{:?}", list.values()).contains("materialized: false"));
            // Physical access materializes the unchanged standard Arrow Int32.
            assert_eq!(
                list.values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .len(),
                list.value_length(0) as usize
            );
        }
        // Split a continuation across the normal64KiB Input buffer, retaining
        // exact framing and leaving the following field byte untouched.
        for truncated in [false, true] {
            let prefix = 65534;
            let mut bytes = vec![0; prefix];
            bytes.extend_from_slice(&[1, 255, 255, 255, 255, 15]);
            if truncated {
                bytes.pop();
            } else {
                bytes.push(99);
            }
            let bytes: Arc<[u8]> = bytes.into();
            let mut left = Input::from_memory(bytes.clone(), false);
            let mut right = Input::from_memory(bytes, false);
            left.skip(prefix)?;
            right.skip(prefix)?;
            let mut eager = Column::new(&datatype);
            let mut retained = Column::projected_encoding(&node, &datatype, true);
            let eager_result = node.append(&mut left, &mut eager);
            let retained_result = node.append(&mut right, &mut retained);
            assert_eq!(left.bytes, right.bytes);
            if truncated {
                assert_eq!(
                    eager_result.unwrap_err().to_string(),
                    retained_result.unwrap_err().to_string()
                );
            } else {
                eager_result?;
                retained_result?;
                assert_eq!(eager.finish()?.as_ref(), retained.finish()?.as_ref());
                assert_eq!(right.byte()?, 99);
            }
        }
        let mut nullable = encoding.clone();
        nullable.fields[0].1.required = false;
        let nullable_node = Node::new(&nullable, &ty);
        let mut nullable = Column::projected(&nullable_node, &datatype);
        nullable_node.append(
            &mut Input::from_memory(Arc::from([3u8, 2, 5, 7]), false),
            &mut nullable,
        )?;
        let values = nullable.finish()?;
        let list = values.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            list.values().as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(5), None, Some(7)])
        );
        let Column::List(_, child, _, _) = nullable else {
            unreachable!()
        };
        assert!(matches!(*child, Column::Int32(_)));
        assert!(matches!(
            Column::projected(&node.children[0], &DataType::Int32),
            Column::Int32(_)
        ));
        // Framing-only retained leaves intentionally keep low32 bits for an
        // oversized numeric payload; ordinary scalar decoders remain strict.
        let mut bytes = vec![1];
        leb(1u64 << 32, &mut bytes);
        node.append(&mut Input::from_memory(bytes.into(), false), &mut encoded)?;
        let array = encoded.finish()?;
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            0
        );
        Ok(())
    }

    #[test]
    fn buffered_program_cache_is_per_codec_and_resets_after_demand() -> Result<()> {
        let integer = Encoding {
            kind: "EInt32".into(),
            required: false,
            fields: vec![],
            rank: 0,
        };
        let array = Encoding {
            kind: "EArray2".into(),
            required: false,
            fields: vec![(
                "element".into(),
                Encoding {
                    required: true,
                    ..integer.clone()
                },
            )],
            rank: 0,
        };
        let encoding = Encoding {
            kind: "EArray2".into(),
            required: true,
            fields: vec![(
                "element".into(),
                Encoding {
                    kind: "EBaseStruct".into(),
                    required: true,
                    fields: vec![("x".into(), integer), ("unused".into(), array)],
                    rank: 0,
                },
            )],
            rank: 0,
        };
        let ty = Type::Array {
            array: Box::new(Type::Struct {
                r#struct: vec![
                    ("x".into(), Type::Primitive(P::Int32)),
                    (
                        "unused".into(),
                        Type::Array {
                            array: Box::new(Type::Primitive(P::Int32)),
                        },
                    ),
                ],
            }),
        };
        let mut node = Node::new(&encoding, &ty);
        for projected in [false, true] {
            if projected {
                node.demand(&[vec!["x".into()]]);
                assert!(
                    node.children[0]
                        .buffered_scalars
                        .iter()
                        .all(|cached| cached.get().is_none())
                );
            }
            for leb in [false, true] {
                let mut bytes = vec![2];
                for x in [7i32, 8] {
                    bytes.push(0);
                    if leb {
                        bytes.push(x as u8);
                    } else {
                        bytes.extend_from_slice(&x.to_le_bytes());
                    }
                    bytes.push(2);
                    for value in [3i32, 4] {
                        if leb {
                            bytes.push(value as u8);
                        } else {
                            bytes.extend_from_slice(&value.to_le_bytes());
                        }
                    }
                }
                let bytes: Arc<[u8]> = bytes.into();
                let mut expected = Column::projected(&node, &ty.to_arrow()?);
                let mut actual = Column::projected(&node, &ty.to_arrow()?);
                append_unbound_struct_array(
                    &node,
                    &mut Input::from_memory(bytes.clone(), leb),
                    &mut expected,
                )?;
                node.append(&mut Input::from_memory(bytes, leb), &mut actual)?;
                assert_eq!(expected.finish()?.as_ref(), actual.finish()?.as_ref());
                assert_eq!(
                    node.children[0].buffered_scalars[usize::from(leb)]
                        .get()
                        .unwrap()
                        .is_some(),
                    projected && leb
                );
            }
        }
        Ok(())
    }

    fn append_unbound_struct_array(
        node: &Node,
        input: &mut Input,
        output: &mut Column,
    ) -> Result<()> {
        let n = node.length(input)?;
        let element = &node.children[0];
        let bits = if element.e.required {
            Bitmap::Inline([0; 8])
        } else {
            bitmap(input, n)?
        };
        let Column::List(_, child, _, _) = output else {
            unreachable!()
        };
        for i in 0..n {
            if !element.e.required && absent(&bits, i) {
                child.null()?;
            } else {
                element.append_struct(input, child)?;
            }
        }
        output.end_list()
    }

    fn compare_buffered_array(node: &Node, bytes: Arc<[u8]>, prefix: usize) -> Result<()> {
        fn lengths(column: &Column, out: &mut Vec<usize>) {
            // Skipped placeholders inherit length at finish, not per element.
            if matches!(column, Column::Skipped(..)) {
                return;
            }
            out.push(column.len());
            match column {
                Column::Struct(_, children, _) => {
                    for child in children {
                        lengths(child, out);
                    }
                }
                Column::List(_, child, _, _) => lengths(child, out),
                _ => {}
            }
        }
        let mut generic = Input::from_memory(bytes.clone(), true);
        let mut buffered = Input::from_memory(bytes, true);
        generic.skip(prefix)?;
        buffered.skip(prefix)?;
        let arrow = node.ty.to_arrow()?;
        let mut expected = Column::projected(node, &arrow);
        let mut actual = Column::projected(node, &arrow);
        let left = append_unbound_struct_array(node, &mut generic, &mut expected);
        let right = node.append(&mut buffered, &mut actual);
        assert_eq!(left.is_ok(), right.is_ok());
        assert_eq!(generic.bytes, buffered.bytes);
        let mut a = Vec::new();
        let mut b = Vec::new();
        lengths(&expected, &mut a);
        lengths(&actual, &mut b);
        assert_eq!(a, b);
        match (left, right) {
            (Ok(()), Ok(())) => assert_eq!(expected.finish()?.as_ref(), actual.finish()?.as_ref()),
            (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
            _ => unreachable!(),
        }
        Ok(())
    }
    #[test]
    fn buffered_scalars_match_generic_boundaries_missing_continuations_and_errors() -> Result<()> {
        fn leb(mut value: u64, out: &mut Vec<u8>) {
            loop {
                let byte = (value & 127) as u8;
                value >>= 7;
                out.push(byte | if value == 0 { 0 } else { 128 });
                if value == 0 {
                    break;
                }
            }
        }
        let integer = || Encoding {
            kind: "EInt32".into(),
            required: false,
            fields: vec![],
            rank: 0,
        };
        let array = Encoding {
            kind: "EArray2".into(),
            required: false,
            fields: vec![(
                "element".into(),
                Encoding {
                    required: true,
                    ..integer()
                },
            )],
            rank: 0,
        };
        let encoding = Encoding {
            kind: "EArray2".into(),
            required: true,
            fields: vec![(
                "element".into(),
                Encoding {
                    kind: "EBaseStruct".into(),
                    required: false,
                    fields: vec![
                        ("unused".into(), array),
                        (
                            "flag".into(),
                            Encoding {
                                kind: "EBoolean".into(),
                                ..integer()
                            },
                        ),
                        ("call".into(), integer()),
                        ("value".into(), integer()),
                        (
                            "float".into(),
                            Encoding {
                                kind: "EFloat64".into(),
                                ..integer()
                            },
                        ),
                    ],
                    rank: 0,
                },
            )],
            rank: 0,
        };
        let ty = Type::Array {
            array: Box::new(Type::Struct {
                r#struct: vec![
                    ("value".into(), Type::Primitive(P::Int32)),
                    ("float".into(), Type::Primitive(P::Float64)),
                    ("call".into(), Type::Primitive(P::Call)),
                    ("flag".into(), Type::Primitive(P::Bool)),
                    (
                        "unused".into(),
                        Type::Array {
                            array: Box::new(Type::Primitive(P::Int32)),
                        },
                    ),
                ],
            }),
        };
        for error in [0, 1, 2] {
            let n = if error == 0 { 8192usize } else { 128 };
            let mut bytes = Vec::new();
            leb(n as u64, &mut bytes);
            let bitmap_start = bytes.len();
            bytes.resize(bytes.len() + n.div_ceil(8), 0);
            for row in 0..n {
                if row % 17 == 0 {
                    bytes[bitmap_start + row / 8] |= 1 << (row % 8);
                    continue;
                }
                let last = row == n - 1;
                let missing = if last {
                    0
                } else {
                    u8::from(row % 11 == 0)
                        | u8::from(row % 23 == 0) << 1
                        | u8::from(row % 7 == 0) << 2
                        | u8::from(row % 5 == 0) << 3
                        | u8::from(row % 13 == 0) << 4
                };
                bytes.push(missing | 0xe0);
                if missing & 1 == 0 {
                    let count = if row == 1 { 127 } else { 3 };
                    bytes.push(count);
                    for _ in 0..count {
                        if row == 1 {
                            bytes.extend_from_slice(&[128; 22]);
                        }
                        bytes.extend_from_slice(&[128, 1]);
                    }
                }
                if missing & 2 == 0 {
                    bytes.push(if error == 2 && last { 2 } else { 1 });
                }
                if missing & 4 == 0 {
                    bytes.push(if error == 1 && last { 6 } else { 12 });
                }
                if missing & 8 == 0 {
                    leb(
                        if row % 29 == 0 {
                            300
                        } else {
                            (row % 100) as u64
                        },
                        &mut bytes,
                    );
                }
                if missing & 16 == 0 {
                    bytes.extend_from_slice(&(f64::NAN.to_bits() | row as u64).to_le_bytes());
                }
            }
            for selected in [
                vec![vec!["value".into()], vec!["float".into()]],
                vec![vec!["call".into()], vec!["flag".into()]],
            ] {
                let mut node = Node::new(&encoding, &ty);
                node.demand(&selected);
                for prefix in [0usize, 1, 65520, 65535] {
                    let mut padded = vec![0; prefix];
                    padded.extend_from_slice(&bytes);
                    compare_buffered_array(&node, padded.into(), prefix)?;
                }
                for removed in [1, 2, 7, 17, bytes.len() - 1] {
                    compare_buffered_array(&node, bytes[..bytes.len() - removed].into(), 0)?;
                }
            }
        }
        Ok(())
    }
    #[test]
    fn buffered_scalars_support_high_missing_bit_and_zero_byte_null_entries() -> Result<()> {
        let integer = Encoding {
            kind: "EInt32".into(),
            required: false,
            fields: vec![],
            rank: 0,
        };
        let fields = (0..64)
            .map(|i| (format!("x{i}"), Type::Primitive(P::Int32)))
            .collect();
        let ty = Type::Array {
            array: Box::new(Type::Struct { r#struct: fields }),
        };
        let encoding = Encoding {
            kind: "EArray2".into(),
            required: true,
            fields: vec![(
                "element".into(),
                Encoding {
                    kind: "EBaseStruct".into(),
                    required: false,
                    fields: (0..64)
                        .map(|i| (format!("x{i}"), integer.clone()))
                        .collect(),
                    rank: 0,
                },
            )],
            rank: 0,
        };
        let mut node = Node::new(&encoding, &ty);
        node.demand(&[vec!["x63".into()]]);
        let mut bytes = vec![3, 4];
        bytes.extend_from_slice(&[0; 8]);
        bytes.extend_from_slice(&[1; 64]);
        bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 128]);
        bytes.extend_from_slice(&[2; 63]);
        compare_buffered_array(&node, bytes.into(), 0)?;
        compare_buffered_array(&node, Arc::from([3u8, 7]), 0)?;
        Ok(())
    }

    #[test]
    fn bound_struct_arrays_match_generic_high_masks_nested_calls_and_reordering() -> Result<()> {
        let path =
            std::env::temp_dir().join(format!("vail-bound-struct-{}.bin", std::process::id()));
        let result = (|| -> Result<()> {
            let mut fields = (0..67)
                .map(|i| (format!("x{i}"), Type::Primitive(P::Int32)))
                .collect::<Vec<_>>();
            fields.push(("call".into(), Type::Primitive(P::Call)));
            fields.push((
                "array".into(),
                Type::Array {
                    array: Box::new(Type::Primitive(P::Int32)),
                },
            ));
            let ty = Type::Array {
                array: Box::new(Type::Struct {
                    r#struct: fields.clone(),
                }),
            };
            let scalar = |required| Encoding {
                kind: "EInt32".into(),
                required,
                fields: vec![],
                rank: 0,
            };
            let physical = fields
                .iter()
                .rev()
                .map(|(name, ty)| {
                    (
                        name.clone(),
                        if matches!(ty, Type::Array { .. }) {
                            Encoding {
                                kind: "EArray2".into(),
                                required: false,
                                fields: vec![("element".into(), scalar(true))],
                                rank: 0,
                            }
                        } else {
                            scalar(false)
                        },
                    )
                })
                .collect::<Vec<_>>();
            let encoding = Encoding {
                kind: "EArray2".into(),
                required: true,
                fields: vec![(
                    "element".into(),
                    Encoding {
                        kind: "EBaseStruct".into(),
                        required: false,
                        fields: physical,
                        rank: 0,
                    },
                )],
                rank: 0,
            };
            let mut bytes = vec![4, 8];
            for row in 0..3 {
                let mut missing = vec![0u8; 9];
                missing[8] = 0xe0; // unused bitmap padding
                if row == 1 {
                    missing[8] |= 1;
                } // physical bit64
                if row == 2 {
                    missing[0] |= 3;
                } // absent nested array and Call
                bytes.extend_from_slice(&missing);
                for i in 0..69 {
                    if missing[i / 8] & (1 << (i % 8)) != 0 {
                        continue;
                    }
                    match i {
                        0 => bytes.extend_from_slice(&[2, 1, 2]),
                        1 => bytes.push(12),
                        _ => bytes.push(i as u8 + 1),
                    }
                }
            }
            std::fs::write(&path, bytes)?;
            for projection in [
                None,
                Some(vec![vec!["x0".into()]]),
                Some(vec![vec!["call".into()], vec!["array".into()]]),
            ] {
                let mut node = Node::new(&encoding, &ty);
                if let Some(paths) = projection {
                    node.demand(&paths);
                }
                let mut expected = Column::projected(&node, &ty.to_arrow()?);
                let mut actual = Column::projected(&node, &ty.to_arrow()?);
                let spec = serde_json::json!({"name":"LEB128BufferSpec", "child":{"name":"StreamBufferSpec"}});
                append_unbound_struct_array(&node, &mut Input::open(&path, &spec)?, &mut expected)?;
                node.append(&mut Input::open(&path, &spec)?, &mut actual)?;
                assert_eq!(expected.finish()?.as_ref(), actual.finish()?.as_ref());
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn dense_struct_array_matches_missing_aware_loop_with_projection() -> Result<()> {
        let path =
            std::env::temp_dir().join(format!("vail-dense-struct-{}.bin", std::process::id()));
        let result = (|| -> Result<()> {
            for required_fields in [false, true] {
                for required_elements in [false, true] {
                    for projection in [None, Some("x"), Some("flag")] {
                        let fields = vec![
                            ("x".into(), Type::Primitive(P::Int32)),
                            ("y".into(), Type::Primitive(P::Int64)),
                            ("flag".into(), Type::Primitive(P::Bool)),
                        ];
                        let ty = Type::Array {
                            array: Box::new(Type::Struct { r#struct: fields }),
                        };
                        let scalar = |kind: &str| Encoding {
                            kind: kind.into(),
                            required: required_fields,
                            fields: vec![],
                            rank: 0,
                        };
                        let encoding = Encoding {
                            kind: "EArray2".into(),
                            required: true,
                            fields: vec![(
                                "element".into(),
                                Encoding {
                                    kind: "EBaseStruct".into(),
                                    required: required_elements,
                                    fields: vec![
                                        ("y".into(), scalar("EInt64")),
                                        ("x".into(), scalar("EInt32")),
                                        ("flag".into(), scalar("EBoolean")),
                                    ],
                                    rank: 0,
                                },
                            )],
                            rank: 0,
                        };
                        let mut fast = Node::new(&encoding, &ty);
                        if let Some(field) = projection {
                            fast.demand(&[vec![field.into()]]);
                        }
                        let mut slow = Node::new(&encoding, &ty);
                        if let Some(field) = projection {
                            slow.demand(&[vec![field.into()]]);
                        }
                        slow.children[0].all_present_mask = None;
                        let mut bytes = vec![if required_elements { 3 } else { 4 }];
                        if !required_elements {
                            bytes.push(8);
                        }
                        for row in 0..3 {
                            let missing = if required_fields { 0 } else { [0, 2, 4][row] };
                            // High padding bits must not reject the dense path.
                            if !required_fields {
                                bytes.push(0xf8 | missing);
                            }
                            bytes.push(13 + row as u8);
                            if missing & 2 == 0 {
                                bytes.push(7 + row as u8);
                            }
                            if missing & 4 == 0 {
                                bytes.push(1);
                            }
                        }
                        std::fs::write(&path, bytes)?;
                        let spec = serde_json::json!({"name":"LEB128BufferSpec", "child":{"name":"StreamBufferSpec"}});
                        let mut expected = Column::projected(&slow, &ty.to_arrow()?);
                        let mut actual = Column::projected(&fast, &ty.to_arrow()?);
                        append_unbound_struct_array(
                            &slow,
                            &mut Input::open(&path, &spec)?,
                            &mut expected,
                        )?;
                        fast.append(&mut Input::open(&path, &spec)?, &mut actual)?;
                        assert_eq!(
                            expected.finish()?.as_ref(),
                            actual.finish()?.as_ref(),
                            "required fields {required_fields}, elements {required_elements}, projection {projection:?}"
                        );
                    }
                }
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn adjacent_varint_run_masks_handle_high_and_wide_missingness() -> Result<()> {
        let path =
            std::env::temp_dir().join(format!("vail-run-missing-masks-{}.bin", std::process::id()));
        let result = (|| -> Result<()> {
            for preceding in [62usize, 65] {
                let mut types = (0..preceding)
                    .map(|i| (format!("b{i}"), Type::Primitive(P::Bool)))
                    .collect::<Vec<_>>();
                types.extend([
                    ("x".into(), Type::Primitive(P::Int32)),
                    ("y".into(), Type::Primitive(P::Int32)),
                ]);
                let ty = Type::Struct { r#struct: types };
                let mut fields = (0..preceding)
                    .map(|i| {
                        (
                            format!("b{i}"),
                            Encoding {
                                kind: "EBoolean".into(),
                                required: false,
                                fields: vec![],
                                rank: 0,
                            },
                        )
                    })
                    .collect::<Vec<_>>();
                fields.extend(["x", "y"].into_iter().map(|name| {
                    (
                        name.into(),
                        Encoding {
                            kind: "EVarint".into(),
                            required: false,
                            fields: vec![],
                            rank: 0,
                        },
                    )
                }));
                let encoding = Encoding {
                    kind: "EBaseStruct".into(),
                    required: true,
                    fields,
                    rank: 0,
                };
                for missing in 0..4u8 {
                    let mut fast = Node::new(&encoding, &ty);
                    fast.demand(&[vec!["x".into()]]);
                    let mut slow = Node::new(&encoding, &ty);
                    slow.demand(&[vec!["x".into()]]);
                    for field in &mut slow.fields {
                        field.varint_run = 0;
                    }
                    assert_eq!(fast.fields[preceding].run_wide_missingness, preceding >= 64);
                    let mut bytes = vec![255u8; (preceding + 2).div_ceil(8)];
                    for offset in 0..2 {
                        if missing & (1 << offset) == 0 {
                            let bit = preceding + offset;
                            bytes[bit / 8] &= !(1 << (bit % 8));
                        }
                    }
                    if missing & 1 == 0 {
                        bytes.push(10);
                    }
                    if missing & 2 == 0 {
                        bytes.push(20);
                    }
                    std::fs::write(&path, bytes)?;
                    let spec = serde_json::json!({"name":"StreamBufferSpec"});
                    let mut fast_column = Column::projected(&fast, &ty.to_arrow()?);
                    let mut slow_column = Column::projected(&slow, &ty.to_arrow()?);
                    fast.append(&mut Input::open(&path, &spec)?, &mut fast_column)?;
                    slow.append(&mut Input::open(&path, &spec)?, &mut slow_column)?;
                    assert_eq!(
                        fast_column.finish()?.as_ref(),
                        slow_column.finish()?.as_ref()
                    );
                }
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn adjacent_varint_runs_match_scalar_fields_with_projection_and_missingness() -> Result<()> {
        fn leb(mut value: u64, bytes: &mut Vec<u8>) {
            loop {
                let byte = (value & 127) as u8;
                value >>= 7;
                bytes.push(byte | if value != 0 { 128 } else { 0 });
                if value == 0 {
                    break;
                }
            }
        }
        let ty = Type::Struct {
            r#struct: vec![
                ("a".into(), Type::Primitive(P::Int32)),
                ("call".into(), Type::Primitive(P::Call)),
                ("z".into(), Type::Primitive(P::Int64)),
            ],
        };
        let path =
            std::env::temp_dir().join(format!("vail-adjacent-varints-{}.bin", std::process::id()));
        let result = (|| -> Result<()> {
            for mode in 0..3 {
                for optional in [false, true] {
                    for mask in 0..8u8 {
                        if !optional && mask != 0 {
                            continue;
                        }
                        for selection in 0..4 {
                            for values in [[3i64, 12, 5], [-1, 12, -2], [128, 20, 129], [3, 6, 5]] {
                                for prefix in [0usize, 65534] {
                                    let field = |width| Encoding {
                                        kind: if mode == 0 {
                                            "EVarint"
                                        } else if width == 64 {
                                            "EInt64"
                                        } else {
                                            "EInt32"
                                        }
                                        .into(),
                                        required: !optional,
                                        fields: vec![],
                                        rank: 0,
                                    };
                                    let encoding = Encoding {
                                        kind: "EBaseStruct".into(),
                                        required: true,
                                        fields: vec![
                                            ("z".into(), field(64)),
                                            ("call".into(), field(32)),
                                            ("a".into(), field(32)),
                                        ],
                                        rank: 0,
                                    };
                                    let paths = match selection {
                                        0 => vec![vec![]],
                                        1 => vec![vec!["a".into()]],
                                        2 => vec![vec!["call".into()]],
                                        _ => vec![],
                                    };
                                    let mut fast = Node::new(&encoding, &ty);
                                    fast.demand(&paths);
                                    assert_eq!(fast.fields[0].varint_run, 3);
                                    let mut slow = Node::new(&encoding, &ty);
                                    slow.demand(&paths);
                                    for field in &mut slow.fields {
                                        field.varint_run = 0;
                                    }
                                    let mut bytes = vec![0; prefix];
                                    if optional {
                                        bytes.push(mask | 0b1111_1000);
                                    }
                                    for (i, value) in values.into_iter().enumerate() {
                                        if optional && mask & (1 << i) != 0 {
                                            continue;
                                        }
                                        let width = if i == 0 { 64 } else { 32 };
                                        if mode != 2 {
                                            leb(
                                                if width == 32 {
                                                    value as u32 as u64
                                                } else {
                                                    value as u64
                                                },
                                                &mut bytes,
                                            );
                                        } else if width == 32 {
                                            bytes.extend((value as i32).to_le_bytes());
                                        } else {
                                            bytes.extend(value.to_le_bytes());
                                        }
                                    }
                                    std::fs::write(&path, &bytes)?;
                                    let spec = if mode == 1 {
                                        serde_json::json!({"name":"LEB128BufferSpec", "child":{"name":"StreamBufferSpec"}})
                                    } else {
                                        serde_json::json!({"name":"StreamBufferSpec"})
                                    };
                                    let mut fast_input = Input::open(&path, &spec)?;
                                    let mut slow_input = Input::open(&path, &spec)?;
                                    fast_input.skip(prefix)?;
                                    slow_input.skip(prefix)?;
                                    let mut fast_column = Column::projected(&fast, &ty.to_arrow()?);
                                    let mut slow_column = Column::projected(&slow, &ty.to_arrow()?);
                                    let fast_result =
                                        fast.append(&mut fast_input, &mut fast_column);
                                    let slow_result =
                                        slow.append(&mut slow_input, &mut slow_column);
                                    assert_eq!(fast_result.is_ok(), slow_result.is_ok());
                                    if fast_result.is_err() {
                                        continue;
                                    }
                                    assert_eq!(fast_input.bytes, bytes.len() as u64);
                                    assert_eq!(slow_input.bytes, fast_input.bytes);
                                    fast_column.null()?;
                                    slow_column.null()?;
                                    assert_eq!(
                                        fast_column.finish()?.as_ref(),
                                        slow_column.finish()?.as_ref(),
                                        "mode={mode} optional={optional} mask={mask} selection={selection} values={values:?} prefix={prefix}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn packed_calls_nested_vortex_export_matches_arrow_across_batches() -> Result<()> {
        use vortex::{VortexSessionDefault, arrow::ArrowSessionExt};
        let session = vortex::session::VortexSession::default();
        let call_type = Type::Primitive(P::Call).to_arrow()?;
        let DataType::Struct(call_fields) = &call_type else {
            unreachable!()
        };
        let element = Arc::new(arrow_schema::Field::new("item", call_type.clone(), true));
        let lists = Arc::new(arrow_schema::Field::new(
            "calls",
            DataType::List(element.clone()),
            true,
        ));
        let fields: Fields = vec![lists.clone()].into();
        let field = arrow_schema::Field::new("root", DataType::Struct(fields.clone()), true);
        let make = || {
            Column::Struct(
                fields.clone(),
                vec![Column::List(
                    element.clone(),
                    Box::new(Column::Call(Box::new(CallColumn::new(call_fields.clone())))),
                    vec![0],
                    NullBufferBuilder::new(8),
                )],
                NullBufferBuilder::new(8),
            )
        };
        let node = Node::new(
            &Encoding {
                kind: "EInt32".into(),
                required: false,
                fields: vec![],
                rank: 0,
            },
            &Type::Primitive(P::Call),
        );
        let mut arrow = make();
        let mut vortex = make();
        for values in [
            vec![0, 4, 5, 12, 13, 20, 21, u32::MAX - 2],
            vec![2, 65535 << 3 | 2],
        ] {
            for column in [&mut arrow, &mut vortex] {
                let list = &mut column.children()[0];
                let Column::List(_, child, _, _) = list else {
                    unreachable!()
                };
                for value in &values {
                    node.append_call_value(*value, child)?;
                    child.null()?;
                }
                list.end_list()?;
                column.valid(true);
                column.children()[0].null()?;
                column.valid(true);
                column.null()?;
            }
            let expected = arrow.finish()?;
            let packed = vortex.finish_vortex(&field, &session)?;
            assert_eq!(packed.dtype(), &session.arrow().from_arrow_field(&field)?);
            let actual = super::super::packed_call::export(
                packed,
                Some(&field),
                &mut vortex::array::ExecutionCtx::new(session.clone()),
            )?;
            assert_eq!(expected.as_ref(), actual.as_ref());
        }
        Ok(())
    }

    #[test]
    fn packed_call_validation_matches_expansion_at_boundaries() {
        let payloads = [
            0u32,
            1,
            2,
            3,
            65534,
            65535,
            65536,
            65537,
            (1 << 28) - 1,
            (1 << 29) - 2,
            (1 << 29) - 1,
        ];
        for payload in payloads {
            for tag in 0..8 {
                let value = (payload << 3) | tag;
                assert_eq!(
                    validate_packed_call(value).is_ok(),
                    call_parts(value).is_ok(),
                    "{value}"
                );
            }
        }
        for value in [
            u32::MAX,
            i32::MIN as u32,
            (-8i32) as u32,
            (-6i32) as u32,
            (-4i32) as u32,
        ] {
            assert_eq!(
                validate_packed_call(value).is_ok(),
                call_parts(value).is_ok(),
                "{value}"
            );
        }
        // Deterministic samples include the entire UInt32 domain, both phases,
        // all ploidies, and the original signed-wire-to-UInt32 interpretation.
        let mut value = 0u32;
        for _ in 0..16384 {
            value = value.wrapping_mul(1664525).wrapping_add(1013904223);
            assert_eq!(
                validate_packed_call(value).is_ok(),
                call_parts(value).is_ok(),
                "{value}"
            );
        }
    }

    #[test]
    fn packed_calls_validate_before_materialization() -> Result<()> {
        let ty = Type::Primitive(P::Call);
        let node = Node::new(
            &Encoding {
                kind: "EInt32".into(),
                required: false,
                fields: vec![],
                rank: 0,
            },
            &ty,
        );
        let mut column = Column::projected(&node, &ty.to_arrow()?);
        // Invalid ploidy, and a haploid allele exceeding the public UInt16.
        for packed in [6, (65536 << 3) | 2] {
            assert!(node.append_call_value(packed, &mut column).is_err());
            assert_eq!(column.len(), 0);
        }
        node.append_call_value(12, &mut column)?;
        column.null()?;
        let Column::Call(call) = &column else {
            panic!("expected packed Call builder")
        };
        assert_eq!(call.packed, vec![12, 0]);
        assert_eq!(call.valid.len(), 2);
        Ok(())
    }

    #[test]
    fn typed_call_builder_matches_generic_schema_and_child_validity() -> Result<()> {
        let ty = Type::Primitive(P::Call);
        let encoding = Encoding {
            kind: "EInt32".into(),
            required: false,
            fields: vec![],
            rank: 0,
        };
        let node = Node::new(&encoding, &ty);
        let path =
            std::env::temp_dir().join(format!("vail-typed-calls-{}.bin", std::process::id()));
        let result = (|| -> Result<()> {
            let mut typed = Column::projected(&node, &ty.to_arrow()?);
            let mut generic = Column::new(&ty.to_arrow()?);
            for values in [
                vec![0i32, 1, 2, 3, 4, 5, 12, 13, 20, 21, 44, 45],
                vec![12, 4],
            ] {
                for value in values {
                    std::fs::write(&path, value.to_le_bytes())?;
                    let spec = serde_json::json!({"name":"StreamBufferSpec"});
                    node.append(&mut Input::open(&path, &spec)?, &mut typed)?;
                    node.append(&mut Input::open(&path, &spec)?, &mut generic)?;
                    typed.null()?;
                    generic.null()?;
                }
                let typed_array = typed.finish()?;
                let generic_array = generic.finish()?;
                assert_eq!(typed_array.as_ref(), generic_array.as_ref());
                let calls = typed_array.as_any().downcast_ref::<StructArray>().unwrap();
                for child in calls.columns() {
                    assert_eq!(child.null_count(), calls.null_count());
                    assert_eq!(
                        child.nulls().unwrap().inner().values().as_ptr(),
                        calls.nulls().unwrap().inner().values().as_ptr()
                    );
                    for i in 0..calls.len() {
                        assert_eq!(child.is_null(i), calls.is_null(i));
                    }
                }
                for i in (1..calls.len()).step_by(2) {
                    assert_eq!(
                        calls
                            .column(0)
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap()
                            .value(i),
                        0
                    );
                    assert_eq!(
                        calls
                            .column(1)
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap()
                            .value(i),
                        0
                    );
                    assert_eq!(
                        calls
                            .column(2)
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap()
                            .value(i),
                        0
                    );
                    assert!(
                        !calls
                            .column(3)
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(i)
                    );
                }
            }
            // Invalid ploidy still follows the validated generic decoder path.
            std::fs::write(&path, 6i32.to_le_bytes())?;
            assert!(
                node.append(
                    &mut Input::open(&path, &serde_json::json!({"name":"StreamBufferSpec"}))?,
                    &mut typed
                )
                .is_err()
            );
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn compact_skip_retains_boolean_overflow_and_truncation_checks() -> Result<()> {
        let path =
            std::env::temp_dir().join(format!("vail-compact-skip-{}.bin", std::process::id()));
        let result = (|| -> Result<()> {
            for (kind, ty, bytes, leb) in [
                ("EBoolean", P::Bool, vec![2], false),
                ("EInt32", P::Int32, vec![255, 255, 255, 255, 16], true),
                (
                    "EVarint",
                    P::Int64,
                    vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 2],
                    false,
                ),
                ("EFloat64", P::Float64, vec![0; 7], false),
            ] {
                std::fs::write(&path, bytes)?;
                let encoding = Encoding {
                    kind: kind.into(),
                    required: true,
                    fields: vec![],
                    rank: 0,
                };
                let node = Node::new(&encoding, &Type::Primitive(ty));
                let spec = if leb {
                    serde_json::json!({"name":"LEB128BufferSpec", "child":{"name":"StreamBufferSpec"}})
                } else {
                    serde_json::json!({"name":"StreamBufferSpec"})
                };
                assert!(
                    node.skip(&mut Input::open(&path, &spec)?).is_err(),
                    "{kind}"
                );
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn skipped_children_inherit_parent_length_across_missing_structs_and_batches() -> Result<()> {
        let ty = Type::Struct {
            r#struct: vec![
                ("selected".into(), Type::Primitive(P::Int32)),
                (
                    "unused".into(),
                    Type::Array {
                        array: Box::new(Type::Primitive(P::Int32)),
                    },
                ),
            ],
        };
        let integer = Encoding {
            kind: "EInt32".into(),
            required: true,
            fields: vec![],
            rank: 0,
        };
        let encoding = Encoding {
            kind: "EBaseStruct".into(),
            required: true,
            fields: vec![
                ("selected".into(), integer.clone()),
                (
                    "unused".into(),
                    Encoding {
                        kind: "EArray".into(),
                        required: true,
                        fields: vec![("element".into(), integer)],
                        rank: 0,
                    },
                ),
            ],
            rank: 0,
        };
        let mut node = Node::new(&encoding, &ty);
        node.demand(&[vec!["selected".into()]]);
        let mut column = Column::projected(&node, &ty.to_arrow()?);
        let path =
            std::env::temp_dir().join(format!("vail-deferred-lengths-{}.bin", std::process::id()));
        let bytes = [4i32, 2, 8, 9, 5, 1, 6]
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        std::fs::write(&path, bytes)?;
        let result = (|| -> Result<()> {
            for leading_nulls in [1usize, 3, 0] {
                let mut input =
                    Input::open(&path, &serde_json::json!({"name":"StreamBufferSpec"}))?;
                for _ in 0..leading_nulls {
                    column.null()?;
                }
                node.append(&mut input, &mut column)?;
                column.null()?;
                node.append(&mut input, &mut column)?;
                let array = column.finish()?;
                let structs = array.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(structs.len(), leading_nulls + 3);
                assert_eq!(structs.column(1).len(), structs.len());
                assert_eq!(structs.column(1).null_count(), structs.len());
                let values = structs
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(values.value(leading_nulls), 4);
                assert_eq!(values.value(leading_nulls + 2), 5);
                assert_eq!(structs.null_count(), leading_nulls + 1);
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn compiled_leaf_dispatch_preserves_physical_field_order_and_missingness() -> Result<()> {
        let ty = Type::Struct {
            r#struct: vec![
                ("a".into(), Type::Primitive(P::Int32)),
                ("b".into(), Type::Primitive(P::Bool)),
                ("c".into(), Type::Primitive(P::Float64)),
            ],
        };
        let field = |kind: &str| Encoding {
            kind: kind.into(),
            required: false,
            fields: vec![],
            rank: 0,
        };
        let encoding = Encoding {
            kind: "EBaseStruct".into(),
            required: true,
            fields: vec![
                ("b".into(), field("EBoolean")),
                ("c".into(), field("EFloat64")),
                ("a".into(), field("EInt32")),
            ],
            rank: 0,
        };
        let node = Node::new(&encoding, &ty);
        let mut bytes = vec![0b1111_1001]; // b absent; trailing bits are padding.
        bytes.extend(1.5f64.to_le_bytes());
        bytes.extend((-3i32).to_le_bytes());
        let path =
            std::env::temp_dir().join(format!("vail-compiled-leaves-{}.bin", std::process::id()));
        std::fs::write(&path, bytes)?;
        let result = (|| -> Result<()> {
            let mut input = Input::open(&path, &serde_json::json!({"name":"StreamBufferSpec"}))?;
            let mut column = Column::new(&ty.to_arrow()?);
            node.append(&mut input, &mut column)?;
            column.null()?;
            let output = column.finish()?;
            let output = output.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(
                output
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                -3
            );
            assert!(output.column(1).is_null(0));
            assert_eq!(
                output
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                1.5
            );
            assert!(output.is_null(1));
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn container_validity_is_lazy_and_resets_between_batches() -> Result<()> {
        let mut column = Column::new(&DataType::Struct(Default::default()));
        column.valid(true);
        column.valid(true);
        assert_eq!(column.len(), 2);
        let first = column.finish()?;
        assert_eq!(first.len(), 2);
        assert!(first.nulls().is_none());
        assert_eq!(column.len(), 0);
        column.null()?;
        column.valid(true);
        let second = column.finish()?;
        assert!(second.is_null(0));
        assert!(!second.is_null(1));
        column.valid(true);
        assert!(column.finish()?.nulls().is_none());
        Ok(())
    }

    #[test]
    fn primitive_array_specialization_matches_recursive_decoder() -> Result<()> {
        fn leb(mut value: u64, bytes: &mut Vec<u8>) {
            loop {
                let byte = (value & 127) as u8;
                value >>= 7;
                bytes.push(byte | if value != 0 { 128 } else { 0 });
                if value == 0 {
                    break;
                }
            }
        }
        let path =
            std::env::temp_dir().join(format!("vail-primitive-arrays-{}.bin", std::process::id()));
        let result = (|| -> Result<()> {
            for required in [false, true] {
                for compressed_int in [false, true] {
                    for explicit_varint in [false, true] {
                        for modern_length in [false, true] {
                            for width in [32, 64] {
                                let child_encoding = Encoding {
                                    kind: if explicit_varint {
                                        "EVarint"
                                    } else if width == 32 {
                                        "EInt32"
                                    } else {
                                        "EInt64"
                                    }
                                    .into(),
                                    required,
                                    fields: vec![],
                                    rank: 0,
                                };
                                let encoding = Encoding {
                                    kind: if modern_length { "EArray2" } else { "EArray" }.into(),
                                    required: false,
                                    fields: vec![("element".into(), child_encoding)],
                                    rank: 0,
                                };
                                let ty = Type::Array {
                                    array: Box::new(Type::Primitive(if width == 32 {
                                        P::Int32
                                    } else {
                                        P::Int64
                                    })),
                                };
                                let node = Node::new(&encoding, &ty);
                                let mut bytes = vec![];
                                if modern_length || compressed_int {
                                    leb(4, &mut bytes);
                                } else {
                                    bytes.extend(4i32.to_le_bytes());
                                }
                                if !required {
                                    bytes.push(0b1111_0010);
                                }
                                for (i, value) in [-1i64, 2, 127, 128].into_iter().enumerate() {
                                    if !required && i == 1 {
                                        continue;
                                    }
                                    if explicit_varint || compressed_int {
                                        leb(
                                            if width == 32 {
                                                value as u32 as u64
                                            } else {
                                                value as u64
                                            },
                                            &mut bytes,
                                        );
                                    } else if width == 32 {
                                        bytes.extend((value as i32).to_le_bytes());
                                    } else {
                                        bytes.extend(value.to_le_bytes());
                                    }
                                }
                                std::fs::write(&path, &bytes)?;
                                let spec = if compressed_int {
                                    serde_json::json!({"name":"LEB128BufferSpec", "child":{"name":"StreamBufferSpec"}})
                                } else {
                                    serde_json::json!({"name":"StreamBufferSpec"})
                                };
                                let mut skipped = Input::open(&path, &spec)?;
                                node.skip(&mut skipped)?;
                                assert_eq!(skipped.bytes, bytes.len() as u64);
                                skipped.finish()?;
                                let mut specialized = Column::new(&ty.to_arrow()?);
                                node.append(&mut Input::open(&path, &spec)?, &mut specialized)?;
                                specialized.null()?; // A missing owning array retains its own null slot.
                                let mut generic = Column::new(&ty.to_arrow()?);
                                let mut input = Input::open(&path, &spec)?;
                                let len = node.length(&mut input)?;
                                let bitmap = if required {
                                    Bitmap::Inline([0; 8])
                                } else {
                                    bitmap(&mut input, len)?
                                };
                                let Column::List(_, values, _, _) = &mut generic else {
                                    unreachable!()
                                };
                                for i in 0..len {
                                    if !required && absent(&bitmap, i) {
                                        values.null()?;
                                    } else {
                                        node.children[0].append(&mut input, values)?;
                                    }
                                }
                                generic.end_list()?;
                                generic.null()?;
                                assert_eq!(
                                    specialized.finish()?.as_ref(),
                                    generic.finish()?.as_ref(),
                                    "required={required} leb={compressed_int} varint={explicit_varint} length2={modern_length} width={width}"
                                );
                            }
                        }
                    }
                }
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn bitmap_present_count_ignores_padding_bits() {
        assert_eq!(Bitmap::Inline([255; 8]).present_count(0), 0);
        assert_eq!(Bitmap::Inline([0b1111_0101; 8]).present_count(3), 1);
        assert_eq!(
            Bitmap::Heap(vec![0b0101_0101, 0b1111_1110]).present_count(9),
            5
        );
    }

    #[test]
    fn skipped_placeholders_reuse_largest_batch() -> Result<()> {
        let mut column = Column::Skipped(DataType::Utf8, 4, None);
        let first = column.finish()?;
        let Column::Skipped(_, n, cached) = &mut column else {
            unreachable!()
        };
        let original = cached.clone().unwrap();
        *n = 2;
        let second = column.finish()?;
        let Column::Skipped(_, n, cached) = &mut column else {
            unreachable!()
        };
        assert!(Arc::ptr_eq(&original, cached.as_ref().unwrap()));
        assert_eq!(first.len(), 4);
        assert_eq!(second.len(), 2);
        *n = 5;
        let third = column.finish()?;
        let Column::Skipped(_, _, cached) = &column else {
            unreachable!()
        };
        assert!(!Arc::ptr_eq(&original, cached.as_ref().unwrap()));
        assert_eq!(third.len(), 5);
        assert_eq!(cached.as_ref().unwrap().len(), 5);
        Ok(())
    }

    #[test]
    fn nested_array_demand_skips_invalid_unselected_string() -> Result<()> {
        fn encoding(kind: &str, fields: Vec<(String, Encoding)>) -> Encoding {
            Encoding {
                kind: kind.into(),
                required: true,
                fields,
                rank: 0,
            }
        }
        let ty = Type::Array {
            array: Box::new(Type::Struct {
                r#struct: vec![
                    ("DP".into(), Type::Primitive(P::Int32)),
                    ("unused".into(), Type::Primitive(P::Str)),
                ],
            }),
        };
        let enc = encoding(
            "EArray",
            vec![(
                "element".into(),
                encoding(
                    "EBaseStruct",
                    vec![
                        ("DP".into(), encoding("EInt32", vec![])),
                        ("unused".into(), encoding("EBinary", vec![])),
                    ],
                ),
            )],
        );
        let mut node = Node::new(&enc, &ty);
        node.demand(&[vec!["DP".into()]]);
        let arrow_ty = ty.to_arrow()?;
        let mut column = Column::projected(&node, &arrow_ty);
        let path = std::env::temp_dir().join(format!(
            "vail-nested-demand-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        let mut bytes = 2i32.to_le_bytes().to_vec();
        for dp in [7i32, 9] {
            bytes.extend(dp.to_le_bytes());
            bytes.extend(1i32.to_le_bytes());
            bytes.push(255); // Selected UTF-8 decoding would fail.
        }
        std::fs::write(&path, bytes)?;
        let result = (|| -> Result<()> {
            let mut input = Input::open(&path, &serde_json::json!({"name":"StreamBufferSpec"}))?;
            node.append(&mut input, &mut column)?;
            let array = column.finish()?;
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let dp = values
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(dp.values().as_ref(), &[7, 9]);
            assert_eq!(values.column(1).null_count(), 2);
            assert_eq!(list.value_length(0), 2);

            let whole = Node::new(&enc, &ty);
            let mut input = Input::open(&path, &serde_json::json!({"name":"StreamBufferSpec"}))?;
            assert!(
                whole
                    .append(&mut input, &mut Column::new(&arrow_ty))
                    .is_err()
            );

            let mut shape = Node::new(&enc, &ty);
            shape.demand(&[vec!["\0shape".into()]]);
            let mut input = Input::open(&path, &serde_json::json!({"name":"StreamBufferSpec"}))?;
            let mut column = Column::projected(&shape, &arrow_ty);
            shape.append(&mut input, &mut column)?;
            let array = column.finish()?;
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(list.value_length(0), 2);
            assert_eq!(values.null_count(), 0);
            assert_eq!(values.column(0).null_count(), 2);
            assert_eq!(values.column(1).null_count(), 2);
            Ok(())
        })();
        std::fs::remove_file(path)?;
        result
    }
}

#[cfg(test)]
#[path = "decode_bench.rs"]
mod decode_bench;

#[cfg(test)]
#[path = "numeric_array_bench.rs"]
mod numeric_array_bench;
