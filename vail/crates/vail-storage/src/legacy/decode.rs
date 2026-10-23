use super::input::Input;
use super::*;
#[cfg(test)]
#[path = "schema_bench.rs"]
mod schema_bench;
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
        let nulls = packed.logical_nulls();
        let mut first = Vec::with_capacity(packed.len());
        let mut second = Vec::with_capacity(packed.len());
        let mut ploidies = Vec::with_capacity(packed.len());
        let mut phased = BooleanBufferBuilder::new(packed.len());
        for i in 0..packed.len() {
            let (a, b, p, phase) = if nulls.as_ref().is_some_and(|nulls| !nulls.is_valid(i)) {
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
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    UInt16(UInt16Builder),
    UInt8(UInt8Builder),
    Call(Box<CallColumn>),
    Ndarray(Box<NdarrayColumn>),
    Struct(Fields, Vec<Column>, NullBufferBuilder),
    List(FieldRef, Box<Column>, Vec<i32>, NullBufferBuilder),
}
// Reader-owned scratch is prepared with the output column and retained across
// cells and finished batches. Shape arithmetic depends on the actual input;
// schema conversion, element dispatch, and allocations do not occur per cell.
struct NdarrayColumn {
    output: Column,
    shape: Vec<usize>,
    source: Vec<usize>,
    values: NdarrayValues,
}
enum NdarrayValues {
    Bool(Vec<bool>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Unsupported,
}
impl NdarrayValues {
    fn new(ty: &Type) -> Self {
        match ty {
            Type::Primitive(P::Bool) => Self::Bool(Vec::new()),
            Type::Primitive(P::Int32) => Self::I32(Vec::new()),
            Type::Primitive(P::Int64) => Self::I64(Vec::new()),
            Type::Primitive(P::Float32) => Self::F32(Vec::new()),
            Type::Primitive(P::Float64) => Self::F64(Vec::new()),
            _ => Self::Unsupported,
        }
    }
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
        if let Type::Ndarray { ndarray } = &node.ty {
            return Self::Ndarray(Box::new(NdarrayColumn {
                output: Self::new(ty),
                shape: vec![0; ndarray.1],
                source: Vec::new(),
                values: NdarrayValues::new(&ndarray.0),
            }));
        }
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
                            Some(j) => Self::projected(&node.children[j], f.data_type()),
                            None => Self::new(f.data_type()),
                        },
                    )
                    .collect();
                Self::Struct(fields.clone(), children, NullBufferBuilder::new(8192))
            }
            DataType::List(field) if node.e.kind.starts_with("EArray") => Self::List(
                field.clone(),
                Box::new(Self::projected(&node.children[0], field.data_type())),
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
            Self::Ndarray(c) => c.output.len(),
            Self::Bool(b) => b.len(),
            Self::Int32(b) => b.len(),
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
            Self::Ndarray(c) => c.output.null()?,
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
            Self::Ndarray(c) => c.output.finish_vortex(field, session)?,
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
            Self::Ndarray(c) => c.output.finish()?,
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
enum BoundOutput<'n, 'a> {
    Skip,
    Bool(&'a mut BooleanBuilder),
    InvertedBool(&'a mut BooleanBuilder),
    I32(&'a mut Int32Builder),
    I64(&'a mut Int64Builder),
    F32(&'a mut Float32Builder),
    F64(&'a mut Float64Builder),
    Call(&'a mut CallColumn),
    ArrowList(BoundList<'a, Int32Builder>),
    Other(Box<BatchColumn<'n, 'a>>),
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
fn decode_arrow_list<const MODE: u8>(
    input: &mut Input,
    list: &mut BoundList<'_, Int32Builder>,
) -> Result<()> {
    let count = bound_list_length::<MODE>(input)?;
    append_buffered_i32_values(input, list.values, count)?;
    list.offsets.push(i32::try_from(list.values.len())?);
    list.validity.append_non_null();
    Ok(())
}
// Decode complete values from one borrowed block. On any exceptional word,
// leave that word untouched for the ordinary decoder's exact error behavior.
fn append_buffered_i32_values(
    input: &mut Input,
    output: &mut Int32Builder,
    count: usize,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }
    let completed = input.with_buffer(|bytes| {
        let mut cursor = 0;
        let mut completed = 0;
        'values: while completed < count {
            let start = cursor;
            let mut value = 0u64;
            for shift in (0..64).step_by(7) {
                let Some(&byte) = bytes.get(cursor) else {
                    cursor = start;
                    break 'values;
                };
                // The tenth byte may carry only the top bit of a u64.
                if shift == 63 && byte & 0x7f > 1 {
                    cursor = start;
                    break 'values;
                }
                cursor += 1;
                value |= u64::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    if value > u32::MAX as u64 {
                        cursor = start;
                        break 'values;
                    }
                    output.append_value(value as u32 as i32);
                    completed += 1;
                    continue 'values;
                }
            }
            // Ten continuation bytes are invalid, even if their payloads fit.
            cursor = start;
            break;
        }
        (completed, cursor)
    })?;
    for _ in completed..count {
        let value = input.varint(64)?;
        if value > u32::MAX as u64 {
            return Err("Hail int32 varint overflow".into());
        }
        output.append_value(value as u32 as i32);
    }
    Ok(())
}

struct BoundField<'n, 'a> {
    node: &'n Node,
    missing: usize,
    output: BoundOutput<'n, 'a>,
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
            BoundOutput::Other(b) => b.append(input)?,
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
            BoundOutput::Other(b) => {
                let BatchColumn::Leaf { column, .. } = b.as_mut() else {
                    unreachable!("bound integer output")
                };
                self.node.append_call_value(value as u32, column)?;
            }
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
    SkipByte,
    Byte,
    I64,
    Bool,
    Call,
    F32,
    F64,
    IntegerArray,
    I32List(u8),
}
struct ProbeField {
    kind: ProbeKind,
    missing: u64,
    output: u8,
    bits: u8,
}
struct BufferedScalars {
    fields: Vec<ProbeField>,
    bitmap_bytes: usize,
    has_lists: bool,
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
#[inline]
fn slice_varint(bytes: &[u8], cursor: &mut usize, bits: u8) -> Option<u64> {
    let first = *bytes.get(*cursor)?;
    if first < 128 {
        *cursor += 1;
        return Some(u64::from(first));
    }
    slice_varint_long(bytes, cursor, bits)
}
#[inline(never)]
fn slice_varint_long(bytes: &[u8], cursor: &mut usize, bits: u8) -> Option<u64> {
    let mut position = *cursor;
    let mut value = 0u64;
    for shift in (0..u32::from(bits)).step_by(7) {
        let byte = *bytes.get(position)?;
        position += 1;
        let payload = u64::from(byte & 127);
        if shift + 7 > u32::from(bits) && payload >= (1u64 << (u32::from(bits) - shift)) {
            return None;
        }
        value |= payload << shift;
        if byte < 128 {
            *cursor = position;
            return Some(value);
        }
    }
    None
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
                    } else if op.project && matches!(child.append_kind, AppendKind::Int64) {
                        ProbeKind::I64
                    } else if op.project {
                        ProbeKind::Byte
                    } else {
                        ProbeKind::SkipByte
                    }
                }
                AppendKind::Bool => ProbeKind::Bool,
                AppendKind::Float32 => ProbeKind::F32,
                AppendKind::Float64 => ProbeKind::F64,
                AppendKind::List
                    if op.project
                        && child.children[0].e.required
                        && matches!(child.children[0].wire, Wire::Varint)
                        && matches!(child.children[0].append_kind, AppendKind::Int32) =>
                {
                    ProbeKind::I32List(if child.varint_length {
                        2
                    } else if leb {
                        1
                    } else {
                        0
                    })
                }
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
                index as u8
            } else {
                u8::MAX
            };
            if selected > 16 {
                return None;
            }
            fields.push(ProbeField {
                kind,
                bits: if matches!(child.wire, Wire::Varint)
                    || matches!(child.append_kind, AppendKind::Int64)
                {
                    64
                } else {
                    32
                },
                missing: if op.missing_bit == usize::MAX {
                    0
                } else {
                    1u64 << op.missing_bit
                },
                output,
            });
        }
        Some(Self {
            has_lists: fields
                .iter()
                .any(|field| matches!(field.kind, ProbeKind::I32List(_))),
            fields,
            bitmap_bytes: node.optional_count.div_ceil(8),
        })
    }
    fn probe(&self, bytes: &[u8], cursor: &mut usize, values: &mut [u64; 16]) -> Option<u16> {
        self.probe_values::<false>(bytes, cursor, values, &mut [0; 32], &mut [(0, 0); 16])
    }
    fn probe_values<const LISTS: bool>(
        &self,
        bytes: &[u8],
        cursor: &mut usize,
        values: &mut [u64; 16],
        elements: &mut [i32; 32],
        ranges: &mut [(u8, u8); 16],
    ) -> Option<u16> {
        let mut used = 0usize;
        let missing = if self.bitmap_bytes == 1 {
            let byte = u64::from(*bytes.get(*cursor)?);
            *cursor += 1;
            byte
        } else {
            let bitmap = bytes.get(*cursor..cursor.checked_add(self.bitmap_bytes)?)?;
            let mut word = [0u8; 8];
            word[..self.bitmap_bytes].copy_from_slice(bitmap);
            *cursor += self.bitmap_bytes;
            u64::from_le_bytes(word)
        };
        let mut nulls = 0;
        for field in &self.fields {
            if missing & field.missing != 0 {
                if field.output != u8::MAX {
                    nulls |= 1 << field.output;
                }
                continue;
            }
            let value = match field.kind {
                ProbeKind::SkipByte => {
                    slice_skip_varints(bytes, cursor, 1)?;
                    continue;
                }
                ProbeKind::Byte => {
                    let value = slice_varint(bytes, cursor, field.bits)?;
                    if value > u32::MAX as u64 {
                        return None;
                    }
                    value
                }
                ProbeKind::I64 => slice_varint(bytes, cursor, field.bits)?,
                ProbeKind::Bool => {
                    let value = *bytes.get(*cursor)?;
                    if value > 1 {
                        return None;
                    }
                    *cursor += 1;
                    value as u64
                }
                ProbeKind::Call => {
                    let value = slice_varint(bytes, cursor, field.bits)?;
                    if value > u32::MAX as u64 || validate_packed_call(value as u32).is_err() {
                        return None;
                    }
                    value
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
                ProbeKind::I32List(mode) => {
                    if !LISTS {
                        return None;
                    }
                    let count = if mode == 0 {
                        let end = cursor.checked_add(4)?;
                        let signed = i32::from_le_bytes(bytes.get(*cursor..end)?.try_into().ok()?);
                        *cursor = end;
                        usize::try_from(signed).ok()?
                    } else {
                        let value = slice_varint(bytes, cursor, 32)?;
                        if mode == 1 {
                            usize::try_from(value as u32 as i32).ok()?
                        } else {
                            usize::try_from(value).ok()?
                        }
                    };
                    let end = used.checked_add(count)?;
                    if end > elements.len() {
                        return None;
                    }
                    for element in &mut elements[used..end] {
                        let value = slice_varint(bytes, cursor, 64)?;
                        *element = u32::try_from(value).ok()? as i32;
                    }
                    ranges[usize::from(field.output)] = (used as u8, end as u8);
                    used = end;
                    continue;
                }
                ProbeKind::IntegerArray => {
                    let n = *bytes.get(*cursor)?;
                    if n >= 128 {
                        return None;
                    }
                    *cursor += 1;
                    slice_skip_varints(bytes, cursor, usize::from(n))?;
                    continue;
                }
            };
            if field.output != u8::MAX {
                values[usize::from(field.output)] = value;
            }
        }
        Some(nulls)
    }
}
fn buffered_scalar_output(output: &mut BoundOutput<'_, '_>, value: u64, missing: bool) {
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
        BoundOutput::Other(_) | BoundOutput::ArrowList(_) => {
            unreachable!("buffered scalar eligibility")
        }
    }
}
fn has_buffered_outputs(ops: &[BoundOp<'_, '_>]) -> bool {
    let eligible = |f: &BoundField<'_, '_>| {
        !matches!(f.output, BoundOutput::Other(_) | BoundOutput::ArrowList(_))
    };
    ops.iter().all(|op| match op {
        BoundOp::Field(f) => eligible(f),
        BoundOp::Run { fields, .. } => fields.iter().all(eligible),
    })
}

// Check every selected list offset before publishing any field of the entry.
fn mixed_list_end(length: usize, range: (u8, u8)) -> Option<i32> {
    i32::try_from(length.checked_add(usize::from(range.1.checked_sub(range.0)?))?).ok()
}
fn mixed_list_block_end(length: usize, remaining: usize) -> Option<i32> {
    i32::try_from(length.checked_add(remaining.checked_mul(32)?)?).ok()
}
fn mixed_offsets_all_fit(
    ops: &[BoundOp<'_, '_>],
    selected: &[(usize, usize)],
    remaining: usize,
) -> bool {
    selected.iter().all(|&location| {
        let output = selected_output_ref(ops, location);
        if let BoundOutput::ArrowList(list) = output {
            mixed_list_block_end(list.values.len(), remaining).is_some()
        } else {
            true
        }
    })
}
const MIXED_COMMIT_ROWS: usize = 16;
struct MixedBatch {
    scalars: [[u64; 16]; MIXED_COMMIT_ROWS],
    elements: [[i32; 32]; MIXED_COMMIT_ROWS],
    ranges: [[(u8, u8); 16]; MIXED_COMMIT_ROWS],
    nulls: [u16; MIXED_COMMIT_ROWS],
    entry_valid: [bool; MIXED_COMMIT_ROWS],
    len: usize,
}
impl MixedBatch {
    fn new() -> Self {
        Self {
            scalars: [[0; 16]; MIXED_COMMIT_ROWS],
            elements: [[0; 32]; MIXED_COMMIT_ROWS],
            ranges: [[(0, 0); 16]; MIXED_COMMIT_ROWS],
            nulls: [0; MIXED_COMMIT_ROWS],
            entry_valid: [false; MIXED_COMMIT_ROWS],
            len: 0,
        }
    }
}
fn mixed_staged_offsets_fit(
    ops: &[BoundOp<'_, '_>],
    selected: &[(usize, usize)],
    ranges: &[(u8, u8); 16],
    nulls: u16,
    growth: &mut [usize; 16],
) -> bool {
    for (index, &location) in selected.iter().enumerate() {
        let output = selected_output_ref(ops, location);
        if nulls & (1 << index) != 0 {
            continue;
        }
        if let BoundOutput::ArrowList(list) = output {
            let Some(length) = list.values.len().checked_add(growth[index]) else {
                return false;
            };
            if mixed_list_end(length, ranges[index]).is_none() {
                return false;
            }
        }
    }
    for (index, &location) in selected.iter().enumerate() {
        let output = selected_output_ref(ops, location);
        if nulls & (1 << index) == 0 && matches!(output, BoundOutput::ArrowList(_)) {
            growth[index] += usize::from(ranges[index].1 - ranges[index].0);
        }
    }
    true
}
// The enum/type dispatch occurs once per output column and completed chunk.
fn buffered_mixed_batch(
    ops: &mut [BoundOp<'_, '_>],
    selected: &[(usize, usize)],
    batch: &MixedBatch,
    gathered: &mut [i32; MIXED_COMMIT_ROWS * 32],
) {
    let n = batch.len;
    let mut valid = [false; MIXED_COMMIT_ROWS];
    for (index, &location) in selected.iter().enumerate() {
        let output = selected_output(ops, location);
        let mut all_valid = true;
        for row in 0..n {
            valid[row] = batch.nulls[row] & (1 << index) == 0;
            all_valid &= valid[row];
        }
        macro_rules! primitive {
            ($builder:expr, $zero:expr, $convert:expr) => {{
                let mut values = [$zero; MIXED_COMMIT_ROWS];
                for row in 0..n {
                    if valid[row] {
                        values[row] = ($convert)(batch.scalars[row][index]);
                    }
                }
                if all_valid {
                    $builder.append_slice(&values[..n]);
                } else {
                    $builder.append_values(&values[..n], &valid[..n]);
                }
            }};
        }
        let inverted = matches!(output, BoundOutput::InvertedBool(_));
        match output {
            BoundOutput::I32(builder) => primitive!(builder, 0i32, |v: u64| v as i32),
            BoundOutput::I64(builder) => primitive!(builder, 0i64, |v: u64| v as i64),
            BoundOutput::F32(builder) => {
                primitive!(builder, 0f32, |v: u64| f32::from_bits(v as u32))
            }
            BoundOutput::F64(builder) => primitive!(builder, 0f64, f64::from_bits),
            BoundOutput::Bool(builder) | BoundOutput::InvertedBool(builder) => {
                let mut values = [false; MIXED_COMMIT_ROWS];
                for row in 0..n {
                    if valid[row] {
                        values[row] = (batch.scalars[row][index] != 0) ^ inverted;
                    }
                }
                if all_valid {
                    builder.append_slice(&values[..n]);
                } else {
                    builder
                        .append_values(&values[..n], &valid[..n])
                        .expect("matched staged lengths");
                }
            }
            BoundOutput::Call(builder) => {
                let mut values = [0u32; MIXED_COMMIT_ROWS];
                for row in 0..n {
                    if valid[row] {
                        values[row] = batch.scalars[row][index] as u32;
                    }
                }
                builder.packed.extend_from_slice(&values[..n]);
                if all_valid {
                    builder.valid.append_n_non_nulls(n);
                } else {
                    builder.valid.append_slice(&valid[..n]);
                }
            }
            BoundOutput::ArrowList(list) => {
                let mut offsets = [0i32; MIXED_COMMIT_ROWS];
                let mut used = 0;
                let base = list.values.len();
                for row in 0..n {
                    if valid[row] {
                        let (start, end) = batch.ranges[row][index];
                        let values = &batch.elements[row][usize::from(start)..usize::from(end)];
                        gathered[used..used + values.len()].copy_from_slice(values);
                        used += values.len();
                    }
                    offsets[row] = (base + used) as i32; // preflight checked the chunk
                }
                list.values.append_slice(&gathered[..used]);
                list.offsets.extend_from_slice(&offsets[..n]);
                if all_valid {
                    list.validity.append_n_non_nulls(n);
                } else {
                    list.validity.append_slice(&valid[..n]);
                }
            }
            _ => unreachable!("mixed buffered output eligibility"),
        }
    }
}
fn has_mixed_outputs(ops: &[BoundOp<'_, '_>]) -> bool {
    let eligible = |field: &BoundField<'_, '_>| !matches!(field.output, BoundOutput::Other(_));
    ops.iter().all(|op| match op {
        BoundOp::Field(field) => eligible(field),
        BoundOp::Run { fields, .. } => fields.iter().all(eligible),
    })
}

// Builder borrows and schema choices live for one read_columns call. Owned
// Node descriptors remain reusable across batches; no references point back
// into this tree or into movable sibling fields.
fn reused_bitmap(input: &mut Input, n: usize, scratch: &mut Vec<u8>) -> Result<Bitmap> {
    if n <= 64 {
        return bitmap(input, n);
    }
    scratch.resize(n.div_ceil(8), 0);
    let mut written = 0;
    while written < scratch.len() {
        let used = input.with_buffer(|bytes| {
            let used = bytes.len().min(scratch.len() - written);
            scratch[written..written + used].copy_from_slice(&bytes[..used]);
            (used, used)
        })?;
        if used == 0 {
            return Err("truncated Hail data".into());
        }
        written += used;
    }
    Ok(Bitmap::Heap(std::mem::take(scratch)))
}
struct PreparedStruct<'n, 'a> {
    ops: Vec<BoundOp<'n, 'a>>,
    selected: Vec<(usize, usize)>,
    program: Option<&'n BufferedScalars>,
    bitmap: Vec<u8>,
    batch: MixedBatch,
    gathered: [i32; MIXED_COMMIT_ROWS * 32],
    values: [u64; 16],
}
impl<'n, 'a> PreparedStruct<'n, 'a> {
    fn new(node: &'n Node, fields: &'a mut [Column], leb: bool) -> Self {
        let ops = node.bind_struct(fields, leb);
        let mut selected = Vec::new();
        for (op_index, op) in ops.iter().enumerate() {
            let fields = match op {
                BoundOp::Field(field) => std::slice::from_ref(field),
                BoundOp::Run { fields, .. } => fields.as_slice(),
            };
            for (field_index, field) in fields.iter().enumerate() {
                if !matches!(field.output, BoundOutput::Skip) {
                    selected.push((op_index, field_index));
                }
            }
        }
        let program = node.buffered_scalars[usize::from(leb)]
            .get_or_init(|| BufferedScalars::new(node, leb))
            .as_ref()
            .filter(|program| {
                if program.has_lists {
                    has_mixed_outputs(&ops)
                } else {
                    has_buffered_outputs(&ops)
                }
            });
        Self {
            ops,
            selected,
            program,
            bitmap: Vec::with_capacity(if node.optional_count > 64 {
                node.optional_count.div_ceil(8)
            } else {
                0
            }),
            batch: MixedBatch::new(),
            gathered: [0; MIXED_COMMIT_ROWS * 32],
            values: [0; 16],
        }
    }
    fn null(&mut self) -> Result<()> {
        for op in &mut self.ops {
            op.null()?;
        }
        Ok(())
    }
    fn append(&mut self, node: &Node, input: &mut Input) -> Result<()> {
        let bits = reused_bitmap(input, node.optional_count, &mut self.bitmap)?;
        let all_present = node
            .all_present_mask
            .is_some_and(|mask| bits.low_word() & mask == 0);
        let result = self
            .ops
            .iter_mut()
            .try_for_each(|op| op.append(input, &bits, all_present));
        if let Bitmap::Heap(bytes) = bits {
            self.bitmap = bytes;
        }
        result
    }
}
enum BatchColumn<'n, 'a> {
    Leaf {
        node: &'n Node,
        column: &'a mut Column,
    },
    Struct {
        node: &'n Node,
        prepared: Box<PreparedStruct<'n, 'a>>,
        validity: &'a mut NullBufferBuilder,
    },
    List {
        node: &'n Node,
        child: Box<BatchColumn<'n, 'a>>,
        offsets: &'a mut Vec<i32>,
        validity: &'a mut NullBufferBuilder,
        bitmap: Vec<u8>,
    },
}
impl<'n, 'a> BatchColumn<'n, 'a> {
    fn new(node: &'n Node, column: &'a mut Column, leb: bool) -> Self {
        if matches!(column, Column::Skipped(..)) {
            return Self::Leaf { node, column };
        }
        let structure = matches!(node.wire, Wire::Struct)
            && matches!(node.struct_postlude, StructPostlude::Plain)
            && node.distinct_destinations;
        let nested_list = matches!(node.wire, Wire::Array)
            && matches!(node.children[0].wire, Wire::Array | Wire::Struct);
        if structure {
            let Column::Struct(_, fields, validity) = column else {
                unreachable!("prepared struct")
            };
            Self::Struct {
                node,
                prepared: Box::new(PreparedStruct::new(node, fields, leb)),
                validity,
            }
        } else if nested_list {
            let Column::List(_, child, offsets, validity) = column else {
                unreachable!("prepared list")
            };
            Self::List {
                node,
                child: Box::new(Self::new(&node.children[0], child, leb)),
                offsets,
                validity,
                bitmap: Vec::new(),
            }
        } else {
            Self::Leaf { node, column }
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Leaf { column, .. } => column.len(),
            Self::Struct { validity, .. } => validity.len(),
            Self::List { offsets, .. } => offsets.len() - 1,
        }
    }
    fn null(&mut self) -> Result<()> {
        match self {
            Self::Leaf { column, .. } => column.null(),
            Self::Struct {
                prepared, validity, ..
            } => {
                prepared.null()?;
                validity.append_null();
                Ok(())
            }
            Self::List {
                offsets, validity, ..
            } => {
                offsets.push(*offsets.last().unwrap());
                validity.append_null();
                Ok(())
            }
        }
    }
    fn append(&mut self, input: &mut Input) -> Result<()> {
        match self {
            Self::Leaf { node, column } => node.append(input, column),
            Self::Struct {
                node,
                prepared,
                validity,
            } => {
                prepared.append(node, input)?;
                validity.append_non_null();
                Ok(())
            }
            Self::List {
                node,
                child,
                offsets,
                validity,
                bitmap: scratch,
            } => {
                let n = node.length(input)?;
                let element = &node.children[0];
                let bits = if element.e.required {
                    Bitmap::Inline([0; 8])
                } else {
                    reused_bitmap(input, n, scratch)?
                };
                let result = if let Self::Struct {
                    node,
                    prepared,
                    validity,
                } = child.as_mut()
                {
                    if element.e.required {
                        node.append_prepared_struct_elements::<true>(
                            input, n, &bits, prepared, validity,
                        )
                    } else {
                        node.append_prepared_struct_elements::<false>(
                            input, n, &bits, prepared, validity,
                        )
                    }
                } else {
                    (|| -> Result<()> {
                        for i in 0..n {
                            if !element.e.required && absent(&bits, i) {
                                child.null()?;
                            } else {
                                child.append(input)?;
                            }
                        }
                        Ok(())
                    })()
                };
                if let Bitmap::Heap(bytes) = bits {
                    *scratch = bytes;
                }
                result?;
                offsets.push(i32::try_from(child.len())?);
                validity.append_non_null();
                Ok(())
            }
        }
    }
}
struct BatchRootField<'n, 'a> {
    node: &'n Node,
    missing_bit: usize,
    output: Option<BatchColumn<'n, 'a>>,
    decode: bool,
}
impl BatchRootField<'_, '_> {
    fn append(&mut self, input: &mut Input, bits: &Bitmap) -> Result<()> {
        let missing = self.missing_bit != usize::MAX && absent(bits, self.missing_bit);
        if let Some(output) = &mut self.output {
            if missing {
                output.null()?;
            } else if self.decode {
                output.append(input)?;
            } else {
                self.node.skip(input)?;
                output.null()?;
            }
        } else if !missing {
            self.node.skip(input)?;
        }
        Ok(())
    }
}
fn selected_output<'p, 'n, 'a>(
    ops: &'p mut [BoundOp<'n, 'a>],
    location: (usize, usize),
) -> &'p mut BoundOutput<'n, 'a> {
    match &mut ops[location.0] {
        BoundOp::Field(field) => &mut field.output,
        BoundOp::Run { fields, .. } => &mut fields[location.1].output,
    }
}
fn selected_output_ref<'p, 'n, 'a>(
    ops: &'p [BoundOp<'n, 'a>],
    location: (usize, usize),
) -> &'p BoundOutput<'n, 'a> {
    match &ops[location.0] {
        BoundOp::Field(field) => &field.output,
        BoundOp::Run { fields, .. } => &fields[location.1].output,
    }
}
fn prepared_scalar_row(
    ops: &mut [BoundOp<'_, '_>],
    selected: &[(usize, usize)],
    values: &[u64; 16],
    nulls: u16,
) {
    for (index, &location) in selected.iter().enumerate() {
        buffered_scalar_output(
            selected_output(ops, location),
            values[index],
            nulls & (1 << index) != 0,
        );
    }
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
            Wire::Int32 if input.leb => input.skip_varints(1, 32)?,
            Wire::Int64 if input.leb => input.skip_varints(1, 64)?,
            Wire::Int32 => {
                input.int(32)?;
            }
            Wire::Int64 => {
                input.int(64)?;
            }
            // Unselected integers need framing, not value decoding or range
            // validation. Match the existing required-array skip path.
            Wire::Varint => input.skip_varints(1, 64)?,
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
        if n == 0 {
            out.string().append_value("");
            return Ok(());
        }
        // A contiguous string needs one copy into Arrow, without a temporary
        // allocation. Crossing a block retains the existing read/error path.
        let appended: Result<bool> = input.with_buffer(|available| {
            let Some(bytes) = available.get(..n) else {
                return (Ok(false), 0);
            };
            match std::str::from_utf8(bytes) {
                Ok(value) => {
                    out.string().append_value(value);
                    (Ok(true), n)
                }
                Err(error) => (Err(error.into()), n),
            }
        })?;
        if !appended? {
            let bytes = input.bytes(n)?;
            out.string().append_value(std::str::from_utf8(&bytes)?);
        }
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
                            other => {
                                BoundOutput::Other(Box::new(BatchColumn::new(node, other, leb)))
                            }
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
        for i in 0..n {
            if !REQUIRED && absent(element_bits, i) {
                child.null()?;
            } else {
                self.append_struct(input, child)?;
            }
        }
        Ok(())
    }
    fn append_prepared_struct_elements<const REQUIRED: bool>(
        &self,
        input: &mut Input,
        n: usize,
        element_bits: &Bitmap,
        prepared: &mut PreparedStruct<'_, '_>,
        validity: &mut NullBufferBuilder,
    ) -> Result<()> {
        let PreparedStruct {
            ops,
            selected,
            program,
            batch,
            gathered,
            values,
            ..
        } = prepared;
        let buffered = *program;
        let mut i = 0;
        while i < n {
            // Leading absent entries need no input; preserve that property
            // even if refilling the underlying reader would fail.
            if !REQUIRED && absent(element_bits, i) {
                for op in ops.iter_mut() {
                    op.null()?;
                }
                validity.append_null();
                i += 1;
                continue;
            }

            if let Some(program) = buffered.filter(|program| program.has_lists) {
                let accepted = input.with_buffer(|bytes| {
                    let offsets_safe = mixed_offsets_all_fit(ops, selected, n - i);
                    let mut cursor = 0;
                    let mut accepted = 0;
                    while i + accepted < n {
                        batch.len = 0;
                        let mut growth = [0usize; 16];
                        while batch.len < MIXED_COMMIT_ROWS && i + accepted + batch.len < n {
                            let row = batch.len;
                            if !REQUIRED && absent(element_bits, i + accepted + row) {
                                batch.nulls[row] = u16::MAX;
                                batch.entry_valid[row] = false;
                            } else {
                                let start = cursor;
                                let Some(nulls) = program.probe_values::<true>(
                                    bytes,
                                    &mut cursor,
                                    &mut batch.scalars[row],
                                    &mut batch.elements[row],
                                    &mut batch.ranges[row],
                                ) else {
                                    cursor = start;
                                    break;
                                };
                                if !offsets_safe
                                    && !mixed_staged_offsets_fit(
                                        ops,
                                        selected,
                                        &batch.ranges[row],
                                        nulls,
                                        &mut growth,
                                    )
                                {
                                    cursor = start;
                                    break;
                                }
                                batch.nulls[row] = nulls;
                                batch.entry_valid[row] = true;
                            }
                            batch.len += 1;
                        }
                        if batch.len == 0 {
                            break;
                        }
                        buffered_mixed_batch(ops, selected, batch, gathered);
                        validity.append_slice(&batch.entry_valid[..batch.len]);
                        accepted += batch.len;
                        if batch.len < MIXED_COMMIT_ROWS {
                            break;
                        }
                    }
                    (accepted, cursor)
                })?;
                if accepted != 0 {
                    i += accepted;
                    continue;
                }
            }
            if let Some(program) = buffered.filter(|program| !program.has_lists) {
                let accepted = input.with_buffer(|bytes| {
                    let mut cursor = 0;
                    let mut accepted = 0;
                    while i + accepted < n {
                        if !REQUIRED && absent(element_bits, i + accepted) {
                            prepared_scalar_row(ops, selected, values, u16::MAX);
                            validity.append_null();
                        } else {
                            let start = cursor;
                            let Some(nulls) = program.probe(bytes, &mut cursor, values) else {
                                cursor = start;
                                break;
                            };
                            prepared_scalar_row(ops, selected, values, nulls);
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
                for op in ops.iter_mut() {
                    op.null()?;
                }
                validity.append_null();
            } else {
                let bits = bitmap(input, self.optional_count)?;
                let all_present = self
                    .all_present_mask
                    .is_some_and(|mask| bits.low_word() & mask == 0);
                for op in ops.iter_mut() {
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
                if c.e.required && matches!(c.wire, Wire::Varint) {
                    append_buffered_i32_values(input, child.int32(), n)?;
                } else {
                    primitive_elements!(child.int32(), c.integer(input, 32)? as i32)
                }
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
        let Column::Ndarray(state) = out else {
            return Err("Hail ndarray output requires prepared column".into());
        };
        let mut changed = false;
        for dimension in &mut state.shape {
            let next = usize::try_from(input.int(64)?)?;
            changed |= next != *dimension;
            *dimension = next;
        }
        let count = state.shape.iter().try_fold(1usize, |n, &dimension| {
            n.checked_mul(dimension)
                .ok_or("Hail ndarray shape overflow")
        })?;
        // Reuse the permutation buffer; no per-cell Arrow array or kernel is
        // constructed. The shape may vary, so indices are input-derived data.
        if changed || state.source.len() != count {
            state.source.clear();
            for row in 0..count {
                let mut rest = row;
                let mut source = 0;
                let mut stride = count;
                let mut fstride = 1;
                for &dimension in &state.shape {
                    stride /= dimension;
                    let coordinate = rest / stride;
                    rest %= stride;
                    source += coordinate * fstride;
                    fstride *= dimension;
                }
                state.source.push(source);
            }
        }
        let columns = state.output.children();
        let Column::List(_, data, _, _) = &mut columns[0] else {
            unreachable!()
        };
        // Select the element kernel once per array; retained typed vectors grow
        // only as necessary and are never converted into temporary Arrow arrays.
        macro_rules! transpose {
            ($values:expr, $read:expr, $builder:expr) => {{
                let values = $values;
                values.clear();
                for _ in 0..count {
                    values.push($read);
                }
                let builder = $builder;
                for &source in &state.source {
                    builder.append_value(values[source]);
                }
            }};
        }
        match &mut state.values {
            NdarrayValues::Bool(values) => transpose!(values, boolean(input)?, data.bool_builder()),
            NdarrayValues::I32(values) => transpose!(
                values,
                self.children[0].integer(input, 32)? as i32,
                data.int32()
            ),
            NdarrayValues::I64(values) => {
                transpose!(values, self.children[0].integer(input, 64)?, data.int64())
            }
            NdarrayValues::F32(values) => {
                transpose!(values, f32::from_le_bytes(input.fixed()?), data.float32())
            }
            NdarrayValues::F64(values) => {
                transpose!(values, f64::from_le_bytes(input.fixed()?), data.float64())
            }
            NdarrayValues::Unsupported => {
                return Err("unsupported Hail ndarray element type".into());
            }
        }
        columns[0].end_list()?;
        let Column::List(_, dimensions, _, _) = &mut columns[1] else {
            unreachable!()
        };
        let builder = dimensions.int64();
        for &dimension in &state.shape {
            builder.append_value(dimension as i64);
        }
        columns[1].end_list()?;
        state.output.valid(true);
        Ok(())
    }
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
        let mut destinations = columns.iter_mut().map(Some).collect::<Vec<_>>();
        let mut root_fields = self
            .node
            .fields
            .iter()
            .zip(&self.node.children)
            .map(|(op, node)| {
                let output = self
                    .projection
                    .iter()
                    .position(|dest| *dest == op.dest)
                    .map(|dest| {
                        BatchColumn::new(node, destinations[dest].take().unwrap(), self.input.leb)
                    });
                BatchRootField {
                    node,
                    missing_bit: op.missing_bit,
                    output,
                    decode: op.project
                        && self
                            .decode_columns
                            .as_ref()
                            .is_none_or(|columns| columns.contains(&op.dest)),
                }
            })
            .collect::<Vec<_>>();
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
            for field in &mut root_fields {
                field.append(&mut self.input, &bits)?;
            }
            n += 1;
            self.rows += 1;
        }
        drop(root_fields);
        drop(destinations);
        if n == 0 {
            return Ok(None);
        }
        Ok(Some((n, columns)))
    }
}

#[cfg(test)]
#[path = "decode_bench.rs"]
mod decode_bench;

#[cfg(test)]
#[path = "numeric_array_bench.rs"]
mod numeric_array_bench;
