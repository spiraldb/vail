use super::input::Input;
use super::*;
use arrow_array::{builder::*, *};
use arrow_buffer::{BooleanBufferBuilder, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef, Fields};
use vail_execution::expressions::PrimitiveType as P;

// Calls retain their public Arrow struct and all child null semantics, while
// their hot append path binds the four physical builders without enum lookups.
struct CallColumn {
    fields: Fields,
    first: Vec<u16>,
    second: Vec<u16>,
    ploidy: Vec<u8>,
    phased: BooleanBufferBuilder,
    valid: NullBufferBuilder,
}
impl CallColumn {
    fn new(fields: Fields) -> Self {
        Self {
            fields,
            first: Vec::with_capacity(8192),
            second: Vec::with_capacity(8192),
            ploidy: Vec::with_capacity(8192),
            phased: BooleanBufferBuilder::new(8192),
            valid: NullBufferBuilder::new(8192),
        }
    }
    fn null(&mut self) {
        self.first.push(0);
        self.second.push(0);
        self.ploidy.push(0);
        self.phased.append(false);
        self.valid.append_null();
    }
    fn finish(&mut self) -> Result<ArrayRef> {
        let len = self.valid.len();
        // Hail Call children share exactly their parent's missingness. Clone
        // this immutable Arrow bitmap instead of appending/materializing five
        // independent validity buffers for every call.
        let nulls = self.valid.finish();
        Ok(Arc::new(StructArray::try_new_with_length(
            self.fields.clone(),
            vec![
                Arc::new(UInt16Array::new(
                    ScalarBuffer::from(std::mem::take(&mut self.first)),
                    nulls.clone(),
                )),
                Arc::new(UInt16Array::new(
                    ScalarBuffer::from(std::mem::take(&mut self.second)),
                    nulls.clone(),
                )),
                Arc::new(UInt8Array::new(
                    ScalarBuffer::from(std::mem::take(&mut self.ploidy)),
                    nulls.clone(),
                )),
                Arc::new(BooleanArray::new(self.phased.finish(), nulls.clone())),
            ],
            nulls,
            len,
        )?))
    }
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
    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(match self {
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
        let fields = children
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
                }
            })
            .collect();
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
        Self {
            e: e.clone(),
            wire: Wire::new(&e.kind),
            append_kind: AppendKind::new(t),
            fields,
            array_skip,
            ty: t.clone(),
            children,
            optional_count,
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
    #[inline(always)]
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
            if count <= 4 {
                // Short primitive arrays stay on the inlined varint
                // fast path; longer arrays use the validated bulk skip.
                for _ in 0..count {
                    input.varint(bits_per_value)?;
                }
            } else {
                input.skip_varints(count, bits_per_value)?;
            }
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
            self.skip_array_elements(input, n, &bits)?;
        }

        Ok(())
    }
    fn skip_array_elements(&self, input: &mut Input, n: usize, bits: &Bitmap) -> Result<()> {
        let child = &self.children[0];
        for i in 0..n {
            if child.e.required || !absent(bits, i) {
                child.skip(input)?;
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
        let v = self.integer(input, 32)? as u32;
        let ploidy = ((v >> 1) & 3) as u8;
        let phased = v & 1 != 0;
        let allele = (v >> 3) as u64;
        let (a, b) = match ploidy {
            0 => (0, 0),
            1 => (allele, 0),
            // Common unphased biallelic calls avoid integer square root
            // for every GT in dense genotype datasets.
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
        let first = u16::try_from(a)?;
        let second = u16::try_from(b)?;
        if let Column::Call(c) = out {
            c.first.push(first);
            c.second.push(second);
            c.ploidy.push(ploidy);
            c.phased.append(phased);
            c.valid.append_non_null();
        } else {
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
    fn append_struct(&self, input: &mut Input, out: &mut Column) -> Result<()> {
        let bits = bitmap(input, self.optional_count)?;
        let Column::Struct(_, outputs, validity) = out else {
            unreachable!("compiled struct output")
        };
        for (op, c) in self.fields.iter().zip(&self.children) {
            let bit = op.missing_bit;
            let missing = bit != usize::MAX && absent(&bits, bit);
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
        if let Type::Locus { locus } = &self.ty {
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
        if matches!(&self.ty,Type::Tuple{tuple}if tuple.is_empty()) {
            outputs[0].bool_builder().append_value(true)
        }
        validity.append_non_null();

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
        if c.e.required
            && n <= 8
            && matches!(c.ty, Type::Primitive(P::Int32 | P::Int64))
            && (matches!(c.wire, Wire::Varint)
                || (input.leb && matches!(c.wire, Wire::Int32 | Wire::Int64)))
        {
            if let Some(bytes) = input.try_small_varints(n)? {
                // A complete run of one-byte integers can share one buffered
                // read and one builder validity append. Continued/negative
                // values and buffer boundaries retain the ordinary decoder.
                match c.ty {
                    Type::Primitive(P::Int32) => {
                        let mut values = [0i32; 8];
                        for (value, byte) in values.iter_mut().zip(bytes).take(n) {
                            *value = i32::from(byte);
                        }
                        child.int32().append_slice(&values[..n]);
                    }
                    Type::Primitive(P::Int64) => {
                        let mut values = [0i64; 8];
                        for (value, byte) in values.iter_mut().zip(bytes).take(n) {
                            *value = i64::from(byte);
                        }
                        child.int64().append_slice(&values[..n]);
                    }
                    _ => unreachable!("compiled short integer array"),
                }
                out.end_list()?;
                return Ok(());
            }
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
    fn read_batch(&mut self, size: usize) -> Result<Option<RecordBatch>> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_integer_arrays_match_scalar_decode_at_boundaries() -> Result<()> {
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
        let path = std::env::temp_dir().join(format!(
            "vail-short-array-decode-{}.bin",
            std::process::id()
        ));
        let result = (|| -> Result<()> {
            for n in 0..=9 {
                for width in [32, 64] {
                    for explicit_varint in [false, true] {
                        for prefix in [0usize, 65534] {
                            for mode in 0..3 {
                                let child = Encoding {
                                    kind: if explicit_varint {
                                        "EVarint"
                                    } else if width == 32 {
                                        "EInt32"
                                    } else {
                                        "EInt64"
                                    }
                                    .into(),
                                    required: true,
                                    fields: vec![],
                                    rank: 0,
                                };
                                let encoding = Encoding {
                                    kind: "EArray2".into(),
                                    required: true,
                                    fields: vec![("element".into(), child)],
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
                                let mut values = (0..n).map(|i| i as i64 + 1).collect::<Vec<_>>();
                                if n != 0 && mode != 0 {
                                    values[n / 2] = if mode == 1 { 128 } else { -1 };
                                }
                                let mut bytes = vec![0; prefix];
                                leb(n as u64, &mut bytes);
                                for value in values {
                                    leb(
                                        if width == 32 {
                                            value as u32 as u64
                                        } else {
                                            value as u64
                                        },
                                        &mut bytes,
                                    );
                                }
                                std::fs::write(&path, &bytes)?;
                                let spec = if explicit_varint {
                                    serde_json::json!({"name":"StreamBufferSpec"})
                                } else {
                                    serde_json::json!({"name":"LEB128BufferSpec", "child":{"name":"StreamBufferSpec"}})
                                };
                                let mut input = Input::open(&path, &spec)?;
                                input.skip(prefix)?;
                                let mut specialized = Column::new(&ty.to_arrow()?);
                                node.append(&mut input, &mut specialized)?;
                                assert_eq!(input.bytes, bytes.len() as u64);
                                let mut input = Input::open(&path, &spec)?;
                                input.skip(prefix)?;
                                let count = node.length(&mut input)?;
                                let mut generic = Column::new(&ty.to_arrow()?);
                                let Column::List(_, child, _, _) = &mut generic else {
                                    unreachable!()
                                };
                                for _ in 0..count {
                                    node.children[0].append(&mut input, child)?;
                                }
                                generic.end_list()?;
                                assert_eq!(
                                    specialized.finish()?.as_ref(),
                                    generic.finish()?.as_ref(),
                                    "n={n} width={width} varint={explicit_varint} prefix={prefix} mode={mode}"
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
    fn primitive_array_inline_skip_keeps_short_and_bulk_varint_validation() -> Result<()> {
        let path =
            std::env::temp_dir().join(format!("vail-inline-array-skip-{}.bin", std::process::id()));
        let result = (|| -> Result<()> {
            for count in [1u8, 2, 4, 5, 8] {
                for optional in [false, true] {
                    for width in [32, 64] {
                        let child = Encoding {
                            kind: if width == 32 { "EInt32" } else { "EInt64" }.into(),
                            required: !optional,
                            fields: vec![],
                            rank: 0,
                        };
                        let encoding = Encoding {
                            kind: "EArray".into(),
                            required: true,
                            fields: vec![("element".into(), child)],
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
                        for truncated in [false, true] {
                            let mut bytes = vec![count];
                            if optional {
                                bytes.push(0);
                            }
                            bytes.extend(std::iter::repeat_n(0, count as usize - 1));
                            if truncated {
                                bytes.push(128);
                            } else if width == 32 {
                                bytes.extend([255, 255, 255, 255, 16]);
                            } else {
                                bytes.extend([255, 255, 255, 255, 255, 255, 255, 255, 255, 2]);
                            }
                            std::fs::write(&path, bytes)?;
                            let spec = serde_json::json!({"name":"LEB128BufferSpec", "child":{"name":"StreamBufferSpec"}});
                            assert!(
                                node.skip(&mut Input::open(&path, &spec)?).is_err(),
                                "count={count} optional={optional} width={width} truncated={truncated}"
                            );
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
