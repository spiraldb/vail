use super::input::Input;
use super::*;
use arrow_array::{builder::*, *};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef, Fields};
use vail_execution::expressions::PrimitiveType as P;

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
    Struct(Fields, Vec<Column>, Vec<bool>),
    List(FieldRef, Box<Column>, Vec<i32>, Vec<bool>),
}
impl Column {
    fn new(t: &DataType) -> Self {
        match t {
            DataType::Struct(fs) => Self::Struct(
                fs.clone(),
                fs.iter().map(|f| Self::new(f.data_type())).collect(),
                vec![],
            ),
            DataType::List(f) => Self::List(
                f.clone(),
                Box::new(Self::new(f.data_type())),
                vec![0],
                vec![],
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
            DataType::Struct(fields) if node.e.kind == "EBaseStruct" => {
                let children = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| match node.dest.iter().position(|&d| d == i) {
                        Some(j) if !node.project[j] => {
                            Self::Skipped(f.data_type().clone(), 0, None)
                        }
                        Some(j) => Self::projected(&node.children[j], f.data_type()),
                        None => Self::new(f.data_type()),
                    })
                    .collect();
                Self::Struct(fields.clone(), children, vec![])
            }
            DataType::List(field) if node.e.kind.starts_with("EArray") => Self::List(
                field.clone(),
                Box::new(Self::projected(&node.children[0], field.data_type())),
                vec![0],
                vec![],
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
    fn children(&mut self) -> &mut Vec<Column> {
        let Self::Struct(_, c, _) = self else {
            unreachable!()
        };
        c
    }
    fn valid(&mut self, v: bool) {
        let Self::Struct(_, _, valid) = self else {
            unreachable!()
        };
        valid.push(v)
    }
    fn len(&self) -> usize {
        match self {
            Self::Skipped(_, n, _) => *n,
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
            Self::Struct(_, c, v) => {
                for c in c {
                    c.null()?
                }
                v.push(false)
            }
            Self::List(_, _, o, v) => {
                o.push(*o.last().unwrap());
                v.push(false)
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
    fn end_list(&mut self) -> Result<()> {
        let Self::List(_, c, o, v) = self else {
            unreachable!()
        };
        o.push(i32::try_from(c.len())?);
        v.push(true);
        Ok(())
    }
    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(match self {
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
                let arrays = c.iter_mut().map(Self::finish).collect::<Result<_>>()?;
                Arc::new(StructArray::try_new_with_length(
                    fs.clone(),
                    arrays,
                    Some(NullBuffer::from(std::mem::take(v))),
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
                    Some(NullBuffer::from(std::mem::take(v))),
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
fn integer(input: &mut Input, e: &Encoding, bits: u32) -> Result<i64> {
    if e.kind == "EVarint" {
        let v = input.varint(64)?;
        Ok(if bits == 32 {
            if v > u32::MAX as u64 {
                return Err("Hail int32 varint overflow".into());
            }
            v as u32 as i32 as i64
        } else {
            v as i64
        })
    } else {
        input.int(bits)
    }
}
enum Bitmap {
    Inline([u8; 8]),
    Heap(Vec<u8>),
}
fn bitmap(input: &mut Input, n: usize) -> Result<Bitmap> {
    let bytes = n.div_ceil(8);
    if bytes <= 8 {
        let mut bits = [0; 8];
        for byte in &mut bits[..bytes] {
            *byte = input.byte()?;
        }
        Ok(Bitmap::Inline(bits))
    } else {
        Ok(Bitmap::Heap(input.bytes(bytes)?))
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

/// Compile physical destination mapping once, outside the row loop.
struct Node {
    e: Encoding,
    wire: Wire,
    ty: Type,
    children: Vec<Node>,
    dest: Vec<usize>,
    project: Vec<bool>,
    optional_count: usize,
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
        let project = vec![true; children.len()];
        let optional_count = children.iter().filter(|c| !c.e.required).count();
        Self {
            e: e.clone(),
            wire: Wire::new(&e.kind),
            ty: t.clone(),
            children,
            dest,
            project,
            optional_count,
        }
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
                self.project[i] &= !selected.is_empty();
                if self.project[i] {
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
            Wire::Binary => {
                let n = input.length(&self.e)?;
                input.skip(n)?
            }
            Wire::Struct => {
                let bits = bitmap(input, self.optional_count)?;
                let mut optional = 0;
                for c in &self.children {
                    let missing = if c.e.required {
                        false
                    } else {
                        let b = absent(&bits, optional);
                        optional += 1;
                        b
                    };
                    if !missing {
                        c.skip(input)?
                    }
                }
            }
            Wire::Array => {
                let n = input.length(&self.e)?;
                let c = &self.children[0];
                let bits = if c.e.required {
                    Bitmap::Inline([0; 8])
                } else {
                    bitmap(input, n)?
                };
                let varint_bits = match c.wire {
                    Wire::Int32 if input.leb => Some(32),
                    Wire::Int64 if input.leb => Some(64),
                    Wire::Varint => Some(64),
                    _ => None,
                };
                if let Some(bits_per_value) = varint_bits {
                    let count = if c.e.required {
                        n
                    } else {
                        bits.present_count(n)
                    };
                    input.skip_varints(count, bits_per_value)?;
                } else if let Some(width) = c.fixed_width(input.leb) {
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
        }
        Ok(())
    }
    fn append(&self, input: &mut Input, out: &mut Column) -> Result<()> {
        match &self.ty {
            Type::Primitive(P::Bool) => out.bool_builder().append_value(boolean(input)?),
            Type::Primitive(P::Int32) => out
                .int32()
                .append_value(integer(input, &self.e, 32)? as i32),
            Type::Primitive(P::Int64) => out.int64().append_value(integer(input, &self.e, 64)?),
            Type::Primitive(P::Float32) => out
                .float32()
                .append_value(f32::from_le_bytes(input.fixed()?)),
            Type::Primitive(P::Float64) => out
                .float64()
                .append_value(f64::from_le_bytes(input.fixed()?)),
            Type::Primitive(P::Str) => {
                let n = input.length(&self.e)?;
                let bytes = input.bytes(n)?;
                out.string().append_value(std::str::from_utf8(&bytes)?);
            }
            Type::Primitive(P::Call) => {
                let v = integer(input, &self.e, 32)? as u32;
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
                let c = out.children();
                c[0].uint16().append_value(u16::try_from(a)?);
                c[1].uint16().append_value(u16::try_from(b)?);
                c[2].uint8().append_value(ploidy);
                c[3].bool_builder().append_value(phased);
                out.valid(true)
            }
            Type::Struct { .. }
            | Type::Tuple { .. }
            | Type::Interval { .. }
            | Type::Locus { .. } => {
                let bits = bitmap(input, self.optional_count)?;
                let mut optional = 0;
                for (i, c) in self.children.iter().enumerate() {
                    let missing = if c.e.required {
                        false
                    } else {
                        let b = absent(&bits, optional);
                        optional += 1;
                        b
                    };
                    if !self.project[i] {
                        if !missing {
                            c.skip(input)?
                        }
                        out.children()[self.dest[i]].null()?;
                        continue;
                    }
                    let target = &mut out.children()[self.dest[i]];
                    if missing {
                        target.null()?
                    } else if matches!(self.ty, Type::Interval { .. })
                        && self.e.fields[i].0 == "includesStart"
                    {
                        target.bool_builder().append_value(!boolean(input)?)
                    } else {
                        c.append(input, target)?
                    }
                }
                if let Type::Locus { locus } = &self.ty {
                    let c = out.children();
                    let builder = c[1].string();
                    let bytes = builder.values_slice();
                    let offsets = builder.offsets_slice();
                    let n = offsets.len();
                    let contig = std::str::from_utf8(
                        &bytes[offsets[n - 2] as usize..offsets[n - 1] as usize],
                    )?;
                    let ordinal = locus.ordinal(contig).ok_or("invalid Hail locus contig")?;
                    c[0].int32().append_value(ordinal);
                }
                if matches!(&self.ty,Type::Tuple{tuple}if tuple.is_empty()) {
                    out.children()[0].bool_builder().append_value(true)
                }
                out.valid(true);
            }
            Type::Array { .. } | Type::Set { .. } | Type::Dict { .. } => {
                let n = input.length(&self.e)?;
                let c = &self.children[0];
                let bits = if c.e.required {
                    Bitmap::Inline([0; 8])
                } else {
                    bitmap(input, n)?
                };
                let Column::List(_, child, _, _) = out else {
                    unreachable!()
                };
                for i in 0..n {
                    if !c.e.required && absent(&bits, i) {
                        child.null()?
                    } else {
                        c.append(input, child)?
                    }
                }
                out.end_list()?
            }
            Type::Ndarray { ndarray } => {
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
            }
        }
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
    let mut column = Column::new(&ty.to_arrow()?);
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
        node.project
            .iter_mut()
            .enumerate()
            .for_each(|(i, p)| *p = projection.contains(&node.dest[i]));
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
                        .dest
                        .iter()
                        .position(|&d| d == self.projection[i])
                        .unwrap();
                    if !self.node.project[physical] {
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
            let mut optional = 0;
            for (i, c) in self.node.children.iter().enumerate() {
                let missing = if c.e.required {
                    false
                } else {
                    let b = absent(&bits, optional);
                    optional += 1;
                    b
                };
                if let Some(dest) = self.projection.iter().position(|p| *p == self.node.dest[i]) {
                    if missing {
                        columns[dest].null()?
                    } else if !self.node.project[i]
                        || self
                            .decode_columns
                            .as_ref()
                            .is_some_and(|columns| !columns.contains(&self.node.dest[i]))
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
