//! Hail-compatible 20-round Threefry and PMAC streams (see Hail Random.scala).
//! The fixed key and construction are from Hail, distributed under MIT.
use crate::expressions::ExprError;
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, new_empty_array};
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use statrs::distribution::{Beta, ContinuousCDF, DiscreteCDF, Poisson};
use std::sync::Arc;

const KEY: [u64; 5] = [
    0x215d6dfdb7dfdf6b,
    0x045cfa043329c49f,
    0x9ec75a93692444dd,
    0x1284681663220f1c,
    0x215d6dfdb7dfdf6b
        ^ 0x045cfa043329c49f
        ^ 0x9ec75a93692444dd
        ^ 0x1284681663220f1c
        ^ 0x1bd11bdaa9fc1a22,
];
const ROT: [[u32; 2]; 8] = [
    [14, 16],
    [52, 57],
    [23, 40],
    [5, 37],
    [25, 33],
    [46, 12],
    [58, 22],
    [32, 32],
];
fn encrypt(mut x: [u64; 4], a: u64, b: u64) -> [u64; 4] {
    let t = [a, b, a ^ b];
    for d in 0..=20 {
        if d % 4 == 0 {
            let s = d / 4;
            x[0] = x[0].wrapping_add(KEY[s % 5]);
            x[1] = x[1].wrapping_add(KEY[(s + 1) % 5]).wrapping_add(t[s % 3]);
            x[2] = x[2]
                .wrapping_add(KEY[(s + 2) % 5])
                .wrapping_add(t[(s + 1) % 3]);
            x[3] = x[3].wrapping_add(KEY[(s + 3) % 5]).wrapping_add(s as u64);
        }
        if d == 20 {
            break;
        }
        x[0] = x[0].wrapping_add(x[1]);
        x[1] = x[1].rotate_left(ROT[d % 8][0]) ^ x[0];
        x[2] = x[2].wrapping_add(x[3]);
        x[3] = x[3].rotate_left(ROT[d % 8][1]) ^ x[2];
        x.swap(1, 3);
    }
    x
}
struct Rng {
    state: [u64; 4],
    tweak: u64,
    counter: u64,
    buffer: [u64; 4],
    used: usize,
    gaussian: Option<f64>,
}
impl Rng {
    fn new(nonce: u64, site: u64, mut uid: impl ExactSizeIterator<Item = u64>) -> Self {
        let mut state = encrypt([nonce, site, 0, 0], u64::MAX, 0);
        let length = uid.len();
        let pad = length == 0 || length % 4 != 0;
        let blocks = length / 4 + usize::from(pad);
        let mut padded = false;
        for i in 0..blocks {
            let mut b = [0; 4];
            for word in &mut b {
                *word = uid.next().unwrap_or_else(|| {
                    if !padded {
                        padded = true;
                        1
                    } else {
                        0
                    }
                });
            }
            if i + 1 < blocks {
                b = encrypt(b, i as u64, 0);
            }
            for j in 0..4 {
                state[j] ^= b[j];
            }
        }
        Self {
            state,
            tweak: if pad {
                (-3_i64) as u64
            } else {
                (-2_i64) as u64
            },
            counter: 0,
            buffer: [0; 4],
            used: 8,
            gaussian: None,
        }
    }
    fn fill(&mut self) {
        self.buffer = encrypt(self.state, self.tweak, self.counter);
        self.counter += 1;
        self.used = 0;
    }
    fn long(&mut self) -> u64 {
        self.used += self.used & 1;
        if self.used >= 8 {
            self.fill();
        }
        let x = self.buffer[self.used / 2];
        self.used += 2;
        x
    }
    fn int(&mut self) -> u32 {
        if self.used >= 8 {
            self.fill();
        }
        let x = self.buffer[self.used / 2];
        self.used += 1;
        (x >> ((self.used & 1) * 32)) as u32
    }
    fn bounded_int(&mut self, n: u32) -> u32 {
        let mult = n as u64 * self.int() as u64;
        let result = (mult >> 32) as u32;
        let fraction = mult & 0xffffffff;
        if fraction < (1_u64 << 32) - n as u64 {
            return result;
        }
        let high = (((n as u64 * self.int() as u64) >> 32) + (n as u64 * self.int() as u64)) >> 32;
        result.wrapping_add(((fraction + high) >> 32) as u32)
    }
    fn bounded_long(&mut self, n: u64) -> u64 {
        loop {
            let x = self.long() >> 1;
            let r = x % n;
            if x - r <= i64::MAX as u64 - (n - 1) {
                return r;
            }
        }
    }
    fn direct_uniform(&mut self) -> f64 {
        let words = encrypt(self.state, self.tweak, 0);
        let mut exponent = 1022_i64;
        let mut bits = words[3];
        for i in (0..3).rev() {
            if bits != 0 {
                break;
            }
            exponent -= 64;
            bits = words[i];
        }
        exponent -= i64::from(bits.trailing_zeros());
        f64::from_bits(((exponent as u64) << 52) | (words[0] >> 12))
    }
    fn uniform(&mut self) -> f64 {
        let mut bits = self.long();
        let mut exponent = 1022_i64;
        while bits == 0 {
            bits = self.long();
            exponent -= 64;
            if exponent < 0 {
                return 0.0;
            }
        }
        let e = bits.trailing_zeros();
        exponent -= i64::from(e);
        let significand = if e < 12 { bits } else { self.long() } >> 12;
        if exponent <= 0 {
            return (significand as f64 / (1_u64 << 52) as f64 + 1.0)
                * 2.0_f64.powi((exponent - 1023) as i32);
        }
        f64::from_bits(((exponent as u64) << 52) | significand)
    }
    fn normal(&mut self) -> f64 {
        if let Some(x) = self.gaussian.take() {
            return x;
        }
        loop {
            let x = 2.0 * self.uniform() - 1.0;
            let y = 2.0 * self.uniform() - 1.0;
            let s = x * x + y * y;
            if s > 0.0 && s < 1.0 {
                let m = (-2.0 * s.ln() / s).sqrt();
                self.gaussian = Some(y * m);
                return x * m;
            }
        }
    }
    fn gamma(&mut self, shape: f64) -> f64 {
        if shape < 1.0 {
            return self.gamma(shape + 1.0) * self.uniform().powf(1.0 / shape);
        }
        let d = shape - 1.0 / 3.0;
        let c = (9.0 * d).sqrt().recip();
        loop {
            let x = self.normal();
            let v = 1.0 + c * x;
            if v <= 0.0 {
                continue;
            }
            let v = v * v * v;
            let u = self.uniform();
            if u < 1.0 - 0.0331 * x.powi(4) || u.ln() < 0.5 * x * x + d * (1.0 - v + v.ln()) {
                return d * v;
            }
        }
    }
    /// The raw value and stream are identical to gamma(). A common scale
    /// keeps log weights finite even for subnormal positive concentrations.
    fn gamma_with_scaled_log(&mut self, shape: f64, scale: f64) -> (f64, f64) {
        if shape < 1.0 {
            let base = self.gamma(shape + 1.0);
            let u = self.uniform();
            (
                base * u.powf(1.0 / shape),
                scale * base.ln() + (scale / shape) * u.max(f64::from_bits(1)).ln(),
            )
        } else {
            let value = self.gamma(shape);
            (value, scale * value.ln())
        }
    }
    fn hyper(&mut self, mut pop: i64, mut good: i64, sample: i64) -> i32 {
        let mut result = 0;
        for _ in 0..sample {
            if self.uniform() < good as f64 / pop as f64 {
                result += 1;
                good -= 1;
            }
            pop -= 1;
        }
        result
    }
}
fn error(s: &str) -> ExprError {
    ExprError::Invalid(format!("random distribution: {s}"))
}
enum Kind<'a> {
    I32(&'a arrow_array::Int32Array),
    I64(&'a Int64Array),
    F64(&'a arrow_array::Float64Array),
    List(&'a ListArray),
    ListI64(&'a ListArray, &'a Int64Array),
    ListI32(&'a ListArray, &'a arrow_array::Int32Array),
    ListF64(&'a ListArray, &'a arrow_array::Float64Array),
    Other,
}
struct Arg<'a> {
    array: &'a ArrayRef,
    scalar: bool,
    kind: Kind<'a>,
}
impl<'a> Arg<'a> {
    fn new(array: &'a ArrayRef, scalar: bool) -> Self {
        let kind = match array.data_type() {
            DataType::Int32 => Kind::I32(array.as_any().downcast_ref().unwrap()),
            DataType::Int64 => Kind::I64(array.as_any().downcast_ref().unwrap()),
            DataType::Float64 => Kind::F64(array.as_any().downcast_ref().unwrap()),
            DataType::List(item) => {
                let list: &ListArray = array.as_any().downcast_ref().unwrap();
                match item.data_type() {
                    DataType::Int64 => {
                        Kind::ListI64(list, list.values().as_any().downcast_ref().unwrap())
                    }
                    DataType::Int32 => {
                        Kind::ListI32(list, list.values().as_any().downcast_ref().unwrap())
                    }
                    DataType::Float64 => {
                        Kind::ListF64(list, list.values().as_any().downcast_ref().unwrap())
                    }
                    _ => Kind::List(list),
                }
            }
            _ => Kind::Other,
        };
        Self {
            array,
            scalar,
            kind,
        }
    }
    fn row(&self, row: usize) -> usize {
        if self.scalar { 0 } else { row }
    }
    fn null(&self, row: usize) -> bool {
        self.array.is_null(self.row(row))
    }
    fn integer(&self, row: usize) -> Result<i64, ExprError> {
        let row = self.row(row);
        match self.kind {
            Kind::I32(a) => Ok(i64::from(a.value(row))),
            Kind::I64(a) => Ok(a.value(row)),
            _ => Err(error("integer argument required")),
        }
    }
    fn number(&self, row: usize) -> Result<f64, ExprError> {
        let row = self.row(row);
        match self.kind {
            Kind::I32(a) => Ok(f64::from(a.value(row))),
            Kind::I64(a) => Ok(a.value(row) as f64),
            Kind::F64(a) => Ok(a.value(row)),
            _ => Err(error("numeric argument required")),
        }
    }
    fn list_ref(&self) -> Result<&'a ListArray, ExprError> {
        match self.kind {
            Kind::List(a) | Kind::ListI64(a, _) | Kind::ListI32(a, _) | Kind::ListF64(a, _) => {
                Ok(a)
            }
            _ => Err(error("array argument required")),
        }
    }
    fn range(&self, row: usize) -> Result<std::ops::Range<usize>, ExprError> {
        let list = self.list_ref()?;
        let row = self.row(row);
        let start = list.value_offsets()[row] as usize;
        let end = list.value_offsets()[row + 1] as usize;
        if list
            .values()
            .nulls()
            .is_some_and(|n| n.slice(start, end - start).null_count() > 0)
        {
            return Err(error("array elements must be defined"));
        }
        Ok(start..end)
    }
    fn uid(&self, row: usize) -> Result<&'a [i64], ExprError> {
        let range = self.range(row)?;
        match self.kind {
            Kind::ListI64(_, a) => Ok(&a.values()[range]),
            _ => Err(error("invalid UID words")),
        }
    }
    fn colors(&self, row: usize) -> Result<&'a [i32], ExprError> {
        let range = self.range(row)?;
        match self.kind {
            Kind::ListI32(_, a) => Ok(&a.values()[range]),
            _ => Err(error("integer array argument required")),
        }
    }
    fn numbers(&self, row: usize) -> Result<&'a [f64], ExprError> {
        let range = self.range(row)?;
        match self.kind {
            Kind::ListF64(_, a) => Ok(&a.values()[range]),
            _ => Err(error("float64 array argument required")),
        }
    }
}
fn floats(values: Vec<f64>) -> Result<ScalarValue, ExprError> {
    let n = values.len();
    Ok(ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(arrow_schema::Field::new("item", DataType::Float64, true)),
        arrow_buffer::OffsetBuffer::from_lengths([n]),
        Arc::new(arrow_array::Float64Array::from(values)),
        None,
    ))))
}
fn ints(values: Vec<i32>) -> Result<ScalarValue, ExprError> {
    let n = values.len();
    Ok(ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(arrow_schema::Field::new("item", DataType::Int32, true)),
        arrow_buffer::OffsetBuffer::from_lengths([n]),
        Arc::new(arrow_array::Int32Array::from(values)),
        None,
    ))))
}
fn categorical_total(weights: &[f64]) -> Result<f64, ExprError> {
    let total = weights.iter().sum::<f64>();
    if weights.iter().any(|v| !v.is_finite() || *v < 0.0) || !total.is_finite() || total <= 0.0 {
        Err(error("invalid categorical weights"))
    } else {
        Ok(total)
    }
}
fn uniform_between(lo: f64, hi: f64, u: f64) -> f64 {
    let width = hi - lo;
    if width.is_finite() {
        lo + width * u
    } else {
        lo * (1.0 - u) + hi * u
    }
}
pub(crate) fn validate(
    function: &str,
    args: &[crate::expressions::Type],
    output: &crate::expressions::Type,
) -> Result<(), ExprError> {
    use crate::expressions::{PrimitiveType as P, Type as T};
    let i32t = T::Primitive(P::Int32);
    let i64t = T::Primitive(P::Int64);
    let f64t = T::Primitive(P::Float64);
    let boolean = T::Primitive(P::Bool);
    let array = |t: T| T::Array { array: Box::new(t) };
    if args.len() < 3 || args[0] != i64t || args[1] != i64t || args[2] != array(i64t.clone()) {
        return Err(error("invalid random stream identity"));
    }
    let (expected, result) = match function {
        "int32" => (vec![i32t.clone(), i32t.clone()], i32t.clone()),
        "int64" if args.len() == 3 => (vec![], i64t.clone()),
        "int64" => (vec![i64t.clone(), i64t.clone()], i64t.clone()),
        "unif" | "norm" | "gamma" => (vec![f64t.clone(); 2], f64t.clone()),
        "bool" => (vec![f64t.clone()], boolean),
        "pois" => (vec![f64t.clone()], f64t.clone()),
        "beta" => (vec![f64t.clone(); 4], f64t.clone()),
        "cat" => (vec![array(f64t.clone())], i32t.clone()),
        "dirichlet" => (vec![array(f64t.clone())], array(f64t.clone())),
        "hyper" => (vec![i32t.clone(); 3], i32t.clone()),
        "multi_hyper" => (vec![array(i32t.clone()), i32t.clone()], array(i32t)),
        "norm2d" => (vec![array(f64t.clone()); 2], array(f64t.clone())),
        "norm_nd" | "unif_nd" => (
            vec![i64t.clone(), i64t, f64t.clone(), f64t.clone()],
            T::Ndarray {
                ndarray: Box::new((f64t, 2)),
            },
        ),
        "shuffle" if args.len() == 4 && matches!(&args[3], T::Array { .. }) => {
            (vec![args[3].clone()], args[3].clone())
        }
        _ => return Err(error("unknown distribution or invalid argument count")),
    };
    if args[3..] != expected || *output != result {
        return Err(error("invalid argument or result types"));
    }
    Ok(())
}

pub(crate) fn evaluate(
    function: &str,
    values: &[datafusion_expr::ColumnarValue],
    dtype: &DataType,
    len: usize,
) -> Result<ArrayRef, ExprError> {
    use datafusion_expr::ColumnarValue;
    if len == 0 {
        return Ok(new_empty_array(dtype));
    }
    let arrays = values
        .iter()
        .map(|v| match v {
            ColumnarValue::Array(a) => Ok(a.clone()),
            ColumnarValue::Scalar(v) => v.to_array(),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let inputs = arrays
        .iter()
        .zip(values)
        .map(|(a, v)| Arg::new(a, matches!(v, ColumnarValue::Scalar(_))))
        .collect::<Vec<_>>();
    if inputs.len() < 3 {
        return Err(error("missing random stream identity"));
    }
    if function == "shuffle" {
        return shuffle(&inputs, dtype, len);
    }
    macro_rules! sample {
        (@primitive,$builder:ty,$variant:ident,$row:ident,$rng:ident,$a:ident,$p:ident,$list:ident,$body:block) => {{
            let mut out = <$builder>::with_capacity(len);
            for $row in 0..len {
                if inputs.iter().any(|argument| argument.null($row)) {
                    out.append_null();
                    continue;
                }
                #[allow(unused_mut)]
                let mut $rng = stream(&inputs, $row)?;
                #[allow(unused_variables)]
                let $a = &inputs[3..];
                #[allow(unused_variables)]
                let $p = |i: usize| $a[i].number($row);
                #[allow(unused_variables)]
                let $list = |i: usize| $a[i].numbers($row);
                let ScalarValue::$variant(Some(value)) = ($body) else {
                    unreachable!("typed random scalar output");
                };
                out.append_value(value);
            }
            Ok(Arc::new(out.finish()) as ArrayRef)
        }};

        ($row:ident,$rng:ident,$a:ident,$p:ident,$list:ident,$body:block) => {{
            let mut out = Vec::with_capacity(len);
            for $row in 0..len {
                if inputs.iter().any(|argument| argument.null($row)) {
                    out.push(ScalarValue::try_new_null(dtype)?);
                    continue;
                }
                #[allow(unused_mut)]
                let mut $rng = stream(&inputs, $row)?;
                #[allow(unused_variables)]
                let $a = &inputs[3..];
                #[allow(unused_variables)]
                let $p = |i: usize| $a[i].number($row);
                #[allow(unused_variables)]
                let $list = |i: usize| $a[i].numbers($row);
                let value = $body;
                out.push(value);
            }
            Ok(ScalarValue::iter_to_array(out)?)
        }};
    }
    match function {
        "int64" if inputs.len() == 3 => {
            sample!(@primitive,arrow_array::builder::Int64Builder,Int64,row, rng, a, p, list, {
                ScalarValue::Int64(Some(rng.long() as i64))
            })
        }
        "int32" => {
            sample!(@primitive,arrow_array::builder::Int32Builder,Int32,row, rng, a, p, list, {
                let lo = p(0)? as i64;
                let hi = p(1)? as i64;
                if hi <= lo {
                    return Err(error("empty integer interval"));
                }
                ScalarValue::Int32(Some((lo + rng.bounded_int((hi - lo) as u32) as i64) as i32))
            })
        }
        "int64" => {
            sample!(@primitive,arrow_array::builder::Int64Builder,Int64,row, rng, a, p, list, {
                let lo = a[0].integer(row)?;
                let hi = a[1].integer(row)?;
                let n = hi as i128 - lo as i128;
                if n <= 0 {
                    return Err(error("empty integer interval"));
                }
                let offset = if n <= i64::MAX as i128 {
                    rng.bounded_long(n as u64)
                } else {
                    loop {
                        let x = rng.long();
                        if (x as u128) < ((u64::MAX as u128 + 1) / n as u128) * n as u128 {
                            break x % (n as u64);
                        }
                    }
                };
                ScalarValue::Int64(Some(lo.wrapping_add(offset as i64)))
            })
        }
        "unif" => {
            sample!(@primitive,arrow_array::builder::Float64Builder,Float64,row, rng, a, p, list, {
                let lo = p(0)?;
                let hi = p(1)?;
                if !lo.is_finite() || !hi.is_finite() || hi < lo {
                    return Err(error("invalid uniform bounds"));
                }
                ScalarValue::Float64(Some(uniform_between(lo, hi, rng.direct_uniform())))
            })
        }
        "norm" => {
            sample!(@primitive,arrow_array::builder::Float64Builder,Float64,row, rng, a, p, list, {
                let sd = p(1)?;
                if sd < 0.0 || !sd.is_finite() {
                    return Err(error("negative or nonfinite standard deviation"));
                }
                ScalarValue::Float64(Some(p(0)? + sd * rng.normal()))
            })
        }
        "norm_nd" | "unif_nd" => sample!(row, rng, a, p, list, {
            let rows = a[0].integer(row)?;
            let cols = a[1].integer(row)?;
            let count = rows
                .checked_mul(cols)
                .filter(|_| rows >= 0 && cols >= 0)
                .ok_or_else(|| error("invalid ndarray dimensions"))?;
            let count = usize::try_from(count).map_err(|_| error("ndarray is too large"))?;
            let lo = p(2)?;
            let hi = p(3)?;
            if function == "norm_nd" && (hi < 0.0 || !hi.is_finite()) {
                return Err(error("invalid standard deviation"));
            }
            if function == "unif_nd" && (hi < lo || !lo.is_finite() || !hi.is_finite()) {
                return Err(error("invalid uniform bounds"));
            }
            let mut values = Vec::new();
            values
                .try_reserve(count)
                .map_err(|_| error("ndarray is too large"))?;
            let draw: fn(&mut Rng, f64, f64) -> f64 = if function == "norm_nd" {
                |rng, mean, sd| mean + sd * rng.normal()
            } else {
                |rng, lo, hi| uniform_between(lo, hi, rng.uniform())
            };
            for _ in 0..count {
                values.push(draw(&mut rng, lo, hi));
            }
            let ScalarValue::List(data) = floats(values)? else {
                unreachable!()
            };
            let shape = ScalarValue::new_list(
                &[
                    ScalarValue::Int64(Some(rows)),
                    ScalarValue::Int64(Some(cols)),
                ],
                &DataType::Int64,
                true,
            );
            let DataType::Struct(fields) = dtype else {
                return Err(error("invalid ndarray output type"));
            };
            ScalarValue::Struct(Arc::new(arrow_array::StructArray::try_new(
                fields.clone(),
                vec![
                    arrow_cast::cast(data.as_ref(), fields[0].data_type())?,
                    arrow_cast::cast(shape.as_ref(), fields[1].data_type())?,
                ],
                None,
            )?))
        }),
        "bool" => {
            sample!(@primitive,arrow_array::builder::BooleanBuilder,Boolean,row, rng, a, p, list, {
                let p = p(0)?;
                if !(0.0..=1.0).contains(&p) {
                    return Err(error("probability outside [0,1]"));
                }
                ScalarValue::Boolean(Some(rng.direct_uniform() < p))
            })
        }
        "gamma" => {
            sample!(@primitive,arrow_array::builder::Float64Builder,Float64,row, rng, a, p, list, {
                let s = p(0)?;
                let scale = p(1)?;
                if s <= 0.0 || !s.is_finite() || scale < 0.0 || !scale.is_finite() {
                    return Err(error("invalid gamma parameters"));
                }
                ScalarValue::Float64(Some(rng.gamma(s) * scale))
            })
        }
        "beta" => {
            sample!(@primitive,arrow_array::builder::Float64Builder,Float64,row, rng, a, p, list, {
                let dist = Beta::new(p(0)?, p(1)?).map_err(|_| error("invalid beta parameters"))?;
                let lo = p(2)?;
                let hi = p(3)?;
                if lo < 0.0 || hi > 1.0 || hi <= lo {
                    return Err(error("invalid beta truncation"));
                }
                let u = dist.cdf(lo) + (dist.cdf(hi) - dist.cdf(lo)) * rng.uniform();
                ScalarValue::Float64(Some(dist.inverse_cdf(u)))
            })
        }
        "pois" => {
            sample!(@primitive,arrow_array::builder::Float64Builder,Float64,row, rng, a, p, list, {
                let rate = p(0)?;
                if rate == 0.0 {
                    ScalarValue::Float64(Some(0.0))
                } else {
                    let d = Poisson::new(rate).map_err(|_| error("invalid Poisson rate"))?;
                    let u = rng.uniform();
                    let mut lo = 0_u64;
                    let mut hi = (rate + 10.0 * rate.sqrt() + 10.0).ceil() as u64;
                    while d.cdf(hi) < u {
                        hi = hi
                            .checked_mul(2)
                            .ok_or_else(|| error("Poisson rate too large"))?;
                    }
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        if d.cdf(mid) >= u {
                            hi = mid;
                        } else {
                            lo = mid + 1;
                        }
                    }
                    ScalarValue::Float64(Some(lo as f64))
                }
            })
        }
        "dirichlet" => sample!(row, rng, a, p, list, {
            let shapes = list(0)?;
            if shapes.iter().any(|x| *x < 0.0 || !x.is_finite()) {
                return Err(error("invalid Dirichlet concentration"));
            }
            let maximum = shapes.iter().copied().fold(0.0, f64::max);
            if maximum == 0.0 {
                return Err(error(
                    "Dirichlet concentrations must include a positive value",
                ));
            }
            let log_scale = shapes
                .iter()
                .copied()
                .filter(|&v| v > 0.0)
                .fold(f64::INFINITY, f64::min);
            // Shapes >=1 have positive base-Gamma draws. Only shapes <1
            // apply U^(1/shape), which can underflow every raw vector element.
            let track_logs = maximum > 0.0 && maximum < 1.0;
            let mut values = Vec::with_capacity(shapes.len());
            let mut logs = if track_logs {
                Vec::with_capacity(shapes.len())
            } else {
                Vec::new()
            };
            for &shape in shapes {
                if shape == 0.0 {
                    values.push(0.0);
                    if track_logs {
                        logs.push(f64::NEG_INFINITY);
                    }
                } else if track_logs {
                    let (value, log) = rng.gamma_with_scaled_log(shape, log_scale);
                    values.push(value);
                    logs.push(log);
                } else {
                    values.push(rng.gamma(shape));
                }
            }
            let total = values.iter().sum::<f64>();
            if total.is_infinite() {
                let maximum = values.iter().copied().fold(0.0, f64::max);
                for value in &mut values {
                    *value /= maximum;
                }
                let total = values.iter().sum::<f64>();
                floats(values.into_iter().map(|v| v / total).collect())?
            } else if total == 0.0 && track_logs {
                let maximum = logs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                if !maximum.is_finite() {
                    return Err(error("Dirichlet log weights require a finite value"));
                }
                let weights = logs
                    .into_iter()
                    .map(|v| ((v - maximum) / log_scale).exp())
                    .collect::<Vec<_>>();
                let total = weights.iter().sum::<f64>();
                floats(weights.into_iter().map(|v| v / total).collect())?
            } else {
                floats(values.into_iter().map(|v| v / total).collect())?
            }
        }),
        "cat" => {
            let mut constant_total = None;
            sample!(@primitive,arrow_array::builder::Int32Builder,Int32,row, rng, a, p, list, {
                let weights=list(0)?;
                let total=if a[0].scalar {
                    if let Some(total)=constant_total {total} else {let total=categorical_total(weights)?;constant_total=Some(total);total}
                } else {categorical_total(weights)?};
                let mut u = total * rng.uniform();
                let mut index = weights.len() - 1;
                for (i, w) in weights.iter().enumerate() {
                    u -= w;
                    if u < 0.0 {
                        index = i;
                        break;
                    }
                }
                ScalarValue::Int32(Some(index as i32))
            })
        }
        "hyper" => {
            sample!(@primitive,arrow_array::builder::Int32Builder,Int32,row, rng, a, p, list, {
                let pop = p(0)? as i64;
                let good = p(1)? as i64;
                let sample = p(2)? as i64;
                if pop < 0 || good < 0 || good > pop || sample < 0 || sample > pop {
                    return Err(error("invalid hypergeometric parameters"));
                }
                ScalarValue::Int32(Some(if sample == 0 || good == 0 {
                    0
                } else if good == pop {
                    sample as i32
                } else if sample == pop {
                    good as i32
                } else {
                    rng.hyper(pop, good, sample)
                }))
            })
        }
        "multi_hyper" => sample!(row, rng, a, p, list, {
            let colors = a[0].colors(row)?;
            let mut pop = colors.iter().map(|&v| i64::from(v)).sum::<i64>();
            let mut sample = p(1)? as i64;
            if colors.iter().any(|x| *x < 0) || sample < 0 || sample > pop {
                return Err(error("invalid multivariate hypergeometric parameters"));
            }
            let mut values = Vec::with_capacity(colors.len());
            let mut remaining_colors = colors.iter().filter(|&&v| v > 0).count();
            for &color in colors {
                let good = i64::from(color);
                // Skip draws only after all remaining outcomes are deterministic.
                // Earlier zero-color draws affect later seeded samples.
                let x = if sample == 0 {
                    0
                } else if sample == pop {
                    good as i32
                } else if remaining_colors <= 1 {
                    if good > 0 { sample as i32 } else { 0 }
                } else {
                    rng.hyper(pop, good, sample)
                };
                if good > 0 {
                    remaining_colors -= 1;
                }
                pop -= good;
                sample -= x as i64;
                values.push(x);
            }
            ints(values)?
        }),
        "norm2d" => sample!(row, rng, a, p, list, {
            let mean = list(0)?;
            let cov = list(1)?;
            if mean.len() != 2
                || cov.len() != 3
                || cov.iter().any(|x| !x.is_finite())
                || cov[0] < 0.0
                || cov[2] < 0.0
                || mean.iter().any(|x| !x.is_finite())
            {
                return Err(error("invalid bivariate normal parameters"));
            }
            let sx = cov[0].sqrt();
            let sy = cov[2].sqrt();
            let scale = sx.max(sy);
            let bound = if scale == 0.0 { 0.0 } else { (sx / scale) * sy };
            // Compare the covariance after scaling, so finite PSD matrices
            // never require forming variance products or covariance squares.
            let scaled = if scale == 0.0 { cov[1] } else { cov[1] / scale };
            if ((sx == 0.0 || sy == 0.0) && cov[1] != 0.0) || scaled.abs() > bound {
                return Err(error("invalid bivariate normal parameters"));
            }
            let correlation = if sx == 0.0 || sy == 0.0 {
                0.0
            } else {
                (cov[1] / sx) / sy
            };
            let x = rng.normal();
            let y = rng.normal();
            let output_x = mean[0] + sx * x;
            let output_y = mean[1]
                + sy * (correlation * x + (1.0 - correlation * correlation).max(0.0).sqrt() * y);
            floats(vec![output_x, output_y])?
        }),
        _ => Err(error("unknown distribution")),
    }
}
fn stream(inputs: &[Arg<'_>], row: usize) -> Result<Rng, ExprError> {
    let uid = inputs[2].uid(row)?;
    Ok(Rng::new(
        inputs[0].integer(row)? as u64,
        inputs[1].integer(row)? as u64,
        uid.iter().map(|x| *x as u64),
    ))
}
fn shuffle(inputs: &[Arg<'_>], dtype: &DataType, len: usize) -> Result<ArrayRef, ExprError> {
    let values = inputs[3].list_ref()?;
    let mut indices = Vec::new();
    let mut lengths = Vec::with_capacity(len);
    let mut valid = Vec::with_capacity(len);
    for row in 0..len {
        if inputs.iter().any(|a| a.null(row)) {
            lengths.push(0);
            valid.push(false);
            continue;
        }
        let mut rng = stream(inputs, row)?;
        let ar = inputs[3].row(row);
        let start = values.value_offsets()[ar] as usize;
        let end = values.value_offsets()[ar + 1] as usize;
        let count = end - start;
        let offset = indices.len();
        i32::try_from(offset + count)
            .map_err(|_| error("shuffled batch exceeds Arrow offset capacity"))?;
        indices
            .try_reserve(count)
            .map_err(|_| error("shuffled array is too large"))?;
        indices.extend((start..end).map(|i| i as u32));
        for i in (1..count).rev() {
            let j = rng.bounded_long((i + 1) as u64) as usize;
            indices.swap(offset + i, offset + j);
        }
        lengths.push(count);
        valid.push(true);
    }
    let values = arrow_select::take::take(
        values.values().as_ref(),
        &arrow_array::UInt32Array::from(indices),
        None,
    )?;
    let DataType::List(item) = dtype else {
        return Err(error("shuffle requires list output"));
    };
    Ok(Arc::new(ListArray::try_new(
        item.clone(),
        arrow_buffer::OffsetBuffer::from_lengths(lengths),
        values,
        Some(arrow_buffer::NullBuffer::from(valid)),
    )?))
}
