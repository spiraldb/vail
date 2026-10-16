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
    fn new(nonce: u64, site: u64, uid: &[u64]) -> Self {
        let mut state = encrypt([nonce, site, 0, 0], u64::MAX, 0);
        let mut padded = uid.to_vec();
        let pad = uid.is_empty() || uid.len() % 4 != 0;
        if pad {
            padded.push(1);
            while padded.len() % 4 != 0 {
                padded.push(0);
            }
        }
        let n = padded.len() / 4;
        for (i, block) in padded.chunks_exact(4).enumerate() {
            let mut b = [block[0], block[1], block[2], block[3]];
            if i + 1 < n {
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
fn number(a: &ArrayRef, row: usize) -> Result<f64, ExprError> {
    match ScalarValue::try_from_array(a, row)? {
        ScalarValue::Float64(Some(x)) => Ok(x),
        ScalarValue::Int32(Some(x)) => Ok(x as f64),
        ScalarValue::Int64(Some(x)) => Ok(x as f64),
        _ => Err(error("numeric argument required")),
    }
}
fn integers(a: &ArrayRef, row: usize) -> Result<Vec<f64>, ExprError> {
    let a = a
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| error("array argument required"))?;
    let values = a.value(row);
    (0..values.len())
        .map(|i| {
            if values.is_null(i) {
                Err(error("array elements must be defined"))
            } else {
                number(&values, i)
            }
        })
        .collect()
}
fn floats(values: Vec<f64>) -> Result<ScalarValue, ExprError> {
    Ok(ScalarValue::List(ScalarValue::new_list(
        &values
            .into_iter()
            .map(|x| ScalarValue::Float64(Some(x)))
            .collect::<Vec<_>>(),
        &DataType::Float64,
        true,
    )))
}
fn ints(values: Vec<i32>) -> Result<ScalarValue, ExprError> {
    Ok(ScalarValue::List(ScalarValue::new_list(
        &values
            .into_iter()
            .map(|x| ScalarValue::Int32(Some(x)))
            .collect::<Vec<_>>(),
        &DataType::Int32,
        true,
    )))
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
    args: &[ArrayRef],
    dtype: &DataType,
    len: usize,
) -> Result<ArrayRef, ExprError> {
    if len == 0 {
        return Ok(new_empty_array(dtype));
    }
    if args.len() < 3 {
        return Err(error("missing random stream identity"));
    }
    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        if args.iter().any(|a| a.is_null(row)) {
            out.push(ScalarValue::try_new_null(dtype)?);
            continue;
        }
        let nonce = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| error("invalid nonce"))?
            .value(row) as u64;
        let site = args[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| error("invalid call site"))?
            .value(row) as u64;
        let uid = args[2]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| error("invalid UID"))?
            .value(row);
        let uid = uid
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| error("invalid UID words"))?
            .values()
            .iter()
            .map(|x| *x as u64)
            .collect::<Vec<_>>();
        let mut rng = Rng::new(nonce, site, &uid);
        let a = &args[3..];
        let p = |i: usize| number(&a[i], row);
        let list = |i: usize| integers(&a[i], row);
        let value = match function {
            "int64" if a.is_empty() => ScalarValue::Int64(Some(rng.long() as i64)),
            "int32" => {
                let lo = p(0)? as i64;
                let hi = p(1)? as i64;
                if hi <= lo {
                    return Err(error("empty integer interval"));
                }
                ScalarValue::Int32(Some((lo + rng.bounded_int((hi - lo) as u32) as i64) as i32))
            }
            "int64" => {
                let lo = ScalarValue::try_from_array(&a[0], row)?;
                let hi = ScalarValue::try_from_array(&a[1], row)?;
                let (ScalarValue::Int64(Some(lo)), ScalarValue::Int64(Some(hi))) = (lo, hi) else {
                    return Err(error("integer bounds required"));
                };
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
            }
            "unif" => {
                let lo = p(0)?;
                let hi = p(1)?;
                if !lo.is_finite() || !hi.is_finite() || hi < lo {
                    return Err(error("invalid uniform bounds"));
                }
                ScalarValue::Float64(Some(lo + (hi - lo) * rng.direct_uniform()))
            }
            "norm" => {
                let sd = p(1)?;
                if sd < 0.0 || !sd.is_finite() {
                    return Err(error("negative or nonfinite standard deviation"));
                }
                ScalarValue::Float64(Some(p(0)? + sd * rng.normal()))
            }
            "norm_nd" | "unif_nd" => {
                let rows = p(0)? as i64;
                let cols = p(1)? as i64;
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
                for _ in 0..count {
                    values.push(ScalarValue::Float64(Some(if function == "norm_nd" {
                        lo + hi * rng.normal()
                    } else {
                        lo + (hi - lo) * rng.uniform()
                    })));
                }
                let data = ScalarValue::new_list(&values, &DataType::Float64, true);
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
            }
            "bool" => {
                let p = p(0)?;
                if !(0.0..=1.0).contains(&p) {
                    return Err(error("probability outside [0,1]"));
                }
                ScalarValue::Boolean(Some(rng.direct_uniform() < p))
            }
            "gamma" => {
                let s = p(0)?;
                let scale = p(1)?;
                if s <= 0.0 || !s.is_finite() || scale < 0.0 || !scale.is_finite() {
                    return Err(error("invalid gamma parameters"));
                }
                ScalarValue::Float64(Some(rng.gamma(s) * scale))
            }
            "beta" => {
                let dist = Beta::new(p(0)?, p(1)?).map_err(|_| error("invalid beta parameters"))?;
                let lo = p(2)?;
                let hi = p(3)?;
                if lo < 0.0 || hi > 1.0 || hi <= lo {
                    return Err(error("invalid beta truncation"));
                }
                let u = dist.cdf(lo) + (dist.cdf(hi) - dist.cdf(lo)) * rng.uniform();
                ScalarValue::Float64(Some(dist.inverse_cdf(u)))
            }
            "pois" => {
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
            }
            "dirichlet" => {
                let shapes = list(0)?;
                if shapes.iter().any(|x| *x < 0.0 || !x.is_finite()) {
                    return Err(error("invalid Dirichlet concentration"));
                }
                let values = shapes
                    .into_iter()
                    .map(|x| if x == 0.0 { 0.0 } else { rng.gamma(x) })
                    .collect::<Vec<_>>();
                let total = values.iter().sum::<f64>();
                floats(values.into_iter().map(|x| x / total).collect())?
            }
            "cat" => {
                let weights = list(0)?;
                let total: f64 = weights.iter().sum();
                if weights.iter().any(|x| !x.is_finite() || *x < 0.0)
                    || !total.is_finite()
                    || total <= 0.0
                {
                    return Err(error("invalid categorical weights"));
                }
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
            }
            "hyper" => {
                let pop = p(0)? as i64;
                let good = p(1)? as i64;
                let sample = p(2)? as i64;
                if pop < 0 || good < 0 || good > pop || sample < 0 || sample > pop {
                    return Err(error("invalid hypergeometric parameters"));
                }
                ScalarValue::Int32(Some(rng.hyper(pop, good, sample)))
            }
            "multi_hyper" => {
                let colors = list(0)?;
                let mut pop = colors.iter().sum::<f64>() as i64;
                let mut sample = p(1)? as i64;
                if colors.iter().any(|x| *x < 0.0) || sample < 0 || sample > pop {
                    return Err(error("invalid multivariate hypergeometric parameters"));
                }
                let mut values = vec![];
                for color in colors {
                    let good = color as i64;
                    let x = rng.hyper(pop, good, sample);
                    pop -= good;
                    sample -= x as i64;
                    values.push(x);
                }
                ints(values)?
            }
            "norm2d" => {
                let mean = list(0)?;
                let cov = list(1)?;
                if mean.len() != 2
                    || cov.len() != 3
                    || cov.iter().any(|x| !x.is_finite())
                    || cov[0] < 0.0
                    || cov[2] < 0.0
                    || cov[0] * cov[2] < cov[1] * cov[1]
                {
                    return Err(error("invalid bivariate normal parameters"));
                }
                let x = rng.normal();
                let y = rng.normal();
                let s = cov[0].sqrt();
                let (x, y) = if s == 0.0 {
                    (mean[0], mean[1] + cov[2].sqrt() * y)
                } else {
                    (
                        mean[0] + s * x,
                        mean[1]
                            + cov[1] / s * x
                            + (cov[2] - cov[1] * cov[1] / cov[0]).max(0.0).sqrt() * y,
                    )
                };
                floats(vec![x, y])?
            }
            "shuffle" => {
                let values = a[0]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| error("shuffle requires array"))?
                    .value(row);
                let mut indices = (0..values.len()).collect::<Vec<_>>();
                for i in (1..indices.len()).rev() {
                    let j = rng.bounded_long((i + 1) as u64) as usize;
                    indices.swap(i, j);
                }
                let indices = arrow_array::UInt32Array::from(
                    indices
                        .into_iter()
                        .map(|index| {
                            u32::try_from(index)
                                .map_err(|_| error("shuffle exceeds Arrow take index capacity"))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let selected = arrow_select::take::take(values.as_ref(), &indices, None)?;
                let DataType::List(field) = dtype else {
                    return Err(error("shuffle requires list output type"));
                };
                // Arrow take reconstructs nested list fields and may discard
                // logical Set/Dict metadata. Restore the declared descriptor
                // without copying the selected buffers before list validation.
                let selected =
                    crate::logical_expr::restore_arrow_array_type(&selected, field.data_type())?;
                ScalarValue::List(Arc::new(ListArray::try_new(
                    field.clone(),
                    arrow_buffer::OffsetBuffer::from_lengths([selected.len()]),
                    selected,
                    None,
                )?))
            }
            _ => return Err(error("unknown distribution")),
        };
        out.push(value);
    }
    Ok(ScalarValue::iter_to_array(out)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hail_integer_golden() {
        let values = (0..5)
            .map(|i| Rng::new(0, 0, &[i]).bounded_int(5))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![4, 3, 2, 3, 3]);
        let values = (0..5)
            .flat_map(|i| (0..2).map(move |j| Rng::new(0, 0, &[i, j]).bounded_int(5)))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![0, 3, 2, 4, 1, 4, 4, 2, 4, 4]);
    }
    #[test]
    fn hail_uniform_golden() {
        assert_eq!(Rng::new(0, 0, &[]).direct_uniform(), 0.9828239225846387);
        assert_eq!(Rng::new(0, 1, &[]).direct_uniform(), 0.49094525115847415);
    }
}
