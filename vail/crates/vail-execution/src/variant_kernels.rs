//! Linear Unicode normalization and shared per-original-entry HTS reductions.
use crate::expressions::{ExprError, PrimitiveType, Type};
use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, StructArray, UInt8Array, UInt16Array,
};
use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, expr_fn::create_udf};
use std::sync::Arc;

fn array(t: Type) -> Type {
    Type::Array { array: Box::new(t) }
}
pub(crate) fn function(
    name: &str,
) -> std::result::Result<(Arc<ScalarUDF>, Vec<Type>, Type), ExprError> {
    let i = Type::Primitive(PrimitiveType::Int32);
    let s = Type::Primitive(PrimitiveType::Str);
    let (args, output) = if name == "unphased_diploid_gt_index_call" {
        (vec![i.clone()], Type::Primitive(PrimitiveType::Call))
    } else if name == "variant_min_rep" {
        (
            vec![s.clone(), s.clone()],
            Type::Struct {
                r#struct: vec![("shift".into(), i.clone()), ("alleles".into(), array(s))],
            },
        )
    } else if name == "gq_from_pl" {
        (vec![array(i.clone())], i)
    } else {
        (
            vec![
                i.clone(),
                array(i.clone()),
                array(i.clone()),
                array(i.clone()),
                array(i.clone()),
            ],
            Type::Struct {
                r#struct: vec![
                    ("ad".into(), array(array(i.clone()))),
                    ("pl".into(), array(array(i))),
                ],
            },
        )
    };
    let kernel = name.to_owned();
    let dtype = output.to_arrow()?;
    let out_type = dtype.clone();
    let udf = create_udf(
        name,
        args.iter()
            .map(Type::to_arrow)
            .collect::<std::result::Result<Vec<_>, _>>()?,
        dtype,
        Volatility::Immutable,
        Arc::new(move |values| {
            // A scalar owns one row, irrespective of the batch cardinality.
            let rows = values
                .iter()
                .find_map(|v| match v {
                    ColumnarValue::Array(a) => Some(a.len()),
                    _ => None,
                })
                .unwrap_or(1);
            let result = if kernel == "unphased_diploid_gt_index_call" {
                diploid_index_call(&values[0], rows, &out_type)?
            } else if kernel == "variant_min_rep" {
                min_rep(values, rows, &out_type)?
            } else if kernel == "gq_from_pl" {
                gq_from_pl(&Input::new(&values[0])?)?
            } else {
                let arrays = values.iter().map(Input::new).collect::<Result<Vec<_>>>()?;
                entries(&arrays, rows, &out_type)?
            };
            if values.iter().all(|v| matches!(v, ColumnarValue::Scalar(_))) {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            }
        }),
    );
    Ok((Arc::new(udf), args, output))
}

// Each nonnegative int32 index fits the diploid UInt16 allele representation.
// Estimate its triangular bucket once, then correct with exact int64 bounds.
fn diploid_index_call(value: &ColumnarValue, rows: usize, dtype: &DataType) -> Result<ArrayRef> {
    let input = Input::new(value)?;
    let indices = input.down::<Int32Array>()?;
    let mut first = Vec::with_capacity(rows);
    let mut second = Vec::with_capacity(rows);
    let mut valid = Vec::with_capacity(rows);
    for row in 0..rows {
        let index = row;
        if indices.is_null(index) {
            first.push(0u16);
            second.push(0u16);
            valid.push(false);
            continue;
        }
        let value = i64::from(indices.value(index));
        if value < 0 {
            return exec_err!("genotype index must be nonnegative");
        }
        let mut b = (((8 * value + 1) as f64).sqrt() - 1.0) as i64 / 2;
        while b * (b + 1) / 2 > value {
            b -= 1;
        }
        while (b + 1) * (b + 2) / 2 <= value {
            b += 1;
        }
        let a = value - b * (b + 1) / 2;
        debug_assert!(0 <= a && a <= b && b <= i64::from(u16::MAX));
        first.push(a as u16);
        second.push(b as u16);
        valid.push(true);
    }
    let DataType::Struct(fields) = dtype else {
        return exec_err!("Call kernel requires struct output storage");
    };
    Ok(Arc::new(StructArray::new(
        fields.clone(),
        vec![
            Arc::new(UInt16Array::from(first)),
            Arc::new(UInt16Array::from(second)),
            Arc::new(UInt8Array::from(vec![2; rows])),
            Arc::new(BooleanArray::from(vec![false; rows])),
        ],
        Some(arrow_buffer::NullBuffer::from(valid)),
    )))
}

// Array ownership is shared; scalar conversion happens once, never per row.
struct Input {
    array: ArrayRef,
    scalar: bool,
}
impl Input {
    fn new(value: &ColumnarValue) -> Result<Self> {
        Ok(match value {
            ColumnarValue::Array(array) => Self {
                array: array.clone(),
                scalar: false,
            },
            ColumnarValue::Scalar(value) => Self {
                array: value.to_array()?,
                scalar: true,
            },
        })
    }
    fn row(&self, row: usize) -> usize {
        if self.scalar { 0 } else { row }
    }
    fn down<T: Array + 'static>(&self) -> Result<&T> {
        self.array.as_any().downcast_ref().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "invalid variant kernel Arrow storage".into(),
            )
        })
    }
}

fn min_rep(values: &[ColumnarValue], rows: usize, dtype: &DataType) -> Result<ArrayRef> {
    let r = crate::string_kernels::Text::new(&values[0])?;
    let a = crate::string_kernels::Text::new(&values[1])?;
    let mut shifts = Int32Builder::new();
    let mut alleles = ListBuilder::new(StringBuilder::new());
    for row in 0..rows {
        let (Some(reference), Some(alternate)) = (r.get(row), a.get(row)) else {
            return exec_err!("split_multi_hts requires defined nonempty alleles");
        };
        if reference.is_empty() || alternate.is_empty() {
            return exec_err!("split_multi_hts requires defined nonempty alleles");
        }
        let (mut re, mut ae, mut shift, mut start) = (reference.len(), alternate.len(), 0, 0);
        if !alternate.starts_with('<') && !alternate.contains(['[', ']']) {
            // Byte boundaries move only by complete Unicode scalar values. Leave
            // one scalar in each allele; scratch is independent of allele length.
            let mut ri = reference.chars().rev();
            let mut ai = alternate.chars().rev();
            while let (Some(rc), Some(ac)) = (ri.next(), ai.next()) {
                if rc != ac || re == rc.len_utf8() || ae == ac.len_utf8() {
                    break;
                }
                re -= rc.len_utf8();
                ae -= ac.len_utf8();
            }
            for (rc, ac) in reference[..re].chars().zip(alternate[..ae].chars()) {
                if rc != ac || start + rc.len_utf8() == re || start + ac.len_utf8() == ae {
                    break;
                }
                start += rc.len_utf8();
                shift += 1;
            }
        }
        shifts.append_value(shift);
        alleles.values().append_value(&reference[start..re]);
        alleles.values().append_value(&alternate[start..ae]);
        alleles.append(true);
    }
    let DataType::Struct(fields) = dtype else {
        unreachable!()
    };
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![Arc::new(shifts.finish()), Arc::new(alleles.finish())],
        None,
    )?))
}

// Resolve the Arrow children once per batch. Entry spans borrow primitive
// buffers and offsets rather than constructing an Array for every list slot.
struct Int32Lists<'a> {
    list: &'a ListArray,
    values: &'a Int32Array,
    scalar: bool,
}
impl<'a> Int32Lists<'a> {
    fn new(input: &'a Input) -> Result<Self> {
        let list = input.down::<ListArray>()?;
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "variant kernel requires int32 array elements".into(),
                )
            })?;
        Ok(Self {
            list,
            values,
            scalar: input.scalar,
        })
    }
    fn get(&self, row: usize) -> Option<Int32Span<'a>> {
        let row = if self.scalar { 0 } else { row };
        if self.list.is_null(row) {
            return None;
        }
        let offsets = self.list.value_offsets();
        Some(Int32Span {
            values: self.values,
            start: offsets[row] as usize,
            len: (offsets[row + 1] - offsets[row]) as usize,
        })
    }
}
#[derive(Clone, Copy)]
// The range lies within logical child values, established by ListArray offsets.
struct Int32Span<'a> {
    values: &'a Int32Array,
    start: usize,
    len: usize,
}
impl Int32Span<'_> {
    fn len(&self) -> usize {
        self.len
    }
    fn iter(&self) -> impl Iterator<Item = Option<i32>> + '_ {
        (0..self.len).map(|i| value(self, i))
    }
}

fn update(slot: &mut Option<i32>, value: Option<i32>) {
    if let Some(value) = value {
        *slot = Some(slot.map_or(value, |old| old.min(value)));
    }
}
fn value(a: &Int32Span<'_>, i: usize) -> Option<i32> {
    let i = a.start + i;
    (!a.values.is_null(i)).then(|| a.values.value(i))
}
fn append(out: &mut ListBuilder<ListBuilder<Int32Builder>>, values: Option<&[Option<i32>]>) {
    if let Some(values) = values {
        for v in values {
            out.values().values().append_option(*v);
        }
        out.values().append(true);
    } else {
        out.values().append(false);
    }
}
fn entries(values: &[Input], rows: usize, dtype: &DataType) -> Result<ArrayRef> {
    let counts = values[0].down::<Int32Array>()?;
    let mut ads = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    let mut pls = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    let gt = Int32Lists::new(&values[1])?;
    let pgt = Int32Lists::new(&values[2])?;
    let ad = Int32Lists::new(&values[3])?;
    let pl = Int32Lists::new(&values[4])?;
    let mut result = Vec::new();
    for row in 0..rows {
        let cr = values[0].row(row);
        if counts.is_null(cr) || counts.value(cr) < 1 {
            return exec_err!("split_multi_hts requires at least one allele");
        }
        let n = counts.value(cr) as usize;
        let gt = gt.get(row);
        let pgt = pgt.get(row);
        for call in [&gt, &pgt].into_iter().flatten() {
            if !(1..=2).contains(&call.len())
                || call.iter().any(|a| a.is_none_or(|a| a < 0 || a as usize >= n))
            {
                return exec_err!(
                    "split_multi_hts requires haploid or diploid GT/PGT with allele indices in range"
                );
            }
        }
        let ad = ad.get(row);
        if ad.as_ref().is_some_and(|a| a.len() != n) {
            return exec_err!("split_multi_hts AD length must equal allele count");
        }
        let total = ad
            .as_ref()
            .map(|a| a.iter().flatten().map(i64::from).sum::<i64>());
        let pl = pl.get(row);
        let ploidy = gt.as_ref().map(Int32Span::len);
        let diploid_len = n.checked_mul(n + 1).and_then(|x| x.checked_div(2));
        if let Some(pl) = &pl {
            let valid = match ploidy {
                Some(1) => pl.len() == n,
                Some(2) => Some(pl.len()) == diploid_len,
                _ => pl.len() == n || Some(pl.len()) == diploid_len,
            };
            if !valid {
                return exec_err!(
                    "split_multi_hts PL length must match GT ploidy and allele count"
                );
            }
        }
        result.clear();
        result.resize(n, [None; 3]);
        if let (Some(pl), Some(ploidy)) = (&pl, ploidy) {
            // The global minimum edge supplies bucket zero for every vertex
            // except its (at most two) endpoints. Re-scan only those endpoints.
            let mut best: Option<(i32, usize, usize)> = None;
            if ploidy == 2 {
                let mut idx = 0;
                for b in 0..n {
                    for a in 0..=b {
                        let v = value(pl, idx);
                        idx += 1;
                        if let Some(v) = v {
                            if best.is_none_or(|x| v < x.0) {
                                best = Some((v, a, b));
                            }
                        }
                        if a == b {
                            result[a][2] = v;
                        } else {
                            update(&mut result[a][1], v);
                            update(&mut result[b][1], v);
                        }
                    }
                }
                if let Some((v, a, b)) = best {
                    for bucket in &mut result {
                        bucket[0] = Some(v);
                    }
                    for excluded in [Some(a), (b != a).then_some(b)].into_iter().flatten() {
                        let mut minimum = None;
                        let mut idx = 0;
                        for y in 0..n {
                            for x in 0..=y {
                                if x != excluded && y != excluded {
                                    update(&mut minimum, value(pl, idx));
                                }
                                idx += 1;
                            }
                        }
                        result[excluded][0] = minimum;
                    }
                }
            } else {
                for a in 0..n {
                    let v = value(pl, a);
                    result[a][1] = v;
                    if let Some(v) = v {
                        if best.is_none_or(|x| v < x.0) {
                            best = Some((v, a, a));
                        }
                    }
                }
                if let Some((v, a, _)) = best {
                    for bucket in &mut result {
                        bucket[0] = Some(v);
                    }
                    let mut minimum = None;
                    for b in 0..n {
                        if b != a {
                            update(&mut minimum, value(pl, b));
                        }
                    }
                    result[a][0] = minimum;
                }
            }
        }
        for a in 1..n {
            if let Some(ad) = &ad {
                let alt = value(ad, a);
                let other = match alt {
                    Some(v) => {
                        Some(i32::try_from(total.unwrap() - i64::from(v)).map_err(|_| {
                            datafusion_common::DataFusionError::Execution(
                                "split_multi_hts AD total exceeds int32".into(),
                            )
                        })?)
                    }
                    None => None,
                };
                append(&mut ads, Some(&[other, alt]));
            } else {
                append(&mut ads, None);
            }
            if pl.is_some() && ploidy.is_some() {
                append(
                    &mut pls,
                    Some(&result[a][..if ploidy == Some(1) { 2 } else { 3 }]),
                );
            } else {
                append(&mut pls, None);
            }
        }
        ads.append(true);
        pls.append(true);
    }
    let DataType::Struct(fields) = dtype else {
        unreachable!()
    };
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![Arc::new(ads.finish()), Arc::new(pls.finish())],
        None,
    )?))
}

fn gq_from_pl(input: &Input) -> Result<ArrayRef> {
    let lists = input.down::<ListArray>()?;
    let Some(values) = lists.values().as_any().downcast_ref::<Int32Array>() else {
        return exec_err!("gq_from_pl requires int32 likelihoods");
    };
    let offsets = lists.value_offsets();
    let mut output = Int32Builder::with_capacity(lists.len());
    for row in 0..lists.len() {
        if lists.is_null(row) {
            output.append_null();
            continue;
        }
        // Hail caps both minima at 99, including empty and singleton lists.
        let (mut minimum, mut second) = (99i32, 99i32);
        for index in offsets[row] as usize..offsets[row + 1] as usize {
            if values.is_null(index) {
                continue;
            }
            let value = values.value(index);
            if value < minimum {
                second = minimum;
                minimum = value;
            } else if value < second {
                second = value;
            }
        }
        output.append_value(second.wrapping_sub(minimum));
    }
    Ok(Arc::new(output.finish()))
}
