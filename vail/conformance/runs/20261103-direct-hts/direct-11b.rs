//! Experimental fused HTS operator: direct original buffers to final allele entries.
use crate::calls::{CallRows, FIRST, PHASED, PLOIDY, SECOND};
use crate::expressions::{ExprError, Type};
use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, StructArray, UInt8Array, UInt16Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Fields};
use arrow_select::take::take;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, expr_fn::create_udf};
use std::sync::Arc;

pub(crate) fn function(
    types: &[Type],
) -> std::result::Result<(Arc<ScalarUDF>, Vec<Type>, Type), ExprError> {
    if types.len() != 3 {
        return Err(ExprError::Invalid(
            "direct HTS expects cells, allele counts, variants".into(),
        ));
    }
    let Type::Array { array: variant } = &types[2] else {
        return Err(ExprError::Invalid(
            "direct HTS variants must be lists".into(),
        ));
    };
    let output = Type::Array {
        array: Box::new(Type::Struct {
            r#struct: vec![
                ("variant".into(), (**variant).clone()),
                ("cells".into(), types[0].clone()),
            ],
        }),
    };
    let dtype = output.to_arrow()?;
    let udf = create_udf(
        "experiment_direct_hts",
        types
            .iter()
            .map(Type::to_arrow)
            .collect::<std::result::Result<_, _>>()?,
        dtype.clone(),
        Volatility::Immutable,
        Arc::new(move |args| {
            let rows = args
                .iter()
                .find_map(|a| {
                    if let ColumnarValue::Array(a) = a {
                        Some(a.len())
                    } else {
                        None
                    }
                })
                .unwrap_or(1);
            let args = args
                .iter()
                .map(|a| a.to_array(rows))
                .collect::<Result<Vec<_>>>()?;
            Ok(ColumnarValue::Array(execute(&args, &dtype)?))
        }),
    );
    Ok((Arc::new(udf), types.to_vec(), output))
}
fn down<T: Array + 'static>(a: &dyn Array) -> Result<&T> {
    a.as_any().downcast_ref().ok_or_else(|| {
        datafusion_common::DataFusionError::Execution("direct HTS storage mismatch".into())
    })
}
struct Lists<'a> {
    list: &'a ListArray,
    values: &'a Int32Array,
}
impl<'a> Lists<'a> {
    fn new(a: &'a ArrayRef) -> Result<Self> {
        let list = down::<ListArray>(a.as_ref())?;
        Ok(Self {
            list,
            values: down(list.values().as_ref())?,
        })
    }
    fn get(&self, row: usize) -> Option<std::ops::Range<usize>> {
        (!self.list.is_null(row)).then(|| {
            self.list.value_offsets()[row] as usize..self.list.value_offsets()[row + 1] as usize
        })
    }
    fn value(&self, i: usize) -> Option<i32> {
        (!self.values.is_null(i)).then(|| self.values.value(i))
    }
}
struct Calls {
    a: Vec<u16>,
    b: Vec<u16>,
    n: Vec<u8>,
    p: Vec<bool>,
    valid: Vec<bool>,
}
impl Calls {
    fn new(cap: usize) -> Self {
        Self {
            a: Vec::with_capacity(cap),
            b: Vec::with_capacity(cap),
            n: Vec::with_capacity(cap),
            p: Vec::with_capacity(cap),
            valid: Vec::with_capacity(cap),
        }
    }
    fn append(&mut self, call: Option<crate::calls::Call>, allele: usize) {
        self.valid.push(call.is_some());
        let (mut a, mut b, n, p) = call
            .map(|c| {
                (
                    (c.first() as usize == allele) as u16,
                    (c.ploidy() > 1 && c.second() as usize == allele) as u16,
                    c.ploidy(),
                    c.phased(),
                )
            })
            .unwrap_or((0, 0, 0, false));
        if n == 0 {
            a = 0;
        }
        if !p && a > b && n == 2 {
            std::mem::swap(&mut a, &mut b);
        }
        self.a.push(a);
        self.b.push(b);
        self.n.push(n);
        self.p.push(p);
    }
    fn finish(self, fields: Fields) -> Result<ArrayRef> {
        debug_assert_eq!(
            fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
            [FIRST, SECOND, PLOIDY, PHASED]
        );
        Ok(Arc::new(StructArray::try_new(
            fields,
            vec![
                Arc::new(UInt16Array::from(self.a)),
                Arc::new(UInt16Array::from(self.b)),
                Arc::new(UInt8Array::from(self.n)),
                Arc::new(BooleanArray::from(self.p)),
            ],
            Some(NullBuffer::from(self.valid)),
        )?))
    }
}
fn execute(args: &[ArrayRef], dtype: &DataType) -> Result<ArrayRef> {
    let cells = down::<ListArray>(args[0].as_ref())?;
    let cell = down::<StructArray>(cells.values().as_ref())?;
    let entry = down::<StructArray>(cell.column_by_name("value").unwrap().as_ref())?;
    let counts = down::<Int32Array>(args[1].as_ref())?;
    let variants = down::<ListArray>(args[2].as_ref())?;
    let variant = down::<StructArray>(variants.values().as_ref())?;
    let alleles = down::<Int32Array>(variant.column_by_name("a_index").unwrap().as_ref())?;
    let gt = entry
        .column_by_name("GT")
        .map(CallRows::new)
        .transpose()
        .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?;
    let pgt = entry
        .column_by_name("PGT")
        .map(CallRows::new)
        .transpose()
        .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?;
    let ad = entry.column_by_name("AD").map(Lists::new).transpose()?;
    let pl = entry.column_by_name("PL").map(Lists::new).transpose()?;
    let old_gq = entry
        .column_by_name("GQ")
        .map(|a| down::<Int32Array>(a.as_ref()))
        .transpose()?;
    let output_len = (0..cells.len()).try_fold(0usize, |sum, r| {
        if cells.is_null(r) || variants.is_null(r) {
            return Ok(sum);
        }
        let count = (cells.value_offsets()[r + 1] - cells.value_offsets()[r]) as usize;
        let copies = (variants.value_offsets()[r + 1] - variants.value_offsets()[r]) as usize;
        sum.checked_add(count.checked_mul(copies).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("HTS output overflow".into())
        })?)
        .filter(|n| *n <= i32::MAX as usize)
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("HTS output exceeds list offsets".into())
        })
    })?;
    let mut source = Vec::<u32>::with_capacity(output_len);
    let mut variant_indices = Vec::<u32>::new();
    let mut cell_offsets = vec![0i32];
    let mut outer_offsets = vec![0i32];
    let mut list_valid = Vec::new();
    let mut valid = Vec::with_capacity(output_len);
    let mut gt_out = Calls::new(output_len);
    let mut pgt_out = Calls::new(output_len);
    let mut ad_out =
        ListBuilder::with_capacity(Int32Builder::with_capacity(output_len * 2), output_len);
    let mut pl_out =
        ListBuilder::with_capacity(Int32Builder::with_capacity(output_len * 3), output_len);
    let mut gq_out = Int32Builder::with_capacity(output_len);
    for r in 0..cells.len() {
        if variants.is_null(r) {
            outer_offsets.push(variant_indices.len() as i32);
            continue;
        }
        let n = counts.value(r) as usize;
        let start = cells.value_offsets()[r] as usize;
        let width = if cells.is_valid(r) {
            (cells.value_offsets()[r + 1] as usize) - start
        } else {
            0
        };
        let selected = variants.value_offsets()[r + 1] > variants.value_offsets()[r];
        let mut totals = vec![None; width];
        let mut prepared = if pl.is_some() && selected {
            vec![[None; 3]; width * n]
        } else {
            vec![]
        };
        if selected {
            for local in 0..width {
                let i = start + local;
                if cell.is_null(i) || entry.is_null(i) {
                    continue;
                }
                if let Some(ad) = &ad {
                    if let Some(span) = ad.get(i) {
                        if span.len() != n {
                            return exec_err!("direct HTS AD length mismatch");
                        }
                        totals[local] =
                            Some(span.filter_map(|j| ad.value(j)).map(i64::from).sum::<i64>());
                    }
                }
                let ploidy = if let Some(gt) = &gt {
                    gt.get(i)
                        .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?
                        .map(|c| c.ploidy() as usize)
                } else {
                    Some(2)
                };
                if let (Some(pl), Some(ploidy)) = (&pl, ploidy) {
                    if let Some(span) = pl.get(i) {
                        if !matches!(ploidy, 1 | 2)
                            || span.len() != if ploidy == 1 { n } else { n * (n + 1) / 2 }
                        {
                            return exec_err!("direct HTS PL length mismatch");
                        }
                        prepare_buckets(
                            pl,
                            span,
                            ploidy,
                            n,
                            &mut prepared[local * n..(local + 1) * n],
                        );
                    }
                }
            }
        }
        for v in variants.value_offsets()[r] as usize..variants.value_offsets()[r + 1] as usize {
            let allele = alleles.value(v) as usize;
            variant_indices.push(v as u32);
            list_valid.push(cells.is_valid(r));
            if cells.is_valid(r) {
                for i in cells.value_offsets()[r] as usize..cells.value_offsets()[r + 1] as usize {
                    source.push(i as u32);
                    let present = cell.is_valid(i) && entry.is_valid(i);
                    valid.push(present);
                    let call = if present {
                        gt.as_ref()
                            .map(|g| g.get(i))
                            .transpose()
                            .map_err(|e| {
                                datafusion_common::DataFusionError::Execution(e.to_string())
                            })?
                            .flatten()
                    } else {
                        None
                    };
                    let phase_call = if present {
                        pgt.as_ref()
                            .map(|g| g.get(i))
                            .transpose()
                            .map_err(|e| {
                                datafusion_common::DataFusionError::Execution(e.to_string())
                            })?
                            .flatten()
                    } else {
                        None
                    };
                    gt_out.append(call, allele);
                    pgt_out.append(phase_call, allele);
                    if let Some(span) = ad
                        .as_ref()
                        .and_then(|a| present.then(|| a.get(i)).flatten())
                    {
                        if span.len() != n {
                            return exec_err!("direct HTS AD length mismatch");
                        }
                        let ad = ad.as_ref().unwrap();
                        let alt = ad.value(span.start + allele);
                        let total = totals[i - start].unwrap();
                        let other = alt
                            .map(|a| i32::try_from(total - i64::from(a)))
                            .transpose()
                            .map_err(|_| {
                                datafusion_common::DataFusionError::Execution("AD overflow".into())
                            })?;
                        ad_out.values().append_option(other);
                        ad_out.values().append_option(alt);
                        ad_out.append(true);
                    } else {
                        ad_out.append(false);
                    }
                    let ploidy = if gt.is_some() {
                        call.map(|c| c.ploidy() as usize)
                    } else {
                        Some(2)
                    };
                    if let (Some(span), Some(ploidy)) = (
                        pl.as_ref()
                            .and_then(|p| present.then(|| p.get(i)).flatten()),
                        ploidy,
                    ) {
                        let _ = span;
                        let buckets = prepared[(i - start) * n + allele];
                        let (mut min, mut second) = (99i32, 99i32);
                        for x in &buckets[..ploidy + 1] {
                            pl_out.values().append_option(*x);
                            if let Some(x) = x {
                                if *x < min {
                                    second = min;
                                    min = *x;
                                } else if *x < second {
                                    second = *x;
                                }
                            }
                        }
                        pl_out.append(true);
                        gq_out.append_value(second.wrapping_sub(min));
                    } else {
                        pl_out.append(false);
                        gq_out.append_option(
                            old_gq.and_then(|a| (!a.is_null(i) && present).then(|| a.value(i))),
                        );
                    }
                }
            }
            cell_offsets.push(source.len().try_into().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "direct HTS output exceeds list offsets".into(),
                )
            })?);
        }
        outer_offsets.push(variant_indices.len() as i32);
    }
    let indices = arrow_array::UInt32Array::from(source);
    let ad_out = Arc::new(ad_out.finish()) as ArrayRef;
    let pl_out = Arc::new(pl_out.finish()) as ArrayRef;
    let gq_out = Arc::new(gq_out.finish()) as ArrayRef;
    let gt_type = entry.column_by_name("GT").map(|a| {
        if let DataType::Struct(f) = a.data_type() {
            f.clone()
        } else {
            unreachable!()
        }
    });
    let pgt_type = entry.column_by_name("PGT").map(|a| {
        if let DataType::Struct(f) = a.data_type() {
            f.clone()
        } else {
            unreachable!()
        }
    });
    let gt_out = gt_type.map(|f| gt_out.finish(f)).transpose()?;
    let pgt_out = pgt_type.map(|f| pgt_out.finish(f)).transpose()?;
    let columns = entry
        .fields()
        .iter()
        .zip(entry.columns())
        .map(|(f, a)| match f.name().as_str() {
            "GT" => Ok(gt_out.as_ref().unwrap().clone()),
            "PGT" => Ok(pgt_out.as_ref().unwrap().clone()),
            "AD" => Ok(ad_out.clone()),
            "PL" => Ok(pl_out.clone()),
            "GQ" if pl.is_some() => Ok(gq_out.clone()),
            _ => Ok(take(a.as_ref(), &indices, None)?),
        })
        .collect::<Result<Vec<_>>>()?;
    let entries = Arc::new(StructArray::try_new(
        entry.fields().clone(),
        columns,
        Some(NullBuffer::from(valid)),
    )?) as ArrayRef;
    let cell_columns = cell
        .fields()
        .iter()
        .zip(cell.columns())
        .map(|(f, a)| {
            if f.name() == "value" {
                Ok(entries.clone())
            } else {
                Ok(take(a.as_ref(), &indices, None)?)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let cell_array = Arc::new(StructArray::try_new(
        cell.fields().clone(),
        cell_columns,
        None,
    )?) as ArrayRef;
    let DataType::List(cell_field) = cells.data_type() else {
        unreachable!()
    };
    let lists = Arc::new(ListArray::try_new(
        cell_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(cell_offsets)),
        cell_array,
        Some(NullBuffer::from(list_valid)),
    )?) as ArrayRef;
    let DataType::List(out_field) = dtype else {
        unreachable!()
    };
    let DataType::Struct(fields) = out_field.data_type() else {
        unreachable!()
    };
    let selected_variants = take(
        variants.values().as_ref(),
        &arrow_array::UInt32Array::from(variant_indices),
        None,
    )?;
    let outputs = Arc::new(StructArray::try_new(
        fields.clone(),
        vec![selected_variants, lists],
        None,
    )?) as ArrayRef;
    Ok(Arc::new(ListArray::try_new(
        out_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(outer_offsets)),
        outputs,
        None,
    )?))
}

fn prepare_buckets(
    pl: &Lists<'_>,
    span: std::ops::Range<usize>,
    ploidy: usize,
    n: usize,
    result: &mut [[Option<i32>; 3]],
) {
    fn update(slot: &mut Option<i32>, v: Option<i32>) {
        if let Some(v) = v {
            *slot = Some(slot.map_or(v, |old| old.min(v)));
        }
    }
    let mut best = None::<(i32, usize, usize)>;
    if ploidy == 2 {
        let mut index = span.start;
        for b in 0..n {
            for a in 0..=b {
                let v = pl.value(index);
                index += 1;
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
            for bucket in result.iter_mut() {
                bucket[0] = Some(v);
            }
            for excluded in [Some(a), (b != a).then_some(b)].into_iter().flatten() {
                let mut minimum = None;
                let mut index = span.start;
                for y in 0..n {
                    for x in 0..=y {
                        if x != excluded && y != excluded {
                            update(&mut minimum, pl.value(index));
                        }
                        index += 1;
                    }
                }
                result[excluded][0] = minimum;
            }
        }
    } else {
        for a in 0..n {
            let v = pl.value(span.start + a);
            result[a][1] = v;
            if let Some(v) = v {
                if best.is_none_or(|x| v < x.0) {
                    best = Some((v, a, a));
                }
            }
        }
        if let Some((v, a, _)) = best {
            for bucket in result.iter_mut() {
                bucket[0] = Some(v);
            }
            let mut minimum = None;
            for b in 0..n {
                if b != a {
                    update(&mut minimum, pl.value(span.start + b));
                }
            }
            result[a][0] = minimum;
        }
    }
}
