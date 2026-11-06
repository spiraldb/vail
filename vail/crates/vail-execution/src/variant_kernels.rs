//! Linear Unicode normalization and shared per-original-entry HTS reductions.
use crate::expressions::{ExprError, PrimitiveType, Type};
use crate::selected_list_producer::{Argument, Request, SelectedListProducer, SourceDomain};
use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, StructArray, UInt8Array, UInt16Array,
};
use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    expr_fn::create_udf,
};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Deterministic, context-independent evaluation with no effects. Failures
/// validate input values; this does not claim the kernel can never fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EvaluationProperties {
    validation_only: bool,
}
const VALIDATION_ONLY: EvaluationProperties = EvaluationProperties {
    validation_only: true,
};

/// Query preparation inspects exact registered implementations. An unrelated
/// UDF with the same display name, volatility, or endpoint gains no permission.
pub(crate) fn validation_only(function: &ScalarUDF) -> bool {
    function
        .inner()
        .downcast_ref::<EntryReductions>()
        .map(|kernel| kernel.properties)
        .or_else(|| {
            function
                .inner()
                .downcast_ref::<MinRepresentation>()
                .map(|kernel| kernel.properties)
        })
        .is_some_and(|properties| properties.validation_only)
}

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
    let trace_quality = std::env::var_os("VAIL_TRACE_COALESCE_ONCE").is_some();
    let trace_entry_reductions = std::env::var_os("VAIL_TRACE_VARIANT_ENTRY").is_some();
    let dtype = output.to_arrow()?;
    if name == "variant_min_rep" {
        let signature = Signature::exact(
            args.iter()
                .map(Type::to_arrow)
                .collect::<std::result::Result<Vec<_>, _>>()?,
            Volatility::Immutable,
        );
        return Ok((
            Arc::new(ScalarUDF::from(MinRepresentation {
                signature,
                output: dtype,
                properties: VALIDATION_ONLY,
            })),
            args,
            output,
        ));
    }
    if name == "variant_entry" {
        let signature = Signature::exact(
            args.iter()
                .map(Type::to_arrow)
                .collect::<std::result::Result<Vec<_>, _>>()?,
            Volatility::Immutable,
        );
        return Ok((
            Arc::new(ScalarUDF::from(EntryReductions {
                signature,
                output: dtype,
                trace: trace_entry_reductions,
                trace_selected: std::env::var_os("VAIL_TRACE_SELECTED_LIST_PRODUCER").is_some(),
                properties: VALIDATION_ONLY,
            })),
            args,
            output,
        ));
    }
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
            } else if kernel == "gq_from_pl" {
                if trace_quality {
                    eprintln!("VAIL_QUALITY_EVAL rows={rows}");
                }
                gq_from_pl(&Input::new(&values[0])?)?
            } else {
                let arrays = values.iter().map(Input::new).collect::<Result<Vec<_>>>()?;
                if trace_entry_reductions {
                    eprintln!("VAIL_VARIANT_ENTRY_REDUCE rows={rows}");
                }
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
#[derive(Clone, Copy)]
struct Reduction {
    total: Option<i64>,
    ploidy: Option<usize>,
}
fn reduce_entry(
    n: usize,
    gt: Option<Int32Span<'_>>,
    pgt: Option<Int32Span<'_>>,
    ad: Option<Int32Span<'_>>,
    pl: Option<Int32Span<'_>>,
    result: &mut Vec<[Option<i32>; 3]>,
) -> Result<Reduction> {
    for call in [&gt, &pgt].into_iter().flatten() {
        if !(1..=2).contains(&call.len())
            || call
                .iter()
                .any(|a| a.is_none_or(|a| a < 0 || a as usize >= n))
        {
            return exec_err!(
                "split_multi_hts requires haploid or diploid GT/PGT with allele indices in range"
            );
        }
    }
    if ad.as_ref().is_some_and(|a| a.len() != n) {
        return exec_err!("split_multi_hts AD length must equal allele count");
    }
    let total = ad
        .as_ref()
        .map(|a| a.iter().flatten().map(i64::from).sum::<i64>());
    let ploidy = gt.as_ref().map(Int32Span::len);
    let diploid_len = n.checked_mul(n + 1).and_then(|x| x.checked_div(2));
    if let Some(pl) = &pl {
        let valid = match ploidy {
            Some(1) => pl.len() == n,
            Some(2) => Some(pl.len()) == diploid_len,
            _ => pl.len() == n || Some(pl.len()) == diploid_len,
        };
        if !valid {
            return exec_err!("split_multi_hts PL length must match GT ploidy and allele count");
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
                for bucket in result.iter_mut() {
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
                for bucket in result.iter_mut() {
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
    Ok(Reduction { total, ploidy })
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
        let ad = ad.get(row);
        let pl = pl.get(row);
        let reduction = reduce_entry(n, gt, pgt.get(row), ad, pl, &mut result)?;
        let total = reduction.total;
        let ploidy = reduction.ploidy;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct MinRepresentation {
    signature: Signature,
    output: DataType,
    properties: EvaluationProperties,
}
impl ScalarUDFImpl for MinRepresentation {
    fn name(&self) -> &str {
        "variant_min_rep"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.output.clone())
    }
    fn invoke_with_args(&self, invocation: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if invocation.args.len() != 2 {
            return exec_err!("minimum representation requires two arguments");
        }
        let rows = invocation
            .args
            .iter()
            .find_map(|value| match value {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        let output = min_rep(&invocation.args, rows, &self.output)?;
        if invocation
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

#[derive(Debug)]
struct EntryReductions {
    signature: Signature,
    output: DataType,
    trace: bool,
    trace_selected: bool,
    properties: EvaluationProperties,
}
impl PartialEq for EntryReductions {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
            && self.output == other.output
            && self.properties == other.properties
    }
}
impl Eq for EntryReductions {}
impl Hash for EntryReductions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        self.output.hash(state);
        self.properties.hash(state);
    }
}
impl ScalarUDFImpl for EntryReductions {
    fn name(&self) -> &str {
        "variant_entry"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.output.clone())
    }
    fn invoke_with_args(&self, invocation: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if invocation.args.len() != 5 {
            return exec_err!("entry reductions require five arguments");
        }
        let rows = invocation
            .args
            .iter()
            .find_map(|v| match v {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .unwrap_or(1);
        let inputs = invocation
            .args
            .iter()
            .map(Input::new)
            .collect::<Result<Vec<_>>>()?;
        if self.trace {
            eprintln!("VAIL_VARIANT_ENTRY_REDUCE rows={rows}");
        }
        let output = entries(&inputs, rows, &self.output)?;
        if invocation
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                output.as_ref(),
                0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}
pub(crate) fn selected_list_producer(function: &ScalarUDF) -> Option<&dyn SelectedListProducer> {
    function
        .inner()
        .downcast_ref::<EntryReductions>()
        .map(|kernel| kernel as &dyn SelectedListProducer)
}

fn source_index(domain: SourceDomain, row: usize, entry: usize) -> usize {
    match domain {
        SourceDomain::Scalar => 0,
        SourceDomain::OriginalRow => row,
        SourceDomain::OriginalEntry => entry,
    }
}
fn argument_domain_matches(argument: &Argument, rows: usize, entries: usize) -> bool {
    match (&argument.value, argument.domain) {
        (ColumnarValue::Scalar(_), SourceDomain::Scalar) => true,
        (ColumnarValue::Array(array), SourceDomain::OriginalRow) => array.len() == rows,
        (ColumnarValue::Array(array), SourceDomain::OriginalEntry) => array.len() == entries,
        _ => false,
    }
}
#[derive(Clone, Copy)]
struct PreparedReduction {
    n: usize,
    buckets: usize,
    total: Option<i64>,
    ploidy: Option<usize>,
    has_pl: bool,
}
#[derive(Clone, Copy, Default)]
struct Scratch {
    entries: usize,
    buckets: usize,
    reduction: usize,
    output_entries: usize,
}
// The check reads only source coordinates/counts, before native validation,
// reductions, or output allocation. A refusal leaves the whole old route intact.
fn selected_scratch(request: &Request<'_>, counts: &Int32Array) -> Option<Scratch> {
    if request.parents.len() != request.selectors.len()
        || request.parents.null_count() != 0
        || request
            .entry_validity
            .is_some_and(|mask| mask.len() != request.entries.values().len())
    {
        return None;
    }
    let mut scratch = Scratch::default();
    let mut at = 0;
    while at < request.parents.len() {
        let parent = usize::try_from(request.parents.value(at)).ok()?;
        if parent >= request.entries.len() {
            return None;
        }
        let mut stop = at + 1;
        while stop < request.parents.len() && request.parents.value(stop) == parent as i32 {
            stop += 1;
        }
        if request.entries.is_valid(parent) {
            let offsets = request.entries.value_offsets();
            let start = offsets[parent] as usize;
            let end = offsets[parent + 1] as usize;
            scratch.entries = scratch.entries.max(end - start);
            scratch.output_entries = scratch
                .output_entries
                .checked_add((end - start).checked_mul(stop - at)?)?;
            let mut buckets = 0usize;
            for entry in start..end {
                if request
                    .entry_validity
                    .is_some_and(|mask| !mask.is_valid(entry))
                {
                    continue;
                }
                let index = source_index(request.arguments[0].domain, parent, entry);
                if counts.is_null(index) || counts.value(index) < 1 {
                    return None;
                }
                let n = counts.value(index) as usize;
                buckets = buckets.checked_add(n)?;
                scratch.reduction = scratch.reduction.max(n);
            }
            scratch.buckets = scratch.buckets.max(buckets);
        }
        at = stop;
    }
    let bytes = scratch
        .entries
        .checked_mul(std::mem::size_of::<Option<PreparedReduction>>())?
        .checked_add(
            scratch
                .buckets
                .checked_mul(std::mem::size_of::<[Option<i32>; 3]>())?,
        )?
        .checked_add(
            scratch
                .reduction
                .checked_mul(std::mem::size_of::<[Option<i32>; 3]>())?,
        )?
        .checked_add(3 * std::mem::size_of::<Vec<usize>>())?;
    if bytes > request.max_scratch_bytes || scratch.output_entries > i32::MAX as usize / 3 {
        return None;
    }
    Some(scratch)
}
fn selected_allele(index: i32, n: usize) -> Result<usize> {
    let length = (n - 1) as i64;
    let index = if index < 0 {
        length + i64::from(index)
    } else {
        i64::from(index)
    };
    if index < 0 || index >= length {
        return exec_err!("array index out of bounds");
    }
    Ok(index as usize + 1)
}
impl SelectedListProducer for EntryReductions {
    fn selected_type(&self, fields: u64) -> Option<DataType> {
        if fields != 3 {
            return None;
        }
        let DataType::Struct(output) = &self.output else {
            return None;
        };
        let mut selected = Vec::with_capacity(output.len());
        for field in output {
            let DataType::List(values) = field.data_type() else {
                return None;
            };
            selected.push(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(values.data_type().clone()),
            );
        }
        Some(DataType::Struct(selected.into()))
    }
    fn evaluate_selected(&self, request: &Request<'_>) -> Result<Option<ArrayRef>> {
        let Some(DataType::Struct(fields)) = self.selected_type(request.fields) else {
            return Ok(None);
        };
        if request.arguments.len() != 5
            || request.arguments.iter().any(|argument| {
                !argument_domain_matches(
                    argument,
                    request.entries.len(),
                    request.entries.values().len(),
                )
            })
        {
            return Ok(None);
        }
        let inputs = request
            .arguments
            .iter()
            .map(|arg| Input::new(&arg.value))
            .collect::<Result<Vec<_>>>()?;
        let Ok(counts) = inputs[0].down::<Int32Array>() else {
            return Ok(None);
        };
        let (Ok(gt), Ok(pgt), Ok(ad), Ok(pl)) = (
            Int32Lists::new(&inputs[1]),
            Int32Lists::new(&inputs[2]),
            Int32Lists::new(&inputs[3]),
            Int32Lists::new(&inputs[4]),
        ) else {
            return Ok(None);
        };
        let Some(scratch) = selected_scratch(request, counts) else {
            return Ok(None);
        };
        let mut prepared = Vec::<Option<PreparedReduction>>::with_capacity(scratch.entries);
        let mut buckets = Vec::<[Option<i32>; 3]>::with_capacity(scratch.buckets);
        let mut reduction = Vec::<[Option<i32>; 3]>::with_capacity(scratch.reduction);
        let mut ads = ListBuilder::with_capacity(
            Int32Builder::with_capacity(scratch.output_entries * 2),
            scratch.output_entries,
        );
        let mut pls = ListBuilder::with_capacity(
            Int32Builder::with_capacity(scratch.output_entries * 3),
            scratch.output_entries,
        );
        let mut valid = arrow_buffer::NullBufferBuilder::new(scratch.output_entries);
        let mut reduced_entries = 0usize;
        let mut at = 0;
        while at < request.parents.len() {
            let parent = request.parents.value(at) as usize;
            let mut stop = at + 1;
            while stop < request.parents.len() && request.parents.value(stop) == parent as i32 {
                stop += 1;
            }
            if request.entries.is_null(parent) {
                at = stop;
                continue;
            }
            let offsets = request.entries.value_offsets();
            let start = offsets[parent] as usize;
            let end = offsets[parent + 1] as usize;
            prepared.clear();
            buckets.clear();
            for entry in start..end {
                if request
                    .entry_validity
                    .is_some_and(|mask| !mask.is_valid(entry))
                {
                    prepared.push(None);
                    continue;
                }
                let index = |argument: usize| {
                    source_index(request.arguments[argument].domain, parent, entry)
                };
                let n = counts.value(index(0)) as usize;
                let gt = gt.get(index(1));
                let pgt = pgt.get(index(2));
                let ad = ad.get(index(3));
                let pl = pl.get(index(4));
                let state = reduce_entry(n, gt, pgt, ad, pl, &mut reduction)?;
                let bucket_start = buckets.len();
                buckets.extend_from_slice(&reduction);
                prepared.push(Some(PreparedReduction {
                    n,
                    buckets: bucket_start,
                    total: state.total,
                    ploidy: state.ploidy,
                    has_pl: pl.is_some(),
                }));
            }
            if self.trace_selected {
                reduced_entries += prepared.iter().flatten().count();
            }
            for selected in at..stop {
                for (entry, state) in (start..end).zip(&prepared) {
                    valid.append(state.is_some());
                    let Some(state) = state else {
                        ads.append(false);
                        pls.append(false);
                        continue;
                    };
                    if request.selectors.is_null(selected) {
                        ads.append(false);
                        pls.append(false);
                        continue;
                    }
                    let allele = selected_allele(request.selectors.value(selected), state.n)?;
                    let ad_index = source_index(request.arguments[3].domain, parent, entry);
                    if let Some(ad) = ad.get(ad_index) {
                        let alt = value(&ad, allele);
                        let other = match alt {
                            Some(alt) => Some(
                                i32::try_from(state.total.unwrap() - i64::from(alt)).map_err(
                                    |_| {
                                        datafusion_common::exec_datafusion_err!(
                                            "split_multi_hts AD total exceeds int32"
                                        )
                                    },
                                )?,
                            ),
                            None => None,
                        };
                        ads.values().append_option(other);
                        ads.values().append_option(alt);
                        ads.append(true);
                    } else {
                        ads.append(false);
                    }
                    if state.has_pl && state.ploidy.is_some() {
                        let width = if state.ploidy == Some(1) { 2 } else { 3 };
                        for value in &buckets[state.buckets + allele][..width] {
                            pls.values().append_option(*value);
                        }
                        pls.append(true);
                    } else {
                        pls.append(false);
                    }
                }
            }
            at = stop;
        }
        let ad = ads.finish();
        let pl = pls.finish();
        if self.trace_selected {
            eprintln!(
                "VAIL_SELECTED_LIST_PRODUCER parents={} reduced_entries={} output_entries={} ad_values={} pl_values={}",
                request.parents.len(),
                reduced_entries,
                scratch.output_entries,
                ad.values().len(),
                pl.values().len()
            );
        }
        Ok(Some(Arc::new(StructArray::try_new(
            fields,
            vec![Arc::new(ad), Arc::new(pl)],
            valid.finish(),
        )?)))
    }
}
