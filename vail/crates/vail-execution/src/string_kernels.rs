//! Native Unicode-scalar string operations without a DataFusion built-in.
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, LargeStringArray, ListArray, StringArray,
    StringViewArray, StructArray,
};
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, expr_fn::create_udf};
use std::{collections::HashMap, fmt::Write, sync::Arc};

pub(crate) fn udf(name: &str) -> Arc<ScalarUDF> {
    let text = DataType::Utf8;
    let int = DataType::Int32;
    let list = DataType::List(Arc::new(Field::new("item", text.clone(), true)));
    let (args, output) = match name {
        "regex_split" => (vec![text.clone(), text, int], list),
        "first_match_in" => (vec![text.clone(), text], list),
        "hamming" => (vec![text.clone(), text], int),
        "entropy" => (vec![text], DataType::Float64),
        "unicode_slice" => (
            vec![text.clone(), int.clone(), int.clone(), int.clone(), int],
            text,
        ),
        "unicode_translate" => {
            let dictionary = crate::expressions::Type::Dict {
                dict: Box::new((
                    crate::expressions::Type::Primitive(crate::expressions::PrimitiveType::Str),
                    crate::expressions::Type::Primitive(crate::expressions::PrimitiveType::Str),
                )),
            }
            .to_arrow()
            .expect("valid string dictionary type");
            (vec![text.clone(), dictionary], text)
        }
        _ => unreachable!("validated string kernel"),
    };
    let kernel = name.to_owned();
    Arc::new(create_udf(
        &format!("vail_{name}"),
        args,
        output,
        Volatility::Immutable,
        Arc::new(move |values| evaluate(&kernel, values)),
    ))
}

/// Scalars keep one shared value; arrays are borrowed without broadcasting or
/// copying strings. All three Arrow UTF-8 storage forms are accepted.
enum Text<'a> {
    Scalar(Option<&'a str>),
    Array(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}
impl<'a> Text<'a> {
    fn new(value: &'a ColumnarValue) -> Result<Self> {
        match value {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v),
            ) => Ok(Self::Scalar(v.as_deref())),
            ColumnarValue::Array(v) => match v.data_type() {
                DataType::Utf8 => v.as_any().downcast_ref().map(Self::Array),
                DataType::LargeUtf8 => v.as_any().downcast_ref().map(Self::Large),
                DataType::Utf8View => v.as_any().downcast_ref().map(Self::View),
                _ => None,
            }
            .ok_or_else(|| {
                DataFusionError::Execution("string argument requires UTF-8 storage".into())
            }),
            _ => exec_err!("string argument requires Utf8"),
        }
    }
    fn get(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Scalar(v) => *v,
            Self::Array(v) => (!v.is_null(row)).then(|| v.value(row)),
            Self::Large(v) => (!v.is_null(row)).then(|| v.value(row)),
            Self::View(v) => (!v.is_null(row)).then(|| v.value(row)),
        }
    }
}
enum Integer<'a> {
    Scalar(Option<i32>),
    Array(&'a Int32Array),
}
impl<'a> Integer<'a> {
    fn new(value: &'a ColumnarValue) -> Result<Self> {
        match value {
            ColumnarValue::Scalar(ScalarValue::Int32(v)) => Ok(Self::Scalar(*v)),
            ColumnarValue::Array(v) => {
                v.as_any().downcast_ref().map(Self::Array).ok_or_else(|| {
                    DataFusionError::Execution("integer argument requires Int32".into())
                })
            }
            _ => exec_err!("integer argument requires Int32"),
        }
    }
    fn get(&self, row: usize) -> Option<i32> {
        match self {
            Self::Scalar(v) => *v,
            Self::Array(v) => (!v.is_null(row)).then(|| v.value(row)),
        }
    }
}

fn evaluate(name: &str, values: &[ColumnarValue]) -> Result<ColumnarValue> {
    let rows = values
        .iter()
        .find_map(|v| match v {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        })
        .unwrap_or(1);
    let text = Text::new(&values[0])?;
    let result: ArrayRef = match name {
        "hamming" => {
            let right = Text::new(&values[1])?;
            Arc::new(Int32Array::from_iter((0..rows).map(|row| {
                let (left, right) = (text.get(row)?, right.get(row)?);
                // One pass checks length equality and counts mismatches.
                let mut left = left.chars();
                let mut right = right.chars();
                let mut count = 0;
                loop {
                    match (left.next(), right.next()) {
                        (Some(a), Some(b)) => count += i32::from(a != b),
                        (None, None) => return Some(count),
                        _ => return None,
                    }
                }
            })))
        }
        "entropy" => Arc::new(Float64Array::from_iter((0..rows).map(|row| {
            let value = text.get(row)?;
            let mut counts = HashMap::new();
            let mut total = 0usize;
            for c in value.chars() {
                *counts.entry(c).or_insert(0usize) += 1;
                total += 1;
            }
            Some(
                counts
                    .values()
                    .map(|&n| {
                        let p = n as f64 / total as f64;
                        -p * p.log2()
                    })
                    .sum::<f64>(),
            )
        }))),
        "regex_split" => split(
            &text,
            &Text::new(&values[1])?,
            &Integer::new(&values[2])?,
            rows,
        )?,
        "first_match_in" => first_match(&text, &Text::new(&values[1])?, rows)?,
        "unicode_slice" => slice(
            &text,
            &Integer::new(&values[1])?,
            &Integer::new(&values[2])?,
            &Integer::new(&values[3])?,
            &Integer::new(&values[4])?,
            rows,
        )?,
        "unicode_translate" => translate(&text, &values[1], rows)?,
        _ => unreachable!("validated string kernel"),
    };
    if values.iter().all(|v| matches!(v, ColumnarValue::Scalar(_))) {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

/// Per invocation cache is bounded by the batch's distinct patterns. Scalar
/// patterns retain one borrowed string and compile at most once, only when used.
fn pattern<'c, 'p>(
    cache: &'c mut HashMap<&'p str, regex::Regex>,
    value: &'p str,
) -> Result<&'c regex::Regex> {
    use std::collections::hash_map::Entry;
    Ok(match cache.entry(value) {
        Entry::Occupied(e) => e.into_mut(),
        Entry::Vacant(e) => {
            e.insert(regex::Regex::new(value).map_err(|e| {
                DataFusionError::Execution(format!("invalid regular expression: {e}"))
            })?)
        }
    })
}
fn first_match(text: &Text<'_>, patterns: &Text<'_>, rows: usize) -> Result<ArrayRef> {
    let mut out = ListBuilder::new(StringBuilder::new());
    let mut cache = HashMap::new();
    for row in 0..rows {
        let (Some(value), Some(regex)) = (text.get(row), patterns.get(row)) else {
            out.append(false);
            continue;
        };
        if let Some(captures) = pattern(&mut cache, regex)?.captures(value) {
            for capture in captures.iter().skip(1) {
                out.values().append_option(capture.map(|m| m.as_str()));
            }
            out.append(true);
        } else {
            out.append(false);
        }
    }
    Ok(Arc::new(out.finish()))
}
fn split(
    text: &Text<'_>,
    patterns: &Text<'_>,
    limits: &Integer<'_>,
    rows: usize,
) -> Result<ArrayRef> {
    let mut out = ListBuilder::new(StringBuilder::new());
    let mut cache = HashMap::new();
    for row in 0..rows {
        let (Some(value), Some(regex), Some(limit)) =
            (text.get(row), patterns.get(row), limits.get(row))
        else {
            out.append(false);
            continue;
        };
        let re = pattern(&mut cache, regex)?;
        let mut parts = Vec::new();
        let mut previous = 0;
        for m in re.find_iter(value) {
            if m.start() == 0 && m.end() == 0 {
                continue;
            }
            if limit > 0 && parts.len() == limit as usize - 1 {
                break;
            }
            parts.push(&value[previous..m.start()]);
            previous = m.end();
        }
        parts.push(&value[previous..]);
        if limit == 0 && !value.is_empty() {
            while parts.last() == Some(&"") {
                parts.pop();
            }
        }
        for part in parts {
            out.values().append_value(part);
        }
        out.append(true);
    }
    Ok(Arc::new(out.finish()))
}

/// Bounds are normalized once in codepoint coordinates. Forward and reverse
/// iterators use two linear passes (count and output); only selected output is
/// allocated.
fn slice(
    text: &Text<'_>,
    starts: &Integer<'_>,
    stops: &Integer<'_>,
    steps: &Integer<'_>,
    flags: &Integer<'_>,
    rows: usize,
) -> Result<ArrayRef> {
    let mut out = StringBuilder::new();
    for row in 0..rows {
        let (Some(value), Some(start), Some(stop), Some(step), Some(flags)) = (
            text.get(row),
            starts.get(row),
            stops.get(row),
            steps.get(row),
            flags.get(row),
        ) else {
            out.append_null();
            continue;
        };
        if step == 0 {
            return exec_err!("string slice step cannot be zero");
        }
        let n = value.chars().count() as i64;
        let step = i64::from(step);
        let normalize = |bound: i32| {
            let bound = i64::from(bound);
            let bound = if bound < 0 { bound + n } else { bound };
            if step > 0 {
                bound.clamp(0, n)
            } else {
                bound.clamp(-1, n - 1)
            }
        };
        let start = if flags & 1 != 0 {
            normalize(start)
        } else if step > 0 {
            0
        } else {
            n - 1
        };
        let stop = if flags & 2 != 0 {
            normalize(stop)
        } else if step > 0 {
            n
        } else {
            -1
        };
        if step > 0 {
            for (index, c) in value.chars().enumerate() {
                let index = index as i64;
                if index >= stop {
                    break;
                }
                if index >= start && (index - start) % step == 0 {
                    out.write_char(c).expect("StringBuilder writes cannot fail");
                }
            }
        } else {
            for (index, c) in value.chars().rev().enumerate() {
                let index = n - 1 - index as i64;
                if index <= stop {
                    break;
                }
                if index <= start && (start - index) % (-step) == 0 {
                    out.write_char(c).expect("StringBuilder writes cannot fail");
                }
            }
        }
        out.append_value("");
    }
    Ok(Arc::new(out.finish()))
}

struct Replacements {
    values: ArrayRef,
    scalar: bool,
}
impl Replacements {
    fn new(value: &ColumnarValue) -> Result<Self> {
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let values = value.clone().into_array(1)?;
        if values.as_any().downcast_ref::<ListArray>().is_none() {
            return exec_err!("translation requires dictionary storage");
        }
        Ok(Self { values, scalar })
    }
    fn prepare(&self, row: usize) -> Result<Option<HashMap<char, Option<&str>>>> {
        let lists = self
            .values
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("validated lists");
        if lists.is_null(row) {
            return Ok(None);
        }
        let entries = lists
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("invalid translation dictionary".into()))?;
        let keys = entries
            .column_by_name("key")
            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| DataFusionError::Execution("translation keys require strings".into()))?;
        let values = entries
            .column_by_name("value")
            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Execution("translation values require strings".into())
            })?;
        let mut result = HashMap::with_capacity(lists.value_length(row) as usize);
        for at in lists.value_offsets()[row]..lists.value_offsets()[row + 1] {
            let at = at as usize;
            if entries.is_null(at) || keys.is_null(at) {
                return exec_err!("translate mapping keys must be one Unicode codepoint");
            }
            let mut chars = keys.value(at).chars();
            let Some(key) = chars.next() else {
                return exec_err!("translate mapping keys must be one Unicode codepoint");
            };
            if chars.next().is_some() {
                return exec_err!("translate mapping keys must be one Unicode codepoint");
            }
            result
                .entry(key)
                .or_insert_with(|| (!values.is_null(at)).then(|| values.value(at)));
        }
        Ok(Some(result))
    }
}
fn translate(text: &Text<'_>, mapping: &ColumnarValue, rows: usize) -> Result<ArrayRef> {
    let source = Replacements::new(mapping)?;
    let mut out = StringBuilder::new();
    let write = |out: &mut StringBuilder, value: &str, map: &HashMap<char, Option<&str>>| {
        for c in value.chars() {
            match map.get(&c) {
                Some(replacement) => out
                    .write_str(replacement.unwrap_or("null"))
                    .expect("StringBuilder writes cannot fail"),
                None => out.write_char(c).expect("StringBuilder writes cannot fail"),
            }
        }
        out.append_value("");
    };
    if source.scalar {
        // Do not validate a map in a branch with no defined input values.
        let map = if (0..rows).any(|row| text.get(row).is_some()) {
            source.prepare(0)?
        } else {
            None
        };
        for row in 0..rows {
            match (text.get(row), map.as_ref()) {
                (Some(value), Some(map)) => write(&mut out, value, map),
                _ => out.append_null(),
            }
        }
    } else {
        for row in 0..rows {
            let Some(value) = text.get(row) else {
                out.append_null();
                continue;
            };
            match source.prepare(row)? {
                Some(map) => write(&mut out, value, &map),
                None => out.append_null(),
            }
        }
    }
    Ok(Arc::new(out.finish()))
}
