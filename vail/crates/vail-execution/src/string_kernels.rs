//! Native Unicode-scalar string operations without a DataFusion built-in.
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, Float64Array, Int32Array, StringArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, expr_fn::create_udf};
use std::{collections::HashMap, sync::Arc};

pub(crate) fn udf(name: &str) -> Arc<ScalarUDF> {
    let text = DataType::Utf8;
    let list = DataType::List(Arc::new(Field::new("item", text.clone(), true)));
    let (args, output) = match name {
        "regex_split" => (vec![text.clone(), text, DataType::Int32], list),
        "first_match_in" => (vec![text.clone(), text], list),
        "hamming" => (vec![text.clone(), text], DataType::Int32),
        "entropy" => (vec![text], DataType::Float64),
        _ => unreachable!("validated string kernel"),
    };
    let name = name.to_owned();
    let kernel = name.clone();
    Arc::new(create_udf(
        &format!("vail_{name}"),
        args,
        output,
        Volatility::Immutable,
        Arc::new(move |values| evaluate(&kernel, values)),
    ))
}

fn evaluate(name: &str, values: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(values)?;
    let strings = arrays
        .iter()
        .filter(|a| {
            matches!(
                a.data_type(),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            )
        })
        .map(|a| arrow_cast::cast(a, &DataType::Utf8))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let strings = strings
        .iter()
        .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())
        .collect::<Vec<_>>();
    let text = strings[0];
    let result: ArrayRef = match name {
        "hamming" => Arc::new(Int32Array::from_iter((0..text.len()).map(|i| {
            if text.is_null(i) || strings[1].is_null(i) {
                return None;
            }
            let left = text.value(i);
            let right = strings[1].value(i);
            if left.chars().count() != right.chars().count() {
                return None;
            }
            Some(
                left.chars()
                    .zip(right.chars())
                    .filter(|(a, b)| a != b)
                    .count() as i32,
            )
        }))),
        "entropy" => Arc::new(Float64Array::from_iter((0..text.len()).map(|i| {
            if text.is_null(i) {
                return None;
            }
            let mut counts = std::collections::BTreeMap::new();
            let mut total = 0usize;
            for c in text.value(i).chars() {
                *counts.entry(c).or_insert(0usize) += 1;
                total += 1;
            }
            let entropy = counts
                .values()
                .map(|&n| {
                    let p = n as f64 / total as f64;
                    -p * p.log2()
                })
                .sum::<f64>();
            Some(entropy)
        }))),
        "regex_split" | "first_match_in" => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            let mut patterns = HashMap::new();
            let limits = arrays
                .get(2)
                .and_then(|a| a.as_any().downcast_ref::<Int32Array>());
            for i in 0..text.len() {
                if text.is_null(i) || strings[1].is_null(i) || limits.is_some_and(|a| a.is_null(i))
                {
                    builder.append(false);
                    continue;
                }
                let pattern = strings[1].value(i);
                if !patterns.contains_key(pattern) {
                    let compiled = regex::Regex::new(pattern).map_err(|e| {
                        DataFusionError::Execution(format!("invalid regular expression: {e}"))
                    })?;
                    patterns.insert(pattern, compiled);
                }
                let re = &patterns[pattern];
                let value = text.value(i);
                if name == "first_match_in" {
                    if let Some(captures) = re.captures(value) {
                        for capture in captures.iter().skip(1) {
                            builder.values().append_option(capture.map(|m| m.as_str()));
                        }
                        builder.append(true);
                    } else {
                        builder.append(false);
                    }
                    continue;
                }
                let limit = limits.unwrap().value(i);
                let mut parts = Vec::new();
                let mut previous = 0;
                for m in re.find_iter(value) {
                    // A zero-width match at the beginning does not introduce
                    // an empty leading field; every offset is a UTF-8 boundary.
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
                    builder.values().append_value(part);
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        _ => unreachable!("validated string kernel"),
    };
    if values
        .iter()
        .all(|value| matches!(value, ColumnarValue::Scalar(_)))
    {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}
