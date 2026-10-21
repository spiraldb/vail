//! Typed JSON import and Hail's text-export annotation formatting.
//! JSON values are normalized to physical storage and decoded as one Arrow batch.
use crate::expressions::{PrimitiveType, Type};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};

/// Equality follows normalized JSON values. Hashing also identifies signed
/// floating zeroes, and ignores object insertion order, as Value equality does.
/// Hash-table collisions are resolved with full Value equality.
#[derive(PartialEq, Eq)]
struct JsonKey(Value);
impl Hash for JsonKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        fn value<H: Hasher>(item: &Value, state: &mut H) {
            match item {
                Value::Null => 0u8.hash(state),
                Value::Bool(v) => {
                    1u8.hash(state);
                    v.hash(state);
                }
                Value::Number(v) => {
                    2u8.hash(state);
                    if v.is_f64() && v.as_f64() == Some(0.0) {
                        "0.0".hash(state);
                    } else {
                        v.to_string().hash(state);
                    }
                }
                Value::String(v) => {
                    3u8.hash(state);
                    v.hash(state);
                }
                Value::Array(items) => {
                    4u8.hash(state);
                    items.len().hash(state);
                    for item in items {
                        value(item, state);
                    }
                }
                Value::Object(items) => {
                    5u8.hash(state);
                    items.len().hash(state);
                    let mut fields: Vec<_> = items.iter().collect();
                    fields.sort_unstable_by(|a, b| a.0.cmp(b.0));
                    for (name, item) in fields {
                        name.hash(state);
                        value(item, state);
                    }
                }
            }
        }
        value(&self.0, state);
    }
}

fn error(message: impl ToString) -> DataFusionError {
    DataFusionError::Execution(message.to_string())
}

/// Equality-only representation of an already normalized typed value. Arrays
/// retain order, while sets and dictionaries recursively ignore encounter order.
/// The original normalized value remains the public output, including the last
/// dictionary value at its first key position.
fn equality_key(value: &Value, dtype: &Type) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    fn ordered(items: impl Iterator<Item = Value>) -> Value {
        let mut items: Vec<_> = items.map(|v| (v.to_string(), v)).collect();
        items.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Value::Array(items.into_iter().map(|(_, value)| value).collect())
    }
    match dtype {
        Type::Primitive(PrimitiveType::Float32 | PrimitiveType::Float64)
            if value.as_f64() == Some(0.0) =>
        {
            json!(0.0)
        }
        Type::Array { array } => Value::Array(
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|v| equality_key(v, array))
                .collect(),
        ),
        Type::Set { set } => ordered(
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|v| equality_key(v, set)),
        ),
        Type::Dict { dict } => ordered(value.as_array().unwrap().iter().map(|entry| {
            json!({"key":equality_key(&entry["key"], &dict.0),
                   "value":equality_key(&entry["value"], &dict.1)})
        })),
        Type::Struct { r#struct: fields } => Value::Object(
            fields
                .iter()
                .map(|(name, ty)| (name.clone(), equality_key(&value[name], ty)))
                .collect(),
        ),
        Type::Tuple { tuple } => Value::Object(
            tuple
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    let name = format!("__vail_tuple_{i}");
                    let key = equality_key(&value[&name], ty);
                    (name, key)
                })
                .collect(),
        ),
        Type::Interval { interval } => {
            let mut result = value.as_object().unwrap().clone();
            for name in ["__vail_interval_start", "__vail_interval_end"] {
                result.insert(name.into(), equality_key(&value[name], interval));
            }
            Value::Object(result)
        }
        _ => value.clone(),
    }
}

fn normalize(v: &Value, t: &Type) -> Result<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    Ok(match t {
        Type::Primitive(PrimitiveType::Str) => match v {
            Value::String(_) => v.clone(),
            Value::Number(n) if n.is_i64() || n.is_u64() => Value::String(n.to_string()),
            _ => Value::Null,
        },
        Type::Primitive(PrimitiveType::Bool) => {
            if v.is_boolean() {
                v.clone()
            } else {
                Value::Null
            }
        }
        Type::Primitive(PrimitiveType::Int32) => {
            if let Some(n) = v.as_i64() {
                json!(n as i32)
            } else if let Some(n) = v.as_u64() {
                json!(n as i32)
            } else if let Some(s) = v.as_str() {
                json!(s.parse::<i32>().map_err(error)?)
            } else {
                Value::Null
            }
        }
        Type::Primitive(PrimitiveType::Int64) => {
            if let Some(n) = v.as_i64() {
                json!(n)
            } else if let Some(n) = v.as_u64() {
                json!(n as i64)
            } else {
                Value::Null
            }
        }
        Type::Primitive(PrimitiveType::Float32 | PrimitiveType::Float64) => {
            if v.is_number() {
                let n = v
                    .as_f64()
                    .ok_or_else(|| error("invalid JSON floating value"))?;
                // Deduplication must see the target numeric value, including
                // equal integer/float tokens and float32 rounding collisions.
                let n = if matches!(t, Type::Primitive(PrimitiveType::Float32)) {
                    f64::from(n as f32)
                } else {
                    n
                };
                if n.is_finite() {
                    json!(n)
                } else {
                    Value::String(if n.is_sign_negative() { "-inf" } else { "inf" }.into())
                }
            } else if let Some(s) = v.as_str() {
                let s = match s {
                    "nan" | "NaN" => "NaN",
                    "inf" | "Infinity" => "inf",
                    "-inf" | "-Infinity" => "-inf",
                    other => other.strip_prefix("-:").unwrap_or(other),
                };
                if matches!(t, Type::Primitive(PrimitiveType::Float32))
                    && !matches!(s, "NaN" | "inf" | "-inf")
                {
                    Value::Null
                } else {
                    let n = s.parse::<f64>().map_err(error)?;
                    if n.is_finite() {
                        json!(n)
                    } else {
                        Value::String(
                            if n.is_nan() {
                                "NaN"
                            } else if n.is_sign_negative() {
                                "-inf"
                            } else {
                                "inf"
                            }
                            .into(),
                        )
                    }
                }
            } else {
                Value::Null
            }
        }
        Type::Primitive(PrimitiveType::Call) => {
            let Some(s) = v.as_str() else {
                return Ok(Value::Null);
            };
            let phased = s.contains('|');
            let s = s.strip_prefix('|').unwrap_or(s);
            let alleles = if s == "-" || s.is_empty() {
                vec![]
            } else {
                s.split(if phased { '|' } else { '/' })
                    .map(|a| a.parse::<i32>().map_err(error))
                    .collect::<Result<Vec<_>>>()?
            };
            let call = crate::calls::Call::new(&alleles, phased).map_err(error)?;
            json!({"__vail_call_allele0":call.first(), "__vail_call_allele1":call.second(), "__vail_call_ploidy":call.ploidy(), "__vail_call_phased":call.phased()})
        }
        Type::Struct { r#struct: fields } => {
            let Some(obj) = v.as_object() else {
                return Ok(Value::Null);
            };
            let mut out = serde_json::Map::new();
            for (name, ty) in fields {
                out.insert(
                    name.clone(),
                    normalize(obj.get(name).unwrap_or(&Value::Null), ty)?,
                );
            }
            Value::Object(out)
        }
        Type::Array { array: element } => {
            let Some(items) = v.as_array() else {
                return Ok(Value::Null);
            };
            Value::Array(
                items
                    .iter()
                    .map(|item| normalize(item, element))
                    .collect::<Result<_>>()?,
            )
        }
        Type::Set { set: element } => {
            let Some(items) = v.as_array() else {
                return Ok(Value::Null);
            };
            let mut out = Vec::with_capacity(items.len());
            let mut seen = HashSet::with_capacity(items.len());
            for item in items {
                let item = normalize(item, element)?;
                if seen.insert(JsonKey(equality_key(&item, element))) {
                    out.push(item);
                }
            }
            Value::Array(out)
        }
        Type::Tuple { tuple } => {
            let Some(items) = v.as_array() else {
                return Ok(Value::Null);
            };
            if items.len() > tuple.len() {
                return Err(error("JSON tuple has too many elements"));
            }
            let mut out = serde_json::Map::new();
            if tuple.is_empty() {
                out.insert("__vail_tuple_empty".into(), json!(true));
            }
            for (i, ty) in tuple.iter().enumerate() {
                out.insert(
                    format!("__vail_tuple_{i}"),
                    normalize(items.get(i).unwrap_or(&Value::Null), ty)?,
                );
            }
            Value::Object(out)
        }
        Type::Dict { dict } => {
            let items = if let Some(items) = v.as_array() {
                items.clone()
            } else if let Some(obj) = v
                .as_object()
                .filter(|_| dict.0 == Type::Primitive(PrimitiveType::Str))
            {
                obj.iter()
                    .map(|(k, v)| json!({"key":k,"value":v}))
                    .collect()
            } else {
                return Ok(Value::Null);
            };
            let mut out: Vec<Value> = Vec::new();
            let mut positions = HashMap::with_capacity(items.len());
            for item in items {
                if item.get("key").is_none() || item.get("value").is_none() {
                    continue;
                }
                let key = normalize(&item["key"], &dict.0)?;
                let value = normalize(&item["value"], &dict.1)?;
                let equality_key = JsonKey(equality_key(&key, &dict.0));
                let item = json!({"key":key,"value":value});
                if let Some(&i) = positions.get(&equality_key) {
                    out[i] = item;
                } else {
                    positions.insert(equality_key, out.len());
                    out.push(item);
                }
            }
            Value::Array(out)
        }
        Type::Interval { interval } => {
            let Some(obj) = v.as_object() else {
                return Ok(Value::Null);
            };
            if !["start", "end", "includeStart", "includeEnd"]
                .iter()
                .all(|k| obj.contains_key(*k))
            {
                return Ok(Value::Null);
            }
            json!({"__vail_interval_start":normalize(&v["start"], interval)?, "__vail_interval_end":normalize(&v["end"], interval)?, "__vail_interval_start_exclusive":!v["includeStart"].as_bool().ok_or_else(||error("interval includeStart must be Boolean"))?, "__vail_interval_end_inclusive":v["includeEnd"].as_bool().ok_or_else(||error("interval includeEnd must be Boolean"))?})
        }
        Type::Locus { locus } => {
            let Some(contig) = v["contig"].as_str() else {
                return Ok(Value::Null);
            };
            let position = v["position"]
                .as_i64()
                .and_then(|x| i32::try_from(x).ok())
                .ok_or_else(|| error("locus position must be int32"))?;
            let ordinal = locus
                .ordinal(contig)
                .ok_or_else(|| error("invalid locus contig"))?;
            locus
                .validate_coordinate(ordinal, contig, position)
                .map_err(error)?;
            json!({"__vail_locus_contig":contig,"__vail_locus_position":position,"__vail_locus_ordinal":ordinal})
        }
        Type::Ndarray { ndarray } => {
            if !v["shape"].is_array() || !v["data"].is_array() {
                return Ok(Value::Null);
            }
            if v["shape"].as_array().unwrap().len() != ndarray.1 {
                return Err(error("JSON ndarray rank mismatch"));
            }
            let count = v["shape"]
                .as_array()
                .unwrap()
                .iter()
                .try_fold(1usize, |n, dim| {
                    let dim = dim
                        .as_u64()
                        .and_then(|x| usize::try_from(x).ok())
                        .ok_or_else(|| {
                            error("JSON ndarray dimensions must be nonnegative integers")
                        })?;
                    n.checked_mul(dim)
                        .ok_or_else(|| error("JSON ndarray shape overflow"))
                })?;
            if count != v["data"].as_array().unwrap().len() {
                return Err(error("JSON ndarray shape does not match data length"));
            }
            json!({"__vail_ndarray_shape":v["shape"],"__vail_ndarray_data":normalize(&v["data"], &Type::Array {array:Box::new(ndarray.0.clone())})?})
        }
    })
}

fn decode(strings: &StringArray, target: &Type) -> Result<ArrayRef> {
    let mut rows = Vec::with_capacity(strings.len());
    for i in 0..strings.len() {
        rows.push(if strings.is_null(i) {
            Value::Null
        } else {
            normalize(
                &serde_json::from_str::<Value>(strings.value(i)).map_err(error)?,
                target,
            )?
        });
    }
    let dtype = vail_ordering::without_type_metadata(&target.to_arrow().map_err(error)?);
    if strings.is_empty() {
        return Ok(arrow_array::new_empty_array(&dtype));
    }
    let mut decoder = arrow_json::ReaderBuilder::new_with_field(Field::new("value", dtype, true))
        .build_decoder()?;
    decoder.serialize(&rows)?;
    Ok(decoder
        .flush()?
        .ok_or_else(|| error("JSON decoder produced no batch"))?
        .column(0)
        .clone())
}

pub(crate) fn parse_udf(target: Type) -> Result<ScalarUDF> {
    let output = vail_ordering::without_type_metadata(&target.to_arrow().map_err(error)?);
    let identity = serde_json::to_string(&target).map_err(error)?;
    Ok(crate::kernel_identity::scalar(
        "parse_json",
        identity,
        Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        output,
        Arc::new(move |args| {
            let [input] = args else {
                return Err(error("parse_json requires one string"));
            };
            let scalar = matches!(input, ColumnarValue::Scalar(_));
            let input = input.to_array(1)?;
            let strings = input
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| error("parse_json requires strings"))?;
            let out = decode(strings, &target)?;
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
            } else {
                Ok(ColumnarValue::Array(out))
            }
        }),
    ))
}

pub(crate) fn export_udf(target: Type, json: bool) -> Result<ScalarUDF> {
    let identity = serde_json::to_string(&target).map_err(error)?;
    Ok(crate::kernel_identity::scalar(
        "export_annotation",
        (identity, json),
        Signature::any(1, Volatility::Immutable),
        DataType::Utf8,
        Arc::new(move |args| {
            let [input] = args else {
                return Err(error("export annotation requires one argument"));
            };
            let scalar = matches!(input, ColumnarValue::Scalar(_));
            let input = input.to_array(1)?;
            let out = StringArray::from_iter(
                (0..input.len())
                    .map(|i| {
                        if json {
                            crate::expressions::json_annotation(&input, i, &target)
                                .map(Some)
                                .map_err(error)
                        } else if input.is_null(i) {
                            Ok(None)
                        } else {
                            crate::expressions::export_annotation(&input, i, &target)
                                .map(Some)
                                .map_err(error)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
            } else {
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
        }),
    ))
}
