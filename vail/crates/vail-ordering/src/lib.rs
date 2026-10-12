//! Canonical, lexicographically ordered keys for Vail values.
//!
//! A key is the same byte sequence in DataFusion sorting/grouping/joining and
//! in the Iceberg fragment dictionary. Missing sorts after present values at
//! every nesting level. Lists and structs compare lexicographically; sets and
//! dictionaries compare their sorted elements/entries, independently of their
//! physical iteration order.
use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeStringArray, ListArray, StringArray, StructArray,
};
use arrow_schema::{ArrowError, DataType};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KeyError {
    #[error("unsupported Hail key type {0:?}")]
    Unsupported(DataType),
    #[error("invalid Hail key representation: {0}")]
    Invalid(&'static str),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

/// Versioned logical type metadata lives on the first struct child or list
/// element field, so it remains accessible from Arrow's DataType API.
pub const TYPE_VERSION: &str = "vail.type.version";
pub const TYPE_DESCRIPTOR: &str = "vail.type";

pub fn type_metadata(dtype: &DataType) -> Result<Option<Value>, KeyError> {
    let field = match dtype {
        DataType::Struct(fields) => fields.first(),
        DataType::List(field) | DataType::LargeList(field) => Some(field),
        _ => None,
    };
    let Some(field) = field else { return Ok(None) };
    let metadata = field.metadata();
    match (metadata.get(TYPE_VERSION), metadata.get(TYPE_DESCRIPTOR)) {
        (None, None) => Ok(None),
        (Some(version), Some(raw)) if version == "1" => serde_json::from_str(raw)
            .map(Some)
            .map_err(|_| KeyError::Invalid("invalid Arrow logical type metadata")),
        (Some(_), Some(_)) => Err(KeyError::Invalid(
            "unsupported Arrow logical type metadata version",
        )),
        _ => Err(KeyError::Invalid("incomplete Arrow logical type metadata")),
    }
}

pub fn tag_type(dtype: DataType, logical: &Value) -> DataType {
    let tag = |field: &arrow_schema::Field| {
        let mut metadata = field.metadata().clone();
        metadata.insert(TYPE_VERSION.into(), "1".into());
        metadata.insert(TYPE_DESCRIPTOR.into(), logical.to_string());
        std::sync::Arc::new(field.clone().with_metadata(metadata))
    };
    match dtype {
        DataType::Struct(fields) => {
            let mut fields: Vec<_> = fields.iter().cloned().collect();
            if let Some(first) = fields.first_mut() {
                *first = tag(first);
            }
            DataType::Struct(fields.into())
        }
        DataType::List(field) => DataType::List(tag(&field)),
        other => other,
    }
}

/// Metadata and nullable flags do not change the buffers' physical layout.
pub fn same_layout(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(a, b)| a.name() == b.name() && same_layout(a.data_type(), b.data_type()))
        }
        (DataType::List(a), DataType::List(b)) => same_layout(a.data_type(), b.data_type()),
        _ => a == b,
    }
}

/// A metadata-free view for library kernels that build fresh child fields.
/// Logical typing remains on the IR and is restored on the result.
pub fn without_type_metadata(dtype: &DataType) -> DataType {
    let clean = |field: &arrow_schema::Field| {
        let mut metadata = field.metadata().clone();
        metadata.remove(TYPE_VERSION);
        metadata.remove(TYPE_DESCRIPTOR);
        std::sync::Arc::new(
            field
                .clone()
                .with_metadata(metadata)
                .with_data_type(without_type_metadata(field.data_type())),
        )
    };
    match dtype {
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|f| clean(f)).collect::<Vec<_>>().into())
        }
        DataType::List(field) => DataType::List(clean(field)),
        other => other.clone(),
    }
}

pub fn has_type_metadata(dtype: &DataType) -> bool {
    match dtype {
        DataType::Struct(fields) => fields
            .iter()
            .any(|f| f.metadata().contains_key(TYPE_VERSION) || has_type_metadata(f.data_type())),
        DataType::List(field) => {
            field.metadata().contains_key(TYPE_VERSION) || has_type_metadata(field.data_type())
        }
        _ => false,
    }
}

pub fn supports(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary => true,
        DataType::Struct(fields) => fields.iter().all(|f| supports(f.data_type())),
        DataType::List(field) => supports(field.data_type()),
        _ => false,
    }
}

pub fn encode_array(array: &dyn Array) -> Result<BinaryArray, KeyError> {
    if !supports(array.data_type()) {
        return Err(KeyError::Unsupported(array.data_type().clone()));
    }
    // Arrow field metadata is constant for the whole array. Walk the type tree
    // once so a reference definition (which may contain thousands of contigs)
    // is not parsed for every row during key encoding.
    let logical = logical_type_from_arrow(array.data_type())?;
    let values = (0..array.len())
        .map(|row| encode_typed_value(array, row, logical.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BinaryArray::from_iter_values(
        values.iter().map(Vec::as_slice),
    ))
}

/// Build only the logical wrappers needed to retain embedded reference
/// metadata through nested Arrow types. `null` children mean that the child
/// needs no logical override and are filtered out at the corresponding node.
fn logical_type_from_arrow(dtype: &DataType) -> Result<Option<Value>, KeyError> {
    if let Some(logical) = type_metadata(dtype)? {
        return Ok(Some(logical));
    }
    match dtype {
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|field| {
                    Ok(serde_json::json!([
                        field.name(),
                        logical_type_from_arrow(field.data_type())?.unwrap_or(Value::Null)
                    ]))
                })
                .collect::<Result<Vec<_>, KeyError>>()?;
            Ok(Some(serde_json::json!({"struct": children})))
        }
        DataType::List(field) => Ok(Some(serde_json::json!({"array":
            logical_type_from_arrow(field.data_type())?.unwrap_or(Value::Null)}))),
        _ => Ok(None),
    }
}

pub fn encode_value(array: &dyn Array, row: usize) -> Result<Vec<u8>, KeyError> {
    encode_typed_value(array, row, None)
}

/// Encode a value using its Hail logical type. Iceberg can rename nested Arrow
/// child fields, so storage must use this descriptor for sets, maps and calls.
pub fn encode_typed_value(
    array: &dyn Array,
    row: usize,
    logical: Option<&Value>,
) -> Result<Vec<u8>, KeyError> {
    let mut out = Vec::new();
    let inferred;
    let logical = match logical {
        Some(value) => Some(value),
        None => {
            inferred = logical_type_from_arrow(array.data_type())?;
            inferred.as_ref()
        }
    };
    append_value(&mut out, array, row, logical)?;
    Ok(out)
}

fn append_value(
    out: &mut Vec<u8>,
    array: &dyn Array,
    row: usize,
    logical: Option<&Value>,
) -> Result<(), KeyError> {
    if array.is_null(row) {
        out.push(2);
        return Ok(());
    }
    if logical.is_none() {
        if let DataType::Struct(fields) = array.data_type() {
            if fields.len() == 3
                && fields[1].data_type() == &DataType::Utf8
                && let Some(raw) = fields[1].metadata().get("vail.reference")
            {
                let definition: Value = serde_json::from_str(raw)
                    .map_err(|_| KeyError::Invalid("invalid locus reference metadata"))?;
                let descriptor = serde_json::json!({"locus": definition});
                return append_value(out, array, row, Some(&descriptor));
            }
        }
    }
    out.push(1);
    match array.data_type() {
        DataType::Struct(fields) if logical.and_then(|v| v.get("locus")).is_some() => {
            if fields.len() != 3
                || fields[0].data_type() != &DataType::Int32
                || fields[1].data_type() != &DataType::Utf8
                || fields[2].data_type() != &DataType::Int32
            {
                return Err(KeyError::Invalid("invalid locus fields"));
            }
            let reference = logical.unwrap().get("locus").unwrap();
            let contigs = reference
                .get("contigs")
                .and_then(Value::as_array)
                .ok_or(KeyError::Invalid("locus reference missing contigs"))?;
            let lengths = reference
                .get("lengths")
                .and_then(Value::as_object)
                .ok_or(KeyError::Invalid("locus reference missing lengths"))?;
            let values = down::<StructArray>(array)?;
            let ordinal = down::<Int32Array>(values.column(0).as_ref())?;
            let contig = down::<StringArray>(values.column(1).as_ref())?;
            let position = down::<Int32Array>(values.column(2).as_ref())?;
            if ordinal.is_null(row) || contig.is_null(row) || position.is_null(row) {
                return Err(KeyError::Invalid("locus fields must be present"));
            }
            let name = contig.value(row);
            usize::try_from(ordinal.value(row))
                .ok()
                .and_then(|i| contigs.get(i))
                .and_then(Value::as_str)
                .filter(|expected| *expected == name)
                .ok_or(KeyError::Invalid("locus ordinal does not match contig"))?;
            let length = lengths
                .get(name)
                .and_then(Value::as_i64)
                .ok_or(KeyError::Invalid("locus contig length is missing"))?;
            if position.value(row) < 1 || i64::from(position.value(row)) > length {
                return Err(KeyError::Invalid("invalid locus coordinate"));
            }
            append_value(out, values.column(0).as_ref(), row, None)?;
            append_value(out, values.column(1).as_ref(), row, None)?;
            append_value(out, values.column(2).as_ref(), row, None)?;
        }
        DataType::Boolean => out.push(u8::from(down::<BooleanArray>(array)?.value(row))),
        DataType::Int32 => {
            let bits = (down::<Int32Array>(array)?.value(row) as u32) ^ (1 << 31);
            out.extend(bits.to_be_bytes());
        }
        DataType::Int64 => {
            let bits = (down::<Int64Array>(array)?.value(row) as u64) ^ (1 << 63);
            out.extend(bits.to_be_bytes());
        }
        DataType::Float32 => {
            let value = down::<Float32Array>(array)?.value(row);
            // IEEE 754 totalOrder: preserve NaN sign, signaling bit and payload.
            // This sortable transform agrees with f32::total_cmp.
            let bits = value.to_bits();
            let ordered = if bits & (1 << 31) != 0 {
                !bits
            } else {
                bits ^ (1 << 31)
            };
            out.extend(ordered.to_be_bytes());
        }
        DataType::Float64 => {
            let value = down::<Float64Array>(array)?.value(row);
            // Same totalOrder transform as float32, at the native width.
            let bits = value.to_bits();
            let ordered = if bits & (1 << 63) != 0 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            out.extend(ordered.to_be_bytes());
        }
        DataType::Utf8 => append_bytes(out, down::<StringArray>(array)?.value(row).as_bytes()),
        DataType::LargeUtf8 => {
            append_bytes(out, down::<LargeStringArray>(array)?.value(row).as_bytes())
        }
        DataType::Binary => append_bytes(out, down::<BinaryArray>(array)?.value(row)),
        DataType::Struct(fields)
            if fields.len() == 4 && logical.is_some_and(|v| v.get("interval").is_some()) =>
        {
            let interval = down::<StructArray>(array)?;
            if interval.column(1).is_null(row) || interval.column(3).is_null(row) {
                return Err(KeyError::Invalid(
                    "interval inclusivity flags must be present",
                ));
            }
            let point_type = logical.and_then(|v| v.get("interval"));
            append_value(out, interval.column(0).as_ref(), row, point_type)?;
            let start_exclusive = down::<BooleanArray>(interval.column(1).as_ref())?.value(row);
            // Hail compares left endpoints by point, then sign (-1 inclusive, +1 exclusive).
            out.push(u8::from(start_exclusive));
            append_value(out, interval.column(2).as_ref(), row, point_type)?;
            let includes_end = down::<BooleanArray>(interval.column(3).as_ref())?.value(row);
            // Right endpoints use sign (-1 exclusive, +1 inclusive).
            out.push(u8::from(includes_end));
        }
        DataType::Struct(fields) if logical.is_some_and(|v| v == "call") => {
            let call = down::<StructArray>(array)?;
            let alleles = down::<ListArray>(call.column(0).as_ref())?.value(row);
            let alleles = down::<Int32Array>(alleles.as_ref())?;
            if alleles.len() > 2 || alleles.iter().any(|a| a.is_none_or(|a| a < 0)) {
                return Err(KeyError::Invalid("invalid call alleles"));
            }
            let phased = down::<BooleanArray>(call.column(1).as_ref())?.value(row);
            let repr = match alleles.len() {
                0 => 0,
                1 => alleles.value(0) as i64,
                2 => {
                    let a = alleles.value(0) as i64;
                    let b = alleles.value(1) as i64;
                    let (j, k) = if phased {
                        (a, a + b)
                    } else {
                        (a.min(b), a.max(b))
                    };
                    k * (k + 1) / 2 + j
                }
                _ => unreachable!(),
            };
            if repr > (1 << 29) - 1 {
                return Err(KeyError::Invalid("call allele representation overflow"));
            }
            let call_bits =
                ((repr as i32) << 3) | ((alleles.len() as i32) << 1) | i32::from(phased);
            out.extend(((call_bits as u32) ^ (1 << 31)).to_be_bytes());
        }
        DataType::Struct(fields) if logical.is_some_and(|t| t.get("ndarray").is_some()) => {
            // Shape first, then row-major elements. Never compare buffers by
            // their physical layout; the logical value defines the key.
            let values = down::<StructArray>(array)?;
            let shape_type = serde_json::json!({"array":"int64"});
            let element_type = logical
                .and_then(|t| t.get("ndarray"))
                .and_then(Value::as_array)
                .and_then(|parts| parts.first());
            let data_type = element_type.map(|t| serde_json::json!({"array":t}));
            append_value(out, values.column(1).as_ref(), row, Some(&shape_type))?;
            append_value(out, values.column(0).as_ref(), row, data_type.as_ref())?;
        }
        DataType::Struct(_) => {
            let values = down::<StructArray>(array)?;
            let children = logical
                .and_then(|t| t.get("struct").or_else(|| t.get("tuple")))
                .and_then(Value::as_array);
            for (i, child) in values.columns().iter().enumerate() {
                let child_type = children
                    .and_then(|fields| fields.get(i))
                    .map(|v| if v.is_array() { &v[1] } else { v })
                    .filter(|v| !v.is_null());
                append_value(out, child.as_ref(), row, child_type)?;
            }
        }
        DataType::List(_) => {
            let values = down::<ListArray>(array)?.value(row);
            let element_type = logical
                .and_then(|t| t.get("array").or_else(|| t.get("set")))
                .filter(|v| !v.is_null());
            let dict_types = logical
                .and_then(|t| t.get("dict"))
                .and_then(Value::as_array);
            let mut elements = (0..values.len())
                .map(|i| {
                    if let Some(types) = dict_types {
                        let entry = down::<StructArray>(values.as_ref())?;
                        let mut encoded = vec![1];
                        append_value(
                            &mut encoded,
                            entry.column(0).as_ref(),
                            i,
                            types.first().filter(|v| !v.is_null()),
                        )?;
                        append_value(
                            &mut encoded,
                            entry.column(1).as_ref(),
                            i,
                            types.get(1).filter(|v| !v.is_null()),
                        )?;
                        Ok(encoded)
                    } else {
                        encode_typed_value(values.as_ref(), i, element_type)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let is_set = logical.is_some_and(|t| t.get("set").is_some());
            let is_dict = dict_types.is_some();
            if is_set || is_dict {
                elements.sort();
                // Sets and maps contain unique members/keys by construction.
                // Deduplication also makes a malformed duplicate set harmless.
                if is_set {
                    elements.dedup();
                }
            }
            for element in elements {
                out.extend(element);
            }
            out.push(0); // strict prefix sorts before an extension
        }
        other => return Err(KeyError::Unsupported(other.clone())),
    }
    Ok(())
}

fn append_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        if *byte == 0 {
            out.extend([0, 255]);
        } else {
            out.push(*byte);
        }
    }
    out.extend([0, 0]);
}

fn down<T: Array + 'static>(array: &dyn Array) -> Result<&T, KeyError> {
    array.as_any().downcast_ref::<T>().ok_or(KeyError::Invalid(
        "Arrow array does not match its data type",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, ListArray};
    use std::sync::Arc;

    #[test]
    fn versioned_locus_keys_infer_reference_and_reject_forged_coordinates() {
        let reference = serde_json::json!({"name":"r","contigs":["chr1"],"lengths":{"chr1":10},"x_contigs":[],"y_contigs":[],"mt_contigs":[],"par":[]});
        let DataType::Struct(fields) = tag_type(
            DataType::Struct(
                vec![
                    Arc::new(arrow_schema::Field::new("renamed0", DataType::Int32, true)),
                    Arc::new(arrow_schema::Field::new("renamed1", DataType::Utf8, true)),
                    Arc::new(arrow_schema::Field::new("renamed2", DataType::Int32, true)),
                ]
                .into(),
            ),
            &serde_json::json!({"locus": reference}),
        ) else {
            unreachable!()
        };
        let forged = StructArray::try_new(
            fields.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec![Some("chr1")])),
                Arc::new(Int32Array::from(vec![Some(1)])),
            ],
            None,
        )
        .unwrap();
        assert!(encode_value(&forged, 0).is_err());
        assert!(
            encode_typed_value(&forged, 0, Some(&serde_json::json!({"locus":reference}))).is_err()
        );

        let valid = StructArray::try_new(
            fields.into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(0)])),
                Arc::new(StringArray::from(vec![Some("chr1")])),
                Arc::new(Int32Array::from(vec![Some(1)])),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            encode_value(&valid, 0).unwrap(),
            encode_typed_value(&valid, 0, Some(&serde_json::json!({"locus":reference}))).unwrap()
        );
        let encoded = encode_array(&valid).unwrap();
        assert_eq!(encoded.value(0), encode_value(&valid, 0).unwrap());

        let nested = StructArray::try_new(
            vec![Arc::new(arrow_schema::Field::new(
                "nested_locus",
                valid.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(valid.clone())],
            None,
        )
        .unwrap();
        let nested_keys = encode_array(&nested).unwrap();
        assert_eq!(nested_keys.value(0), encode_value(&nested, 0).unwrap());
    }

    #[test]
    fn float_keys_agree_with_ieee_total_order() {
        // Boundary patterns include both NaN signs, signaling/quiet bits and
        // multiple payloads, infinities, subnormals, and signed zeros.
        let bits64 = [
            0xffff_ffff_ffff_ffff,
            0xfff8_0000_0000_0002,
            0xfff8_0000_0000_0001,
            0xfff0_0000_0000_0001,
            0xfff0_0000_0000_0000,
            0xbff0_0000_0000_0000,
            0x8000_0000_0000_0001,
            0x8000_0000_0000_0000,
            0,
            1,
            0x3ff0_0000_0000_0000,
            0x7ff0_0000_0000_0000,
            0x7ff0_0000_0000_0001,
            0x7ff8_0000_0000_0001,
            0x7ff8_0000_0000_0002,
            0x7fff_ffff_ffff_ffff,
        ];
        let values64 = bits64.map(f64::from_bits);
        let a = Float64Array::from(values64.to_vec());
        for (i, left) in values64.iter().enumerate() {
            for (j, right) in values64.iter().enumerate() {
                assert_eq!(
                    encode_value(&a, i)
                        .unwrap()
                        .cmp(&encode_value(&a, j).unwrap()),
                    left.total_cmp(right)
                );
            }
        }
        let bits32 = [
            0xffff_ffff,
            0xffc0_0002,
            0xffc0_0001,
            0xff80_0001,
            0xff80_0000,
            0xbf80_0000,
            0x8000_0001,
            0x8000_0000,
            0,
            1,
            0x3f80_0000,
            0x7f80_0000,
            0x7f80_0001,
            0x7fc0_0001,
            0x7fc0_0002,
            0x7fff_ffff,
        ];
        let values32 = bits32.map(f32::from_bits);
        let a = Float32Array::from(values32.to_vec());
        for (i, left) in values32.iter().enumerate() {
            for (j, right) in values32.iter().enumerate() {
                assert_eq!(
                    encode_value(&a, i)
                        .unwrap()
                        .cmp(&encode_value(&a, j).unwrap()),
                    left.total_cmp(right)
                );
            }
        }
        let missing = encode_value(&Float64Array::from(vec![None]), 0).unwrap();
        for value in values64 {
            assert!(encode_value(&Float64Array::from(vec![value]), 0).unwrap() < missing);
        }
    }

    #[test]
    fn interval_encoding_observes_endpoint_inclusivity() {
        use arrow_array::{BooleanArray, Int32Array};
        use arrow_schema::{Field, Fields};
        let point = DataType::Int32;
        let fields = Fields::from(vec![
            Field::new("__vail_interval_start", point.clone(), false),
            Field::new("__vail_interval_start_exclusive", DataType::Boolean, false),
            Field::new("__vail_interval_end", point.clone(), false),
            Field::new("__vail_interval_end_inclusive", DataType::Boolean, false),
        ]);
        let array = StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1, 1, 1])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false, false, false, false])) as ArrayRef,
                Arc::new(Int32Array::from(vec![5, 5, 4, 100, 5])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false, false, true, false, true])) as ArrayRef,
            ],
            None,
        );
        let logical = serde_json::json!({"interval":"int32"});
        let keys = (0..5)
            .map(|row| encode_typed_value(&array, row, Some(&logical)).unwrap())
            .collect::<Vec<_>>();
        assert!(keys[1] < keys[0]); // inclusive start sorts first
        assert!(keys[2] < keys[1]); // earlier end sorts first after equal start
        assert!(keys[3] < keys[0]); // start inclusivity precedes even a much earlier end
        assert!(keys[1] < keys[4]); // exclusive end precedes inclusive end
    }

    #[test]
    fn interval_encoding_preserves_missing_endpoints() {
        use arrow_array::{BooleanArray, Int32Array};
        use arrow_schema::{Field, Fields};
        let fields = Fields::from(vec![
            Field::new("__vail_interval_start", DataType::Int32, true),
            Field::new("__vail_interval_start_exclusive", DataType::Boolean, false),
            Field::new("__vail_interval_end", DataType::Int32, true),
            Field::new("__vail_interval_end_inclusive", DataType::Boolean, false),
        ]);
        let array = StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(3), Some(3)])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            ],
            None,
        );
        let logical = serde_json::json!({"interval":"int32"});
        let present = encode_typed_value(&array, 0, Some(&logical)).unwrap();
        let missing = encode_typed_value(&array, 1, Some(&logical)).unwrap();
        assert!(present < missing); // Hail's extended ordering puts missing points last.
    }

    #[test]
    fn strings_and_nested_missing_values() {
        let a = StringArray::from(vec![Some("a"), Some("a\0"), Some("a\u{80}"), None]);
        let values = (0..a.len())
            .map(|i| encode_value(&a, i).unwrap())
            .collect::<Vec<_>>();
        assert!(values.windows(2).all(|w| w[0] < w[1]));
        let list = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![Some(1), None]),
            Some(vec![Some(2)]),
            None,
        ]);
        let values = (0..list.len())
            .map(|i| encode_value(&list, i).unwrap())
            .collect::<Vec<_>>();
        assert!(values.windows(2).all(|w| w[0] < w[1]));
        let _as_ref: ArrayRef = Arc::new(list);
    }
}
