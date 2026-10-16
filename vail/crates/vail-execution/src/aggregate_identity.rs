//! Lossless names for configured aggregate UDFs.
//!
//! DataFusion identifies calls by name and signature, so a digest alone cannot
//! establish equality of captured initializer options.
use arrow_schema::DataType;
use serde_json::Value;

fn canonical(value: Value) -> Value {
    match value {
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(name, value)| (name, canonical(value)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        value => value,
    }
}

pub(crate) fn configured_udaf_name(name: &str, input_type: &DataType, options: &Value) -> String {
    // Arrow descriptors and JSON values have infallible JSON representations.
    let descriptor = canonical(
        serde_json::to_value((name, input_type, options))
            .expect("Arrow/JSON configuration serializes"),
    );
    let identity = serde_json::to_vec(&descriptor).expect("JSON configuration serializes");
    let mut result = format!("{name}__vail_");
    result.reserve(identity.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in identity {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 15) as usize] as char);
    }
    result
}
