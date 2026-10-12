//! Stable function names for configured aggregate UDFs.
//!
//! DataFusion identifies UDF calls by function name and signature. Captured
//! initializer options must therefore participate in the name when they alter
//! accumulator behavior.

use arrow_schema::DataType;

pub(crate) fn configured_udaf_name(
    name: &str,
    input_type: &DataType,
    options: &serde_json::Value,
) -> String {
    let identity = format!("{name}|{input_type:?}|{options}");
    let hash = identity.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    format!("{name}__vail_{hash:016x}")
}
