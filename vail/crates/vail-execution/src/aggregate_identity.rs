//! Diagnostic names for configured UDAFs. Optimizer identity uses the complete
//! native key in the shared reducer registry; a digest is never equality.
use arrow_schema::DataType;
pub(crate) fn configured_udaf_name(
    name: &str,
    input_type: &DataType,
    options: &serde_json::Value,
) -> String {
    crate::kernel_identity::name(&(name, input_type, options), name)
}
