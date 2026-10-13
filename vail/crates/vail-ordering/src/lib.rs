//! Arrow type metadata helpers for Vail's logical values.
//!
//! Ordering, grouping, and equality use native Arrow/DataFusion operators;
//! this crate only preserves logical type descriptors across Arrow operations.
pub mod call;

use arrow_schema::DataType;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("invalid Arrow logical type metadata: {0}")]
    Invalid(&'static str),
}

/// Versioned logical type metadata lives on the first struct child or list
/// element field, so it remains accessible from Arrow's DataType API.
pub const TYPE_VERSION: &str = "vail.type.version";
pub const TYPE_DESCRIPTOR: &str = "vail.type";

pub fn type_metadata(dtype: &DataType) -> Result<Option<Value>, MetadataError> {
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
            .map_err(|_| MetadataError::Invalid("invalid descriptor")),
        (Some(_), Some(_)) => Err(MetadataError::Invalid(
            "unsupported Arrow logical type metadata version",
        )),
        _ => Err(MetadataError::Invalid("incomplete descriptor")),
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
        // Nested functions produce generic list child fields. Normalize the
        // logical Array/Set field-name distinction on arguments; the IR cast
        // at each UDF result restores the declared logical type.
        DataType::List(field) => {
            let mut metadata = field.metadata().clone();
            metadata.remove(TYPE_VERSION);
            metadata.remove(TYPE_DESCRIPTOR);
            DataType::List(std::sync::Arc::new(
                arrow_schema::Field::new(
                    "item",
                    without_type_metadata(field.data_type()),
                    field.is_nullable(),
                )
                .with_metadata(metadata),
            ))
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn logical_type_metadata_round_trips_without_changing_layout() {
        let logical = serde_json::json!({"locus":{"name":"r","contigs":["chr1"],"lengths":{"chr1":10},"x_contigs":[],"y_contigs":[],"mt_contigs":[],"par":[]}});
        let physical = DataType::Struct(
            vec![
                Arc::new(arrow_schema::Field::new("ordinal", DataType::Int32, false)),
                Arc::new(arrow_schema::Field::new("contig", DataType::Utf8, false)),
                Arc::new(arrow_schema::Field::new("position", DataType::Int32, false)),
            ]
            .into(),
        );
        let tagged = tag_type(physical.clone(), &logical);
        assert_eq!(type_metadata(&tagged).unwrap(), Some(logical));
        assert!(same_layout(&physical, &tagged));
        assert!(has_type_metadata(&tagged));
        assert!(!has_type_metadata(&without_type_metadata(&tagged)));
    }

    #[test]
    fn udf_argument_layout_normalizes_nested_list_field_names() {
        let nested = DataType::List(Arc::new(arrow_schema::Field::new(
            "__vail_set_item",
            DataType::List(Arc::new(arrow_schema::Field::new(
                "__vail_set_item",
                DataType::Int32,
                true,
            ))),
            true,
        )));
        let normalized = without_type_metadata(&nested);
        let DataType::List(outer) = &normalized else {
            panic!("expected list")
        };
        assert_eq!(outer.name(), "item");
        let DataType::List(inner) = outer.data_type() else {
            panic!("expected nested list")
        };
        assert_eq!(inner.name(), "item");
        assert!(same_layout(&nested, &normalized));
    }
}
