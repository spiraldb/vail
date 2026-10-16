//! Immutable single-row Arrow IPC globals, bound into the snapshot descriptor.
use super::{Result, StorageError, file_uri, local_path};
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{fs::File, path::Path, sync::Arc};
use vail_execution::expressions::Type;

#[derive(Serialize, Deserialize)]
struct Pointer {
    format: String,
    path: String,
}

/// Validate a globals frame without changing names, logical types or metadata.
/// Alternate Arrow string storage is permitted by the logical Type boundary.
pub fn validate_schema(schema: &Schema) -> Result<()> {
    if schema.fields().iter().any(|field| field.name().is_empty()) {
        return Err(StorageError::Invalid(
            "globals field names must be nonempty".into(),
        ));
    }
    Type::from_arrow(&DataType::Struct(schema.fields().clone()))
        .map_err(|error| StorageError::Invalid(error.to_string()))?;
    Ok(())
}
pub fn schema_from_wire(value: &Value) -> Result<SchemaRef> {
    if *value == json!({}) {
        return Ok(Arc::new(Schema::empty()));
    }
    let fields: Vec<(String, Type)> = serde_json::from_value(value.clone())?;
    let schema = Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(name, dtype)| {
                dtype
                    .to_arrow()
                    .map(|dtype| Field::new(name, dtype, true))
                    .map_err(|error| StorageError::Invalid(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?,
    ));
    validate_schema(&schema)?;
    Ok(schema)
}
pub fn validate_agreement(actual: &Schema, expected: &Schema) -> Result<()> {
    validate_schema(actual)?;
    validate_schema(expected)?;
    if actual.fields().len() != expected.fields().len() {
        return Err(StorageError::Invalid(
            "globals schema field count mismatch".into(),
        ));
    }
    for (actual, expected) in actual.fields().iter().zip(expected.fields()) {
        let actual_type = Type::from_arrow(actual.data_type())
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        let expected_type = Type::from_arrow(expected.data_type())
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        if actual.name() != expected.name() || actual_type != expected_type {
            return Err(StorageError::Invalid(
                "globals schema names or logical types disagree".into(),
            ));
        }
    }
    Ok(())
}

pub fn empty() -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?)
}

pub fn write(directory: &Path, batch: &RecordBatch) -> Result<Value> {
    validate_schema(batch.schema_ref())?;
    if batch.num_rows() != 1 {
        return Err(StorageError::Invalid(
            "globals must contain exactly one row".into(),
        ));
    }
    if batch.num_columns() == 0 {
        return Ok(json!({}));
    }
    std::fs::create_dir_all(directory)?;
    let path = directory.join("globals.arrow");
    let mut file = File::create_new(&path)?;
    {
        let mut writer = FileWriter::try_new(&mut file, batch.schema_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    file.sync_all()?;
    Ok(serde_json::to_value(Pointer {
        format: "arrow-ipc-v1".into(),
        path: file_uri(&path)?,
    })?)
}

pub fn read(pointer: &Value) -> Result<RecordBatch> {
    if *pointer == json!({}) {
        return empty();
    }
    let pointer: Pointer = serde_json::from_value(pointer.clone())?;
    if pointer.format != "arrow-ipc-v1" {
        return Err(StorageError::Unsupported("unknown globals encoding".into()));
    }
    let path = local_path(&pointer.path)?;
    let mut reader = FileReader::try_new(File::open(path)?, None)?;
    if reader.num_batches() != 1 {
        return Err(StorageError::Invalid(
            "globals must contain exactly one batch".into(),
        ));
    }
    validate_schema(&reader.schema())?;
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| StorageError::Invalid("globals file is empty".into()))?;
    if batch.num_rows() != 1 {
        return Err(StorageError::Invalid(
            "globals must contain exactly one row in one batch".into(),
        ));
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field};
    use serde_json::json;

    #[test]
    fn globals_roundtrip_without_checksum_and_ignores_legacy_checksum() {
        let directory = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("g", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![7]))]).unwrap();
        let mut pointer = write(directory.path(), &batch).unwrap();
        assert!(pointer.get("sha256").is_none());

        pointer["sha256"] = json!("legacy-checksum-is-ignored");
        assert_eq!(read(&pointer).unwrap(), batch);
    }
}
