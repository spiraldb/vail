//! Immutable single-row Arrow IPC globals, bound into the snapshot descriptor.
use super::{Result, StorageError, file_uri, local_path, sha256_file};
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::Schema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{fs::File, path::Path, sync::Arc};

#[derive(Serialize, Deserialize)]
struct Pointer {
    format: String,
    path: String,
    sha256: String,
}

pub fn empty() -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?)
}

pub fn write(directory: &Path, batch: &RecordBatch) -> Result<Value> {
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
        sha256: sha256_file(&path)?,
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
    if sha256_file(&path)? != pointer.sha256 {
        return Err(StorageError::Invalid("globals checksum mismatch".into()));
    }
    let mut reader = FileReader::try_new(File::open(path)?, None)?;
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| StorageError::Invalid("globals file is empty".into()))?;
    if batch.num_rows() != 1 || reader.next().transpose()?.is_some() {
        return Err(StorageError::Invalid(
            "globals must contain exactly one row in one batch".into(),
        ));
    }
    Ok(batch)
}
