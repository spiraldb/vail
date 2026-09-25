//! Local directory entrypoint. Immutable generations make publication atomic and
//! keep readers of the previous snapshot valid across an overwrite.
use super::*;
use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::table::StaticTable;
use iceberg::{
    Catalog, CatalogBuilder, MemoryCatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashMap, fs, io::Write, path::PathBuf, sync::Arc};

const ENTRY: &str = "_vail_table.json";
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Entry {
    version: u32,
    metadata: String,
    partition_rows: Vec<usize>,
    empty_row: bool,
}

pub struct LocalTable {
    pub partitions: Vec<Vec<RecordBatch>>,
    pub semantics: VailSemantics,
    pub keys: Vec<String>,
}

fn directory(path: &str) -> Result<PathBuf> {
    let path = if path.starts_with("file://") {
        local_path(path)?
    } else {
        if path.contains("://") {
            return Err(StorageError::Unsupported(
                "only local table paths are supported".into(),
            ));
        }
        let path = PathBuf::from(path);
        if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        }
    };
    file_uri(&path)?;
    Ok(path)
}

/// Rebind nested Arrow fields to the schema's Iceberg field IDs and normalize
/// reader view arrays. This changes representation, never row order.
pub fn normalize(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() != schema.fields().len() {
        return Err(StorageError::Invalid(
            "physical and logical column counts differ".into(),
        ));
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| arrow_cast::cast(array, field.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

pub async fn write_local(
    path: &str,
    overwrite: bool,
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    row_type: Value,
    keys: Vec<String>,
) -> Result<()> {
    let input = async_stream::try_stream! {
        for partition in partitions {
            yield PartitionEvent::StartPartition;
            for batch in partition { yield PartitionEvent::Batch(batch); }
        }
    }
    .boxed();
    write_local_stream(
        path,
        overwrite,
        schema,
        input,
        row_type,
        keys,
        WriteOptions::default(),
    )
    .await
}

/// Staging limits are measured in retained Arrow array bytes, not compressed
/// output size. A single oversized input batch may exceed the byte target; it is
/// flushed alone. Row limits also bound zero-column or highly compressed data.
#[derive(Clone, Copy, Debug)]
pub struct WriteOptions {
    pub max_buffer_bytes: usize,
    pub max_rows_per_file: usize,
}
impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 64 * 1024 * 1024,
            max_rows_per_file: 1_048_576,
        }
    }
}

pub struct WriteGlobals {
    pub logical_type: Value,
    pub batch: RecordBatch,
}

pub async fn write_local_stream(
    path: &str,
    overwrite: bool,
    schema: SchemaRef,
    input: futures::stream::BoxStream<'_, Result<PartitionEvent>>,
    row_type: Value,
    keys: Vec<String>,
    options: WriteOptions,
) -> Result<()> {
    write_local_stream_with_globals(
        path, overwrite, schema, input, row_type, keys, options, None,
    )
    .await
}

pub async fn write_local_stream_with_globals(
    path: &str,
    overwrite: bool,
    schema: SchemaRef,
    mut input: futures::stream::BoxStream<'_, Result<PartitionEvent>>,
    row_type: Value,
    keys: Vec<String>,
    options: WriteOptions,
    globals: Option<WriteGlobals>,
) -> Result<()> {
    if options.max_buffer_bytes == 0 || options.max_rows_per_file == 0 {
        return Err(StorageError::Invalid(
            "write buffer limits must be positive".into(),
        ));
    }
    let root = directory(path)?;
    let empty_row = schema.fields().is_empty();
    let physical = if empty_row {
        Arc::new(Schema::new(vec![Field::new(
            "__vail_empty_row",
            DataType::Int32,
            false,
        )]))
    } else {
        schema
    };
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&physical)?;
    let canonical = Arc::new(schema_to_arrow_schema(&iceberg_schema)?);
    let mut key_fields = Vec::new();
    for name in keys {
        let index = canonical.index_of(&name)?;
        let data_type = match canonical.field(index).data_type() {
            DataType::Int32 => KeyType::Int32,
            DataType::Int64 => KeyType::Int64,
            DataType::Boolean => KeyType::Bool,
            DataType::Utf8 => KeyType::AsciiString,
            other => {
                return Err(StorageError::Unsupported(format!(
                    "ordered storage key type {other}"
                )));
            }
        };
        key_fields.push(KeyField {
            field_id: iceberg_schema.as_struct().fields()[index].id,
            data_type,
            descending: false,
            nulls_last: true,
        });
    }
    let existed = root.exists();
    if existed && !overwrite {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "table output already exists",
        )
        .into());
    }
    if existed {
        let entry: Entry = serde_json::from_slice(&fs::read(root.join(ENTRY))?)?;
        if entry.version != 1 {
            return Err(StorageError::Unsupported("local table version".into()));
        }
    }
    fs::create_dir_all(&root)?;
    let lock_path = root.join(".write.lock");
    let _file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)?;
    struct Lock(PathBuf);
    impl Drop for Lock {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }
    let lock = Lock(lock_path);
    // A competing initial writer may have published while we acquired the lock.
    if !overwrite && root.join(ENTRY).exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "table output already exists",
        )
        .into());
    }
    let generation = root.join("generations").join(Uuid::now_v7().to_string());
    let result = async {
        let location = file_uri(&generation)?;
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "local",
                HashMap::from([("warehouse".into(), location.clone())]),
            )
            .await?;
        let namespace = NamespaceIdent::new("vail".into());
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("rows".into())
                    .location(location)
                    .schema(iceberg_schema)
                    .build(),
            )
            .await?;
        let mut partition_rows = Vec::<usize>::new();
        let counts = &mut partition_rows;
        // The consumer closes each file before polling this producer again.
        // Partition boundaries flush the buffer without dropping empty partitions.
        let files = async_stream::try_stream! {
            let mut buffer = Vec::new();
            let (mut buffered_bytes, mut buffered_rows) = (0usize, 0usize);
            while let Some(event) = input.try_next().await? {
                match event {
                    PartitionEvent::StartPartition => {
                        if !buffer.is_empty() { yield std::mem::take(&mut buffer); }
                        buffered_bytes = 0;
                        buffered_rows = 0;
                        counts.push(0);
                    }
                    PartitionEvent::Batch(batch) => {
                        let count = counts.last_mut().ok_or_else(||
                            StorageError::Invalid("batch outside partition".into()))?;
                        *count = count.checked_add(batch.num_rows()).ok_or_else(||
                            StorageError::Invalid("partition row count overflow".into()))?;
                        let mut offset = 0;
                        while offset < batch.num_rows() {
                            let length = (batch.num_rows() - offset).min(options.max_rows_per_file - buffered_rows);
                            let piece = batch.slice(offset, length);
                            let piece = if empty_row {
                                RecordBatch::try_new(canonical.clone(), vec![Arc::new(Int32Array::from(vec![0; length]))])?
                            } else { normalize(&piece, canonical.clone())? };
                            let bytes = piece.get_array_memory_size();
                            if !buffer.is_empty() && buffered_bytes.saturating_add(bytes) > options.max_buffer_bytes {
                                yield std::mem::take(&mut buffer);
                                buffered_bytes = 0;
                                buffered_rows = 0;
                            }
                            buffer.push(piece);
                            buffered_bytes = buffered_bytes.saturating_add(bytes);
                            buffered_rows += length;
                            offset += length;
                            if buffered_bytes >= options.max_buffer_bytes || buffered_rows >= options.max_rows_per_file {
                                yield std::mem::take(&mut buffer);
                                buffered_bytes = 0;
                                buffered_rows = 0;
                            }
                        }
                    }
                }
            }
            if !buffer.is_empty() { yield buffer; }
        }.boxed();
        let (global_type, globals) = match globals {
            Some(globals) => (globals.logical_type, super::globals::write(&generation.join("metadata"), &globals.batch)?),
            None => (json!({}), json!({})),
        };
        let semantics = VailSemantics {
            row_type,
            global_type,
            globals,
            key_field_ids: key_fields.iter().map(|k| k.field_id).collect(),
            comparator_version: PRIMITIVE_COMPARATOR_V1.into(),
            reference_genomes: json!({}),
        };
        let ordered = (!key_fields.is_empty()).then_some(key_fields);
        let committed = super::write_initial_file_stream(&catalog, &table, files, semantics, ordered).await?;
        let entry = Entry {
            version: 1,
            metadata: committed
                .metadata_location()
                .ok_or_else(|| StorageError::Invalid("commit has no metadata location".into()))?
                .into(),
            partition_rows,
            empty_row,
        };
        let mut temporary = tempfile::NamedTempFile::new_in(&root)?;
        temporary.write_all(&serde_json::to_vec(&entry)?)?;
        temporary.as_file().sync_all()?;
        temporary.persist(root.join(ENTRY)).map_err(|e| e.error)?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_dir_all(generation);
        drop(lock);
        if !existed {
            let _ = fs::remove_dir(root.join("generations"));
            let _ = fs::remove_dir(&root);
        }
    }
    result
}

/// A metadata-only, snapshot-pinned read source. It contains no row batches.
#[derive(Clone, Debug)]
pub struct LocalRead {
    entry: Entry,
    table: iceberg::table::Table,
    pub semantics: VailSemantics,
    pub keys: Vec<String>,
}

/// Stream boundaries preserve empty logical partitions without buffering their rows.
#[derive(Debug)]
pub enum PartitionEvent {
    StartPartition,
    Batch(RecordBatch),
}

impl LocalRead {
    pub fn partition_rows(&self) -> &[usize] {
        &self.entry.partition_rows
    }
    pub fn metadata_location(&self) -> &str {
        &self.entry.metadata
    }

    /// Reopen data on every action. The immutable metadata location stays pinned
    /// even if another writer replaces the directory entrypoint in the meantime.
    pub fn batches(&self) -> futures::stream::BoxStream<'static, Result<PartitionEvent>> {
        let read = self.clone();
        async_stream::try_stream! {
            let snapshot_id = read.table.metadata().current_snapshot_id()
                .ok_or_else(|| StorageError::Invalid("table has no snapshot".into()))?;
            let (mut batches, _) = super::stream_snapshot(&read.table, snapshot_id, None).await?;
            let mut pending: Option<RecordBatch> = None;
            for mut remaining in read.entry.partition_rows {
                yield PartitionEvent::StartPartition;
                while remaining > 0 {
                    let batch = match pending.take() {
                        Some(batch) => batch,
                        None => batches.try_next().await?.ok_or_else(||
                            StorageError::Invalid("partition row counts differ from snapshot".into()))?,
                    };
                    let count = remaining.min(batch.num_rows());
                    if count < batch.num_rows() { pending = Some(batch.slice(count, batch.num_rows() - count)); }
                    let batch = batch.slice(0, count);
                    let batch = if read.entry.empty_row {
                        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![],
                            &RecordBatchOptions::new().with_row_count(Some(count)))?
                    } else { batch };
                    yield PartitionEvent::Batch(batch);
                    remaining -= count;
                }
            }
            if pending.is_some() || batches.try_next().await?.is_some() {
                Err(StorageError::Invalid("partition row counts differ from snapshot".into()))?;
            }
        }.boxed()
    }
}

/// Resolve only the directory entrypoint, Iceberg metadata and Vail descriptor.
/// No manifests, fragment data, checksums of data, or row decoding are read here.
pub async fn open_local(path: &str) -> Result<LocalRead> {
    let root = directory(path)?;
    let entry: Entry = serde_json::from_slice(&fs::read(root.join(ENTRY))?)?;
    if entry.version != 1 {
        return Err(StorageError::Unsupported("local table version".into()));
    }
    let table = StaticTable::from_metadata_file(
        &entry.metadata,
        TableIdent::new(NamespaceIdent::new("vail".into()), "rows".into()),
        FileIO::new_with_fs(),
    )
    .await?
    .into_table();
    let descriptor = super::snapshot_descriptor(
        &table,
        table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| StorageError::Invalid("table has no snapshot".into()))?,
    )?;
    let keys = descriptor
        .semantics
        .key_field_ids
        .iter()
        .map(|id| {
            table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .find(|f| f.id == *id)
                .map(|f| f.name.clone())
                .ok_or_else(|| StorageError::Invalid("unknown key field ID".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    let expected = entry
        .partition_rows
        .iter()
        .try_fold(0usize, |n, count| n.checked_add(*count))
        .ok_or_else(|| StorageError::Invalid("partition row count overflow".into()))?;
    let file_rows = descriptor
        .files
        .iter()
        .try_fold(0u64, |n, file| n.checked_add(file.row_count))
        .ok_or_else(|| StorageError::Invalid("file row count overflow".into()))?;
    if expected as u64 != file_rows {
        return Err(StorageError::Invalid(
            "partition row counts differ from snapshot".into(),
        ));
    }
    if entry.empty_row && (descriptor.semantics.row_type != json!([]) || !keys.is_empty()) {
        return Err(StorageError::Invalid("invalid empty row encoding".into()));
    }
    Ok(LocalRead {
        entry,
        table,
        semantics: descriptor.semantics,
        keys,
    })
}

/// Eager convenience API for Rust callers; lazy TableIR uses open_local instead.
pub async fn read_local(path: &str) -> Result<LocalTable> {
    let read = open_local(path).await?;
    let mut batches = read.batches();
    let mut partitions = Vec::new();
    while let Some(event) = batches.try_next().await? {
        match event {
            PartitionEvent::StartPartition => partitions.push(Vec::new()),
            PartitionEvent::Batch(batch) => partitions
                .last_mut()
                .ok_or_else(|| StorageError::Invalid("batch without partition".into()))?
                .push(batch),
        }
    }
    Ok(LocalTable {
        partitions,
        semantics: read.semantics,
        keys: read.keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn certification_failure_preserves_published_snapshot_and_cleans_new_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("table");
        let path_str = path.to_str().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let batch = |values| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
        };
        let row_type = json!([["k", "int32"]]);
        let write = |values, overwrite| {
            write_local(
                path_str,
                overwrite,
                schema.clone(),
                vec![vec![batch(values)]],
                row_type.clone(),
                vec!["k".into()],
            )
        };
        assert!(write(vec![2, 1], false).await.is_err());
        assert!(!path.exists());
        write(vec![1, 2], false).await.unwrap();
        let before = fs::read(path.join(ENTRY)).unwrap();
        assert!(write(vec![2, 1], true).await.is_err());
        assert_eq!(fs::read(path.join(ENTRY)).unwrap(), before);
        assert!(!path.join(".write.lock").exists());
        assert_eq!(
            read_local(path_str).await.unwrap().partitions[0][0].num_rows(),
            2
        );
    }
}
