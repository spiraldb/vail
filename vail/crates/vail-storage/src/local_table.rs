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
        let data_type = if vail_ordering::supports(canonical.field(index).data_type()) {
            let logical_type = row_type
                .as_array()
                .and_then(|fields| {
                    fields
                        .iter()
                        .find(|field| field[0] == name)
                        .map(|field| field[1].clone())
                })
                .ok_or_else(|| {
                    StorageError::Invalid(format!("logical type for key {name:?} is missing"))
                })?;
            KeyType::Canonical(logical_type)
        } else {
            return Err(StorageError::Unsupported(format!(
                "ordered storage key type {}",
                canonical.field(index).data_type()
            )));
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
        let reference_genomes = collect_reference_genomes(&row_type, &global_type)?;
        let semantics = VailSemantics {
            row_type,
            global_type,
            globals,
            key_field_ids: key_fields.iter().map(|k| k.field_id).collect(),
            comparator_version: super::fragment_index::TOTAL_ORDER_COMPARATOR_V3.into(),
            reference_genomes,
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

/// Persist definitions needed by row/global types, without loading sequence data.
fn collect_reference_genomes(row_type: &Value, global_type: &Value) -> Result<Value> {
    fn visit(value: &Value, definitions: &mut serde_json::Map<String, Value>) -> Result<()> {
        match value {
            Value::Object(fields) => {
                if let Some(reference) = fields.get("locus") {
                    let name = reference
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|name| !name.is_empty())
                        .ok_or_else(|| {
                            StorageError::Invalid("locus reference requires a name".into())
                        })?;
                    if let Some(existing) = definitions.get(name) {
                        if existing != reference {
                            return Err(StorageError::Invalid(format!(
                                "conflicting reference definitions for {name}"
                            )));
                        }
                    } else {
                        definitions.insert(name.to_owned(), reference.clone());
                    }
                } else {
                    for child in fields.values() {
                        visit(child, definitions)?;
                    }
                }
            }
            Value::Array(elements) => {
                for child in elements {
                    visit(child, definitions)?;
                }
            }
            _ => (),
        }
        Ok(())
    }
    let mut definitions = serde_json::Map::new();
    visit(row_type, &mut definitions)?;
    visit(global_type, &mut definitions)?;
    Ok(Value::Object(definitions))
}

/// A metadata-only, snapshot-pinned read source. It contains no row batches.
#[derive(Clone, Debug)]
pub struct LocalRead {
    entry: Entry,
    table: iceberg::table::Table,
    descriptor: SnapshotDescriptor,
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

    /// The validated immutable snapshot descriptor loaded when this reader was opened.
    pub fn descriptor(&self) -> &SnapshotDescriptor {
        &self.descriptor
    }

    /// Return the certified index binding needed to construct full-key queries.
    /// This loads only the small index metadata file; candidate planning still
    /// runs through the snapshot reader to validate all snapshot bindings.
    pub fn index_binding(&self) -> Result<IndexBinding> {
        let path = self
            .descriptor
            .index_path
            .as_deref()
            .ok_or_else(|| StorageError::Invalid("ordered fragment index missing".into()))?;
        let expected_hash = self
            .descriptor
            .index_sha256
            .as_deref()
            .ok_or_else(|| StorageError::Invalid("fragment index checksum missing".into()))?;
        let bytes = fs::read(local_path(path)?)?;
        if sha256_hex(&bytes) != expected_hash {
            return Err(StorageError::Invalid(
                "fragment index checksum mismatch".into(),
            ));
        }
        let stored: FragmentIndex = serde_json::from_slice(&bytes)?;
        let binding = stored.binding().clone();
        if binding.table_uuid != self.descriptor.table_uuid
            || binding.snapshot_id != self.descriptor.snapshot_id
            || binding.schema_id != self.descriptor.schema_id
            || binding.comparator_version != self.descriptor.semantics.comparator_version
            || binding.live_files_sha256 != self.descriptor.live_files_sha256
            || binding
                .key_fields
                .iter()
                .map(|field| field.field_id)
                .collect::<Vec<_>>()
                != self.descriptor.semantics.key_field_ids
        {
            return Err(StorageError::Invalid(
                "fragment index binding mismatch".into(),
            ));
        }
        let rebuilt = FragmentIndex::new(binding.clone(), stored.fragments().to_vec())
            .map_err(|error| StorageError::Invalid(error.to_string()))?;
        if rebuilt.fragments() != stored.fragments()
            || rebuilt.fragments().len() != self.descriptor.files.len()
        {
            return Err(StorageError::Invalid(
                "fragment index order or count mismatch".into(),
            ));
        }
        Ok(binding)
    }

    /// Certified full-key bounds for each original reader partition. The
    /// descriptor records partition row counts while the fragment index records
    /// inclusive file key bounds; intersecting those two ordinal layouts gives
    /// conservative bounds without opening a data file.
    pub fn partition_key_bounds(
        &self,
    ) -> Result<
        Vec<
            Option<(
                super::fragment_index::CompositeKey,
                super::fragment_index::CompositeKey,
            )>,
        >,
    > {
        use super::fragment_index::{FragmentIndex, QueryInterval};
        use std::collections::HashMap;

        let binding = self.index_binding()?;
        let path = self
            .descriptor
            .index_path
            .as_deref()
            .ok_or_else(|| StorageError::Invalid("ordered fragment index missing".into()))?;
        let expected_hash = self
            .descriptor
            .index_sha256
            .as_deref()
            .ok_or_else(|| StorageError::Invalid("fragment index checksum missing".into()))?;
        let bytes = fs::read(local_path(path)?)?;
        if sha256_hex(&bytes) != expected_hash {
            return Err(StorageError::Invalid(
                "fragment index checksum mismatch".into(),
            ));
        }
        let stored: FragmentIndex = serde_json::from_slice(&bytes)?;
        if stored.binding() != &binding {
            return Err(StorageError::Invalid(
                "fragment index binding mismatch".into(),
            ));
        }
        let all = stored
            .select(&binding, &QueryInterval::default())
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        if all.len() != self.descriptor.files.len() {
            return Err(StorageError::Invalid(
                "index and descriptor file counts differ".into(),
            ));
        }
        let by_path = all
            .into_iter()
            .map(|fragment| (fragment.identity.file_path.as_str(), fragment))
            .collect::<HashMap<_, _>>();
        let mut file_ranges = Vec::with_capacity(self.descriptor.files.len());
        let mut offset = 0u64;
        for file in &self.descriptor.files {
            let end = offset
                .checked_add(file.row_count)
                .ok_or_else(|| StorageError::Invalid("file row offset overflow".into()))?;
            let fragment = by_path.get(file.path.as_str()).ok_or_else(|| {
                StorageError::Invalid("descriptor file is absent from fragment index".into())
            })?;
            if fragment.row_count != file.row_count
                || fragment.identity.content_sha256 != file.sha256
            {
                return Err(StorageError::Invalid(
                    "fragment identity differs from descriptor".into(),
                ));
            }
            file_ranges.push((
                offset,
                end,
                fragment.first_key.clone(),
                fragment.last_key.clone(),
            ));
            offset = end;
        }
        let total = self
            .entry
            .partition_rows
            .iter()
            .try_fold(0u64, |sum, rows| {
                sum.checked_add(*rows as u64)
                    .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))
            })?;
        if total != offset {
            return Err(StorageError::Invalid(
                "partition and file row counts differ".into(),
            ));
        }
        let mut result = Vec::with_capacity(self.entry.partition_rows.len());
        let mut start = 0u64;
        for &rows in &self.entry.partition_rows {
            let end = start + rows as u64;
            let mut bounds: Option<(
                super::fragment_index::CompositeKey,
                super::fragment_index::CompositeKey,
            )> = None;
            for (file_start, file_end, first, last) in &file_ranges {
                if start < end && *file_start < end && start < *file_end {
                    bounds = Some(match bounds {
                        None => (first.clone(), last.clone()),
                        Some((old_first, old_last)) => (
                            if super::fragment_index::compare_keys(&binding, first, &old_first)
                                .is_lt()
                            {
                                first.clone()
                            } else {
                                old_first
                            },
                            if super::fragment_index::compare_keys(&binding, last, &old_last)
                                .is_gt()
                            {
                                last.clone()
                            } else {
                                old_last
                            },
                        ),
                    });
                }
            }
            result.push(bounds);
            start = end;
        }
        Ok(result)
    }

    /// Return source partition IDs whose physical row ranges overlap any
    /// indexed candidate file for these queries. Selection is metadata-only;
    /// row-level interval filtering remains the caller's responsibility.
    pub async fn candidate_partitions(&self, queries: &[QueryInterval]) -> Result<Vec<usize>> {
        let snapshot_id = self
            .table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| StorageError::Invalid("table has no snapshot".into()))?;
        let (file_ranges, _) =
            super::snapshot_interval_row_ranges(&self.table, snapshot_id, queries).await?;
        let mut partition_start = 0u64;
        let mut candidates = Vec::new();
        for (partition, row_count) in self.entry.partition_rows.iter().enumerate() {
            let partition_end = partition_start
                .checked_add(*row_count as u64)
                .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))?;
            if file_ranges
                .iter()
                .any(|file| file.start < partition_end && partition_start < file.end)
            {
                candidates.push(partition);
            }
            partition_start = partition_end;
        }
        Ok(candidates)
    }

    /// Read one logical partition by its original row ordinal range. Only
    /// physical files intersecting this partition are opened, so requesting a
    /// later partition does not scan or decode earlier partition files.
    pub fn partition_batches(
        &self,
        partition: usize,
    ) -> futures::stream::BoxStream<'static, Result<PartitionEvent>> {
        let read = self.clone();
        async_stream::try_stream! {
            let count = *read.entry.partition_rows.get(partition).ok_or_else(||
                StorageError::Invalid(format!("partition index {partition} is out of range")))?;
            let start = read.entry.partition_rows[..partition].iter().try_fold(0u64, |sum, rows| {
                sum.checked_add(*rows as u64)
                    .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))
            })?;
            let end = start.checked_add(count as u64)
                .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))?;
            let snapshot_id = read.table.metadata().current_snapshot_id()
                .ok_or_else(|| StorageError::Invalid("table has no snapshot".into()))?;
            let (mut batches, _) = super::stream_snapshot_rows(&read.table, snapshot_id, start..end).await?;
            yield PartitionEvent::StartPartition;
            let mut emitted = 0u64;
            while let Some((offset, batch)) = batches.try_next().await? {
                let batch_end = offset.checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| StorageError::Invalid("batch row offset overflow".into()))?;
                let overlap_start = offset.max(start);
                let overlap_end = batch_end.min(end);
                if overlap_start < overlap_end {
                    let local_start = (overlap_start - offset) as usize;
                    let length = (overlap_end - overlap_start) as usize;
                    let batch = batch.slice(local_start, length);
                    let batch = if read.entry.empty_row {
                        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![],
                            &RecordBatchOptions::new().with_row_count(Some(length)))?
                    } else { batch };
                    emitted += length as u64;
                    yield PartitionEvent::Batch(batch);
                }
            }
            if emitted != count as u64 {
                Err(StorageError::Invalid("partition row counts differ from snapshot".into()))?;
            }
        }.boxed()
    }

    /// Stream candidate files for key intervals. File selection is indexed;
    /// batches remain unfiltered and carry their original snapshot row offsets.
    pub fn interval_batches(
        &self,
        queries: &[QueryInterval],
    ) -> futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>> {
        self.interval_batches_impl(queries, None)
    }

    /// Prune by both key intervals and the requested execution partition's
    /// original row range. No unrelated candidate file is opened.
    pub fn interval_batches_in_rows(
        &self,
        queries: &[QueryInterval],
        rows: std::ops::Range<u64>,
    ) -> futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>> {
        self.interval_batches_impl(queries, Some(rows))
    }

    fn interval_batches_impl(
        &self,
        queries: &[QueryInterval],
        rows: Option<std::ops::Range<u64>>,
    ) -> futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>> {
        let read = self.clone();
        let queries = queries.to_vec();
        async_stream::try_stream! {
            let snapshot_id = read.table.metadata().current_snapshot_id()
                .ok_or_else(|| StorageError::Invalid("table has no snapshot".into()))?;
            let (mut batches, _) = match rows {
                Some(rows) => super::stream_snapshot_intervals_rows(&read.table, snapshot_id, &queries, rows).await?,
                None => super::stream_snapshot_intervals(&read.table, snapshot_id, &queries).await?,
            };
            while let Some((offset, batch)) = batches.try_next().await? {
                let batch = if read.entry.empty_row {
                    RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![],
                        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())))?
                } else { batch };
                yield (offset, batch);
            }
        }.boxed()
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
    let references = collect_reference_genomes(
        &descriptor.semantics.row_type,
        &descriptor.semantics.global_type,
    )?;
    for (name, definition) in references.as_object().expect("reference map") {
        if descriptor.semantics.reference_genomes.get(name) != Some(definition) {
            return Err(StorageError::Invalid(format!(
                "missing or inconsistent stored reference definition for {name}"
            )));
        }
    }
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
        semantics: descriptor.semantics.clone(),
        keys,
        descriptor,
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
    use futures::TryStreamExt;

    #[test]
    fn reference_definitions_are_collected_from_nested_rows_and_globals() {
        let reference = json!({"name": "test", "contigs": ["chr2"], "lengths": {"chr2": 20},
            "x_contigs": [], "y_contigs": [], "mt_contigs": [], "par": []});
        let locus = json!({"locus": reference});
        let rows = json!([["regions", {"array": {"interval": locus}}]]);
        let globals = json!([["anchor", locus]]);
        assert_eq!(
            collect_reference_genomes(&rows, &globals).unwrap(),
            json!({"test": reference})
        );

        let mut conflicting = reference.clone();
        conflicting["lengths"]["chr2"] = json!(21);
        let incompatible_globals = json!([["anchor", {"locus": conflicting}]]);
        assert!(collect_reference_genomes(&rows, &incompatible_globals).is_err());
    }

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

    #[tokio::test]
    async fn ordinal_partition_reads_open_only_overlapping_duplicate_key_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("partition-read");
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = |values: Vec<i32>| {
            let keys = vec![1; values.len()];
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(keys)),
                    Arc::new(Int32Array::from(values)),
                ],
            )
            .unwrap()
        };
        let input = futures::stream::iter(vec![
            Ok(PartitionEvent::StartPartition),
            Ok(PartitionEvent::Batch(batch(vec![0, 1, 2, 3]))),
            Ok(PartitionEvent::StartPartition),
            Ok(PartitionEvent::Batch(batch(vec![4, 5, 6, 7]))),
            Ok(PartitionEvent::StartPartition),
            Ok(PartitionEvent::Batch(batch(vec![8, 9, 10, 11]))),
        ])
        .boxed();
        write_local_stream(
            root.to_str().unwrap(),
            false,
            schema,
            input,
            json!([["k", "int32"], ["v", "int32"]]),
            vec!["k".into()],
            WriteOptions {
                max_buffer_bytes: usize::MAX,
                max_rows_per_file: 2,
            },
        )
        .await
        .unwrap();

        let read = open_local(root.to_str().unwrap()).await.unwrap();
        let snapshot_id = read.table.metadata().current_snapshot_id().unwrap();
        let descriptor = crate::snapshot_descriptor(&read.table, snapshot_id).unwrap();
        assert_eq!(
            descriptor.files.len(),
            6,
            "each 4-row partition spans two files"
        );

        // A later file is absent, but reading partition zero only opens files
        // whose descriptor row offsets overlap [0, 4).
        let later = crate::local_path(&descriptor.files[5].path).unwrap();
        assert!(later.exists(), "descriptor path {:?} does not exist", later);
        fs::remove_file(later).unwrap();
        let key = Int32Array::from(vec![1]);
        let point = crate::fragment_index::Endpoint {
            key: vec![crate::fragment_index::KeyValue::Canonical(
                vail_ordering::encode_typed_value(&key, 0, Some(&json!("int32"))).unwrap(),
            )],
            inclusive: true,
        };
        let query = QueryInterval {
            lower: Some(point.clone()),
            upper: Some(point),
        };
        assert_eq!(
            read.candidate_partitions(std::slice::from_ref(&query))
                .await
                .unwrap(),
            vec![0, 1, 2]
        );
        let mut first = read.partition_batches(0);
        let mut first_values = Vec::new();
        while let Some(event) = first.try_next().await.unwrap() {
            if let PartitionEvent::Batch(batch) = event {
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                first_values.extend((0..values.len()).map(|i| values.value(i)));
            }
        }
        assert_eq!(first_values, vec![0, 1, 2, 3]);
        let indexed_first = read
            .interval_batches_in_rows(std::slice::from_ref(&query), 0..4)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            indexed_first
                .iter()
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            vec![0, 2],
        );

        // The middle partition remains readable after an earlier file is
        // removed. Its duplicated keys do not affect ordinal selection.
        let earlier = crate::local_path(&descriptor.files[0].path).unwrap();
        fs::remove_file(earlier).unwrap();
        let mut second = read.partition_batches(1);
        let mut second_values = Vec::new();
        while let Some(event) = second
            .try_next()
            .await
            .unwrap_or_else(|error| panic!("second partition failed: {error}"))
        {
            if let PartitionEvent::Batch(batch) = event {
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                second_values.extend((0..values.len()).map(|i| values.value(i)));
            }
        }
        assert_eq!(second_values, vec![4, 5, 6, 7]);
        let indexed_middle = read
            .interval_batches_in_rows(std::slice::from_ref(&query), 4..8)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            indexed_middle
                .iter()
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            vec![4, 6],
        );
        // Boundary files are retained even when the row range cuts through
        // them; the caller slices the returned batches using these offsets.
        let partial = read
            .interval_batches_in_rows(std::slice::from_ref(&query), 5..7)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            partial
                .iter()
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            vec![4, 6]
        );
        // Empty row ranges must not open a file, including an absent file
        // containing that ordinal.
        assert!(
            read.interval_batches_in_rows(std::slice::from_ref(&query), 11..11)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn ordinal_partition_read_preserves_empty_partition_events() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("empty-partitions");
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![5, 6]))])
                .unwrap();
        let input = futures::stream::iter(vec![
            Ok(PartitionEvent::StartPartition),
            Ok(PartitionEvent::StartPartition),
            Ok(PartitionEvent::Batch(batch)),
            Ok(PartitionEvent::StartPartition),
        ])
        .boxed();
        write_local_stream(
            root.to_str().unwrap(),
            false,
            schema,
            input,
            json!([["x", "int32"]]),
            vec![],
            WriteOptions::default(),
        )
        .await
        .unwrap();

        let read = open_local(root.to_str().unwrap()).await.unwrap();
        assert_eq!(read.partition_rows(), &[0, 2, 0]);
        for partition in [0, 2] {
            let events = read
                .partition_batches(partition)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert!(matches!(events[0], PartitionEvent::StartPartition));
        }
        assert!(
            read.partition_batches(3)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
    }
}
