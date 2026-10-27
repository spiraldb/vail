//! Local directory entrypoint. Immutable generations make publication atomic and
//! keep readers of the previous snapshot valid across an overwrite.
use super::*;
use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::FutureExt;
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

fn start_partition_count(counts: &std::sync::Mutex<Vec<usize>>) {
    counts.lock().unwrap().push(0);
}

fn add_partition_count(
    counts: &std::sync::Mutex<Vec<usize>>,
    partition: usize,
    rows: usize,
) -> Result<()> {
    let mut counts = counts.lock().unwrap();
    let count = counts
        .get_mut(partition)
        .ok_or_else(|| StorageError::Invalid("batch outside partition".into()))?;
    *count = count
        .checked_add(rows)
        .ok_or_else(|| StorageError::Invalid("partition row count overflow".into()))?;
    Ok(())
}

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
    input: futures::stream::BoxStream<'_, Result<PartitionEvent>>,
    row_type: Value,
    keys: Vec<String>,
    options: WriteOptions,
    globals: Option<WriteGlobals>,
) -> Result<()> {
    write_local_source_with_globals(
        path,
        overwrite,
        schema,
        WriteSource::Events(input),
        row_type,
        keys,
        options,
        globals,
    )
    .await
}

/// Write independently executable table partitions concurrently. At most 32
/// partitions are polled at once and each holds only one configured file
/// buffer; files are committed to the snapshot in partition/file order.
pub async fn write_local_partition_streams_with_globals(
    path: &str,
    overwrite: bool,
    schema: SchemaRef,
    partitions: Vec<futures::stream::BoxStream<'static, Result<RecordBatch>>>,
    row_type: Value,
    keys: Vec<String>,
    options: WriteOptions,
    globals: Option<WriteGlobals>,
) -> Result<()> {
    write_local_source_with_globals(
        path,
        overwrite,
        schema,
        WriteSource::Partitions(partitions),
        row_type,
        keys,
        options,
        globals,
    )
    .await
}

enum WriteSource<'a> {
    Events(futures::stream::BoxStream<'a, Result<PartitionEvent>>),
    Partitions(Vec<futures::stream::BoxStream<'static, Result<RecordBatch>>>),
}

async fn write_local_source_with_globals(
    path: &str,
    overwrite: bool,
    schema: SchemaRef,
    source: WriteSource<'_>,
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
    if let Some(globals) = &globals {
        let expected = super::globals::schema_from_wire(&globals.logical_type)?;
        super::globals::validate_agreement(globals.batch.schema_ref(), &expected)?;
        if globals.batch.num_rows() != 1 {
            return Err(StorageError::Invalid(
                "globals must contain exactly one row".into(),
            ));
        }
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
        let physical_type = canonical.field(index).data_type();
        if !super::fragment_index::is_certifiable_key_type(&logical_type, physical_type) {
            return Err(StorageError::Unsupported(format!(
                "ordered storage key type {logical_type} with Arrow type {physical_type}"
            )));
        }
        key_fields.push(KeyField {
            field_id: iceberg_schema.as_struct().fields()[index].id,
            data_type: KeyType::Arrow(logical_type),
            physical_type: physical_type.clone(),
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
        if entry.version != 2 {
            return Err(StorageError::Unsupported(
                "local table version 1 used byte-encoded ordered keys; rewrite the table with the Arrow-typed index format".into(),
            ));
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
        let (global_type, globals) = match globals {
            Some(globals) => (
                globals.logical_type,
                super::globals::write(&generation.join("metadata"), &globals.batch)?,
            ),
            None => (json!({}), json!({})),
        };
        let reference_genomes = collect_reference_genomes(&row_type, &global_type)?;
        let semantics = VailSemantics {
            row_type,
            global_type,
            globals,
            key_field_ids: key_fields.iter().map(|k| k.field_id).collect(),
            comparator_version: super::fragment_index::ARROW_TYPED_COMPARATOR_V4.into(),
            reference_genomes,
        };
        let ordered = (!key_fields.is_empty()).then_some(key_fields);
        let (counts, files, mut partition_tasks) = match source {
            WriteSource::Events(mut input) => {
                let counts = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
                let recorded = counts.clone();
                let files = async_stream::try_stream! {
                    let mut buffer = Vec::new();
                    let (mut buffered_bytes, mut buffered_rows, mut file_ordinal) = (0usize, 0usize, 0usize);
                    while let Some(event) = input.try_next().await? {
                        match event {
                            PartitionEvent::StartPartition => {
                                if !buffer.is_empty() { yield ((file_ordinal, 0), std::mem::take(&mut buffer)); file_ordinal += 1; }
                                buffered_bytes = 0;
                                buffered_rows = 0;
                                start_partition_count(&recorded);
                            }
                            PartitionEvent::Batch(batch) => {
                                let partition = recorded.lock().unwrap().len().saturating_sub(1);
                                add_partition_count(&recorded, partition, batch.num_rows())?;
                                let mut offset = 0;
                                while offset < batch.num_rows() {
                                    let length = (batch.num_rows() - offset).min(options.max_rows_per_file - buffered_rows);
                                    let piece = batch.slice(offset, length);
                                    let piece = if empty_row {
                                        RecordBatch::try_new(canonical.clone(), vec![Arc::new(Int32Array::from(vec![0; length]))])?
                                    } else { normalize(&piece, canonical.clone())? };
                                    let bytes = piece.get_array_memory_size();
                                    if !buffer.is_empty() && buffered_bytes.saturating_add(bytes) > options.max_buffer_bytes {
                                        yield ((file_ordinal, 0), std::mem::take(&mut buffer));
                                        file_ordinal += 1;
                                        buffered_bytes = 0;
                                        buffered_rows = 0;
                                    }
                                    buffer.push(piece);
                                    buffered_bytes = buffered_bytes.saturating_add(bytes);
                                    buffered_rows += length;
                                    offset += length;
                                    if buffered_bytes >= options.max_buffer_bytes || buffered_rows >= options.max_rows_per_file {
                                        yield ((file_ordinal, 0), std::mem::take(&mut buffer));
                                        file_ordinal += 1;
                                        buffered_bytes = 0;
                                        buffered_rows = 0;
                                    }
                                }
                            }
                        }
                    }
                    if !buffer.is_empty() { yield ((file_ordinal, 0), buffer); }
                }.boxed();
                (counts, files, tokio::task::JoinSet::new())
            }
            WriteSource::Partitions(partitions) => {
                let counts = Arc::new(std::sync::Mutex::new(vec![0usize; partitions.len()]));
                let concurrency = Arc::new(tokio::sync::Semaphore::new(32));
                let mut receivers = Vec::with_capacity(partitions.len());
                let mut tasks = tokio::task::JoinSet::new();
                for (partition, mut input) in partitions.into_iter().enumerate() {
                    let (sender, receiver) = tokio::sync::mpsc::channel(1);
                    receivers.push(receiver);
                    let canonical = canonical.clone();
                    let counts = counts.clone();
                    let concurrency = concurrency.clone();
                    tasks.spawn(async move {
                        let failure_sender = sender.clone();
                        let producer = async move {
                            let Ok(_permit) = concurrency.acquire_owned().await else { return };
                            let chunks = async_stream::try_stream! {
                                let mut buffer = Vec::new();
                                let (mut buffered_bytes, mut buffered_rows, mut file_ordinal) = (0usize, 0usize, 0usize);
                                while let Some(batch) = input.try_next().await? {
                                    add_partition_count(&counts, partition, batch.num_rows())?;
                                    let mut offset = 0;
                                    while offset < batch.num_rows() {
                                        let length = (batch.num_rows() - offset).min(options.max_rows_per_file - buffered_rows);
                                        let piece = batch.slice(offset, length);
                                        let piece = if empty_row {
                                            RecordBatch::try_new(canonical.clone(), vec![Arc::new(Int32Array::from(vec![0; length]))])?
                                        } else { normalize(&piece, canonical.clone())? };
                                        let bytes = piece.get_array_memory_size();
                                        if !buffer.is_empty() && buffered_bytes.saturating_add(bytes) > options.max_buffer_bytes {
                                            yield ((partition, file_ordinal), std::mem::take(&mut buffer));
                                            file_ordinal += 1;
                                            buffered_bytes = 0;
                                            buffered_rows = 0;
                                        }
                                        buffer.push(piece);
                                        buffered_bytes = buffered_bytes.saturating_add(bytes);
                                        buffered_rows += length;
                                        offset += length;
                                        if buffered_bytes >= options.max_buffer_bytes || buffered_rows >= options.max_rows_per_file {
                                            yield ((partition, file_ordinal), std::mem::take(&mut buffer));
                                            file_ordinal += 1;
                                            buffered_bytes = 0;
                                            buffered_rows = 0;
                                        }
                                    }
                                }
                                if !buffer.is_empty() { yield ((partition, file_ordinal), buffer); }
                            }.boxed();
                            futures::pin_mut!(chunks);
                            while let Some(item) = chunks.next().await {
                                let failed = item.is_err();
                                if sender.send(item).await.is_err() || failed {
                                    break;
                                }
                            }
                        };
                        if std::panic::AssertUnwindSafe(producer).catch_unwind().await.is_err() {
                            let _ = failure_sender
                                .send(Err(StorageError::Input("partition producer panicked".into())))
                                .await;
                        }
                    });
                }
                let streams = futures::stream::iter(receivers)
                    .map(|receiver| {
                        futures::stream::unfold(receiver, |mut receiver| async move {
                            receiver.recv().await.map(|item| (item, receiver))
                        })
                        .boxed()
                    })
                    // All receiver streams are polled; the semaphore, not the
                    // receiver merge, bounds active producer workers. Limiting
                    // this merge to 32 would let later producers hold permits
                    // while their full channels block the first 32 receivers.
                    .flatten_unordered(0)
                    .boxed();
                (counts, streams, tasks)
            }
        };
        let committed_result =
            super::write_initial_file_stream(&catalog, &table, files, semantics, ordered).await;
        if committed_result.is_err() {
            partition_tasks.abort_all();
        }
        let mut task_error = None;
        while let Some(result) = partition_tasks.join_next().await {
            if let Err(error) = result {
                task_error.get_or_insert_with(|| {
                    StorageError::Execution(Box::new(error))
                });
                partition_tasks.abort_all();
            }
        }
        let committed = committed_result?;
        if let Some(error) = task_error {
            return Err(error);
        }
        let partition_rows = counts.lock().unwrap().clone();
        let entry = Entry {
            version: 2,
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
    entry: Arc<Entry>,
    descriptor: Arc<SnapshotDescriptor>,
    pub semantics: VailSemantics,
    pub keys: Vec<String>,
    field_ids: Arc<Vec<i32>>,
    plan: std::sync::Arc<super::SnapshotReadPlan>,
    partition_offsets: std::sync::Arc<Vec<u64>>,
    nested_projection_paths: Option<Vec<Vec<String>>>,
    scan_metrics: Option<ScanMetrics>,
    typed_partition_bounds: Arc<
        Vec<
            Option<(
                super::fragment_index::TypedKey,
                super::fragment_index::TypedKey,
            )>,
        >,
    >,
}

/// Stream boundaries preserve empty logical partitions without buffering their rows.
#[derive(Debug)]
pub enum PartitionEvent {
    StartPartition,
    Batch(RecordBatch),
}

impl LocalRead {
    /// Attach an execution-owned live counter without changing shared snapshot metadata.
    pub fn with_scan_metrics(&self, metrics: ScanMetrics) -> Self {
        let mut read = self.clone();
        read.scan_metrics = Some(metrics);
        read
    }

    /// Return a reader clone configured with nested Vortex projection paths.
    /// Paths are full field-name paths (including the top-level field name).
    pub fn with_nested_projection_paths(&self, paths: Vec<Vec<String>>) -> Self {
        let mut read = self.clone();
        read.nested_projection_paths = Some(paths);
        read
    }

    /// Iceberg field IDs in the current row-schema order.
    pub fn field_ids(&self) -> &[i32] {
        self.field_ids.as_slice()
    }

    pub fn partition_rows(&self) -> &[usize] {
        &self.entry.partition_rows
    }
    pub fn metadata_location(&self) -> &str {
        &self.entry.metadata
    }

    /// The validated immutable snapshot descriptor loaded when this reader was opened.
    pub fn descriptor(&self) -> &SnapshotDescriptor {
        self.descriptor.as_ref()
    }

    /// Return the certified index binding needed to construct full-key queries.
    /// The snapshot and index metadata are validated once when this reader opens.
    pub fn index_binding(&self) -> Result<IndexBinding> {
        self.plan
            .index
            .as_ref()
            .map(|index| index.binding().clone())
            .ok_or_else(|| StorageError::Invalid("ordered fragment index missing".into()))
    }

    /// Certified full-key bounds for each file-aligned execution partition. The
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
        use super::fragment_index::encode_typed_key_component;
        self.partition_typed_key_bounds()?
            .into_iter()
            .map(|bounds| {
                bounds
                    .map(|(first, last)| {
                        let encode = |key: super::fragment_index::TypedKey| {
                            key.components
                                .iter()
                                .map(|component| {
                                    encode_typed_key_component(component)
                                        .map(super::fragment_index::KeyValue::ArrowIpc)
                                        .map_err(|error| StorageError::Invalid(error.to_string()))
                                })
                                .collect::<Result<Vec<_>>>()
                        };
                        Ok((encode(first)?, encode(last)?))
                    })
                    .transpose()
            })
            .collect()
    }

    /// Typed Arrow view of each file-aligned source partition's certified key
    /// bounds. Components are one-row arrays in key-field order.
    pub fn partition_typed_key_bounds(
        &self,
    ) -> Result<
        Vec<
            Option<(
                super::fragment_index::TypedKey,
                super::fragment_index::TypedKey,
            )>,
        >,
    > {
        Ok(self.typed_partition_bounds.as_ref().clone())
    }

    /// Return source partition IDs whose physical row ranges overlap any
    /// indexed candidate file for these queries. Selection is metadata-only;
    /// row-level interval filtering remains the caller's responsibility.
    pub async fn candidate_partitions(&self, queries: &[QueryInterval]) -> Result<Vec<usize>> {
        let (_, file_ranges) = super::stream_from_plan(
            self.plan.clone(),
            Some(queries.to_vec()),
            false,
            None,
            Some(Vec::new()),
        )?;
        candidate_partitions_for_ranges(
            &self.entry.partition_rows,
            &self.partition_offsets,
            file_ranges,
        )
    }

    /// Read one file-aligned execution partition by its snapshot row ordinal range. Only
    /// physical files intersecting this partition are opened, so requesting a
    /// later partition does not scan or decode earlier partition files.
    pub fn partition_batches(
        &self,
        partition: usize,
    ) -> futures::stream::BoxStream<'static, Result<PartitionEvent>> {
        self.projected_partition_batches(partition, self.field_ids.as_slice())
    }

    /// Read a source partition with Iceberg/Vortex projection pushdown. Field
    /// IDs are ordered exactly as they should appear in each returned batch.
    pub fn projected_partition_batches(
        &self,
        partition: usize,
        field_ids: &[i32],
    ) -> futures::stream::BoxStream<'static, Result<PartitionEvent>> {
        self.partition_batches_with_computation(partition, field_ids, None)
    }

    /// Evaluate a typed row-preserving projection in the encoded Vortex scan.
    /// Callers retain DataFusion operators above this source for global work.
    /// Prepare a fail-closed, immutable metadata proof for the requested dense
    /// integer-list operands. Positive and negative proofs are cached per snapshot.
    async fn computed_projection_certificate(
        &self,
        mut operands: Vec<super::computed_capability::ComputedOperand>,
    ) -> Result<Option<Arc<super::computed_capability::ComputedProjectionCertificate>>> {
        operands.sort();
        operands.dedup();
        let partitions = self
            .partition_offsets
            .iter()
            .zip(&self.entry.partition_rows)
            .map(|(&start, &count)| {
                start
                    .checked_add(count as u64)
                    .map(|end| start..end)
                    .ok_or_else(|| StorageError::Invalid("source row range overflow".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        let key = (
            operands.clone(),
            partitions.iter().map(|r| (r.start, r.end)).collect(),
        );
        let cell = {
            let mut cache = self.plan.computed_proofs.lock().await;
            if let Some(cell) = cache.get(&key) {
                cell.clone()
            } else if cache.len() < 16 {
                cache
                    .entry(key)
                    .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                    .clone()
            } else {
                Arc::new(tokio::sync::OnceCell::new())
            }
        };
        cell.get_or_try_init(|| {
            super::computed_capability::prepare(self.plan.clone(), partitions, operands)
        })
        .await
        .cloned()
    }

    /// Trusted cross-crate preparation boundary for the closed native translator.
    ///
    /// The caller must have proved that the expression is a row-preserving pack
    /// containing only field passthroughs, Int32 ArrayLength over exactly ListLength
    /// operands, and skip-null widened i32
    /// list-field sums over exactly DenseI32Sum operands, with the existing missing-list
    /// guard/empty-zero semantics, no bindings/globals/effects or other operators,
    /// and the declared exact result schema. This is an execution-planner
    /// interface, not an arbitrary Vortex expression entrypoint. Physical-domain
    /// evidence is checked here before binding the immutable request; execution
    /// cannot substitute an expression or schema after that preparation.
    #[doc(hidden)]
    pub async fn prepare_computed_projection(
        &self,
        operands: Vec<super::computed_capability::ComputedOperand>,
        expression: vortex::expr::Expression,
        output_schema: Arc<Schema>,
    ) -> Result<Option<Arc<super::computed_capability::PreparedComputedProjection>>> {
        Ok(self
            .computed_projection_certificate(operands)
            .await?
            .map(|certificate| {
                Arc::new(super::computed_capability::PreparedComputedProjection {
                    certificate,
                    expression,
                    output_schema,
                })
            }))
    }

    pub fn computed_partition_batches(
        &self,
        partition: usize,
        prepared: &super::computed_capability::PreparedComputedProjection,
    ) -> futures::stream::BoxStream<'static, Result<PartitionEvent>> {
        let certificate = &prepared.certificate;
        let expected = self
            .partition_offsets
            .get(partition)
            .zip(self.entry.partition_rows.get(partition))
            .and_then(|(&start, &count)| start.checked_add(count as u64).map(|end| start..end));
        if !certificate
            .plan
            .upgrade()
            .is_some_and(|plan| Arc::ptr_eq(&plan, &self.plan))
            || expected.as_ref() != certificate.partitions.get(partition)
        {
            return futures::stream::once(async {
                Err(StorageError::Invalid(
                    "computed source certificate does not match immutable source selection".into(),
                ))
            })
            .boxed();
        }
        self.partition_batches_with_computation(
            partition,
            self.field_ids.as_slice(),
            Some((prepared.expression.clone(), prepared.output_schema.clone())),
        )
    }

    fn partition_batches_with_computation(
        &self,
        partition: usize,
        field_ids: &[i32],
        computed: Option<(vortex::expr::Expression, Arc<Schema>)>,
    ) -> futures::stream::BoxStream<'static, Result<PartitionEvent>> {
        let has_computation = computed.is_some();
        let entry = self.entry.clone();
        let plan = self.plan.clone();
        let partition_offsets = self.partition_offsets.clone();
        let nested_projection_paths = self.nested_projection_paths.clone();
        let scan_metrics = self.scan_metrics.clone();
        let field_ids = field_ids.to_vec();
        async_stream::try_stream! {
            let count = *entry.partition_rows.get(partition).ok_or_else(||
                StorageError::Invalid(format!("partition index {partition} is out of range")))?;
            let start = partition_offsets[partition];
            let end = start.checked_add(count as u64)
                .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))?;
            let (mut batches, _) = super::stream_from_plan_with_projection(
                plan, None, false, Some(start..end), Some(field_ids),
                nested_projection_paths,
                scan_metrics,
                computed,
            )?;
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
                    let batch = if entry.empty_row && !has_computation {
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
        }
        .boxed()
    }

    /// Stream candidate files for key intervals. File selection is indexed;
    /// batches remain unfiltered and carry their original snapshot row offsets.
    pub fn interval_batches(
        &self,
        queries: &[QueryInterval],
    ) -> futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>> {
        self.projected_interval_batches(queries, self.field_ids.as_slice())
    }

    /// Prune by both key intervals and the requested execution partition's
    /// original row range. No unrelated candidate file is opened.
    pub fn interval_batches_in_rows(
        &self,
        queries: &[QueryInterval],
        rows: std::ops::Range<u64>,
    ) -> futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>> {
        self.projected_interval_batches_in_rows(queries, rows, self.field_ids.as_slice())
    }

    /// Stream unfiltered indexed candidate rows with a storage-level field
    /// projection. The caller applies the interval predicate to these rows.
    pub fn projected_interval_batches(
        &self,
        queries: &[QueryInterval],
        field_ids: &[i32],
    ) -> futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>> {
        self.projected_interval_batches_impl(queries, None, field_ids)
    }

    /// Stream indexed candidates intersecting an original snapshot row range,
    /// with a storage-level field projection.
    pub fn projected_interval_batches_in_rows(
        &self,
        queries: &[QueryInterval],
        rows: std::ops::Range<u64>,
        field_ids: &[i32],
    ) -> futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>> {
        self.projected_interval_batches_impl(queries, Some(rows), field_ids)
    }

    fn projected_interval_batches_impl(
        &self,
        queries: &[QueryInterval],
        rows: Option<std::ops::Range<u64>>,
        field_ids: &[i32],
    ) -> futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>> {
        let plan = self.plan.clone();
        let nested_projection_paths = self.nested_projection_paths.clone();
        let scan_metrics = self.scan_metrics.clone();
        let queries = queries.to_vec();
        let field_ids = field_ids.to_vec();
        async_stream::try_stream! {
            let (mut batches, _) = super::stream_from_plan_with_nested_projection(
                plan, Some(queries), false, rows, Some(field_ids),
                nested_projection_paths,
                scan_metrics,
            )?;
            while let Some((offset, batch)) = batches.try_next().await? {
                yield (offset, batch);
            }
        }
        .boxed()
    }

    /// Reopen data on every action. The immutable metadata location stays pinned
    /// even if another writer replaces the directory entrypoint in the meantime.
    pub fn batches(&self) -> futures::stream::BoxStream<'static, Result<PartitionEvent>> {
        let entry = self.entry.clone();
        let plan = self.plan.clone();
        let field_ids = self.field_ids.as_ref().clone();
        let nested_projection_paths = self.nested_projection_paths.clone();
        let scan_metrics = self.scan_metrics.clone();
        async_stream::try_stream! {
            let (mut batches, _) = super::stream_from_plan_with_nested_projection(
                plan, None, false, None, Some(field_ids),
                nested_projection_paths,
                scan_metrics,
            )?;
            let mut pending: Option<RecordBatch> = None;
            for mut remaining in entry.partition_rows.iter().copied() {
                yield PartitionEvent::StartPartition;
                while remaining > 0 {
                    let batch = match pending.take() {
                        Some(batch) => batch,
                        None => batches.try_next().await?.map(|(_, batch)| batch).ok_or_else(||
                            StorageError::Invalid("partition row counts differ from snapshot".into()))?,
                    };
                    let count = remaining.min(batch.num_rows());
                    if count < batch.num_rows() { pending = Some(batch.slice(count, batch.num_rows() - count)); }
                    let batch = batch.slice(0, count);
                    let batch = if entry.empty_row {
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

fn candidate_partitions_for_ranges(
    partition_rows: &[usize],
    partition_offsets: &[u64],
    mut file_ranges: Vec<Range<u64>>,
) -> Result<Vec<usize>> {
    if partition_rows.len() != partition_offsets.len() {
        return Err(StorageError::Invalid(
            "partition offsets and row counts differ".into(),
        ));
    }
    file_ranges.sort_unstable_by_key(|range| range.start);
    let mut merged_ranges: Vec<Range<u64>> = Vec::with_capacity(file_ranges.len());
    for range in file_ranges {
        if range.start >= range.end {
            continue;
        }
        if let Some(previous) = merged_ranges.last_mut()
            && range.start <= previous.end
        {
            previous.end = previous.end.max(range.end);
        } else {
            merged_ranges.push(range);
        }
    }

    let mut candidates = Vec::new();
    let mut next_partition = 0;
    for range in merged_ranges {
        let first_starting_at_or_before = partition_offsets
            .partition_point(|offset| *offset <= range.start)
            .saturating_sub(1);
        let mut partition = next_partition.max(first_starting_at_or_before);
        while partition < partition_rows.len() {
            let start = partition_offsets[partition];
            if start >= range.end {
                break;
            }
            let end = start
                .checked_add(partition_rows[partition] as u64)
                .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))?;
            if start < end && end > range.start {
                candidates.push(partition);
            }
            partition += 1;
            next_partition = partition;
        }
    }
    Ok(candidates)
}

fn partition_bounds_from_plan(
    plan: &super::SnapshotReadPlan,
    partition_rows: &[usize],
) -> Result<
    Vec<
        Option<(
            super::fragment_index::TypedKey,
            super::fragment_index::TypedKey,
        )>,
    >,
> {
    use super::fragment_index::TypedKey;
    let Some(index) = plan.index.as_ref() else {
        return Ok(vec![None; partition_rows.len()]);
    };
    let typed_by_path = index
        .fragments()
        .iter()
        .zip(index.typed_fragment_bounds())
        .map(|(fragment, bounds)| (fragment.identity.file_path.as_str(), bounds))
        .collect::<HashMap<_, _>>();
    let mut file_ranges = Vec::with_capacity(plan.files.len());
    for file in &plan.files {
        let (first, last) = typed_by_path
            .get(file.identity.path.as_str())
            .ok_or_else(|| {
                StorageError::Invalid("descriptor file is absent from fragment index".into())
            })?;
        file_ranges.push((
            file.offset,
            file.offset + file.identity.row_count,
            first,
            last,
        ));
    }
    let partition_total = partition_rows.iter().try_fold(0u64, |sum, rows| {
        sum.checked_add(*rows as u64)
            .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))
    })?;
    if partition_total != plan.total_rows {
        return Err(StorageError::Invalid(
            "partition and file row counts differ".into(),
        ));
    }
    let binding = index.binding();
    let compare = |left: &TypedKey, right: &TypedKey| {
        super::fragment_index::compare_typed_keys(binding, &left.components, &right.components)
            .map_err(|error| StorageError::Invalid(error.to_string()))
    };
    let mut result = Vec::with_capacity(partition_rows.len());
    let mut start = 0u64;
    let mut first_file = 0;
    for &rows in partition_rows {
        let end = start
            .checked_add(rows as u64)
            .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))?;
        let mut bounds: Option<(TypedKey, TypedKey)> = None;
        while first_file < file_ranges.len() && file_ranges[first_file].1 <= start {
            first_file += 1;
        }
        for (file_start, file_end, first, last) in file_ranges[first_file..]
            .iter()
            .take_while(|(file_start, _, _, _)| *file_start < end)
        {
            if start < end && *file_start < end && start < *file_end {
                bounds = Some(match bounds {
                    None => ((*first).clone(), (*last).clone()),
                    Some((old_first, old_last)) => (
                        if compare(first, &old_first)?.is_lt() {
                            (*first).clone()
                        } else {
                            old_first
                        },
                        if compare(last, &old_last)?.is_gt() {
                            (*last).clone()
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

/// Resolve the immutable directory entrypoint and load one validated Iceberg
/// snapshot read plan. Data files remain unopened until a stream is consumed.
pub async fn open_local(path: &str) -> Result<LocalRead> {
    let root = directory(path)?;
    let mut entry: Entry = serde_json::from_slice(&fs::read(root.join(ENTRY))?)?;
    if entry.version != 2 {
        return Err(StorageError::Unsupported(
            "local table version 1 used byte-encoded ordered keys; rewrite the table with the Arrow-typed index format".into(),
        ));
    }
    let table = StaticTable::from_metadata_file(
        &entry.metadata,
        TableIdent::new(NamespaceIdent::new("vail".into()), "rows".into()),
        FileIO::new_with_fs(),
    )
    .await?
    .into_table();
    let snapshot_id = table
        .metadata()
        .current_snapshot_id()
        .ok_or_else(|| StorageError::Invalid("table has no snapshot".into()))?;
    let plan =
        std::sync::Arc::new(super::load_snapshot_read_plan(&table, snapshot_id, true).await?);
    let descriptor = plan.descriptor.clone();
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
    let field_ids = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();
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
    // Writer partitions are not an execution contract. Each ordered physical
    // file becomes an independent task, avoiding both serial multi-file reads
    // and repeated whole-file decoding by overlapping ordinal slices.
    entry.partition_rows = plan
        .files
        .iter()
        .filter(|file| file.identity.row_count != 0)
        .map(|file| {
            usize::try_from(file.identity.row_count)
                .map_err(|_| StorageError::Invalid("file row count exceeds address space".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    if entry.partition_rows.is_empty() {
        entry.partition_rows.push(0);
    }
    let typed_partition_bounds = partition_bounds_from_plan(&plan, &entry.partition_rows)?;
    if entry.empty_row && (descriptor.semantics.row_type != json!([]) || !keys.is_empty()) {
        return Err(StorageError::Invalid("invalid empty row encoding".into()));
    }
    let mut partition_offsets = Vec::with_capacity(entry.partition_rows.len());
    let mut partition_offset = 0u64;
    for rows in &entry.partition_rows {
        partition_offsets.push(partition_offset);
        partition_offset = partition_offset
            .checked_add(*rows as u64)
            .ok_or_else(|| StorageError::Invalid("partition row offset overflow".into()))?;
    }
    Ok(LocalRead {
        entry: Arc::new(entry),
        semantics: descriptor.semantics.clone(),
        keys,
        descriptor: Arc::new(descriptor),
        field_ids: Arc::new(field_ids),
        plan,
        partition_offsets: std::sync::Arc::new(partition_offsets),
        nested_projection_paths: None,
        scan_metrics: None,
        typed_partition_bounds: Arc::new(typed_partition_bounds),
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
    fn candidate_partition_ranges_skip_empty_partitions_and_deduplicate() {
        let rows = [0, 0, 3, 0, 0, 2, 0, 5];
        let offsets = [0, 0, 0, 3, 3, 3, 5, 5];

        assert_eq!(
            candidate_partitions_for_ranges(&rows, &offsets, vec![0..3]).unwrap(),
            vec![2]
        );
        assert_eq!(
            candidate_partitions_for_ranges(&rows, &offsets, vec![3..5]).unwrap(),
            vec![5]
        );
        assert_eq!(
            candidate_partitions_for_ranges(&rows, &offsets, vec![5..10]).unwrap(),
            vec![7]
        );
        assert_eq!(
            candidate_partitions_for_ranges(&rows, &offsets, vec![0..1, 2..3]).unwrap(),
            vec![2]
        );
        assert_eq!(
            candidate_partitions_for_ranges(&rows, &offsets, vec![0..10]).unwrap(),
            vec![2, 5, 7]
        );
    }

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
    async fn file_partitions_split_one_writer_partition_and_prune_unrelated_files() {
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
            Ok(PartitionEvent::Batch(batch(vec![4, 5, 6, 7]))),
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
        assert_eq!(read.field_ids(), &[1, 2]);
        let snapshot_id = read.descriptor().snapshot_id;
        let descriptor = read.descriptor().clone();
        assert!(descriptor.files.iter().all(|file| file.sha256.is_none()));
        assert_eq!(
            descriptor.files.len(),
            6,
            "one writer partition is flushed into six files"
        );
        assert_eq!(read.partition_rows(), &[2, 2, 2, 2, 2, 2]);
        let all = read_local(root.to_str().unwrap()).await.unwrap();
        let values = all
            .partitions
            .iter()
            .flatten()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, (0..12).collect::<Vec<_>>());
        assert_eq!(all.partitions.len(), 6);
        let index_path = crate::local_path(descriptor.index_path.as_deref().unwrap()).unwrap();
        fs::write(&index_path, b"changed after open").unwrap();
        assert_eq!(read.index_binding().unwrap().snapshot_id, snapshot_id);

        // A later file is absent, but execution partition zero opens only
        // its own file, whose snapshot row range is [0, 2).
        let later = crate::local_path(&descriptor.files[5].path).unwrap();
        assert!(later.exists(), "descriptor path {:?} does not exist", later);
        fs::remove_file(later).unwrap();
        let key = Int32Array::from(vec![1]);
        let key = Arc::new(key) as arrow_array::ArrayRef;
        let point = crate::fragment_index::Endpoint {
            key: vec![crate::fragment_index::KeyValue::ArrowIpc(
                crate::fragment_index::encode_typed_key_component(&key).unwrap(),
            )],
            inclusive: true,
        };
        let query = QueryInterval {
            lower: Some(point.clone()),
            upper: Some(point),
        };
        let projected = read
            .projected_partition_batches(0, &[2])
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let projected_batches = projected
            .iter()
            .filter_map(|event| match event {
                PartitionEvent::Batch(batch) => Some(batch),
                PartitionEvent::StartPartition => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(projected_batches.len(), 1);
        assert_eq!(projected_batches[0].schema().field(0).name(), "v");
        assert_eq!(projected_batches[0].num_columns(), 1);
        let projected_interval = read
            .projected_interval_batches_in_rows(std::slice::from_ref(&query), 0..4, &[2])
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(projected_interval.len(), 2);
        assert_eq!(projected_interval[0].1.schema().field(0).name(), "v");
        assert_eq!(projected_interval[0].1.num_columns(), 1);
        assert_eq!(
            read.candidate_partitions(std::slice::from_ref(&query))
                .await
                .unwrap(),
            vec![0, 1, 2, 3, 4, 5]
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
        assert_eq!(first_values, vec![0, 1]);
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
        let mut second = read.partition_batches(2);
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
        assert_eq!(second_values, vec![4, 5]);
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
    async fn empty_snapshot_has_one_empty_execution_partition() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("empty-table");
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let input = futures::stream::iter(vec![
            Ok(PartitionEvent::StartPartition),
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
        assert_eq!(read.partition_rows(), &[0]);
        let events = read
            .partition_batches(0)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], PartitionEvent::StartPartition));
        assert_eq!(
            read_local(root.to_str().unwrap())
                .await
                .unwrap()
                .partitions
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn file_partitions_discard_empty_writer_partitions() {
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
        assert_eq!(read.partition_rows(), &[2]);
        let events = read
            .partition_batches(0)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(matches!(events[0], PartitionEvent::StartPartition));
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    PartitionEvent::Batch(batch) => Some(batch.num_rows()),
                    _ => None,
                })
                .sum::<usize>(),
            2
        );
        assert!(
            read.partition_batches(1)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
    }
}
