//! Vail snapshot semantics around iceberg-rust PR 2835's Vortex integration.
//!
//! This first slice supports local, unpartitioned, initial table writes. It uses
//! Iceberg's real catalog transaction, manifest, and snapshot implementation.
//! Vortex file writing and reading are delegated to the pinned Iceberg PR.

pub mod fragment_index;
pub mod globals;
pub mod local_table;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use fragment_index::{
    CompositeKey, FragmentIdentity, FragmentIndex, FragmentInterval, IndexBinding, KeyField,
    KeyType, KeyValue, PRIMITIVE_COMPARATOR_V1, QueryInterval, compare_keys,
};
use futures::{StreamExt, TryStreamExt};
use iceberg::Catalog;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, VortexWriterBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;

const FORMAT_VERSION: u32 = 1;
const PROP_PATH: &str = "vail.descriptor.path";
const PROP_SHA256: &str = "vail.descriptor.sha256";
const PROP_VERSION: &str = "vail.format.version";
const PROP_INDEX_PATH: &str = "vail.index.path";
const PROP_INDEX_SHA256: &str = "vail.index.sha256";

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("input stream failed: {0}")]
    Input(String),
    #[error("input execution failed: {0}")]
    Execution(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("unsupported storage state: {0}")]
    Unsupported(String),
    #[error("invalid Vail descriptor or snapshot: {0}")]
    Invalid(String),
    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Required Hail-level semantics. The JSON values carry typed Hail metadata,
/// including globals, rather than relying on Arrow's lossy physical type names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VailSemantics {
    pub row_type: Value,
    pub global_type: Value,
    pub globals: Value,
    pub key_field_ids: Vec<i32>,
    pub comparator_version: String,
    pub reference_genomes: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedRun {
    pub file_path: String,
    pub first_key: Value,
    pub last_key: Value,
    pub comparator_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFileIdentity {
    pub path: String,
    pub sha256: String,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDescriptor {
    pub format_version: u32,
    pub table_uuid: Uuid,
    pub snapshot_id: i64,
    pub schema_id: i32,
    pub semantics: VailSemantics,
    pub files: Vec<DataFileIdentity>,
    pub live_files_sha256: String,
    /// Optional proof, never inferred from Iceberg's per-file sort ID.
    pub ordered_runs: Option<Vec<OrderedRun>>,
    /// Explicit arrival order for unkeyed multi-file snapshots, protected by the
    /// descriptor hash. Iceberg manifest enumeration is not an ordering proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_sequence: Option<Vec<String>>,
    #[serde(default)]
    pub index_path: Option<String>,
    #[serde(default)]
    pub index_sha256: Option<String>,
}

/// Commit one immutable Vortex data file and its descriptor as an initial Iceberg snapshot.
/// The table must be unpartitioned and have no previous snapshot. Other write modes
/// need a separate commit coordinator that recomputes the complete live-file set.
pub async fn write_initial_table(
    catalog: &dyn Catalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    semantics: VailSemantics,
) -> Result<Table> {
    write_initial_files(catalog, table, vec![batches], semantics, None).await
}

/// Commit separately certified, ordered Vortex files into one initial snapshot.
/// Each inner vector is one physical file; empty file inputs are ignored.
pub async fn write_initial_ordered_files(
    catalog: &dyn Catalog,
    table: &Table,
    files: Vec<Vec<RecordBatch>>,
    semantics: VailSemantics,
    key_fields: Vec<KeyField>,
) -> Result<Table> {
    if semantics.comparator_version != PRIMITIVE_COMPARATOR_V1
        || key_fields
            .iter()
            .map(|field| field.field_id)
            .collect::<Vec<_>>()
            != semantics.key_field_ids
    {
        return Err(StorageError::Invalid(
            "ordered writer key binding differs from semantics".into(),
        ));
    }
    write_initial_files(catalog, table, files, semantics, Some(key_fields)).await
}

async fn write_initial_files(
    catalog: &dyn Catalog,
    table: &Table,
    files: Vec<Vec<RecordBatch>>,
    semantics: VailSemantics,
    key_fields: Option<Vec<KeyField>>,
) -> Result<Table> {
    write_initial_file_stream(
        catalog,
        table,
        futures::stream::iter(files.into_iter().map(Ok)).boxed(),
        semantics,
        key_fields,
    )
    .await
}

/// Consume and close one bounded file buffer before polling the next. Only file
/// identities, intervals and Iceberg metadata accumulate across the snapshot.
async fn write_initial_file_stream(
    catalog: &dyn Catalog,
    table: &Table,
    mut files: futures::stream::BoxStream<'_, Result<Vec<RecordBatch>>>,
    semantics: VailSemantics,
    key_fields: Option<Vec<KeyField>>,
) -> Result<Table> {
    if let Some(fields) = &key_fields {
        if semantics.comparator_version != PRIMITIVE_COMPARATOR_V1
            || fields.iter().map(|f| f.field_id).collect::<Vec<_>>() != semantics.key_field_ids
        {
            return Err(StorageError::Invalid(
                "ordered writer key binding differs from semantics".into(),
            ));
        }
    }
    if table.metadata().current_snapshot().is_some() {
        return Err(StorageError::Unsupported(
            "initial write requires a table without a snapshot".into(),
        ));
    }
    if !table
        .metadata()
        .default_partition_spec()
        .fields()
        .is_empty()
    {
        return Err(StorageError::Unsupported(
            "partitioned writes are not implemented".into(),
        ));
    }
    if semantics.comparator_version.is_empty()
        || semantics.row_type.is_null()
        || semantics.global_type.is_null()
        || semantics.globals.is_null()
        || semantics.reference_genomes.is_null()
    {
        return Err(StorageError::Invalid(
            "missing required Hail semantics".into(),
        ));
    }
    let mut key_ids = HashSet::new();
    for id in &semantics.key_field_ids {
        if !key_ids.insert(*id)
            || !table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .any(|field| field.id == *id)
        {
            return Err(StorageError::Invalid(format!(
                "duplicate or unknown key field ID {id}"
            )));
        }
    }

    let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())?;

    let snapshot_id = reserve_snapshot_id(table);
    let mut data_files = Vec::new();
    let mut identities = Vec::new();
    let mut intervals = Vec::new();
    let mut binding = key_fields.map(|key_fields| IndexBinding {
        table_uuid: table.metadata().uuid(),
        snapshot_id,
        schema_id: table.metadata().current_schema_id(),
        comparator_version: semantics.comparator_version.clone(),
        live_files_sha256: "0".repeat(64),
        key_fields,
    });
    while let Some(batches) = files.try_next().await? {
        if !batches.iter().any(|b| b.num_rows() > 0) {
            continue;
        }
        for batch in &batches {
            if batch.num_columns() != arrow_schema.fields().len() {
                return Err(StorageError::Invalid(
                    "batch column count differs from table schema".into(),
                ));
            }
            for ((column, input_field), field) in batch
                .columns()
                .iter()
                .zip(batch.schema().fields().iter())
                .zip(arrow_schema.fields())
            {
                if input_field.name() != field.name()
                    || input_field.is_nullable() != field.is_nullable()
                    || column.data_type() != field.data_type()
                {
                    return Err(StorageError::Invalid(format!(
                        "batch field name, requiredness, or type differs from Iceberg field {}",
                        field.name()
                    )));
                }
                if !field.is_nullable() && column.null_count() != 0 {
                    return Err(StorageError::Invalid(format!(
                        "required Iceberg field {} contains nulls",
                        field.name()
                    )));
                }
            }
        }

        let endpoints = if let Some(binding) = &binding {
            let field_ids = table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .map(|field| field.id)
                .collect::<Vec<_>>();
            Some(certify_file(binding, &arrow_schema, &field_ids, &batches)?)
        } else {
            None
        };
        let data_path = local_path(table.metadata().location())?
            .join("data")
            .join(format!(
                "{snapshot_id}-{:020}-{}.vortex",
                identities.len(),
                Uuid::now_v7()
            ));
        std::fs::create_dir_all(
            data_path
                .parent()
                .ok_or_else(|| StorageError::Invalid("no data parent".into()))?,
        )?;
        let file_path = file_uri(&data_path)?;
        let output = table.file_io().new_output(&file_path)?;
        let mut writer = VortexWriterBuilder::new(
            table.metadata().current_schema().clone(),
            VortexSession::default(),
        )
        .build(output)
        .await?;
        for batch in &batches {
            writer.write(batch).await?;
        }
        let writer_files = writer.close().await?;
        let mut writer_files = writer_files.into_iter();
        let data_file = writer_files
            .next()
            .ok_or_else(|| StorageError::Invalid("Vortex writer produced no file".into()))?
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .build()
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        if writer_files.next().is_some() || data_file.file_path() != file_path {
            return Err(StorageError::Invalid(
                "Vortex writer returned unexpected files".into(),
            ));
        }
        identities.push(DataFileIdentity {
            path: file_path.clone(),
            sha256: sha256_file(&data_path)?,
            row_count: data_file.record_count(),
        });
        if let Some((first_key, last_key, certified_rows)) = endpoints {
            let identity = identities.last().unwrap();
            if certified_rows != identity.row_count {
                return Err(StorageError::Invalid(
                    "writer row count differs from order certificate".into(),
                ));
            }
            intervals.push(FragmentInterval {
                identity: FragmentIdentity {
                    file_path,
                    content_sha256: identity.sha256.clone(),
                },
                row_count: certified_rows,
                first_key,
                last_key,
            });
        }
        data_files.push(data_file);
    }
    let live_files_sha256 = live_files_hash(&identities)?;
    let mut index_pointer = None;
    if let Some(mut binding_value) = binding.take() {
        binding_value.live_files_sha256 = live_files_sha256.clone();
        let index = FragmentIndex::new(binding_value, intervals)
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        let bytes = serde_json::to_vec(&index)?;
        let path = local_path(table.metadata().location())?
            .join("metadata")
            .join(format!("vail-index-{snapshot_id}-{}.json", Uuid::now_v7()));
        write_immutable(&path, &bytes)?;
        index_pointer = Some((file_uri(&path)?, sha256_hex(&bytes)));
    }
    let file_sequence = if index_pointer.is_none() && semantics.key_field_ids.is_empty() {
        Some(identities.iter().map(|f| f.path.clone()).collect())
    } else {
        None
    };
    let descriptor = SnapshotDescriptor {
        format_version: FORMAT_VERSION,
        table_uuid: table.metadata().uuid(),
        snapshot_id,
        schema_id: table.metadata().current_schema_id(),
        semantics,
        live_files_sha256,
        files: identities,
        ordered_runs: None,
        file_sequence,
        index_path: index_pointer.as_ref().map(|(path, _)| path.clone()),
        index_sha256: index_pointer.as_ref().map(|(_, hash)| hash.clone()),
    };
    let descriptor_bytes = serde_json::to_vec(&descriptor)?;
    let descriptor_path = local_path(table.metadata().location())?
        .join("metadata")
        .join(format!("vail-{snapshot_id}-{}.json", Uuid::now_v7()));
    write_immutable(&descriptor_path, &descriptor_bytes)?;
    let descriptor_uri = file_uri(&descriptor_path)?;

    let mut props = HashMap::from([
        (PROP_PATH.to_string(), descriptor_uri),
        (PROP_SHA256.to_string(), sha256_hex(&descriptor_bytes)),
        (PROP_VERSION.to_string(), FORMAT_VERSION.to_string()),
    ]);
    if let Some((path, hash)) = index_pointer {
        props.insert(PROP_INDEX_PATH.to_owned(), path);
        props.insert(PROP_INDEX_SHA256.to_owned(), hash);
    }
    let tx = Transaction::new(table);
    let action = tx
        .fast_append()
        .with_snapshot_id(snapshot_id)
        .require_empty_table()
        .set_snapshot_properties(props)
        .add_data_files(data_files);
    let tx = action.apply(tx)?;
    let committed = tx.commit(catalog).await?;
    if committed.metadata().current_snapshot_id() != Some(snapshot_id) {
        return Err(StorageError::Invalid(
            "committed snapshot ID differs from reservation".into(),
        ));
    }
    Ok(committed)
}

/// Read a pinned Iceberg snapshot by planning its actual manifest entries.
/// The descriptor and every data file are checked before rows are returned.
/// This reader follows the certified fragment index and never sorts rows.
pub async fn read_snapshot(
    table: &Table,
    snapshot_id: i64,
) -> Result<(Vec<RecordBatch>, SnapshotDescriptor)> {
    read_snapshot_with_range(table, snapshot_id, None).await
}

/// Read a full-key range from an ordered snapshot. Index selection skips
/// nonintersecting data files; selected rows are checked against the range.
pub async fn read_snapshot_range(
    table: &Table,
    snapshot_id: i64,
    query: &QueryInterval,
) -> Result<(Vec<RecordBatch>, SnapshotDescriptor)> {
    read_snapshot_with_range(table, snapshot_id, Some(query)).await
}

/// Inspect immutable snapshot semantics without opening any data files.
pub fn snapshot_descriptor(table: &Table, snapshot_id: i64) -> Result<SnapshotDescriptor> {
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| StorageError::Invalid(format!("snapshot {snapshot_id} not found")))?;
    let props = &snapshot.summary().additional_properties;
    let descriptor_uri = props
        .get(PROP_PATH)
        .ok_or_else(|| StorageError::Invalid("missing Vail descriptor".into()))?;
    if props.get(PROP_VERSION).map(String::as_str) != Some("1") {
        return Err(StorageError::Invalid(
            "unsupported Vail format version".into(),
        ));
    }
    let expected_hash = props
        .get(PROP_SHA256)
        .ok_or_else(|| StorageError::Invalid("missing descriptor checksum".into()))?;
    let descriptor_bytes = std::fs::read(local_path(descriptor_uri)?)?;
    if &sha256_hex(&descriptor_bytes) != expected_hash {
        return Err(StorageError::Invalid("descriptor checksum mismatch".into()));
    }
    let descriptor: SnapshotDescriptor = serde_json::from_slice(&descriptor_bytes)?;
    if descriptor.format_version != FORMAT_VERSION
        || descriptor.snapshot_id != snapshot_id
        || descriptor.table_uuid != table.metadata().uuid()
        || snapshot.schema_id() != Some(descriptor.schema_id)
        || descriptor.semantics.comparator_version.is_empty()
        || descriptor.semantics.row_type.is_null()
        || descriptor.semantics.global_type.is_null()
        || descriptor.semantics.globals.is_null()
        || descriptor.semantics.reference_genomes.is_null()
    {
        return Err(StorageError::Invalid(
            "descriptor identity or semantics mismatch".into(),
        ));
    }
    if descriptor.ordered_runs.is_some() {
        return Err(StorageError::Unsupported(
            "legacy ordered runs are not supported".into(),
        ));
    }
    if let Some(sequence) = &descriptor.file_sequence {
        if !descriptor.semantics.key_field_ids.is_empty()
            || descriptor.index_path.is_some()
            || sequence
                != &descriptor
                    .files
                    .iter()
                    .map(|f| f.path.clone())
                    .collect::<Vec<_>>()
        {
            return Err(StorageError::Invalid(
                "invalid unkeyed file sequence".into(),
            ));
        }
    }
    Ok(descriptor)
}

async fn read_snapshot_with_range(
    table: &Table,
    snapshot_id: i64,
    query: Option<&QueryInterval>,
) -> Result<(Vec<RecordBatch>, SnapshotDescriptor)> {
    let (stream, descriptor) = stream_snapshot(table, snapshot_id, query).await?;
    Ok((stream.try_collect().await?, descriptor))
}

/// Plan metadata now; open and verify each data file only when the stream pulls it.
/// Dropping the stream (for example after Head) leaves later files unopened.
pub async fn stream_snapshot(
    table: &Table,
    snapshot_id: i64,
    query: Option<&QueryInterval>,
) -> Result<(
    futures::stream::BoxStream<'static, Result<RecordBatch>>,
    SnapshotDescriptor,
)> {
    let descriptor = snapshot_descriptor(table, snapshot_id)?;
    let snapshot = table.metadata().snapshot_by_id(snapshot_id).unwrap();
    let props = &snapshot.summary().additional_properties;
    let snapshot_schema = snapshot.schema(table.metadata())?;
    let mut key_ids = HashSet::new();
    for id in &descriptor.semantics.key_field_ids {
        if !key_ids.insert(*id)
            || !snapshot_schema
                .as_struct()
                .fields()
                .iter()
                .any(|field| field.id == *id)
        {
            return Err(StorageError::Invalid(format!(
                "duplicate or unknown key field ID {id}"
            )));
        }
    }
    if descriptor.live_files_sha256 != live_files_hash(&descriptor.files)? {
        return Err(StorageError::Invalid(
            "live-file fingerprint mismatch".into(),
        ));
    }
    let index = match (&descriptor.index_path, &descriptor.index_sha256) {
        (Some(path), Some(hash)) => {
            if props.get(PROP_INDEX_PATH) != Some(path)
                || props.get(PROP_INDEX_SHA256) != Some(hash)
            {
                return Err(StorageError::Invalid(
                    "snapshot and descriptor index pointers differ".into(),
                ));
            }
            let bytes = std::fs::read(local_path(path)?)?;
            if sha256_hex(&bytes) != *hash {
                return Err(StorageError::Invalid(
                    "fragment index checksum mismatch".into(),
                ));
            }
            let stored: FragmentIndex = serde_json::from_slice(&bytes)?;
            let binding = stored.binding();
            if binding.table_uuid != descriptor.table_uuid
                || binding.snapshot_id != snapshot_id
                || binding.schema_id != descriptor.schema_id
                || binding.comparator_version != descriptor.semantics.comparator_version
                || binding.live_files_sha256 != descriptor.live_files_sha256
                || binding
                    .key_fields
                    .iter()
                    .map(|field| field.field_id)
                    .collect::<Vec<_>>()
                    != descriptor.semantics.key_field_ids
            {
                return Err(StorageError::Invalid(
                    "fragment index binding mismatch".into(),
                ));
            }
            let rebuilt = FragmentIndex::new(binding.clone(), stored.fragments().to_vec())
                .map_err(|e| StorageError::Invalid(e.to_string()))?;
            if rebuilt.fragments() != stored.fragments()
                || rebuilt.fragments().len() != descriptor.files.len()
            {
                return Err(StorageError::Invalid(
                    "fragment index order or count mismatch".into(),
                ));
            }
            Some(stored)
        }
        (None, None)
            if query.is_none()
                && (descriptor.files.len() <= 1 || descriptor.file_sequence.is_some()) =>
        {
            None
        }
        (None, None) => {
            return Err(StorageError::Invalid(
                "ordered fragment index missing".into(),
            ));
        }
        _ => {
            return Err(StorageError::Invalid(
                "incomplete fragment index pointer".into(),
            ));
        }
    };
    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .with_data_file_concurrency_limit(1)
        .build()?;
    let mut tasks = scan.plan_files().await?.try_collect::<Vec<_>>().await?;
    if tasks.len() != descriptor.files.len() {
        return Err(StorageError::Invalid(
            "manifest and descriptor file counts differ".into(),
        ));
    }
    let paths: HashSet<_> = descriptor.files.iter().map(|f| f.path.as_str()).collect();
    if paths.len() != descriptor.files.len()
        || tasks
            .iter()
            .any(|task| !paths.contains(task.data_file_path()))
    {
        return Err(StorageError::Invalid(
            "manifest and descriptor file sets differ".into(),
        ));
    }
    let identity_by_path: HashMap<_, _> = descriptor
        .files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    for task in &tasks {
        let identity = identity_by_path
            .get(task.data_file_path())
            .ok_or_else(|| StorageError::Invalid("unexpected scan task".into()))?;
        if task.data_file_format() != DataFileFormat::Vortex
            || !task.deletes().is_empty()
            || task.start() != 0
            || task.length() != task.file_size_in_bytes()
            || task.record_count() != Some(identity.row_count)
        {
            return Err(StorageError::Unsupported(
                "non-Vortex, deleted, split, or mismatched data file".into(),
            ));
        }
    }
    let mut by_path: HashMap<String, _> = tasks
        .drain(..)
        .map(|task| (task.data_file_path().to_owned(), task))
        .collect();
    let selected = if let Some(index) = &index {
        let leaves = index
            .select(index.binding(), query.unwrap_or(&QueryInterval::default()))
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        let indexed_paths: HashSet<_> = index
            .fragments()
            .iter()
            .map(|leaf| leaf.identity.file_path.as_str())
            .collect();
        if indexed_paths.len() != descriptor.files.len()
            || descriptor
                .files
                .iter()
                .any(|f| !indexed_paths.contains(f.path.as_str()))
        {
            return Err(StorageError::Invalid(
                "index and descriptor file sets differ".into(),
            ));
        }
        leaves
            .into_iter()
            .map(|leaf| leaf.identity.file_path.as_str())
            .collect::<Vec<_>>()
    } else {
        descriptor
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>()
    };
    let mut checked_tasks = Vec::with_capacity(selected.len());
    for path in selected {
        let identity = identity_by_path
            .get(path)
            .ok_or_else(|| StorageError::Invalid("index leaf has no descriptor file".into()))?;
        let task = by_path
            .remove(path)
            .ok_or_else(|| StorageError::Invalid("missing scan task".into()))?;
        if task.data_file_format() != DataFileFormat::Vortex
            || !task.deletes().is_empty()
            || task.start() != 0
            || task.length() != task.file_size_in_bytes()
            || task.record_count() != Some(identity.row_count)
        {
            return Err(StorageError::Unsupported(
                "non-Vortex, deleted, split, or mismatched data file".into(),
            ));
        }
        checked_tasks.push((task, (**identity).clone()));
    }
    // All unselected live files still need their task identity checked against
    // the index; only their bytes are spared by a range query.
    if let Some(index) = &index {
        for leaf in index.fragments() {
            let identity = identity_by_path
                .get(leaf.identity.file_path.as_str())
                .ok_or_else(|| StorageError::Invalid("index leaf has no descriptor file".into()))?;
            if identity.sha256 != leaf.identity.content_sha256
                || identity.row_count != leaf.row_count
            {
                return Err(StorageError::Invalid(
                    "index leaf and descriptor identity differ".into(),
                ));
            }
        }
    }
    let table = table.clone();
    let query = query.cloned();
    let schema = schema_to_arrow_schema(&snapshot_schema)?;
    let field_ids = snapshot_schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.id)
        .collect::<Vec<_>>();
    let stream = async_stream::try_stream! {
        for (task, identity) in checked_tasks {
            let path = local_path(&identity.path)?;
            if std::fs::metadata(&path)?.len() != task.file_size_in_bytes() {
                Err(StorageError::Invalid("Vortex file size mismatch".into()))?;
            }
            if sha256_file(&path)? != identity.sha256 {
                Err(StorageError::Invalid("Vortex file checksum mismatch".into()))?;
            }
            let tasks = futures::stream::iter(vec![Ok(task)]).boxed();
            let mut batches = table.reader_builder().with_data_file_concurrency_limit(1)
                .build().read(tasks)?.stream();
            let mut decoded_rows = 0u64;
            while let Some(batch) = batches.try_next().await? {
                decoded_rows += batch.num_rows() as u64;
                if decoded_rows > identity.row_count {
                    Err(StorageError::Invalid("decoded Vortex row count mismatch".into()))?;
                }
                let batch = if let (Some(index), Some(query)) = (&index, &query) {
                    filter_batch_range(&batch, &schema, &field_ids, index.binding(), query)?
                } else { batch };
                if batch.num_rows() > 0 { yield batch; }
            }
            if decoded_rows != identity.row_count {
                Err(StorageError::Invalid("decoded Vortex row count mismatch".into()))?;
            }
        }
    }
    .boxed();
    Ok((stream, descriptor))
}

pub async fn read_current(table: &Table) -> Result<(Vec<RecordBatch>, SnapshotDescriptor)> {
    let snapshot_id = table
        .metadata()
        .current_snapshot_id()
        .ok_or_else(|| StorageError::Invalid("table has no snapshot".into()))?;
    read_snapshot(table, snapshot_id).await
}

fn key_for_row(
    batch: &RecordBatch,
    field_ids: &[i32],
    binding: &IndexBinding,
    row: usize,
) -> Result<CompositeKey> {
    binding
        .key_fields
        .iter()
        .map(|field| {
            let position = field_ids
                .iter()
                .position(|id| *id == field.field_id)
                .ok_or_else(|| {
                    StorageError::Invalid(format!(
                        "key field {} absent from schema",
                        field.field_id
                    ))
                })?;
            let column = batch.column(position);
            if column.is_null(row) {
                return Ok(KeyValue::Missing);
            }
            let value = match field.data_type {
                KeyType::Int32 => KeyValue::Int32(
                    column
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| StorageError::Invalid("key type differs from index".into()))?
                        .value(row),
                ),
                KeyType::Int64 => KeyValue::Int64(
                    column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| StorageError::Invalid("key type differs from index".into()))?
                        .value(row),
                ),
                KeyType::Bool => KeyValue::Bool(
                    column
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| StorageError::Invalid("key type differs from index".into()))?
                        .value(row),
                ),
                KeyType::AsciiString => {
                    let value = column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| StorageError::Invalid("key type differs from index".into()))?
                        .value(row);
                    if !value.is_ascii() {
                        return Err(StorageError::Invalid("non-ASCII key is unsupported".into()));
                    }
                    KeyValue::AsciiString(value.to_owned())
                }
            };
            Ok(value)
        })
        .collect()
}

fn certify_file(
    binding: &IndexBinding,
    _schema: &arrow_schema::Schema,
    field_ids: &[i32],
    batches: &[RecordBatch],
) -> Result<(CompositeKey, CompositeKey, u64)> {
    let mut first = None;
    let mut previous = None;
    let mut count = 0;
    for batch in batches {
        for row in 0..batch.num_rows() {
            let key = key_for_row(batch, field_ids, binding, row)?;
            if previous
                .as_ref()
                .is_some_and(|last| compare_keys(binding, last, &key) == Ordering::Greater)
            {
                return Err(StorageError::Invalid(
                    "Vortex file rows are not sorted by bound key".into(),
                ));
            }
            if first.is_none() {
                first = Some(key.clone());
            }
            previous = Some(key);
            count += 1;
        }
    }
    Ok((
        first.ok_or_else(|| StorageError::Invalid("empty ordered file".into()))?,
        previous.unwrap(),
        count,
    ))
}

fn key_in_range(binding: &IndexBinding, key: &CompositeKey, query: &QueryInterval) -> bool {
    let lower = query.lower.as_ref().is_none_or(|end| {
        let comparison = compare_keys(binding, key, &end.key);
        comparison == Ordering::Greater || (comparison == Ordering::Equal && end.inclusive)
    });
    let upper = query.upper.as_ref().is_none_or(|end| {
        let comparison = compare_keys(binding, key, &end.key);
        comparison == Ordering::Less || (comparison == Ordering::Equal && end.inclusive)
    });
    lower && upper
}

fn filter_batch_range(
    batch: &RecordBatch,
    _schema: &arrow_schema::Schema,
    field_ids: &[i32],
    binding: &IndexBinding,
    query: &QueryInterval,
) -> Result<RecordBatch> {
    let mut first = None;
    let mut last = 0;
    for row in 0..batch.num_rows() {
        if key_in_range(
            binding,
            &key_for_row(batch, field_ids, binding, row)?,
            query,
        ) {
            first.get_or_insert(row);
            last = row + 1;
        }
    }
    // Certified input and a single lexicographic interval imply a contiguous
    // matching slice, including equal-key groups at file boundaries.
    let start = first.unwrap_or(0);
    Ok(batch.slice(start, last - start))
}

fn reserve_snapshot_id(table: &Table) -> i64 {
    loop {
        let uuid = Uuid::now_v7();
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&uuid.as_bytes()[8..]);
        let id = (u64::from_be_bytes(bytes) & (i64::MAX as u64)) as i64;
        if id > 0 && table.metadata().snapshot_by_id(id).is_none() {
            return id;
        }
    }
}

fn local_path(uri: &str) -> Result<PathBuf> {
    let path = uri.strip_prefix("file://").ok_or_else(|| {
        StorageError::Unsupported(format!("only file:// storage is supported: {uri}"))
    })?;
    if !path.starts_with('/') {
        return Err(StorageError::Invalid("file URI is not absolute".into()));
    }
    if !supported_path(path) {
        return Err(StorageError::Unsupported("encoded, whitespace, or non-ASCII local paths are not supported by the current Iceberg local FileIO".into()));
    }
    Ok(PathBuf::from(path))
}

fn file_uri(path: &Path) -> Result<String> {
    if !path.is_absolute() {
        return Err(StorageError::Invalid("path is not absolute".into()));
    }
    let path = path
        .to_str()
        .ok_or_else(|| StorageError::Unsupported("non-UTF8 path".into()))?;
    if !supported_path(path) {
        return Err(StorageError::Unsupported("encoded, whitespace, or non-ASCII local paths are not supported by the current Iceberg local FileIO".into()));
    }
    Ok(format!("file://{path}"))
}

fn supported_path(path: &str) -> bool {
    path.bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
}

fn write_immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    std::fs::create_dir_all(
        path.parent()
            .ok_or_else(|| StorageError::Invalid("no parent path".into()))?,
    )?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    use std::io::Write;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn live_files_hash(files: &[DataFileIdentity]) -> Result<String> {
    let mut sorted = files.to_vec();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(sha256_hex(&serde_json::to_vec(&sorted)?))
}
