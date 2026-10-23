//! Vail snapshot semantics around iceberg-rust PR 2835's Vortex integration.
//!
//! This first slice supports local, unpartitioned, initial table writes. It uses
//! Iceberg's real catalog transaction, manifest, and snapshot implementation.
//! Vortex file writing and reading are delegated to the pinned Iceberg PR.

pub mod legacy;
pub mod fragment_index;
pub mod globals;
pub mod local_table;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use fragment_index::{
    ARROW_TYPED_COMPARATOR_V4, CompositeKey, FragmentIdentity, FragmentIndex, FragmentInterval,
    IndexBinding, KeyField, KeyType, KeyValue, QueryInterval,
};
use futures::{StreamExt, TryStreamExt};
use iceberg::Catalog;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::FileScanTask;
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

const FORMAT_VERSION: u32 = 2;
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
    /// Optional legacy content hash. New snapshots rely on the immutable
    /// filesystem contract and do not read whole data files to compute it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
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
    if !matches!(
        semantics.comparator_version.as_str(),
        ARROW_TYPED_COMPARATOR_V4
    ) || key_fields
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
        futures::stream::iter(
            files
                .into_iter()
                .enumerate()
                .map(|(index, batches)| Ok(((index, 0), batches))),
        )
        .boxed(),
        semantics,
        key_fields,
    )
    .await
}

/// Write bounded file buffers concurrently and restore their source ordinals
/// before assembling metadata. Only file identities, intervals and Iceberg
/// metadata accumulate across the snapshot.
async fn write_initial_file_stream(
    catalog: &dyn Catalog,
    table: &Table,
    files: futures::stream::BoxStream<'_, Result<((usize, usize), Vec<RecordBatch>)>>,
    semantics: VailSemantics,
    key_fields: Option<Vec<KeyField>>,
) -> Result<Table> {
    if let Some(fields) = &key_fields {
        if !matches!(
            semantics.comparator_version.as_str(),
            ARROW_TYPED_COMPARATOR_V4
        ) || fields.iter().map(|f| f.field_id).collect::<Vec<_>>() != semantics.key_field_ids
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
    // The input remains demand-driven, but once it has yielded independently
    // buffered file chunks their physical writes can overlap. The ordinal is
    // restored below before descriptor/index metadata is assembled.
    let file_schema = table.metadata().current_schema().clone();
    let field_ids = file_schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    let mut pending = files;
    let mut writers = tokio::task::JoinSet::new();
    let mut file_outputs = Vec::new();
    let mut ordinal = 0usize;
    let mut input_done = false;
    let write_result: Result<()> = async {
        loop {
            while !input_done && writers.len() < 32 {
                let next = if writers.is_empty() {
                    pending.next().await
                } else {
                    tokio::select! {
                        biased;
                        joined = writers.join_next() => {
                            match joined {
                                Some(Ok(Ok(Some(output)))) => file_outputs.push(output),
                                Some(Ok(Ok(None))) => (),
                                Some(Ok(Err(error))) => return Err(error),
                                Some(Err(error)) => return Err(StorageError::Execution(Box::new(error))),
                                None => (),
                            }
                            continue;
                        }
                        item = pending.next() => item,
                    }
                };
                match next {
                    Some(Ok((order, batches))) => {
                        let table = table.clone();
                        let binding = binding.clone();
                        let arrow_schema = arrow_schema.clone();
                        let field_ids = field_ids.clone();
                        let file_schema = file_schema.clone();
                        let file_ordinal = ordinal;
                        ordinal += 1;
                        writers.spawn(async move {
                            write_initial_file(
                                &table,
                                snapshot_id,
                                file_ordinal,
                                order,
                                batches,
                                binding.as_ref(),
                                &arrow_schema,
                                &field_ids,
                                file_schema,
                            )
                            .await
                            .map(|output| {
                                output.map(|(_, file, identity, interval)| {
                                    (order, file, identity, interval)
                                })
                            })
                        });
                    }
                    Some(Err(error)) => return Err(error),
                    None => input_done = true,
                }
            }
            if writers.is_empty() {
                if input_done {
                    break;
                }
                continue;
            }
            match writers.join_next().await {
                Some(Ok(Ok(Some(output)))) => file_outputs.push(output),
                Some(Ok(Ok(None))) => (),
                Some(Ok(Err(error))) => return Err(error),
                Some(Err(error)) => return Err(StorageError::Execution(Box::new(error))),
                None => (),
            }
        }
        Ok(())
    }
    .await;
    if write_result.is_err() {
        writers.abort_all();
        while writers.join_next().await.is_some() {}
    }
    write_result?;
    file_outputs.sort_by_key(|(order, _, _, _)| *order);
    for (_, data_file, identity, interval) in file_outputs {
        data_files.push(data_file);
        identities.push(identity);
        if let Some(interval) = interval {
            intervals.push(interval);
        }
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

async fn write_initial_file(
    table: &Table,
    snapshot_id: i64,
    ordinal: usize,
    source_order: (usize, usize),
    batches: Vec<RecordBatch>,
    binding: Option<&IndexBinding>,
    arrow_schema: &arrow_schema::Schema,
    field_ids: &[i32],
    file_schema: iceberg::spec::SchemaRef,
) -> Result<
    Option<(
        usize,
        iceberg::spec::DataFile,
        DataFileIdentity,
        Option<FragmentInterval>,
    )>,
> {
    if !batches.iter().any(|batch| batch.num_rows() > 0) {
        return Ok(None);
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
    let endpoints = binding
        .map(|binding| certify_file(binding, arrow_schema, field_ids, &batches))
        .transpose()?;
    let data_path = local_path(table.metadata().location())?
        .join("data")
        .join(format!(
            "{snapshot_id}-{:020}-{:020}-{}.vortex",
            source_order.0,
            source_order.1,
            Uuid::now_v7()
        ));
    std::fs::create_dir_all(
        data_path
            .parent()
            .ok_or_else(|| StorageError::Invalid("no data parent".into()))?,
    )?;
    let file_path = file_uri(&data_path)?;
    let output = table.file_io().new_output(&file_path)?;
    let mut writer = VortexWriterBuilder::new(file_schema, VortexSession::default())
        .with_structural_lists()
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
    let identity = DataFileIdentity {
        path: file_path.clone(),
        sha256: None,
        row_count: data_file.record_count(),
    };
    let interval = endpoints
        .map(|(first_key, last_key, certified_rows)| {
            if certified_rows != identity.row_count {
                return Err(StorageError::Invalid(
                    "writer row count differs from order certificate".into(),
                ));
            }
            Ok(FragmentInterval {
                identity: FragmentIdentity {
                    file_path,
                    content_sha256: identity.sha256.clone(),
                },
                row_count: certified_rows,
                first_key,
                last_key,
            })
        })
        .transpose()?;
    Ok(Some((ordinal, data_file, identity, interval)))
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
    if props.get(PROP_VERSION).map(String::as_str) != Some(FORMAT_VERSION.to_string().as_str()) {
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
    if descriptor.format_version == 1 {
        return Err(StorageError::Unsupported(
            "snapshot format version 1 uses byte-encoded ordered-key bounds; rewrite with the Arrow-typed index format".into(),
        ));
    }
    if matches!(
        descriptor.semantics.comparator_version.as_str(),
        fragment_index::TOTAL_ORDER_COMPARATOR_V3
    ) && !descriptor.semantics.key_field_ids.is_empty()
    {
        return Err(StorageError::Unsupported(
            "snapshot uses byte-encoded ordered-key bounds; rewrite with the Arrow-typed index format".into(),
        ));
    }
    if descriptor.semantics.comparator_version == fragment_index::HAIL_COMPARATOR_V2
        && !descriptor.semantics.key_field_ids.is_empty()
    {
        return Err(StorageError::Unsupported(
            "snapshot uses the retired Hail key comparator; rewrite with the IEEE total-order comparator".into(),
        ));
    }

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
    let (stream, descriptor, _) = stream_snapshot_internal(
        table,
        snapshot_id,
        query.map(std::slice::from_ref),
        true,
        None,
    )
    .await?;
    Ok((stream.map_ok(|(_, batch)| batch).boxed(), descriptor))
}

/// Stream unfiltered batches from the union of fragments that may intersect
/// any requested interval. Each batch is paired with its zero-based row offset
/// in the descriptor's original file sequence. The index orders the selected
/// files, while offsets retain the original logical row sequence for callers
/// that reconstruct partition boundaries. Data files are opened lazily and
/// every fragment in the snapshot is still validated against snapshot metadata.
pub async fn stream_snapshot_intervals(
    table: &Table,
    snapshot_id: i64,
    queries: &[QueryInterval],
) -> Result<(
    futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>>,
    SnapshotDescriptor,
)> {
    let (stream, descriptor, _) =
        stream_snapshot_internal(table, snapshot_id, Some(queries), false, None).await?;
    Ok((stream, descriptor))
}

/// Intersect indexed candidate files with an original snapshot row range
/// before opening them. Batches retain original offsets and need residual
/// filtering and boundary slicing by the caller.
pub async fn stream_snapshot_intervals_rows(
    table: &Table,
    snapshot_id: i64,
    queries: &[QueryInterval],
    rows: Range<u64>,
) -> Result<(
    futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>>,
    SnapshotDescriptor,
)> {
    let (stream, descriptor, _) =
        stream_snapshot_internal(table, snapshot_id, Some(queries), false, Some(rows)).await?;
    Ok((stream, descriptor))
}

/// Return original row ranges of the candidate files selected by an interval
/// query. This performs the same descriptor, manifest, and index binding
/// validation as the streaming reader but does not open or decode data files.
pub async fn snapshot_interval_row_ranges(
    table: &Table,
    snapshot_id: i64,
    queries: &[QueryInterval],
) -> Result<(Vec<Range<u64>>, SnapshotDescriptor)> {
    let (stream, descriptor, ranges) =
        stream_snapshot_internal(table, snapshot_id, Some(queries), false, None).await?;
    drop(stream);
    Ok((ranges, descriptor))
}

/// Stream only physical data files overlapping an original snapshot row range.
/// File selection uses descriptor row counts and order, so it also works for
/// unkeyed snapshots. Returned offsets are original snapshot row offsets; the
/// caller can slice boundary batches to the requested half-open range.
pub async fn stream_snapshot_rows(
    table: &Table,
    snapshot_id: i64,
    rows: Range<u64>,
) -> Result<(
    futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>>,
    SnapshotDescriptor,
)> {
    let (stream, descriptor, _) =
        stream_snapshot_internal(table, snapshot_id, None, false, Some(rows)).await?;
    Ok((stream, descriptor))
}

async fn stream_snapshot_internal(
    table: &Table,
    snapshot_id: i64,
    queries: Option<&[QueryInterval]>,
    residual_filter: bool,
    row_range: Option<Range<u64>>,
) -> Result<(
    futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>>,
    SnapshotDescriptor,
    Vec<Range<u64>>,
)> {
    let plan = Arc::new(load_snapshot_read_plan(table, snapshot_id, queries.is_none()).await?);
    let (stream, selected_row_ranges) = stream_from_plan(
        plan.clone(),
        queries.map(<[QueryInterval]>::to_vec),
        residual_filter,
        row_range,
        None,
    )?;
    Ok((stream, plan.descriptor.clone(), selected_row_ranges))
}

/// Immutable, snapshot-pinned metadata shared by every LocalRead clone.
/// It contains no row batches and performs no whole-file reads.
#[derive(Debug)]
pub(crate) struct SnapshotReadPlan {
    table: Table,
    pub(crate) descriptor: SnapshotDescriptor,
    pub(crate) index: Option<FragmentIndex>,
    pub(crate) files: Vec<PlannedFile>,
    file_position_by_path: HashMap<String, usize>,
    ordered_file_positions: Vec<usize>,
    file_key_order_rank: Vec<usize>,
    pub(crate) schema: Arc<arrow_schema::Schema>,
    pub(crate) field_ids: Vec<i32>,
    pub(crate) total_rows: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedFile {
    pub(crate) identity: DataFileIdentity,
    pub(crate) offset: u64,
    task: FileScanTask,
}

pub(crate) async fn load_snapshot_read_plan(
    table: &Table,
    snapshot_id: i64,
    allow_unindexed: bool,
) -> Result<SnapshotReadPlan> {
    let descriptor = snapshot_descriptor(table, snapshot_id)?;
    let total_rows = descriptor.files.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(file.row_count)
            .ok_or_else(|| StorageError::Invalid("snapshot row count overflow".into()))
    })?;
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| StorageError::Invalid(format!("snapshot {snapshot_id} not found")))?;
    let snapshot_schema = snapshot.schema(table.metadata())?;
    let schema = schema_to_arrow_schema(&snapshot_schema)?;
    let field_ids = snapshot_schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.id)
        .collect::<Vec<_>>();
    let mut key_ids = HashSet::new();
    for id in &descriptor.semantics.key_field_ids {
        if !key_ids.insert(*id) || !field_ids.contains(id) {
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
            let props = &snapshot.summary().additional_properties;
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
                    .map(|f| f.field_id)
                    .collect::<Vec<_>>()
                    != descriptor.semantics.key_field_ids
            {
                return Err(StorageError::Invalid(
                    "fragment index binding mismatch".into(),
                ));
            }
            if binding.comparator_version == ARROW_TYPED_COMPARATOR_V4 {
                for key in &binding.key_fields {
                    let name = snapshot_schema
                        .as_struct()
                        .fields()
                        .iter()
                        .find(|field| field.id == key.field_id)
                        .map(|field| field.name.as_str())
                        .ok_or_else(|| StorageError::Invalid("unknown key field ID".into()))?;
                    let physical_type = schema
                        .field_with_name(name)
                        .map(|f| f.data_type().clone())
                        .map_err(|_| StorageError::Invalid("unknown key field ID".into()))?;
                    let logical_type = descriptor
                        .semantics
                        .row_type
                        .as_array()
                        .and_then(|fields| fields.iter().find(|field| field[0] == name))
                        .map(|field| field[1].clone())
                        .ok_or_else(|| StorageError::Invalid("missing logical key type".into()))?;
                    if key.data_type != KeyType::Arrow(logical_type)
                        || key.physical_type != physical_type
                    {
                        return Err(StorageError::Invalid(
                            "fragment key type differs from logical schema".into(),
                        ));
                    }
                }
            }
            if stored.fragments().len() != descriptor.files.len() {
                return Err(StorageError::Invalid(
                    "fragment index order or count mismatch".into(),
                ));
            }
            let by_path = descriptor
                .files
                .iter()
                .map(|f| (f.path.as_str(), f))
                .collect::<HashMap<_, _>>();
            let indexed_paths = stored
                .fragments()
                .iter()
                .map(|fragment| fragment.identity.file_path.as_str())
                .collect::<HashSet<_>>();
            if indexed_paths.len() != descriptor.files.len()
                || stored.fragments().iter().any(|fragment| {
                    by_path
                        .get(fragment.identity.file_path.as_str())
                        .is_none_or(|identity| {
                            identity.row_count != fragment.row_count
                                || identity.sha256 != fragment.identity.content_sha256
                        })
                })
            {
                return Err(StorageError::Invalid(
                    "index and descriptor file identities differ".into(),
                ));
            }
            Some(stored)
        }
        (None, None)
            if allow_unindexed
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
    let tasks = scan.plan_files().await?.try_collect::<Vec<_>>().await?;
    if tasks.len() != descriptor.files.len() {
        return Err(StorageError::Invalid(
            "manifest and descriptor file counts differ".into(),
        ));
    }
    let identities = descriptor
        .files
        .iter()
        .map(|f| (f.path.as_str(), f))
        .collect::<HashMap<_, _>>();
    if identities.len() != descriptor.files.len() {
        return Err(StorageError::Invalid(
            "duplicate descriptor file path".into(),
        ));
    }
    let mut task_by_path = HashMap::with_capacity(tasks.len());
    for task in tasks {
        let identity = identities.get(task.data_file_path()).ok_or_else(|| {
            StorageError::Invalid("manifest and descriptor file sets differ".into())
        })?;
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
        task_by_path.insert(task.data_file_path().to_owned(), task);
    }
    if task_by_path.len() != descriptor.files.len() {
        return Err(StorageError::Invalid(
            "manifest and descriptor file sets differ".into(),
        ));
    }
    // Keep ordinal offsets in descriptor/input order. Indexed stream order is
    // tracked separately below because the fragment index is key ordered.
    let mut files = Vec::with_capacity(descriptor.files.len());
    let mut offset = 0u64;
    for identity in &descriptor.files {
        let task = task_by_path
            .remove(&identity.path)
            .ok_or_else(|| StorageError::Invalid("missing manifest task".into()))?;
        files.push(PlannedFile {
            identity: identity.clone(),
            offset,
            task,
        });
        offset = offset
            .checked_add(identity.row_count)
            .ok_or_else(|| StorageError::Invalid("snapshot row offset overflow".into()))?;
    }
    let file_position_by_path: HashMap<String, usize> = files
        .iter()
        .enumerate()
        .map(|(position, file)| (file.identity.path.clone(), position))
        .collect();
    let ordered_file_positions = match &index {
        Some(index) => index
            .fragments()
            .iter()
            .map(|fragment| {
                file_position_by_path
                    .get(&fragment.identity.file_path)
                    .copied()
                    .ok_or_else(|| StorageError::Invalid("index references unknown file".into()))
            })
            .collect::<Result<Vec<_>>>()?,
        None => (0..files.len()).collect(),
    };
    let mut file_key_order_rank = vec![0; files.len()];
    for (rank, position) in ordered_file_positions.iter().copied().enumerate() {
        file_key_order_rank[position] = rank;
    }
    Ok(SnapshotReadPlan {
        table: table.clone(),
        descriptor,
        index,
        files,
        file_position_by_path,
        ordered_file_positions,
        file_key_order_rank,
        schema: Arc::new(schema),
        field_ids,
        total_rows,
    })
}

fn projected_schema(plan: &SnapshotReadPlan, ids: &[i32]) -> Result<Arc<arrow_schema::Schema>> {
    let mut seen = HashSet::new();
    let fields = ids
        .iter()
        .map(|id| {
            if !seen.insert(*id) {
                return Err(StorageError::Invalid(format!(
                    "duplicate projected field ID {id}"
                )));
            }
            let position = plan
                .field_ids
                .iter()
                .position(|candidate| candidate == id)
                .ok_or_else(|| StorageError::Invalid(format!("unknown projected field ID {id}")))?;
            Ok(plan.schema.field(position).clone())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        plan.schema.metadata().clone(),
    )))
}

fn task_with_projection_and_nested_paths(
    task: &FileScanTask,
    field_ids: &[i32],
    nested_paths: Option<&[Vec<String>]>,
) -> Result<FileScanTask> {
    Ok(FileScanTask::builder()
        .with_file_size_in_bytes(task.file_size_in_bytes())
        .with_start(task.start())
        .with_length(task.length())
        .with_record_count(task.record_count())
        .with_first_row_id(task.first_row_id())
        .with_data_sequence_number(task.data_sequence_number())
        .with_data_file_path(task.data_file_path().to_owned())
        .with_data_file_format(task.data_file_format())
        .with_schema(task.schema_ref())
        .with_project_field_ids(field_ids.to_vec())
        .with_nested_projection_paths(nested_paths.map(<[Vec<String>]>::to_vec))
        .with_predicate(task.predicate().cloned())
        .with_deletes(task.deletes().to_vec())
        .with_partition(task.partition().cloned())
        .with_partition_spec(task.partition_spec().cloned())
        .with_name_mapping(task.name_mapping().cloned())
        .with_unified_partition_type(task.unified_partition_type().cloned())
        .with_sort_order_id(task.sort_order_id())
        .with_sort_order(task.sort_order().cloned())
        .with_case_sensitive(task.case_sensitive())
        .with_key_metadata(task.key_metadata().map(Box::<[u8]>::from))
        .build()?)
}

pub(crate) fn stream_from_plan(
    plan: Arc<SnapshotReadPlan>,
    queries: Option<Vec<QueryInterval>>,
    residual_filter: bool,
    row_range: Option<Range<u64>>,
    projection: Option<Vec<i32>>,
) -> Result<(
    futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>>,
    Vec<Range<u64>>,
)> {
    stream_from_plan_with_nested_projection(
        plan,
        queries,
        residual_filter,
        row_range,
        projection,
        None,
    )
}

pub(crate) fn stream_from_plan_with_nested_projection(
    plan: Arc<SnapshotReadPlan>,
    queries: Option<Vec<QueryInterval>>,
    residual_filter: bool,
    row_range: Option<Range<u64>>,
    projection: Option<Vec<i32>>,
    nested_paths: Option<Vec<Vec<String>>>,
) -> Result<(
    futures::stream::BoxStream<'static, Result<(u64, RecordBatch)>>,
    Vec<Range<u64>>,
)> {
    if let Some(rows) = &row_range {
        if rows.start > rows.end {
            return Err(StorageError::Invalid(
                "snapshot row range start exceeds end".into(),
            ));
        }
        if rows.end > plan.total_rows {
            return Err(StorageError::Invalid(
                "snapshot row range exceeds row count".into(),
            ));
        }
    }
    let query_candidates = match (&plan.index, &queries) {
        (Some(index), Some(queries)) => {
            let positions = index
                .select_many(index.binding(), queries)
                .map_err(|e| StorageError::Invalid(e.to_string()))?
                .into_iter()
                .map(|fragment| {
                    plan.file_position_by_path
                        .get(&fragment.identity.file_path)
                        .copied()
                        .ok_or_else(|| {
                            StorageError::Invalid("index references unknown file".into())
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            Some(positions)
        }
        (Some(_), None) => None,
        (None, Some(_)) => {
            return Err(StorageError::Invalid(
                "ordered fragment index missing".into(),
            ));
        }
        (None, None) => None,
    };
    let row_file_range = row_range.as_ref().map(|rows| {
        if rows.start == rows.end {
            let insertion = plan.files.partition_point(|file| file.offset < rows.start);
            return insertion..insertion;
        }
        let start = plan
            .files
            .partition_point(|file| file.offset + file.identity.row_count <= rows.start);
        let end = plan.files.partition_point(|file| file.offset < rows.end);
        start..end
    });
    let candidate_positions = match (&query_candidates, &row_file_range) {
        (Some(candidates), Some(window)) => candidates
            .iter()
            .copied()
            .filter(|position| window.contains(position))
            .collect(),
        (Some(candidates), None) => candidates.clone(),
        (None, Some(window)) => {
            let mut positions = (window.start..window.end).collect::<Vec<_>>();
            positions.sort_unstable_by_key(|position| plan.file_key_order_rank[*position]);
            positions
        }
        (None, None) => plan.ordered_file_positions.clone(),
    };
    let selected = candidate_positions
        .into_iter()
        .map(|position| plan.files[position].clone())
        .collect::<Vec<_>>();
    let mut selected_row_ranges = selected
        .iter()
        .map(|file| file.offset..file.offset + file.identity.row_count)
        .collect::<Vec<_>>();
    selected_row_ranges.sort_unstable_by_key(|range| range.start);
    let mut output_ids = projection.unwrap_or_else(|| plan.field_ids.clone());
    // An empty Hail row intentionally maps to a zero-column RecordBatch.
    if plan.field_ids.is_empty() {
        output_ids.clear();
    }
    let output_schema = projected_schema(&plan, &output_ids)?;
    let residual_query = if residual_filter {
        queries
            .as_ref()
            .and_then(|q| (q.len() == 1).then(|| q[0].clone()))
    } else {
        None
    };
    let mut read_ids = output_ids.clone();
    if residual_query.is_some() {
        if let Some(index) = &plan.index {
            for key in &index.binding().key_fields {
                if !read_ids.contains(&key.field_id) {
                    read_ids.push(key.field_id);
                }
            }
        }
    }
    let read_schema = projected_schema(&plan, &read_ids)?;
    let mut project_positions = Vec::with_capacity(output_ids.len());
    for id in &output_ids {
        project_positions.push(
            read_ids
                .iter()
                .position(|candidate| candidate == id)
                .ok_or_else(|| StorageError::Invalid("projected field was not read".into()))?,
        );
    }
    let plan_for_stream = plan.clone();
    let output_schema_for_stream = output_schema.clone();
    let read_schema_for_stream = read_schema.clone();
    let stream = async_stream::try_stream! {
        for file in selected {
            let path = local_path(&file.identity.path)?;
            if std::fs::metadata(&path)?.len() != file.task.file_size_in_bytes() {
                Err(StorageError::Invalid("Vortex file size mismatch".into()))?;
            }
            let task = task_with_projection_and_nested_paths(&file.task, &read_ids, nested_paths.as_deref())?;
            let tasks = futures::stream::iter(vec![Ok(task)]).boxed();
            let mut batches = plan_for_stream.table.reader_builder().with_data_file_concurrency_limit(1).build().read(tasks)?.stream();
            let mut decoded_rows = 0u64;
            while let Some(batch) = batches.try_next().await? {
                let batch_offset = file.offset.checked_add(decoded_rows)
                    .ok_or_else(|| StorageError::Invalid("batch row offset overflow".into()))?;
                decoded_rows = decoded_rows.checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| StorageError::Invalid("decoded row count overflow".into()))?;
                if decoded_rows > file.identity.row_count { Err(StorageError::Invalid("decoded Vortex row count mismatch".into()))?; }
                let batch = if let (Some(index), Some(query)) = (&plan_for_stream.index, &residual_query) {
                    filter_batch_range(&batch, &read_schema_for_stream, &read_ids, index.binding(), query)?
                } else { batch };
                if batch.num_rows() > 0 {
                    let columns = project_positions.iter().map(|position| batch.column(*position).clone()).collect::<Vec<_>>();
                    let batch = RecordBatch::try_new_with_options(output_schema_for_stream.clone(), columns,
                        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())))?;
                    yield (batch_offset, batch);
                }
            }
            if decoded_rows != file.identity.row_count { Err(StorageError::Invalid("decoded Vortex row count mismatch".into()))?; }
        }
    }.boxed();
    Ok((stream, selected_row_ranges))
}

pub async fn read_current(table: &Table) -> Result<(Vec<RecordBatch>, SnapshotDescriptor)> {
    let snapshot_id = table
        .metadata()
        .current_snapshot_id()
        .ok_or_else(|| StorageError::Invalid("table has no snapshot".into()))?;
    read_snapshot(table, snapshot_id).await
}

fn certify_file(
    binding: &IndexBinding,
    schema: &arrow_schema::Schema,
    field_ids: &[i32],
    batches: &[RecordBatch],
) -> Result<(CompositeKey, CompositeKey, u64)> {
    let positions = key_positions(schema, field_ids, binding)?;
    let options = key_sort_options(binding);
    let mut first: Option<Vec<arrow_array::ArrayRef>> = None;
    let mut previous: Option<Vec<arrow_array::ArrayRef>> = None;
    let mut count = 0u64;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let columns = key_columns(batch, &positions);
        let comparators = fragment_index::make_typed_key_comparators(&columns, &columns, &options)
            .map_err(|error| StorageError::Invalid(error.to_string()))?;
        if let Some(last) = &previous {
            let boundary = fragment_index::make_typed_key_comparators(last, &columns, &options)
                .map_err(|error| StorageError::Invalid(error.to_string()))?;
            if compare_at(&boundary, 0, 0) == Ordering::Greater {
                return Err(StorageError::Invalid(
                    "Vortex file rows are not sorted by bound key".into(),
                ));
            }
        } else {
            first = Some(key_at(&columns, 0));
        }
        for row in 1..batch.num_rows() {
            if compare_at(&comparators, row - 1, row) == Ordering::Greater {
                return Err(StorageError::Invalid(
                    "Vortex file rows are not sorted by bound key".into(),
                ));
            }
        }
        previous = Some(key_at(&columns, batch.num_rows() - 1));
        count = count
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| StorageError::Invalid("ordered file row count overflow".into()))?;
    }
    let first = first.ok_or_else(|| StorageError::Invalid("empty ordered file".into()))?;
    let last = previous.expect("nonempty file has last key");
    Ok((persist_typed_key(&first)?, persist_typed_key(&last)?, count))
}

fn key_positions(
    schema: &arrow_schema::Schema,
    field_ids: &[i32],
    binding: &IndexBinding,
) -> Result<Vec<usize>> {
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
            if position >= schema.fields().len()
                || schema.field(position).data_type() != &field.physical_type
            {
                return Err(StorageError::Invalid(format!(
                    "key field {} type differs from binding",
                    field.field_id
                )));
            }
            Ok(position)
        })
        .collect()
}

fn key_columns(batch: &RecordBatch, positions: &[usize]) -> Vec<arrow_array::ArrayRef> {
    positions
        .iter()
        .map(|position| batch.column(*position).clone())
        .collect()
}

fn key_at(columns: &[arrow_array::ArrayRef], row: usize) -> Vec<arrow_array::ArrayRef> {
    columns.iter().map(|column| column.slice(row, 1)).collect()
}

fn key_sort_options(binding: &IndexBinding) -> Vec<arrow_schema::SortOptions> {
    binding
        .key_fields
        .iter()
        .map(|field| arrow_schema::SortOptions {
            descending: field.descending,
            nulls_first: !field.nulls_last,
        })
        .collect()
}

fn compare_at(
    comparators: &[arrow_ord::ord::DynComparator],
    left: usize,
    right: usize,
) -> Ordering {
    for comparator in comparators {
        let order = comparator(left, right);
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

fn bound_position(
    columns: &[arrow_array::ArrayRef],
    endpoint: &[arrow_array::ArrayRef],
    options: &[arrow_schema::SortOptions],
    inclusive: bool,
    lower_bound: bool,
    rows: usize,
) -> Result<usize> {
    let comparators = fragment_index::make_typed_key_comparators(columns, endpoint, options)
        .map_err(|error| StorageError::Invalid(error.to_string()))?;
    let mut lo = 0;
    let mut hi = rows;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let comparison = compare_at(&comparators, mid, 0);
        let advance = if lower_bound {
            comparison == Ordering::Less || (comparison == Ordering::Equal && !inclusive)
        } else {
            comparison == Ordering::Less || (comparison == Ordering::Equal && inclusive)
        };
        if advance {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn persist_typed_key(key: &[arrow_array::ArrayRef]) -> Result<CompositeKey> {
    key.iter()
        .map(|component| {
            fragment_index::encode_typed_key_component(component)
                .map(KeyValue::ArrowIpc)
                .map_err(|error| StorageError::Invalid(error.to_string()))
        })
        .collect()
}

fn filter_batch_range(
    batch: &RecordBatch,
    _schema: &arrow_schema::Schema,
    field_ids: &[i32],
    binding: &IndexBinding,
    query: &QueryInterval,
) -> Result<RecordBatch> {
    let positions = key_positions(_schema, field_ids, binding)?;
    let columns = key_columns(batch, &positions);
    let options = key_sort_options(binding);
    let lower = query
        .lower
        .as_ref()
        .map(|endpoint| {
            fragment_index::typed_key_from_transport(binding, &endpoint.key)
                .map(|key| (key, endpoint.inclusive))
                .map_err(|error| StorageError::Invalid(error.to_string()))
        })
        .transpose()?;
    let upper = query
        .upper
        .as_ref()
        .map(|endpoint| {
            fragment_index::typed_key_from_transport(binding, &endpoint.key)
                .map(|key| (key, endpoint.inclusive))
                .map_err(|error| StorageError::Invalid(error.to_string()))
        })
        .transpose()?;
    let start = if let Some((key, inclusive)) = &lower {
        bound_position(
            &columns,
            &key.components,
            &options,
            *inclusive,
            true,
            batch.num_rows(),
        )?
    } else {
        0
    };
    let end = if let Some((key, inclusive)) = &upper {
        bound_position(
            &columns,
            &key.components,
            &options,
            *inclusive,
            false,
            batch.num_rows(),
        )?
    } else {
        batch.num_rows()
    };
    // Certified input and a single lexicographic interval imply a contiguous
    // matching slice, including equal-key groups at file boundaries.
    Ok(batch.slice(start.min(end), end.saturating_sub(start)))
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

fn live_files_hash(files: &[DataFileIdentity]) -> Result<String> {
    let mut sorted = files.to_vec();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(sha256_hex(&serde_json::to_vec(&sorted)?))
}

#[cfg(test)]
mod storage_key_search_tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder};
    use arrow_array::{Int32Array, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    fn setup(descending: bool) -> (IndexBinding, Arc<Schema>, Vec<i32>, Vec<KeyField>) {
        let fields = vec![
            KeyField {
                field_id: 10,
                data_type: KeyType::Arrow(serde_json::json!("int32")),
                physical_type: DataType::Int32,
                descending,
                nulls_last: false,
            },
            KeyField {
                field_id: 11,
                data_type: KeyType::Arrow(serde_json::json!("str")),
                physical_type: DataType::Utf8,
                descending: false,
                nulls_last: false,
            },
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let binding = IndexBinding {
            table_uuid: Uuid::nil(),
            snapshot_id: 1,
            schema_id: 0,
            comparator_version: ARROW_TYPED_COMPARATOR_V4.into(),
            live_files_sha256: "0".repeat(64),
            key_fields: fields.clone(),
        };
        (binding, schema, vec![10, 11], fields)
    }

    fn batch(schema: Arc<Schema>, a: &[Option<i32>], b: &[Option<&str>]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(a.to_vec())),
                Arc::new(StringArray::from(b.to_vec())),
            ],
        )
        .unwrap()
    }

    fn endpoint(a: Option<i32>, b: Option<&str>, inclusive: bool) -> fragment_index::Endpoint {
        let components: Vec<arrow_array::ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![a])),
            Arc::new(StringArray::from(vec![b])),
        ];
        fragment_index::Endpoint {
            key: components
                .iter()
                .map(|x| KeyValue::ArrowIpc(fragment_index::encode_typed_key_component(x).unwrap()))
                .collect(),
            inclusive,
        }
    }

    #[test]
    fn certification_and_range_search_handle_duplicates_nulls_and_batch_edges() {
        let (binding, schema, ids, _) = setup(false);
        let batches = vec![
            batch(schema.clone(), &[Some(1), Some(2)], &[Some("a"), Some("a")]),
            batch(
                schema.clone(),
                &[Some(2), Some(3), Some(3)],
                &[Some("a"), None, Some("a")],
            ),
        ];
        let (_, _, count) = certify_file(&binding, &schema, &ids, &batches).unwrap();
        assert_eq!(count, 5);
        let query = QueryInterval {
            lower: Some(endpoint(Some(2), Some("a"), false)),
            upper: Some(endpoint(Some(3), Some("a"), true)),
        };
        let filtered = filter_batch_range(&batches[1], &schema, &ids, &binding, &query).unwrap();
        assert_eq!(filtered.num_rows(), 2); // (2,a) excluded, then (3,null), (3,a) retained.
        assert_eq!(
            filtered
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[3, 3]
        );
    }

    #[test]
    fn descending_ranges_and_unsorted_rejection() {
        let (binding, schema, ids, _) = setup(true);
        let ordered = vec![batch(
            schema.clone(),
            &[Some(5), Some(4), Some(4), Some(2)],
            &[Some("x"); 4],
        )];
        certify_file(&binding, &schema, &ids, &ordered).unwrap();
        let query = QueryInterval {
            lower: Some(endpoint(Some(5), Some("x"), true)),
            upper: Some(endpoint(Some(4), Some("x"), false)),
        };
        let filtered = filter_batch_range(&ordered[0], &schema, &ids, &binding, &query).unwrap();
        assert_eq!(
            filtered
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[5]
        );
        let bad = vec![batch(
            schema.clone(),
            &[Some(1), Some(3)],
            &[Some("x"), Some("x")],
        )];
        assert!(certify_file(&binding, &schema, &ids, &bad).is_err());
    }

    #[test]
    fn nested_struct_key_uses_arrow_comparator() {
        let child_fields: Fields = vec![
            Field::new("major", DataType::Int32, false),
            Field::new("minor", DataType::Int32, false),
        ]
        .into();
        let physical_type = DataType::Struct(child_fields.clone());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            physical_type.clone(),
            false,
        )]));
        let binding = IndexBinding {
            table_uuid: Uuid::nil(),
            snapshot_id: 1,
            schema_id: 0,
            comparator_version: ARROW_TYPED_COMPARATOR_V4.into(),
            live_files_sha256: "0".repeat(64),
            key_fields: vec![KeyField {
                field_id: 12,
                data_type: KeyType::Arrow(
                    serde_json::json!({"struct": [["major", "int32"], ["minor", "int32"]]}),
                ),
                physical_type,
                descending: false,
                nulls_last: false,
            }],
        };
        let nested = Arc::new(StructArray::new(
            child_fields,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 2, 0])),
            ],
            None,
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![nested]).unwrap();
        assert_eq!(
            certify_file(&binding, &schema, &[12], &[batch]).unwrap().2,
            3
        );
    }

    #[test]
    fn nested_list_key_uses_arrow_comparator() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        for values in [&[1][..], &[1, 2][..], &[2][..]] {
            builder.values().append_slice(values);
            builder.append(true);
        }
        let nested = Arc::new(builder.finish());
        let physical_type = nested.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            physical_type.clone(),
            false,
        )]));
        let binding = IndexBinding {
            table_uuid: Uuid::nil(),
            snapshot_id: 1,
            schema_id: 0,
            comparator_version: ARROW_TYPED_COMPARATOR_V4.into(),
            live_files_sha256: "0".repeat(64),
            key_fields: vec![KeyField {
                field_id: 13,
                data_type: KeyType::Arrow(serde_json::json!({"array": "int32"})),
                physical_type,
                descending: false,
                nulls_last: false,
            }],
        };
        let batch = RecordBatch::try_new(schema.clone(), vec![nested]).unwrap();
        assert_eq!(
            certify_file(&binding, &schema, &[13], &[batch]).unwrap().2,
            3
        );
    }
}
