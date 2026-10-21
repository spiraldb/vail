//! Streaming MatrixTable entry expansion ordered by shared column metadata.
//!
//! Input rows are already ordered by their row key and each row's cell list is
//! already permuted to the same order as the column-record global. The
//! execution operator retains only the current equal-row-key run, then uses
//! Arrow `take` kernels to emit each column across the run before advancing to
//! the next key. This preserves composite ordering without sorting the
//! expanded table.

use crate::plan::{PhysicalPlan, properties};
use arrow_array::{
    Array, ArrayRef, LargeListArray, ListArray, RecordBatch, StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion_common::{DataFusionError, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr, expressions::Column,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{collections::HashSet, fmt, fs::File, path::PathBuf, sync::Arc};
use tempfile::TempDir;
use tokio::sync::OnceCell;
use vail_storage::fragment_index::make_typed_key_comparators;

const RUN_MEMORY_BUDGET: usize = 64 * 1024 * 1024;
const OUTPUT_BATCH_MEMORY_BUDGET: usize = 32 * 1024 * 1024;
const OUTPUT_BATCH_ROW_BUDGET: usize = 8192;
const PENDING_SINGLETON_ROW_BUDGET: usize = 8192;

/// A single equal-row-key run. Keep ordinary duplicate runs in memory, but
/// spill oversized runs batch-by-batch so transposition never retains an
/// unbounded expanded output or raw input run in memory.
#[derive(Debug, Default)]
struct KeyRun {
    batches: Vec<RecordBatch>,
    spilled_batches: Vec<PathBuf>,
    spill_dir: Option<TempDir>,
    memory_bytes: usize,
    rows: usize,
    last_source_batch: Option<u64>,
    first_source_batch: Option<(u64, usize)>,
}

impl KeyRun {
    async fn push(
        &mut self,
        batch: RecordBatch,
        source_batch_id: u64,
        source_batch_bytes: usize,
    ) -> datafusion_common::Result<()> {
        let added_bytes = if self.last_source_batch == Some(source_batch_id) {
            0
        } else {
            source_batch_bytes
        };
        let new_rows = self.rows.saturating_add(batch.num_rows());
        if self.spill_dir.is_none()
            && new_rows > 1
            && self.memory_bytes.saturating_add(added_bytes) > RUN_MEMORY_BUDGET
        {
            self.spill_dir = Some(tempfile::tempdir()?);
            for existing in std::mem::take(&mut self.batches) {
                self.spill_batch(existing).await?;
            }
            self.memory_bytes = 0;
        }
        if self.first_source_batch.is_none() {
            self.first_source_batch = Some((source_batch_id, source_batch_bytes));
        }
        self.rows = new_rows;
        self.last_source_batch = Some(source_batch_id);
        if self.spill_dir.is_some() {
            self.spill_batch(batch).await?;
        } else {
            self.memory_bytes = self.memory_bytes.saturating_add(added_bytes);
            self.batches.push(batch);
        }
        Ok(())
    }

    async fn spill_batch(&mut self, batch: RecordBatch) -> datafusion_common::Result<()> {
        let path = self
            .spill_dir
            .as_ref()
            .expect("spill directory exists")
            .path()
            .join(format!("batch-{}.arrow", self.spilled_batches.len()));
        let write_path = path.clone();
        tokio::task::spawn_blocking(move || write_ipc_batch(write_path, batch))
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))??;
        self.spilled_batches.push(path);
        Ok(())
    }

    fn batch_count(&self) -> usize {
        if self.spill_dir.is_some() {
            self.spilled_batches.len()
        } else {
            self.batches.len()
        }
    }

    async fn batch(&self, index: usize) -> datafusion_common::Result<RecordBatch> {
        if self.spill_dir.is_none() {
            return self.batches.get(index).cloned().ok_or_else(|| {
                DataFusionError::Execution("matrix key-run batch is absent".into())
            });
        }
        let path = self.spilled_batches.get(index).ok_or_else(|| {
            DataFusionError::Execution("spilled matrix key-run batch is absent".into())
        })?;
        let path = path.clone();
        tokio::task::spawn_blocking(move || read_ipc_batch(path))
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
    }
}

fn write_ipc_batch(path: PathBuf, batch: RecordBatch) -> datafusion_common::Result<()> {
    let file = File::create(path)?;
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(file, batch.schema().as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}

fn read_ipc_batch(path: PathBuf) -> datafusion_common::Result<RecordBatch> {
    let mut reader = arrow_ipc::reader::StreamReader::try_new(File::open(path)?, None)?;
    reader
        .next()
        .transpose()?
        .ok_or_else(|| DataFusionError::Execution("spilled matrix key-run batch is empty".into()))
}

fn batch_memory_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size())
        .sum()
}

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub row_struct: usize,
    pub cells: usize,
    pub key_indices: Vec<usize>,
    pub columns_global: String,
    pub column_struct: String,
    pub row_field_names: Vec<String>,
    pub col_field_names: Vec<String>,
    pub value_field: String,
    pub output_schema: SchemaRef,
    pub output_partition_tag: String,
    pub output_key_fields: Vec<String>,
    pub global_schema: SchemaRef,
}

/// Cache one globals batch across all independently executable entry ranges.
/// The cache is an execution-plan node so reset_plan_states resets it once and
/// preserves the shared identity for every output partition.
#[derive(Debug)]
pub(crate) struct SharedColumnsExec {
    input: PhysicalPlan,
    schema: SchemaRef,
    config: Config,
    cached_batch: Arc<OnceCell<RecordBatch>>,
    properties: Arc<PlanProperties>,
}

impl SharedColumnsExec {
    pub(crate) fn new(input: PhysicalPlan, config: Config) -> Self {
        let schema = input.schema();
        Self {
            input,
            schema: schema.clone(),
            config,
            cached_batch: Arc::new(OnceCell::new()),
            properties: properties(schema, EmissionType::Incremental),
        }
    }
}

impl DisplayAs for SharedColumnsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MatrixEntriesColumnsCacheExec")
    }
}

impl ExecutionPlan for SharedColumnsExec {
    fn name(&self) -> &'static str { "MatrixEntriesColumnsCacheExec" }
    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
    fn children(&self) -> Vec<&PhysicalPlan> { vec![&self.input] }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![datafusion_physical_plan::Distribution::SinglePartition]
    }
    fn maintains_input_order(&self) -> Vec<bool> { vec![true] }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "MatrixEntriesColumnsCacheExec expects one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children[0].clone(),
            schema: self.schema.clone(),
            config: self.config.clone(),
            cached_batch: self.cached_batch.clone(),
            properties: self.properties.clone(),
        }))
    }
    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
        Ok(Arc::new(Self::new(self.input.clone(), self.config.clone())))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "MatrixEntriesColumnsCacheExec requires partition zero".into(),
            ));
        }
        let input = self.input.clone();
        let cache = self.cached_batch.clone();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                let batch = cache.get_or_try_init(|| async {
                    let mut stream = input.execute(0, context.clone())?;
                    let batch = stream.try_next().await?.ok_or_else(|| {
                        DataFusionError::Execution("matrix column globals produced no row".into())
                    })?;
                    if batch.num_rows() != 1 {
                        return Err(DataFusionError::Execution(
                            "matrix column globals must have exactly one row".into(),
                        ));
                    }
                    Ok::<_, DataFusionError>(batch)
                }).await?;
                yield batch.clone();
            },
        )))
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

pub(crate) fn lower(
    rows: PhysicalPlan,
    columns: PhysicalPlan,
    config: Config,
) -> datafusion_common::Result<PhysicalPlan> {
    Ok(Arc::new(MatrixEntriesExec::new(rows, columns, config)?))
}

#[derive(Debug)]
struct MatrixEntriesExec {
    rows: PhysicalPlan,
    columns: PhysicalPlan,
    config: Config,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl MatrixEntriesExec {
    fn new(
        rows: PhysicalPlan,
        columns: PhysicalPlan,
        config: Config,
    ) -> datafusion_common::Result<Self> {
        let mut fields = config.output_schema.fields().to_vec();
        let tag_name = config.output_partition_tag.clone();
        fields.push(Arc::new(Field::new(tag_name, DataType::UInt64, false)));
        let schema = Arc::new(Schema::new(fields));
        let mut equivalences = EquivalenceProperties::new(schema.clone());
        let ordering = config
            .output_key_fields
            .iter()
            .map(|name| {
                let index = schema.index_of(name)?;
                Ok(PhysicalSortExpr::new(
                    Arc::new(Column::new(name, index)),
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                ))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        if let Some(ordering) = LexOrdering::new(ordering) {
            equivalences.add_ordering(ordering);
        }
        let properties = Arc::new(PlanProperties::new(
            equivalences,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            rows,
            columns,
            config,
            schema,
            properties,
        })
    }
}

impl DisplayAs for MatrixEntriesExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MatrixEntriesByKeyRunsExec")
    }
}

impl ExecutionPlan for MatrixEntriesExec {
    fn name(&self) -> &'static str {
        "MatrixEntriesByKeyRunsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.rows, &self.columns]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // The operator preserves the input row-key sequence, but emits each
        // duplicate row-key run column-major. That reorders occurrences
        // within the run, so it does not preserve either input's row order.
        vec![false, false]
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![
            datafusion_physical_plan::Distribution::SinglePartition,
            datafusion_physical_plan::Distribution::SinglePartition,
        ]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 2 {
            return Err(DataFusionError::Plan(
                "MatrixEntriesByKeyRunsExec expects row and global inputs".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
            self.config.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "MatrixEntriesByKeyRunsExec requires partition zero".into(),
            ));
        }
        let rows_plan = self.rows.clone();
        let columns_plan = self.columns.clone();
        let config = self.config.clone();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                let columns_batch = columns_plan.execute(0, context.clone())?
                    .try_next().await?.ok_or_else(|| DataFusionError::Execution(
                        "matrix column cache produced no row".into()
                    ))?;
                let columns = read_columns(&columns_batch, &config)?;
                let mut input = rows_plan.execute(0, context)?;
                let mut run = KeyRun::default();
                let mut singleton_rows = Vec::<RecordBatch>::new();
                let mut singleton_bytes = 0usize;
                let mut singleton_source_batches = HashSet::<u64>::new();
                let mut last_budget_flush_source_batch = None;
                let mut previous_key: Option<Vec<ArrayRef>> = None;
                let mut source_batch_id = 0u64;
                while let Some(batch) = input.try_next().await? {
                    if batch.num_rows() == 0 { continue; }
                    let current_batch_id = source_batch_id;
                    source_batch_id = source_batch_id.wrapping_add(1);
                    let source_batch_bytes = batch_memory_size(&batch);
                    let key_columns = config.key_indices.iter().map(|&i| batch.column(i).clone()).collect::<Vec<_>>();
                    let options = vec![SortOptions { descending: false, nulls_first: false }; key_columns.len()];
                    let within = if key_columns.is_empty() { vec![] } else {
                        make_typed_key_comparators(&key_columns, &key_columns, &options)
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?
                    };
                    let mut start = 0usize;
                    for row in 0..batch.num_rows() {
                        let comparison = if row > 0 && !within.is_empty() {
                            compare(&within, row - 1, row)
                        } else if row == 0 && !key_columns.is_empty() {
                            if let Some(previous) = &previous_key {
                                let boundary = make_typed_key_comparators(previous, &key_columns, &options)
                                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                                compare(&boundary, 0, 0)
                            } else { None }
                        } else { None };
                        // The keyed TableIR contract already guarantees input
                        // row-key order. Compare adjacent keys only to locate
                        // equal-key run boundaries; do not revalidate or sort
                        // the full row stream here.
                        if comparison == Some(std::cmp::Ordering::Less) {
                            if row > start { run.push(batch.slice(start, row - start), current_batch_id, source_batch_bytes).await?; }
                            let finished = std::mem::take(&mut run);
                            if finished.rows == 1 {
                                if finished.spill_dir.is_some() {
                                    let mut pending = emit_singletons(&mut singleton_rows, columns.clone(), config.clone(), schema.clone())?;
                                    while let Some(output) = pending.try_next().await? { yield output; }
                                    singleton_bytes = 0;
                                    singleton_source_batches.clear();
                                    let mut expanded = emit_run(finished, columns.clone(), config.clone(), schema.clone());
                                    while let Some(output) = expanded.try_next().await? { yield output; }
                                } else {
                                    let (id, bytes) = finished.first_source_batch.expect("singleton has an input batch");
                                    let added_source = singleton_source_batches.insert(id);
                                    if added_source {
                                        singleton_bytes = singleton_bytes.saturating_add(bytes);
                                    }
                                    let hit_byte_limit = singleton_bytes >= RUN_MEMORY_BUDGET;
                                    let hit_row_limit = singleton_rows.len() >= PENDING_SINGLETON_ROW_BUDGET;
                                    singleton_rows.extend(finished.batches);
                                    if hit_row_limit
                                        || (added_source
                                            && hit_byte_limit
                                            && last_budget_flush_source_batch != Some(id))
                                    {
                                        let mut pending = emit_singletons(&mut singleton_rows, columns.clone(), config.clone(), schema.clone())?;
                                        while let Some(output) = pending.try_next().await? { yield output; }
                                        singleton_bytes = 0;
                                        singleton_source_batches.clear();
                                        if hit_byte_limit {
                                            last_budget_flush_source_batch = Some(id);
                                        }
                                    }
                                }
                            } else if finished.rows > 1 {
                                let mut pending = emit_singletons(&mut singleton_rows, columns.clone(), config.clone(), schema.clone())?;
                                while let Some(output) = pending.try_next().await? { yield output; }
                                singleton_bytes = 0;
                                singleton_source_batches.clear();
                                let mut expanded = emit_run(finished, columns.clone(), config.clone(), schema.clone());
                                while let Some(output) = expanded.try_next().await? {
                                    yield output;
                                }
                            }
                            start = row;
                        }
                    }
                    if start < batch.num_rows() { run.push(batch.slice(start, batch.num_rows() - start), current_batch_id, source_batch_bytes).await?; }
                    if !key_columns.is_empty() {
                        previous_key = Some(key_columns.iter().map(|column| column.slice(batch.num_rows() - 1, 1)).collect());
                    }
                }
                let finished = std::mem::take(&mut run);
                if finished.rows == 1 {
                    if finished.spill_dir.is_some() {
                        let mut pending = emit_singletons(&mut singleton_rows, columns.clone(), config.clone(), schema.clone())?;
                        while let Some(output) = pending.try_next().await? { yield output; }
                        let mut expanded = emit_run(finished, columns.clone(), config.clone(), schema.clone());
                        while let Some(output) = expanded.try_next().await? { yield output; }
                    } else {
                        singleton_rows.extend(finished.batches);
                    }
                } else if finished.rows > 1 {
                    let mut pending = emit_singletons(&mut singleton_rows, columns.clone(), config.clone(), schema.clone())?;
                    while let Some(output) = pending.try_next().await? { yield output; }
                    let mut expanded = emit_run(finished, columns.clone(), config.clone(), schema.clone());
                    while let Some(output) = expanded.try_next().await? {
                        yield output;
                    }
                }
                let mut pending = emit_singletons(&mut singleton_rows, columns.clone(), config.clone(), schema.clone())?;
                while let Some(output) = pending.try_next().await? { yield output; }
            },
        )))
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

fn emit_run(
    run: KeyRun,
    columns: Arc<StructArray>,
    config: Config,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        async_stream::try_stream! {
            if columns.is_empty() { return; }
            let column_index = columns.fields().iter().position(|field| field.name() == &config.column_struct)
                .ok_or_else(|| DataFusionError::Execution("matrix column struct field is absent".into()))?;
            let col_struct = downcast_struct(columns.column(column_index).clone(), "matrix columns")?;
            let value_index = config.value_field.clone();
            if run.spill_dir.is_none() {
                let mut prepared = Vec::new();
                for index in 0..run.batch_count() {
                    prepared.push(prepare_run_chunk(run.batch(index).await?, &config, &value_index)?);
                }
                for col in 0..columns.len() {
                    for chunk in &prepared {
                        let rows_per_chunk = chunk.output_chunk_rows(&col_struct);
                        let mut start = 0;
                        while start < chunk.num_rows {
                            let end = (start + rows_per_chunk).min(chunk.num_rows);
                            if let Some(batch) = emit_prepared_run_chunk(chunk, col, start, end, &col_struct, &config, &schema)? {
                                yield batch;
                            }
                            start = end;
                        }
                    }
                }
            } else {
                // Spilled input batches are read once and transposed into
                // per-column IPC stripes. Replaying each raw batch for every
                // column would make large-run I/O quadratic in column count.
                let stripes = tempfile::tempdir()?;
                let mut stripe_paths = vec![Vec::<PathBuf>::new(); columns.len()];
                let mut serial = 0usize;
                for index in 0..run.batch_count() {
                    let chunk = prepare_run_chunk(run.batch(index).await?, &config, &value_index)?;
                    let rows_per_chunk = chunk.output_chunk_rows(&col_struct);
                    for col in 0..columns.len() {
                        let mut start = 0;
                        while start < chunk.num_rows {
                            let end = (start + rows_per_chunk).min(chunk.num_rows);
                            if let Some(batch) = emit_prepared_run_chunk(&chunk, col, start, end, &col_struct, &config, &schema)? {
                                let path = stripes.path().join(format!("column-{col}-chunk-{serial}.arrow"));
                                let write_path = path.clone();
                                tokio::task::spawn_blocking(move || write_ipc_batch(write_path, batch))
                                    .await
                                    .map_err(|error| DataFusionError::Execution(error.to_string()))??;
                                stripe_paths[col].push(path);
                                serial += 1;
                            }
                            start = end;
                        }
                    }
                }
                for paths in stripe_paths {
                    for path in paths {
                        let batch = tokio::task::spawn_blocking(move || read_ipc_batch(path))
                            .await
                            .map_err(|error| DataFusionError::Execution(error.to_string()))??;
                        yield batch;
                    }
                }
            }
        },
    ))
}

struct PreparedRunChunk {
    row: Arc<StructArray>,
    cells: ArrayRef,
    cell_values: Arc<StructArray>,
    entry_values: Arc<StructArray>,
    row_offsets: Vec<i64>,
    num_rows: usize,
}

impl PreparedRunChunk {
    fn output_chunk_rows(&self, columns: &StructArray) -> usize {
        let entry_bytes =
            self.entry_values.get_array_memory_size() / self.entry_values.len().max(1);
        let row_bytes = self.row.get_array_memory_size() / self.row.len().max(1);
        let column_bytes = columns.get_array_memory_size() / columns.len().max(1);
        let estimated_bytes = entry_bytes
            .saturating_add(row_bytes)
            .saturating_add(column_bytes)
            .max(1);
        (OUTPUT_BATCH_MEMORY_BUDGET / estimated_bytes).clamp(1, OUTPUT_BATCH_ROW_BUDGET)
    }
}

fn prepare_run_chunk(
    batch: RecordBatch,
    config: &Config,
    value_field: &str,
) -> datafusion_common::Result<PreparedRunChunk> {
    let row = downcast_struct(batch.column(config.row_struct).clone(), "matrix row")?;
    let cells = batch.column(config.cells).clone();
    let row_offsets = offsets(&cells)?;
    let cell_values = downcast_struct(list_child_values(&cells)?, "matrix cells")?;
    let value_index = cell_values
        .fields()
        .iter()
        .position(|field| field.name() == value_field)
        .ok_or_else(|| DataFusionError::Execution("matrix entry value field is absent".into()))?;
    let entry_values = downcast_struct(
        cell_values.column(value_index).clone(),
        "matrix entry values",
    )?;
    Ok(PreparedRunChunk {
        row,
        cells,
        cell_values,
        entry_values,
        row_offsets,
        num_rows: batch.num_rows(),
    })
}

fn emit_prepared_run_chunk(
    chunk: &PreparedRunChunk,
    col: usize,
    start: usize,
    end: usize,
    col_struct: &StructArray,
    config: &Config,
    output_schema: &SchemaRef,
) -> datafusion_common::Result<Option<RecordBatch>> {
    let mut row_indices = Vec::<u64>::with_capacity(end - start);
    let mut entry_indices = Vec::<u64>::with_capacity(end - start);
    for row_index in start..end {
        if chunk.cells.is_null(row_index) {
            continue;
        }
        let cell_start = chunk.row_offsets[row_index];
        let cell_end = chunk.row_offsets[row_index + 1];
        if cell_end - cell_start != col_struct.len() as i64 {
            return Err(DataFusionError::Execution(format!(
                "matrix row has {} cells for {} columns",
                cell_end - cell_start,
                col_struct.len()
            )));
        }
        let slot = usize::try_from(cell_start + col as i64)
            .map_err(|_| DataFusionError::Execution("negative matrix cell offset".into()))?;
        if chunk.cell_values.is_null(slot) || chunk.entry_values.is_null(slot) {
            continue;
        }
        row_indices.push(row_index as u64);
        entry_indices.push(slot as u64);
    }
    if row_indices.is_empty() {
        return Ok(None);
    }
    let output_len = row_indices.len();
    let row_take = UInt64Array::from(row_indices);
    let entry_take = UInt64Array::from(entry_indices);
    let column_take = UInt64Array::from_value(col as u64, output_len);
    let mut arrays = Vec::<ArrayRef>::with_capacity(output_schema.fields().len());
    for name in &config.row_field_names {
        let index = chunk
            .row
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("matrix row field {name:?} is absent"))
            })?;
        arrays.push(arrow_select::take::take(
            chunk.row.column(index),
            &row_take,
            None,
        )?);
    }
    for name in &config.col_field_names {
        let index = col_struct
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("matrix column field {name:?} is absent"))
            })?;
        arrays.push(arrow_select::take::take(
            col_struct.column(index),
            &column_take,
            None,
        )?);
    }
    for (index, _) in chunk.entry_values.fields().iter().enumerate() {
        arrays.push(arrow_select::take::take(
            chunk.entry_values.column(index),
            &entry_take,
            None,
        )?);
    }
    arrays.push(Arc::new(UInt64Array::from_value(0, output_len)));
    Ok(Some(RecordBatch::try_new(output_schema.clone(), arrays)?))
}

fn emit_singletons(
    singletons: &mut Vec<RecordBatch>,
    columns: Arc<StructArray>,
    config: Config,
    schema: SchemaRef,
) -> datafusion_common::Result<SendableRecordBatchStream> {
    if singletons.is_empty() || columns.is_empty() {
        singletons.clear();
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(),
        )));
    }
    let input_schema = singletons[0].schema();
    let batch = if singletons.len() == 1 {
        singletons.pop().expect("single row batch exists")
    } else {
        let combined = arrow_select::concat::concat_batches(&input_schema, singletons.iter())?;
        singletons.clear();
        combined
    };
    let column_index = columns
        .fields()
        .iter()
        .position(|field| field.name() == &config.column_struct)
        .ok_or_else(|| DataFusionError::Execution("matrix column struct field is absent".into()))?;
    let col_struct = downcast_struct(columns.column(column_index).clone(), "matrix columns")?;
    let prepared = prepare_run_chunk(batch, &config, &config.value_field)?;
    let config = Arc::new(config);
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        async_stream::try_stream! {
            let record_limit = prepared.output_chunk_rows(&col_struct);
            let mut row_indices = Vec::<u64>::with_capacity(record_limit);
            let mut column_indices = Vec::<u64>::with_capacity(record_limit);
            let mut entry_indices = Vec::<u64>::with_capacity(record_limit);
            for row_index in 0..prepared.num_rows {
                if prepared.cells.is_null(row_index) { continue; }
                let start = prepared.row_offsets[row_index];
                let end = prepared.row_offsets[row_index + 1];
                if end - start != columns.len() as i64 {
                    Err(DataFusionError::Execution(format!("matrix row has {} cells for {} columns", end - start, columns.len())))?;
                }
                for col in 0..columns.len() {
                    let slot = usize::try_from(start + col as i64)
                        .map_err(|_| DataFusionError::Execution("negative matrix cell offset".into()))?;
                    if prepared.cell_values.is_null(slot) || prepared.entry_values.is_null(slot) { continue; }
                    row_indices.push(row_index as u64);
                    column_indices.push(col as u64);
                    entry_indices.push(slot as u64);
                    if row_indices.len() == record_limit {
                        let row_take = UInt64Array::from(std::mem::take(&mut row_indices));
                        let column_take = UInt64Array::from(std::mem::take(&mut column_indices));
                        let entry_take = UInt64Array::from(std::mem::take(&mut entry_indices));
                        yield build_output_batch(
                            &prepared.row, &col_struct, &prepared.entry_values,
                            &config, &schema, row_take.len(), &row_take, &column_take, &entry_take,
                        )?;
                        row_indices = Vec::with_capacity(record_limit);
                        column_indices = Vec::with_capacity(record_limit);
                        entry_indices = Vec::with_capacity(record_limit);
                    }
                }
            }
            if !row_indices.is_empty() {
                let row_take = UInt64Array::from(row_indices);
                let column_take = UInt64Array::from(column_indices);
                let entry_take = UInt64Array::from(entry_indices);
                yield build_output_batch(
                    &prepared.row, &col_struct, &prepared.entry_values,
                    &config, &schema, row_take.len(), &row_take, &column_take, &entry_take,
                )?;
            }
        },
    )))
}

fn build_output_batch(
    row: &StructArray,
    col_struct: &StructArray,
    entry_values: &StructArray,
    config: &Config,
    output_schema: &SchemaRef,
    output_len: usize,
    row_take: &UInt64Array,
    column_take: &UInt64Array,
    entry_take: &UInt64Array,
) -> datafusion_common::Result<RecordBatch> {
    let mut arrays = Vec::<ArrayRef>::with_capacity(output_schema.fields().len());
    for name in &config.row_field_names {
        let index = row
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("matrix row field {name:?} is absent"))
            })?;
        arrays.push(arrow_select::take::take(row.column(index), row_take, None)?);
    }
    for name in &config.col_field_names {
        let index = col_struct
            .fields()
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("matrix column field {name:?} is absent"))
            })?;
        arrays.push(arrow_select::take::take(
            col_struct.column(index),
            column_take,
            None,
        )?);
    }
    for (index, _) in entry_values.fields().iter().enumerate() {
        arrays.push(arrow_select::take::take(
            entry_values.column(index),
            entry_take,
            None,
        )?);
    }
    arrays.push(Arc::new(UInt64Array::from_value(0, output_len)));
    Ok(RecordBatch::try_new(output_schema.clone(), arrays)?)
}

fn compare(
    comparators: &[arrow_ord::ord::DynComparator],
    left: usize,
    right: usize,
) -> Option<std::cmp::Ordering> {
    for comparator in comparators {
        let ordering = comparator(left, right);
        if ordering != std::cmp::Ordering::Equal {
            return Some(ordering);
        }
    }
    Some(std::cmp::Ordering::Equal)
}

fn read_columns(
    batch: &RecordBatch,
    config: &Config,
) -> datafusion_common::Result<Arc<StructArray>> {
    if batch.num_rows() != 1 {
        return Err(DataFusionError::Execution(
            "matrix column globals must have exactly one row".into(),
        ));
    }
    let index = config.global_schema.index_of(&config.columns_global)?;
    let list = batch.column(index);
    let values = list_values(list)?;
    downcast_struct(values, "matrix column records")
}

fn list_values(array: &ArrayRef) -> datafusion_common::Result<ArrayRef> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        if list.is_null(0) {
            return Ok(arrow_array::new_empty_array(&list.value_type()));
        }
        return Ok(list.value(0));
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        if list.is_null(0) {
            return Ok(arrow_array::new_empty_array(&list.value_type()));
        }
        return Ok(list.value(0));
    }
    Err(DataFusionError::Execution(
        "matrix array field must be a list".into(),
    ))
}

fn list_child_values(array: &ArrayRef) -> datafusion_common::Result<ArrayRef> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return Ok(list.values().clone());
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        return Ok(list.values().clone());
    }
    Err(DataFusionError::Execution(
        "matrix cell field must be a list".into(),
    ))
}

fn downcast_struct(array: ArrayRef, what: &str) -> datafusion_common::Result<Arc<StructArray>> {
    array
        .as_any()
        .downcast_ref::<StructArray>()
        .map(|value| Arc::new(value.clone()))
        .ok_or_else(|| DataFusionError::Execution(format!("{what} must be a struct array")))
}

fn offsets(array: &ArrayRef) -> datafusion_common::Result<Vec<i64>> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return Ok(list
            .value_offsets()
            .iter()
            .map(|&value| i64::from(value))
            .collect());
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        return Ok(list.value_offsets().to_vec());
    }
    Err(DataFusionError::Execution(
        "matrix cell field must be a list".into(),
    ))
}
