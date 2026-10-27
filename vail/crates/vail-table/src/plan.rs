//! Leaf sources and physical implementations of semantic logical extensions.
//! Child relations must already be planned by DataFusion; this module does not
//! recursively compile TableIR. Internal row identity uses a private Arrow column.
use crate::globals::GlobalBindings;
use crate::{PartitionEvent, Result, TableError, TableIR, TableNode, TableRef, order_requirement};
use arrow_array::{Int32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion_common::{
    DataFusionError, NullHandling, ScalarValue, UnnestOptions, config::ConfigOptions,
    tree_node::TreeNodeRecursion,
};
use datafusion_datasource::{
    PartitionedFile, file_scan_config::FileScanConfigBuilder, source::DataSourceExec,
};
use datafusion_datasource_csv::source::CsvSource;
use datafusion_datasource_json::source::JsonSource;
use datafusion_execution::TaskContext;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::Operator;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
    expressions::{BinaryExpr, Column, Literal, NotExpr},
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    coalesce_partitions::CoalescePartitionsExec,
    empty::EmptyExec,
    execution_plan::{Boundedness, EmissionType},
    filter::FilterExec,
    joins::CrossJoinExec,
    limit::GlobalLimitExec,
    projection::ProjectionExec,
    sorts::sort::SortExec,
    stream::RecordBatchStreamAdapter,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    union::UnionExec,
    unnest::{ListUnnest, UnnestExec},
};
use futures::TryStreamExt;
use std::{collections::VecDeque, fmt, sync::Arc};
use vail_execution::expressions::{Expr, lower_with_bindings, lower_with_global_columns};
use vail_storage::local_table::{LocalRead, normalize};

pub type PhysicalPlan = Arc<dyn ExecutionPlan>;

fn external(error: impl Into<TableError>) -> DataFusionError {
    DataFusionError::External(Box::new(error.into()))
}
pub(crate) fn properties(schema: SchemaRef, emission: EmissionType) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        emission,
        Boundedness::Bounded,
    ))
}
pub(crate) fn column(plan: &PhysicalPlan, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(plan.schema().field(index).name(), index))
}
pub(crate) fn private_name(schema: &Schema, globals: &Schema, base: &str) -> String {
    let mut name = base.to_owned();
    while schema.index_of(&name).is_ok() || globals.index_of(&name).is_ok() {
        name.push('_');
    }
    name
}
fn source_properties(schema: SchemaRef, keys: &[String]) -> Result<Arc<PlanProperties>> {
    let mut cache = (*properties(schema.clone(), EmissionType::Incremental)).clone();
    if !keys.is_empty() {
        let requirement = order_requirement(
            &keys
                .iter()
                .map(|key| (key.clone(), false))
                .collect::<Vec<_>>(),
        )?;
        cache
            .eq_properties
            .add_ordering(requirement.physical_ordering(&schema)?);
    }
    cache.set_eq_properties(cache.eq_properties.clone());
    Ok(Arc::new(cache))
}
fn with_partition(schema: &Schema, name: &str) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(name, DataType::UInt64, false)));
    Arc::new(Schema::new(fields))
}
fn tag(
    batch: RecordBatch,
    partition: usize,
    schema: SchemaRef,
) -> datafusion_common::Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from_value(
        partition as u64,
        batch.num_rows(),
    )));
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[derive(Debug, Clone)]
enum Source {
    ReadPartition(Arc<LocalRead>, usize),
    ReadPartitionProjected(Arc<LocalRead>, usize, Vec<i32>),
    ReadPartitionNested(Arc<LocalRead>, usize, Vec<i32>, Vec<Vec<String>>),
    Parallelize {
        rows: crate::arrow_batches::ArrowBatches,
        partitions: usize,
    },
    Globals(Arc<LocalRead>),
}
#[derive(Debug)]
struct SourceExec {
    source: Source,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
    row_schema: SchemaRef,
}

/// The globals decoder validates exactly one row before emitting any batch.
pub(crate) fn singleton_globals_source(input: &PhysicalPlan) -> bool {
    input
        .as_ref()
        .downcast_ref::<SourceExec>()
        .is_some_and(|scan| matches!(scan.source, Source::Globals(_)))
}

/// Match an immutable source partition to the metadata used by a range proof.
pub(crate) fn matches_source_partition(
    input: &PhysicalPlan,
    table: &TableRef,
    partition: usize,
) -> bool {
    // DataFusion inserts cooperative yielding around source streams without
    // changing their batches, row order, or partition assignment.
    if input
        .as_ref()
        .is::<datafusion_physical_plan::coop::CooperativeExec>()
    {
        return input.children().len() == 1
            && matches_source_partition(input.children()[0], table, partition);
    }
    let identity = match table.node() {
        TableNode::Read { source } => {
            input
                .as_ref()
                .downcast_ref::<SourceExec>()
                .is_some_and(|scan| match &scan.source {
                    Source::ReadPartition(actual, index)
                    | Source::ReadPartitionProjected(actual, index, _)
                    | Source::ReadPartitionNested(actual, index, _, _) => {
                        Arc::ptr_eq(actual, source) && *index == partition
                    }
                    _ => false,
                })
        }
        TableNode::Range {
            n, n_partitions, ..
        } => input
            .as_ref()
            .downcast_ref::<RangePartitionExec>()
            .is_some_and(|scan| {
                let start = n / n_partitions * partition + partition.min(n % n_partitions);
                let end = start + n / n_partitions + usize::from(partition < n % n_partitions);
                scan.start == start && scan.end == end
            }),
        _ => false,
    };
    // Certified bounds complement, rather than replace, the source's existing
    // ascending/nulls-last within-partition ordering contract.
    identity
        && order_requirement(
            &table
                .keys()
                .iter()
                .map(|name| (name.clone(), false))
                .collect::<Vec<_>>(),
        )
        .and_then(|requirement| {
            requirement
                .physical_ordering(&input.schema())
                .map_err(TableError::from)
        })
        .is_ok_and(|ordering| {
            input
                .properties()
                .equivalence_properties()
                .ordering_satisfy(ordering)
                .unwrap_or(false)
        })
}

impl DisplayAs for SourceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
impl ExecutionPlan for SourceExec {
    fn name(&self) -> &'static str {
        match self.source {
            Source::ReadPartition(_, _)
            | Source::ReadPartitionProjected(_, _, _)
            | Source::ReadPartitionNested(_, _, _, _) => "VicebergScanExec",
            Source::Globals(_) => "GlobalsScanExec",
            Source::Parallelize { .. } => "ParallelizeExec",
        }
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn metrics(&self) -> Option<MetricsSet> { Some(self.metrics.clone_inner()) }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("source has no children".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "invalid source partition".into(),
            ));
        }
        let source = match &self.source {
            Source::ReadPartition(source, part) => Source::ReadPartition(
                Arc::new(source.with_scan_metrics(crate::read_metrics::register(&self.metrics, partition))), *part),
            Source::ReadPartitionProjected(source, part, fields) => Source::ReadPartitionProjected(
                Arc::new(source.with_scan_metrics(crate::read_metrics::register(&self.metrics, partition))), *part, fields.clone()),
            Source::ReadPartitionNested(source, part, fields, paths) => Source::ReadPartitionNested(
                Arc::new(source.with_scan_metrics(crate::read_metrics::register(&self.metrics, partition))), *part, fields.clone(), paths.clone()),
            source => source.clone(),
        };
        let schema = self.schema();
        let output_schema = schema.clone();
        let row_schema = self.row_schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            async_stream::try_stream! {
                match source {
                    Source::ReadPartition(source, part) => {
                        let mut input=source.partition_batches(part);
                        while let Some(event)=input.try_next().await.map_err(external)? {
                            if let PartitionEvent::Batch(batch)=event {let batch=normalize(&batch,row_schema.clone()).map_err(external)?;yield tag(batch,0,schema.clone())?;}
                        }
                    }
                    Source::ReadPartitionProjected(source, part, field_ids) => {
                        let mut input = source.projected_partition_batches(part, &field_ids);
                        while let Some(event) = input.try_next().await.map_err(external)? {
                            if let PartitionEvent::Batch(batch) = event {
                                let batch = normalize(&batch, row_schema.clone()).map_err(external)?;
                                yield tag(batch, 0, schema.clone())?;
                            }
                        }
                    }
                    Source::ReadPartitionNested(source, part, field_ids, paths) => {
                        let source = source.with_nested_projection_paths(paths);
                        let mut input = source.projected_partition_batches(part, &field_ids);
                        while let Some(event) = input.try_next().await.map_err(external)? {
                            if let PartitionEvent::Batch(batch) = event {
                                let batch = normalize(&batch, row_schema.clone()).map_err(external)?;
                                yield tag(batch, 0, schema.clone())?;
                            }
                        }
                    }
                    Source::Parallelize { rows, partitions } => {
                        for p in 0..partitions {
                            let bounds=rows.partition_bounds(p,partitions);
                            let window=rows.slice(bounds.start,bounds.len());
                            for batch in window.batches() {
                                for offset in (0..batch.num_rows()).step_by(65536) {
                                    yield tag(batch.slice(offset,(batch.num_rows()-offset).min(65536)),p,schema.clone())?;
                                }
                            }
                        }
                    }
                    Source::Globals(source) => {
                        let batch = vail_storage::globals::read(&source.semantics.globals).map_err(external)?;
                        vail_storage::globals::validate_agreement(batch.schema_ref(),&row_schema).map_err(external)?;
                        yield normalize(&batch, row_schema).map_err(external)?;
                    }
                }
            },
        )))
    }
}
pub(crate) fn source_partition(table: &Arc<TableIR>, part: usize) -> Result<PhysicalPlan> {
    source_partition_projected(table, part, None)
}

pub(crate) fn source_partition_projected(
    table: &Arc<TableIR>,
    part: usize,
    projection: Option<&Vec<usize>>,
) -> Result<PhysicalPlan> {
    source_partition_projected_with_nested_paths(table, part, projection, Vec::new())
}

pub(crate) fn source_partition_projected_with_nested_paths(
    table: &Arc<TableIR>,
    part: usize,
    projection: Option<&Vec<usize>>,
    nested_paths: Vec<Vec<String>>,
) -> Result<PhysicalPlan> {
    source_partition_with_selection(table, part, projection, nested_paths, None, Vec::new())
}
pub(crate) fn bgen_source_partition(
    table: &Arc<TableIR>,
    part: usize,
    selection: Arc<crate::bgen_select::Selection>,
) -> Result<PhysicalPlan> {
    source_partition_with_selection(table, part, None, Vec::new(), Some(selection), Vec::new())
}
pub(crate) fn legacy_source_partition(table: &Arc<TableIR>, part: usize, projection: Option<&Vec<usize>>, filters: Vec<Arc<dyn PhysicalExpr>>) -> Result<PhysicalPlan> {
    source_partition_with_selection(table, part, projection, Vec::new(), None, filters)
}
fn source_partition_with_selection(
    table: &Arc<TableIR>,
    part: usize,
    projection: Option<&Vec<usize>>,
    nested_paths: Vec<Vec<String>>,
    bgen_selection: Option<Arc<crate::bgen_select::Selection>>,
    legacy_filters: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<PhysicalPlan> {
    let row_schema = table.schema().clone();
    let name = private_name(&row_schema, table.globals().schema(), "__vail_partition");
    let schema = with_partition(&row_schema, &name);
    if let TableNode::TextRead {
        paths,
        format,
        options_json,
        legacy,
    } = table.node()
    {
        let path = paths
            .get(part)
            .ok_or_else(|| TableError::Invalid("text source partition out of range".into()))?;
        let file_size = if format == "legacy" { 0 } else {
            std::fs::metadata(path).map_err(|e| TableError::Invalid(format!("cannot stat text input {path:?}: {e}")))?.len()
        };
        let options: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(options_json.clone())
                .map_err(|e| TableError::Invalid(e.to_string()))?;
        let (file_schema, source): (SchemaRef, Arc<dyn datafusion_datasource::file::FileSource>) =
            match format.as_str() {
                "plink" => (
                    row_schema.clone(),
                    Arc::new(crate::plink_source::PlinkSource::new(
                        row_schema.clone(),
                        options_json,
                    )?),
                ),
                "legacy" => (
                    row_schema.clone(),
                    Arc::new(crate::legacy_source::LegacySource::new(row_schema.clone(), legacy.as_ref().ok_or_else(||TableError::Invalid("missing legacy source descriptor".into()))?.clone(), part).with_decode_paths(options_json).with_predicates(&legacy_filters)),
                ),
                "avro" => (
                    row_schema.clone(),
                    Arc::new(crate::avro_source::AvroSource::new(
                        row_schema.clone(),
                        options_json,
                    )?),
                ),
                "bgen" => {
                    let mut selected = if let Some(parts) = options_json.get("partition_options") {
                        parts.as_array().and_then(|parts| parts.get(part)).cloned()
                            .ok_or_else(|| TableError::Invalid(
                                "BGEN partition option count disagrees with source paths".into(),
                            ))?
                    } else {
                        options_json.clone()
                    };
                    if let Some(paths) = options_json.get("decode_paths") {
                        selected["decode_paths"] = paths.clone();
                    }
                    (
                        row_schema.clone(),
                        Arc::new(crate::bgen_source::BgenSource::new(
                            row_schema.clone(),
                            &selected,
                        )?.with_selection(bgen_selection.clone())),
                    )
                },
                "vcf" => (
                    row_schema.clone(),
                    Arc::new(crate::vcf_source::VcfSource::new(
                        row_schema.clone(),
                        options_json,
                    )?),
                ),
                "csv" => {
                    let csv = crate::text_schema::csv_options(options)?;
                    (
                        row_schema.clone(),
                        Arc::new(CsvSource::new(row_schema.clone()).with_csv_options(csv)),
                    )
                }
                "json" | "ndjson" => {
                    let json = crate::text_schema::json_options(options)?;
                    let newline = json.newline_delimited;
                    (
                        row_schema.clone(),
                        Arc::new(
                            JsonSource::new(row_schema.clone()).with_newline_delimited(newline),
                        ),
                    )
                }
                "lines" => {
                    let fields = vec![Arc::new(arrow_schema::Field::new(
                        "text",
                        DataType::Utf8,
                        false,
                    ))];
                    let lines_schema = Arc::new(Schema::new(fields));
                    (
                        lines_schema.clone(),
                        Arc::new(
                            crate::line_source::LineSource::new().with_gzip(
                                options
                                    .get("force_bgz")
                                    .and_then(|value| value.as_bool())
                                    .unwrap_or(false)
                                    || path.ends_with(".gz")
                                    || path.ends_with(".bgz"),
                            ).with_bgzf(path.clone(), options.get("bgzf_ranges")
                                .and_then(serde_json::Value::as_array).and_then(|ranges| ranges.get(part))
                                .filter(|range| !range.is_null())
                                .map(|range| serde_json::from_value(range.clone()))
                                .transpose().map_err(|error| TableError::Invalid(format!("invalid BGZF range: {error}")))?),
                        ),
                    )
                }
                _ => {
                    return Err(TableError::Invalid(format!(
                        "unsupported text format {format:?}"
                    )));
                }
            };
        let file_path = path.trim_start_matches('/');
        let file_projection = projection.map(|indices| {
            indices
                .iter()
                .copied()
                .filter(|&i| i < file_schema.fields().len())
                .collect::<Vec<_>>()
        });
        let mut builder = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file(PartitionedFile::new(file_path, file_size))
            .with_preserve_order(true);
        if format != "lines" {
            builder = builder.with_projection(file_projection.clone());
        }
        let config = builder.build();
        let scan: PhysicalPlan = Arc::new(crate::rng_context::SourceLineageExec::new(
            DataSourceExec::from_data_source(config),
        ));
        let mut exprs = Vec::new();
        if format == "lines" {
            exprs.push((
                Arc::new(Literal::new(ScalarValue::Utf8(Some(format!(
                    "file:{path}"
                ))))) as Arc<dyn PhysicalExpr>,
                "file".to_owned(),
            ));
            exprs.push((
                Arc::new(Column::new("text", 0)) as Arc<dyn PhysicalExpr>,
                "text".to_owned(),
            ));
        } else {
            let indices =
                file_projection.unwrap_or_else(|| (0..file_schema.fields().len()).collect());
            for (index, source_index) in indices.into_iter().enumerate() {
                let field = file_schema.field(source_index);
                exprs.push((
                    Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                ));
            }
        }
        if projection.is_none_or(|indices| indices.contains(&row_schema.fields().len())) {
            exprs.push((
                Arc::new(Literal::new(ScalarValue::UInt64(Some(part as u64))))
                    as Arc<dyn PhysicalExpr>,
                name,
            ));
        }
        return Ok(Arc::new(ProjectionExec::try_new(exprs, scan)?));
    }
    if let TableNode::Read { source } = table.node()
        && let Some(projection) = projection
    {
        let tag_index = row_schema.fields().len();
        let row_indices = projection
            .iter()
            .copied()
            .filter(|&index| index != tag_index)
            .collect::<Vec<_>>();
        let field_ids = row_indices
            .iter()
            .map(|&index| {
                source.field_ids().get(index).copied().ok_or_else(|| {
                    TableError::Invalid(format!("read projection field {index} is out of range"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let projected_schema = Arc::new(row_schema.project(&row_indices)?);
        let projected_name = private_name(
            &projected_schema,
            table.globals().schema(),
            "__vail_partition",
        );
        let projected_output_schema = with_partition(&projected_schema, &projected_name);
        // A source's certified full key ordering survives projection only if
        // every key root remains intact. Provider nested pruning excludes keys.
        let ordering_keys = if table.keys().iter().all(|key| {
            projected_schema.index_of(key).is_ok()
                && !nested_paths.iter().any(|path| path.first() == Some(key))
        }) {
            table.keys()
        } else {
            &[]
        };
        return Ok(Arc::new(SourceExec {
            metrics: ExecutionPlanMetricsSet::new(),
            source: if nested_paths.is_empty() {
                Source::ReadPartitionProjected(source.clone(), part, field_ids)
            } else {
                Source::ReadPartitionNested(source.clone(), part, field_ids, nested_paths)
            },
            cache: source_properties(projected_output_schema, ordering_keys)?,
            row_schema: projected_schema,
        }));
    }
    let source = match table.node() {
        TableNode::Read { source } => Source::ReadPartition(source.clone(), part),
        TableNode::Range {
            n,
            n_partitions,
            batch_size,
        } => {
            let start = n / n_partitions * part + part.min(n % n_partitions);
            let end = start + n / n_partitions + usize::from(part < n % n_partitions);
            // Use a shared Arrow-free range context for this exact partition.
            return Ok(Arc::new(RangePartitionExec {
                start,
                end,
                batch_size: *batch_size,
                cache: source_properties(schema, &["idx".into()])?,
                row_schema,
            }));
        }
        TableNode::Parallelize { rows, n_partitions } => {
            let bounds = rows.partition_bounds(part, *n_partitions);
            Source::Parallelize {
                rows: rows.slice(bounds.start, bounds.len()),
                partitions: 1,
            }
        }
        _ => return Err(TableError::Invalid("not a partitioned source".into())),
    };
    Ok(Arc::new(SourceExec {
        metrics: ExecutionPlanMetricsSet::new(),
        source,
        cache: source_properties(schema, table.keys())?,
        row_schema,
    }))
}

/// Construct the part of a range source that can contribute to an integer
/// key interval. The bounds are applied before any Arrow rows are generated.
pub(crate) fn range_partition_clipped(
    table: &Arc<TableIR>,
    part: usize,
    lower: usize,
    upper: usize,
) -> Result<PhysicalPlan> {
    let TableNode::Range {
        n,
        n_partitions,
        batch_size,
    } = table.node()
    else {
        return Err(TableError::Invalid("not a range source".into()));
    };
    if *n_partitions == 0 || part >= *n_partitions {
        return Err(TableError::Invalid("range partition out of range".into()));
    }
    let start = n / n_partitions * part + part.min(n % n_partitions);
    let end = start + n / n_partitions + usize::from(part < n % n_partitions);
    let start = start.max(lower);
    let end = end.min(upper).max(start);
    let row_schema = table.schema().clone();
    let name = private_name(&row_schema, table.globals().schema(), "__vail_partition");
    let schema = with_partition(&row_schema, &name);
    Ok(Arc::new(RangePartitionExec {
        start,
        end,
        batch_size: *batch_size,
        cache: source_properties(schema, table.keys())?,
        row_schema,
    }))
}
#[derive(Debug)]
struct RangePartitionExec {
    start: usize,
    end: usize,
    batch_size: std::num::NonZeroUsize,
    cache: Arc<PlanProperties>,
    row_schema: SchemaRef,
}
impl DisplayAs for RangePartitionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RangeExec: [{}, {})", self.start, self.end)
    }
}
impl ExecutionPlan for RangePartitionExec {
    fn name(&self) -> &'static str {
        "RangeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("range source has no children".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        p: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if p != 0 {
            return Err(DataFusionError::Execution("invalid range partition".into()));
        }
        let (start, end, schema, row_schema) =
            (self.start, self.end, self.schema(), self.row_schema.clone());
        // Ordinary ranges retain their explicit IR batch size. Only actions
        // carrying the typed wide-list advice cap opt in to the session limit.
        let batch_size = context
            .session_config()
            .get_extension::<crate::wide_batch::WideBatchLimit>()
            .map_or(self.batch_size, |limit| {
                std::num::NonZeroUsize::new(self.batch_size.get().min(limit.rows.max(1)))
                    .expect("batch size stays positive")
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                for offset in (start..end).step_by(batch_size.get()) {
                    let batch=RecordBatch::try_new(row_schema.clone(),vec![Arc::new(Int32Array::from_iter_values((offset..end.min(offset.saturating_add(batch_size.get()))).map(|i|i as i32)))])?;
                    yield tag(batch,0,schema.clone())?;
                }
            },
        )))
    }
}
pub(crate) fn globals_scan(source: Arc<LocalRead>, schema: SchemaRef) -> PhysicalPlan {
    Arc::new(SourceExec {
        metrics: ExecutionPlanMetricsSet::new(),
        source: Source::Globals(source),
        cache: properties(schema.clone(), EmissionType::Incremental),
        row_schema: schema,
    })
}

/// Streaming extensions for operations without an equivalent stock DF operator.
/// These consume child ExecutionPlans, never TableIR or a second interpreter.
#[derive(Clone, Debug)]
enum Unary {
    Tail(usize),
    Ordinal,
}
#[derive(Debug)]
struct UnaryExec {
    input: PhysicalPlan,
    kind: Unary,
    cache: Arc<PlanProperties>,
}
impl UnaryExec {
    fn create(input: PhysicalPlan, kind: Unary) -> PhysicalPlan {
        let mut schema = input.schema();
        let emission = if matches!(kind, Unary::Tail(_)) {
            EmissionType::Final
        } else {
            EmissionType::Incremental
        };
        if matches!(kind, Unary::Ordinal) {
            let name = private_name(&schema, &Schema::empty(), "__vail_ordinal");
            schema = with_partition(&schema, &name);
        }
        let mut cache = (*properties(schema.clone(), emission)).clone();
        if matches!(kind, Unary::Ordinal) {
            cache.eq_properties.add_ordering(
                LexOrdering::new(vec![PhysicalSortExpr::new(
                    Arc::new(Column::new(
                        schema.field(schema.fields().len() - 1).name(),
                        schema.fields().len() - 1,
                    )),
                    SortOptions::default(),
                )])
                .unwrap(),
            );
        }
        cache.set_eq_properties(cache.eq_properties.clone());
        Arc::new(Self {
            input,
            kind,
            cache: Arc::new(cache),
        })
    }
}
impl DisplayAs for UnaryExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self.kind)
    }
}
impl ExecutionPlan for UnaryExec {
    fn name(&self) -> &'static str {
        match self.kind {
            Unary::Tail(_) => "OrderedTailExec",
            Unary::Ordinal => "OrdinalExec",
        }
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![datafusion_physical_plan::Distribution::SinglePartition]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "unary operator requires one child".into(),
            ));
        }
        Ok(Self::create(children[0].clone(), self.kind.clone()))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "ordered operator requires one physical partition".into(),
            ));
        }
        let input = self.input.clone();
        let kind = self.kind.clone();
        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                // A zero suffix must not open its source or evaluate its expressions.
                if !matches!(kind, Unary::Tail(0)) {
                    let mut input = input.execute(0, context)?;
                    match kind {
                        Unary::Tail(n) => {
                            let mut retained = VecDeque::new();
                            let mut rows = 0;
                            while let Some(batch) = input.try_next().await? {
                                if batch.num_rows() == 0 { continue; }
                                rows += batch.num_rows();
                                retained.push_back(batch);
                                while rows > n {
                                    let first = retained.pop_front().unwrap();
                                    let remove = (rows - n).min(first.num_rows());
                                    rows -= remove;
                                    if remove < first.num_rows() { retained.push_front(first.slice(remove, first.num_rows()-remove)); }
                                }
                            }
                            for batch in retained { yield batch; }
                        }
                        Unary::Ordinal => {
                            let mut offset = 0u64;
                            while let Some(batch) = input.try_next().await? {
                                let end = offset.checked_add(batch.num_rows() as u64).ok_or_else(|| DataFusionError::Execution("row ordinal overflow".into()))?;
                                let mut columns = batch.columns().to_vec();
                                columns.push(Arc::new(UInt64Array::from_iter_values(offset..end)));
                                offset = end;
                                yield RecordBatch::try_new(schema.clone(), columns)?;
                            }
                        }
                    }
                }
            },
        )))
    }
}
pub(crate) fn ordinal(input: PhysicalPlan) -> PhysicalPlan {
    UnaryExec::create(input, Unary::Ordinal)
}

#[derive(Clone, Debug)]
pub enum PartitionLayout {
    Fixed(usize),
    Observed,
}
pub struct TablePlan {
    pub rows: PhysicalPlan,
    pub layout: PartitionLayout,
}

pub(crate) fn physical_schema(table: &Arc<TableIR>) -> SchemaRef {
    with_partition(
        table.schema(),
        &private_name(table.schema(), table.globals().schema(), "__vail_partition"),
    )
}

pub(crate) fn expression_array(
    expression: &Expr,
    bindings: &GlobalBindings,
) -> Result<PhysicalPlan> {
    let empty = crate::globals::GlobalsIR::empty().lower()?;
    let (input, indices) = bind_external(empty, bindings)?;
    let array_name = "__vail_expression_array";
    let value = lower_with_bindings(expression, &input.schema(), &Schema::empty(), &indices)?;
    Ok(Arc::new(ProjectionExec::try_new(
        vec![(value, array_name.into())],
        input,
    )?))
}
/// Obtain a physical child without recursively lowering TableIR.
fn prepared(table: &TableRef) -> Result<TablePlan> {
    let TableNode::Prepared {
        rows, n_partitions, ..
    } = table.node()
    else {
        return Err(TableError::Invalid(
            "physical operation received an unplanned child".into(),
        ));
    };
    Ok(TablePlan {
        rows: rows.clone(),
        layout: PartitionLayout::Fixed(*n_partitions),
    })
}

pub(crate) fn lower_operation(table: &Arc<TableIR>) -> Result<TablePlan> {
    let row_schema = table.schema().clone();
    let partition_name = private_name(&row_schema, table.globals().schema(), "__vail_partition");
    let schema = with_partition(&row_schema, &partition_name);
    let result = match table.node() {
        TableNode::Prepared { .. } => prepared(table)?,
        TableNode::GroupWithinPartitions { child, name, n } => {
            crate::partition_grouping::lower_group_within(
                prepared(child)?,
                child.schema().clone(),
                child.keys(),
                name,
                *n,
            )?
        }
        TableNode::MapPartitions {
            child,
            rows_name,
            body,
            bindings,
        } => crate::partition_map::lower(prepared(child)?, child, rows_name, body, bindings)?,
        TableNode::MapGlobals { child } => prepared(child)?,
        TableNode::MapRows {
            child,
            fields,
            bindings,
        } => {
            let input = prepared(child)?;
            let (bound, indices) = bind_external(bind_globals(input.rows, child)?, bindings)?;
            let physical_schema = bound.schema();
            let mut expressions = fields
                .iter()
                .map(|(name, expr)| {
                    Ok((
                        lower_with_bindings(
                            expr,
                            &physical_schema,
                            child.globals().schema(),
                            &indices,
                        )?,
                        name.clone(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            expressions.push((
                column(&bound, physical_schema.fields().len() - 1),
                partition_name,
            ));
            TablePlan {
                rows: Arc::new(ProjectionExec::try_new(expressions, bound)?),
                layout: input.layout,
            }
        }
        TableNode::Filter {
            child,
            predicate,
            keep,
            bindings,
        } => {
            let input = prepared(child)?;
            let (bound, indices) = bind_external(bind_globals(input.rows, child)?, bindings)?;
            let mut predicate = lower_with_bindings(
                predicate,
                &bound.schema(),
                child.globals().schema(),
                &indices,
            )?;
            if !keep {
                predicate = Arc::new(NotExpr::new(predicate));
            }
            let filtered: PhysicalPlan = Arc::new(FilterExec::try_new(predicate, bound)?);
            let offset = filtered.schema().fields().len() - child.schema().fields().len() - 1;
            let expressions: Vec<_> = (offset..filtered.schema().fields().len())
                .map(|i| {
                    (
                        column(&filtered, i),
                        filtered.schema().field(i).name().clone(),
                    )
                })
                .collect();
            TablePlan {
                rows: Arc::new(ProjectionExec::try_new(expressions, filtered)?),
                layout: input.layout,
            }
        }
        TableNode::Explode { child, path } => {
            let input = prepared(child)?;
            let mut rows = input.rows;
            let (index, name) = if path.len() == 1 {
                (rows.schema().index_of(&path[0])?, path[0].clone())
            } else {
                let name = private_name(&rows.schema(), child.globals().schema(), "__vail_explode");
                let mut nested = crate::ScalarIR::Column {
                    name: path[0].clone(),
                };
                for part in &path[1..] {
                    nested = crate::ScalarIR::GetField {
                        value: Arc::new(nested),
                        name: part.clone(),
                    };
                }
                let value = lower_with_bindings(
                    &nested,
                    &rows.schema(),
                    child.globals().schema(),
                    &Default::default(),
                )?;
                let mut expressions = (0..rows.schema().fields().len())
                    .map(|i| (column(&rows, i), rows.schema().field(i).name().clone()))
                    .collect::<Vec<_>>();
                expressions.push((value, name.clone()));
                rows = Arc::new(ProjectionExec::try_new(expressions, rows)?);
                (rows.schema().fields().len() - 1, name)
            };
            let item_type = match rows.schema().field(index).data_type() {
                DataType::List(item) | DataType::LargeList(item) => item.data_type().clone(),
                _ => return Err(TableError::Invalid("explode requires a list column".into())),
            };
            let mut fields = rows.schema().fields().to_vec();
            fields[index] = Arc::new(Field::new(&name, item_type, true));
            let unnested: PhysicalPlan = Arc::new(UnnestExec::new(
                rows,
                vec![ListUnnest {
                    index_in_input_schema: index,
                    depth: 1,
                }],
                vec![],
                Arc::new(Schema::new(fields)),
                UnnestOptions::new().with_null_handling(NullHandling::Drop),
            )?);
            if path.len() == 1 {
                TablePlan {
                    rows: unnested,
                    layout: input.layout,
                }
            } else {
                fn rebuild(
                    dtype: &DataType,
                    path: &[String],
                    value: crate::ScalarIR,
                    exploded: &str,
                ) -> Result<crate::ScalarIR> {
                    if path.is_empty() {
                        return Ok(crate::ScalarIR::Column {
                            name: exploded.into(),
                        });
                    }
                    let DataType::Struct(fields) = dtype else {
                        return Err(TableError::Invalid(
                            "explode path passes through a non-struct".into(),
                        ));
                    };
                    Ok(crate::ScalarIR::Struct {
                        fields: fields
                            .iter()
                            .map(|field| {
                                let field_value = if field.name() == &path[0] {
                                    rebuild(
                                        field.data_type(),
                                        &path[1..],
                                        crate::ScalarIR::GetField {
                                            value: Arc::new(value.clone()),
                                            name: field.name().clone(),
                                        },
                                        exploded,
                                    )?
                                } else {
                                    crate::ScalarIR::GetField {
                                        value: Arc::new(value.clone()),
                                        name: field.name().clone(),
                                    }
                                };
                                Ok((field.name().clone(), field_value))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    })
                }
                let root_index = child.schema().index_of(&path[0])?;
                let root = rebuild(
                    child.schema().field(root_index).data_type(),
                    &path[1..],
                    crate::ScalarIR::Column {
                        name: path[0].clone(),
                    },
                    &name,
                )?;
                let mut expressions = (0..child.schema().fields().len())
                    .map(|i| {
                        if i == root_index {
                            Ok((
                                lower_with_bindings(
                                    &root,
                                    &unnested.schema(),
                                    child.globals().schema(),
                                    &Default::default(),
                                )?,
                                path[0].clone(),
                            ))
                        } else {
                            Ok((column(&unnested, i), child.schema().field(i).name().clone()))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let partition_index = child.schema().fields().len();
                expressions.push((column(&unnested, partition_index), partition_name));
                TablePlan {
                    rows: Arc::new(ProjectionExec::try_new(expressions, unnested)?),
                    layout: input.layout,
                }
            }
        }
        TableNode::Head { n: 0, .. } | TableNode::Tail { n: 0, .. } => TablePlan {
            rows: Arc::new(EmptyExec::new(schema)),
            layout: PartitionLayout::Observed,
        },
        TableNode::Head { child, n } => {
            let input = prepared(child)?;
            TablePlan {
                rows: Arc::new(GlobalLimitExec::new(input.rows, 0, Some(*n))),
                layout: PartitionLayout::Observed,
            }
        }
        TableNode::Tail { child, n } => {
            let input = prepared(child)?;
            TablePlan {
                rows: UnaryExec::create(input.rows, Unary::Tail(*n)),
                layout: PartitionLayout::Observed,
            }
        }
        TableNode::Union { children } => {
            let mut offset = 0u64;
            let mut plans = Vec::with_capacity(children.len());
            let branch_name =
                private_name(&row_schema, table.globals().schema(), "__vail_union_branch");
            let ordinal_name = private_name(
                &row_schema,
                table.globals().schema(),
                "__vail_union_ordinal",
            );
            let partition_name =
                private_name(&row_schema, table.globals().schema(), "__vail_partition");
            for (branch, child) in children.iter().enumerate() {
                let lowered = prepared(child)?;
                let child_parts = child.known_n_partitions().unwrap_or(1);
                let mut aligned = Vec::new();
                for field in row_schema.fields() {
                    let value: Arc<dyn PhysicalExpr> = match child.schema().index_of(field.name()) {
                        Ok(index) => {
                            let target =
                                Arc::new(Field::new(field.name(), field.data_type().clone(), true));
                            Arc::new(datafusion_physical_expr::expressions::CastExpr::new_with_target_field(
                                column(&lowered.rows, index), target, None,
                            ))
                        }
                        Err(_) => Arc::new(Literal::new(ScalarValue::try_from(field.data_type())?)),
                    };
                    aligned.push((value, field.name().clone()));
                }
                aligned.push((
                    column(&lowered.rows, child.schema().fields().len()),
                    partition_name.clone(),
                ));
                let aligned: PhysicalPlan =
                    Arc::new(ProjectionExec::try_new(aligned, lowered.rows)?);
                let numbered = ordinal(aligned);
                let child_schema = numbered.schema();
                let partition_index = row_schema.fields().len();
                let ordinal_index = partition_index + 1;
                let mut fields = (0..row_schema.fields().len())
                    .map(|i| (column(&numbered, i), child_schema.field(i).name().clone()))
                    .collect::<Vec<_>>();
                let partition = if offset == 0 {
                    column(&numbered, partition_index)
                } else {
                    Arc::new(BinaryExpr::new(
                        column(&numbered, partition_index),
                        Operator::Plus,
                        Arc::new(Literal::new(ScalarValue::UInt64(Some(offset)))),
                    ))
                };
                fields.push((partition, partition_name.clone()));
                fields.push((
                    Arc::new(Literal::new(ScalarValue::UInt64(Some(branch as u64)))),
                    branch_name.clone(),
                ));
                fields.push((column(&numbered, ordinal_index), ordinal_name.clone()));
                plans.push(Arc::new(ProjectionExec::try_new(fields, numbered)?) as PhysicalPlan);
                offset = offset
                    .checked_add(child_parts as u64)
                    .ok_or_else(|| TableError::Invalid("union partition count overflow".into()))?;
            }
            let union: PhysicalPlan = UnionExec::try_new(plans)?;
            let coalesced: PhysicalPlan = Arc::new(CoalescePartitionsExec::new(union));
            let order_schema = coalesced.schema();
            let mut ordering = vec![
                PhysicalSortExpr::new(
                    column(&coalesced, row_schema.fields().len() + 1),
                    SortOptions::default(),
                ),
                PhysicalSortExpr::new(
                    column(&coalesced, row_schema.fields().len() + 2),
                    SortOptions::default(),
                ),
            ];
            if !table.keys().is_empty() {
                ordering = order_requirement(
                    &table
                        .keys()
                        .iter()
                        .map(|key| (key.clone(), false))
                        .collect::<Vec<_>>(),
                )?
                .physical_ordering(&order_schema)?
                .into_iter()
                .collect();
                ordering.push(PhysicalSortExpr::new(
                    column(&coalesced, row_schema.fields().len() + 1),
                    SortOptions::default(),
                ));
                ordering.push(PhysicalSortExpr::new(
                    column(&coalesced, row_schema.fields().len() + 2),
                    SortOptions::default(),
                ));
            }
            let sorted: PhysicalPlan = Arc::new(SortExec::new(
                LexOrdering::new(ordering).unwrap(),
                coalesced,
            ));
            let mut fields = (0..row_schema.fields().len())
                .map(|i| (column(&sorted, i), row_schema.field(i).name().clone()))
                .collect::<Vec<_>>();
            let partition = if table.keys().is_empty() {
                column(&sorted, row_schema.fields().len())
            } else {
                Arc::new(Literal::new(ScalarValue::UInt64(Some(0))))
            };
            fields.push((partition, partition_name));
            let union = Arc::new(ProjectionExec::try_new(fields, sorted)?);
            let layout = children
                .iter()
                .map(|child| child.known_n_partitions())
                .collect::<Option<Vec<_>>>()
                .map(|parts| PartitionLayout::Fixed(parts.into_iter().sum()))
                .unwrap_or(PartitionLayout::Observed);
            TablePlan {
                rows: union,
                layout: if table.keys().is_empty() {
                    layout
                } else {
                    PartitionLayout::Fixed(1)
                },
            }
        }
        TableNode::Distinct { child } => {
            let input = prepared(child)?.rows;
            let input_schema = input.schema();
            let group_names = if child.keys().is_empty() {
                child
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
            } else {
                child.keys().to_vec()
            };
            let groups = if group_names.is_empty() {
                vec![(
                    Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
                        as Arc<dyn PhysicalExpr>,
                    "__vail_distinct_group".into(),
                )]
            } else {
                group_names
                    .iter()
                    .map(|name| Ok((column(&input, input_schema.index_of(name)?), name.clone())))
                    .collect::<Result<Vec<_>>>()?
            };
            let mut row_args = Vec::with_capacity(child.schema().fields().len() * 2);
            for field in child.schema().fields() {
                row_args.push(
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(field.name().clone()))))
                        as Arc<dyn PhysicalExpr>,
                );
                row_args.push(column(&input, input_schema.index_of(field.name())?));
            }
            let row = Arc::new(datafusion_physical_expr::ScalarFunctionExpr::try_new(
                datafusion_functions::core::named_struct(),
                row_args,
                &input_schema,
                Arc::new(ConfigOptions::default()),
            )?);
            let representative = datafusion_physical_expr::aggregate::AggregateExprBuilder::new(
                // Retain one Arrow row without DF's scalar compaction, which
                // panics on nested zero-field structs. The take accumulator is
                // bounded to one row per group and preserves nested metadata.
                vail_execution::advanced_collection_aggregates::make_udaf(
                    "take",
                    &row.data_type(&input_schema)?,
                    &serde_json::json!({"n": 1}),
                )?
                .ok_or_else(|| TableError::Invalid("take aggregate unavailable".into()))?,
                vec![row],
            )
            .schema(input_schema.clone())
            .alias("__vail_distinct_row")
            .build()?;
            let grouped: PhysicalPlan = Arc::new(AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(groups),
                vec![Arc::new(representative)],
                vec![None],
                input,
                input_schema,
            )?);
            let row_index = group_names.len().max(1);
            let row_column = vail_execution::expressions::lower(
                &Expr::ArrayIndex {
                    value: Arc::new(Expr::Column {
                        name: grouped.schema().field(row_index).name().clone(),
                    }),
                    index: Arc::new(Expr::Literal {
                        value: serde_json::json!(0),
                        dtype: vail_execution::expressions::Type::Primitive(
                            vail_execution::expressions::PrimitiveType::Int32,
                        ),
                    }),
                },
                &grouped.schema(),
            )?;
            let grouped_schema = grouped.schema();
            let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> =
                Vec::with_capacity(child.schema().fields().len() + 1);
            for field in child.schema().fields() {
                let value = Arc::new(datafusion_physical_expr::ScalarFunctionExpr::try_new(
                    datafusion_functions::core::get_field(),
                    vec![
                        row_column.clone(),
                        Arc::new(Literal::new(ScalarValue::Utf8(Some(field.name().clone())))),
                    ],
                    &grouped_schema,
                    Arc::new(ConfigOptions::default()),
                )?);
                expressions.push((value, field.name().clone()));
            }
            expressions.push((
                Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
                partition_name,
            ));
            let plan = TablePlan {
                rows: Arc::new(ProjectionExec::try_new(expressions, grouped)?),
                layout: PartitionLayout::Fixed(1),
            };
            if table.keys().is_empty() {
                plan
            } else {
                sort(
                    plan,
                    &table
                        .keys()
                        .iter()
                        .map(|key| (key.clone(), false))
                        .collect::<Vec<_>>(),
                )?
            }
        }
        TableNode::KeyBy { child, keys } => sort(
            prepared(child)?,
            &keys.iter().map(|k| (k.clone(), false)).collect::<Vec<_>>(),
        )?,
        TableNode::Join {
            left,
            right,
            kind,
            join_key,
        } => crate::joins::lower(table, left, right, *kind, *join_key)?,
        TableNode::MultiWayZipJoin {
            tables,
            data_field_name,
        } => crate::zip_join::lower(tables, table, data_field_name)?,
        TableNode::OrderBy { child, keys } => sort(prepared(child)?, keys)?,
        _ => {
            return Err(TableError::Invalid(
                "operation is not a physical semantic extension".into(),
            ));
        }
    };
    Ok(result)
}
/// Join captured singleton plans without exposing their field names in the row
/// or local-global namespaces. Physical indices remain explicit after each join.
pub(crate) fn bind_external(
    mut input: PhysicalPlan,
    bindings: &GlobalBindings,
) -> Result<(PhysicalPlan, std::collections::BTreeMap<String, usize>)> {
    let mut indices = std::collections::BTreeMap::<String, usize>::new();
    for (reference, globals) in bindings {
        let plan = crate::partitioned::reset_plan_states(globals.lower()?)?;
        let value = Expr::Struct {
            fields: globals
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.name().clone(),
                        Expr::Global {
                            name: field.name().clone(),
                        },
                    )
                })
                .collect(),
        };
        let expression = lower_with_global_columns(&value, &plan.schema(), globals.schema())?;
        let name = private_name(&input.schema(), &Schema::empty(), "__vail_external_globals");
        let singleton = Arc::new(ProjectionExec::try_new(vec![(expression, name)], plan)?);
        input = cross_join(singleton, input)?;
        for index in indices.values_mut() {
            *index += 1;
        }
        indices.insert(reference.clone(), 0);
    }
    Ok((input, indices))
}

/// DF55 CrossJoinExec rebuilds its left values through scalar conversion,
/// which drops list typing metadata. Its nested-loop join uses Arrow take and
/// provides the same Cartesian relation without changing the representation.
pub(crate) fn cross_join(
    left: PhysicalPlan,
    right: PhysicalPlan,
) -> datafusion_common::Result<PhysicalPlan> {
    let metadata = left
        .schema()
        .fields()
        .iter()
        .chain(right.schema().fields())
        .any(|f| vail_ordering::has_type_metadata(f.data_type()));
    if metadata {
        Ok(Arc::new(
            datafusion_physical_plan::joins::NestedLoopJoinExec::try_new(
                left,
                right,
                None,
                &datafusion_expr::JoinType::Inner,
                None,
            )?,
        ))
    } else {
        Ok(Arc::new(CrossJoinExec::new(left, right)))
    }
}

pub(crate) fn bind_globals(rows: PhysicalPlan, table: &Arc<TableIR>) -> Result<PhysicalPlan> {
    if table.globals().schema().fields().is_empty() {
        return Ok(rows);
    }
    // CrossJoin builds its left input: exactly one row, including for empty tables.
    Ok(cross_join(
        crate::partitioned::reset_plan_states(table.globals().lower()?)?,
        rows,
    )?)
}
pub(crate) fn sort(input: TablePlan, keys: &[(String, bool)]) -> Result<TablePlan> {
    if keys.is_empty() {
        return Ok(input);
    }
    let schema = input.rows.schema();
    let requirement = order_requirement(keys)?;
    let mut ordering = requirement
        .physical_ordering(&schema)?
        .into_iter()
        .collect::<Vec<_>>();
    let numbered = ordinal(input.rows);
    ordering.push(PhysicalSortExpr::new(
        column(&numbered, numbered.schema().fields().len() - 1),
        SortOptions::default(),
    ));
    let sorted: PhysicalPlan =
        Arc::new(SortExec::new(LexOrdering::new(ordering).unwrap(), numbered));
    let mut expressions = (0..schema.fields().len() - 1)
        .map(|i| (column(&sorted, i), schema.field(i).name().clone()))
        .collect::<Vec<_>>();
    expressions.push((
        Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
        schema.field(schema.fields().len() - 1).name().clone(),
    ));
    Ok(TablePlan {
        rows: Arc::new(ProjectionExec::try_new(expressions, sorted)?),
        layout: PartitionLayout::Fixed(1),
    })
}

/// Adapt completed DataFusion batches to the storage boundary's partition events.
/// No TableIR operations are interpreted here.
pub fn execute(table: &Arc<TableIR>) -> crate::BatchStream {
    crate::partitioned::execute(table)
}

/// Compile the complete logical query using the caller's action resources.
/// Supply memory/spill limits here, before singleton parameters are resolved,
/// so initializers and the final data relation share one runtime environment.
pub async fn physical_with_context(
    table: &TableRef,
    context: Arc<TaskContext>,
) -> Result<PhysicalPlan> {
    crate::logical::physical_with_context(table, context).await
}

/// Compile a complete table action with Vail's default action resources.
pub async fn physical(table: &TableRef) -> Result<PhysicalPlan> {
    crate::logical::physical(table).await
}
pub(crate) fn repeatable_node(plan: &PhysicalPlan) -> bool {
    plan.as_ref().is::<SourceExec>()
        || plan.as_ref().is::<RangePartitionExec>()
        || plan.as_ref().is::<UnaryExec>()
}
