//! Independent physical sources for table interval reads.
use crate::read_metrics::register as register_read_metrics;
use crate::partitioned::PreparedTable;
use crate::plan::{self, PhysicalPlan, source_partition};
use crate::{Result, ScalarIR, TableError, TableNode, TableRef};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::{DataFusionError, ScalarValue, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    filter::FilterExec,
    stream::RecordBatchStreamAdapter,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
};
use futures::TryStreamExt;
use intervaltree::IntervalTree;
use std::cmp::Ordering;
use std::{fmt, sync::Arc};
use vail_execution::expressions::{Expr, Type, lower_with_global_columns};
use vail_storage::{
    fragment_index::{Endpoint, IndexBinding, KeyValue, QueryInterval, make_typed_key_comparators},
    local_table::LocalRead,
};

#[derive(Debug)]
struct IntervalSourceExec {
    metrics: ExecutionPlanMetricsSet,
    source: Arc<LocalRead>,
    query: QueryInterval,
    rows: Option<std::ops::Range<u64>>,
    row_schema: SchemaRef,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}
impl IntervalSourceExec {
    fn new(
        source: Arc<LocalRead>,
        query: QueryInterval,
        row_schema: SchemaRef,
        schema: SchemaRef,
    ) -> Self {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            metrics: ExecutionPlanMetricsSet::new(),
            source,
            query,
            rows: None,
            row_schema,
            schema,
            cache,
        }
    }
}
impl DisplayAs for IntervalSourceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntervalSourceExec")
    }
}
impl ExecutionPlan for IntervalSourceExec {
    fn name(&self) -> &'static str {
        "IntervalSourceExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn metrics(&self) -> Option<MetricsSet> { Some(self.metrics.clone_inner()) }
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
            return Err(DataFusionError::Plan(
                "IntervalSourceExec has no children".into(),
            ));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "interval source requires partition zero".into(),
            ));
        }
        let source = self.source.with_scan_metrics(register_read_metrics(&self.metrics, partition));
        let mut batches = if let Some(rows) = self.rows.clone() {
            source.projected_interval_batches_in_rows(
                std::slice::from_ref(&self.query),
                rows,
                source.field_ids(),
            )
        } else {
            source
                .interval_batches(std::slice::from_ref(&self.query))
        };
        let row_schema = self.row_schema.clone();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
            while let Some((_, batch)) = batches.try_next().await.map_err(storage_error)? {
                let batch = vail_storage::local_table::normalize(&batch, row_schema.clone()).map_err(storage_error)?;
                let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(UInt64Array::from_value(0, batch.num_rows())));
                    yield RecordBatch::try_new(schema.clone(), columns)?;
                }
            },
        )))
    }
}

pub(crate) fn source_interval_in_rows(
    table: &TableRef,
    source: Arc<LocalRead>,
    query: QueryInterval,
    rows: std::ops::Range<u64>,
) -> PhysicalPlan {
    let row_schema = table.schema().clone();
    let schema = plan::physical_schema(table);
    let mut exec = IntervalSourceExec::new(source, query, row_schema, schema);
    exec.rows = Some(rows);
    Arc::new(exec)
}

struct IntervalSelection {
    source_table: TableRef,
    bounds: Vec<IntervalBounds>,
    queries: Vec<QueryInterval>,
    full_key: bool,
    candidates: Option<Vec<(usize, Option<Vec<usize>>)>>,
}

async fn select_intervals(
    child: &TableRef,
    intervals: &[ScalarIR],
    point_type: &Type,
    filter: bool,
) -> Result<IntervalSelection> {
    let (source_table, selected_source_partitions) = match child.node() {
        TableNode::Read { .. } => (child, None),
        TableNode::FilterPartitions {
            child: source_table,
            parts,
            keep,
        } if matches!(source_table.node(), TableNode::Read { .. }) => {
            let source_count = source_table.known_n_partitions().ok_or_else(|| {
                TableError::Invalid("partition selection requires a known source count".into())
            })?;
            if let Some(invalid) = parts.iter().find(|&&partition| partition >= source_count) {
                return Err(TableError::Invalid(format!(
                    "_filter_partitions: no partition with index {invalid} (num partitions = {source_count})"
                )));
            }
            let selected = (0..source_count)
                .filter(|partition| parts.contains(partition) == *keep)
                .collect::<Vec<_>>();
            (source_table, Some(selected))
        }
        _ => {
            return Err(TableError::Invalid(
                "interval reads require a table read source".into(),
            ));
        }
    };
    let TableNode::Read { source } = source_table.node() else {
        return Err(TableError::Invalid(
            "interval reads require a table read source".into(),
        ));
    };
    let binding = source.index_binding().map_err(TableError::Storage)?;
    let keys = child.keys();
    let mut bounds = Vec::with_capacity(intervals.len());
    for interval in intervals {
        let literal = eval_scalar(interval)?;
        let ScalarValue::Struct(fields) = literal else {
            return Err(TableError::Invalid(
                "interval literal cannot be missing".into(),
            ));
        };
        if fields.is_null(0) {
            return Err(TableError::Invalid(
                "interval literal cannot be missing".into(),
            ));
        }
        if fields.num_columns() != 4 {
            return Err(TableError::Invalid(
                "interval literal has invalid storage shape".into(),
            ));
        }
        let field = |index| {
            ScalarValue::try_from_array(fields.column(index).as_ref(), 0).map_err(TableError::from)
        };
        bounds.push(IntervalBounds {
            lower: field(0)?,
            upper: field(2)?,
            lower_inclusive: !scalar_boolean(&field(1)?)?,
            upper_inclusive: scalar_boolean(&field(3)?)?,
            sources: vec![interval.clone()],
        });
    }
    let full_key =
        keys.len() == binding.key_fields.len() && point_component_count(point_type) == keys.len();
    let bounds = normalize_bounds(bounds, point_type, &binding)?;
    let queries = if full_key {
        bounds
            .iter()
            .map(|b| make_query(b, point_type, &binding))
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![QueryInterval::default(); bounds.len()]
    };
    let candidates = if filter {
        // Full-key intervals can use the fragment index directly. Prefix
        // intervals cannot form a full-key query, so compare them against the
        // certified per-partition key bounds instead of selecting every source
        // partition. The DataFusion predicate below still removes false rows.
        let mut candidates = if full_key {
            source
                .candidate_partitions(&queries)
                .await
                .map_err(TableError::Storage)?
                .into_iter()
                .map(|partition| (partition, None))
                .collect::<Vec<_>>()
        } else {
            candidate_prefix_partitions(source, &bounds, point_type, &binding)?
        };
        if let Some(selected) = &selected_source_partitions {
            candidates.retain(|(partition, _)| selected.contains(partition));
        }
        Some(candidates)
    } else {
        None
    };
    Ok(IntervalSelection {
        source_table: source_table.clone(),
        bounds,
        queries,
        full_key,
        candidates,
    })
}

/// Count selected partitions using snapshot/index metadata, without compiling
/// residual row filters or creating physical read plans.
pub(crate) async fn n_partitions(
    child: &TableRef,
    intervals: &[ScalarIR],
    point_type: &Type,
    filter: bool,
) -> Result<usize> {
    if intervals.is_empty() {
        return Ok(0);
    }
    let selected = select_intervals(child, intervals, point_type, filter).await?;
    Ok(selected
        .candidates
        .as_ref()
        .map_or(selected.queries.len(), Vec::len))
}

pub(crate) async fn prepare(
    child: &TableRef,
    intervals: &[ScalarIR],
    point_type: &Type,
    filter: bool,
) -> Result<PreparedTable> {
    let schema = plan::physical_schema(child);
    if intervals.is_empty() {
        return Ok(PreparedTable {
            parts: vec![],
            counts: vec![],
            schema,
        });
    }
    let IntervalSelection {
        source_table,
        bounds,
        queries,
        full_key,
        candidates,
    } = select_intervals(child, intervals, point_type, filter).await?;
    let TableNode::Read { source } = source_table.node() else {
        unreachable!()
    };
    let keys = child.keys();
    if let Some(candidates) = candidates {
        let mut parts = Vec::with_capacity(candidates.len());
        let mut counts = Vec::with_capacity(candidates.len());
        for (part, matching_intervals) in candidates {
            let rows = source_partition(&source_table, part)?;
            let residual_intervals = matching_intervals
                .map(|indices| {
                    indices
                        .into_iter()
                        .flat_map(|index| bounds[index].sources.iter().cloned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| intervals.to_vec());
            let predicate = union_predicate(&residual_intervals, keys, point_type)?;
            parts.push(apply_filter(rows, &predicate, &source_table)?);
            counts.push(None);
        }
        return Ok(PreparedTable {
            parts,
            counts,
            schema,
        });
    }

    let mut parts = Vec::with_capacity(queries.len());
    let mut counts = Vec::with_capacity(queries.len());
    for (query, bound) in queries.into_iter().zip(bounds) {
        let candidate_ids = source
            .candidate_partitions(std::slice::from_ref(&query))
            .await
            .map_err(TableError::Storage)?;
        let rows = if full_key {
            Arc::new(IntervalSourceExec::new(
                source.clone(),
                query,
                child.schema().clone(),
                schema.clone(),
            )) as PhysicalPlan
        } else {
            // A prefix cannot be expanded to full composite bounds from index
            // metadata alone. Read ordered source partitions and filter rows.
            let physical = (0..source.partition_rows().len())
                .map(|p| source_partition(child, p))
                .collect::<Result<Vec<_>>>()?;
            PreparedTable {
                parts: physical,
                counts: source.partition_rows().iter().copied().map(Some).collect(),
                schema: schema.clone(),
            }
            .ordered()
        };
        let predicate = union_predicate(&bound.sources, keys, point_type)?;
        parts.push(apply_filter(rows, &predicate, child)?);
        counts.push(if candidate_ids.is_empty() {
            Some(0)
        } else {
            None
        });
    }
    Ok(PreparedTable {
        parts,
        counts,
        schema,
    })
}

/// Apply `_filter_partitions` to the dense output partition IDs produced by
/// an interval read. The underlying read may have skipped source partitions,
/// so source IDs and output IDs are deliberately kept distinct here.
pub(crate) async fn prepare_output_partitions(
    child: &TableRef,
    intervals: &[ScalarIR],
    point_type: &Type,
    parts: &[usize],
    keep: bool,
) -> Result<PreparedTable> {
    let input = prepare(child, intervals, point_type, true).await?;
    if let Some(invalid) = parts
        .iter()
        .find(|&&partition| partition >= input.parts.len())
    {
        return Err(TableError::Invalid(format!(
            "_filter_partitions: no partition with index {invalid} (num partitions = {})",
            input.parts.len()
        )));
    }
    let (selected, counts) = input
        .parts
        .into_iter()
        .zip(input.counts)
        .enumerate()
        .filter(|(partition, _)| parts.contains(partition) == keep)
        .map(|(_, pair)| pair)
        .unzip();
    Ok(PreparedTable {
        parts: selected,
        counts,
        schema: input.schema,
    })
}

struct IntervalBounds {
    lower: ScalarValue,
    upper: ScalarValue,
    lower_inclusive: bool,
    upper_inclusive: bool,
    sources: Vec<ScalarIR>,
}

fn eval_scalar(expr: &Expr) -> Result<ScalarValue> {
    let schema = Schema::empty();
    let expression = lower_with_global_columns(expr, &schema, &schema)?;
    let batch = RecordBatch::try_new_with_options(
        Arc::new(schema),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    let value = expression.evaluate(&batch)?;
    match value {
        ColumnarValue::Scalar(value) => Ok(value),
        ColumnarValue::Array(array) => Ok(ScalarValue::try_from_array(array.as_ref(), 0)?),
    }
}
fn scalar_boolean(value: &ScalarValue) -> Result<bool> {
    match value {
        ScalarValue::Boolean(Some(value)) => Ok(*value),
        other => Err(TableError::Invalid(format!(
            "interval inclusivity must be a nonmissing bool, got {other:?}"
        ))),
    }
}
fn point_component_count(point_type: &Type) -> usize {
    match point_type {
        // Arrays and tuples are logical scalar key types even though their
        // Arrow storage is nested. Only an explicit Hail struct represents a
        // multi-column table-key prefix.
        Type::Primitive(_) | Type::Locus { .. } | Type::Tuple { .. } | Type::Array { .. } => 1,
        Type::Struct { r#struct } => r#struct.len(),
        _ => 0,
    }
}

fn make_query(
    bounds: &IntervalBounds,
    point_type: &Type,
    binding: &IndexBinding,
) -> Result<QueryInterval> {
    let lower = endpoint(&bounds.lower, point_type, binding, bounds.lower_inclusive)?;
    let upper = endpoint(&bounds.upper, point_type, binding, bounds.upper_inclusive)?;
    Ok(QueryInterval { lower, upper })
}
fn endpoint(
    value: &ScalarValue,
    point_type: &Type,
    binding: &IndexBinding,
    inclusive: bool,
) -> Result<Option<Endpoint>> {
    // A null struct scalar can materialize as a one-row StructArray whose
    // children are all null. Check scalar validity before reading the child
    // keys: for composite keys that child tuple would otherwise look like a
    // concrete all-missing key (which sorts after every ordinary key).
    if value.is_null() {
        return Ok(None);
    }
    let array = value.to_array_of_size(1)?;
    if array.is_null(0) {
        // Missing interval endpoints are unbounded. Treating them as actual
        // missing keys makes one-sided predicates such as key < x prune every
        // partition when missing keys sort last.
        return Ok(None);
    }
    let children: Vec<(ArrayRef, Type)> = match point_type {
        Type::Primitive(_) | Type::Locus { .. } | Type::Tuple { .. } | Type::Array { .. } => {
            vec![(array, point_type.clone())]
        }
        Type::Struct { r#struct } => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or_else(|| TableError::Invalid("interval endpoint must be a struct".into()))?;
            structure
                .columns()
                .iter()
                .cloned()
                .zip(r#struct.iter().map(|(_, t)| t.clone()))
                .collect()
        }
        _ => return Ok(None),
    };
    if children.len() != binding.key_fields.len() {
        return Ok(None);
    }
    let key = children
        .into_iter()
        .zip(&binding.key_fields)
        .map(|((array, _logical), field)| key_value(&array, field))
        .collect::<Result<Vec<_>>>()?;
    let typed = key
        .iter()
        .zip(&binding.key_fields)
        .map(|(value, field)| match value {
            KeyValue::ArrowIpc(bytes) => {
                let array = vail_storage::fragment_index::decode_typed_key_component(bytes)
                    .map_err(|error| TableError::Invalid(error.to_string()))?;
                if array.data_type() != &field.physical_type {
                    return Err(TableError::Invalid(
                        "interval endpoint type differs from indexed key type".into(),
                    ));
                }
                Ok(array)
            }
            _ => Err(TableError::Invalid(
                "interval endpoint does not use typed Arrow IPC".into(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let options = binding
        .key_fields
        .iter()
        .take(typed.len())
        .map(|field| arrow_schema::SortOptions {
            descending: field.descending,
            nulls_first: !field.nulls_last,
        })
        .collect::<Vec<_>>();
    vail_storage::fragment_index::compare_typed_key_components(&typed, &typed, &options)
        .map_err(|error| TableError::Invalid(error.to_string()))?;
    Ok(Some(Endpoint { key, inclusive }))
}
fn key_value(array: &ArrayRef, field: &vail_storage::fragment_index::KeyField) -> Result<KeyValue> {
    let array =
        vail_storage::fragment_index::normalize_typed_key_component(array, &field.physical_type)
            .map_err(|error| TableError::Invalid(error.to_string()))?;
    Ok(KeyValue::ArrowIpc(
        vail_storage::fragment_index::encode_typed_key_component(&array)
            .map_err(|e| TableError::Invalid(e.to_string()))?,
    ))
}

fn normalize_bounds(
    mut bounds: Vec<IntervalBounds>,
    point_type: &Type,
    binding: &IndexBinding,
) -> Result<Vec<IntervalBounds>> {
    let mut keyed = bounds
        .drain(..)
        .map(|bound| {
            let lower = endpoint_key(&bound.lower, point_type, binding)?;
            let upper = endpoint_key(&bound.upper, point_type, binding)?;
            Ok((lower, upper, bound))
        })
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|a, b| {
        compare_lower(&a.0, &b.0, binding)
            .then_with(|| b.2.lower_inclusive.cmp(&a.2.lower_inclusive))
    });
    let mut merged: Vec<(Option<Vec<KeyValue>>, Option<Vec<KeyValue>>, IntervalBounds)> =
        Vec::new();
    for (lower, upper, mut current) in keyed {
        if let Some((_, prior_upper, prior)) = merged.last_mut() {
            let relation = compare_upper_lower(prior_upper, &lower, binding);
            if relation == std::cmp::Ordering::Greater
                || (relation == std::cmp::Ordering::Equal
                    // Keep adjacent intervals as distinct output partitions.
                    // Only merge a shared endpoint when both intervals include
                    // it, since otherwise the key is not duplicated.
                    && (prior.upper_inclusive && current.lower_inclusive))
            {
                let end_relation = compare_upper(&upper, prior_upper, binding);
                if end_relation == std::cmp::Ordering::Greater {
                    prior.upper = current.upper.clone();
                    prior.upper_inclusive = current.upper_inclusive;
                    *prior_upper = upper;
                } else if end_relation == std::cmp::Ordering::Equal {
                    prior.upper_inclusive |= current.upper_inclusive;
                }
                prior.sources.append(&mut current.sources);
                continue;
            }
        }
        merged.push((lower, upper, current));
    }
    Ok(merged.into_iter().map(|(_, _, bound)| bound).collect())
}

fn endpoint_key(
    value: &ScalarValue,
    point_type: &Type,
    binding: &IndexBinding,
) -> Result<Option<Vec<KeyValue>>> {
    if value.is_null() {
        return Ok(None);
    }
    let array = value.to_array_of_size(1)?;
    if array.is_null(0) {
        return Ok(None);
    }
    let components: Vec<(ArrayRef, Type)> = match point_type {
        Type::Primitive(_) | Type::Locus { .. } | Type::Tuple { .. } | Type::Array { .. } => {
            vec![(array, point_type.clone())]
        }
        Type::Struct { r#struct } => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or_else(|| TableError::Invalid("interval endpoint must be a struct".into()))?;
            structure
                .columns()
                .iter()
                .cloned()
                .zip(r#struct.iter().map(|(_, ty)| ty.clone()))
                .collect()
        }
        _ => {
            return Err(TableError::Invalid(
                "unsupported interval point type".into(),
            ));
        }
    };
    if components.len() > binding.key_fields.len() {
        return Err(TableError::Invalid(
            "interval point has more fields than the table key".into(),
        ));
    }
    components
        .into_iter()
        .zip(&binding.key_fields)
        .map(|((array, _logical), field)| key_value(&array, field))
        .collect::<Result<Vec<_>>>()
        .map(Some)
}
fn compare_lower(
    a: &Option<Vec<KeyValue>>,
    b: &Option<Vec<KeyValue>>,
    binding: &IndexBinding,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => {
            let common = compare_prefix(a, b, binding);
            if common != std::cmp::Ordering::Equal {
                common
            } else {
                a.len().cmp(&b.len())
            }
        }
    }
}
fn compare_upper_lower(
    upper: &Option<Vec<KeyValue>>,
    lower: &Option<Vec<KeyValue>>,
    binding: &IndexBinding,
) -> std::cmp::Ordering {
    match (upper, lower) {
        (None, None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(upper), Some(lower)) => compare_as_upper_lower(upper, lower, binding),
    }
}
fn compare_upper(
    a: &Option<Vec<KeyValue>>,
    b: &Option<Vec<KeyValue>>,
    binding: &IndexBinding,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(a), Some(b)) => compare_as_upper(a, b, binding),
    }
}
fn compare_as_upper_lower(
    upper: &[KeyValue],
    lower: &[KeyValue],
    binding: &IndexBinding,
) -> std::cmp::Ordering {
    let common = compare_key(
        &upper[..upper.len().min(lower.len())],
        &lower[..upper.len().min(lower.len())],
        binding,
    );
    if common != std::cmp::Ordering::Equal {
        common
    } else if upper.len() == lower.len() {
        std::cmp::Ordering::Equal
    } else {
        // An upper prefix includes every full key sharing that prefix, while a
        // lower prefix starts before every such full key.
        std::cmp::Ordering::Greater
    }
}
fn compare_as_upper(a: &[KeyValue], b: &[KeyValue], binding: &IndexBinding) -> std::cmp::Ordering {
    let common_len = a.len().min(b.len());
    let common = compare_key(&a[..common_len], &b[..common_len], binding);
    if common != std::cmp::Ordering::Equal {
        common
    } else if a.len() == b.len() {
        std::cmp::Ordering::Equal
    } else if a.len() < b.len() {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Less
    }
}

fn candidate_prefix_partitions(
    source: &LocalRead,
    bounds: &[IntervalBounds],
    point_type: &Type,
    binding: &IndexBinding,
) -> Result<Vec<(usize, Option<Vec<usize>>)>> {
    let partition_bounds = source
        .partition_typed_key_bounds()
        .map_err(TableError::Storage)?;
    let typed_bounds = bounds
        .iter()
        .map(|interval| {
            let decode =
                |value: &ScalarValue| -> Result<Option<vail_storage::fragment_index::TypedKey>> {
                    endpoint_key(value, point_type, binding)?
                        .map(|key| decode_typed_prefix(binding, &key))
                        .transpose()
                };
            Ok((decode(&interval.lower)?, decode(&interval.upper)?))
        })
        .collect::<Result<Vec<_>>>()?;
    let index = PrefixPartitionIndex::new(&partition_bounds, binding)?;
    let mut matching_by_partition = vec![Vec::new(); partition_bounds.len()];
    for (interval_index, ((lower, upper), interval)) in typed_bounds.iter().zip(bounds).enumerate()
    {
        for partition in index.query(lower.as_ref(), upper.as_ref(), interval, binding)? {
            matching_by_partition[partition].push(interval_index);
        }
    }
    Ok(matching_by_partition
        .into_iter()
        .enumerate()
        .filter_map(|(partition, intervals)| {
            (!intervals.is_empty()).then_some((partition, Some(intervals)))
        })
        .collect())
}

/// Interval tree for indexed partition bounds. Typed Arrow endpoints are
/// assigned ordinal ranks, then the generic interval tree indexes those ranks.
struct PrefixPartitionIndex {
    endpoints: Vec<vail_storage::fragment_index::TypedKey>,
    tree: IntervalTree<usize, usize>,
    partitions: Vec<
        Option<(
            vail_storage::fragment_index::TypedKey,
            vail_storage::fragment_index::TypedKey,
        )>,
    >,
}

impl PrefixPartitionIndex {
    fn new(
        bounds: &[Option<(
            vail_storage::fragment_index::TypedKey,
            vail_storage::fragment_index::TypedKey,
        )>],
        binding: &IndexBinding,
    ) -> Result<Self> {
        let mut endpoints = bounds
            .iter()
            .filter_map(Option::as_ref)
            .flat_map(|(lower, upper)| [lower.clone(), upper.clone()])
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            return Ok(Self {
                endpoints,
                tree: std::iter::empty::<(std::ops::Range<usize>, usize)>().collect(),
                partitions: bounds.to_vec(),
            });
        }
        let keys = endpoints
            .iter()
            .map(|key| key.components.clone())
            .collect::<Vec<_>>();
        let columns = concatenate_typed_keys(&keys, binding.key_fields.len())?;
        let comparators =
            make_typed_key_comparators(&columns, &columns, &typed_sort_options(binding))
                .map_err(|error| TableError::Invalid(error.to_string()))?;
        let mut order = (0..endpoints.len()).collect::<Vec<_>>();
        order.sort_by(|&left, &right| {
            for comparator in &comparators {
                let ordering = comparator(left, right);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });
        endpoints = order
            .into_iter()
            .map(|index| endpoints[index].clone())
            .collect();
        let mut intervals = Vec::new();
        for (partition, bounds) in bounds.iter().enumerate() {
            let Some((lower, upper)) = bounds else {
                continue;
            };
            let start = full_lower_bound(&endpoints, lower, binding)?;
            let end = full_upper_bound(&endpoints, upper, binding)?;
            if start < end {
                intervals.push((start..end, partition));
            }
        }
        Ok(Self {
            endpoints,
            tree: intervals.into_iter().collect(),
            partitions: bounds.to_vec(),
        })
    }

    fn query(
        &self,
        lower: Option<&vail_storage::fragment_index::TypedKey>,
        upper: Option<&vail_storage::fragment_index::TypedKey>,
        interval: &IntervalBounds,
        binding: &IndexBinding,
    ) -> Result<Vec<usize>> {
        if self.endpoints.is_empty() {
            return Ok(Vec::new());
        }
        let start = lower
            .map(|lower| prefix_lower_bound(&self.endpoints, lower, binding))
            .transpose()?
            .unwrap_or(0);
        let mut end = upper
            .map(|upper| prefix_upper_bound(&self.endpoints, upper, binding))
            .transpose()?
            .unwrap_or(self.endpoints.len());
        if start >= self.endpoints.len() {
            return Ok(Vec::new());
        }
        if start >= end {
            end = start + 1;
        }
        let mut result = self
            .tree
            .query(start..end)
            .filter_map(|entry| {
                let partition = entry.value;
                let (minimum, maximum) = self.partitions[partition].as_ref()?;
                match prefix_interval_overlaps(lower, upper, interval, minimum, maximum, binding) {
                    Ok(true) => Some(Ok(partition)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        result.sort_unstable();
        Ok(result)
    }
}

fn full_lower_bound(
    endpoints: &[vail_storage::fragment_index::TypedKey],
    key: &vail_storage::fragment_index::TypedKey,
    binding: &IndexBinding,
) -> Result<usize> {
    let mut lower = 0;
    let mut upper = endpoints.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if compare_typed_prefix(&endpoints[middle].components, &key.components, binding)?
            == Ordering::Less
        {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    Ok(lower)
}

fn full_upper_bound(
    endpoints: &[vail_storage::fragment_index::TypedKey],
    key: &vail_storage::fragment_index::TypedKey,
    binding: &IndexBinding,
) -> Result<usize> {
    let mut lower = 0;
    let mut upper = endpoints.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if compare_typed_prefix(&endpoints[middle].components, &key.components, binding)?
            != Ordering::Greater
        {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    Ok(lower)
}

fn prefix_lower_bound(
    endpoints: &[vail_storage::fragment_index::TypedKey],
    key: &vail_storage::fragment_index::TypedKey,
    binding: &IndexBinding,
) -> Result<usize> {
    let mut lower = 0;
    let mut upper = endpoints.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if compare_typed_as_upper_lower(&endpoints[middle].components, &key.components, binding)?
            == Ordering::Less
        {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    Ok(lower)
}

fn prefix_upper_bound(
    endpoints: &[vail_storage::fragment_index::TypedKey],
    key: &vail_storage::fragment_index::TypedKey,
    binding: &IndexBinding,
) -> Result<usize> {
    let mut lower = 0;
    let mut upper = endpoints.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if compare_typed_as_upper_lower(&key.components, &endpoints[middle].components, binding)?
            != Ordering::Less
        {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    Ok(lower)
}

fn compare_typed_as_upper_lower(
    upper: &[ArrayRef],
    lower: &[ArrayRef],
    binding: &IndexBinding,
) -> Result<Ordering> {
    let common = compare_typed_prefix(upper, lower, binding)?;
    Ok(if common != Ordering::Equal {
        common
    } else if upper.len() == lower.len() {
        Ordering::Equal
    } else {
        Ordering::Greater
    })
}

fn prefix_interval_overlaps(
    lower: Option<&vail_storage::fragment_index::TypedKey>,
    upper: Option<&vail_storage::fragment_index::TypedKey>,
    interval: &IntervalBounds,
    partition_min: &vail_storage::fragment_index::TypedKey,
    partition_max: &vail_storage::fragment_index::TypedKey,
    binding: &IndexBinding,
) -> Result<bool> {
    if let Some(lower) = lower {
        let prefix = compare_typed_prefix(&lower.components, &partition_max.components, binding)?;
        let ordering =
            prefix.then_with(|| lower.components.len().cmp(&partition_max.components.len()));
        let starts_after_partition = ordering == Ordering::Greater
            || (ordering == Ordering::Equal && !interval.lower_inclusive)
            || (!interval.lower_inclusive
                && lower.components.len() < partition_max.components.len()
                && prefix == Ordering::Equal);
        if starts_after_partition {
            return Ok(false);
        }
    }
    if let Some(upper) = upper {
        let prefix = compare_typed_prefix(&upper.components, &partition_min.components, binding)?;
        let ordering =
            prefix.then_with(|| partition_min.components.len().cmp(&upper.components.len()));
        let ends_before_partition = ordering == Ordering::Less
            || (ordering == Ordering::Equal && !interval.upper_inclusive)
            || (!interval.upper_inclusive
                && upper.components.len() < partition_min.components.len()
                && prefix == Ordering::Equal);
        if ends_before_partition {
            return Ok(false);
        }
    }
    Ok(true)
}

fn typed_sort_options(binding: &IndexBinding) -> Vec<arrow_schema::SortOptions> {
    binding
        .key_fields
        .iter()
        .map(|field| arrow_schema::SortOptions {
            descending: field.descending,
            nulls_first: !field.nulls_last,
        })
        .collect()
}

fn concatenate_typed_keys(keys: &[Vec<ArrayRef>], arity: usize) -> Result<Vec<ArrayRef>> {
    (0..arity)
        .map(|component| {
            let arrays = keys
                .iter()
                .map(|key| key[component].as_ref())
                .collect::<Vec<_>>();
            arrow_select::concat::concat(&arrays).map_err(TableError::from)
        })
        .collect()
}

#[cfg(test)]
mod prefix_partition_index_tests {
    use super::*;
    use vail_storage::fragment_index::{KeyField, KeyType, TypedKey};

    fn binding() -> IndexBinding {
        IndexBinding {
            table_uuid: uuid::Uuid::from_u128(1),
            snapshot_id: 1,
            schema_id: 1,
            comparator_version: "test".into(),
            live_files_sha256: "test".into(),
            key_fields: (0..2)
                .map(|field_id| KeyField {
                    field_id,
                    data_type: KeyType::Int32,
                    physical_type: arrow_schema::DataType::Int32,
                    descending: false,
                    nulls_last: true,
                })
                .collect(),
        }
    }

    fn typed(values: &[i32]) -> TypedKey {
        TypedKey {
            components: values
                .iter()
                .map(|&value| Arc::new(arrow_array::Int32Array::from(vec![value])) as ArrayRef)
                .collect(),
        }
    }

    fn query(
        lower: Option<i32>,
        upper: Option<i32>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    ) -> (Option<TypedKey>, Option<TypedKey>, IntervalBounds) {
        (
            lower.map(|value| typed(&[value])),
            upper.map(|value| typed(&[value])),
            IntervalBounds {
                lower: ScalarValue::Int32(lower),
                upper: ScalarValue::Int32(upper),
                lower_inclusive,
                upper_inclusive,
                sources: vec![],
            },
        )
    }

    #[test]
    fn prefix_interval_predicate_growth_is_linear() {
        // Position predicates may produce one interval per contig. Guard the
        // generated tree size: null checks must not recursively duplicate the
        // Boolean combinations of already-checked key comparisons.
        let point_type = Type::Struct {
            r#struct: vec![(
                "a".into(),
                Type::Primitive(vail_execution::expressions::PrimitiveType::Int32),
            )],
        };
        let intervals = (0..84)
            .map(|i| Expr::Interval {
                start: Arc::new(Expr::Struct {
                    fields: vec![(
                        "a".into(),
                        Expr::RuntimeLiteral {
                            value: ScalarValue::Int32(Some(i * 10)),
                        },
                    )],
                }),
                end: Arc::new(Expr::Struct {
                    fields: vec![(
                        "a".into(),
                        Expr::RuntimeLiteral {
                            value: ScalarValue::Int32(Some(i * 10 + 5)),
                        },
                    )],
                }),
                includes_start: Arc::new(Expr::RuntimeLiteral {
                    value: ScalarValue::Boolean(Some(true)),
                }),
                includes_end: Arc::new(Expr::RuntimeLiteral {
                    value: ScalarValue::Boolean(Some(false)),
                }),
            })
            .collect::<Vec<_>>();
        let predicate =
            union_predicate(&intervals, &["a".into(), "b".into()], &point_type).unwrap();
        let mut nodes = 0;
        predicate.visit(&mut |_| nodes += 1);
        assert!(
            nodes < 200 * intervals.len(),
            "expanded interval predicate contains {nodes} nodes"
        );
    }

    #[test]
    fn indexed_prefix_candidates_match_brute_force_with_overlaps_and_open_bounds() {
        let binding = binding();
        let partitions = vec![
            Some((typed(&[1, 1]), typed(&[1, 5]))),
            Some((typed(&[1, 4]), typed(&[2, 0]))),
            None,
            Some((typed(&[3, 0]), typed(&[4, 5]))),
        ];
        let index = PrefixPartitionIndex::new(&partitions, &binding).unwrap();
        for lower in [None, Some(0), Some(1), Some(2), Some(4), Some(5)] {
            for upper in [None, Some(0), Some(1), Some(2), Some(3), Some(4)] {
                for lower_inclusive in [false, true] {
                    for upper_inclusive in [false, true] {
                        let (lower_key, upper_key, interval) =
                            query(lower, upper, lower_inclusive, upper_inclusive);
                        let actual = index
                            .query(lower_key.as_ref(), upper_key.as_ref(), &interval, &binding)
                            .unwrap();
                        let expected = partitions
                            .iter()
                            .enumerate()
                            .filter_map(|(partition, bounds)| {
                                bounds.as_ref().and_then(|(minimum, maximum)| {
                                    prefix_interval_overlaps(
                                        lower_key.as_ref(),
                                        upper_key.as_ref(),
                                        &interval,
                                        minimum,
                                        maximum,
                                        &binding,
                                    )
                                    .unwrap()
                                    .then_some(partition)
                                })
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(actual, expected);
                    }
                }
            }
        }
    }
}
fn decode_typed_prefix(
    binding: &IndexBinding,
    key: &[KeyValue],
) -> Result<vail_storage::fragment_index::TypedKey> {
    if key.len() > binding.key_fields.len() {
        return Err(TableError::Invalid(
            "interval prefix exceeds indexed key arity".into(),
        ));
    }
    let components = key
        .iter()
        .zip(&binding.key_fields)
        .map(|(value, field)| match value {
            KeyValue::ArrowIpc(bytes) => {
                let array = vail_storage::fragment_index::decode_typed_key_component(bytes)
                    .map_err(|error| TableError::Invalid(error.to_string()))?;
                if array.data_type() != &field.physical_type {
                    return Err(TableError::Invalid(
                        "interval prefix type differs from indexed key type".into(),
                    ));
                }
                Ok(array)
            }
            _ => Err(TableError::Invalid(
                "interval prefix does not use typed Arrow IPC".into(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(vail_storage::fragment_index::TypedKey { components })
}
fn compare_typed_prefix(
    left: &[ArrayRef],
    right: &[ArrayRef],
    binding: &IndexBinding,
) -> Result<std::cmp::Ordering> {
    let len = left.len().min(right.len());
    let options = binding
        .key_fields
        .iter()
        .take(len)
        .map(|field| arrow_schema::SortOptions {
            descending: field.descending,
            nulls_first: !field.nulls_last,
        })
        .collect::<Vec<_>>();
    vail_storage::fragment_index::compare_typed_key_components(
        &left[..len],
        &right[..len],
        &options,
    )
    .map_err(|error| TableError::Invalid(error.to_string()))
}

fn compare_prefix(a: &[KeyValue], b: &[KeyValue], binding: &IndexBinding) -> std::cmp::Ordering {
    let common_len = a.len().min(b.len());
    compare_key(&a[..common_len], &b[..common_len], binding)
}
fn compare_key(a: &[KeyValue], b: &[KeyValue], binding: &IndexBinding) -> std::cmp::Ordering {
    let decode = |value: &KeyValue| match value {
        KeyValue::ArrowIpc(bytes) => {
            vail_storage::fragment_index::decode_typed_key_component(bytes)
                .expect("interval endpoints are typed Arrow IPC")
        }
        _ => unreachable!("interval endpoints use Arrow IPC transport"),
    };
    let left = a.iter().map(decode).collect::<Vec<_>>();
    let right = b.iter().map(decode).collect::<Vec<_>>();
    let options = binding
        .key_fields
        .iter()
        .take(a.len())
        .map(|field| arrow_schema::SortOptions {
            descending: field.descending,
            nulls_first: !field.nulls_last,
        })
        .collect::<Vec<_>>();
    vail_storage::fragment_index::compare_typed_key_components(&left, &right, &options)
        .expect("validated typed interval endpoints")
}

/// Composite storage bounds sort missing key fields last. Represent presence
/// explicitly to retain that order with native DataFusion struct comparisons.
pub(crate) fn ordered_key_point(value: Expr, point_type: &Type) -> Expr {
    if let Type::Struct { r#struct } = point_type {
        Expr::Struct {
            fields: r#struct
                .iter()
                .map(|(name, _)| {
                    let field = Expr::GetField {
                        value: Arc::new(value.clone()),
                        name: name.clone(),
                    };
                    (
                        name.clone(),
                        Expr::Struct {
                            fields: vec![
                                (
                                    "missing".into(),
                                    Expr::IsMissing {
                                        value: Arc::new(field.clone()),
                                    },
                                ),
                                ("value".into(), field),
                            ],
                        },
                    )
                })
                .collect(),
        }
    } else {
        value
    }
}
pub(crate) fn ordered_key_interval(value: Expr, point_type: &Type) -> Expr {
    let field = |name: &str| Expr::GetField {
        value: Arc::new(value.clone()),
        name: name.into(),
    };
    let endpoint = |name: &str| {
        let raw = field(name);
        let ordered = ordered_key_point(raw.clone(), point_type);
        // Preserve a null open endpoint in the ordered-key representation.
        // Replacing it with a struct of {missing,value} fields turns an
        // unbounded bound into a concrete all-missing key, which sorts after
        // ordinary composite keys and can reject every row in the interval.
        let typed_null = Expr::IfElse {
            condition: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Boolean(None),
            }),
            consequent: Arc::new(ordered.clone()),
            alternate: Arc::new(ordered.clone()),
        };
        Expr::IfElse {
            condition: Arc::new(Expr::IsMissing {
                value: Arc::new(raw),
            }),
            consequent: Arc::new(typed_null),
            alternate: Arc::new(ordered),
        }
    };
    Expr::Interval {
        start: Arc::new(endpoint("start")),
        end: Arc::new(endpoint("end")),
        includes_start: Arc::new(field("includes_start")),
        includes_end: Arc::new(field("includes_end")),
    }
}

fn union_predicate(intervals: &[ScalarIR], keys: &[String], point_type: &Type) -> Result<Expr> {
    // Prefix intervals are wrapped in a struct whose fields name the prefix
    // components. A one-component prefix is just the underlying key column;
    // comparing the wrapper struct requires recursively constructing and
    // inferring a second struct (and, for loci, its nested metadata) for every
    // interval boundary. Unwrap it here and use DataFusion's native comparison
    // on the actual typed key column. Missing row keys still yield a null
    // comparison, which a FilterExec drops, matching key comparison semantics.
    let (point, comparison_type, prefix_component) = match point_type {
        Type::Struct { r#struct } if r#struct.len() == 1 && keys.len() > 1 => (
            Expr::Column {
                name: keys
                    .first()
                    .ok_or_else(|| TableError::Invalid("interval read has no key".into()))?
                    .clone(),
            },
            r#struct[0].1.clone(),
            Some(r#struct[0].0.clone()),
        ),
        Type::Primitive(_) | Type::Locus { .. } | Type::Tuple { .. } | Type::Array { .. } => (
            Expr::Column {
                name: keys
                    .first()
                    .ok_or_else(|| TableError::Invalid("interval read has no key".into()))?
                    .clone(),
            },
            point_type.clone(),
            None,
        ),
        Type::Struct { r#struct } => (
            Expr::Struct {
                fields: r#struct
                    .iter()
                    .zip(keys)
                    .map(|((name, _), key)| (name.clone(), Expr::Column { name: key.clone() }))
                    .collect(),
            },
            point_type.clone(),
            None,
        ),
        _ => {
            return Err(TableError::Invalid(
                "unsupported interval point type".into(),
            ));
        }
    };
    // These intervals are internal comparison bounds produced by key-filter
    // extraction. A missing start/end means an unbounded query side here. Do
    // this explicitly instead of IntervalContains: public interval contains
    // must retain Hail's missing propagation for a missing endpoint.
    let predicates = intervals
        .iter()
        .map(|interval| -> Result<Expr> {
            if let Some(component) = &prefix_component {
                return prefix_interval_predicate(interval, component, &point);
            }
            let get = |name: &str| Expr::GetField {
                value: Arc::new(interval.clone()),
                name: name.into(),
            };
            let ordered_interval = ordered_key_interval(interval.clone(), &comparison_type);
            let start = Expr::GetField {
                value: Arc::new(ordered_interval.clone()),
                name: "start".into(),
            };
            let end = Expr::GetField {
                value: Arc::new(ordered_interval),
                name: "end".into(),
            };
            let start_unbounded = Expr::IsMissing {
                value: Arc::new(start.clone()),
            };
            let end_unbounded = Expr::IsMissing {
                value: Arc::new(end.clone()),
            };
            let ordered_point = ordered_key_point(point.clone(), &comparison_type);
            let binary = |left, operator, right| Expr::Binary {
                operator,
                left: Arc::new(left),
                right: Arc::new(right),
            };
            use vail_execution::expressions::BinaryOperator as Op;
            let boundary = |endpoint: Expr, inclusive: Expr, is_lower: bool| {
                let strict = binary(
                    ordered_point.clone(),
                    if is_lower { Op::Gt } else { Op::Lt },
                    endpoint.clone(),
                );
                let equal = binary(ordered_point.clone(), Op::Eq, endpoint);
                binary(strict, Op::Or, binary(equal, Op::And, inclusive))
            };
            let lower = binary(
                start_unbounded,
                Op::Or,
                boundary(start, get("includes_start"), true),
            );
            let upper = binary(
                end_unbounded,
                Op::Or,
                boundary(end, get("includes_end"), false),
            );
            Ok(binary(lower, Op::And, upper))
        })
        .collect::<Result<Vec<_>>>()?;
    if predicates.is_empty() {
        return Err(TableError::Invalid("interval query list is empty".into()));
    }
    // Position-only predicates can expand to many per-contig intervals.
    // Keep the residual expression balanced so planning/evaluation depth is
    // logarithmic in the number of intervals.
    let mut level = predicates;
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        let mut iter = level.into_iter();
        while let Some(left) = iter.next() {
            next.push(if let Some(right) = iter.next() {
                Expr::Binary {
                    operator: vail_execution::expressions::BinaryOperator::Or,
                    left: Arc::new(left),
                    right: Arc::new(right),
                }
            } else {
                left
            });
        }
        level = next;
    }
    Ok(level.pop().unwrap())
}

/// Internal interval endpoints are scalar constants. Resolve them once rather
/// than repeatedly lowering GetField/IsMissing trees with locus type metadata.
/// Missing whole endpoints are unbounded; a missing field in a present prefix
/// struct is a real nulls-last key value.
fn prefix_interval_predicate(interval: &ScalarIR, component: &str, point: &Expr) -> Result<Expr> {
    use vail_execution::expressions::BinaryOperator as Op;
    let ScalarValue::Struct(fields) = eval_scalar(interval)? else {
        return Err(TableError::Invalid(
            "interval literal has invalid storage shape".into(),
        ));
    };
    let literal_bool = |value| Expr::RuntimeLiteral {
        value: ScalarValue::Boolean(Some(value)),
    };
    let binary = |left, operator, right| Expr::Binary {
        operator,
        left: Arc::new(left),
        right: Arc::new(right),
    };
    let missing = || Expr::IsMissing {
        value: Arc::new(point.clone()),
    };
    let present = || Expr::Unary {
        operator: vail_execution::expressions::UnaryOperator::Not,
        value: Arc::new(missing()),
    };
    let boundary = |index, lower: bool| -> Result<Expr> {
        let endpoint = ScalarValue::try_from_array(fields.column(index).as_ref(), 0)?;
        if endpoint.is_null() {
            return Ok(literal_bool(true));
        }
        let ScalarValue::Struct(prefix) = endpoint else {
            return Err(TableError::Invalid(
                "interval prefix must be a struct".into(),
            ));
        };
        let value = prefix
            .column_by_name(component)
            .ok_or_else(|| TableError::Invalid("interval prefix component is missing".into()))?;
        let value = ScalarValue::try_from_array(value.as_ref(), 0)?;
        let inclusive = if lower {
            !scalar_boolean(&ScalarValue::try_from_array(fields.column(1).as_ref(), 0)?)?
        } else {
            scalar_boolean(&ScalarValue::try_from_array(fields.column(3).as_ref(), 0)?)?
        };
        if value.is_null() {
            return Ok(match (lower, inclusive) {
                (true, true) => missing(),
                (true, false) => literal_bool(false),
                (false, true) => literal_bool(true),
                (false, false) => present(),
            });
        }
        let op = match (lower, inclusive) {
            (true, true) => Op::Ge,
            (true, false) => Op::Gt,
            (false, true) => Op::Le,
            (false, false) => Op::Lt,
        };
        let comparison = binary(point.clone(), op, Expr::RuntimeLiteral { value });
        Ok(if lower {
            binary(missing(), Op::Or, comparison)
        } else {
            binary(present(), Op::And, comparison)
        })
    };
    Ok(binary(boundary(0, true)?, Op::And, boundary(2, false)?))
}

fn apply_filter(rows: PhysicalPlan, predicate: &Expr, child: &TableRef) -> Result<PhysicalPlan> {
    let expression =
        lower_with_global_columns(predicate, &rows.schema(), child.globals().schema())?;
    Ok(Arc::new(FilterExec::try_new(expression, rows)?))
}
fn storage_error(error: vail_storage::StorageError) -> DataFusionError {
    DataFusionError::External(Box::new(TableError::Storage(error)))
}
