//! DataFusion table provider for the leaf sources in `TableIR`.
//!
//! The provider exposes each existing Vail source partition as an independent
//! output partition. DataFusion still owns the logical plan and all operators
//! above the scan. Filters are currently inexact at the provider boundary, so
//! DataFusion keeps and evaluates the residual predicate.

use crate::plan::{self, PhysicalPlan};
use crate::{Result, TableError, TableNode, TableRef};
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{
    DataFusionError, Result as DFResult, ScalarValue, tree_node::TreeNodeRecursion,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, expressions::Column};
use datafusion_common::SplitPoint;
use datafusion_physical_expr::{
    AcrossPartitions, ConstExpr, LexOrdering, PhysicalSortExpr, RangePartitioning,
};
use datafusion_physical_expr::projection::ProjectionMapping;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    projection::ProjectionExec,
    metrics::{ExecutionPlanMetricsSet, SplitMetrics},
    stream::{BatchSplitStream, RecordBatchStreamAdapter},
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use vail_storage::fragment_index::{Endpoint, KeyField, KeyValue, QueryInterval, compare_keys};
use vail_storage::local_table::LocalRead;

/// Build the DataFusion scan for a leaf `TableIR`. The scan preserves the
/// source's established partition numbering and private partition column.
pub(crate) fn provider(table: &TableRef) -> Result<Arc<dyn TableProvider>> {
    provider_with_nested_projection_paths(table, Vec::new())
}

pub(crate) fn provider_with_nested_projection_paths(
    table: &TableRef,
    nested_projection_paths: Vec<Vec<String>>,
) -> Result<Arc<dyn TableProvider>> {
    if !matches!(
        table.node(),
        TableNode::Read { .. }
            | TableNode::TextRead { .. }
            | TableNode::Range { .. }
            | TableNode::Parallelize { .. }
            | TableNode::Prepared { .. }
            | TableNode::IntervalRead { .. }
    ) {
        return Err(TableError::Invalid(
            "DataFusion provider requires a leaf table source".into(),
        ));
    }
    Ok(Arc::new(VailSourceProvider {
        table: table.clone(),
        schema: plan::physical_schema(table),
        nested_projection_paths,
    }))
}

#[derive(Debug)]
struct VailSourceProvider {
    table: TableRef,
    schema: SchemaRef,
    nested_projection_paths: Vec<Vec<String>>,
}

#[async_trait::async_trait]
impl TableProvider for VailSourceProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>, // Limits remain as DataFusion operators above the scan.
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if let TableNode::TextRead { format, .. } = self.table.node()
            && format != "lines"
        {
            let count = self.table.known_n_partitions().ok_or_else(|| {
                DataFusionError::Plan("text source partition count is not known".into())
            })?;
            // PartitionSetExec needs its internal tag even when COUNT requests
            // no data columns. Retain it through the adapter, then project it out.
            let tag = self.schema.fields().len() - 1;
            let mut internal = projection.cloned().unwrap_or_else(|| (0..tag).collect());
            internal.retain(|&index| index != tag);
            internal.push(tag);
            let partitions = (0..count)
                .map(|part| {
                    plan::source_partition_projected(&self.table, part, Some(&internal))
                        .map_err(to_df_error)
                })
                .collect::<DFResult<Vec<_>>>()?;
            let schema = Arc::new(self.schema.project(&internal)?);
            let scan = Arc::new(PartitionSetExec::new(partitions, schema));
            if let Some(indices) = projection {
                let expressions = indices
                    .iter()
                    .map(|index| {
                        let position =
                            internal.iter().position(|i| i == index).ok_or_else(|| {
                                DataFusionError::Plan(
                                    "text projection field is out of range".into(),
                                )
                            })?;
                        let field = self.schema.field(*index);
                        Ok((
                            Arc::new(Column::new(field.name(), position))
                                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                            field.name().clone(),
                        ))
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                return Ok(Arc::new(ProjectionExec::try_new(expressions, scan)?));
            }
            return Ok(scan);
        }
        let mut scan: Arc<dyn ExecutionPlan> = match self.table.node() {
            TableNode::Prepared { rows, .. } => rows.clone(),
            TableNode::IntervalRead {
                child,
                intervals,
                point_type,
                filter,
            } => {
                let prepared = crate::interval_read::prepare(child, intervals, point_type, *filter)
                    .await
                    .map_err(to_df_error)?;
                Arc::new(PartitionSetExec::new(prepared.parts, prepared.schema))
            }
            TableNode::Read { source } => {
                let mut queries = vec![QueryInterval::default()];
                let mut constrained = false;
                for filter in filters {
                    if let Some(ranges) = intervals_for_filter(filter, &self.table)? {
                        constrained = true;
                        queries = intersect_intervals(&queries, &ranges, source)?;
                    }
                }
                let tag = self.schema.fields().len() - 1;
                let requested = projection
                    .cloned()
                    .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
                let mut internal = Vec::with_capacity(requested.len() + filters.len() + 1);
                for &index in &requested {
                    if !internal.contains(&index) {
                        internal.push(index);
                    }
                }
                // Inexact filters still execute above the provider. Keep
                // their referenced fields (and key fields) available until
                // that residual predicate has run, then trim to `requested`.
                for name in self.table.keys() {
                    if let Ok(index) = self.schema.index_of(name)
                        && !internal.contains(&index)
                    {
                        internal.push(index);
                    }
                }
                for filter in filters {
                    for column in filter.column_refs() {
                        if let Ok(index) = self.schema.index_of(&column.name)
                            && !internal.contains(&index)
                        {
                            internal.push(index);
                        }
                    }
                }
                internal.retain(|&index| index != tag);
                internal.push(tag);
                let row_indices = internal
                    .iter()
                    .copied()
                    .filter(|&index| index != tag)
                    .collect::<Vec<_>>();
                let projected_rows = Arc::new(
                    self.table
                        .schema()
                        .project(&row_indices)
                        .map_err(DataFusionError::from)?,
                );
                let projected_schema = Arc::new(
                    self.schema
                        .project(&internal)
                        .map_err(DataFusionError::from)?,
                );
                let field_ids = row_indices
                    .iter()
                    .map(|&index| {
                        source.field_ids().get(index).copied().ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "read projection field {index} is out of range"
                            ))
                        })
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                let nested_paths = self
                    .nested_projection_paths
                    .iter()
                    .filter(|path| {
                        path.first().is_some_and(|root| {
                            // The scan advertises the original key ordering.
                            // Its certified key values must remain intact.
                            !self.table.keys().contains(root)
                                && self
                                    .table
                                    .schema()
                                    .index_of(root)
                                    .ok()
                                    .and_then(|index| source.field_ids().get(index))
                                    .is_some_and(|id| field_ids.contains(id))
                        })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let scan: Arc<dyn ExecutionPlan> = if constrained {
                    Arc::new(ReadKeyFilteredExec::new(
                        source.clone(),
                        self.table.clone(),
                        projected_rows,
                        projected_schema,
                        field_ids,
                        queries,
                        nested_paths,
                    )?)
                } else {
                    let parts = (0..source.partition_rows().len())
                        .map(|part| {
                            plan::source_partition_projected_with_nested_paths(
                                &self.table,
                                part,
                                Some(&internal),
                                nested_paths.clone(),
                            )
                            .map_err(to_df_error)
                        })
                        .collect::<DFResult<Vec<_>>>()?;
                    Arc::new(PartitionSetExec::new(parts, projected_schema))
                };
                if let Some(indices) = projection {
                    let exprs = indices
                        .iter()
                        .map(|&index| {
                            let position = internal
                                .iter()
                                .position(|&candidate| candidate == index)
                                .ok_or_else(|| {
                                    DataFusionError::Plan(format!(
                                        "read projection field {index} is out of range"
                                    ))
                                })?;
                            let field = self.schema.field(index);
                            Ok((
                                Arc::new(Column::new(field.name(), position))
                                    as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                                field.name().clone(),
                            ))
                        })
                        .collect::<DFResult<Vec<_>>>()?;
                    return Ok(Arc::new(ProjectionExec::try_new(exprs, scan)?));
                }
                return Ok(scan);
            }
            _ => make_source_parts(&self.table, self.schema.clone())?,
        };
        if let Some(indices) = projection {
            let mut exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
                Vec::with_capacity(indices.len());
            for &index in indices {
                let field = self.schema.fields().get(index).ok_or_else(|| {
                    DataFusionError::Plan(format!("projection field {index} is out of range"))
                })?;
                exprs.push((
                    Arc::new(Column::new(field.name(), index)),
                    field.name().clone(),
                ));
            }
            scan = Arc::new(ProjectionExec::try_new(exprs, scan)?);
        }
        Ok(scan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let mut statuses = Vec::with_capacity(filters.len());
        for filter in filters {
            statuses.push(if intervals_for_filter(filter, &self.table)?.is_some() {
                TableProviderFilterPushDown::Inexact
            } else {
                TableProviderFilterPushDown::Unsupported
            });
        }
        Ok(statuses)
    }
}

fn make_source_parts(table: &TableRef, schema: SchemaRef) -> DFResult<Arc<dyn ExecutionPlan>> {
    let count = table.known_n_partitions().ok_or_else(|| {
        DataFusionError::Plan("source partition count is not known for provider scan".into())
    })?;
    let mut partitions = Vec::with_capacity(count);
    for part in 0..count {
        partitions.push(plan::source_partition(table, part).map_err(to_df_error)?);
    }
    Ok(Arc::new(PartitionSetExec::new(partitions, schema)))
}

fn intervals_for_filter(
    expression: &Expr,
    table: &TableRef,
) -> DFResult<Option<Vec<QueryInterval>>> {
    let TableNode::Read { source } = table.node() else {
        return Ok(None);
    };
    if table.keys().len() != 1 {
        return Ok(None);
    }
    let binding = source
        .index_binding()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    if binding.key_fields.len() != 1 {
        return Ok(None);
    }
    let key = &binding.key_fields[0];
    let name = &table.keys()[0];
    fn visit_bound(
        expression: &Expr,
        name: &str,
        key: &KeyField,
        descending: bool,
        binding: &vail_storage::fragment_index::IndexBinding,
    ) -> DFResult<Option<Vec<QueryInterval>>> {
        let Expr::BinaryExpr(binary) = expression else {
            return Ok(None);
        };
        if binary.op == Operator::And || binary.op == Operator::Or {
            let Some(left) = visit_bound(&binary.left, name, key, descending, binding)? else {
                return Ok(None);
            };
            let Some(right) = visit_bound(&binary.right, name, key, descending, binding)? else {
                return Ok(None);
            };
            if binary.op == Operator::Or {
                return Ok(Some(normalize_intervals(
                    left.into_iter().chain(right).collect(),
                    binding,
                )?));
            }
            return Ok(Some(intersect_unions(&left, &right, binding)?));
        }
        let (mut operator, scalar) = match (&*binary.left, &*binary.right) {
            (Expr::Column(column), Expr::Literal(value, _)) if column.name == name => {
                (binary.op, value)
            }
            (Expr::Literal(value, _), Expr::Column(column)) if column.name == name => {
                let Some(operator) = reverse_operator(binary.op) else {
                    return Ok(None);
                };
                (operator, value)
            }
            _ => return Ok(None),
        };
        if descending {
            let Some(reversed) = reverse_operator(operator) else {
                return Ok(None);
            };
            operator = reversed;
        }
        let Some(value) = scalar_to_key(scalar, key) else {
            return Ok(None);
        };
        let point = || QueryInterval {
            lower: Some(Endpoint {
                key: vec![value.clone()],
                inclusive: true,
            }),
            upper: Some(Endpoint {
                key: vec![value.clone()],
                inclusive: true,
            }),
        };
        let lower = |inclusive| QueryInterval {
            lower: Some(Endpoint {
                key: vec![value.clone()],
                inclusive,
            }),
            upper: None,
        };
        let upper = |inclusive| QueryInterval {
            lower: None,
            upper: Some(Endpoint {
                key: vec![value.clone()],
                inclusive,
            }),
        };
        Ok(Some(vec![match operator {
            Operator::Eq => point(),
            Operator::Gt => lower(false),
            Operator::GtEq => lower(true),
            Operator::Lt => upper(false),
            Operator::LtEq => upper(true),
            _ => return Ok(None),
        }]))
    }
    visit_bound(expression, name, key, key.descending, &binding)
}

fn intersect_intervals(
    left: &[QueryInterval],
    right: &[QueryInterval],
    source: &LocalRead,
) -> DFResult<Vec<QueryInterval>> {
    let binding = source
        .index_binding()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    intersect_unions(left, right, &binding)
}

/// Sort and merge an interval union using typed Arrow key comparisons. Errors
/// from Arrow's fallible comparator construction/comparison are returned after
/// sorting rather than being hidden inside `sort_by`'s infallible closure.
fn normalize_intervals(
    mut intervals: Vec<QueryInterval>,
    binding: &vail_storage::fragment_index::IndexBinding,
) -> DFResult<Vec<QueryInterval>> {
    intervals = intervals
        .into_iter()
        .filter_map(|interval| match interval_is_non_empty(&interval, binding) {
            Ok(true) => Some(Ok(interval)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<DFResult<Vec<_>>>()?;
    let intervals = sort_intervals(intervals, binding)?;
    let mut merged: Vec<QueryInterval> = Vec::with_capacity(intervals.len());
    for current in intervals {
        let Some(previous) = merged.last_mut() else {
            merged.push(current);
            continue;
        };
        let relation = compare_upper_lower_bounds(&previous.upper, &current.lower, binding)?;
        if relation == std::cmp::Ordering::Greater
            || (relation == std::cmp::Ordering::Equal
                && (previous.upper.as_ref().is_none_or(|bound| bound.inclusive)
                    || current.lower.as_ref().is_none_or(|bound| bound.inclusive)))
        {
            match compare_upper_bounds(&current.upper, &previous.upper, binding)? {
                std::cmp::Ordering::Greater => previous.upper = current.upper,
                std::cmp::Ordering::Equal => {
                    if let (Some(previous), Some(current)) = (&mut previous.upper, &current.upper) {
                        previous.inclusive |= current.inclusive;
                    }
                }
                std::cmp::Ordering::Less => (),
            }
        } else {
            merged.push(current);
        }
    }
    Ok(merged)
}

fn interval_is_non_empty(
    interval: &QueryInterval,
    binding: &vail_storage::fragment_index::IndexBinding,
) -> DFResult<bool> {
    let (Some(lower), Some(upper)) = (&interval.lower, &interval.upper) else {
        return Ok(true);
    };
    match compare_keys(binding, &lower.key, &upper.key)
        .map_err(|error| DataFusionError::External(Box::new(error)))?
    {
        std::cmp::Ordering::Less => Ok(true),
        std::cmp::Ordering::Greater => Ok(false),
        std::cmp::Ordering::Equal => Ok(lower.inclusive && upper.inclusive),
    }
}

fn sort_intervals(
    intervals: Vec<QueryInterval>,
    binding: &vail_storage::fragment_index::IndexBinding,
) -> DFResult<Vec<QueryInterval>> {
    let mut row_for_interval = vec![None; intervals.len()];
    let mut typed_keys = Vec::new();
    for (index, interval) in intervals.iter().enumerate() {
        if let Some(lower) = &interval.lower {
            let typed = vail_storage::fragment_index::typed_key_from_transport(binding, &lower.key)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            row_for_interval[index] = Some(typed_keys.len());
            typed_keys.push(typed.components);
        }
    }
    if typed_keys.is_empty() {
        return Ok(intervals);
    }
    let columns = (0..binding.key_fields.len())
        .map(|component| {
            let arrays = typed_keys
                .iter()
                .map(|key| key[component].as_ref())
                .collect::<Vec<_>>();
            arrow_select::concat::concat(&arrays)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
        })
        .collect::<DFResult<Vec<_>>>()?;
    let options = binding
        .key_fields
        .iter()
        .map(|field| arrow_schema::SortOptions {
            descending: field.descending,
            nulls_first: !field.nulls_last,
        })
        .collect::<Vec<_>>();
    let comparators =
        vail_storage::fragment_index::make_typed_key_comparators(&columns, &columns, &options)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut order = (0..intervals.len()).collect::<Vec<_>>();
    order.sort_by(|&left, &right| {
        match (row_for_interval[left], row_for_interval[right]) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some(left_row), Some(right_row)) => {
                for comparator in &comparators {
                    let ordering = comparator(left_row, right_row);
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
            }
        }
        intervals[left]
            .lower
            .as_ref()
            .unwrap()
            .inclusive
            .cmp(&intervals[right].lower.as_ref().unwrap().inclusive)
            .reverse()
    });
    Ok(order
        .into_iter()
        .map(|index| intervals[index].clone())
        .collect())
}

fn intersect_unions(
    left: &[QueryInterval],
    right: &[QueryInterval],
    binding: &vail_storage::fragment_index::IndexBinding,
) -> DFResult<Vec<QueryInterval>> {
    let left = normalize_intervals(left.to_vec(), binding)?;
    let right = normalize_intervals(right.to_vec(), binding)?;
    let mut intersections = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        if let Some(overlap) = intersect_pair(&left[i], &right[j], binding)? {
            intersections.push(overlap);
        }
        match compare_upper_bounds(&left[i].upper, &right[j].upper, binding)? {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    Ok(intersections)
}

fn compare_upper_lower_bounds(
    upper: &Option<Endpoint>,
    lower: &Option<Endpoint>,
    binding: &vail_storage::fragment_index::IndexBinding,
) -> DFResult<std::cmp::Ordering> {
    match (upper, lower) {
        (None, _) | (_, None) => Ok(std::cmp::Ordering::Greater),
        (Some(upper), Some(lower)) => compare_keys(binding, &upper.key, &lower.key)
            .map_err(|error| DataFusionError::External(Box::new(error))),
    }
}

fn compare_upper_bounds(
    left: &Option<Endpoint>,
    right: &Option<Endpoint>,
    binding: &vail_storage::fragment_index::IndexBinding,
) -> DFResult<std::cmp::Ordering> {
    match (left, right) {
        (None, None) => Ok(std::cmp::Ordering::Equal),
        (None, Some(_)) => Ok(std::cmp::Ordering::Greater),
        (Some(_), None) => Ok(std::cmp::Ordering::Less),
        (Some(left), Some(right)) => Ok(compare_keys(binding, &left.key, &right.key)
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .then_with(|| left.inclusive.cmp(&right.inclusive))),
    }
}

fn intersect_pair(
    left: &QueryInterval,
    right: &QueryInterval,
    binding: &vail_storage::fragment_index::IndexBinding,
) -> DFResult<Option<QueryInterval>> {
    let lower = match (&left.lower, &right.lower) {
        (None, bound) | (bound, None) => bound.clone(),
        (Some(a), Some(b)) => Some(
            match compare_keys(binding, &a.key, &b.key)
                .map_err(|error| DataFusionError::External(Box::new(error)))?
            {
                std::cmp::Ordering::Less => b.clone(),
                std::cmp::Ordering::Greater => a.clone(),
                std::cmp::Ordering::Equal => Endpoint {
                    key: a.key.clone(),
                    inclusive: a.inclusive && b.inclusive,
                },
            },
        ),
    };
    let upper = match (&left.upper, &right.upper) {
        (None, bound) | (bound, None) => bound.clone(),
        (Some(a), Some(b)) => Some(
            match compare_keys(binding, &a.key, &b.key)
                .map_err(|error| DataFusionError::External(Box::new(error)))?
            {
                std::cmp::Ordering::Less => a.clone(),
                std::cmp::Ordering::Greater => b.clone(),
                std::cmp::Ordering::Equal => Endpoint {
                    key: a.key.clone(),
                    inclusive: a.inclusive && b.inclusive,
                },
            },
        ),
    };
    if let (Some(lower), Some(upper)) = (&lower, &upper) {
        match compare_keys(binding, &lower.key, &upper.key)
            .map_err(|error| DataFusionError::External(Box::new(error)))?
        {
            std::cmp::Ordering::Greater => return Ok(None),
            std::cmp::Ordering::Equal if !(lower.inclusive && upper.inclusive) => return Ok(None),
            _ => (),
        }
    }
    Ok(Some(QueryInterval { lower, upper }))
}

fn reverse_operator(operator: Operator) -> Option<Operator> {
    Some(match operator {
        Operator::Eq => Operator::Eq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        _ => return None,
    })
}

fn scalar_to_key(value: &ScalarValue, key: &KeyField) -> Option<KeyValue> {
    let array = value.to_array_of_size(1).ok()?;
    let array =
        vail_storage::fragment_index::normalize_typed_key_component(&array, &key.physical_type)
            .ok()?;
    vail_storage::fragment_index::encode_typed_key_component(&array)
        .ok()
        .map(KeyValue::ArrowIpc)
}

#[derive(Debug)]
struct ReadKeyFilteredExec {
    source: Arc<LocalRead>,
    row_schema: SchemaRef,
    schema: SchemaRef,
    field_ids: Vec<i32>,
    queries: Vec<QueryInterval>,
    nested_projection_paths: Vec<Vec<String>>,
    concurrency: Arc<tokio::sync::Semaphore>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    // Contiguous, ordered row intervals, validated once when planning the scan.
    partition_ranges: Vec<std::ops::Range<u64>>,
}

impl ReadKeyFilteredExec {
    fn new(
        source: Arc<LocalRead>,
        table: TableRef,
        row_schema: SchemaRef,
        schema: SchemaRef,
        field_ids: Vec<i32>,
        queries: Vec<QueryInterval>,
        nested_projection_paths: Vec<Vec<String>>,
    ) -> DFResult<Self> {
        let mut partition_ranges = Vec::new();
        let mut start = 0u64;
        for &rows in source.partition_rows() {
            let end = start.checked_add(rows as u64).ok_or_else(|| {
                DataFusionError::Execution("partition offset overflow".into())
            })?;
            partition_ranges.push(start..end);
            start = end;
        }
        let mut equivalence = EquivalenceProperties::new(schema.clone());
        if !table.keys().is_empty() {
            if let Ok(order) = crate::order_requirement(
                &table
                    .keys()
                    .iter()
                    .map(|key| (key.clone(), false))
                    .collect::<Vec<_>>(),
            ) {
                if let Ok(ordering) = order.physical_ordering(&schema) {
                    equivalence.add_ordering(ordering);
                }
            }
        }
        // The output carries its physical source-partition ID in the final
        // column. Preserve that range distribution so downstream operators
        // keep each filtered reader stream in its original partition.
        let partitioning = partition_id_properties(
            &schema,
            &mut equivalence,
            partition_ranges.len(),
            schema.fields().len() - 1,
        );
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            source,
            row_schema,
            schema,
            field_ids,
            queries,
            nested_projection_paths,
            properties,
            concurrency: Arc::new(tokio::sync::Semaphore::new(crate::EXECUTION_CONCURRENCY)),
            metrics: ExecutionPlanMetricsSet::new(),
            partition_ranges,
        })
    }
}

impl DisplayAs for ReadKeyFilteredExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailIndexedRead: {} key interval(s)", self.queries.len())
    }
}

impl ExecutionPlan for ReadKeyFilteredExec {
    fn name(&self) -> &'static str {
        "VailIndexedRead"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("indexed read has no children".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let range = self.partition_ranges.get(partition).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "indexed read partition {partition} is out of range"
            ))
        })?;
        let (start, end) = (range.start, range.end);
        let source = self.source.clone();
        let queries = self.queries.clone();
        let row_schema = self.row_schema.clone();
        let schema = self.schema.clone();
        let field_ids = self.field_ids.clone();
        let source = source.with_nested_projection_paths(self.nested_projection_paths.clone());
        let mut batches =
            source.projected_interval_batches_in_rows(&queries, start..end, &field_ids);
        let concurrency = self.concurrency.clone();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                let _permit = concurrency.acquire_owned().await.map_err(|_| {
                    DataFusionError::Execution("partition concurrency gate closed".into())
                })?;
                while let Some((offset, batch)) = batches.try_next().await.map_err(|e| DataFusionError::External(Box::new(e)))? {
                    let batch_end = offset.checked_add(batch.num_rows() as u64)
                        .ok_or_else(|| DataFusionError::Execution("batch offset overflow".into()))?;
                    let lo = offset.max(start);
                    let hi = batch_end.min(end);
                    if lo >= hi { continue; }
                    let batch = batch.slice((lo - offset) as usize, (hi - lo) as usize);
                    let batch = vail_storage::local_table::normalize(&batch, row_schema.clone())
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(UInt64Array::from_value(partition as u64, batch.num_rows())));
                    yield RecordBatch::try_new(schema.clone(), columns)?;
                }
            },
        ));
        Ok(Box::pin(BatchSplitStream::new(
            stream,
            context.session_config().batch_size(),
            SplitMetrics::new(&self.metrics, partition),
        )))
    }
}

fn to_df_error(error: TableError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn replace_source_partition_id(
    batch: RecordBatch,
    schema: SchemaRef,
    partition: usize,
) -> DFResult<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    let index = columns
        .len()
        .checked_sub(1)
        .ok_or_else(|| DataFusionError::Internal("source partition tag is absent".into()))?;
    columns[index] = Arc::new(UInt64Array::from_value(partition as u64, batch.num_rows()));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

/// Presents one single-partition source plan per source partition without
/// merging or reordering them. DataFusion's operators consume this as an
/// ordinary partitioned leaf.
#[derive(Debug)]
struct PartitionSetExec {
    partitions: Vec<PhysicalPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    concurrency: Arc<tokio::sync::Semaphore>,
    partition_offset: usize,
    metrics: ExecutionPlanMetricsSet,
}

/// Source identity and ordinal assignment, independently of advertised keys.
pub(crate) fn matches_source_partitions(
    input: &crate::plan::PhysicalPlan,
    table: &TableRef,
    count: usize,
) -> bool {
    if let Some(scan) = input.as_ref().downcast_ref::<ReadKeyFilteredExec>() {
        return matches!(table.node(), TableNode::Read { source } if Arc::ptr_eq(source, &scan.source))
            && scan.partition_ranges.len() == count;
    }
    let Some(set) = input.as_ref().downcast_ref::<PartitionSetExec>() else {
        return false;
    };
    set.partition_offset == 0
        && set.partitions.len() == count
        && set
            .partitions
            .iter()
            .enumerate()
            .all(|(index, part)| crate::plan::matches_source_partition(part, table, index))
}

impl PartitionSetExec {
    fn new(partitions: Vec<PhysicalPlan>, schema: SchemaRef) -> Self {
        let partition_id_index = schema.fields().len() - 1;
        // execute() replaces the final tag column in every child stream.
        // Project away the old tag before preserving child properties. The
        // projection removes any stale constants/equalities/orderings that
        // refer to the overwritten final column while retaining ordinary row
        // properties such as key ordering.
        let mapping = ProjectionMapping::try_new(
            (0..partition_id_index).map(|index| {
                (
                    Arc::new(Column::new(schema.field(index).name(), index))
                        as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    schema.field(index).name().clone(),
                )
            }),
            &schema,
        )
        .expect("source partition identity projection is valid");
        let mut equivalence = partitions
            .first()
            .and_then(|first| {
                let ordering = first
                    .properties()
                    .equivalence_properties()
                    .output_ordering();
                partitions
                    .iter()
                    .all(|part| {
                        part.properties().equivalence_properties().output_ordering() == ordering
                    })
                    .then(|| {
                        first
                            .properties()
                            .equivalence_properties()
                            .project(&mapping, schema.clone())
                    })
            })
            .unwrap_or_else(|| EquivalenceProperties::new(schema.clone()));
        // execute() assigns exactly one private partition ID to each physical
        // partition. Certify that range distribution so partition-local windows
        // need neither a hash exchange nor a sort to rediscover it.
        equivalence
            .add_constants([ConstExpr::new(
                Arc::new(Column::new(
                    schema.field(partition_id_index).name(),
                    partition_id_index,
                )),
                AcrossPartitions::Heterogeneous,
            )])
            .expect("source partition ID is a valid constant expression");
        let partitioning = partition_id_properties(
            &schema,
            &mut equivalence,
            partitions.len(),
            partition_id_index,
        );
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            partitions,
            schema,
            properties,
            concurrency: Arc::new(tokio::sync::Semaphore::new(crate::EXECUTION_CONCURRENCY)),
            partition_offset: 0,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

/// A partition view needs only its selected source plan, not another copy of
/// the complete source tree. Preserve the original ID and concurrency gate.
pub(crate) fn select_source_partition(input: &PhysicalPlan, index: usize) -> Option<PhysicalPlan> {
    let source = input.as_ref().downcast_ref::<PartitionSetExec>()?;
    let part = source.partitions.get(index)?.clone();
    let mut selected = PartitionSetExec::new(vec![part], source.schema.clone());
    selected.partition_offset = source.partition_offset + index;
    selected.concurrency = source.concurrency.clone();
    selected.metrics = source.metrics.clone();
    Some(Arc::new(selected))
}

/// Certify the supplied physical partition ID column as the partition range key.
pub(crate) fn partition_id_properties(
    schema: &SchemaRef,
    equivalence: &mut EquivalenceProperties,
    partitions: usize,
    index: usize,
) -> Partitioning {
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new(schema.field(index).name(), index)),
        arrow_schema::SortOptions {
            descending: false,
            nulls_first: false,
        },
    )])
    .expect("source partition ordering is nonempty");
    equivalence.add_ordering(ordering.clone());
    let splits = (1..partitions)
        .map(|partition| SplitPoint::new(vec![ScalarValue::UInt64(Some(partition as u64))]))
        .collect();
    Partitioning::Range(
        RangePartitioning::try_new(ordering, splits)
            .expect("source partition IDs have increasing UInt64 split points"),
    )
}

impl DisplayAs for PartitionSetExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailSourcePartitions: {}", self.partitions.len())
    }
}

impl ExecutionPlan for PartitionSetExec {
    fn name(&self) -> &'static str {
        "VailSourcePartitions"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.partitions.iter().collect()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.partitions.len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.partitions.len()]
    }
    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition; self.partitions.len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != self.partitions.len() {
            return Err(DataFusionError::Plan(format!(
                "Vail source partition count changed from {} to {}",
                self.partitions.len(),
                children.len()
            )));
        }
        let mut source = Self::new(children, self.schema.clone());
        source.partition_offset = self.partition_offset;
        source.concurrency = self.concurrency.clone();
        source.metrics = self.metrics.clone();
        Ok(Arc::new(source))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if self.partitions.is_empty() && partition == 0 {
            return Ok(Box::pin(
                datafusion::physical_plan::stream::EmptyRecordBatchStream::new(self.schema.clone()),
            ));
        }
        let child = self.partitions.get(partition).ok_or_else(|| {
            DataFusionError::Execution(format!("Vail source partition {partition} is out of range"))
        })?;
        let batch_size = context.session_config().batch_size();
        let input = crate::partitioned::limited_input(
            child.clone(),
            0,
            context,
            self.concurrency.clone(),
        );
        let schema = self.schema.clone();
        let output_schema = schema.clone();
        // Each child executes as partition zero locally. Expose its actual
        // source partition identity to logical windows and partition operations.
        let source_partition = self.partition_offset + partition;
        let stream = input.and_then(move |batch| {
            let schema = output_schema.clone();
            async move {
                replace_source_partition_id(batch, schema, source_partition)
            }
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
        Ok(Box::pin(BatchSplitStream::new(
            stream,
            batch_size,
            SplitMetrics::new(&self.metrics, partition),
        )))
    }
}

#[cfg(test)]
mod interval_intersection_tests {
    use super::*;
    use vail_storage::fragment_index::{IndexBinding, KeyType};

    fn binding() -> vail_storage::fragment_index::IndexBinding {
        IndexBinding {
            table_uuid: uuid::Uuid::from_u128(1),
            snapshot_id: 1,
            schema_id: 1,
            comparator_version: "test".into(),
            live_files_sha256: "test".into(),
            key_fields: vec![KeyField {
                field_id: 1,
                data_type: KeyType::Int32,
                physical_type: arrow_schema::DataType::Int32,
                descending: false,
                nulls_last: true,
            }],
        }
    }

    fn point(value: KeyValue) -> QueryInterval {
        QueryInterval {
            lower: Some(Endpoint {
                key: vec![value.clone()],
                inclusive: true,
            }),
            upper: Some(Endpoint {
                key: vec![value],
                inclusive: true,
            }),
        }
    }

    fn interval(lower: Option<(i32, bool)>, upper: Option<(i32, bool)>) -> QueryInterval {
        let endpoint = |(value, inclusive)| Endpoint {
            key: vec![KeyValue::Int32(value)],
            inclusive,
        };
        QueryInterval {
            lower: lower.map(endpoint),
            upper: upper.map(endpoint),
        }
    }

    fn contains(interval: &QueryInterval, value: i32) -> bool {
        intersect_pair(&point(KeyValue::Int32(value)), interval, &binding())
            .unwrap()
            .is_some()
    }

    #[test]
    fn malformed_key_comparison_is_an_error_not_an_empty_intersection() {
        let error = intersect_pair(
            &point(KeyValue::Int32(1)),
            &point(KeyValue::Int64(2)),
            &binding(),
        )
        .expect_err("wrong key type must not be treated as a disjoint interval");
        assert!(error.to_string().contains("typed key component differs"));
    }

    #[test]
    fn valid_disjoint_intersection_is_empty() {
        assert_eq!(
            intersect_pair(
                &point(KeyValue::Int32(1)),
                &point(KeyValue::Int32(2)),
                &binding(),
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn interval_union_normalization_preserves_open_shared_endpoints() {
        let normalized = normalize_intervals(
            vec![
                interval(Some((2, false)), Some((3, true))),
                interval(Some((1, true)), Some((2, false))),
                interval(Some((8, true)), None),
            ],
            &binding(),
        )
        .unwrap();
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized[0], interval(Some((1, true)), Some((2, false))));
        assert_eq!(normalized[1], interval(Some((2, false)), Some((3, true))));
        assert_eq!(normalized[2], interval(Some((8, true)), None));
    }

    #[test]
    fn intersection_sweep_handles_overlaps_and_unbounded_endpoints() {
        let left = vec![
            interval(Some((4, true)), Some((8, true))),
            interval(Some((1, true)), Some((5, true))),
            interval(None, Some((-2, false))),
        ];
        let right = vec![
            interval(Some((3, true)), Some((6, true))),
            interval(None, Some((-2, true))),
        ];
        let intersections = intersect_unions(&left, &right, &binding()).unwrap();
        assert_eq!(
            intersections,
            vec![
                interval(None, Some((-2, false))),
                interval(Some((3, true)), Some((6, true))),
            ]
        );
    }

    #[test]
    fn intersection_sweep_matches_cartesian_oracle_for_generated_bounds() {
        let b = binding();
        let lowers = [
            None,
            Some((-1, false)),
            Some((-1, true)),
            Some((1, false)),
            Some((1, true)),
        ];
        let uppers = [
            None,
            Some((-1, false)),
            Some((-1, true)),
            Some((0, false)),
            Some((1, true)),
        ];
        for seed in 0..20 {
            let left = (0..4)
                .map(|offset| {
                    interval(
                        lowers[(seed + offset * 3) % lowers.len()],
                        uppers[(seed * 2 + offset) % uppers.len()],
                    )
                })
                .collect::<Vec<_>>();
            let right = (0..3)
                .map(|offset| {
                    interval(
                        lowers[(seed * 3 + offset + 1) % lowers.len()],
                        uppers[(seed + offset * 2 + 2) % uppers.len()],
                    )
                })
                .collect::<Vec<_>>();
            let swept = intersect_unions(&left, &right, &b).unwrap();
            for value in -3..=3 {
                let expected = left.iter().any(|a| {
                    right.iter().any(|b| {
                        intersect_pair(a, b, &binding())
                            .unwrap()
                            .is_some_and(|overlap| contains(&overlap, value))
                    })
                });
                let actual = swept.iter().any(|interval| contains(interval, value));
                assert_eq!(actual, expected, "seed={seed}, key={value}");
            }
        }
    }
}

#[cfg(test)]
mod projected_provider_tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Int32Array, ListArray, RecordBatch, StructArray};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::context::SessionContext;
    use futures::TryStreamExt;
    use serde_json::json;

    #[tokio::test]
    async fn selected_source_view_keeps_only_one_partition_and_original_id() {
        let table = crate::TableIR::range(512, 128).unwrap();
        let source = make_source_parts(&table, plan::physical_schema(&table)).unwrap();
        let original = source.as_ref().downcast_ref::<PartitionSetExec>().unwrap();
        for partition in [0, 1, 127] {
            let selected = select_source_partition(&source, partition).unwrap();
            let rebuilt = selected.clone().with_new_children(
                selected.children().into_iter().cloned().collect(),
            ).unwrap();
            let inner = rebuilt.as_ref().downcast_ref::<PartitionSetExec>().unwrap();
            assert_eq!(inner.partitions.len(), 1);
            assert!(Arc::ptr_eq(&inner.concurrency, &original.concurrency));
            let view = crate::partitioned::PartitionViewExec::new(source.clone(), partition);
            assert_eq!(view.children()[0].children().len(), 1);
            for plan in [rebuilt, Arc::new(view) as PhysicalPlan] {
                let batches = plan.execute(0, Arc::new(datafusion_execution::TaskContext::default()))
                    .unwrap().try_collect::<Vec<_>>().await.unwrap();
                let values = batches.iter().flat_map(|batch| {
                    let ids = batch.columns().last().unwrap().as_any()
                        .downcast_ref::<UInt64Array>().unwrap();
                    assert!(ids.values().iter().all(|id| *id == partition as u64));
                    batch.column(0).as_any().downcast_ref::<Int32Array>()
                        .unwrap().values().to_vec()
                }).collect::<Vec<_>>();
                assert_eq!(values, (partition as i32 * 4..partition as i32 * 4 + 4).collect::<Vec<_>>());
            }
        }
    }

    #[tokio::test]
    async fn source_partition_range_matches_emitted_ids() {
        let table = crate::TableIR::range(12, 3).unwrap();
        let source = make_source_parts(&table, plan::physical_schema(&table)).unwrap();
        let Partitioning::Range(range) = source.properties().output_partitioning() else {
            panic!("source must advertise its partition ID ranges");
        };
        assert_eq!(range.partition_count(), 3);
        let context = Arc::new(datafusion_execution::TaskContext::default());
        for partition in 0..3 {
            let mut rows = 0;
            let mut stream = source.execute(partition, context.clone()).unwrap();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let ids = batch.columns().last().unwrap()
                    .as_any().downcast_ref::<UInt64Array>().unwrap();
                assert!(ids.values().iter().all(|id| *id == partition as u64));
                rows += batch.num_rows();
            }
            assert_eq!(rows, 4);
        }
    }

    #[tokio::test]
    async fn read_projection_reaches_storage_and_returns_only_selected_field() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("projected-read");
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("payload", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        vail_storage::local_table::write_local(
            path.to_str().unwrap(),
            false,
            schema,
            vec![vec![batch]],
            json!([["k", "int32"], ["payload", "int32"]]),
            vec!["k".into()],
        )
        .await
        .unwrap();

        let table = crate::TableIR::read(path.to_str().unwrap()).await.unwrap();
        let provider = provider(&table).unwrap();
        let context = SessionContext::new();
        let plan = provider
            .scan(&context.state(), Some(&vec![1]), &[], None)
            .await
            .unwrap();
        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "payload");
        let batches = plan
            .execute(0, context.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_columns)
                .collect::<Vec<_>>(),
            vec![1]
        );
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn map_rows_nested_array_projection_reads_selected_struct_child() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested-projected-read");
        let value_fields = vec![
            Arc::new(Field::new("GT", DataType::Int32, true)),
            Arc::new(Field::new("DP", DataType::Int32, true)),
        ];
        let cell_fields = vec![
            Arc::new(Field::new("cid", DataType::Int32, true)),
            Arc::new(Field::new("present", DataType::Boolean, true)),
            Arc::new(Field::new(
                "value",
                DataType::Struct(value_fields.clone().into()),
                true,
            )),
        ];
        let cells_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(cell_fields.clone().into()),
            true,
        )));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("cells", cells_type, true),
        ]));
        let values = Arc::new(StructArray::new(
            value_fields.into(),
            vec![
                Arc::new(Int32Array::from(vec![10, 11, 20])),
                Arc::new(Int32Array::from(vec![90, 91, 92])),
            ],
            None,
        ));
        let cells = Arc::new(StructArray::new(
            cell_fields.into(),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 0])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
                values,
            ],
            None,
        ));
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", cells.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 3])),
            cells,
            None,
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2])), list],
        )
        .unwrap();
        vail_storage::local_table::write_local(
            path.to_str().unwrap(),
            false,
            schema,
            vec![vec![batch]],
            json!([["k", "int32"], ["cells", {"array": {"struct": [["cid", "int32"], ["present", "bool"], ["value", {"struct": [["GT", "int32"], ["DP", "int32"]]}]]}}]]),
            vec!["k".into()],
        )
        .await
        .unwrap();

        let table = crate::TableIR::read(path.to_str().unwrap()).await.unwrap();
        let mapped = table
            .map_rows(vec![
                ("k".into(), crate::ScalarIR::Column { name: "k".into() }),
                (
                    "genotypes".into(),
                    crate::ScalarIR::ArrayMap {
                        value: Arc::new(crate::ScalarIR::Column {
                            name: "cells".into(),
                        }),
                        parameter: "cell".into(),
                        body: Arc::new(crate::ScalarIR::GetField {
                            value: Arc::new(crate::ScalarIR::GetField {
                                value: Arc::new(crate::ScalarIR::LambdaVariable {
                                    name: "cell".into(),
                                    dtype: None,
                                }),
                                name: "value".into(),
                            }),
                            name: "GT".into(),
                        }),
                    },
                ),
            ])
            .unwrap();
        let rows = mapped.collect_partitions().await.unwrap();
        let genotypes = rows
            .iter()
            .flatten()
            .flat_map(|batch| {
                let arrays = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                arrays
                    .values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(genotypes, vec![10, 11, 20]);
    }
}
