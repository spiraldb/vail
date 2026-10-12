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
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    projection::ProjectionExec,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use vail_storage::fragment_index::{Endpoint, KeyType, KeyValue, QueryInterval, compare_keys};
use vail_storage::local_table::LocalRead;

/// Build the DataFusion scan for a leaf `TableIR`. The scan preserves the
/// source's established partition numbering and private partition column.
pub(crate) fn provider(table: &TableRef) -> Result<Arc<dyn TableProvider>> {
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
    }))
}

#[derive(Debug)]
struct VailSourceProvider {
    table: TableRef,
    schema: SchemaRef,
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
                    if let Some(ranges) = intervals_for_filter(filter, &self.table) {
                        constrained = true;
                        queries = intersect_intervals(&queries, &ranges, source)?;
                    }
                }
                if !constrained {
                    make_source_parts(&self.table, self.schema.clone())?
                } else {
                    Arc::new(ReadKeyFilteredExec::new(
                        source.clone(),
                        self.table.clone(),
                        self.schema.clone(),
                        queries,
                    ))
                }
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
        let statuses = filters
            .iter()
            .map(|filter| {
                if intervals_for_filter(filter, &self.table).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect::<Vec<_>>();
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

fn intervals_for_filter(expression: &Expr, table: &TableRef) -> Option<Vec<QueryInterval>> {
    let TableNode::Read { source } = table.node() else {
        return None;
    };
    if table.keys().len() != 1 {
        return None;
    }
    let binding = source.index_binding().ok()?;
    if binding.key_fields.len() != 1 {
        return None;
    }
    let key = &binding.key_fields[0];
    let name = &table.keys()[0];
    fn visit_bound(
        expression: &Expr,
        name: &str,
        key_type: &KeyType,
        descending: bool,
        binding: &vail_storage::fragment_index::IndexBinding,
    ) -> Option<Vec<QueryInterval>> {
        let Expr::BinaryExpr(binary) = expression else {
            return None;
        };
        if binary.op == Operator::And || binary.op == Operator::Or {
            let left = visit_bound(&binary.left, name, key_type, descending, binding)?;
            let right = visit_bound(&binary.right, name, key_type, descending, binding)?;
            if binary.op == Operator::Or {
                return Some(left.into_iter().chain(right).collect());
            }
            return Some(
                left.iter()
                    .flat_map(|a| {
                        right
                            .iter()
                            .filter_map(move |b| intersect_pair(a, b, binding))
                    })
                    .collect(),
            );
        }
        let (mut operator, scalar) = match (&*binary.left, &*binary.right) {
            (Expr::Column(column), Expr::Literal(value, _)) if column.name == name => {
                (binary.op, value)
            }
            (Expr::Literal(value, _), Expr::Column(column)) if column.name == name => {
                (reverse_operator(binary.op)?, value)
            }
            _ => return None,
        };
        if descending {
            operator = reverse_operator(operator)?;
        }
        let value = scalar_to_key(scalar, key_type)?;
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
        Some(vec![match operator {
            Operator::Eq => point(),
            Operator::Gt => lower(false),
            Operator::GtEq => lower(true),
            Operator::Lt => upper(false),
            Operator::LtEq => upper(true),
            _ => return None,
        }])
    }
    visit_bound(expression, name, &key.data_type, key.descending, &binding)
}

fn intersect_intervals(
    left: &[QueryInterval],
    right: &[QueryInterval],
    source: &LocalRead,
) -> DFResult<Vec<QueryInterval>> {
    let binding = source
        .index_binding()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(left
        .iter()
        .flat_map(|a| right.iter().filter_map(|b| intersect_pair(a, b, &binding)))
        .collect())
}

fn intersect_pair(
    left: &QueryInterval,
    right: &QueryInterval,
    binding: &vail_storage::fragment_index::IndexBinding,
) -> Option<QueryInterval> {
    let lower = match (&left.lower, &right.lower) {
        (None, bound) | (bound, None) => bound.clone(),
        (Some(a), Some(b)) => Some(match compare_keys(binding, &a.key, &b.key) {
            std::cmp::Ordering::Less => b.clone(),
            std::cmp::Ordering::Greater => a.clone(),
            std::cmp::Ordering::Equal => Endpoint {
                key: a.key.clone(),
                inclusive: a.inclusive && b.inclusive,
            },
        }),
    };
    let upper = match (&left.upper, &right.upper) {
        (None, bound) | (bound, None) => bound.clone(),
        (Some(a), Some(b)) => Some(match compare_keys(binding, &a.key, &b.key) {
            std::cmp::Ordering::Less => a.clone(),
            std::cmp::Ordering::Greater => b.clone(),
            std::cmp::Ordering::Equal => Endpoint {
                key: a.key.clone(),
                inclusive: a.inclusive && b.inclusive,
            },
        }),
    };
    if let (Some(lower), Some(upper)) = (&lower, &upper) {
        match compare_keys(binding, &lower.key, &upper.key) {
            std::cmp::Ordering::Greater => return None,
            std::cmp::Ordering::Equal if !(lower.inclusive && upper.inclusive) => return None,
            _ => (),
        }
    }
    Some(QueryInterval { lower, upper })
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

fn scalar_to_key(value: &ScalarValue, key_type: &KeyType) -> Option<KeyValue> {
    if let KeyType::Canonical(logical_type) = key_type {
        // The index stores order-preserving Vail encodings. Keep metadata
        // pruning to primitive non-float types whose DataFusion comparison
        // order matches the certified Vail order; float predicates stay on
        // the residual path because NaN comparison semantics need special care.
        if !matches!(logical_type.as_str()?, "bool" | "int32" | "int64" | "str") {
            return None;
        }
        let array = value.to_array_of_size(1).ok()?;
        let bytes =
            vail_ordering::encode_typed_value(array.as_ref(), 0, Some(logical_type)).ok()?;
        return Some(KeyValue::Canonical(bytes));
    }
    Some(match (value, key_type) {
        (ScalarValue::Int32(None), KeyType::Int32) => KeyValue::Missing,
        (ScalarValue::Int64(None), KeyType::Int64) => KeyValue::Missing,
        (ScalarValue::Boolean(None), KeyType::Bool) => KeyValue::Missing,
        (ScalarValue::Utf8(None), KeyType::AsciiString) => KeyValue::Missing,
        (ScalarValue::Int32(Some(value)), KeyType::Int32) => KeyValue::Int32(*value),
        (ScalarValue::Int64(Some(value)), KeyType::Int64) => KeyValue::Int64(*value),
        (ScalarValue::Boolean(Some(value)), KeyType::Bool) => KeyValue::Bool(*value),
        (ScalarValue::Utf8(Some(value)), KeyType::AsciiString) if value.is_ascii() => {
            KeyValue::AsciiString(value.clone())
        }
        _ => return None,
    })
}

#[derive(Debug)]
struct ReadKeyFilteredExec {
    source: Arc<LocalRead>,
    table: TableRef,
    schema: SchemaRef,
    queries: Vec<QueryInterval>,
    properties: Arc<PlanProperties>,
}

impl ReadKeyFilteredExec {
    fn new(
        source: Arc<LocalRead>,
        table: TableRef,
        schema: SchemaRef,
        queries: Vec<QueryInterval>,
    ) -> Self {
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
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(source.partition_rows().len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            source,
            table,
            schema,
            queries,
            properties,
        }
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
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let rows = self.source.partition_rows();
        let count = *rows.get(partition).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "indexed read partition {partition} is out of range"
            ))
        })?;
        let start = rows[..partition].iter().try_fold(0u64, |sum, &n| {
            sum.checked_add(n as u64)
                .ok_or_else(|| DataFusionError::Execution("partition offset overflow".into()))
        })?;
        let end = start
            .checked_add(count as u64)
            .ok_or_else(|| DataFusionError::Execution("partition offset overflow".into()))?;
        let source = self.source.clone();
        let queries = self.queries.clone();
        let row_schema = self.table.schema().clone();
        let schema = self.schema.clone();
        let mut batches = source.interval_batches_in_rows(&queries, start..end);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
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
        )))
    }
}

fn to_df_error(error: TableError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Presents one single-partition source plan per source partition without
/// merging or reordering them. DataFusion's operators consume this as an
/// ordinary partitioned leaf.
#[derive(Debug)]
struct PartitionSetExec {
    partitions: Vec<PhysicalPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl PartitionSetExec {
    fn new(partitions: Vec<PhysicalPlan>, schema: SchemaRef) -> Self {
        let equivalence = partitions
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
                    .then(|| first.properties().equivalence_properties().clone())
            })
            .unwrap_or_else(|| EquivalenceProperties::new(schema.clone()));
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            partitions,
            schema,
            properties,
        }
    }
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
        Ok(Arc::new(Self::new(children, self.schema.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let child = self.partitions.get(partition).ok_or_else(|| {
            DataFusionError::Execution(format!("Vail source partition {partition} is out of range"))
        })?;
        let input = child.execute(0, context)?;
        let schema = self.schema.clone();
        let output_schema = schema.clone();
        // Each child executes as partition zero locally. Expose its actual
        // source partition identity to logical windows and partition operations.
        let stream = input.and_then(move |batch| {
            let schema = output_schema.clone();
            async move {
                let mut columns = batch.columns().to_vec();
                let index = columns.len().checked_sub(1).ok_or_else(|| {
                    DataFusionError::Internal("source partition tag is absent".into())
                })?;
                columns[index] =
                    Arc::new(UInt64Array::from_value(partition as u64, batch.num_rows()));
                RecordBatch::try_new(schema, columns).map_err(Into::into)
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
