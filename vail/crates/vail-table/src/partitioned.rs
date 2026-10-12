//! Independently executable Table partitions. A partition is a DataFusion plan,
//! not a row label. Narrow transformations share immutable input plans; global
//! operators explicitly concatenate or redistribute their inputs.
use crate::plan::{self, PartitionLayout, PhysicalPlan, TablePlan, column, properties};
use crate::{
    BatchStream, PartitionEvent, RepartitionStrategy, Result, TableError, TableIR, TableNode,
    TableRef,
};
use arrow_array::{Array, LargeListArray, ListArray, RecordBatch, StructArray, UInt64Array};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::{DataFusionError, ScalarValue, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr, RangePartitioning,
    expressions::Literal,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream, execution_plan::EmissionType,
    limit::GlobalLimitExec, projection::ProjectionExec, repartition::RepartitionExec,
    sorts::sort::SortExec, stream::RecordBatchStreamAdapter,
};
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture};
use std::{fmt, sync::Arc};
use tokio::sync::OnceCell;

pub struct PreparedTable {
    pub parts: Vec<PhysicalPlan>,
    pub counts: Vec<Option<usize>>,
    pub schema: SchemaRef,
}
/// Reset DataFusion operator state once per node, preserving shared plan inputs.
/// Interval partitions can reference one physical DAG through several views.
/// DataFusion's tree reset expands such shared inputs into independent trees.
/// Each call uses its own cache, so distinct interval evaluations stay independent.
pub(crate) fn reset_plan_states(plan: PhysicalPlan) -> datafusion_common::Result<PhysicalPlan> {
    fn reset(
        plan: PhysicalPlan,
        memo: &mut std::collections::HashMap<usize, PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        let id = Arc::as_ptr(&plan) as *const () as usize;
        if let Some(reset) = memo.get(&id) {
            return Ok(reset.clone());
        }
        let children = plan
            .children()
            .into_iter()
            .cloned()
            .map(|child| reset(child, memo))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        // Resetting changes execution state, not child schemas or ordering.
        let fresh = plan
            .replace_children(
                children,
                datafusion_physical_plan::ReplaceChildrenOptions::new(
                    datafusion_physical_plan::ChildrenPropertiesMode::Keep,
                ),
            )?
            .reset_state()?;
        memo.insert(id, fresh.clone());
        Ok(fresh)
    }
    reset(plan, &mut std::collections::HashMap::new())
}

fn required_ordering(input: &PhysicalPlan) -> Option<OrderingRequirements> {
    input
        .output_ordering()
        .and_then(|sorts| LexOrdering::new(sorts.to_vec()))
        .map(OrderingRequirements::from)
}
impl PreparedTable {
    pub fn physical_plan(&self) -> PhysicalPlan {
        Arc::new(PartitionSetExec::new(
            self.parts.clone(),
            self.schema.clone(),
        ))
    }
    pub(crate) fn ordered(&self) -> PhysicalPlan {
        Arc::new(OrderedConcatExec::new(
            self.parts.clone(),
            self.schema.clone(),
        ))
    }
    pub(crate) async fn partition_counts(&self) -> Result<Vec<usize>> {
        let mut result = Vec::with_capacity(self.parts.len());
        for (plan, known) in self.parts.iter().zip(&self.counts) {
            result.push(match known {
                Some(n) => *n,
                None => count_rows(plan.clone()).await?,
            });
        }
        Ok(result)
    }
}

/// One array-valued expression supplies all source partitions. Evaluation is
/// deferred until a partition is pulled, and shared by partitions of an action.
#[derive(Debug)]
struct ExpressionSourceExec {
    input: PhysicalPlan,
    table: TableRef,
    n_partitions: usize,
    evaluated: Arc<OnceCell<std::result::Result<RecordBatch, Arc<DataFusionError>>>>,
    cache: Arc<PlanProperties>,
}
impl ExpressionSourceExec {
    fn new(input: PhysicalPlan, table: TableRef, n_partitions: usize) -> Self {
        let mut cache =
            (*properties(plan::physical_schema(&table), EmissionType::Incremental)).clone();
        cache.partitioning = Partitioning::UnknownPartitioning(n_partitions);
        Self {
            input,
            table,
            n_partitions,
            evaluated: Arc::new(OnceCell::new()),
            cache: Arc::new(cache),
        }
    }
}
pub(crate) fn expression_source_plan(
    input: PhysicalPlan,
    table: TableRef,
    n_partitions: usize,
) -> PhysicalPlan {
    Arc::new(ExpressionSourceExec::new(input, table, n_partitions))
}
impl DisplayAs for ExpressionSourceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExpressionSourceExec: partitions={}", self.n_partitions)
    }
}
impl ExecutionPlan for ExpressionSourceExec {
    fn name(&self) -> &'static str {
        "ExpressionSourceExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "expression source needs one input".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.table.clone(),
            self.n_partitions,
        )))
    }
    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
        // Only reset local state; the tree traversal resets the exposed input.
        Ok(Arc::new(Self::new(
            self.input.clone(),
            self.table.clone(),
            self.n_partitions,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition >= self.n_partitions {
            return Err(DataFusionError::Execution(
                "expression source partition out of range".into(),
            ));
        }
        let input = self.input.clone();
        let table = self.table.clone();
        let evaluated = self.evaluated.clone();
        let n_partitions = self.n_partitions;
        let schema = self.schema();
        let stream = async_stream::try_stream! {
            let evaluated = evaluated.get_or_init(|| async {
                async fn evaluate(input: PhysicalPlan, context: Arc<TaskContext>) -> datafusion_common::Result<RecordBatch> {
                    let batches = datafusion_physical_plan::execution_plan::collect(input, context).await?;
                    if batches.len() != 1 || batches[0].num_rows() != 1 {
                        return Err(DataFusionError::Execution("expression parallelize must produce exactly one array value".into()));
                    }
                    let batch = batches[0].clone();
                    let array = batch.column(0);
                    if array.is_null(0) {
                        return Err(DataFusionError::Execution("expression parallelize does not accept a missing array".into()));
                    }
                    let values = match array.data_type() {
                        arrow_schema::DataType::List(_) => array.as_any().downcast_ref::<ListArray>().unwrap().value(0),
                        arrow_schema::DataType::LargeList(_) => array.as_any().downcast_ref::<LargeListArray>().unwrap().value(0),
                        _ => return Err(DataFusionError::Execution("expression parallelize expects array<struct>".into())),
                    };
                    let structs = values.as_any().downcast_ref::<StructArray>()
                        .ok_or_else(|| DataFusionError::Execution("expression parallelize expects array<struct>".into()))?;
                    if structs.null_count() > 0 {
                        return Err(DataFusionError::Execution("expression parallelize does not accept missing struct rows".into()));
                    }
                    Ok(batch)
                }
                evaluate(input, context.clone()).await.map_err(Arc::new)
            }).await;
            let batch = evaluated.as_ref().map_err(|error| DataFusionError::Shared(error.clone()))?;
            let array = batch.column(0);
            let length = match array.data_type() {
                arrow_schema::DataType::List(_) => array.as_any().downcast_ref::<ListArray>().unwrap().value_length(0) as usize,
                arrow_schema::DataType::LargeList(_) => array.as_any().downcast_ref::<LargeListArray>().unwrap().value_length(0) as usize,
                _ => unreachable!(),
            };
            let start = ((length as u128 * partition as u128) / n_partitions as u128) as usize;
            let end = ((length as u128 * (partition + 1) as u128) / n_partitions as u128) as usize;
            let values = match array.data_type() {
                arrow_schema::DataType::List(_) => array.as_any().downcast_ref::<ListArray>().unwrap().value(0),
                arrow_schema::DataType::LargeList(_) => array.as_any().downcast_ref::<LargeListArray>().unwrap().value(0),
                _ => unreachable!(),
            };
            let structs = values.as_any().downcast_ref::<StructArray>().unwrap();
            let mut columns = structs.columns().iter().map(|column| column.slice(start, end-start)).collect::<Vec<_>>();
            columns.push(Arc::new(UInt64Array::from(vec![partition as u64; end-start])));
            let rows = RecordBatch::try_new(plan::physical_schema(&table), columns)?;
            yield rows;
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// The action planner optimizes one logical tree and lowers it once. Splitting
/// its output into views preserves the independent partition execution API.
pub fn prepare(table: TableRef) -> BoxFuture<'static, Result<PreparedTable>> {
    async move {
        let physical = crate::logical::physical(&table).await?;
        let n = physical
            .properties()
            .output_partitioning()
            .partition_count();
        let schema = physical.schema();
        let known = table.partition_rows();
        let parts: Vec<PhysicalPlan> = (0..n)
            .map(|part| Arc::new(PartitionViewExec::new(physical.clone(), part)) as PhysicalPlan)
            .collect();
        let counts = known
            .filter(|counts| counts.len() == n)
            .map(|counts| counts.into_iter().map(Some).collect())
            .unwrap_or_else(|| vec![None; n]);
        Ok(PreparedTable {
            parts,
            counts,
            schema,
        })
    }
    .boxed()
}

#[derive(Debug)]
pub(crate) struct PartitionViewExec {
    input: PhysicalPlan,
    index: usize,
    cache: Arc<PlanProperties>,
    requirement: Option<OrderingRequirements>,
}

/// Preserve an internal sort requirement until the operator that consumes it
/// has restored Vail's row order and dropped its private ordinal.
#[derive(Debug)]
pub(crate) struct ProtectedExec {
    input: PhysicalPlan,
    label: &'static str,
    cache: Arc<PlanProperties>,
    requirement: Option<OrderingRequirements>,
}

/// Keep a DataFusion range repartition visible as an opaque execution node.
/// DataFusion does not model range partitions on its logical plan, and some
/// physical optimizer rules otherwise replace the output partition count while
/// leaving TableIR's partition views indexed by the original boundaries.
#[derive(Debug)]
pub(crate) struct ProtectedRangeRepartitionExec {
    input: PhysicalPlan,
    range: RangePartitioning,
    inner: PhysicalPlan,
    cache: Arc<PlanProperties>,
    requirement: Option<OrderingRequirements>,
}
impl ProtectedRangeRepartitionExec {
    pub(crate) fn new(
        input: PhysicalPlan,
        range: RangePartitioning,
    ) -> datafusion_common::Result<Self> {
        let inner: PhysicalPlan = Arc::new(RepartitionExec::try_new(
            input.clone(),
            Partitioning::Range(range.clone()),
        )?);
        let requirement = required_ordering(&input);
        let cache = inner.properties().clone();
        Ok(Self {
            input,
            range,
            inner,
            cache,
            requirement,
        })
    }
}
impl DisplayAs for ProtectedRangeRepartitionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailProtectedRangeRepartitionExec")
    }
}
impl ExecutionPlan for ProtectedRangeRepartitionExec {
    fn name(&self) -> &'static str {
        "VailProtectedRangeRepartitionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![datafusion_physical_plan::Distribution::SinglePartition]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.requirement.clone()]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "range repartition needs one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.range.clone(),
        )?))
    }
    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
        // The DataFusion tree traversal has already reset the exposed child.
        let input = self.input.clone();
        Ok(Arc::new(Self::new(input, self.range.clone())?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}
impl ProtectedExec {
    pub(crate) fn new(input: PhysicalPlan, label: &'static str) -> Self {
        let requirement = required_ordering(&input);
        Self {
            cache: input.properties().clone(),
            input,
            label,
            requirement,
        }
    }
}
impl DisplayAs for ProtectedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.label)
    }
}
impl ExecutionPlan for ProtectedExec {
    fn name(&self) -> &'static str {
        "VailProtectedExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.requirement.clone()]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "order contract needs one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children[0].clone(),
            cache: self.cache.clone(),
            label: self.label,
            requirement: self.requirement.clone(),
        }))
    }
    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
        Ok(Arc::new(Self::new(self.input.clone(), self.label)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
impl PartitionViewExec {
    pub(crate) fn new(input: PhysicalPlan, index: usize) -> Self {
        let mut cache = (**input.properties()).clone();
        cache.partitioning = Partitioning::UnknownPartitioning(1);
        let requirement = required_ordering(&input);
        Self {
            cache: Arc::new(cache),
            input,
            index,
            requirement,
        }
    }
}
impl DisplayAs for PartitionViewExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TablePartitionViewExec: partition={}", self.index)
    }
}
impl ExecutionPlan for PartitionViewExec {
    fn name(&self) -> &'static str {
        "TablePartitionViewExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.requirement.clone()]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "partition view requires one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(children[0].clone(), self.index)))
    }
    fn execute(
        &self,
        p: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if p != 0 {
            return Err(DataFusionError::Execution(
                "partition view has one output partition".into(),
            ));
        }
        self.input.execute(self.index, context)
    }
}

/// DataFusion exposes each child as a real output partition. Executing partition
/// i never starts any other child. All row transforms remain library plans.
#[derive(Debug)]
struct PartitionSetExec {
    parts: Vec<PhysicalPlan>,
    cache: Arc<PlanProperties>,
    requirements: Vec<Option<OrderingRequirements>>,
}
impl PartitionSetExec {
    fn new(parts: Vec<PhysicalPlan>, schema: SchemaRef) -> Self {
        let mut cache = (*properties(schema, EmissionType::Incremental)).clone();
        cache.partitioning = Partitioning::UnknownPartitioning(parts.len());
        if let Some(ordering) = parts
            .first()
            .and_then(|part| part.output_ordering())
            .cloned()
        {
            if parts
                .iter()
                .all(|part| part.output_ordering() == Some(&ordering))
            {
                cache.eq_properties.add_ordering(ordering);
            }
        }
        Self {
            requirements: parts.iter().map(required_ordering).collect(),
            parts,
            cache: Arc::new(cache),
        }
    }
}
impl DisplayAs for PartitionSetExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TablePartitionsExec: partitions={}", self.parts.len())
    }
}
impl ExecutionPlan for PartitionSetExec {
    fn name(&self) -> &'static str {
        "TablePartitionsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        self.parts.iter().collect()
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.parts.len()]
    }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        // Each child is one independently executable Table partition; execute
        // reads its partition zero. DF rewrites (e.g. sort over union) must
        // coalesce any newly introduced child partitions before this boundary.
        vec![datafusion_physical_plan::Distribution::SinglePartition; self.parts.len()]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.requirements.clone()
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.parts.len()]
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
        Ok(Arc::new(Self::new(children, self.schema())))
    }
    fn execute(
        &self,
        p: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let input = self
            .parts
            .get(p)
            .ok_or_else(|| DataFusionError::Execution(format!("invalid table partition {p}")))?
            .execute(0, context)?;
        let schema = self.schema();
        let output_schema = schema.clone();
        let stream = input.and_then(move |batch| {
            let schema = output_schema.clone();
            async move {
                let mut columns = batch.columns().to_vec();
                let index = columns
                    .len()
                    .checked_sub(1)
                    .ok_or_else(|| DataFusionError::Internal("partition tag is absent".into()))?;
                columns[index] = Arc::new(UInt64Array::from_value(p as u64, batch.num_rows()));
                let columns = columns.iter().zip(schema.fields())
                    .map(|(array, field)| arrow_cast::cast(array, field.data_type()))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                RecordBatch::try_new(schema, columns).map_err(Into::into)
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Ordered actions and global algorithms opt into concatenation. This avoids
/// CoalescePartitionsExec's unspecified interleaving and preserves keyed ranges.
#[derive(Debug)]
pub(crate) struct OrderedConcatExec {
    parts: Vec<PhysicalPlan>,
    cache: Arc<PlanProperties>,
    requirements: Vec<Option<OrderingRequirements>>,
}
impl OrderedConcatExec {
    pub(crate) fn new(parts: Vec<PhysicalPlan>, schema: SchemaRef) -> Self {
        Self {
            requirements: parts.iter().map(required_ordering).collect(),
            parts,
            cache: properties(schema, EmissionType::Incremental),
        }
    }
}
impl DisplayAs for OrderedConcatExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OrderedConcatExec: children={}", self.parts.len())
    }
}
impl ExecutionPlan for OrderedConcatExec {
    fn name(&self) -> &'static str {
        "OrderedConcatExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        self.parts.iter().collect()
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.parts.len()]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.requirements.clone()
    }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![datafusion_physical_plan::Distribution::SinglePartition; self.parts.len()]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.parts.len()]
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
        Ok(Arc::new(Self::new(children, self.schema())))
    }
    fn execute(
        &self,
        p: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if p != 0 {
            return Err(DataFusionError::Execution(
                "ordered concat has one output partition".into(),
            ));
        }
        let parts = self.parts.clone();
        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                for (id,part) in parts.iter().enumerate() {
                    let mut stream=part.execute(0,context.clone())?;
                    while let Some(batch)=stream.try_next().await? {
                        let mut columns=batch.columns().to_vec();
                        if !columns.is_empty() {let last=columns.len()-1;columns[last]=Arc::new(UInt64Array::from_iter_values(std::iter::repeat_n(id as u64,batch.num_rows())));}
                        let columns = columns.iter().zip(schema.fields())
                            .map(|(array, field)| arrow_cast::cast(array, field.data_type()))
                            .collect::<std::result::Result<Vec<_>, _>>()?;
                        yield RecordBatch::try_new(schema.clone(),columns)?;
                    }
                }
            },
        )))
    }
}

/// An action-local adapter for callers requiring one ordered input (for example
/// a total aggregate). Its execution still resolves real independent children.
#[derive(Debug)]
struct DeferredOrderedExec {
    table: TableRef,
    cache: Arc<PlanProperties>,
}
impl DisplayAs for DeferredOrderedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OrderedTablePartitionsExec")
    }
}
impl ExecutionPlan for DeferredOrderedExec {
    fn name(&self) -> &'static str {
        "OrderedTablePartitionsExec"
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
            return Err(DataFusionError::Plan(
                "deferred table has no physical children until preparation".into(),
            ));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
        // Prepared leaves contain action-local physical plans outside the
        // physical child tree. Replays must reset those plans as well.
        let table = match self.table.node() {
            TableNode::Prepared {
                rows,
                count,
                n_partitions,
                partition_counts,
            } => self.table.replacing_node(TableNode::Prepared {
                rows: crate::partitioned::reset_plan_states(rows.clone())?,
                count: *count,
                n_partitions: *n_partitions,
                partition_counts: partition_counts.clone(),
            }),
            _ => self.table.clone(),
        };
        Ok(Arc::new(Self {
            table,
            cache: self.cache.clone(),
        }))
    }
    fn execute(
        &self,
        p: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if p != 0 {
            return Err(DataFusionError::Execution(
                "ordered table adapter has one output partition".into(),
            ));
        }
        let table = self.table.clone();
        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                let prepared=prepare_legacy(table).await.map_err(external)?;
                let mut rows=prepared.ordered().execute(0,context)?;
                while let Some(batch)=rows.try_next().await? {yield batch;}
            },
        )))
    }
}
fn external(error: TableError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
pub(crate) fn deferred_ordered(table: TableRef) -> Result<TablePlan> {
    let schema = plan::physical_schema(&table);
    Ok(TablePlan {
        rows: Arc::new(DeferredOrderedExec {
            table,
            cache: properties(schema, EmissionType::Incremental),
        }),
        layout: PartitionLayout::Observed,
    })
}

pub(crate) fn prepare_legacy(table: TableRef) -> BoxFuture<'static, Result<PreparedTable>> {
    async move {
        let schema=plan::physical_schema(&table);
        match table.node() {
            TableNode::Lookup {left,right,keys,field,bindings,all_matches,interval_mode,query_interval_mode} => crate::lookup::prepare(&table,left,right,keys,field,bindings,*all_matches,*interval_mode,*query_interval_mode).await,
            TableNode::Join {left,right,kind,join_key} if matches!(kind,crate::TableJoinKind::Semi|crate::TableJoinKind::Anti) =>
                crate::join_interval::prepare_membership(&table,left,right,*kind,*join_key).await,
            TableNode::Join {left,right,kind,join_key} =>
                crate::join_interval::prepare(&table,left,right,*kind,*join_key).await,
            TableNode::MultiWayZipJoin {tables,data_field_name} =>
                crate::join_interval::prepare_zip(&table,tables,data_field_name).await,
            TableNode::Read {source}=>Ok(PreparedTable {parts:(0..source.partition_rows().len()).map(|p|plan::source_partition(&table,p)).collect::<Result<Vec<_>>>()?,counts:source.partition_rows().iter().copied().map(Some).collect(),schema}),
            TableNode::TextRead {paths,..}=>Ok(PreparedTable {parts:(0..paths.len()).map(|p|plan::source_partition(&table,p)).collect::<Result<Vec<_>>>()?,counts:vec![None;paths.len()],schema}),
            TableNode::Range {n_partitions,..}|TableNode::Parallelize {n_partitions,..}=>Ok(PreparedTable {parts:(0..*n_partitions).map(|p|plan::source_partition(&table,p)).collect::<Result<Vec<_>>>()?,counts:table.partition_rows().unwrap().into_iter().map(Some).collect(),schema}),
            TableNode::Expression { expression, bindings, n_partitions } => {
                let input = plan::expression_array(expression, bindings)?;
                let source: PhysicalPlan = Arc::new(ExpressionSourceExec::new(input, table.clone(), *n_partitions));
                let parts = (0..*n_partitions).map(|p| Arc::new(PartitionViewExec::new(source.clone(), p)) as PhysicalPlan).collect();
                Ok(PreparedTable { parts, counts: vec![None; *n_partitions], schema })
            }
            TableNode::Prepared {rows,count,n_partitions,partition_counts}=> {
                let actual=rows.properties().output_partitioning().partition_count();
                if actual!=*n_partitions{return Err(TableError::Invalid(format!("prepared leaf has {actual} physical partitions, expected {n_partitions}")));}
                let fresh=crate::partitioned::reset_plan_states(rows.clone())?;
                let parts=(0..actual).map(|p|Arc::new(PartitionViewExec::new(fresh.clone(),p)) as PhysicalPlan).collect();
                let counts=partition_counts.as_ref().filter(|counts|counts.len()==actual)
                    .map(|counts|counts.iter().copied().map(Some).collect())
                    .unwrap_or_else(||if actual==1{vec![*count]}else{vec![None;actual]});
                Ok(PreparedTable {parts,counts,schema})
            }
            TableNode::IntervalRead {child,intervals,point_type,filter}=>crate::interval_read::prepare(child,intervals,point_type,*filter).await,
            TableNode::MapGlobals {child}=> {let mut input=prepare_legacy(child.clone()).await?;input.schema=schema;Ok(input)},
            TableNode::MapRows {child,..}|TableNode::Filter {child,..}|TableNode::Explode {child,..}|TableNode::GroupWithinPartitions {child,..}|TableNode::MapPartitions {child,..}=> {
                let input=prepare_legacy(child.clone()).await?;
                let mut parts=Vec::with_capacity(input.parts.len());let mut counts=Vec::with_capacity(input.parts.len());
                for (physical,count) in input.parts.into_iter().zip(input.counts) {
                    let replacement=TableIR::prepared(child,physical,count,1);
                    let node=replace_child(table.node().clone(),replacement)?;
                    let one=table.replacing_node(node);
                    parts.push(plan::lower(&one)?.rows);
                    counts.push(match table.node() {TableNode::MapRows {..}=>count,TableNode::GroupWithinPartitions {n,..}=>count.map(|r|r.div_ceil(*n)),_=>None});
                }
                Ok(PreparedTable {parts,counts,schema})
            }
            TableNode::FilterPartitions {child,parts,keep}=> {
                let input=prepare_legacy(child.clone()).await?;
                if let Some(invalid)=parts.iter().find(|&&p|p>=input.parts.len()) {return Err(TableError::Invalid(format!("_filter_partitions: no partition with index {invalid} (num partitions = {})",input.parts.len())));}
                let (selected,counts)=input.parts.into_iter().zip(input.counts).enumerate().filter(|(i,_)|parts.contains(i)==*keep).map(|(_,entry)|entry).unzip();
                Ok(PreparedTable {parts:selected,counts,schema})
            }
            TableNode::Head {child,n}|TableNode::Tail {child,n}=> {
                if *n==0{return Ok(PreparedTable {parts:vec![],counts:vec![],schema});}
                let mut input=prepare_legacy(child.clone()).await?;
                let tail=matches!(table.node(),TableNode::Tail {..});
                let known=if let Some(lengths)=child.partition_rows(){lengths}else {input.partition_counts().await?};
                if input.counts.iter().any(Option::is_none) { input=prepare_legacy(child.clone()).await?; }
                let mut remaining=*n;let mut selected=Vec::new();
                let indices:Vec<usize>=if tail {(0..input.parts.len()).rev().collect()}else{(0..input.parts.len()).collect()};
                for i in indices {
                    let count=known[i].min(remaining);
                    if count==0{continue;}
                    let skip=if tail {known[i]-count}else{0};
                    let plan:PhysicalPlan=Arc::new(GlobalLimitExec::new(input.parts[i].clone(),skip,Some(count)));
                    selected.push((i,plan,Some(count)));remaining-=count;
                    if remaining==0{break;}
                }
                selected.sort_by_key(|(i,_,_)|*i);
                let (parts,counts)=selected.into_iter().map(|(_,p,n)|(p,n)).unzip();
                Ok(PreparedTable {parts,counts,schema})
            }
            TableNode::Repartition {child,n,strategy}=> {
                // Hail's Range simplification can regenerate any requested layout
                // without moving input rows, including a naive/coalesce increase.
                if let TableNode::Range {n:rows,..}=child.node(){return prepare_legacy(TableIR::range(*rows,*n)?).await;}
                let mut input=prepare_legacy(child.clone()).await?;
                if *strategy!=RepartitionStrategy::Shuffle {
                    let counts=if *strategy==RepartitionStrategy::NaiveCoalesce {vec![0;input.parts.len()]} else {input.partition_counts().await?};
                    if *strategy!=RepartitionStrategy::NaiveCoalesce && input.counts.iter().any(Option::is_none) { input=prepare_legacy(child.clone()).await?; }
                    let entries=counts.iter().copied().enumerate().collect::<Vec<_>>();
                    let ends=crate::partitioning::coalesce_ends(&entries,*n,*strategy);
                    let mut start=0;let mut parts=Vec::new();let mut sizes=Vec::new();
                    for end in ends {
                        parts.push(Arc::new(OrderedConcatExec::new(input.parts[start..=end].to_vec(),schema.clone())) as PhysicalPlan);
                        sizes.push(input.counts[start..=end].iter().copied().collect::<Option<Vec<_>>>().map(|c|c.into_iter().sum()));start=end+1;
                    }
                    Ok(PreparedTable {parts,counts:sizes,schema})
                } else {shuffle(input,child,child.keys(),*n).await}
            }
            TableNode::KeyBy {child,keys} if keys.is_empty()=>prepare_legacy(child.clone()).await,
            TableNode::KeyBy {child,..} => {
                let n = child.known_n_partitions().unwrap_or(1);
                let sorted = global(&table).await?;
                if n == 1 { Ok(sorted) } else {
                    // The ordinary sort produces one ordered stream. Split it
                    // with DataFusion range repartitioning by stable ordinal,
                    // keeping the original number of real execution partitions.
                    let count = sorted.counts.iter().copied().collect::<Option<Vec<_>>>().map(|c|c.into_iter().sum());
                    let prepared = TableIR::prepared(&table, sorted.ordered(), count, 1);
                    shuffle(sorted, &prepared, &[], n).await
                }
            },
            TableNode::OrderBy {child,keys} if keys.is_empty()=>prepare_legacy(child.clone()).await,
            TableNode::Union {children} if table.keys().is_empty()=> {
                let mut parts = Vec::new();
                let mut counts = Vec::new();
                for child in children {
                    let input = prepare_legacy(child.clone()).await?;
                    for (part, count) in input.parts.into_iter().zip(input.counts) {
                        let mut fields: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
                        for field in table.schema().fields() {
                            let value: Arc<dyn PhysicalExpr> = match part.schema().index_of(field.name()) {
                                Ok(index) => Arc::new(datafusion_physical_expr::expressions::CastExpr::new_with_target_field(column(&part,index), field.clone(),None)),
                                Err(_) => Arc::new(Literal::new(ScalarValue::try_from(field.data_type())?)),
                            };
                            fields.push((value,field.name().clone()));
                        }
                        fields.push((Arc::new(Literal::new(ScalarValue::UInt64(Some(parts.len() as u64)))), schema.field(table.schema().fields().len()).name().clone()));
                        parts.push(Arc::new(ProjectionExec::try_new(fields,part)?) as PhysicalPlan);
                        counts.push(count);
                    }
                }
                Ok(PreparedTable {parts,counts,schema})
            }
            _=>global(&table).await,
        }
    }.boxed()
}
fn replace_child(mut node: TableNode, child: TableRef) -> Result<TableNode> {
    match &mut node {
        TableNode::MapRows { child: c, .. }
        | TableNode::Filter { child: c, .. }
        | TableNode::Explode { child: c, .. }
        | TableNode::GroupWithinPartitions { child: c, .. }
        | TableNode::MapPartitions { child: c, .. }
        | TableNode::KeyBy { child: c, .. }
        | TableNode::OrderBy { child: c, .. }
        | TableNode::Aggregate { child: c, .. }
        | TableNode::Distinct { child: c, .. } => *c = child,
        _ => {
            return Err(TableError::Invalid(
                "operator cannot replace one child".into(),
            ));
        }
    }
    Ok(node)
}
async fn global(table: &TableRef) -> Result<PreparedTable> {
    let child = match table.node() {
        TableNode::KeyBy { child, .. }
        | TableNode::OrderBy { child, .. }
        | TableNode::Aggregate { child, .. }
        | TableNode::Distinct { child, .. } => Some(child),
        _ => None,
    };
    let one = if let Some(child) = child {
        let input = prepare_legacy(child.clone()).await?;
        let count = input
            .counts
            .iter()
            .copied()
            .collect::<Option<Vec<_>>>()
            .map(|c| c.into_iter().sum());
        table.replacing_node(replace_child(
            table.node().clone(),
            TableIR::prepared(child, input.ordered(), count, input.parts.len()),
        )?)
    } else {
        table.clone()
    };
    let rows = plan::lower(&one)?.rows;
    Ok(PreparedTable {
        schema: rows.schema(),
        parts: vec![rows],
        counts: vec![table.known_count()],
    })
}
async fn count_rows(input: PhysicalPlan) -> Result<usize> {
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    let schema = input.schema();
    let count = AggregateExprBuilder::new(
        datafusion_functions_aggregate::count::count_udaf(),
        vec![Arc::new(Literal::new(ScalarValue::Int32(Some(1))))],
    )
    .schema(schema.clone())
    .alias("count")
    .build()?;
    let aggregate = AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![]),
        vec![Arc::new(count)],
        vec![None],
        input,
        schema,
    )?;
    let mut stream = aggregate.execute(0, Arc::new(TaskContext::default()))?;
    let batch = stream
        .try_next()
        .await?
        .ok_or_else(|| TableError::Invalid("count produced no row".into()))?;
    match ScalarValue::try_from_array(batch.column(0), 0)? {
        ScalarValue::Int64(Some(n)) => {
            usize::try_from(n).map_err(|_| TableError::Invalid("row count overflow".into()))
        }
        _ => Err(TableError::Invalid("invalid count result".into())),
    }
}

async fn shuffle(
    mut input: PreparedTable,
    child: &TableRef,
    keys: &[String],
    n: usize,
) -> Result<PreparedTable> {
    let counts = input.partition_counts().await?;
    let total: usize = counts.iter().sum();
    let context = Arc::new(TaskContext::default());
    if input.counts.iter().any(Option::is_none) {
        input = prepare_legacy(child.clone()).await?;
    }
    let ordered = input.ordered();
    let (numbered, range) = if keys.is_empty() {
        let numbered = plan::ordinal(ordered);
        let index = numbered.schema().fields().len() - 1;
        let expression = column(&numbered, index);
        let splits = (1..n)
            .filter_map(|p| {
                let k = (total as u128 * p as u128 / n as u128) as u64;
                if k == 0 || k == total as u64 {
                    None
                } else {
                    Some(datafusion_common::SplitPoint::new(vec![
                        ScalarValue::UInt64(Some(k)),
                    ]))
                }
            })
            .collect::<Vec<_>>();
        let mut unique = Vec::new();
        for s in splits {
            if unique.last() != Some(&s) {
                unique.push(s);
            }
        }
        (
            numbered,
            RangePartitioning::try_new(
                LexOrdering::new(vec![PhysicalSortExpr::new(
                    expression,
                    SortOptions::default(),
                )])
                .unwrap(),
                unique,
            )?,
        )
    } else {
        let ranges =
            crate::partitioning::key_ranges(ordered.clone(), keys, total, n, context).await?;
        input = prepare_legacy(child.clone()).await?;
        let ordered = input.ordered();
        let range = RangePartitioning::try_new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                ranges.expression,
                SortOptions::default(),
            )])
            .unwrap(),
            ranges.splits,
        )?;
        (plan::ordinal(ordered), range)
    };
    // Each input partition independently labels its rows with a stable global
    // ordinal before DataFusion redistributes the real input partitions.
    let _ = numbered;
    let mut offset = 0u64;
    let mut numbered_parts = Vec::with_capacity(input.parts.len());
    for (part, rows) in input.parts.iter().zip(&counts) {
        let numbered = plan::ordinal(part.clone());
        let schema = numbered.schema();
        let last = schema.fields().len() - 1;
        let ordinal: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion_physical_expr::expressions::BinaryExpr::new(
                column(&numbered, last),
                datafusion_expr::Operator::Plus,
                Arc::new(Literal::new(ScalarValue::UInt64(Some(offset)))),
            ));
        offset = offset
            .checked_add(*rows as u64)
            .ok_or_else(|| TableError::Invalid("row ordinal overflow".into()))?;
        let mut fields = (0..last)
            .map(|i| (column(&numbered, i), schema.field(i).name().clone()))
            .collect::<Vec<_>>();
        fields.push((ordinal, schema.field(last).name().clone()));
        numbered_parts.push(Arc::new(ProjectionExec::try_new(fields, numbered)?) as PhysicalPlan);
    }
    let numbered_schema = numbered_parts
        .first()
        .map(|p| p.schema())
        .unwrap_or_else(|| {
            let ordinal = plan::ordinal(input.ordered());
            ordinal.schema()
        });
    let inputs: PhysicalPlan = Arc::new(PartitionSetExec::new(numbered_parts, numbered_schema));
    let shuffled: PhysicalPlan = Arc::new(RepartitionExec::try_new(
        inputs,
        Partitioning::Range(range),
    )?);
    let count = shuffled
        .properties()
        .output_partitioning()
        .partition_count();
    let mut parts = Vec::with_capacity(count);
    for p in 0..count {
        let selected: PhysicalPlan = Arc::new(OutputPartitionExec::new(shuffled.clone(), p));
        let last = selected.schema().fields().len() - 1;
        let sorted: PhysicalPlan = Arc::new(SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                column(&selected, last),
                SortOptions::default(),
            )])
            .unwrap(),
            selected,
        ));
        let expressions: Vec<_> = (0..last)
            .map(|i| (column(&sorted, i), sorted.schema().field(i).name().clone()))
            .collect();
        parts.push(Arc::new(ProjectionExec::try_new(expressions, sorted)?) as PhysicalPlan);
    }
    if keys.is_empty() && parts.len() < n {
        while parts.len() < n {
            parts.push(Arc::new(datafusion_physical_plan::empty::EmptyExec::new(
                input.schema.clone(),
            )));
        }
    }
    let sizes = vec![None; parts.len()];
    Ok(PreparedTable {
        schema: input.schema,
        counts: sizes,
        parts,
    })
}
#[derive(Debug)]
struct OutputPartitionExec {
    input: PhysicalPlan,
    partition: usize,
    cache: Arc<PlanProperties>,
}
impl OutputPartitionExec {
    fn new(input: PhysicalPlan, partition: usize) -> Self {
        Self {
            cache: properties(input.schema(), EmissionType::Incremental),
            input,
            partition,
        }
    }
}
impl DisplayAs for OutputPartitionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // This selected-partition boundary owns the exact internal partition
        // map. Show its physical subtree even though the optimizer must not
        // rewrite that subtree independently of the selected index.
        write!(
            f,
            "OutputPartitionExec: partition={}\n{}",
            self.partition,
            datafusion_physical_plan::displayable(self.input.as_ref()).indent(true)
        )
    }
}
impl ExecutionPlan for OutputPartitionExec {
    fn name(&self) -> &'static str {
        "OutputPartitionExec"
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
            return Err(DataFusionError::Plan(
                "selected partition boundary has no rewritable children".into(),
            ));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
        Ok(Arc::new(Self::new(
            crate::partitioned::reset_plan_states(self.input.clone())?,
            self.partition,
        )))
    }
    fn execute(
        &self,
        p: usize,
        ctx: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if p != 0 {
            return Err(DataFusionError::Execution(
                "partition view has one output".into(),
            ));
        }
        self.input.execute(self.partition, ctx)
    }
}

pub async fn count(table: TableRef) -> Result<usize> {
    crate::logical::count(&table).await
}

pub fn execute(table: &TableRef) -> BatchStream {
    let table = table.clone();
    async_stream::try_stream!{
        let prepared=prepare(table.clone()).await?;
        let context=Arc::new(TaskContext::default());
        for part in &prepared.parts {
            yield PartitionEvent::StartPartition;
            let mut input=part.execute(0,context.clone())?;
            while let Some(batch)=input.try_next().await? {
                if batch.num_rows()==0 {continue;}
                let row=batch.project(&(0..table.schema().fields().len()).collect::<Vec<_>>())?;
                yield PartitionEvent::Batch(vail_storage::local_table::normalize(&row,table.schema().clone())?);
            }
        }
    }.boxed()
}

#[cfg(test)]
mod expression_source_tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_physical_plan::execution_plan::{collect, reset_plan_states};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CountingExec {
        input: PhysicalPlan,
        count: Arc<AtomicUsize>,
        resets: Arc<AtomicUsize>,
    }
    impl DisplayAs for CountingExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CountingExec")
        }
    }
    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }
        fn children(&self) -> Vec<&PhysicalPlan> {
            vec![&self.input]
        }
        fn apply_expressions(
            &self,
            _: &mut dyn FnMut(
                &Arc<dyn PhysicalExpr>,
            ) -> datafusion_common::Result<TreeNodeRecursion>,
        ) -> datafusion_common::Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<PhysicalPlan>,
        ) -> datafusion_common::Result<PhysicalPlan> {
            Ok(Arc::new(Self {
                input: children[0].clone(),
                count: self.count.clone(),
                resets: self.resets.clone(),
            }))
        }
        fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
            self.resets.fetch_add(1, Ordering::SeqCst);
            Ok(self)
        }
        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> datafusion_common::Result<SendableRecordBatchStream> {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.input.execute(partition, context)
        }
    }

    #[tokio::test]
    async fn array_expression_evaluates_once_per_action_across_partitions() {
        let fields: Fields = vec![Arc::new(Field::new("x", DataType::Int32, true))].into();
        let structs = StructArray::try_new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
            None,
        )
        .unwrap();
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(fields), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 4])),
            Arc::new(structs),
            None,
        );
        let table = TableIR::parallelize_expression(
            crate::ScalarIR::RuntimeLiteral {
                value: ScalarValue::List(Arc::new(list.clone())),
            },
            Default::default(),
            3,
        )
        .unwrap();
        let batch_schema = Arc::new(Schema::new(vec![Field::new(
            "__vail_expression_array",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(batch_schema.clone(), vec![Arc::new(list)]).unwrap();
        let memory = MemorySourceConfig::try_new_exec(&[vec![batch]], batch_schema, None).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let resets = Arc::new(AtomicUsize::new(0));
        let input: PhysicalPlan = Arc::new(CountingExec {
            input: memory,
            count: count.clone(),
            resets: resets.clone(),
        });
        let source = expression_source_plan(input, table, 3);
        let batches = collect(source.clone(), Arc::new(TaskContext::default()))
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let reset = reset_plan_states(source).unwrap();
        assert_eq!(resets.load(Ordering::SeqCst), 1);
        let batches = collect(reset, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn exposed_children_are_reset_once_through_nested_ordering_wrappers() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let memory = MemorySourceConfig::try_new_exec(&[vec![]], schema, None).unwrap();
        let resets = Arc::new(AtomicUsize::new(0));
        let mut input: PhysicalPlan = Arc::new(CountingExec {
            input: memory,
            count: Arc::new(AtomicUsize::new(0)),
            resets: resets.clone(),
        });
        for _ in 0..12 {
            input = Arc::new(ProtectedExec::new(input, "NestedOrder"));
            let range = RangePartitioning::try_new(
                LexOrdering::new(vec![PhysicalSortExpr::new(
                    column(&input, 0),
                    SortOptions::default(),
                )])
                .unwrap(),
                vec![],
            )
            .unwrap();
            input = Arc::new(ProtectedRangeRepartitionExec::new(input, range).unwrap());
        }
        reset_plan_states(input).unwrap();
        assert_eq!(resets.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn replay_preserves_shared_physical_inputs_without_sharing_across_actions() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let memory = MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap();
        let resets = Arc::new(AtomicUsize::new(0));
        let input: PhysicalPlan = Arc::new(CountingExec {
            input: memory,
            count: Arc::new(AtomicUsize::new(0)),
            resets: resets.clone(),
        });
        let dag: PhysicalPlan = Arc::new(OrderedConcatExec::new(vec![input.clone(), input], schema));
        let first = super::reset_plan_states(dag.clone()).unwrap();
        assert_eq!(resets.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(first.children()[0], first.children()[1]));
        let second = super::reset_plan_states(dag).unwrap();
        assert_eq!(resets.load(Ordering::SeqCst), 2);
        assert!(!Arc::ptr_eq(first.children()[0], second.children()[0]));
    }

}
