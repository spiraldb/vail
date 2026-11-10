//! Batch-preserving partial states for plain nested collections. The final
//! DataFusion aggregate still performs all grouping and duplicate-key merging.
use crate::plan::PhysicalPlan;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::{DataFusionError, Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_expr::GroupsAccumulator;
use datafusion_functions_aggregate::array_agg::ArrayAgg;
use datafusion_physical_expr::{
    PhysicalExpr, aggregate::AggregateFunctionExpr, equivalence::EquivalenceProperties,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, InputOrderMode,
    Partitioning, PlanProperties, SendableRecordBatchStream,
    aggregates::{AggregateExec, AggregateMode},
    apply_expression_roots,
    execution_plan::EmissionType,
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{any::Any, fmt, sync::Arc};

fn nested_payload(dtype: &DataType) -> bool {
    match dtype {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => true,
        DataType::Struct(fields) => fields.iter().any(|f| nested_payload(f.data_type())),
        _ => false,
    }
}

pub(crate) fn rewrite(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    let Some(aggregate) = plan.downcast_ref::<AggregateExec>() else {
        return Ok(plan);
    };
    if *aggregate.mode() != AggregateMode::Partial
        || *aggregate.input_order_mode() != InputOrderMode::Linear
        || aggregate.limit_options().is_some()
        || !aggregate.group_expr().is_single()
        || aggregate.group_expr().is_empty()
        || aggregate.aggr_expr().is_empty()
        || aggregate.aggr_expr().iter().any(|expr| {
            !(expr.fun().inner().as_ref() as &dyn Any).is::<ArrayAgg>()
                || expr.is_distinct()
                || !expr.order_bys().is_empty()
                || !expr.groups_accumulator_supported()
        })
    {
        return Ok(plan);
    }
    let schema = aggregate.input().schema();
    let mut nested = false;
    for expr in aggregate.aggr_expr() {
        for arg in expr.expressions() {
            nested |= nested_payload(&arg.data_type(&schema)?);
        }
    }
    if !nested {
        return Ok(plan);
    }
    Ok(Arc::new(CollectStateExec::new(
        aggregate.input().clone(),
        aggregate.schema(),
        aggregate.group_expr().input_exprs(),
        aggregate.aggr_expr().to_vec(),
        aggregate.filter_expr().to_vec(),
    )))
}

#[derive(Debug)]
struct CollectStateExec {
    input: PhysicalPlan,
    groups: Vec<Arc<dyn PhysicalExpr>>,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}
impl CollectStateExec {
    fn new(
        input: PhysicalPlan,
        schema: SchemaRef,
        groups: Vec<Arc<dyn PhysicalExpr>>,
        aggregates: Vec<Arc<AggregateFunctionExpr>>,
        filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    ) -> Self {
        // States contain one row per input, not one per group. They establish
        // neither unique group keys nor a hash/range distribution of groups.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            input.boundedness(),
        ));
        Self {
            input,
            groups,
            aggregates,
            filters,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}
impl DisplayAs for CollectStateExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailCollectStateExec")
    }
}
impl ExecutionPlan for CollectStateExec {
    fn name(&self) -> &'static str {
        "VailCollectStateExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        let mut roots = self.groups.clone();
        for aggregate in &self.aggregates {
            roots.extend(aggregate.expressions());
        }
        roots.extend(self.filters.iter().flatten().cloned());
        apply_expression_roots(roots.iter(), f)
    }
    fn with_new_children(self: Arc<Self>, mut children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "collect state requires one input".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.schema(),
            self.groups.clone(),
            self.aggregates.clone(),
            self.filters.clone(),
        )))
    }
    fn metrics(&self) -> Option<datafusion_physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let output_schema = schema.clone();
        let groups = self.groups.clone();
        // Prepare the existing native conversion kernels once per execution.
        let mut accumulators = self
            .aggregates
            .iter()
            .map(|expr| expr.create_groups_accumulator())
            .collect::<Result<Vec<Box<dyn GroupsAccumulator>>>>()?;
        let args = self
            .aggregates
            .iter()
            .map(|expr| expr.expressions())
            .collect::<Vec<_>>();
        let filters = self.filters.clone();
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = async_stream::try_stream! {
            while let Some(batch) = input.try_next().await? {
                let timer = metrics.elapsed_compute().timer();
                let n = batch.num_rows();
                let mut columns = groups.iter().map(|expr| expr.evaluate(&batch)?.into_array(n)).collect::<Result<Vec<_>>>()?;
                for ((accumulator, args), filter) in accumulators.iter_mut().zip(&args).zip(&filters) {
                    let values = args.iter().map(|expr| expr.evaluate(&batch)?.into_array(n)).collect::<Result<Vec<_>>>()?;
                    let filter = filter.as_ref().map(|expr| expr.evaluate(&batch)?.into_array(n)).transpose()?;
                    let filter = filter.as_ref().map(|array| array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| DataFusionError::Execution("aggregate filter must be Boolean".into()))).transpose()?;
                    columns.extend(accumulator.convert_to_state(&values, filter)?);
                }
                let result = RecordBatch::try_new(output_schema.clone(), columns)?;
                timer.done();
                metrics.record_output(result.num_rows());
                yield result;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
