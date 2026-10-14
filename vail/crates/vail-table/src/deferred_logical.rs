//! Bridge synchronous legacy operators to the shared logical compiler. The
//! logical subtree is retained intact instead of rebuilding its input through
//! the old physical TableIR planner.
use crate::{Result, plan::PhysicalPlan};
use datafusion_common::{DataFusionError, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_expr::LogicalPlan;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    coalesce_partitions::CoalescePartitionsExec,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};

pub(crate) fn lower(plan: LogicalPlan, partitions: usize) -> Result<PhysicalPlan> {
    let schema = Arc::new(plan.schema().as_arrow().clone());
    Ok(Arc::new(DeferredLogicalExec {
        plan,
        partitions,
        cache: Arc::new(PlanProperties::new(
            datafusion_physical_expr::EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
    }))
}

#[derive(Debug)]
struct DeferredLogicalExec {
    plan: LogicalPlan,
    partitions: usize,
    cache: Arc<PlanProperties>,
}
impl DisplayAs for DeferredLogicalExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeferredLogicalExec: {}", self.plan.display_indent())
    }
}
impl ExecutionPlan for DeferredLogicalExec {
    fn name(&self) -> &'static str {
        "DeferredLogicalExec"
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
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "deferred logical input has no physical children".into(),
            ));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "deferred logical input has one output partition".into(),
            ));
        }
        let plan = self.plan.clone();
        let partitions = self.partitions;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            async_stream::try_stream! {
                let physical = crate::logical::physical_plan(plan, partitions).await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let physical = if physical.properties().output_partitioning().partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(physical)) as PhysicalPlan
                } else { physical };
                let mut stream = physical.execute(0, context)?;
                while let Some(batch) = stream.try_next().await? { yield batch; }
            },
        )))
    }
}
