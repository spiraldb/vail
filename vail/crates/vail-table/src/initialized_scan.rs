//! Bind singleton scan configuration before executing partition-local windows.
use crate::plan::PhysicalPlan;
use arrow_array::RecordBatch;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use futures::{TryStreamExt, future::BoxFuture};
use std::{fmt, sync::Arc};
use tokio::sync::OnceCell;

type Build = Arc<
    dyn Fn(RecordBatch) -> BoxFuture<'static, datafusion_common::Result<PhysicalPlan>>
        + Send
        + Sync,
>;

pub(crate) fn initialized_scan(
    initializers: PhysicalPlan,
    template: PhysicalPlan,
    build: Build,
) -> PhysicalPlan {
    Arc::new(InitializedScanExec {
        initializers,
        cache: template.properties().clone(),
        template,
        build,
        initialized: Arc::new(OnceCell::new()),
    })
}
struct InitializedScanExec {
    initializers: PhysicalPlan,
    template: PhysicalPlan,
    build: Build,
    initialized: Arc<OnceCell<PhysicalPlan>>,
    cache: Arc<PlanProperties>,
}
impl fmt::Debug for InitializedScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitializedScanExec")
            .field("initializers", &self.initializers)
            .field("template", &self.template)
            .finish_non_exhaustive()
    }
}
impl DisplayAs for InitializedScanExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InitializedScanExec")
    }
}
impl ExecutionPlan for InitializedScanExec {
    fn name(&self) -> &'static str {
        "InitializedScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        ) -> datafusion_common::Result<
            datafusion_common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.initializers]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "scan initialization requires one singleton child".into(),
            ));
        }
        Ok(initialized_scan(
            children[0].clone(),
            self.template.clone(),
            self.build.clone(),
        ))
    }
    // Physical plans are shared only within one action. Prepared execution
    // resets this node before replay, so configuration is read anew per action.
    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<PhysicalPlan> {
        Ok(initialized_scan(
            self.initializers.clone(),
            self.template.clone(),
            self.build.clone(),
        ))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let initializer = self.initializers.clone();
        let template = self.template.clone();
        let build = self.build.clone();
        let initialized = self.initialized.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            async_stream::try_stream! {
                let plan = initialized.get_or_try_init(|| async {
                    let batches = datafusion_physical_plan::collect(initializer, context.clone()).await?;
                    let mut rows = batches.into_iter().filter(|b| b.num_rows() != 0);
                    let batch = rows.next().ok_or_else(|| DataFusionError::Execution("scan initializer produced no rows".into()))?;
                    if batch.num_rows() != 1 || rows.next().is_some() {
                        return Err(DataFusionError::Execution("scan initializer must produce exactly one row".into()));
                    }
                    let plan = build(batch).await?;
                    if plan.schema() != template.schema() || plan.properties().output_partitioning().partition_count()
                        != template.properties().output_partitioning().partition_count() {
                        return Err(DataFusionError::Plan("initialized scan changed schema or partition count".into()));
                    }
                    Ok(plan)
                }).await?;
                let mut stream = plan.execute(partition, context)?;
                while let Some(batch) = stream.try_next().await? { yield batch; }
            },
        )))
    }
}
