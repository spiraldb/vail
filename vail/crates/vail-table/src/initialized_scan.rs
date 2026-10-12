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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_datasource::memory::MemorySourceConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn schema(name: &str) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]))
    }

    fn batch(schema: Arc<Schema>, values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))]).unwrap()
    }

    fn source(partitions: Vec<Vec<RecordBatch>>, schema: Arc<Schema>) -> PhysicalPlan {
        MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap()
    }

    fn builder(calls: Arc<AtomicUsize>, output: PhysicalPlan) -> Build {
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            let output = output.clone();
            Box::pin(async move { Ok(output) })
        })
    }

    #[tokio::test]
    async fn initializes_lazily_and_only_once_for_concurrent_partitions() {
        use futures::TryStreamExt;

        let init_schema = schema("init");
        let output_schema = schema("value");
        let initializers = source(vec![vec![batch(init_schema.clone(), &[7])]], init_schema);
        let template = source(vec![vec![], vec![]], output_schema.clone());
        let output = source(
            vec![
                vec![batch(output_schema.clone(), &[1])],
                vec![batch(output_schema.clone(), &[2])],
            ],
            output_schema,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let plan = initialized_scan(initializers, template, builder(calls.clone(), output));
        let context = Arc::new(TaskContext::default());

        let left = plan.execute(0, context.clone()).unwrap();
        let right = plan.execute(1, context).unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "execute must not initialize the scan"
        );
        let (left, right) =
            tokio::join!(left.try_collect::<Vec<_>>(), right.try_collect::<Vec<_>>());
        assert_eq!(
            left.unwrap()[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            right.unwrap()[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rejects_non_singleton_initializer() {
        use futures::TryStreamExt;

        let init_schema = schema("init");
        let output_schema = schema("value");
        let initializers = source(vec![vec![batch(init_schema.clone(), &[1, 2])]], init_schema);
        let template = source(vec![vec![]], output_schema.clone());
        let output = source(vec![vec![]], output_schema);
        let plan = initialized_scan(
            initializers,
            template,
            builder(Arc::new(AtomicUsize::new(0)), output),
        );
        assert!(
            plan.execute(0, Arc::new(TaskContext::default()))
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
                .to_string()
                .contains("exactly one row")
        );
    }

    #[tokio::test]
    async fn rejects_initialized_schema_or_partition_count_mismatch() {
        use futures::TryStreamExt;

        for (output_schema, partitions, expected) in [
            (schema("wrong"), 2, "schema or partition count"),
            (schema("value"), 1, "schema or partition count"),
        ] {
            let init_schema = schema("init");
            let template_schema = schema("value");
            let initializers = source(vec![vec![batch(init_schema.clone(), &[1])]], init_schema);
            let template = source(vec![vec![], vec![]], template_schema);
            let output = source((0..partitions).map(|_| vec![]).collect(), output_schema);
            let plan = initialized_scan(
                initializers,
                template,
                builder(Arc::new(AtomicUsize::new(0)), output),
            );
            assert!(
                plan.execute(0, Arc::new(TaskContext::default()))
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }
}
