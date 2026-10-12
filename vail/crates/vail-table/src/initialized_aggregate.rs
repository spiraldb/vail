//! Runtime initialization for aggregates whose initial values are computed by
//! an ordinary DataFusion physical plan.
use crate::plan::PhysicalPlan;
use arrow_array::RecordBatch;
use datafusion_common::{DataFusionError, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};

type BuildAggregate =
    Arc<dyn Fn(RecordBatch) -> datafusion_common::Result<PhysicalPlan> + Send + Sync>;

pub(crate) fn initialized_aggregate(
    initializers: PhysicalPlan,
    template: PhysicalPlan,
    build: BuildAggregate,
) -> PhysicalPlan {
    let schema = template.schema();
    let cache = Arc::new(PlanProperties::new(
        datafusion_physical_expr::EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ));
    Arc::new(InitializeAggregateExec {
        initializers,
        template,
        build,
        cache,
    })
}

struct InitializeAggregateExec {
    initializers: PhysicalPlan,
    // Retained for the standard physical-plan explanation and schema only.
    // Runtime planning happens in `build`; this template is never executed.
    template: PhysicalPlan,
    build: BuildAggregate,
    cache: Arc<PlanProperties>,
}

impl fmt::Debug for InitializeAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitializeAggregateExec")
            .field("initializers", &self.initializers)
            .field("template", &self.template)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for InitializeAggregateExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for InitializeAggregateExec {
    fn name(&self) -> &'static str {
        "InitializeAggregateExec"
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
        vec![&self.initializers]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "initialized aggregate requires one child".into(),
            ));
        }
        Ok(initialized_aggregate(
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
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "initialized aggregate requires a single physical partition".into(),
            ));
        }

        let initializers = self.initializers.clone();
        let expected_schema = self.template.schema();
        let build = self.build.clone();
        let output_schema = expected_schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema.clone(),
            async_stream::try_stream! {
                let mut stream = initializers.execute(0, context.clone())?;
                let mut initializer: Option<RecordBatch> = None;
                while let Some(batch) = stream.try_next().await? {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    if initializer.is_some() || batch.num_rows() != 1 {
                        Err(DataFusionError::Execution(
                            "aggregate initializer plan must produce exactly one row".into(),
                        ))?;
                    }
                    initializer = Some(batch);
                }
                let initializer = initializer.ok_or_else(|| DataFusionError::Execution(
                    "aggregate initializer plan produced no rows".into(),
                ))?;
                let aggregate = build(initializer)?;
                if aggregate.schema().as_ref() != expected_schema.as_ref() {
                    Err(DataFusionError::Plan(format!(
                        "initialized aggregate schema mismatch: expected {:?}, got {:?}",
                        expected_schema,
                        aggregate.schema(),
                    )))?;
                }
                let mut output = aggregate.execute(0, context)?;
                while let Some(batch) = output.try_next().await? {
                    yield batch;
                }
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_datasource::memory::MemorySourceConfig;

    fn batch(schema: arrow_schema::SchemaRef, values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))]).unwrap()
    }

    fn source(batches: Vec<RecordBatch>, schema: arrow_schema::SchemaRef) -> PhysicalPlan {
        MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap()
    }

    fn schema(name: &str) -> arrow_schema::SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]))
    }

    fn initialized(
        initializer_batches: Vec<RecordBatch>,
        build: BuildAggregate,
    ) -> SendableRecordBatchStream {
        let initializer_schema = schema("init");
        let output_schema = schema("value");
        let initializers = source(initializer_batches, initializer_schema);
        let template = source(vec![], output_schema);
        initialized_aggregate(initializers, template, build)
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
    }

    #[tokio::test]
    async fn rejects_empty_initializer() {
        let initializer_schema = schema("init");
        let output_schema = schema("value");
        let template = source(vec![], output_schema);
        let initializers = source(vec![], initializer_schema);
        let build: BuildAggregate = Arc::new(move |_| Ok(source(vec![], schema("value"))));
        let mut stream = initialized_aggregate(initializers, template, build)
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        assert!(
            stream
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("no rows")
        );
    }

    #[tokio::test]
    async fn rejects_multiple_initializer_rows() {
        let init_schema = schema("init");
        let init = batch(init_schema.clone(), &[1, 2]);
        let mut stream = initialized(
            vec![init],
            Arc::new(move |_| Ok(source(vec![], schema("value")))),
        );
        assert!(
            stream
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("exactly one row")
        );
    }

    #[tokio::test]
    async fn rejects_runtime_schema_mismatch() {
        let init_schema = schema("init");
        let init = batch(init_schema, &[1]);
        let mut stream = initialized(
            vec![init],
            Arc::new(move |_| Ok(source(vec![], schema("wrong")))),
        );
        assert!(
            stream
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("schema mismatch")
        );
    }

    #[tokio::test]
    async fn yields_runtime_output_batches_incrementally() {
        let init_schema = schema("init");
        let init = batch(init_schema, &[7]);
        let first = batch(schema("value"), &[11]);
        let second = batch(schema("value"), &[12]);
        let mut stream = initialized(
            vec![init],
            Arc::new(move |_| Ok(source(vec![first.clone(), second.clone()], schema("value")))),
        );
        assert_eq!(
            stream
                .try_next()
                .await
                .unwrap()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            11
        );
        assert_eq!(
            stream
                .try_next()
                .await
                .unwrap()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            12
        );
        assert!(stream.try_next().await.unwrap().is_none());
    }
}
