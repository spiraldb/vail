//! Apply DataFusion's zero-copy batch splitter at real source boundaries.
//!
//! Install after physical optimization, below the action-context wrapper. This
//! does not select a budget or replace a context: the caller's planned batch
//! size must also reach downstream operators that coalesce or emit batches.
//! Opaque operators with hidden sources must expose their children or enforce
//! the same context's limit internally; slicing their output would be too late.
use crate::plan::PhysicalPlan;
use datafusion_common::{
    Result, Statistics,
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    execution_plan::CardinalityEffect,
    metrics::{ExecutionPlanMetricsSet, MetricsSet, SplitMetrics},
    statistics::{ChildStats, StatisticsArgs},
    stream::BatchSplitStream,
};
use std::{fmt, sync::Arc};

pub(crate) fn with_source_batch_limits(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    Ok(plan
        .transform_down(|plan| {
            if plan
                .as_ref()
                .downcast_ref::<SourceBatchLimitExec>()
                .is_some()
            {
                return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
            }
            if !plan.children().is_empty() {
                return Ok(Transformed::no(plan));
            }
            let limited: PhysicalPlan = Arc::new(SourceBatchLimitExec {
                input: plan,
                metrics: ExecutionPlanMetricsSet::new(),
            });
            // Do not visit the original leaf again inside its newly added wrapper.
            Ok(Transformed::new(limited, true, TreeNodeRecursion::Jump))
        })?
        .data)
}

#[derive(Debug)]
struct SourceBatchLimitExec {
    input: PhysicalPlan,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for SourceBatchLimitExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailSourceBatchLimitExec")
    }
}

impl ExecutionPlan for SourceBatchLimitExec {
    fn name(&self) -> &'static str {
        "VailSourceBatchLimitExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        input_stats.first().cloned().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "source batch limit requires input statistics"
            )
        })
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        let [input] = children.as_slice() else {
            return datafusion_common::internal_err!("source batch limit requires one input");
        };
        Ok(Arc::new(Self {
            input: input.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(BatchSplitStream::new(
            input,
            batch_size,
            SplitMetrics::new(&self.metrics, partition),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_execution::config::SessionConfig;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn real_arrow_sources_preserve_partitions_rows_schema_and_replay() {
        let field = Field::new("idx", DataType::Int32, false).with_metadata(
            std::collections::HashMap::from([("test.annotation".into(), "retained".into())]),
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let parts = [vec![0, 1, 2, 3, 4, 5, 6], vec![10, 11, 12, 13, 14]];
        let batches = parts
            .iter()
            .map(|values| {
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int32Array::from(values.clone()))],
                    )
                    .unwrap(),
                ]
            })
            .collect::<Vec<_>>();
        let source = MemorySourceConfig::try_new_exec(&batches, schema.clone(), None).unwrap();
        let statistics = datafusion_physical_plan::statistics::StatisticsContext::new();
        let args = StatisticsArgs::new();
        let expected_stats = statistics.compute(source.as_ref(), &args).unwrap();
        let plan = with_source_batch_limits(source).unwrap();
        // A second application must remain safe for caller-controlled rewrites.
        let plan = with_source_batch_limits(plan).unwrap();
        assert_eq!(
            statistics.compute(plan.as_ref(), &args).unwrap(),
            expected_stats
        );
        assert_eq!(
            plan.properties().partitioning.partition_count(),
            parts.len()
        );
        for batch_size in [1, 3, 8192] {
            let context = Arc::new(
                TaskContext::default()
                    .with_session_config(SessionConfig::new().with_batch_size(batch_size)),
            );
            for (partition, expected) in parts.iter().enumerate() {
                let batches = plan
                    .execute(partition, context.clone())
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let mut actual = Vec::new();
                for batch in batches {
                    assert_eq!(batch.schema(), schema);
                    assert!(batch.num_rows() <= batch_size);
                    actual.extend_from_slice(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values(),
                    );
                }
                assert_eq!(&actual, expected);
            }
        }
    }

    #[tokio::test]
    async fn zero_field_sources_keep_explicit_row_counts() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(7)),
        )
        .unwrap();
        let source =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
        let plan = with_source_batch_limits(source).unwrap();
        let context = Arc::new(
            TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(3)),
        );
        let batches = plan
            .execute(0, context)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [3, 3, 1]
        );
        assert!(batches.iter().all(|batch| batch.schema() == schema));
    }
}
