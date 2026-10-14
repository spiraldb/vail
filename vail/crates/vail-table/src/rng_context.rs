//! An executable boundary around an explicitly numbered RNG context.
//!
//! The context producer is fully optimized once before being hidden here.
//! Consumers may take independent partition views without tree optimizers
//! cloning a stateful repartitioner while retaining its shared input stream.
use crate::plan::PhysicalPlan;
use datafusion_common::{DataFusionError, Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use std::{
    collections::HashSet,
    fmt,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub(crate) struct RngContextExec {
    input: PhysicalPlan,
    active: Mutex<Vec<Run>>,
}
#[derive(Debug)]
struct Run {
    input: PhysicalPlan,
    used: HashSet<usize>,
}
impl RngContextExec {
    pub(crate) fn new(input: PhysicalPlan) -> Self {
        Self {
            input,
            active: Mutex::new(Vec::new()),
        }
    }
}
impl DisplayAs for RngContextExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RngContextExec\n{}",
            datafusion_physical_plan::displayable(self.input.as_ref()).indent(true)
        )
    }
}
impl ExecutionPlan for RngContextExec {
    fn name(&self) -> &'static str {
        "RngContextExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "RNG context has no rewritable children".into(),
            ));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> Result<PhysicalPlan> {
        Ok(Arc::new(Self::new(self.input.clone())))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // A partition is consumable once by DataFusion's repartitioner. A
        // second request identifies a replay (for example the summary and
        // local branches of a scan). Keep in-flight streams on their own run,
        // and start fresh operator state for the new consumer. Partition views
        // of a single run continue sharing one numbered input.
        let input = {
            let mut active = self.active.lock().map_err(|_| {
                DataFusionError::Execution("RNG context execution state was poisoned".into())
            })?;
            let index = match active.iter().position(|run| !run.used.contains(&partition)) {
                Some(index) => index,
                None => {
                    active.push(Run {
                        input: crate::partitioned::reset_plan_states(self.input.clone())?,
                        used: HashSet::new(),
                    });
                    active.len() - 1
                }
            };
            let run = &mut active[index];
            run.used.insert(partition);
            run.input.clone()
        };
        input.execute(partition, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_physical_plan::{Partitioning, repartition::RepartitionExec};
    use futures::TryStreamExt;

    #[tokio::test]
    async fn interleaved_partition_replays_keep_independent_state() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batches = (0..3)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![i * 2, i * 2 + 1])) as ArrayRef],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let source = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let input =
            Arc::new(RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(3)).unwrap());
        let replay = Arc::new(RngContextExec::new(input));
        let context = Arc::new(TaskContext::default());
        for partition in [0, 0, 1, 1, 2, 2] {
            let batches = replay
                .execute(partition, context.clone())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let values = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, vec![partition as i64 * 2, partition as i64 * 2 + 1]);
        }
        assert_eq!(replay.active.lock().unwrap().len(), 2);
        let fresh = replay.reset_state().unwrap();
        let batches = fresh
            .execute(0, context)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
    }
}
