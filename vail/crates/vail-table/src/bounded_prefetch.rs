//! Partition-local FIFO prefetch before a consumer which pulls unevenly.
//!
//! Each partition's queue holds at most 32 batches. Its independent byte quota
//! is min(32 MiB, max(1 byte, 256 MiB / input partitions)) of Arrow-reported
//! bytes. The 256 MiB figure is this plan's aggregate target, not a shared
//! semaphore or a whole-query memory limit.
//! One oversized batch may occupy all local credit. One upstream batch can be
//! computed or awaiting byte credit while holding a queue-slot reservation.
//! That pending batch, downstream-held batches, and foreign owners retained by
//! Arrow views are outside the byte budget; it is not an RSS cap.
use crate::plan::PhysicalPlan;
use arrow_array::RecordBatch;
use datafusion_common::{DataFusionError, Result, Statistics, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const BATCHES: usize = 32;
const MAX_REPORTED_BYTES: u32 = 32 * 1024 * 1024;
const REPORTED_TARGET: usize = 256 * 1024 * 1024;

/// Consume the policy captured once at the action's preparation boundary.
pub(crate) fn maybe_wrap(input: PhysicalPlan, enabled: bool) -> PhysicalPlan {
    if enabled {
        wrap(input)
    } else {
        input
    }
}

pub(crate) fn wrap(input: PhysicalPlan) -> PhysicalPlan {
    PrefetchExec::create(
        input,
        std::env::var_os("VAIL_TRACE_BOUNDED_PREFETCH").is_some(),
    )
}

#[derive(Debug)]
struct PrefetchExec {
    input: PhysicalPlan,
    trace: bool,
    reported_bytes: u32,
}
impl PrefetchExec {
    fn create(input: PhysicalPlan, trace: bool) -> PhysicalPlan {
        let partitions = input.properties().partitioning.partition_count().max(1);
        let reported_bytes =
            (REPORTED_TARGET / partitions).clamp(1, MAX_REPORTED_BYTES as usize) as u32;
        Arc::new(Self {
            input,
            trace,
            reported_bytes,
        })
    }
}
impl DisplayAs for PrefetchExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BoundedPrefetchExec: batches={BATCHES}, reported_bytes={} per_partition, reported_target_bytes={REPORTED_TARGET}, partitions={}",
            self.reported_bytes,
            self.input.properties().partitioning.partition_count()
        )
    }
}
impl ExecutionPlan for PrefetchExec {
    fn name(&self) -> &str {
        "BoundedPrefetchExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn apply_expressions(
        &self,
        _visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 || children[0].schema() != self.schema() {
            return Err(DataFusionError::Plan(
                "prefetch requires one child with the same schema".into(),
            ));
        }
        Ok(Self::create(children[0].clone(), self.trace))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Synchronous execution failures remain synchronous. Each execute owns
        // its queue and byte credit, so a different merge partition cannot
        // consume this partition's credit and block the next required key.
        let input = self.input.execute(partition, context)?;
        if self.trace {
            eprintln!(
                "VAIL_BOUNDED_PREFETCH_EXEC partition={partition} batches={BATCHES} reported_bytes={} reported_target_bytes={REPORTED_TARGET} partitions={}",
                self.reported_bytes,
                self.input.properties().partitioning.partition_count()
            );
        }
        Ok(stream(input, BATCHES, self.reported_bytes))
    }
}

struct ProducerGuard(tokio::task::AbortHandle);
impl Drop for ProducerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type Queued = (RecordBatch, OwnedSemaphorePermit);

fn stream(
    mut input: SendableRecordBatchStream,
    batches: usize,
    reported_bytes: u32,
) -> SendableRecordBatchStream {
    assert!(batches > 0 && reported_bytes > 0);
    let schema = input.schema();
    let (sender, mut receiver) = mpsc::channel::<Queued>(batches);
    let bytes = Arc::new(Semaphore::new(reported_bytes as usize));
    let task = tokio::spawn(async move {
        loop {
            // A full FIFO must not ask upstream to compute another batch.
            let Ok(slot) = sender.reserve().await else {
                return Ok::<_, DataFusionError>(());
            };
            let Some(batch) = input.try_next().await? else {
                return Ok(());
            };
            // An oversized batch waits for all existing byte credit and then
            // consumes it all. At most this one pending computed batch sits
            // outside the credited queue while waiting.
            let credit = batch.get_array_memory_size().min(reported_bytes as usize) as u32;
            let permit = bytes
                .clone()
                .acquire_many_owned(credit)
                .await
                .map_err(|_| DataFusionError::Execution("prefetch byte credit closed".into()))?;
            slot.send((batch, permit));
        }
    });
    // Construct the guard before the generator: dropping even an unpolled
    // output stream aborts its producer and drops its upstream stream.
    let cancel = ProducerGuard(task.abort_handle());
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        async_stream::try_stream! {
            let _cancel=cancel;
            while let Some((batch,credit))=receiver.recv().await {
                drop(credit);
                yield batch;
            }
            // The sender closes only when production ends, errors, or panics.
            // Preserve FIFO delivery of already queued rows before that failure.
            task.await.map_err(|error|DataFusionError::Execution(format!("prefetch producer failed: {error}")))??;
        },
    ))
}

#[cfg(test)]
#[path = "bounded_prefetch_tests.rs"]
mod tests;
