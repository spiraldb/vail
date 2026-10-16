//! Action-owned batch replay for zip consumers of the same computed source.
//! Requested partitions/cells advance a batch at a time. A shared memory
//! budget bounds retained batches; excess batches spill to action-owned IPC files.
use crate::plan::PhysicalPlan;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt, sync::Arc};
use tokio::sync::Mutex;
// One shared budget across every input and interval cell in this zip. Batch
// ownership is charged while retained; excess batches use action-owned IPC files.
const REPLAY_BYTES: usize = 8 * 1024 * 1024;
#[derive(Debug, Default)]
pub(crate) struct Budget(AtomicUsize);
#[derive(Debug)]
struct MemoryBatch {
    batch: RecordBatch,
    bytes: usize,
    budget: Arc<Budget>,
}
impl Drop for MemoryBatch {
    fn drop(&mut self) {
        self.budget.0.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
#[derive(Clone, Debug)]
enum StoredBatch {
    Memory(Arc<MemoryBatch>),
    Disk(Arc<tempfile::TempPath>),
}
impl StoredBatch {
    async fn store(batch: RecordBatch, budget: Arc<Budget>) -> Result<Self> {
        let bytes = batch.get_array_memory_size();
        if budget
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= REPLAY_BYTES)
            })
            .is_ok()
        {
            return Ok(Self::Memory(Arc::new(MemoryBatch {
                batch,
                bytes,
                budget,
            })));
        }
        tokio::task::spawn_blocking(move || -> Result<Self> {
            let mut file = tempfile::NamedTempFile::new()?;
            {
                let mut writer =
                    arrow_ipc::writer::FileWriter::try_new(file.as_file_mut(), &batch.schema())?;
                writer.write(&batch)?;
                writer.finish()?;
            }
            Ok(Self::Disk(Arc::new(file.into_temp_path())))
        })
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?
    }
    async fn read(self) -> Result<RecordBatch> {
        match self {
            Self::Memory(batch) => Ok(batch.batch.clone()),
            Self::Disk(path) => tokio::task::spawn_blocking(move || -> Result<RecordBatch> {
                let file = std::fs::File::open(path.as_ref() as &std::path::Path)?;
                let mut reader = arrow_ipc::reader::FileReader::try_new(file, None)?;
                reader
                    .next()
                    .transpose()?
                    .ok_or_else(|| DataFusionError::Execution("empty zip replay spill".into()))
            })
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?,
        }
    }
}
#[derive(Default)]
struct ReplayState {
    batches: Vec<StoredBatch>,
    stream: Option<SendableRecordBatchStream>,
    done: bool,
    error: Option<Arc<DataFusionError>>,
}
#[derive(Default)]
struct Replay {
    state: Mutex<ReplayState>,
}
impl fmt::Debug for Replay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZipReplay")
    }
}
impl Replay {
    fn stream(
        self: Arc<Self>,
        input: PhysicalPlan,
        partition: usize,
        context: Arc<TaskContext>,
        schema: SchemaRef,
        budget: Arc<Budget>,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                let mut index=0;
                loop {
                    // Finishing one producer step must survive cancellation of
                    // a consumer while an IPC write is in flight.
                    let replay=self.clone();
                    let producer=input.clone();
                    let task_context=context.clone();
                    let task_budget=budget.clone();
                    let stored=tokio::spawn(async move {
                        let mut state=replay.state.lock().await;
                        if index < state.batches.len() { Ok::<_,DataFusionError>(Some(state.batches[index].clone())) }
                        else if let Some(error)=&state.error { Err(shared(error))?; unreachable!() }
                        else if state.done { Ok(None) }
                        else {
                            if state.stream.is_none() {
                                match producer.execute(partition,task_context) {
                                    Ok(stream)=>state.stream=Some(stream),
                                    Err(error)=>{let error=Arc::new(error);state.error=Some(error.clone());Err(shared(&error))?;}
                                }
                            }
                            match state.stream.as_mut().unwrap().try_next().await {
                                Ok(Some(batch))=>match StoredBatch::store(batch,task_budget).await {
                                    Ok(stored)=>{state.batches.push(stored.clone());Ok(Some(stored))},
                                    Err(error)=>{let error=Arc::new(error);state.error=Some(error.clone());state.stream=None;Err(shared(&error))?;unreachable!()}
                                },
                                Ok(None)=>{state.done=true;state.stream=None;Ok(None)},
                                Err(error)=>{let error=Arc::new(error);state.error=Some(error.clone());state.stream=None;Err(shared(&error))?;unreachable!()}
                            }
                        }
                    }).await.map_err(|e|DataFusionError::Execution(e.to_string()))??;
                    let Some(stored)=stored else { break; };
                    yield stored.read().await?;
                    index+=1;
                }
            },
        ))
    }
}
fn shared(error: &Arc<DataFusionError>) -> DataFusionError {
    DataFusionError::External(Box::new(error.clone()))
}
#[derive(Debug)]
struct Source {
    input: PhysicalPlan,
    batches: Vec<Arc<Replay>>,
    budget: Arc<Budget>,
    limit: Option<usize>,
}
pub(crate) fn source(
    input: PhysicalPlan,
    limit: Option<usize>,
    budget: Arc<Budget>,
) -> PhysicalPlan {
    let count = input.properties().output_partitioning().partition_count();
    Arc::new(Source {
        input,
        limit,
        budget,
        batches: (0..count).map(|_| Arc::new(Replay::default())).collect(),
    })
}
impl DisplayAs for Source {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZipSourceSnapshotExec")
    }
}
impl ExecutionPlan for Source {
    fn name(&self) -> &str {
        "ZipSourceSnapshotExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "zip source snapshot has no rewritable children".into(),
            ));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> Result<PhysicalPlan> {
        Ok(source(
            crate::partitioned::reset_plan_states(self.input.clone())?,
            self.limit,
            self.budget.clone(),
        ))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let cell = self.batches.get(partition).ok_or_else(|| {
            DataFusionError::Execution("zip source partition out of range".into())
        })?;
        // Preparation and bounds discovery belong to this initial action.
        // Explicit reset_state creates a fresh hidden producer DAG for replay.
        let fresh = self.input.clone();
        let view: PhysicalPlan =
            Arc::new(crate::partitioned::PartitionViewExec::new(fresh, partition));
        let input: PhysicalPlan = if let Some(limit) = self.limit {
            Arc::new(datafusion_physical_plan::limit::GlobalLimitExec::new(
                view,
                0,
                Some(limit),
            ))
        } else {
            view
        };
        Ok(cell
            .clone()
            .stream(input, 0, context, self.schema(), self.budget.clone()))
    }
}
#[derive(Debug)]
struct Cell {
    input: PhysicalPlan,
    source: PhysicalPlan,
    batches: Arc<Replay>,
    budget: Arc<Budget>,
}
// Native scheduling may wrap the exposed owner in a schema-preserving unary
// adapter. Retain its real Source ownership proof through those adapters.
fn source_budget(plan: &PhysicalPlan) -> Option<Arc<Budget>> {
    if let Some(source) = (plan.as_ref() as &dyn std::any::Any).downcast_ref::<Source>() {
        return Some(source.budget.clone());
    }
    let children = plan.children();
    if children.len() != 1
        || children[0].schema() != plan.schema()
        || children[0]
            .properties()
            .output_partitioning()
            .partition_count()
            != plan.properties().output_partitioning().partition_count()
    {
        return None;
    }
    source_budget(children[0])
}
pub(crate) fn cell(input: PhysicalPlan, source: PhysicalPlan) -> Result<PhysicalPlan> {
    let budget = source_budget(&source);
    if budget.is_none()
        || input.schema() != source.schema()
        || input.properties().output_partitioning().partition_count() != 1
    {
        return Err(DataFusionError::Plan(format!(
            "zip cell snapshot requires a matching owned source and one ordered stream: input={:?}, source={:?}, partitions={}",
            input.schema(),
            source.schema(),
            input.properties().output_partitioning().partition_count()
        )));
    }
    let budget = budget.unwrap();
    Ok(Arc::new(Cell {
        budget,
        input,
        source,
        batches: Arc::new(Replay::default()),
    }))
}
impl DisplayAs for Cell {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZipCellSnapshotExec")
    }
}
impl ExecutionPlan for Cell {
    fn name(&self) -> &str {
        "ZipCellSnapshotExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    // Expose the shared owner for DAG-preserving action reset, but keep the
    // already prepared cell pipeline opaque to repeated tree optimization.
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.source]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, mut children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "zip cell snapshot requires one source owner".into(),
            ));
        }
        let source = children.remove(0);
        if Arc::ptr_eq(&source, &self.source) {
            return Ok(self);
        }
        let input = crate::partitioned::reset_plan_states_replacing(
            self.input.clone(),
            &[(self.source.clone(), source.clone())],
        )?;
        cell(input, source)
    }
    fn reset_state(self: Arc<Self>) -> Result<PhysicalPlan> {
        cell(self.input.clone(), self.source.clone())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "zip cell snapshot has one partition".into(),
            ));
        }
        Ok(self.batches.clone().stream(
            self.input.clone(),
            0,
            context,
            self.schema(),
            self.budget.clone(),
        ))
    }
}

pub(crate) fn is_cell(plan: &PhysicalPlan) -> bool {
    (plan.as_ref() as &dyn std::any::Any).is::<Cell>()
}
