//! Consumer-local read advice for a bounded streaming prefix.
use crate::plan::PhysicalPlan;
use crate::{partitioned, provider, rng_context};
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{coop::CooperativeExec, filter::FilterExec, projection::ProjectionExec, unnest::UnnestExec};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Debug)]
pub(crate) struct ReadPrefix {
    pub(crate) batch_rows: usize,
    pub(crate) output_rows: Option<usize>,
}
impl ReadPrefix {
    pub(crate) fn context(&self, context: &TaskContext) -> Arc<TaskContext> {
        let rows = context.session_config().batch_size().min(self.batch_rows.max(1));
        let config = context.session_config().clone().with_batch_size(rows)
            .with_extension(Arc::new(Self { batch_rows: rows, output_rows: self.output_rows }));
        crate::wide_batch::task_context_with_config(context, config)
    }
}

/// A hard input cap is valid only along a row-preserving path. Dropping nodes
/// retain only batch advice; blocking and unknown nodes retain their old plan.
fn admit(plan: &PhysicalPlan, memo: &mut HashMap<usize, Option<bool>>) -> Option<bool> {
    let id = Arc::as_ptr(plan) as *const () as usize;
    if let Some(result) = memo.get(&id) { return *result; }
    let mut exact = if crate::plan::prefix_source(plan) { true }
        else if let Some(exact) = provider::prefix_node(plan) { exact }
        else if plan.is::<FilterExec>() || plan.is::<UnnestExec>()
            || plan.is::<crate::list_child_unnest::ListChildUnnestExec>() { false }
        else if plan.is::<ProjectionExec>() || plan.is::<CooperativeExec>()
            || plan.is::<partitioned::PartitionViewExec>()
            || plan.is::<partitioned::PartitionSetExec>()
            || plan.is::<rng_context::ConsumerScopeExec>()
            || plan.is::<rng_context::SourceLineageExec>() { true }
        else { memo.insert(id, None); return None; };
    for child in plan.children() {
        match admit(child, memo) { Some(child_exact) => exact &= child_exact, None => { memo.insert(id, None); return None; } }
    }
    memo.insert(id, Some(exact));
    Some(exact)
}

pub(crate) fn prepare(parts: Vec<PhysicalPlan>, rows: usize) -> Result<(Vec<PhysicalPlan>, Option<ReadPrefix>)> {
    let mut memo = HashMap::new();
    let mut exact = true;
    for part in &parts {
        let Some(part_exact) = admit(part, &mut memo) else { return Ok((parts, None)); };
        exact &= part_exact;
    }
    let parts = crate::wide_batch::cap_filter_batches_many(parts, rows)
        .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
    Ok((parts, Some(ReadPrefix { batch_rows: rows, output_rows: exact.then_some(rows) })))
}
