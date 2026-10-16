//! Action-scoped singleton parameter binding, analogous to scalar subqueries.
//! Only configuration rows are executed here. The complete data logical tree is
//! then optimized and physically planned once, without runtime plan callbacks.
use crate::{Result, TableError};
use arrow_array::RecordBatch;
use datafusion_execution::TaskContext;
use datafusion_expr::LogicalPlan;
use futures::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, sync::Arc};

pub(crate) struct ActionParameters {
    values: HashMap<(bool, u64), RecordBatch>,
    pub(crate) context: Arc<TaskContext>,
    pub(crate) state: datafusion::execution::SessionState,
}
impl ActionParameters {
    pub(crate) fn with_context(
        state: datafusion::execution::SessionState,
        context: Arc<TaskContext>,
    ) -> Self {
        Self {
            values: HashMap::new(),
            context,
            state,
        }
    }
}

fn singleton<'a>(
    key: (bool, u64),
    initializer: LogicalPlan,
    bindings: &'a mut ActionParameters,
) -> BoxFuture<'a, Result<RecordBatch>> {
    async move {
        if let Some(value) = bindings.values.get(&key) {
            return Ok(value.clone());
        }
        let initializer = resolve(initializer, bindings).await?;
        let physical = crate::logical::compile(initializer, &bindings.state).await?;
        use datafusion_physical_plan::{
            ExecutionPlanProperties, coalesce_partitions::CoalescePartitionsExec,
        };
        let physical = if physical.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(physical)) as crate::plan::PhysicalPlan
        } else {
            physical
        };
        use futures::TryStreamExt;
        let mut rows = physical.execute(0, bindings.context.clone())?;
        let mut value = None;
        while let Some(batch) = rows.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            if batch.num_rows() != 1 || value.is_some() {
                return Err(TableError::Invalid(
                    "initializer must produce exactly one row".into(),
                ));
            }
            value = Some(batch);
        }
        let value =
            value.ok_or_else(|| TableError::Invalid("initializer produced no rows".into()))?;
        bindings.values.insert(key, value.clone());
        Ok(value)
    }
    .boxed()
}

/// Visit borrowed nodes once. An untouched subtree is represented by `None`,
/// so binding-free actions retain the original logical DAG without cloning
/// expressions or rebuilding any nodes.
pub(crate) fn resolve<'a>(
    logical: LogicalPlan,
    bindings: &'a mut ActionParameters,
) -> BoxFuture<'a, Result<LogicalPlan>> {
    async move {
        let changed = resolve_changed(&logical, bindings, &mut HashMap::new()).await?;
        Ok(changed.map(Arc::unwrap_or_clone).unwrap_or(logical))
    }
    .boxed()
}
fn resolve_changed<'a>(
    logical: &'a LogicalPlan,
    bindings: &'a mut ActionParameters,
    memo: &'a mut HashMap<usize, Option<Arc<LogicalPlan>>>,
) -> BoxFuture<'a, Result<Option<Arc<LogicalPlan>>>> {
    async move {
        // All keys refer to this borrowed root's live input graph. Bound
        // temporary roots use a separate resolve invocation and memo.
        let key = logical as *const LogicalPlan as usize;
        if let Some(changed) = memo.get(&key) {
            return Ok(changed.clone());
        }
        if let LogicalPlan::Extension(extension) = logical {
            if let Some(node) = extension
                .node
                .as_any()
                .downcast_ref::<crate::logical_aggregate_init::InitNode>()
            {
                let batch = singleton((false, node.id), node.inputs[1].clone(), bindings).await?;
                let child = resolve_changed(&node.inputs[0], bindings, memo)
                    .await?
                    .map(Arc::unwrap_or_clone)
                    .unwrap_or_else(|| node.inputs[0].clone());
                let resolved = crate::logical_aggregate_init::bind(node, &batch, child)?;
                let resolved = Some(Arc::new(resolve(resolved, bindings).await?));
                memo.insert(key, resolved.clone());
                return Ok(resolved);
            }
            if let Some(node) = extension
                .node
                .as_any()
                .downcast_ref::<crate::logical_scan::ScanInitializationNode>()
            {
                let batch = singleton((true, node.id), node.initializer.clone(), bindings).await?;
                let input = resolve_changed(&node.input, bindings, memo)
                    .await?
                    .map(Arc::unwrap_or_clone)
                    .unwrap_or_else(|| node.input.clone());
                let resolved = crate::logical_scan::bind_initialized(node, &batch, input)?;
                let resolved = Some(Arc::new(resolve(resolved, bindings).await?));
                memo.insert(key, resolved.clone());
                return Ok(resolved);
            }
        }
        let mut children = Vec::new();
        let mut changed = false;
        for input in logical.inputs() {
            let replacement = resolve_changed(input, bindings, memo).await?;
            changed |= replacement.is_some();
            children.push(replacement);
        }
        let resolved = if changed {
            Some(Arc::new(replace_changed_inputs(logical, children)?))
        } else {
            None
        };
        memo.insert(key, resolved.clone());
        Ok(resolved)
    }
    .boxed()
}

/// Preserve untouched native input Arcs. DataFusion's generic with_new_exprs
/// accepts owned children and rewraps them, losing this sharing even when only
/// one sibling changed. Other extension contracts still rebuild explicitly.
fn replace_changed_inputs(
    logical: &LogicalPlan,
    children: Vec<Option<Arc<LogicalPlan>>>,
) -> Result<LogicalPlan> {
    let mut result = logical.clone();
    let mut children = children.into_iter();
    let mut replace = |input: &mut Arc<LogicalPlan>| {
        if let Some(Some(changed)) = children.next() {
            *input = changed;
        }
    };
    match &mut result {
        LogicalPlan::Projection(node) => replace(&mut node.input),
        LogicalPlan::Filter(node) => replace(&mut node.input),
        LogicalPlan::Window(node) => replace(&mut node.input),
        LogicalPlan::Aggregate(node) => replace(&mut node.input),
        LogicalPlan::Sort(node) => replace(&mut node.input),
        LogicalPlan::Join(node) => {
            replace(&mut node.left);
            replace(&mut node.right);
        }
        LogicalPlan::Repartition(node) => replace(&mut node.input),
        LogicalPlan::Union(node) => {
            for input in &mut node.inputs {
                replace(input);
            }
        }
        LogicalPlan::SubqueryAlias(node) => replace(&mut node.input),
        LogicalPlan::Limit(node) => replace(&mut node.input),
        LogicalPlan::Unnest(node) => replace(&mut node.input),
        _ => {
            let inputs = logical
                .inputs()
                .into_iter()
                .zip(children)
                .map(|(original, changed)| {
                    changed
                        .map(Arc::unwrap_or_clone)
                        .unwrap_or_else(|| original.clone())
                })
                .collect();
            return Ok(logical.with_new_exprs(logical.expressions(), inputs)?);
        }
    }
    Ok(result)
}

/// Retain one action's runtime resources through execution. The public table
/// stream API predates action contexts; its caller-supplied default context must
/// not create a second memory-pool/session island after scalar binding.
pub(crate) fn with_context(
    input: crate::plan::PhysicalPlan,
    context: Arc<TaskContext>,
) -> crate::plan::PhysicalPlan {
    Arc::new(ActionContextExec { input, context })
}
#[derive(Debug)]
struct ActionContextExec {
    input: crate::plan::PhysicalPlan,
    context: Arc<TaskContext>,
}
impl datafusion_physical_plan::DisplayAs for ActionContextExec {
    fn fmt_as(
        &self,
        _: datafusion_physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "VailActionContextExec")
    }
}
impl datafusion_physical_plan::ExecutionPlan for ActionContextExec {
    fn name(&self) -> &'static str {
        "VailActionContextExec"
    }
    fn properties(&self) -> &Arc<datafusion_physical_plan::PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&crate::plan::PhysicalPlan> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<crate::plan::PhysicalPlan>,
    ) -> datafusion_common::Result<crate::plan::PhysicalPlan> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Plan(
                "action context requires one data child".into(),
            ));
        }
        Ok(with_context(children[0].clone(), self.context.clone()))
    }
    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> datafusion_common::Result<datafusion_physical_plan::SendableRecordBatchStream> {
        self.input.execute(partition, self.context.clone())
    }
}
