//! Lazy selection at the boundary of captured singleton dependencies.
use crate::globals::{GlobalsIR, GlobalsRef, Node};
use crate::plan::{PhysicalPlan, properties};
use arrow_array::{Array, BooleanArray, RecordBatch, new_null_array};
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    execution_plan::EmissionType, stream::RecordBatchStreamAdapter,
};
use futures::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, fmt, sync::Arc};

// Exact node identity is retained with an owning Arc, preventing address reuse.
// The cache belongs to one scalar selection, never to a later action.
type Evaluated = HashMap<usize, (GlobalsRef, RecordBatch)>;

fn evaluate<'a>(
    globals: GlobalsRef,
    values: &'a mut Evaluated,
) -> BoxFuture<'a, crate::Result<RecordBatch>> {
    async move {
        let key = Arc::as_ptr(&globals) as usize;
        if let Some((_, batch)) = values.get(&key) {
            return Ok(batch.clone());
        }
        let batch = match globals.node() {
            Node::Conditional {
                condition,
                consequent,
                alternate,
            } => {
                select(
                    condition.clone(),
                    consequent.clone(),
                    alternate.clone(),
                    globals.schema().clone(),
                    values,
                )
                .await?
            }
            Node::Map {
                child,
                fields,
                bindings,
            } => {
                let mut ready = bindings.clone();
                for (name, target) in bindings {
                    // Atomic scalar results can be shared between selector and
                    // chosen branch. Ordinary record maps/reads remain logical
                    // inputs so projection can discard unused record fields.
                    if matches!(
                        target.node(),
                        Node::Aggregate { .. } | Node::Conditional { .. }
                    ) {
                        let batch = evaluate(target.clone(), values).await?;
                        ready.insert(name.clone(), GlobalsIR::arrow_literal(batch)?);
                    }
                }
                child.map_bound(fields.clone(), ready)?.evaluate().await?
            }
            _ => globals.evaluate().await?,
        };
        values.insert(key, (globals.clone(), batch.clone()));
        Ok(batch)
    }
    .boxed()
}

fn select<'a>(
    condition: GlobalsRef,
    consequent: GlobalsRef,
    alternate: GlobalsRef,
    schema: SchemaRef,
    values: &'a mut Evaluated,
) -> BoxFuture<'a, crate::Result<RecordBatch>> {
    async move {
        let condition = evaluate(condition, values).await?;
        let value = condition
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                crate::TableError::Invalid("conditional globals condition is not Boolean".into())
            })?;
        let result = if value.is_null(0) {
            RecordBatch::try_new(
                schema.clone(),
                schema
                    .fields()
                    .iter()
                    .map(|field| new_null_array(field.data_type(), 1))
                    .collect(),
            )?
        } else {
            let selected = if value.value(0) {
                consequent
            } else {
                alternate
            };
            let selected = evaluate(selected, values).await?;
            RecordBatch::try_new(schema, selected.columns().to_vec())?
        };
        Ok(result)
    }
    .boxed()
}

#[derive(Debug)]
pub(crate) struct ConditionalGlobalsExec {
    condition: GlobalsRef,
    consequent: GlobalsRef,
    alternate: GlobalsRef,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}
impl ConditionalGlobalsExec {
    pub(crate) fn new(
        condition: GlobalsRef,
        consequent: GlobalsRef,
        alternate: GlobalsRef,
        schema: SchemaRef,
    ) -> PhysicalPlan {
        Arc::new(Self {
            condition,
            consequent,
            alternate,
            cache: properties(schema.clone(), EmissionType::Incremental),
            schema,
        })
    }
}
impl DisplayAs for ConditionalGlobalsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailConditionalGlobalsExec")
    }
}
impl ExecutionPlan for ConditionalGlobalsExec {
    fn name(&self) -> &'static str {
        "VailConditionalGlobalsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    // Branches are immutable deferred singleton IR, intentionally not physical
    // children: planning/binding an unused branch must not execute its sources.
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
                "conditional globals has no physical children".into(),
            ));
        }
        Ok(self)
    }
    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "conditional globals requires partition zero".into(),
            ));
        }
        let condition = self.condition.clone();
        let consequent = self.consequent.clone();
        let alternate = self.alternate.clone();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                let mut values=Evaluated::new();
                let result=select(condition,consequent,alternate,schema,&mut values).await.map_err(|error|DataFusionError::External(Box::new(error)))?;
                yield result;
            },
        )))
    }
}
