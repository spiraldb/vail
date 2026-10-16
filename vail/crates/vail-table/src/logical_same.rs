//! A comparison action owns both optimized input query trees.
use crate::{Result, TableRef, comparison::SameOptions, plan::PhysicalPlan};
use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    physical_expr::expressions::Literal,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_session::Session;
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) async fn plan(
    left: &TableRef,
    right: &TableRef,
    options: SameOptions,
    pairs: &[(usize, usize)],
) -> Result<PhysicalPlan> {
    crate::logical::physical_plan(logical(left, right, options, pairs)?, 1).await
}

pub(crate) async fn diagnostic(left: &TableRef, right: &TableRef, options: SameOptions, pairs: &[(usize, usize)]) -> Result<PhysicalPlan> {
    crate::logical::diagnostic(logical(left, right, options, pairs)?, 1).await
}

fn logical(left: &TableRef, right: &TableRef, options: SameOptions, pairs: &[(usize, usize)]) -> Result<LogicalPlan> {
    let schema = Schema::new(vec![Field::new("different", DataType::Boolean, false)]);
    let node = SameNode {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        left: left.clone(),
        right: right.clone(),
        options,
        pairs: pairs.to_vec(),
        inputs: vec![
            crate::logical::ordered(crate::logical::lower(left)?),
            crate::logical::ordered(crate::logical::lower(right)?),
        ],
        schema: Arc::new(DFSchema::try_from(schema)?),
    };
    Ok(LogicalPlan::Extension(Extension { node: Arc::new(node) }))
}

#[derive(Debug, Clone)]
struct SameNode {
    id: u64,
    left: TableRef,
    right: TableRef,
    options: SameOptions,
    pairs: Vec<(usize, usize)>,
    inputs: Vec<LogicalPlan>,
    schema: DFSchemaRef,
}
impl PartialEq for SameNode {
    fn eq(&self, rhs: &Self) -> bool {
        self.id == rhs.id
    }
}
impl Eq for SameNode {}
impl PartialOrd for SameNode {
    fn partial_cmp(&self, rhs: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&rhs.id))
    }
}
impl Hash for SameNode {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.id.hash(h)
    }
}
impl UserDefinedLogicalNodeCore for SameNode {
    fn name(&self) -> &str {
        "VailTableSame"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailTableSame")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 2 {
            return Err(DataFusionError::Plan(
                "invalid table comparison rewrite".into(),
            ));
        }
        Ok(Self {
            inputs,
            ..self.clone()
        })
    }
}
pub(crate) struct SameExtensionPlanner;
#[async_trait::async_trait]
impl ExtensionPlanner for SameExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[PhysicalPlan],
        _session: &dyn Session,
        _context: &PhysicalPlanningContext,
    ) -> datafusion_common::Result<Option<PhysicalPlan>> {
        let Some(node) = node.as_any().downcast_ref::<SameNode>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 2 {
            return Err(DataFusionError::Plan(
                "table comparison requires two inputs".into(),
            ));
        }
        let compared = node
            .left
            .same_from_inputs(
                &node.right,
                node.options,
                &node.pairs,
                physical_inputs[0].clone(),
                physical_inputs[1].clone(),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(ProjectionExec::try_new(
            vec![(
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
                    as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                "different".into(),
            )],
            compared,
        )?)))
    }
}
