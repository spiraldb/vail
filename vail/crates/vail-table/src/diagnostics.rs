//! Nonexecuting representations of action planning dependencies.
//!
//! Scalar parameters and data-dependent partition boundaries are intentionally
//! unbound here. An explain request must not run those relations simply to make
//! the physical description more concrete. Executable action planning resolves
//! them through its normal, separate path.
use crate::plan::PhysicalPlan;
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
};
use std::{fmt, sync::Arc};

pub(crate) fn unbound(label: String, schema: SchemaRef, inputs: Vec<PhysicalPlan>) -> PhysicalPlan {
    Arc::new(UnboundActionExec {
        label,
        inputs,
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
    })
}
#[derive(Debug)]
struct UnboundActionExec {
    label: String,
    inputs: Vec<PhysicalPlan>,
    properties: Arc<PlanProperties>,
}
impl DisplayAs for UnboundActionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnboundActionExec: {} [diagnostic only; dependencies were not executed]",
            self.label
        )
    }
}
impl ExecutionPlan for UnboundActionExec {
    fn name(&self) -> &'static str {
        "UnboundActionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        self.inputs.iter().collect()
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, inputs: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if inputs.len() != self.inputs.len() {
            return Err(DataFusionError::Plan(
                "invalid diagnostic dependency rewrite".into(),
            ));
        }
        Ok(unbound(self.label.clone(), self.schema(), inputs))
    }
    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Plan("a diagnostic plan cannot execute; prepare a new action to bind its parameters and dependencies".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unbound_diagnostics_cannot_be_mistaken_for_executable_actions() {
        let plan = unbound(
            "unbound test configuration".into(),
            Arc::new(arrow_schema::Schema::empty()),
            vec![],
        );
        match plan.execute(0, Arc::new(TaskContext::default())) {
            Err(error) => assert!(error.to_string().contains("diagnostic plan cannot execute")),
            Ok(_) => panic!("unbound diagnostics must not execute"),
        }
    }
}
