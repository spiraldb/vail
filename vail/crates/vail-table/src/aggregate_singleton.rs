//! Identity binding for a total aggregate reused within one action.
use crate::globals::GlobalsRef;
use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::{collections::HashSet, fmt, sync::Arc};

#[derive(Debug, Clone)]
pub(crate) struct AggregateSingleton {
    pub(crate) id: u64,
    pub(crate) input: LogicalPlan,
    // Keep the identity alive until action resolution has finished.
    _owner: GlobalsRef,
}
impl PartialEq for AggregateSingleton {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for AggregateSingleton {}
impl PartialOrd for AggregateSingleton {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl std::hash::Hash for AggregateSingleton {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl UserDefinedLogicalNodeCore for AggregateSingleton {
    fn name(&self) -> &str {
        "VailAggregateSingleton"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailAggregateSingleton")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(datafusion_common::DataFusionError::Plan(
                "invalid aggregate singleton rewrite".into(),
            ));
        }
        Ok(Self {
            input: inputs.remove(0),
            ..self.clone()
        })
    }
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }
}
pub(crate) fn wrap(input: LogicalPlan, owner: &GlobalsRef) -> LogicalPlan {
    LogicalPlan::Extension(datafusion_expr::Extension {
        node: Arc::new(AggregateSingleton {
            id: Arc::as_ptr(owner) as usize as u64,
            input,
            _owner: owner.clone(),
        }),
    })
}
