//! Lazy singleton selection in the action scalar-binding phase.
//! All dependencies remain logical children; no execution operator plans IR.
use crate::{Result, globals::GlobalsRef};
use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::{collections::HashSet, fmt, sync::Arc};

#[derive(Debug, Clone)]
pub(crate) struct ConditionalNode {
    pub(crate) identities: [u64; 3],
    pub(crate) inputs: Vec<LogicalPlan>,
    pub(crate) schema: DFSchemaRef,
    // Retain identity owners throughout action resolution.
    owners: [GlobalsRef; 3],
}
impl PartialEq for ConditionalNode {
    fn eq(&self, other: &Self) -> bool {
        self.identities == other.identities
    }
}
impl Eq for ConditionalNode {}
impl PartialOrd for ConditionalNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ConditionalNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.identities.cmp(&other.identities)
    }
}
impl std::hash::Hash for ConditionalNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.identities.hash(state);
    }
}
impl UserDefinedLogicalNodeCore for ConditionalNode {
    fn name(&self) -> &str {
        "VailConditionalSingleton"
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
        write!(
            f,
            "VailConditionalSingleton: condition, consequent, alternate"
        )
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 3 {
            return Err(datafusion_common::DataFusionError::Plan(
                "invalid conditional singleton rewrite".into(),
            ));
        }
        Ok(Self {
            inputs,
            ..self.clone()
        })
    }
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }
}
pub(crate) fn lower(
    condition: &GlobalsRef,
    consequent: &GlobalsRef,
    alternate: &GlobalsRef,
    schema: &arrow_schema::SchemaRef,
    context: &mut crate::logical::LoweringContext,
) -> Result<LogicalPlan> {
    let owners = [condition.clone(), consequent.clone(), alternate.clone()];
    let identities = owners
        .each_ref()
        .map(|owner| Arc::as_ptr(owner) as usize as u64);
    let inputs = owners
        .iter()
        .map(|globals| crate::logical_globals::lower_in_context(globals, context))
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Extension(datafusion_expr::Extension {
        node: Arc::new(ConditionalNode {
            identities,
            inputs,
            schema: Arc::new(datafusion_common::DFSchema::try_from(
                schema.as_ref().clone(),
            )?),
            owners,
        }),
    }))
}
