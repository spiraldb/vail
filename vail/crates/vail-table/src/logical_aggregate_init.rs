//! Runtime aggregate configuration with logical planning on both sides of the
//! initialization boundary. Placeholder plans validate schemas without reading
//! rows; the same logical lowering runs again with evaluated scalar options.
use crate::globals::GlobalBindings;
use crate::plan::PhysicalPlan;
use crate::{Result, TableError, TableRef};
use arrow_schema::Schema;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::DFSchemaRef;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{
    Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_session::Session;
use futures::FutureExt;
use std::{
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use vail_execution::expressions::Expr;
use vail_execution::logical_expr::lower_logical_with_bindings;

static NEXT: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct InitNode {
    id: u64,
    table: TableRef,
    fields: Vec<(String, Expr)>,
    keys: Option<Vec<(String, Expr)>>,
    bindings: GlobalBindings,
    inputs: Vec<LogicalPlan>,
    schema: DFSchemaRef,
}
impl PartialEq for InitNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for InitNode {}
impl PartialOrd for InitNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl Hash for InitNode {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.id.hash(h)
    }
}
impl UserDefinedLogicalNodeCore for InitNode {
    fn name(&self) -> &str {
        "VailInitializeAggregate"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailInitializeAggregate")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 3 {
            return Err(datafusion_common::DataFusionError::Plan(
                "invalid aggregate initialization rewrite".into(),
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

fn plan(
    table: &TableRef,
    child: LogicalPlan,
    keys: Option<&[(String, Expr)]>,
    fields: &[(String, Expr)],
    bindings: &GlobalBindings,
) -> Result<LogicalPlan> {
    match keys {
        Some(keys) => {
            crate::logical_aggregation::lower_grouped_fields(table, child, keys, fields, bindings)
        }
        None => {
            crate::logical_aggregation::lower_logical_total(table, &fields[0].1, bindings, child)?
                .ok_or_else(|| TableError::Invalid("missing logical total aggregation".into()))
        }
    }
}

pub(crate) fn lower(
    table: &TableRef,
    child: LogicalPlan,
    keys: Option<&[(String, Expr)]>,
    fields: &[(String, Expr)],
    bindings: &GlobalBindings,
) -> Result<Option<LogicalPlan>> {
    let mut parameters = Vec::new();
    let placeholders = fields
        .iter()
        .map(|(name, expr)| {
            let expr = crate::aggregation::bind_initializers(expr, &mut |name, value| {
                let dtype = value
                    .dtype_with_globals(&Schema::empty(), table.globals().schema())?
                    .to_arrow()?;
                parameters.push(value.clone());
                crate::aggregation::initializer_placeholder(name, &dtype)
            })?;
            Ok((name.clone(), expr))
        })
        .collect::<Result<Vec<_>>>()?;
    if parameters.is_empty() {
        return Ok(None);
    }
    let (globals, indices) = crate::logical_globals::bind_external(
        crate::logical_globals::lower(table.globals())?,
        bindings,
    )?;
    let expressions = parameters
        .iter()
        .enumerate()
        .map(|(i, value)| {
            Ok(lower_logical_with_bindings(
                value,
                globals.schema(),
                table.globals().schema(),
                &indices,
            )?
            .alias(format!("init_{i}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let initializers = LogicalPlanBuilder::from(globals)
        .project(expressions)?
        .build()?;
    let template = plan(table, child.clone(), keys, &placeholders, bindings)?;
    let schema = template.schema().clone();
    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(InitNode {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            table: table.clone(),
            fields: fields.to_vec(),
            keys: keys.map(<[_]>::to_vec),
            bindings: bindings.clone(),
            inputs: vec![child, initializers, template],
            schema,
        }),
    })))
}

pub(crate) struct InitializerPlanner;
#[async_trait::async_trait]
impl ExtensionPlanner for InitializerPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[PhysicalPlan],
        _session: &dyn Session,
        _context: &PhysicalPlanningContext,
    ) -> datafusion_common::Result<Option<PhysicalPlan>> {
        let Some(node) = node.as_any().downcast_ref::<InitNode>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 3 {
            return Err(datafusion_common::DataFusionError::Plan(
                "aggregate initialization requires three inputs".into(),
            ));
        }
        let node = node.clone();
        let build = Arc::new(move |batch: arrow_array::RecordBatch| {
            let node = node.clone();
            async move {
                let build = || -> Result<LogicalPlan> {
                    let mut index = 0;
                    let fields = node
                        .fields
                        .iter()
                        .map(|(name, expr)| {
                            let expr = crate::aggregation::bind_initializers(expr, &mut |_, _| {
                                let value =
                                    vail_execution::expressions::scalar_at(batch.column(index), 0)?;
                                index += 1;
                                Ok(value)
                            })?;
                            Ok((name.clone(), expr))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    plan(
                        &node.table,
                        node.inputs[0].clone(),
                        node.keys.as_deref(),
                        &fields,
                        &node.bindings,
                    )
                };
                let logical = build()
                    .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
                let target_partitions = match (node.keys.as_ref(), node.fields.first()) {
                    (None, Some((_, expression))) => {
                        crate::logical::total_aggregate_target(&node.table, expression)
                    }
                    _ => crate::logical::max_partitions(&node.table),
                };
                crate::logical::physical_plan(logical, target_partitions)
                    .await
                    .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))
            }
            .boxed()
        });
        Ok(Some(
            crate::initialized_aggregate::initialized_aggregate_async(
                physical_inputs[1].clone(),
                physical_inputs[2].clone(),
                build,
            ),
        ))
    }
}
