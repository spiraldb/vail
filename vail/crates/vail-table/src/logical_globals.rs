//! Singleton globals participate in the same optimizable logical query tree.
use crate::{
    Result, TableRef,
    globals::{GlobalBindings, GlobalsRef, Node},
    plan::PhysicalPlan,
};
use arrow_array::StructArray;
use datafusion::{
    datasource::{MemTable, provider_as_source},
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError};
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore, lit,
};
use datafusion_session::Session;
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use vail_execution::logical_expr::lower_logical_with_bindings;

static NEXT_GLOBAL: AtomicU64 = AtomicU64::new(1);

pub(crate) fn lower(globals: &GlobalsRef) -> Result<LogicalPlan> {
    match globals.node() {
        Node::Empty => Ok(LogicalPlanBuilder::empty(true).build()?),
        Node::ArrowLiteral(batch) => {
            let provider = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])?;
            Ok(LogicalPlanBuilder::scan(
                "vail_globals_literal",
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?)
        }
        Node::Map {
            child,
            fields,
            bindings,
        } => {
            // Selecting no globals has no dependency on the former environment.
            // Avoid expanding discarded zip-global DAGs before optimization.
            if fields.is_empty() { return Ok(LogicalPlanBuilder::empty(true).build()?); }
            let input = lower(child)?;
            let (input, indices) = bind_external(input, bindings)?;
            let expressions =
                fields
                    .iter()
                    .map(|(name, expr)| {
                        Ok(lower_logical_with_bindings(
                            expr,
                            input.schema(),
                            child.schema(),
                            &indices,
                        )?
                        .alias(name))
                    })
                    .collect::<Result<Vec<_>>>()?;
            Ok(LogicalPlanBuilder::from(input)
                .project(expressions)?
                .build()?)
        }
        Node::Aggregate {
            table,
            expression,
            bindings,
        } => {
            let input = crate::logical::lower_for_expressions(table, std::iter::once(expression))?;
            if let Some(plan) = crate::logical_aggregation::lower_logical_total(
                table,
                expression,
                bindings,
                input.clone(),
            )? {
                return Ok(plan);
            }
            Err(crate::TableError::Invalid(
                "missing logical aggregate plan".into(),
            ))
        }
        Node::Read(_) | Node::Prepared(_) | Node::Conditional { .. } => extension(globals, vec![]),
    }
}

fn extension(globals: &GlobalsRef, inputs: Vec<LogicalPlan>) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(GlobalNode {
            id: NEXT_GLOBAL.fetch_add(1, Ordering::Relaxed),
            globals: globals.clone(),
            inputs,
            schema: Arc::new(DFSchema::try_from(globals.schema().as_ref().clone())?),
        }),
    }))
}

pub(crate) fn bind_globals(input: LogicalPlan, table: &TableRef) -> Result<LogicalPlan> {
    if table.globals().schema().fields().is_empty() {
        return Ok(input);
    }
    let globals = LogicalPlanBuilder::from(lower(table.globals())?)
        .alias("__vail_globals")?
        .build()?;
    let input = LogicalPlanBuilder::from(input)
        .alias("__vail_rows")?
        .build()?;
    Ok(LogicalPlanBuilder::from(globals)
        .cross_join(input)?
        .build()?)
}

pub(crate) fn bind_external(
    mut input: LogicalPlan,
    bindings: &GlobalBindings,
) -> Result<(LogicalPlan, BTreeMap<String, usize>)> {
    let mut indices = BTreeMap::new();
    for (ordinal, (reference, globals)) in bindings.iter().enumerate() {
        let bound = lower(globals)?;
        let mut arguments = Vec::new();
        for column in bound.schema().columns() {
            arguments.push(lit(column.name.clone()));
            arguments.push(Expr::Column(column));
        }
        let value = if arguments.is_empty() {
            lit(datafusion_common::ScalarValue::Struct(Arc::new(
                StructArray::new_empty_fields(1, None),
            )))
        } else {
            datafusion_functions::core::named_struct().call(arguments)
        };
        let mut name = format!("__vail_external_global_{ordinal}");
        while input.schema().fields().iter().any(|f| f.name() == &name) {
            name.push('_');
        }
        let bound = LogicalPlanBuilder::from(bound)
            .project(vec![value.alias(&name)])?
            .build()?;
        indices.insert(reference.clone(), input.schema().fields().len());
        let mut columns = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        columns.extend(bound.schema().columns().into_iter().map(Expr::Column));
        input = LogicalPlanBuilder::from(bound)
            .cross_join(input)?
            .project(columns)?
            .build()?;
    }
    Ok((input, indices))
}

#[derive(Debug, Clone)]
struct GlobalNode {
    id: u64,
    globals: GlobalsRef,
    inputs: Vec<LogicalPlan>,
    schema: DFSchemaRef,
}
impl PartialEq for GlobalNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for GlobalNode {}
impl PartialOrd for GlobalNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl Hash for GlobalNode {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.id.hash(h)
    }
}
impl UserDefinedLogicalNodeCore for GlobalNode {
    fn name(&self) -> &str {
        "VailGlobalsOperation"
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
        write!(f, "VailGlobalsOperation: SingletonSource")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != self.inputs.len() {
            return Err(DataFusionError::Plan(
                "invalid globals extension rewrite".into(),
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

pub(crate) struct GlobalsExtensionPlanner;
#[async_trait::async_trait]
impl ExtensionPlanner for GlobalsExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[PhysicalPlan],
        _session: &dyn Session,
        _context: &PhysicalPlanningContext,
    ) -> datafusion_common::Result<Option<PhysicalPlan>> {
        let Some(node) = node.as_any().downcast_ref::<GlobalNode>() else {
            return Ok(None);
        };
        let build = || -> Result<PhysicalPlan> {
            match node.globals.node() {
                Node::Read(_) | Node::Prepared(_) | Node::Conditional { .. } => {
                    node.globals.lower()
                }
                _ => Err(crate::TableError::Invalid(
                    "unexpected globals logical extension".into(),
                )),
            }
        };
        build()
            .map(Some)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
