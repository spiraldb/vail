//! Singleton aggregate configuration is resolved before data physical planning.
//! Placeholder logical plans validate schemas and expose dependencies to EXPLAIN.
//! Action binding replaces them before optimizing the complete data relation.
use crate::globals::GlobalBindings;
use crate::{Result, TableError, TableRef};
use arrow_schema::Schema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::{Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore};
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
pub(crate) struct InitNode {
    pub(crate) id: u64,
    table: TableRef,
    fields: Vec<(String, Expr)>,
    keys: Option<Vec<(String, Expr)>>,
    bindings: GlobalBindings,
    pub(crate) inputs: Vec<LogicalPlan>,
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

/// Bind the scalar configuration before physical planning. Neither the
/// placeholder template nor any input data physical plan has been compiled yet.
pub(crate) fn bind(
    node: &InitNode,
    batch: &arrow_array::RecordBatch,
    child: LogicalPlan,
) -> Result<LogicalPlan> {
    let mut index = 0;
    let fields = node
        .fields
        .iter()
        .map(|(name, expr)| {
            let expr = crate::aggregation::bind_initializers(expr, &mut |_, _| {
                let value = vail_execution::expressions::scalar_at(batch.column(index), 0)?;
                index += 1;
                Ok(value)
            })?;
            Ok((name.clone(), expr))
        })
        .collect::<Result<Vec<_>>>()?;
    if index != batch.num_columns() {
        return Err(TableError::Invalid(
            "aggregate initializer arity mismatch".into(),
        ));
    }
    plan(
        &node.table,
        child,
        node.keys.as_deref(),
        &fields,
        &node.bindings,
    )
}
