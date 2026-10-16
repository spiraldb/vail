//! A lazy, single-row relation for table globals. Global updates execute as
//! DataFusion ProjectionExec plans; row plans bind it with a singleton cross join.
use crate::{Result, TableError};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{execution_plan::collect, projection::ProjectionExec};
use futures::{FutureExt, future::BoxFuture};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use vail_execution::expressions::{
    Expr, Type, lower_with_bindings, validate_projection_with_globals,
};
use vail_storage::{
    StorageError,
    local_table::{LocalRead, normalize},
};

pub type GlobalsRef = Arc<GlobalsIR>;
pub type GlobalBindings = BTreeMap<String, GlobalsRef>;

/// Validate external expression types against their native IR targets and retain
/// only referenced bindings. A malformed wire expression cannot forge a type.
pub fn referenced_bindings<'a>(
    expressions: impl IntoIterator<Item = &'a Expr>,
    bindings: GlobalBindings,
) -> Result<GlobalBindings> {
    let mut pending = expressions.into_iter().collect::<Vec<_>>();
    let mut seen = std::collections::HashSet::new();
    let mut actual_types = BTreeMap::new();
    let mut used = GlobalBindings::new();
    while let Some(expr) = pending.pop() {
        if !seen.insert(expr as *const Expr) {
            continue;
        }
        if let Expr::ExternalGlobal { reference, dtype } = expr {
            let target = bindings
                .get(reference)
                .ok_or_else(|| TableError::Invalid("unbound external globals".into()))?;
            if !actual_types.contains_key(reference) {
                actual_types.insert(
                    reference,
                    Type::from_arrow(&arrow_schema::DataType::Struct(
                        target.schema().fields().clone(),
                    ))?,
                );
            }
            if actual_types.get(reference) != Some(dtype) {
                return Err(TableError::Invalid("external globals type mismatch".into()));
            }
            used.insert(reference.clone(), target.clone());
        }
        expr.visit_children(&mut |child| pending.push(child));
    }
    Ok(used)
}

#[derive(Debug)]
pub(crate) enum Node {
    Empty,
    Conditional {
        condition: GlobalsRef,
        consequent: GlobalsRef,
        alternate: GlobalsRef,
    },
    Prepared(crate::plan::PhysicalPlan),
    ArrowLiteral(RecordBatch),
    Aggregate {
        table: crate::TableRef,
        expression: Expr,
        bindings: GlobalBindings,
    },
    Read(Arc<LocalRead>),
    Map {
        child: GlobalsRef,
        fields: Vec<(String, Expr)>,
        bindings: GlobalBindings,
    },
}
#[derive(Debug)]
pub struct GlobalsIR {
    schema: SchemaRef,
    node: Node,
}

pub fn schema_from_wire(value: &Value) -> Result<SchemaRef> {
    Ok(vail_storage::globals::schema_from_wire(value)?)
}

impl GlobalsIR {
    /// Retain one Arrow row as a literal globals value. Arrow IPC is the wire
    /// boundary; the native IR never serializes the contained values as JSON.
    pub fn arrow_literal(batch: RecordBatch) -> Result<GlobalsRef> {
        vail_storage::globals::validate_schema(batch.schema_ref())?;
        if batch.num_rows() != 1 {
            return Err(TableError::Invalid(
                "Arrow globals literal must contain exactly one row".into(),
            ));
        }
        Ok(Arc::new(Self {
            schema: batch.schema(),
            node: Node::ArrowLiteral(batch),
        }))
    }
    pub fn aggregate(
        table: crate::TableRef,
        expression: Expr,
        bindings: GlobalBindings,
    ) -> Result<GlobalsRef> {
        let bindings = referenced_bindings([&expression], bindings)?;
        // Keep schema derivation on the same logical tree used by execution.
        // The legacy physical aggregate planner cannot consume a MapRows child
        // whose expressions include scan windows.
        let logical_schema = crate::logical::lower(&table)?;
        let schema = match crate::logical_aggregation::lower_logical_total(
            &table,
            &expression,
            &bindings,
            logical_schema,
        )? {
            Some(plan) => Arc::new(plan.schema().as_arrow().clone()),
            None => {
                return Err(crate::TableError::Invalid(
                    "missing logical aggregate plan".into(),
                ));
            }
        };
        Ok(Arc::new(Self {
            schema,
            node: Node::Aggregate {
                table,
                expression,
                bindings,
            },
        }))
    }

    /// Select one validated singleton relation only after evaluating its Boolean
    /// selector. Unused branch dependencies are neither bound nor executed.
    pub fn conditional(
        condition: GlobalsRef,
        consequent: GlobalsRef,
        alternate: GlobalsRef,
    ) -> Result<GlobalsRef> {
        if condition.schema.fields().len() != 1
            || condition.schema.field(0).data_type() != &arrow_schema::DataType::Boolean
            || consequent.schema.fields().len() != 1
            || alternate.schema.fields().len() != 1
        {
            return Err(TableError::Invalid(
                "conditional globals schema mismatch".into(),
            ));
        }
        let field = consequent.schema.field(0).clone().with_nullable(true);
        if field != alternate.schema.field(0).clone().with_nullable(true)
            || consequent.schema.metadata() != alternate.schema.metadata()
        {
            return Err(TableError::Invalid(
                "conditional globals branch types differ".into(),
            ));
        }
        let schema = Arc::new(Schema::new_with_metadata(
            vec![field],
            consequent.schema.metadata().clone(),
        ));
        Ok(Arc::new(Self {
            schema,
            node: Node::Conditional {
                condition,
                consequent,
                alternate,
            },
        }))
    }

    pub fn physical_plan(self: &Arc<Self>) -> Result<String> {
        let globals = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TableError::Execution(e.to_string()))?;
            let plan = runtime.block_on(crate::logical::physical_globals(&globals))?;
            Ok(datafusion_physical_plan::displayable(plan.as_ref())
                .indent(true)
                .to_string())
        })
        .join()
        .map_err(|_| TableError::Execution("globals plan diagnostic panicked".into()))?
    }
    pub fn empty() -> GlobalsRef {
        Arc::new(Self {
            schema: Arc::new(Schema::empty()),
            node: Node::Empty,
        })
    }
    pub fn read(source: Arc<LocalRead>) -> Result<GlobalsRef> {
        Ok(Arc::new(Self {
            schema: schema_from_wire(&source.semantics.global_type)?,
            node: Node::Read(source),
        }))
    }
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn logical_type(&self) -> Result<Value> {
        let fields = self
            .schema
            .fields()
            .iter()
            .map(|f| Ok((f.name(), Type::from_arrow(f.data_type())?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(serde_json::to_value(fields).map_err(StorageError::from)?)
    }
    pub fn map(self: &Arc<Self>, fields: Vec<(String, Expr)>) -> Result<GlobalsRef> {
        self.map_bound(fields, GlobalBindings::new())
    }
    pub fn map_bound(
        self: &Arc<Self>,
        fields: Vec<(String, Expr)>,
        bindings: GlobalBindings,
    ) -> Result<GlobalsRef> {
        let bindings = referenced_bindings(fields.iter().map(|(_, expr)| expr), bindings)?;
        // An empty row environment rejects row-indexed expressions in globals.
        let schema = validate_projection_with_globals(&Schema::empty(), &self.schema, &fields)?;
        Ok(Arc::new(Self {
            schema,
            node: Node::Map {
                child: self.clone(),
                fields,
                bindings,
            },
        }))
    }
    pub fn explain(&self) -> Value {
        match &self.node {
            Node::Aggregate {
                table, expression, ..
            } => json!({"node":"TableAggregate", "table":table.explain(), "expression":expression}),
            Node::Conditional {
                condition,
                consequent,
                alternate,
            } => {
                json!({"node":"GlobalsConditional", "condition":condition.explain(), "consequent":consequent.explain(), "alternate":alternate.explain()})
            }
            Node::Prepared(_) => json!({"node":"GlobalsPrepared"}),
            Node::Empty => json!({"node":"GlobalsLiteral", "fields":[]}),
            Node::ArrowLiteral(batch) => {
                json!({"node":"GlobalsArrowLiteral", "fields":batch.num_columns()})
            }
            Node::Read(source) => {
                json!({"node":"GlobalsRead", "metadata":source.metadata_location()})
            }
            Node::Map {
                child,
                fields,
                bindings,
            } => {
                json!({"node":"GlobalsMap", "child":child.explain(), "fields":fields,
                    "bindings": bindings.iter().map(|(name, ir)| (name, ir.explain())).collect::<BTreeMap<_,_>>()})
            }
        }
    }
    pub(crate) fn prepared(plan: crate::plan::PhysicalPlan) -> GlobalsRef {
        Arc::new(Self {
            schema: plan.schema(),
            node: Node::Prepared(plan),
        })
    }

    pub(crate) fn node(&self) -> &Node {
        &self.node
    }

    pub fn lower(self: &Arc<Self>) -> Result<crate::plan::PhysicalPlan> {
        Ok(match &self.node {
            Node::Aggregate { table, .. } => crate::deferred_logical::lower(
                crate::logical_globals::lower(self)?,
                crate::logical::max_partitions(table),
            )?,
            Node::Conditional {
                condition,
                consequent,
                alternate,
            } => crate::conditional_globals::ConditionalGlobalsExec::new(
                condition.clone(),
                consequent.clone(),
                alternate.clone(),
                self.schema.clone(),
            ),
            Node::Prepared(plan) => plan.clone(),
            Node::Empty => {
                let batch = vail_storage::globals::empty()?;
                MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?
            }
            Node::ArrowLiteral(batch) => {
                MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?
            }
            Node::Read(source) => crate::plan::globals_scan(source.clone(), self.schema.clone()),
            Node::Map {
                child,
                fields,
                bindings,
            } => {
                if fields.is_empty() {
                    return Self::empty().lower();
                }
                let (input, indices) = crate::plan::bind_external(child.lower()?, bindings)?;
                let expressions = fields
                    .iter()
                    .map(|(name, expr)| {
                        Ok((
                            lower_with_bindings(expr, &input.schema(), child.schema(), &indices)?,
                            name.clone(),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(ProjectionExec::try_new(expressions, input)?)
            }
        })
    }
    pub fn evaluate(self: &Arc<Self>) -> BoxFuture<'static, Result<RecordBatch>> {
        let globals = self.clone();
        async move {
            let mut batches = collect(
                crate::logical::physical_globals(&globals).await?,
                Arc::new(TaskContext::default()),
            )
            .await?;
            if batches.len() != 1 || batches[0].num_rows() != 1 {
                return Err(TableError::Invalid(
                    "global projection must produce exactly one row".into(),
                ));
            }
            Ok(normalize(&batches.remove(0), globals.schema.clone())?)
        }
        .boxed()
    }
}
