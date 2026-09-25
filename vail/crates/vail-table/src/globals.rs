//! A lazy, single-row relation for table globals. Global updates execute as
//! DataFusion ProjectionExec plans; row plans bind it with a singleton cross join.
use crate::{Result, TableError};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
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
    let mut references = Vec::new();
    for expr in expressions {
        expr.visit(&mut |node| {
            if let Expr::ExternalGlobal { reference, dtype } = node {
                references.push((reference.clone(), dtype.clone()));
            }
        });
    }
    let mut used = GlobalBindings::new();
    for (reference, dtype) in references {
        let target = bindings
            .get(&reference)
            .ok_or_else(|| TableError::Invalid("unbound external globals".into()))?;
        let actual = Type::from_arrow(&arrow_schema::DataType::Struct(
            target.schema().fields().clone(),
        ))?;
        if dtype != actual {
            return Err(TableError::Invalid("external globals type mismatch".into()));
        }
        used.insert(reference, target.clone());
    }
    Ok(used)
}

#[derive(Debug)]
enum Node {
    Empty,
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
    if *value == json!({}) {
        return Ok(Arc::new(Schema::empty()));
    }
    let fields: Vec<(String, Type)> =
        serde_json::from_value(value.clone()).map_err(StorageError::from)?;
    Ok(Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(name, ty)| Ok(Field::new(name, ty.to_arrow()?, true)))
            .collect::<Result<Vec<_>>>()?,
    )))
}

impl GlobalsIR {
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
            Node::Empty => json!({"node":"GlobalsLiteral", "fields":[]}),
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
    pub fn lower(self: &Arc<Self>) -> Result<crate::plan::PhysicalPlan> {
        Ok(match &self.node {
            Node::Empty => {
                let batch = vail_storage::globals::empty()?;
                MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?
            }
            Node::Read(source) => crate::plan::globals_scan(source.clone(), self.schema.clone()),
            Node::Map {
                child,
                fields,
                bindings,
            } => {
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
            let mut batches = collect(globals.lower()?, Arc::new(TaskContext::default())).await?;
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
