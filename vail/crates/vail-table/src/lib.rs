//! Immutable, typed TableIR. Construction checks types and shares child nodes;
//! actions lower expression IR and pull Arrow batches through the graph.
pub mod comparison;
pub mod globals;
pub mod plan;
use globals::{GlobalBindings, GlobalsIR, GlobalsRef, referenced_bindings};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use serde_json::{Value, json};
use std::sync::Arc;
use vail_execution::expressions::{
    ExprError, PrimitiveType, Type, validate_projection_with_globals,
};
use vail_execution::{OrderKey, OrderRequirement};
use vail_storage::{
    StorageError,
    local_table::{
        LocalRead, WriteGlobals, WriteOptions, open_local, write_local_stream_with_globals,
    },
};

pub use vail_execution::expressions::Expr as ScalarIR;
pub use vail_storage::local_table::PartitionEvent;
pub type TableRef = Arc<TableIR>;
pub type BatchStream = BoxStream<'static, Result<PartitionEvent>>;
pub type Result<T> = std::result::Result<T, TableError>;

#[derive(Debug, thiserror::Error)]
pub enum TableError {
    #[error("invalid TableIR: {0}")]
    Invalid(String),
    #[error(transparent)]
    Expression(#[from] ExprError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error(transparent)]
    Order(#[from] vail_execution::OrderError),
    #[error(transparent)]
    DataFusion(#[from] datafusion_common::DataFusionError),
    #[error("expression execution failed: {0}")]
    Execution(String),
}

/// Supported Hail TableIR counterparts. Children are shared Arc references, so
/// branching a Python Table creates a DAG without copying source data or plans.
#[derive(Debug)]
pub enum TableNode {
    Read {
        source: Arc<LocalRead>,
    },
    Parallelize {
        rows: RecordBatch,
        n_partitions: usize,
    },
    Range {
        n: usize,
        n_partitions: usize,
    },
    MapGlobals {
        child: TableRef,
    },
    MapRows {
        child: TableRef,
        fields: Vec<(String, ScalarIR)>,
        bindings: GlobalBindings,
    },
    Filter {
        child: TableRef,
        predicate: ScalarIR,
        keep: bool,
        bindings: GlobalBindings,
    },
    KeyBy {
        child: TableRef,
        keys: Vec<String>,
    },
    OrderBy {
        child: TableRef,
        keys: Vec<(String, bool)>,
    },
    Head {
        child: TableRef,
        n: usize,
    },
    Tail {
        child: TableRef,
        n: usize,
    },
}

#[derive(Debug)]
pub struct TableIR {
    schema: SchemaRef,
    keys: Vec<String>,
    node: TableNode,
    globals: GlobalsRef,
}

impl TableIR {
    fn new(schema: SchemaRef, keys: Vec<String>, globals: GlobalsRef, node: TableNode) -> TableRef {
        Arc::new(Self {
            schema,
            keys,
            node,
            globals,
        })
    }
    pub fn globals(&self) -> &GlobalsRef {
        &self.globals
    }
    pub fn map_globals(self: &Arc<Self>, fields: Vec<(String, ScalarIR)>) -> Result<TableRef> {
        self.map_globals_bound(fields, GlobalBindings::new())
    }
    pub fn map_globals_bound(
        self: &Arc<Self>,
        fields: Vec<(String, ScalarIR)>,
        bindings: GlobalBindings,
    ) -> Result<TableRef> {
        let globals = self.globals.map_bound(fields, bindings)?;
        for field in globals.schema().fields() {
            if self.schema.index_of(field.name()).is_ok() {
                return Err(TableError::Invalid(format!(
                    "global field {:?} collides with rows",
                    field.name()
                )));
            }
        }
        Ok(Arc::new(Self {
            schema: self.schema.clone(),
            keys: self.keys.clone(),
            globals,
            node: TableNode::MapGlobals {
                child: self.clone(),
            },
        }))
    }
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn keys(&self) -> &[String] {
        &self.keys
    }
    pub fn node(&self) -> &TableNode {
        &self.node
    }

    pub async fn read(path: &str) -> Result<TableRef> {
        let source = Arc::new(open_local(path).await?);
        let globals = GlobalsIR::read(source.clone())?;
        let fields: Vec<(String, Type)> = serde_json::from_value(source.semantics.row_type.clone())
            .map_err(StorageError::from)?;
        let schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dtype)| Ok(Field::new(name, dtype.to_arrow()?, true)))
                .collect::<std::result::Result<Vec<_>, ExprError>>()?,
        ));
        for field in globals.schema().fields() {
            if schema.index_of(field.name()).is_ok() {
                return Err(TableError::Invalid(
                    "row/global field name collision in snapshot".into(),
                ));
            }
        }
        Ok(Self::new(
            schema,
            source.keys.clone(),
            globals,
            TableNode::Read { source },
        ))
    }

    /// Owning Arrow buffers here is allowed: these are the input context, not an
    /// executed table. Partitioning/slicing happens only when an action polls it.
    pub fn parallelize(rows: RecordBatch, n_partitions: usize) -> Result<TableRef> {
        if n_partitions == 0 {
            return Err(TableError::Invalid("n_partitions must be positive".into()));
        }
        Ok(Self::new(
            rows.schema(),
            vec![],
            GlobalsIR::empty(),
            TableNode::Parallelize { rows, n_partitions },
        ))
    }
    pub fn range(n: usize, n_partitions: usize) -> Result<TableRef> {
        if n_partitions == 0 || n > i32::MAX as usize {
            return Err(TableError::Invalid(
                "range requires positive partitions and n <= 2**31 - 1".into(),
            ));
        }
        Ok(Self::new(
            Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)])),
            vec!["idx".into()],
            GlobalsIR::empty(),
            TableNode::Range {
                n,
                n_partitions: n.min(n_partitions).max(1),
            },
        ))
    }
    pub fn map_rows(self: &Arc<Self>, fields: Vec<(String, ScalarIR)>) -> Result<TableRef> {
        self.map_rows_bound(fields, GlobalBindings::new())
    }
    pub fn map_rows_bound(
        self: &Arc<Self>,
        fields: Vec<(String, ScalarIR)>,
        bindings: GlobalBindings,
    ) -> Result<TableRef> {
        let bindings = referenced_bindings(fields.iter().map(|(_, expr)| expr), bindings)?;
        let schema =
            validate_projection_with_globals(&self.schema, self.globals.schema(), &fields)?;
        for field in schema.fields() {
            if self.globals.schema().index_of(field.name()).is_ok() {
                return Err(TableError::Invalid(format!(
                    "row field {:?} collides with globals",
                    field.name()
                )));
            }
        }
        for key in &self.keys {
            if !fields.iter().any(|(name, expr)| {
                name == key && matches!(expr, ScalarIR::Column { name } if name == key)
            }) {
                return Err(TableError::Invalid(format!(
                    "MapRows must preserve key field {key:?}; unkey before changing it"
                )));
            }
        }
        Ok(Self::new(
            schema,
            self.keys.clone(),
            self.globals.clone(),
            TableNode::MapRows {
                child: self.clone(),
                fields,
                bindings,
            },
        ))
    }
    pub fn filter(self: &Arc<Self>, predicate: ScalarIR, keep: bool) -> Result<TableRef> {
        self.filter_bound(predicate, keep, GlobalBindings::new())
    }
    pub fn filter_bound(
        self: &Arc<Self>,
        predicate: ScalarIR,
        keep: bool,
        bindings: GlobalBindings,
    ) -> Result<TableRef> {
        let bindings = referenced_bindings([&predicate], bindings)?;
        if predicate.dtype_with_globals(&self.schema, self.globals.schema())?
            != Type::Primitive(PrimitiveType::Bool)
        {
            return Err(TableError::Invalid("filter predicate must be bool".into()));
        }
        Ok(Self::new(
            self.schema.clone(),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::Filter {
                child: self.clone(),
                predicate,
                keep,
                bindings,
            },
        ))
    }
    pub fn key_by(self: &Arc<Self>, keys: Vec<String>) -> Result<TableRef> {
        for name in &keys {
            self.schema.index_of(name)?;
        }
        if !keys.is_empty() {
            OrderRequirement::new(keys.iter().map(|k| OrderKey::new(k, false, true)).collect())?;
        }
        Ok(Self::new(
            self.schema.clone(),
            keys.clone(),
            self.globals.clone(),
            TableNode::KeyBy {
                child: self.clone(),
                keys,
            },
        ))
    }
    pub fn order_by(self: &Arc<Self>, keys: Vec<(String, bool)>) -> Result<TableRef> {
        for (name, _) in &keys {
            self.schema.index_of(name)?;
        }
        if !keys.is_empty() {
            order_requirement(&keys)?;
        }
        Ok(Self::new(
            self.schema.clone(),
            vec![],
            self.globals.clone(),
            TableNode::OrderBy {
                child: self.clone(),
                keys,
            },
        ))
    }
    pub fn head(self: &Arc<Self>, n: usize) -> TableRef {
        Self::new(
            self.schema.clone(),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::Head {
                child: self.clone(),
                n,
            },
        )
    }
    pub fn tail(self: &Arc<Self>, n: usize) -> TableRef {
        Self::new(
            self.schema.clone(),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::Tail {
                child: self.clone(),
                n,
            },
        )
    }

    pub fn row_type(&self) -> Result<Value> {
        let fields = self
            .schema
            .fields()
            .iter()
            .map(|f| Type::from_arrow(f.data_type()).map(|t| (f.name(), t)))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        serde_json::to_value(fields).map_err(|e| StorageError::from(e).into())
    }

    /// Diagnostic tree includes scalar IR, but never embeds Parallelize's data.
    pub fn explain(&self) -> Value {
        match &self.node {
            TableNode::Read { source } => {
                json!({"node":"TableRead", "metadata":source.metadata_location()})
            }
            TableNode::Parallelize { rows, n_partitions } => {
                json!({"node":"TableParallelize", "rows":rows.num_rows(), "n_partitions":n_partitions})
            }
            TableNode::Range { n, n_partitions } => {
                json!({"node":"TableRange", "n":n, "n_partitions":n_partitions})
            }
            TableNode::MapGlobals { child } => {
                json!({"node":"TableMapGlobals", "child":child.explain(), "globals":self.globals.explain()})
            }
            TableNode::MapRows {
                child,
                fields,
                bindings,
            } => {
                json!({"node":"TableMapRows", "child":child.explain(), "fields":fields,
                    "bindings": bindings.iter().map(|(name, ir)| (name, ir.explain())).collect::<std::collections::BTreeMap<_,_>>()})
            }
            TableNode::Filter {
                child,
                predicate,
                keep,
                bindings,
            } => {
                json!({"node":"TableFilter", "child":child.explain(), "predicate":predicate, "keep":keep,
                    "bindings": bindings.iter().map(|(name, ir)| (name, ir.explain())).collect::<std::collections::BTreeMap<_,_>>()})
            }
            TableNode::KeyBy { child, keys } => {
                json!({"node":"TableKeyBy", "child":child.explain(), "keys":keys})
            }
            TableNode::OrderBy { child, keys } => {
                json!({"node":"TableOrderBy", "child":child.explain(), "keys":keys})
            }
            TableNode::Head { child, n } => {
                json!({"node":"TableHead", "child":child.explain(), "n":n})
            }
            TableNode::Tail { child, n } => {
                json!({"node":"TableTail", "child":child.explain(), "n":n})
            }
        }
    }

    /// Known source cardinalities and cardinality-preserving transformations.
    /// Unknown Filter cardinality must be determined by executing the graph.
    fn partition_rows(&self) -> Option<Vec<usize>> {
        match &self.node {
            TableNode::Read { source } => Some(source.partition_rows().to_vec()),
            TableNode::Parallelize { rows, n_partitions } => Some(
                (0..*n_partitions)
                    .map(|p| {
                        rows.num_rows() * (p + 1) / n_partitions
                            - rows.num_rows() * p / n_partitions
                    })
                    .collect(),
            ),
            TableNode::Range { n, n_partitions } => Some(
                (0..*n_partitions)
                    .map(|p| n / n_partitions + usize::from(p < n % n_partitions))
                    .collect(),
            ),
            TableNode::MapGlobals { child } | TableNode::MapRows { child, .. } => {
                child.partition_rows()
            }
            TableNode::Filter { .. } => None,
            TableNode::OrderBy { child, keys } => {
                if keys.is_empty() {
                    child.partition_rows()
                } else {
                    Some(vec![child.known_count()?])
                }
            }
            TableNode::KeyBy { child, keys } => {
                if keys.is_empty() {
                    child.partition_rows()
                } else {
                    Some(vec![child.known_count()?])
                }
            }
            TableNode::Head { child, n } | TableNode::Tail { child, n } => {
                if *n == 0 {
                    return Some(vec![]);
                }
                let mut lengths = child.partition_rows()?;
                let tail = matches!(self.node, TableNode::Tail { .. });
                if tail {
                    lengths.reverse();
                }
                let mut remaining = *n;
                let mut kept = Vec::new();
                for len in lengths {
                    let len = len.min(remaining);
                    if len > 0 {
                        kept.push(len);
                        remaining -= len;
                    }
                }
                if tail {
                    kept.reverse();
                }
                Some(kept)
            }
        }
    }
    pub fn known_count(&self) -> Option<usize> {
        match &self.node {
            TableNode::Read { source } => Some(source.partition_rows().iter().sum()),
            TableNode::Parallelize { rows, .. } => Some(rows.num_rows()),
            TableNode::Range { n, .. } => Some(*n),
            TableNode::MapGlobals { child }
            | TableNode::MapRows { child, .. }
            | TableNode::OrderBy { child, .. }
            | TableNode::KeyBy { child, .. } => child.known_count(),
            TableNode::Filter { .. } => None,
            TableNode::Head { child, n } | TableNode::Tail { child, n } => {
                if *n == 0 {
                    Some(0)
                } else {
                    Some(child.known_count()?.min(*n))
                }
            }
        }
    }
    pub fn known_n_partitions(&self) -> Option<usize> {
        match &self.node {
            TableNode::Read { source } => Some(source.partition_rows().len()),
            TableNode::Range { n_partitions, .. } | TableNode::Parallelize { n_partitions, .. } => {
                Some(*n_partitions)
            }
            TableNode::MapGlobals { child }
            | TableNode::Filter { child, .. }
            | TableNode::MapRows { child, .. } => child.known_n_partitions(),
            TableNode::OrderBy { child, keys } => {
                if keys.is_empty() {
                    child.known_n_partitions()
                } else {
                    Some(1)
                }
            }
            TableNode::KeyBy { child, keys } => {
                if keys.is_empty() {
                    child.known_n_partitions()
                } else {
                    Some(1)
                }
            }
            _ => Some(self.partition_rows()?.len()),
        }
    }

    pub fn execute(self: &Arc<Self>) -> BatchStream {
        plan::execute(self)
    }

    pub fn physical_plan(self: &Arc<Self>) -> Result<String> {
        let plan = plan::lower(self)?;
        Ok(datafusion_physical_plan::displayable(plan.rows.as_ref())
            .indent(true)
            .to_string())
    }

    pub async fn collect_partitions(self: &Arc<Self>) -> Result<Vec<Vec<RecordBatch>>> {
        collect_partitions(self.execute()).await
    }
    pub async fn count(self: &Arc<Self>) -> Result<usize> {
        plan::count(self).await
    }
    pub async fn n_partitions(self: &Arc<Self>) -> Result<usize> {
        if let Some(count) = self.known_n_partitions() {
            return Ok(count);
        }
        let mut count = 0;
        let mut input = self.execute();
        while let Some(event) = input.try_next().await? {
            if matches!(event, PartitionEvent::StartPartition) {
                count += 1;
            }
        }
        Ok(count)
    }
    /// Pull execution batches directly into the bounded storage writer.
    pub async fn write(self: &Arc<Self>, path: &str, overwrite: bool) -> Result<()> {
        self.write_with_options(path, overwrite, WriteOptions::default())
            .await
    }

    pub async fn write_with_options(
        self: &Arc<Self>,
        path: &str,
        overwrite: bool,
        options: WriteOptions,
    ) -> Result<()> {
        let input = self
            .execute()
            .map_err(|error| match error {
                TableError::Storage(error) => error,
                other => StorageError::Execution(Box::new(other)),
            })
            .boxed();
        write_local_stream_with_globals(
            path,
            overwrite,
            self.schema.clone(),
            input,
            self.row_type()?,
            self.keys.clone(),
            options,
            Some(WriteGlobals {
                logical_type: self.globals.logical_type()?,
                batch: self.globals.evaluate().await?,
            }),
        )
        .await?;
        Ok(())
    }
}

async fn collect_partitions(mut input: BatchStream) -> Result<Vec<Vec<RecordBatch>>> {
    let mut partitions = Vec::new();
    while let Some(event) = input.try_next().await? {
        match event {
            PartitionEvent::StartPartition => partitions.push(Vec::new()),
            PartitionEvent::Batch(batch) => partitions
                .last_mut()
                .ok_or_else(|| TableError::Invalid("batch outside partition".into()))?
                .push(batch),
        }
    }
    Ok(partitions)
}
fn order_requirement(keys: &[(String, bool)]) -> Result<OrderRequirement> {
    Ok(OrderRequirement::new(
        keys.iter()
            .map(|(name, descending)| OrderKey::new(name, *descending, true))
            .collect(),
    )?)
}
