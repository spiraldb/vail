//! Immutable, typed TableIR. Construction checks types and shares child nodes;
//! actions lower expression IR and pull Arrow batches through the graph.
pub mod aggregation;
pub mod arrow_batches;
use arrow_batches::ArrowBatches;
pub mod comparison;
mod deferred_logical;
pub mod globals;
mod initialized_aggregate;
mod initialized_scan;
mod interval_read;
mod join_interval;
mod joins;
pub mod line_source;
mod logical;
mod logical_aggregate_init;
mod logical_aggregation;
mod logical_globals;
mod logical_nested_aggregation;
mod logical_same;
mod logical_scan;
mod lookup;
mod nested_projection;
mod nested_scan;
mod partition_grouping;
mod partition_map;
pub mod partitioned;
mod partitioning;
pub mod plan;
mod provider;
mod text_export;
pub mod text_schema;
mod vcf_source;
mod zip_join;
use globals::{GlobalBindings, GlobalsIR, GlobalsRef, referenced_bindings};

/// Maximum concurrently executing Table partitions for the embedded backend.
pub const EXECUTION_CONCURRENCY: usize = 32;

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
        LocalRead, WriteGlobals, WriteOptions, open_local,
        write_local_partition_streams_with_globals,
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

fn unify_arrow_types(left: &DataType, right: &DataType) -> Option<DataType> {
    use DataType::*;
    if left == right {
        return Some(left.clone());
    }
    let left_logical = Type::from_arrow(left).ok();
    let right_logical = Type::from_arrow(right).ok();
    if matches!(left_logical, Some(Type::Locus { .. }))
        || matches!(right_logical, Some(Type::Locus { .. }))
    {
        // A structural Arrow promotion must not erase reference identity.
        return if left_logical == right_logical {
            left_logical?.to_arrow().ok()
        } else {
            None
        };
    }

    let rank = |dtype: &DataType| match dtype {
        Int32 => Some(0),
        Int64 => Some(1),
        Float32 => Some(2),
        Float64 => Some(3),
        _ => None,
    };
    match (left, right) {
        (Int64, Float32) | (Float32, Int64) => Some(Float64),
        (a, b) if rank(a).is_some() && rank(b).is_some() => Some(match rank(a)?.max(rank(b)?) {
            0 => Int32,
            1 => Int64,
            2 => Float32,
            _ => Float64,
        }),
        (List(a), List(b)) if a.name() == b.name() => Some(List(Arc::new(Field::new(
            a.name(),
            unify_arrow_types(a.data_type(), b.data_type())?,
            a.is_nullable() || b.is_nullable(),
        )))),
        (LargeList(a), LargeList(b)) if a.name() == b.name() => {
            Some(LargeList(Arc::new(Field::new(
                a.name(),
                unify_arrow_types(a.data_type(), b.data_type())?,
                a.is_nullable() || b.is_nullable(),
            ))))
        }
        (Struct(a), Struct(b))
            if a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.name() == y.name()) =>
        {
            let fields = a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| {
                    Some(Arc::new(
                        Field::new(
                            x.name(),
                            unify_arrow_types(x.data_type(), y.data_type())?,
                            x.is_nullable() || y.is_nullable(),
                        )
                        .with_metadata(x.metadata().clone()),
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(Struct(fields.into()))
        }
        _ => None,
    }
}

fn same_row_schema(left: &Schema, right: &Schema) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| {
                left.name() == right.name() && left.data_type() == right.data_type()
            })
}

/// Supported Hail TableIR counterparts. Children are shared Arc references, so
/// branching a Python Table creates a DAG without copying source data or plans.
#[derive(Debug, Clone)]
pub enum TableNode {
    /// Action-local DataFusion input; never constructed by the Python API.
    Prepared {
        rows: plan::PhysicalPlan,
        count: Option<usize>,
        n_partitions: usize,
        partition_counts: Option<Vec<usize>>,
    },
    IntervalRead {
        child: TableRef,
        intervals: Vec<ScalarIR>,
        point_type: Type,
        filter: bool,
    },
    Repartition {
        child: TableRef,
        n: usize,
        strategy: RepartitionStrategy,
    },
    FilterPartitions {
        child: TableRef,
        parts: Vec<usize>,
        keep: bool,
    },
    GroupWithinPartitions {
        child: TableRef,
        name: String,
        n: usize,
    },
    MapPartitions {
        child: TableRef,
        rows_name: String,
        body: ScalarIR,
        bindings: GlobalBindings,
    },
    Read {
        source: Arc<LocalRead>,
    },
    /// Lazy text scan backed by a DataFusion FileSource. The schema is supplied
    /// by the caller; paths remain metadata until an action executes the plan.
    TextRead {
        paths: Vec<String>,
        format: String,
        options_json: Value,
    },
    Parallelize {
        rows: ArrowBatches,
        n_partitions: usize,
    },
    /// Lazy expression source: the array expression is evaluated at action
    /// time, unnested, then divided into ordered logical partitions.
    Expression {
        expression: ScalarIR,
        bindings: GlobalBindings,
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
    Explode {
        child: TableRef,
        path: Vec<String>,
    },
    KeyBy {
        child: TableRef,
        keys: Vec<String>,
    },
    Lookup {
        left: TableRef,
        right: TableRef,
        keys: Vec<ScalarIR>,
        field: String,
        bindings: GlobalBindings,
        all_matches: bool,
        interval_mode: bool,
        query_interval_mode: bool,
    },
    Join {
        left: TableRef,
        right: TableRef,
        kind: TableJoinKind,
        join_key: usize,
    },
    MultiWayZipJoin {
        tables: Vec<TableRef>,
        data_field_name: String,
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
    Union {
        children: Vec<TableRef>,
    },
    Distinct {
        child: TableRef,
    },
    Aggregate {
        child: TableRef,
        keys: Vec<(String, ScalarIR)>,
        fields: Vec<(String, ScalarIR)>,
        bindings: GlobalBindings,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableJoinKind {
    Inner,
    Left,
    Right,
    Outer,
    Semi,
    Anti,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepartitionStrategy {
    Shuffle,
    Coalesce,
    NaiveCoalesce,
}

#[derive(Debug)]
pub struct TableIR {
    schema: SchemaRef,
    keys: Vec<String>,
    node: TableNode,
    globals: GlobalsRef,
    // Metadata provenance only; executable rows always come from the prepared plan.
    partition_metadata: Option<TableRef>,
}

impl TableIR {
    pub(crate) fn new(
        schema: SchemaRef,
        keys: Vec<String>,
        globals: GlobalsRef,
        node: TableNode,
    ) -> TableRef {
        Arc::new(Self {
            schema,
            keys,
            node,
            globals,
            partition_metadata: None,
        })
    }
    pub(crate) fn prepared(
        child: &TableRef,
        rows: plan::PhysicalPlan,
        count: Option<usize>,
        n_partitions: usize,
    ) -> TableRef {
        let metadata = if rows.properties().output_partitioning().partition_count() == n_partitions
            && child.known_n_partitions() == Some(n_partitions)
        {
            Some(child.clone())
        } else {
            None
        };
        let mut table = Self::new(
            child.schema.clone(),
            child.keys.clone(),
            child.globals.clone(),
            TableNode::Prepared {
                rows,
                count,
                n_partitions,
                partition_counts: if n_partitions == 1 {
                    count.map(|n| vec![n])
                } else {
                    None
                },
            },
        );
        Arc::get_mut(&mut table).unwrap().partition_metadata = metadata;
        table
    }
    pub(crate) fn replacing_node(&self, node: TableNode) -> TableRef {
        let metadata = if matches!(node, TableNode::Prepared { .. }) {
            self.partition_metadata.clone()
        } else {
            None
        };
        let mut table = Self::new(
            self.schema.clone(),
            self.keys.clone(),
            self.globals.clone(),
            node,
        );
        Arc::get_mut(&mut table).unwrap().partition_metadata = metadata;
        table
    }
    pub(crate) fn replacing_globals(&self, globals: GlobalsRef) -> TableRef {
        let mut table = Self::new(
            self.schema.clone(),
            self.keys.clone(),
            globals,
            self.node.clone(),
        );
        Arc::get_mut(&mut table).unwrap().partition_metadata = self.partition_metadata.clone();
        table
    }
    pub fn filter_intervals(
        self: &Arc<Self>,
        intervals: Vec<ScalarIR>,
        point_type: Type,
        filter: bool,
    ) -> Result<TableRef> {
        if self.keys.is_empty() {
            return Err(TableError::Invalid(
                "interval reads require a keyed table".into(),
            ));
        }
        let Some(child) = self.push_interval_read(&intervals, &point_type, filter) else {
            return Err(TableError::Invalid(
                "interval read options require a read source or key-preserving projections".into(),
            ));
        };
        for interval in &intervals {
            let expected = Type::Interval {
                interval: Box::new(point_type.clone()),
            };
            if interval.dtype(&Schema::empty())? != expected {
                return Err(TableError::Invalid("invalid interval endpoint type".into()));
            }
        }
        Ok(child)
    }
    fn push_interval_read(
        self: &Arc<Self>,
        intervals: &[ScalarIR],
        point_type: &Type,
        filter: bool,
    ) -> Option<TableRef> {
        let node = match &self.node {
            TableNode::Read { .. } => TableNode::IntervalRead {
                child: self.clone(),
                intervals: intervals.to_vec(),
                point_type: point_type.clone(),
                filter,
            },
            TableNode::MapRows {
                child,
                fields,
                bindings,
            } if self.keys == child.keys
                && self.keys.iter().all(|key| {
                    fields.iter().any(|(name, expression)| {
                    name == key
                        && matches!(expression, ScalarIR::Column { name: source } if source == key)
                })
                }) =>
            {
                TableNode::MapRows {
                    child: child.push_interval_read(intervals, point_type, filter)?,
                    fields: fields.clone(),
                    bindings: bindings.clone(),
                }
            }
            // Matrix boundary validation is row-local and does not change key
            // values or ordering. Run it on the surviving read partitions.
            TableNode::Filter {
                child,
                predicate,
                keep: true,
                bindings,
            } if self.keys == child.keys
                && matches!(predicate, ScalarIR::NestedFunction { function, .. }
                        if function == "__vail_matrix_aligned") =>
            {
                TableNode::Filter {
                    child: child.push_interval_read(intervals, point_type, filter)?,
                    predicate: predicate.clone(),
                    keep: true,
                    bindings: bindings.clone(),
                }
            }
            // A plain MapGlobals preserves key identity. `with_keys` is also
            // represented by MapGlobals, but may attach different key names;
            // intervals expressed in this node's key space cannot be passed
            // through that metadata boundary safely.
            TableNode::MapGlobals { child } if self.keys == child.keys => TableNode::MapGlobals {
                child: child.push_interval_read(intervals, point_type, filter)?,
            },
            // Keep the partition selection below the interval read. The
            // interval planner will intersect its candidate source IDs with
            // this selection, preserving operation order and avoiding opens
            // for source partitions excluded by the filter.
            TableNode::FilterPartitions { child, .. }
                if self.keys == child.keys && child.can_pushdown_interval_read() =>
            {
                TableNode::IntervalRead {
                    child: self.clone(),
                    intervals: intervals.to_vec(),
                    point_type: point_type.clone(),
                    filter,
                }
            }
            _ => return None,
        };
        Some(Self::new(
            self.schema.clone(),
            self.keys.clone(),
            self.globals.clone(),
            node,
        ))
    }
    pub fn can_pushdown_interval_read(&self) -> bool {
        match &self.node {
            TableNode::Read { .. } => true,
            TableNode::MapRows {
                child,
                fields,
                ..
            } => {
                self.keys == child.keys
                    && self.keys.iter().all(|key| {
                        fields.iter().any(|(name, expression)| {
                            name == key
                                && matches!(expression, ScalarIR::Column { name: source } if source == key)
                        })
                    })
                    && child.can_pushdown_interval_read()
            }
            TableNode::Filter { child, predicate, keep: true, .. }
                if matches!(predicate, ScalarIR::NestedFunction { function, .. }
                    if function == "__vail_matrix_aligned") =>
            {
                self.keys == child.keys && child.can_pushdown_interval_read()
            }
            TableNode::MapGlobals { child } => {
                self.keys == child.keys && child.can_pushdown_interval_read()
            }
            TableNode::FilterPartitions { child, .. } => {
                self.keys == child.keys && child.can_pushdown_interval_read()
            }
            _ => false,
        }
    }
    pub async fn calculate_new_partitions(
        self: &Arc<Self>,
        n: usize,
    ) -> Result<
        Vec<(
            Vec<datafusion_common::ScalarValue>,
            Vec<datafusion_common::ScalarValue>,
            bool,
            bool,
        )>,
    > {
        if n == 0 {
            return Ok(vec![]);
        }
        if self.keys.is_empty() {
            return Ok(vec![]);
        }
        let input = partitioned::prepare(self.clone()).await?;
        let (distinct, _) =
            partitioning::distinct_key_positions(input.ordered(), &self.keys, &[]).await?;
        if distinct == 0 {
            return Ok(vec![]);
        }
        let parts = distinct.min(n);
        let step = distinct.div_ceil(parts);
        let starts = (0..distinct).step_by(step).collect::<Vec<_>>();
        let mut wanted = starts.clone();
        wanted.push(distinct - 1);
        wanted.sort_unstable();
        wanted.dedup();
        let input = partitioned::prepare(self.clone()).await?;
        let (observed, positions) =
            partitioning::distinct_key_positions(input.ordered(), &self.keys, &wanted).await?;
        if observed != distinct {
            return Err(TableError::Invalid(
                "input keys changed during partition planning".into(),
            ));
        }
        let positions = positions
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        Ok(starts
            .into_iter()
            .map(|i| {
                let closed = i + step < distinct - 1;
                let end = if closed { i + step } else { distinct - 1 };
                (
                    positions[&i].clone(),
                    positions[&end].clone(),
                    true,
                    !closed,
                )
            })
            .collect())
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
            partition_metadata: None,
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

    pub fn read_text(
        schema: SchemaRef,
        paths: Vec<String>,
        format: &str,
        options_json: &Value,
    ) -> Result<TableRef> {
        if !matches!(format, "csv" | "json" | "ndjson" | "lines" | "vcf") {
            return Err(TableError::Invalid(format!(
                "unsupported text format {format:?}"
            )));
        }
        if paths.is_empty() {
            return Err(TableError::Invalid(
                "text input requires at least one path".into(),
            ));
        }
        let options: Value = serde_json::from_value(options_json.clone())
            .map_err(|e| TableError::Invalid(e.to_string()))?;
        let options = options
            .as_object()
            .ok_or_else(|| TableError::Invalid("text read options must be a JSON object".into()))?;
        let mut names = std::collections::HashSet::new();
        for field in schema.fields() {
            if !names.insert(field.name()) {
                return Err(TableError::Invalid(format!(
                    "duplicate field {:?}",
                    field.name()
                )));
            }
        }
        if format == "lines" {
            if schema.fields().len() != 2
                || schema.field(0).name() != "file"
                || schema.field(0).data_type() != &DataType::Utf8
                || schema.field(1).name() != "text"
                || schema.field(1).data_type() != &DataType::Utf8
            {
                return Err(TableError::Invalid(
                    "lines schema must contain file and text Utf8 fields in that order".into(),
                ));
            }
            if !options.is_empty() {
                return Err(TableError::Invalid(
                    "lines input does not accept options".into(),
                ));
            }
        } else if format == "csv" {
            crate::text_schema::csv_options(options.clone())?;
        } else if format == "json" || format == "ndjson" {
            crate::text_schema::json_options(options.clone())?;
        }
        let paths = paths
            .into_iter()
            .map(|path| {
                if path.contains("://") {
                    return Err(TableError::Invalid(format!(
                        "only local text paths are supported: {path:?}"
                    )));
                }
                if format != "vcf"
                    && [".gz", ".bgz", ".bz2", ".xz", ".zst"]
                        .iter()
                        .any(|suffix| path.ends_with(suffix))
                {
                    return Err(TableError::Invalid(
                        "compressed text sources are not implemented".into(),
                    ));
                }
                std::path::absolute(&path)
                    .map(|path| path.to_string_lossy().into_owned())
                    .map_err(|e| {
                        TableError::Invalid(format!("cannot resolve text path {path:?}: {e}"))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::new(
            schema,
            vec![],
            GlobalsIR::empty(),
            TableNode::TextRead {
                paths,
                format: format.to_owned(),
                options_json: options_json.to_owned(),
            },
        ))
    }

    /// Owning Arrow buffers here is allowed: these are the input context, not an
    /// executed table. Partitioning/slicing happens only when an action polls it.
    pub fn parallelize(rows: RecordBatch, n_partitions: usize) -> Result<TableRef> {
        Self::parallelize_batches(rows.schema(), vec![rows], n_partitions)
    }

    /// Retain a schema-aligned batch sequence without concatenating value buffers.
    pub fn parallelize_batches(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        n_partitions: usize,
    ) -> Result<TableRef> {
        if n_partitions == 0 {
            return Err(TableError::Invalid("n_partitions must be positive".into()));
        }
        let rows = ArrowBatches::new(schema, batches)?;
        Ok(Self::new(
            rows.schema(),
            vec![],
            GlobalsIR::empty(),
            TableNode::Parallelize { rows, n_partitions },
        ))
    }
    pub fn parallelize_expression(
        expression: ScalarIR,
        bindings: GlobalBindings,
        n_partitions: usize,
    ) -> Result<TableRef> {
        if n_partitions == 0 {
            return Err(TableError::Invalid("n_partitions must be positive".into()));
        }
        let mut invalid = None;
        expression.visit(&mut |node| match node {
            ScalarIR::Column { .. } => {
                invalid = Some("expression parallelize cannot reference row columns")
            }
            ScalarIR::Aggregate { .. }
            | ScalarIR::AggregateFold { .. }
            | ScalarIR::AggGroupBy { .. }
            | ScalarIR::AggArray { .. }
            | ScalarIR::AggFilter { .. }
            | ScalarIR::AggExplode { .. } => {
                invalid = Some("expression parallelize cannot contain aggregators")
            }
            _ => (),
        });
        if let Some(message) = invalid {
            return Err(TableError::Invalid(message.into()));
        }
        let bindings = referenced_bindings([&expression], bindings)?;
        let ty = expression.dtype_with_globals(&Schema::empty(), &Schema::empty())?;
        let Type::Array { array } = ty else {
            return Err(TableError::Invalid(
                "expression parallelize expects array<struct>".into(),
            ));
        };
        let Type::Struct { r#struct: fields } = *array else {
            return Err(TableError::Invalid(
                "expression parallelize expects array<struct>".into(),
            ));
        };
        let schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, ty)| Ok(Field::new(name, ty.to_arrow()?, true)))
                .collect::<std::result::Result<Vec<_>, ExprError>>()?,
        ));
        Ok(Self::new(
            schema,
            vec![],
            GlobalsIR::empty(),
            TableNode::Expression {
                expression,
                bindings,
                n_partitions,
            },
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
    /// Append one nullable right value struct per left row, preserving its partition.
    pub fn lookup(
        self: &Arc<Self>,
        right: TableRef,
        keys: Vec<ScalarIR>,
        field: String,
        bindings: GlobalBindings,
        all_matches: bool,
        query_interval: bool,
    ) -> Result<TableRef> {
        if right.keys().is_empty() || keys.len() != right.keys().len() {
            return Err(TableError::Invalid(
                "lookup requires the complete nonempty right key".into(),
            ));
        }
        if field.is_empty()
            || self.schema.index_of(&field).is_ok()
            || self.globals.schema().index_of(&field).is_ok()
        {
            return Err(TableError::Invalid(
                "lookup field name is empty or collides with an existing field".into(),
            ));
        }
        let bindings = referenced_bindings(keys.iter(), bindings)?;
        let mut interval_mode = false;
        let mut query_interval_mode = false;
        for (expression, name) in keys.iter().zip(right.keys()) {
            let actual = expression.dtype_with_globals(self.schema(), self.globals.schema())?;
            let expected = Type::from_arrow(right.schema().field_with_name(name)?.data_type())?;
            let point_matches =
                matches!(&expected, Type::Interval { interval } if interval.as_ref() == &actual);
            let range_matches = query_interval
                && matches!(&actual,
                Type::Interval { interval } if interval.as_ref() == &expected);
            if actual != expected && !point_matches && !range_matches {
                return Err(TableError::Invalid("lookup key type mismatch".into()));
            }
            interval_mode |= point_matches;
            query_interval_mode |= range_matches;
        }
        if interval_mode && keys.len() != 1 {
            return Err(TableError::Invalid(
                "point lookup into interval keys requires a single interval key".into(),
            ));
        }
        let value = Type::Struct {
            r#struct: right
                .schema()
                .fields()
                .iter()
                .filter(|f| !right.keys().contains(f.name()))
                .map(|f| Ok((f.name().clone(), Type::from_arrow(f.data_type())?)))
                .collect::<std::result::Result<Vec<_>, ExprError>>()?,
        };
        let mut fields = self
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        let value = if all_matches {
            Type::Array {
                array: Box::new(value),
            }
        } else {
            value
        };
        fields.push(Field::new(&field, value.to_arrow()?, !all_matches));
        Ok(Self::new(
            Arc::new(Schema::new(fields)),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::Lookup {
                left: self.clone(),
                right,
                keys,
                field,
                bindings,
                all_matches,
                interval_mode,
                query_interval_mode,
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
    pub fn explode(self: &Arc<Self>, path: Vec<String>) -> Result<TableRef> {
        if path.is_empty() {
            return Err(TableError::Invalid("explode requires a field path".into()));
        }
        if self.keys.contains(&path[0]) {
            return Err(TableError::Invalid("cannot explode a key field".into()));
        }
        fn replace(field: &Field, path: &[String]) -> Result<Field> {
            let dtype = if path.len() == 1 {
                match field.data_type() {
                    DataType::List(item) | DataType::LargeList(item) => item.data_type().clone(),
                    other => {
                        return Err(TableError::Invalid(format!(
                            "explode expects array or set, found {other}"
                        )));
                    }
                }
            } else {
                let DataType::Struct(fields) = field.data_type() else {
                    return Err(TableError::Invalid(format!(
                        "explode path passes through non-struct field {:?}",
                        field.name()
                    )));
                };
                let mut found = false;
                let mut result = Vec::with_capacity(fields.len());
                for nested in fields {
                    if nested.name() == &path[1] {
                        result.push(Arc::new(replace(nested, &path[1..])?));
                        found = true;
                    } else {
                        result.push(nested.clone());
                    }
                }
                if !found {
                    return Err(TableError::Invalid(format!(
                        "unknown explode field {:?}",
                        path[1]
                    )));
                }
                DataType::Struct(result.into())
            };
            Ok(Field::new(field.name(), dtype, true))
        }
        let mut found = false;
        let mut fields = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            if field.name() == &path[0] {
                fields.push(Arc::new(replace(field, &path)?));
                found = true;
            } else {
                fields.push(field.clone());
            }
        }
        if !found {
            return Err(TableError::Invalid(format!(
                "unknown explode field {:?}",
                path[0]
            )));
        }
        Ok(Self::new(
            Arc::new(Schema::new(fields)),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::Explode {
                child: self.clone(),
                path,
            },
        ))
    }
    pub fn repartition(
        self: &Arc<Self>,
        n: usize,
        strategy: RepartitionStrategy,
    ) -> Result<TableRef> {
        if n == 0 {
            return Err(TableError::Invalid(
                "number of partitions must be positive".into(),
            ));
        }
        Ok(Self::new(
            self.schema.clone(),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::Repartition {
                child: self.clone(),
                n,
                strategy,
            },
        ))
    }
    pub fn filter_partitions(
        self: &Arc<Self>,
        mut parts: Vec<usize>,
        keep: bool,
    ) -> Result<TableRef> {
        parts.sort_unstable();
        parts.dedup();
        Ok(Self::new(
            self.schema.clone(),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::FilterPartitions {
                child: self.clone(),
                parts,
                keep,
            },
        ))
    }
    pub fn group_within_partitions(self: &Arc<Self>, name: String, n: usize) -> Result<TableRef> {
        if n == 0 {
            return Err(TableError::Invalid(
                "stream grouped: non-positive size: 0".into(),
            ));
        }
        if name.is_empty() || self.keys.contains(&name) {
            return Err(TableError::Invalid(
                "group field must be a nonempty name distinct from keys".into(),
            ));
        }
        let mut fields = self
            .keys
            .iter()
            .map(|k| self.schema.field_with_name(k).cloned())
            .collect::<std::result::Result<Vec<_>, _>>()?;
        fields.push(Field::new(
            &name,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    self.schema
                        .fields()
                        .iter()
                        .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
                        .collect(),
                ),
                true,
            ))),
            true,
        ));
        Ok(Self::new(
            Arc::new(Schema::new(fields)),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::GroupWithinPartitions {
                child: self.clone(),
                name,
                n,
            },
        ))
    }
    pub fn map_partitions(
        self: &Arc<Self>,
        rows_name: String,
        body: ScalarIR,
        bindings: GlobalBindings,
    ) -> Result<TableRef> {
        let input = Schema::new(vec![Field::new(
            &rows_name,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(self.schema.fields().clone()),
                true,
            ))),
            false,
        )]);
        let checked = referenced_bindings(std::slice::from_ref(&body), bindings)?;
        let dtype = body.dtype_with_globals(&input, self.globals.schema())?;
        let Type::Array { array } = dtype else {
            return Err(TableError::Invalid(
                "partition mapping requires an array of row structs".into(),
            ));
        };
        let DataType::Struct(fields) = array.to_arrow()? else {
            return Err(TableError::Invalid(
                "partition mapping requires row structs".into(),
            ));
        };
        let schema = Arc::new(Schema::new(fields));
        for field in schema.fields() {
            if self.globals.schema().index_of(field.name()).is_ok() {
                return Err(TableError::Invalid(
                    "partition output field collides with globals".into(),
                ));
            }
        }
        // A partition callback preserves membership, but may change keys or
        // reorder rows. Retain key metadata only for a proven forward traversal.
        let keys = if crate::partition_map::preserves_keys(&body, &rows_name, &self.keys) {
            self.keys.clone()
        } else {
            vec![]
        };
        Ok(Self::new(
            schema,
            keys,
            self.globals.clone(),
            TableNode::MapPartitions {
                child: self.clone(),
                rows_name,
                body,
                bindings: checked,
            },
        ))
    }
    pub fn key_by(self: &Arc<Self>, keys: Vec<String>) -> Result<TableRef> {
        // A keyed Table already guarantees this ordering and partition layout.
        if keys == self.keys {
            return Ok(self.clone());
        }
        for name in &keys {
            let i = self.schema.index_of(name)?;
            vail_execution::expressions::Type::from_arrow(self.schema.field(i).data_type())?
                .ensure_keyable()?;
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
    /// Attach key metadata to rows that are already ordered by `keys`.
    ///
    /// This does not sort or repartition the input. Callers must establish the
    /// ordering invariant before using this method (for example, MatrixTable
    /// row keys are sorted by construction).
    pub fn with_keys(self: &Arc<Self>, keys: Vec<String>) -> Result<TableRef> {
        for name in &keys {
            let i = self.schema.index_of(name)?;
            vail_execution::expressions::Type::from_arrow(self.schema.field(i).data_type())?
                .ensure_keyable()?;
        }
        Ok(Self::new(
            self.schema.clone(),
            keys,
            self.globals.clone(),
            TableNode::MapGlobals {
                child: self.clone(),
            },
        ))
    }
    pub fn order_by(self: &Arc<Self>, keys: Vec<(String, bool)>) -> Result<TableRef> {
        for (name, _) in &keys {
            let i = self.schema.index_of(name)?;
            vail_execution::expressions::Type::from_arrow(self.schema.field(i).data_type())?
                .ensure_keyable()?;
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

    pub fn union(tables: Vec<TableRef>, unify: bool) -> Result<TableRef> {
        let Some(first) = tables.first() else {
            return Err(TableError::Invalid(
                "union requires at least one table".into(),
            ));
        };
        let keys = &first.keys;
        for key in keys {
            let dtype = first.schema.field(first.schema.index_of(key)?).data_type();
            for table in &tables[1..] {
                if table.keys != *keys
                    || table.schema.field(table.schema.index_of(key)?).data_type() != dtype
                {
                    return Err(TableError::Invalid(
                        "union tables must have identical key names and types".into(),
                    ));
                }
            }
        }
        if tables[1..].iter().any(|table| table.keys != *keys) {
            return Err(TableError::Invalid(
                "union tables must have identical key names and types".into(),
            ));
        }
        let schema = if !unify {
            if tables[1..]
                .iter()
                .any(|table| !same_row_schema(&table.schema, &first.schema))
            {
                return Err(TableError::Invalid(
                    "union tables must have identical row schemas when unify=false".into(),
                ));
            }
            Arc::new(Schema::new_with_metadata(
                first
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone().with_nullable(true))
                    .collect::<Vec<_>>(),
                first.schema.metadata().clone(),
            ))
        } else {
            let mut names = keys.clone();
            let mut types = std::collections::BTreeMap::<String, DataType>::new();
            for table in &tables {
                for field in table.schema.fields() {
                    if keys.contains(field.name()) {
                        continue;
                    }
                    if !names.contains(field.name()) {
                        names.push(field.name().clone());
                    }
                    if let Some(current) = types.get(field.name()) {
                        let unified =
                            unify_arrow_types(current, field.data_type()).ok_or_else(|| {
                                TableError::Invalid(format!(
                                    "incompatible union field {:?}: {current:?} and {:?}",
                                    field.name(),
                                    field.data_type()
                                ))
                            })?;
                        types.insert(field.name().clone(), unified);
                    } else {
                        types.insert(field.name().clone(), field.data_type().clone());
                    }
                }
            }
            let fields = names
                .iter()
                .map(|name| {
                    let dtype = if keys.contains(name) {
                        first
                            .schema
                            .field(first.schema.index_of(name).expect("validated key field"))
                            .data_type()
                            .clone()
                    } else {
                        types[name].clone()
                    };
                    Field::new(name, dtype, true)
                })
                .collect::<Vec<_>>();
            Arc::new(Schema::new(fields))
        };
        for field in schema.fields() {
            if first.globals.schema().index_of(field.name()).is_ok() {
                return Err(TableError::Invalid(format!(
                    "union row field {:?} collides with first table globals",
                    field.name()
                )));
            }
        }
        Ok(Self::new(
            schema,
            first.keys.clone(),
            first.globals.clone(),
            TableNode::Union { children: tables },
        ))
    }

    pub fn distinct(self: &Arc<Self>) -> Result<TableRef> {
        if self.keys.is_empty() {
            return Err(TableError::Invalid(
                "distinct requires a keyed table".into(),
            ));
        }
        Ok(Self::new(
            self.schema.clone(),
            self.keys.clone(),
            self.globals.clone(),
            TableNode::Distinct {
                child: self.clone(),
            },
        ))
    }

    pub fn grouped_aggregate_bound(
        self: &Arc<Self>,
        keys: Vec<(String, ScalarIR)>,
        fields: Vec<(String, ScalarIR)>,
        bindings: GlobalBindings,
    ) -> Result<TableRef> {
        for (_, expr) in &keys {
            expr.dtype_with_globals(&self.schema, self.globals.schema())?
                .ensure_keyable()?;
        }
        let mut names = std::collections::HashSet::new();
        for (name, _) in keys.iter().chain(fields.iter()) {
            if !names.insert(name.clone()) {
                return Err(TableError::Invalid(format!(
                    "duplicate grouped aggregation field {name:?}"
                )));
            }
            if self.globals.schema().index_of(name).is_ok() {
                return Err(TableError::Invalid(format!(
                    "row field {name:?} collides with globals"
                )));
            }
        }
        let bindings = referenced_bindings(
            keys.iter().chain(fields.iter()).map(|(_, expr)| expr),
            bindings,
        )?;
        let input = crate::logical::lower(self)?;
        let plan = crate::logical_aggregation::lower_grouped_fields(
            self, input, &keys, &fields, &bindings,
        )?;
        let schema = Arc::new(plan.schema().as_arrow().clone());
        if !keys.is_empty() {
            order_requirement(
                &keys
                    .iter()
                    .map(|(name, _)| (name.clone(), false))
                    .collect::<Vec<_>>(),
            )?
            .physical_ordering(&schema)?;
        }
        Ok(Self::new(
            schema,
            keys.iter().map(|(name, _)| name.clone()).collect(),
            self.globals.clone(),
            TableNode::Aggregate {
                child: self.clone(),
                keys,
                fields,
                bindings,
            },
        ))
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
            TableNode::Lookup {
                left,
                right,
                keys,
                field,
                ..
            } => {
                json!({"node":"TableLookup","left":left.explain(),"right":right.explain(),"keys":keys,"field":field})
            }
            TableNode::Join {
                left,
                right,
                kind,
                join_key,
            } => {
                json!({"node":"TableJoin","left":left.explain(),"right":right.explain(),"kind":format!("{kind:?}"),"join_key":join_key})
            }
            TableNode::MultiWayZipJoin {
                tables,
                data_field_name,
            } => {
                json!({"node":"TableMultiWayZipJoin","tables":tables.iter().map(|t|t.explain()).collect::<Vec<_>>(),"data_field_name":data_field_name})
            }

            TableNode::Prepared { n_partitions, .. } => {
                json!({"node":"PreparedDataFusionPartitions","n_partitions":n_partitions})
            }
            TableNode::IntervalRead {
                child,
                intervals,
                filter,
                ..
            } => {
                json!({"node":"TableReadIntervals","child":child.explain(),"interval_count":intervals.len(),"filter_intervals":filter})
            }
            TableNode::Repartition { child, n, strategy } => {
                json!({"node":"TableRepartition","child":child.explain(),"n":n,"strategy":format!("{strategy:?}")})
            }
            TableNode::FilterPartitions { child, parts, keep } => {
                json!({"node":"TableFilterPartitions","child":child.explain(),"parts":parts,"keep":keep})
            }
            TableNode::GroupWithinPartitions { child, name, n } => {
                json!({"node":"TableGroupWithinPartitions","child":child.explain(),"name":name,"n":n})
            }
            TableNode::MapPartitions {
                child,
                rows_name,
                body,
                ..
            } => {
                json!({"node":"TableMapPartitions","child":child.explain(),"rows_name":rows_name,"body":body})
            }
            TableNode::Read { source } => {
                json!({"node":"TableRead", "metadata":source.metadata_location()})
            }
            TableNode::TextRead {
                paths,
                format,
                options_json,
            } => {
                json!({"node":"DataFusionTextRead", "paths":paths, "format":format, "options":options_json.clone()})
            }
            TableNode::Parallelize { rows, n_partitions } => {
                json!({"node":"TableParallelize", "rows":rows.num_rows(), "n_partitions":n_partitions})
            }
            TableNode::Expression {
                expression,
                n_partitions,
                ..
            } => {
                json!({"node":"TableParallelizeExpression", "expression":expression, "n_partitions":n_partitions})
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
            TableNode::Explode { child, path } => {
                json!({"node":"TableExplode", "child":child.explain(), "path":path})
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
            TableNode::Union { children } => {
                json!({"node":"TableUnion", "children":children.iter().map(|c| c.explain()).collect::<Vec<_>>() })
            }
            TableNode::Distinct { child } => {
                json!({"node":"TableDistinct", "child":child.explain()})
            }
            TableNode::Aggregate {
                child,
                keys,
                fields,
                bindings,
            } => {
                json!({"node":"TableGroupedAggregate", "child":child.explain(), "keys":keys, "fields":fields,
                    "bindings": bindings.iter().map(|(name, ir)| (name, ir.explain())).collect::<std::collections::BTreeMap<_,_>>()})
            }
        }
    }

    /// Known source cardinalities and cardinality-preserving transformations.
    /// Unknown Filter cardinality must be determined by executing the graph.
    pub(crate) fn partition_rows(&self) -> Option<Vec<usize>> {
        match &self.node {
            TableNode::Lookup { left, .. } => left.partition_rows(),
            TableNode::Join { .. } | TableNode::MultiWayZipJoin { .. } => None,
            TableNode::Prepared {
                count,
                n_partitions,
                partition_counts,
                ..
            } => {
                if let Some(counts) = partition_counts {
                    Some(counts.clone())
                } else if *n_partitions == 1 {
                    Some(vec![(*count)?])
                } else {
                    None
                }
            }
            TableNode::IntervalRead { .. } => None,
            TableNode::Read { source } => Some(source.partition_rows().to_vec()),
            TableNode::TextRead { .. } => None,
            TableNode::Parallelize { rows, n_partitions } => Some(
                (0..*n_partitions)
                    .map(|p| rows.partition_bounds(p, *n_partitions).len())
                    .collect(),
            ),
            TableNode::Expression { .. } => None,
            TableNode::Range { n, n_partitions } => Some(
                (0..*n_partitions)
                    .map(|p| n / n_partitions + usize::from(p < n % n_partitions))
                    .collect(),
            ),
            TableNode::MapGlobals { child } | TableNode::MapRows { child, .. } => {
                child.partition_rows()
            }
            TableNode::Filter { .. }
            | TableNode::Explode { .. }
            | TableNode::MapPartitions { .. } => None,
            TableNode::GroupWithinPartitions { child, n, .. } => Some(
                child
                    .partition_rows()?
                    .into_iter()
                    .map(|rows| rows.div_ceil(*n))
                    .collect(),
            ),
            TableNode::FilterPartitions { child, parts, keep } => {
                let counts = child.partition_rows()?;
                if parts.iter().any(|&p| p >= counts.len()) {
                    return None;
                }
                Some(
                    counts
                        .into_iter()
                        .enumerate()
                        .filter(|(i, _)| parts.contains(i) == *keep)
                        .map(|(_, n)| n)
                        .collect(),
                )
            }
            TableNode::Repartition { child, n, strategy } => {
                if let TableNode::Range { n: rows, .. } = child.node() {
                    return TableIR::range(*rows, *n).ok()?.partition_rows();
                }
                if *strategy == RepartitionStrategy::Shuffle {
                    return None;
                }
                let counts = child.partition_rows()?;
                let entries = counts.iter().copied().enumerate().collect::<Vec<_>>();
                let ends = partitioning::coalesce_ends(&entries, *n, *strategy);
                let mut start = 0;
                Some(
                    ends.into_iter()
                        .map(|end| {
                            let rows = counts[start..=end].iter().sum();
                            start = end + 1;
                            rows
                        })
                        .collect(),
                )
            }
            TableNode::Aggregate { .. } => None,
            TableNode::Union { .. } | TableNode::Distinct { .. } => None,
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
            TableNode::Lookup { left, .. } => left.known_count(),
            TableNode::Join { .. } | TableNode::MultiWayZipJoin { .. } => None,
            TableNode::Prepared {
                count,
                partition_counts,
                ..
            } => count.or_else(|| partition_counts.as_ref().map(|counts| counts.iter().sum())),
            TableNode::IntervalRead { .. } => None,
            TableNode::Read { source } => Some(source.partition_rows().iter().sum()),
            TableNode::TextRead { .. } => None,
            TableNode::Parallelize { rows, .. } => Some(rows.num_rows()),
            TableNode::Expression { .. } => None,
            TableNode::Range { n, .. } => Some(*n),
            TableNode::MapGlobals { child }
            | TableNode::MapRows { child, .. }
            | TableNode::OrderBy { child, .. }
            | TableNode::KeyBy { child, .. }
            | TableNode::Repartition { child, .. } => child.known_count(),
            TableNode::Filter { .. }
            | TableNode::Explode { .. }
            | TableNode::MapPartitions { .. } => None,
            TableNode::FilterPartitions { .. } | TableNode::GroupWithinPartitions { .. } => {
                Some(self.partition_rows()?.iter().sum())
            }
            TableNode::Aggregate { .. } => None,
            TableNode::Union { children } => Some(
                children
                    .iter()
                    .map(|c| c.known_count())
                    .collect::<Option<Vec<_>>>()?
                    .into_iter()
                    .sum(),
            ),
            TableNode::Distinct { .. } => None,
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
            TableNode::Lookup { left, .. } => left.known_n_partitions(),
            TableNode::Join { left, kind, .. }
                if matches!(kind, TableJoinKind::Semi | TableJoinKind::Anti) =>
            {
                left.known_n_partitions()
            }
            TableNode::Join { .. } => None,
            TableNode::MultiWayZipJoin { .. } => None,

            TableNode::Prepared { n_partitions, .. } => Some(*n_partitions),
            TableNode::IntervalRead { intervals, .. } => {
                if intervals.is_empty() {
                    Some(0)
                } else {
                    None
                }
            }
            TableNode::Read { source } => Some(source.partition_rows().len()),
            TableNode::TextRead { paths, .. } => Some(paths.len()),
            TableNode::Range { n_partitions, .. }
            | TableNode::Parallelize { n_partitions, .. }
            | TableNode::Expression { n_partitions, .. } => Some(*n_partitions),
            TableNode::MapGlobals { child }
            | TableNode::Filter { child, .. }
            | TableNode::Explode { child, .. }
            | TableNode::MapRows { child, .. }
            | TableNode::GroupWithinPartitions { child, .. }
            | TableNode::MapPartitions { child, .. } => child.known_n_partitions(),
            TableNode::FilterPartitions { child, parts, keep } => {
                let n = child.known_n_partitions()?;
                if parts.iter().any(|&p| p >= n) {
                    return None;
                }
                Some(if *keep { parts.len() } else { n - parts.len() })
            }
            TableNode::Repartition { child, n, strategy } => {
                if let TableNode::Range { n: rows, .. } = child.node() {
                    return Some((*rows).min(*n).max(1));
                }
                if *strategy == RepartitionStrategy::Shuffle {
                    if self.keys.is_empty() { Some(*n) } else { None }
                } else if *strategy == RepartitionStrategy::NaiveCoalesce {
                    Some(child.known_n_partitions()?.min(*n))
                } else {
                    Some(self.partition_rows()?.len())
                }
            }
            TableNode::Aggregate { .. } => Some(1),
            TableNode::Union { children } => {
                if self.keys.is_empty() {
                    Some(
                        children
                            .iter()
                            .map(|c| c.known_n_partitions())
                            .collect::<Option<Vec<_>>>()?
                            .into_iter()
                            .sum(),
                    )
                } else {
                    Some(1)
                }
            }
            TableNode::Distinct { .. } => Some(1),
            TableNode::OrderBy { child, keys } => {
                if keys.is_empty() {
                    child.known_n_partitions()
                } else {
                    Some(1)
                }
            }
            TableNode::KeyBy { child, .. } => child.known_n_partitions(),
            _ => Some(self.partition_rows()?.len()),
        }
    }

    pub fn execute(self: &Arc<Self>) -> BatchStream {
        partitioned::execute(self)
    }

    pub fn physical_plan(self: &Arc<Self>) -> Result<String> {
        let table = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TableError::Execution(e.to_string()))?;
            let plan = runtime.block_on(logical::physical(&table))?;
            Ok(datafusion_physical_plan::displayable(plan.as_ref())
                .indent(true)
                .to_string())
        })
        .join()
        .map_err(|_| TableError::Execution("physical plan diagnostic panicked".into()))?
    }

    pub fn logical_plan(self: &Arc<Self>) -> Result<String> {
        logical::display(self)
    }

    pub async fn collect_partitions(self: &Arc<Self>) -> Result<Vec<Vec<RecordBatch>>> {
        collect_partitions(self.execute()).await
    }

    /// Collect an ordered prefix without counting unknown source partitions.
    /// Used for metadata discovery, where partition-preserving head planning
    /// would otherwise read the input before discovering its first record.
    pub async fn sample(self: &Arc<Self>, limit: usize) -> Result<Vec<RecordBatch>> {
        logical::sample(self, limit).await
    }
    pub async fn count(self: &Arc<Self>) -> Result<usize> {
        partitioned::count(self.clone()).await
    }
    /// Execute every output column for validation, retaining only a row count.
    pub async fn force_count(self: &Arc<Self>) -> Result<usize> {
        partitioned::force_count(self.clone()).await
    }
    pub async fn n_partitions(self: &Arc<Self>) -> Result<usize> {
        if let Some(count) = self.known_n_partitions() {
            return Ok(count);
        }
        match &self.node {
            TableNode::IntervalRead {
                child,
                intervals,
                point_type,
                filter,
            } => interval_read::n_partitions(child, intervals, point_type, *filter).await,
            TableNode::MapGlobals { child }
            | TableNode::Filter { child, .. }
            | TableNode::Explode { child, .. }
            | TableNode::MapRows { child, .. }
            | TableNode::GroupWithinPartitions { child, .. }
            | TableNode::MapPartitions { child, .. }
            | TableNode::KeyBy { child, .. } => Box::pin(child.n_partitions()).await,
            _ => Ok(partitioned::prepare(self.clone()).await?.parts.len()),
        }
    }
    /// Pull execution batches directly into the bounded storage writer.
    pub async fn write(self: &Arc<Self>, path: &str, overwrite: bool) -> Result<()> {
        self.write_with_options(path, overwrite, WriteOptions::default())
            .await
    }

    pub async fn export_text(
        self: &Arc<Self>,
        path: &str,
        format: &str,
        options_json: &Value,
    ) -> Result<()> {
        crate::text_export::export(self.clone(), path, format, options_json).await
    }

    pub async fn write_with_options(
        self: &Arc<Self>,
        path: &str,
        overwrite: bool,
        options: WriteOptions,
    ) -> Result<()> {
        let prepared = partitioned::prepare(self.clone()).await?;
        let context = Arc::new(datafusion_execution::TaskContext::default());
        let input: Vec<BoxStream<'static, std::result::Result<RecordBatch, StorageError>>> =
            prepared
                .parts
                .into_iter()
                .map(|part| {
                    let context = context.clone();
                    let schema = self.schema.clone();
                    async_stream::try_stream! {
                    let mut batches = part.execute(0, context)
                        .map_err(|error| StorageError::Execution(Box::new(error)))?;
                    while let Some(batch) = batches.try_next().await
                        .map_err(|error| StorageError::Execution(Box::new(error)))? {
                        if batch.num_rows() == 0 { continue; }
                        let row = batch.project(&(0..schema.fields().len()).collect::<Vec<_>>())?;
                        yield vail_storage::local_table::normalize(&row, schema.clone())?;
                    }
                }.boxed()
                })
                .collect();
        write_local_partition_streams_with_globals(
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

mod rng_context;
