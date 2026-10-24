//! One DataFusion logical tree for a table action. Ordinary expressions remain
//! visible to DataFusion; partition-sensitive TableIR nodes are logical
//! extensions whose children are planned before their physical implementation.
use crate::plan::PhysicalPlan;
use crate::{Result, TableError, TableIR, TableNode, TableRef};
use arrow_array::Array;
use async_trait::async_trait;
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, ScalarValue, SplitPoint};
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, RangePartitioning};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_session::Session;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Not;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use vail_execution::logical_expr::lower_logical_with_bindings;

static NEXT_NODE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct OrderedNode {
    id: u64,
    freeze: bool,
    independent: bool,
    input: LogicalPlan,
    schema: DFSchemaRef,
}
impl PartialEq for OrderedNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for OrderedNode {}
impl PartialOrd for OrderedNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl Hash for OrderedNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
impl UserDefinedLogicalNodeCore for OrderedNode {
    fn name(&self) -> &str {
        if self.independent {
            "VailIndependentConsumer"
        } else if self.freeze {
            "VailRngContext"
        } else {
            "VailOrderedPartitions"
        }
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
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
            "{}",
            if self.freeze {
                "VailRngContext"
            } else {
                "VailOrderedPartitions"
            }
        )
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "invalid ordered partitions rewrite".into(),
            ));
        }
        let input = inputs.remove(0);
        Ok(Self {
            // A transparent ordered node follows its rewritten child schema.
            // Frozen consumer/RNG contexts keep the schema captured when the
            // context was created, matching their existing identity contract.
            schema: if self.freeze {
                self.schema.clone()
            } else {
                input.schema().clone()
            },
            input,
            ..self.clone()
        })
    }
}

pub(crate) fn ordered(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(OrderedNode {
            id: NEXT_NODE.fetch_add(1, Ordering::Relaxed),
            freeze: false,
            independent: false,
            schema: input.schema().clone(),
            input,
        }),
    })
}

/// Declare an independent relational consumer before physical planning.
pub(crate) fn independent_consumer(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(OrderedNode {
            id: NEXT_NODE.fetch_add(1, Ordering::Relaxed),
            freeze: true,
            independent: true,
            schema: input.schema().clone(),
            input,
        }),
    })
}

fn frozen_rng_context(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(OrderedNode {
            id: NEXT_NODE.fetch_add(1, Ordering::Relaxed),
            freeze: true,
            independent: false,
            schema: input.schema().clone(),
            input,
        }),
    })
}

/// Split aggregate input streams without merging existing source partitions.
/// Keep the input visible for projection pushdown.
#[derive(Clone, Debug)]
struct SplitPartitionsNode {
    id: u64,
    input: LogicalPlan,
    schema: DFSchemaRef,
    partitions: usize,
}
impl PartialEq for SplitPartitionsNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SplitPartitionsNode {}
impl PartialOrd for SplitPartitionsNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl Hash for SplitPartitionsNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
impl UserDefinedLogicalNodeCore for SplitPartitionsNode {
    fn name(&self) -> &str {
        "VailSplitPartitions"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailSplitPartitions: {}", self.partitions)
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "invalid partition split rewrite".into(),
            ));
        }
        let input = inputs.remove(0);
        Ok(Self {
            schema: input.schema().clone(),
            input,
            ..self.clone()
        })
    }
}

pub(crate) fn split_partitions(input: LogicalPlan, partitions: usize) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(SplitPartitionsNode {
            id: NEXT_NODE.fetch_add(1, Ordering::Relaxed),
            schema: input.schema().clone(),
            input,
            partitions: partitions.max(1),
        }),
    })
}

#[derive(Clone, Debug)]
struct RestorePartitionsNode {
    id: u64,
    input: LogicalPlan,
    schema: DFSchemaRef,
    partition_name: String,
    partitions: usize,
}
impl PartialEq for RestorePartitionsNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for RestorePartitionsNode {}
impl PartialOrd for RestorePartitionsNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl Hash for RestorePartitionsNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
impl UserDefinedLogicalNodeCore for RestorePartitionsNode {
    fn name(&self) -> &str {
        "VailRestorePartitions"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let mut required = output_columns.to_vec();
        let partition = self
            .input
            .schema()
            .index_of_column_by_name(None, &self.partition_name)?;
        required.push(partition);
        required.sort_unstable();
        required.dedup();
        Some(vec![required])
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailRestorePartitions: {}", self.partitions)
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "invalid partition restoration rewrite".into(),
            ));
        }
        let input = inputs.remove(0);
        Ok(Self {
            schema: input.schema().clone(),
            input,
            ..self.clone()
        })
    }
}

pub(crate) fn restore_partitions(
    input: LogicalPlan,
    partition_name: String,
    partitions: usize,
) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(RestorePartitionsNode {
            id: NEXT_NODE.fetch_add(1, Ordering::Relaxed),
            schema: input.schema().clone(),
            input,
            partition_name,
            partitions: partitions.max(1),
        }),
    })
}

#[derive(Clone, Debug)]
struct VailNode {
    id: u64,
    table: TableRef,
    inputs: Vec<LogicalPlan>,
    row_input_count: usize,
    row_input_indices: Vec<usize>,
    child_global_inputs: Vec<Option<usize>>,
    output_global_input: Option<usize>,
    binding_names: Vec<String>,
    schema: DFSchemaRef,
}
impl PartialEq for VailNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for VailNode {}
impl PartialOrd for VailNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl Hash for VailNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
impl UserDefinedLogicalNodeCore for VailNode {
    fn name(&self) -> &str {
        "VailPartitionOperation"
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
            "VailPartitionOperation: {}",
            node_name(self.table.node())
        )
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != self.inputs.len() {
            return Err(DataFusionError::Plan(
                "invalid Vail extension rewrite".into(),
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

fn node_name(node: &TableNode) -> &'static str {
    match node {
        TableNode::Prepared { .. } => "Prepared",
        TableNode::IntervalRead { .. } => "IntervalRead",
        TableNode::Repartition { .. } => "Repartition",
        TableNode::FilterPartitions { .. } => "FilterPartitions",
        TableNode::GroupWithinPartitions { .. } => "GroupWithinPartitions",
        TableNode::MatrixEntriesByKeyRuns { .. } => "MatrixEntriesByKeyRuns",
        TableNode::MapPartitions { .. } => "MapPartitions",
        TableNode::Read { .. } => "Read",
        TableNode::TextRead { .. } => "TextRead",
        TableNode::BgenSelect { .. } => "BgenVariantSelection",
        TableNode::Parallelize { .. } => "Parallelize",
        TableNode::Expression { .. } => "Expression",
        TableNode::Range { .. } => "Range",
        TableNode::MapGlobals { .. } => "MapGlobals",
        TableNode::MapRows { .. } => "MapRows",
        TableNode::Filter { .. } => "Filter",
        TableNode::Explode { .. } => "Explode",
        TableNode::KeyBy { .. } => "KeyBy",
        TableNode::Lookup { .. } => "Lookup",
        TableNode::Join { .. } => "Join",
        TableNode::MultiWayZipJoin { .. } => "MultiWayZipJoin",
        TableNode::OrderBy { .. } => "OrderBy",
        TableNode::Head { .. } => "Head",
        TableNode::Tail { .. } => "Tail",
        TableNode::Union { .. } => "Union",
        TableNode::Distinct { .. } => "Distinct",
        TableNode::Aggregate { .. } => "Aggregate",
    }
}

pub(crate) fn children(table: &TableRef) -> Vec<TableRef> {
    match table.node() {
        TableNode::Read { .. }
        | TableNode::IntervalRead { .. }
        | TableNode::TextRead { .. }
        | TableNode::Range { .. }
        | TableNode::Parallelize { .. }
        | TableNode::Expression { .. }
        | TableNode::Prepared { .. } => vec![],
        TableNode::BgenSelect { source, variants } => vec![source.clone(), variants.clone()],
        TableNode::Join { left, right, .. } | TableNode::Lookup { left, right, .. } => {
            vec![left.clone(), right.clone()]
        }
        TableNode::MultiWayZipJoin { tables, .. } => tables.clone(),
        TableNode::Union { children } => children.clone(),
        TableNode::Repartition { child, .. }
        | TableNode::FilterPartitions { child, .. }
        | TableNode::GroupWithinPartitions { child, .. }
        | TableNode::MatrixEntriesByKeyRuns { child, .. }
        | TableNode::MapPartitions { child, .. }
        | TableNode::MapGlobals { child }
        | TableNode::MapRows { child, .. }
        | TableNode::Filter { child, .. }
        | TableNode::Explode { child, .. }
        | TableNode::KeyBy { child, .. }
        | TableNode::OrderBy { child, .. }
        | TableNode::Head { child, .. }
        | TableNode::Tail { child, .. }
        | TableNode::Distinct { child }
        | TableNode::Aggregate { child, .. } => vec![child.clone()],
    }
}

fn extension(
    table: &TableRef,
    context: &mut crate::logical::LoweringContext,
) -> Result<LogicalPlan> {
    let row_children = children(table);
    // A zip can consume the same immutable source in several slots. Lower
    // it once: duplicating logical trees expands nested zip DAGs exponentially
    // before the physical source owner can share their execution.
    let share_zip = matches!(table.node(), TableNode::MultiWayZipJoin { .. });
    let mut unique = std::collections::HashMap::new();
    let mut inputs = Vec::new();
    let mut row_input_indices = Vec::with_capacity(row_children.len());
    for child in &row_children {
        let id = Arc::as_ptr(child) as usize;
        if share_zip && let Some(index) = unique.get(&id) {
            row_input_indices.push(*index);
            continue;
        }
        let index = inputs.len();
        inputs.push(lower_inner(child, context)?);
        row_input_indices.push(index);
        unique.insert(id, index);
    }
    let row_input_count = inputs.len();
    let mut child_global_inputs = Vec::with_capacity(row_children.len());
    for child in &row_children {
        if child.globals().schema().fields().is_empty() {
            child_global_inputs.push(None);
        } else {
            child_global_inputs.push(Some(inputs.len()));
            inputs.push(crate::logical_globals::lower_in_context(
                child.globals(),
                context,
            )?);
        }
    }
    let output_global_input = if table.globals().schema().fields().is_empty() {
        None
    } else {
        let index = inputs.len();
        inputs.push(crate::logical_globals::lower_in_context(
            table.globals(),
            context,
        )?);
        Some(index)
    };
    let bindings = bindings_for_node(table.node());
    let binding_names = bindings
        .map(|bindings| bindings.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    if let Some(bindings) = bindings {
        inputs.extend(
            bindings
                .values()
                .map(|globals| crate::logical_globals::lower_in_context(globals, context))
                .collect::<Result<Vec<_>>>()?,
        );
    }
    let schema = Arc::new(DFSchema::try_from(
        crate::plan::physical_schema(table).as_ref().clone(),
    )?);
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(VailNode {
            id: NEXT_NODE.fetch_add(1, Ordering::Relaxed),
            table: table.clone(),
            inputs,
            row_input_count,
            row_input_indices,
            child_global_inputs,
            output_global_input,
            binding_names,
            schema,
        }),
    }))
}

fn bindings_for_node(node: &TableNode) -> Option<&crate::globals::GlobalBindings> {
    match node {
        TableNode::Expression { bindings, .. }
        | TableNode::MapRows { bindings, .. }
        | TableNode::Filter { bindings, .. }
        | TableNode::MapPartitions { bindings, .. }
        | TableNode::Lookup { bindings, .. }
        | TableNode::Aggregate { bindings, .. } => Some(bindings),
        _ => None,
    }
}

fn indexed_column(plan: &LogicalPlan, index: usize) -> Expr {
    Expr::Column(datafusion_common::Column::from(
        plan.schema().qualified_field(index),
    ))
}

fn contains_global(expr: &vail_execution::expressions::Expr) -> bool {
    let mut found = false;
    expr.visit_unique(&mut |node| {
        found |= matches!(node, vail_execution::expressions::Expr::Global { .. });
    });
    found
}

fn contains_scan_expr(expr: &vail_execution::expressions::Expr) -> bool {
    let mut found = false;
    expr.visit_unique(&mut |node| found |= is_scan(node));
    found
}

fn is_scan(expr: &vail_execution::expressions::Expr) -> bool {
    matches!(expr, vail_execution::expressions::Expr::Aggregate { options, .. }
        if options.get("__vail_scan").and_then(serde_json::Value::as_bool) == Some(true))
}

fn lower_scans(
    _table: &TableRef,
    child: &TableRef,
    input: LogicalPlan,
    fields: &[(String, vail_execution::expressions::Expr)],
    bindings: &std::collections::BTreeMap<String, usize>,
) -> Result<LogicalPlan> {
    use datafusion_expr::WindowFunctionDefinition;
    use vail_execution::expressions::Expr as VExpr;

    let input = crate::logical::ordered(input);
    let mut ordinal_name = "__vail_scan_ordinal".to_owned();
    while input
        .schema()
        .has_column(&datafusion_common::Column::new_unqualified(&ordinal_name))
    {
        ordinal_name.push('_');
    }
    let mut row_number = WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        vec![],
    );
    row_number.params.window_frame = WindowFrame::new(Some(true));
    let numbered = vec![Expr::WindowFunction(Box::new(row_number)).alias(&ordinal_name)];
    let input = LogicalPlanBuilder::from(input).window(numbered)?.build()?;

    let mut scan_windows = Vec::new();
    let mut private_scan_names = HashSet::new();
    let mut identities: Vec<(String, String, Option<datafusion_common::ScalarValue>)> = Vec::new();
    fn replace_scans(
        expr: &VExpr,
        input: &LogicalPlan,
        child: &TableRef,
        bindings: &std::collections::BTreeMap<String, usize>,
        ordinal_name: &str,
        windows: &mut Vec<Expr>,
        names: &mut HashSet<String>,
        identities: &mut Vec<(String, String, Option<datafusion_common::ScalarValue>)>,
    ) -> Result<VExpr> {
        use arrow_schema::DataType;
        use datafusion_common::ScalarValue;
        use datafusion_expr::WindowFunctionDefinition;
        if let VExpr::Aggregate {
            function,
            value,
            options,
            init,
            ..
        } = expr
        {
            if options
                .get("__vail_scan")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            {
                return Err(TableError::Invalid(
                    "aggregations and scans cannot be mixed".into(),
                ));
            }
            if !init.is_empty() {
                return Err(TableError::Invalid(
                    "scan aggregate initializers are not yet supported".into(),
                ));
            }
            let options = options
                .as_object()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|(k, _)| k != "__vail_scan")
                .collect();
            let mut raw_name = "__vail_scan_raw".to_owned();
            while input
                .schema()
                .has_column(&datafusion_common::Column::new_unqualified(&raw_name))
                || !names.insert(raw_name.clone())
            {
                raw_name.push('_');
            }
            let mut name = format!("{raw_name}_value");
            while !names.insert(name.clone()) {
                name.push('_');
            }
            let reducer = crate::aggregation::Reducer {
                name: raw_name.clone(),
                function: function.clone(),
                value: value.as_deref().cloned(),
                options: serde_json::Value::Object(options),
            };
            let mut logical_value = match value {
                Some(value) => vail_execution::logical_expr::lower_logical_with_bindings(
                    value,
                    input.schema(),
                    child.globals().schema(),
                    bindings,
                )?,
                None => datafusion_expr::lit(1i64),
            };
            let dtype = match value {
                Some(value) => Some(
                    value
                        .dtype_with_globals(input.schema().as_arrow(), child.globals().schema())?
                        .to_arrow()?,
                ),
                None => None,
            };
            if function == "count_where" {
                if dtype != Some(DataType::Boolean) {
                    return Err(TableError::Invalid(
                        "count_where requires Boolean input".into(),
                    ));
                }
                logical_value = datafusion_functions::core::nullif()
                    .call(vec![logical_value, datafusion_expr::lit(false)]);
            }
            let udf = crate::logical_aggregation::factory(&reducer, dtype.as_ref())?;
            let mut window = WindowFunction::new(
                WindowFunctionDefinition::AggregateUDF(udf),
                vec![logical_value],
            );
            window.params.order_by = vec![datafusion_expr::col(ordinal_name).sort(true, true)];
            let mut frame = WindowFrame::new(Some(true));
            frame.units = WindowFrameUnits::Rows;
            frame.end_bound = WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1)));
            window.params.window_frame = frame;
            let result = Expr::WindowFunction(Box::new(window));
            let identity = match function.as_str() {
                "count" | "count_where" => Some(ScalarValue::Int64(Some(0))),
                "sum" => match dtype.as_ref() {
                    Some(DataType::Float32 | DataType::Float64) => {
                        Some(ScalarValue::Float64(Some(0.0)))
                    }
                    _ => Some(ScalarValue::Int64(Some(0))),
                },
                "product" => match dtype.as_ref() {
                    Some(DataType::Float32 | DataType::Float64) => {
                        Some(ScalarValue::Float64(Some(1.0)))
                    }
                    _ => Some(ScalarValue::Int64(Some(1))),
                },
                "collect" => match dtype.as_ref() {
                    Some(ty) => Some(ScalarValue::List(ScalarValue::new_list(&[], ty, true))),
                    None => None,
                },
                "any" => Some(ScalarValue::Boolean(Some(false))),
                "all" => Some(ScalarValue::Boolean(Some(true))),
                "fraction" => Some(ScalarValue::Float64(None)),
                "mean" | "stddev" => Some(ScalarValue::Float64(Some(f64::NAN))),
                _ => None,
            };
            windows.push(result.alias(&raw_name));
            identities.push((raw_name, name.clone(), identity));
            return Ok(VExpr::Column { name });
        }
        expr.try_map_children(&mut |child_expr: &VExpr| {
            replace_scans(
                child_expr,
                input,
                child,
                bindings,
                ordinal_name,
                windows,
                names,
                identities,
            )
        })
        .map_err(Into::into)
    }
    let mut rewritten = Vec::with_capacity(fields.len());
    for (name, expr) in fields {
        rewritten.push((
            name.clone(),
            replace_scans(
                expr,
                &input,
                child,
                bindings,
                &ordinal_name,
                &mut scan_windows,
                &mut private_scan_names,
                &mut identities,
            )?,
        ));
    }
    let input = LogicalPlanBuilder::from(input)
        .window(scan_windows)?
        .build()?;
    let input = if identities.is_empty() {
        input
    } else {
        let mut projections = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        for (raw, name, identity) in identities {
            let value = if let Some(identity) = identity {
                datafusion_expr::when(
                    datafusion_expr::col(&ordinal_name).eq(datafusion_expr::lit(1u64)),
                    datafusion_expr::lit(identity),
                )
                .otherwise(datafusion_expr::col(raw))?
            } else {
                datafusion_expr::col(raw)
            };
            projections.push(value.alias(name));
        }
        LogicalPlanBuilder::from(input)
            .project(projections)?
            .build()?
    };
    let mut output = Vec::with_capacity(fields.len() + 1);
    for (name, expr) in &rewritten {
        output.push(
            vail_execution::logical_expr::lower_logical_with_bindings(
                expr,
                input.schema(),
                child.globals().schema(),
                bindings,
            )?
            .alias(name),
        );
    }
    let partition_index = child.globals().schema().fields().len() + child.schema().fields().len();
    output.push(indexed_column(&input, partition_index));
    Ok(LogicalPlanBuilder::from(input).project(output)?.build()?)
}

fn is_rng_uid(expr: &vail_execution::expressions::Expr) -> bool {
    matches!(expr, vail_execution::expressions::Expr::Aggregate{ function, options, .. }
        if function=="count" && options.get("__vail_rng_uid").and_then(serde_json::Value::as_bool)==Some(true))
}

/// RNG identities are ordinals, not a parallel aggregate scan. Number the
/// ordered input once, then restore its logical partition tags. This avoids
/// replaying stateful upstream operators for a scan's summary/carry branches.
fn lower_rng_uids(
    child: &TableRef,
    input: LogicalPlan,
    fields: &[(String, vail_execution::expressions::Expr)],
    bindings: &std::collections::BTreeMap<String, usize>,
) -> Result<LogicalPlan> {
    use datafusion_expr::ExprSchemable;
    let partition_index = child.globals().schema().fields().len() + child.schema().fields().len();
    let partition_name = input.schema().field(partition_index).name().clone();
    let mut ordinal_name = "__vail_rng_ordinal".to_owned();
    while input
        .schema()
        .fields()
        .iter()
        .any(|f| f.name() == &ordinal_name)
    {
        ordinal_name.push('_');
    }
    let mut ordinal = WindowFunction::new(
        datafusion_expr::WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        vec![],
    );
    ordinal.params.window_frame = WindowFrame::new(Some(true));
    let numbered = LogicalPlanBuilder::from(ordered(input))
        .window(vec![
            Expr::WindowFunction(Box::new(ordinal)).alias(&ordinal_name),
        ])?
        .build()?;
    let mut projections = Vec::new();
    for (name, expr) in fields {
        let value = if is_rng_uid(expr) {
            datafusion_expr::col(&ordinal_name)
                .cast_to(&arrow_schema::DataType::Int64, numbered.schema())?
                - datafusion_expr::lit(1_i64)
        } else {
            lower_logical_with_bindings(
                expr,
                numbered.schema(),
                child.globals().schema(),
                bindings,
            )?
        };
        projections.push(value.alias(name));
    }
    projections.push(indexed_column(&numbered, partition_index));
    let result = LogicalPlanBuilder::from(numbered)
        .project(projections)?
        .build()?;
    Ok(frozen_rng_context(restore_partitions(
        result,
        partition_name,
        child.known_n_partitions().unwrap_or(1),
    )))
}

/// Stable SQL sort for native key types. The explicit ordinal is the final
/// key, preserving duplicate-key encounter order through optimizer rewrites.
fn lower_sort(
    child: &TableRef,
    keys: &[(String, bool)],
    context: &mut crate::logical::LoweringContext,
) -> Result<LogicalPlan> {
    if keys.is_empty() {
        return lower_inner(child, context);
    }
    let input = ordered(lower_inner(child, context)?);
    let mut ordinal_name = "__vail_sort_ordinal".to_owned();
    while input
        .schema()
        .has_column_with_unqualified_name(&ordinal_name)
    {
        ordinal_name.push('_');
    }
    let mut ordinal = WindowFunction::new(
        datafusion_expr::WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        vec![],
    );
    ordinal.params.window_frame = WindowFrame::new(Some(true));
    let numbered = LogicalPlanBuilder::from(input)
        .window(vec![
            Expr::WindowFunction(Box::new(ordinal)).alias(&ordinal_name),
        ])?
        .build()?;
    let mut order = keys
        .iter()
        .map(|(name, descending)| {
            Expr::Column(datafusion_common::Column::new_unqualified(name)).sort(!descending, false)
        })
        .collect::<Vec<_>>();
    order.push(
        Expr::Column(datafusion_common::Column::new_unqualified(&ordinal_name)).sort(true, true),
    );
    let sorted = LogicalPlanBuilder::from(numbered).sort(order)?.build()?;
    let mut fields = (0..child.schema().fields().len())
        .map(|i| indexed_column(&sorted, i))
        .collect::<Vec<_>>();
    fields.push(datafusion_expr::lit(0u64).alias(crate::plan::private_name(
        child.schema(),
        child.globals().schema(),
        "__vail_partition",
    )));
    // Row order remains observable after projections/global binding. SQL
    // cross joins can otherwise make the optimizer discard this sort. The
    // sort is already singleton; record its full ordering contract before
    // projection drops the ordinal used to stabilize duplicate-key ties.
    Ok(LogicalPlanBuilder::from(ordered(sorted))
        .project(fields)?
        .build()?)
}

fn native_sort_keys(child: &TableRef, keys: impl Iterator<Item = String>) -> Result<bool> {
    for name in keys {
        if !vail_execution::expressions::Type::from_arrow(
            child.schema().field_with_name(&name)?.data_type(),
        )?
        .native_key()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// One synchronous lowering request shares completed row/global subplans.
/// Owning roots keep native identity keys valid for the full compilation.
#[derive(Default)]
pub(crate) struct LoweringContext {
    tables: std::collections::HashMap<usize, (TableRef, LogicalPlan)>,
    pub(crate) globals: std::collections::HashMap<usize, (crate::globals::GlobalsRef, LogicalPlan)>,
}

pub(crate) fn lower(table: &TableRef) -> Result<LogicalPlan> {
    lower_in_context(table, &mut LoweringContext::default())
}
pub(crate) fn lower_in_context(
    table: &TableRef,
    context: &mut LoweringContext,
) -> Result<LogicalPlan> {
    let table = crate::nested_projection::prepare_vcf(table);
    lower_inner(&table, context)
}

pub(crate) fn lower_for_expressions<'a>(
    table: &'a TableRef,
    expressions: impl Iterator<Item = &'a vail_execution::expressions::Expr>,
) -> Result<LogicalPlan> {
    lower_for_expressions_in_context(table, expressions, &mut LoweringContext::default())
}
pub(crate) fn lower_for_expressions_in_context<'a>(
    table: &'a TableRef,
    expressions: impl Iterator<Item = &'a vail_execution::expressions::Expr>,
    context: &mut LoweringContext,
) -> Result<LogicalPlan> {
    let table = crate::nested_projection::prepare_vcf_for(table, expressions);
    lower_inner(&table, context)
}

fn lower_inner(table: &TableRef, context: &mut LoweringContext) -> Result<LogicalPlan> {
    let key = Arc::as_ptr(table) as usize;
    if let Some((_, plan)) = context.tables.get(&key) {
        return Ok(plan.clone());
    }
    let plan = lower_uncached(table, context)?;
    context.tables.insert(key, (table.clone(), plan.clone()));
    Ok(plan)
}
fn lower_uncached(table: &TableRef, context: &mut LoweringContext) -> Result<LogicalPlan> {
    match table.node() {
        TableNode::BgenSelect { source, variants } => Ok(crate::bgen_select::logical(
            source.clone(),
            variants.clone(),
            vec![lower_inner(source, context)?, lower_inner(variants, context)?],
        )),
        TableNode::Read { .. }
        | TableNode::IntervalRead { .. }
        | TableNode::TextRead { .. }
        | TableNode::Range { .. }
        | TableNode::Parallelize { .. }
        | TableNode::Prepared { .. } => {
            let provider = crate::provider::provider(table)?;
            Ok(
                LogicalPlanBuilder::scan("vail_source", provider_as_source(provider), None)?
                    .build()?,
            )
        }
        TableNode::MapGlobals { child } => lower_inner(child, context),
        // Clearing key metadata does not alter rows or their ordering.
        TableNode::KeyBy { child, keys } if keys.is_empty() => lower_inner(child, context),
        TableNode::KeyBy { child, keys } if native_sort_keys(child, keys.iter().cloned())? => {
            lower_sort(
                child,
                &keys
                    .iter()
                    .map(|name| (name.clone(), false))
                    .collect::<Vec<_>>(),
                context,
            )
        }
        TableNode::OrderBy { child, keys }
            if native_sort_keys(child, keys.iter().map(|(name, _)| name.clone()))? =>
        {
            lower_sort(child, keys, context)
        }
        TableNode::Head { n: 0, .. } => {
            Ok(LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::try_from(
                    crate::plan::physical_schema(table).as_ref().clone(),
                )?),
            }))
        }
        TableNode::Head { child, n } => Ok(LogicalPlanBuilder::from(ordered(lower_inner(
            child, context,
        )?))
        .limit(0, Some(*n))?
        .build()?),
        TableNode::MapPartitions {
            child,
            rows_name,
            body,
            bindings,
        } => match crate::partition_map::lower_stream(child, rows_name, body, bindings)? {
            Some(rewritten) => lower_inner(&rewritten, context),
            None => extension(table, context),
        },
        // Vail union concatenates observable execution partitions. Native SQL
        // Union permits branch interleaving and repartitioning; the extension
        // keeps each optimized child partition as an independent output.
        TableNode::Union { .. } => extension(table, context),
        TableNode::Explode { child, path } if path.len() == 1 => {
            let input = lower_inner(child, context)?;
            Ok(LogicalPlanBuilder::from(input)
                .unnest_column_with_options(
                    datafusion_common::Column::new_unqualified(&path[0]),
                    datafusion_common::UnnestOptions::new()
                        .with_null_handling(datafusion_common::NullHandling::Drop),
                )?
                .build()?)
        }
        TableNode::Aggregate { child, keys, .. } => {
            let input = lower_inner(child, context)?;
            match crate::logical_aggregation::lower_logical_grouped(table, input, context)? {
                Some(plan) => {
                    let plan = if keys.is_empty() {
                        plan
                    } else {
                        let sorts = keys
                            .iter()
                            .map(|(name, _)| {
                                let field = table.schema().field_with_name(name)?;
                                let dtype = vail_execution::expressions::Type::from_arrow(
                                    field.data_type(),
                                )?;
                                dtype.ensure_keyable()?;
                                if !dtype.native_key() {
                                    return Err(TableError::Invalid(
                                        "sorting requires a native DataFusion key type".into(),
                                    ));
                                }
                                Ok(
                                    Expr::Column(datafusion_common::Column::new_unqualified(name))
                                        .sort(true, false),
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        LogicalPlanBuilder::from(plan).sort(sorts)?.build()?
                    };
                    let mut fields = plan
                        .schema()
                        .columns()
                        .into_iter()
                        .map(Expr::Column)
                        .collect::<Vec<_>>();
                    fields.push(datafusion_expr::lit(0u64).alias(crate::plan::private_name(
                        table.schema(),
                        table.globals().schema(),
                        "__vail_partition",
                    )));
                    // OrderedNode tags its final field with the output
                    // partition ID. Place it after this projection so that
                    // the private UInt64 tag, not the last user aggregate
                    // (which may be a nested struct), is replaced. This also
                    // protects the grouped-key sort from later rewrites.
                    Ok(ordered(
                        LogicalPlanBuilder::from(plan).project(fields)?.build()?,
                    ))
                }
                None => Err(TableError::Invalid(
                    "missing logical grouped aggregate plan".into(),
                )),
            }
        }
        TableNode::MapRows {
            child,
            fields,
            bindings,
        } => {
            if bindings.is_empty() {
                if let Some(plan) = crate::computed_source::try_lower(table, child, fields)? {
                    return Ok(plan);
                }
            }
            let lowered_child = if matches!(child.node(), TableNode::Read { .. }) {
                let paths = crate::nested_projection::map_rows_storage_paths(fields, child.schema());
                if paths.is_empty() {
                    lower_inner(child, context)?
                } else {
                    let provider =
                        crate::provider::provider_with_nested_projection_paths(child, paths)?;
                    LogicalPlanBuilder::scan("vail_source", provider_as_source(provider), None)?
                        .build()?
                }
            } else {
                lower_inner(child, context)?
            };
            let binds_globals = fields.iter().any(|(_, expr)| {
                contains_global(expr) || is_rng_uid(expr) || contains_scan_expr(expr)
            });
            let input = if binds_globals {
                crate::logical_globals::bind_globals(lowered_child, child, context)?
            } else {
                lowered_child
            };
            let empty_globals = arrow_schema::Schema::empty();
            let globals_schema = if binds_globals {
                child.globals().schema().as_ref()
            } else {
                &empty_globals
            };
            // Native DataFusion scalar subqueries keep singleton nested
            // metadata scalar all the way into opaque expression arguments.
            // Scans/RNG retain their existing indexed-input lowering contract.
            let scalar_bindings = !fields.iter().any(|(_, expr)|
                is_rng_uid(expr) || contains_scan_expr(expr));
            let (input, binding_schema, indices, scalar_replacements) = if scalar_bindings {
                let (schema, indices, replacements) =
                    crate::logical_globals::scalar_external_bindings(&input, bindings, context)?;
                (input, schema, indices, replacements)
            } else {
                let (input, indices) = crate::logical_globals::bind_external(input, bindings, context)?;
                let schema = input.schema().clone();
                (input, schema, indices, Default::default())
            };
            if fields.iter().any(|(_, expr)| is_rng_uid(expr)) {
                return lower_rng_uids(child, input, fields, &indices);
            }
            if fields.iter().any(|(_, e)| {
                let mut found = false;
                e.visit_unique(&mut |n| found |= is_scan(n));
                found
            }) {
                if let Some(plan) = crate::logical_scan::initialize(
                    table,
                    child,
                    input.clone(),
                    fields,
                    &indices,
                    context,
                )? {
                    return Ok(plan);
                }
                if let Some(plan) =
                    crate::logical_scan::lower(table, child, input.clone(), fields, &indices)?
                {
                    return Ok(plan);
                }
                return lower_scans(table, child, input, fields, &indices);
            }
            let projected = fields
                .iter()
                .map(|(name, expr)| {
                    lower_logical_with_bindings(expr, &binding_schema, globals_schema, &indices)
                        .and_then(|e| {
                            use datafusion_common::tree_node::{Transformed, TreeNode};
                            e.transform_up(|node| {
                                if let Expr::Column(column) = &node
                                    && column.relation.is_none()
                                    && let Some(value) = scalar_replacements.get(&column.name)
                                {
                                    return Ok(Transformed::yes(value.clone()));
                                }
                                Ok(Transformed::no(node))
                            }).map(|result| result.data).map_err(Into::into)
                        })
                        .map(|e| match expr {
                            vail_execution::expressions::Expr::Column { name: source }
                                if source == name =>
                            {
                                e
                            }
                            _ => e.alias(name),
                        })
                })
                .collect::<std::result::Result<Vec<_>, _>>();
            match projected {
                Ok(mut projected) => {
                    let partition_index = (if binds_globals {
                        child.globals().schema().fields().len()
                    } else {
                        0
                    }) + child.schema().fields().len();
                    projected.push(indexed_column(&input, partition_index));
                    Ok(LogicalPlanBuilder::from(input)
                        .project(projected)?
                        .build()?)
                }
                Err(vail_execution::expressions::ExprError::Invalid(message))
                    if message.contains("needs a Vail logical kernel") =>
                {
                    extension(table, context)
                }
                Err(error) => Err(error.into()),
            }
        }
        TableNode::Filter {
            child,
            predicate,
            keep,
            bindings,
        } => {
            let binds_globals = contains_global(predicate);
            let lowered_child = lower_inner(child, context)?;
            let input = if binds_globals {
                crate::logical_globals::bind_globals(lowered_child, child, context)?
            } else {
                lowered_child
            };
            let (input, indices) = crate::logical_globals::bind_external(input, bindings, context)?;
            let empty_globals = arrow_schema::Schema::empty();
            let globals_schema = if binds_globals {
                child.globals().schema().as_ref()
            } else {
                &empty_globals
            };
            match lower_logical_with_bindings(predicate, input.schema(), globals_schema, &indices) {
                Ok(predicate) => {
                    let predicate = if *keep { predicate } else { predicate.not() };
                    let filtered = LogicalPlanBuilder::from(input).filter(predicate)?.build()?;
                    let offset = if binds_globals {
                        child.globals().schema().fields().len()
                    } else {
                        0
                    };
                    let projected = (0..=child.schema().fields().len())
                        .map(|i| indexed_column(&filtered, offset + i))
                        .collect::<Vec<_>>();
                    Ok(LogicalPlanBuilder::from(filtered)
                        .project(projected)?
                        .build()?)
                }
                Err(vail_execution::expressions::ExprError::Invalid(message))
                    if message.contains("needs a Vail logical kernel") =>
                {
                    extension(table, context)
                }
                Err(error) => Err(error.into()),
            }
        }
        _ => extension(table, context),
    }
}

/// Display the plan DataFusion will optimize and lower for an action.
pub(crate) fn display(table: &TableRef) -> Result<String> {
    let state = session_state(max_partitions(table));
    let logical = crate::logical_parameters::parameterize(lower(table)?, state.execution_props())?;
    Ok(state.optimize(&logical)?.display_indent().to_string())
}

pub(crate) async fn physical(table: &TableRef) -> Result<PhysicalPlan> {
    let state = session_state(max_partitions(table));
    let context = state.task_ctx();
    let rows = crate::wide_batch::for_table(table, context.session_config().batch_size());
    physical_in_context(lower(table)?, state, context, rows).await
}

pub(crate) fn max_partitions(table: &TableRef) -> usize {
    let mut seen = HashSet::new();
    let mut pending = vec![table.clone()];
    let mut maximum = 1;
    while let Some(table) = pending.pop() {
        if !seen.insert(Arc::as_ptr(&table) as usize) {
            continue;
        }
        maximum = maximum.max(table.known_n_partitions().unwrap_or(1));
        pending.extend(children(&table));
    }
    maximum
}

/// Whether a total reducer can use parallel partial aggregation without changing order.
pub(crate) fn should_fan_out_total(
    table: &TableRef,
    expression: &vail_execution::expressions::Expr,
) -> bool {
    crate::logical_aggregation::can_parallelize_total(expression)
        && !table
            .known_count()
            .is_some_and(|row_count| row_count <= SessionConfig::new().batch_size())
}

/// Select a target count for total aggregates while preserving source targets elsewhere.
pub(crate) fn total_aggregate_target(
    table: &TableRef,
    expression: &vail_execution::expressions::Expr,
) -> usize {
    let partitions = table.known_n_partitions().unwrap_or_else(|| max_partitions(table));
    if !should_fan_out_total(table, expression) {
        return partitions;
    }
    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(crate::EXECUTION_CONCURRENCY);
    // Do not create more lanes than there are batches of known work. Existing
    // partitions are retained even when there are more of them than workers.
    let useful_lanes = table.known_count().map_or(workers, |rows| {
        rows.div_ceil(SessionConfig::new().batch_size()).max(1).min(workers)
    });
    partitions.max(useful_lanes)
}

/// Scalar subqueries are resolved once per action, before optimizing and
/// compiling the complete data relation. No execution operator invokes a planner.
pub(crate) async fn physical_plan(
    logical: LogicalPlan,
    target_partitions: usize,
) -> Result<PhysicalPlan> {
    let state = session_state(target_partitions);
    let context = state.task_ctx();
    physical_in_context(logical, state, context, None).await
}

pub(crate) async fn physical_with_context(
    table: &TableRef,
    context: Arc<datafusion_execution::TaskContext>,
) -> Result<PhysicalPlan> {
    let caller_config = context
        .session_config()
        .clone()
        // Source partitions are observable table partitions. Caller-owned
        // sessions must preserve that contract just like Vail's default
        // session, which disables DataFusion's round-robin repartition rule.
        .with_round_robin_repartition(false);
    let wide_batch_rows = crate::wide_batch::for_table(table, caller_config.batch_size());
    let state = SessionContext::new_with_config_rt(caller_config, context.runtime_env()).state();
    physical_in_context(
        lower(table)?,
        with_metadata_optimizer(state),
        context,
        wide_batch_rows,
    )
    .await
}

async fn physical_in_context(
    logical: LogicalPlan,
    state: datafusion::execution::SessionState,
    context: Arc<datafusion_execution::TaskContext>,
    wide_batch_rows: Option<usize>,
) -> Result<PhysicalPlan> {
    let mut bindings = crate::action_parameters::ActionParameters::with_context(state, context);
    let logical = crate::action_parameters::resolve(logical, &mut bindings).await?;
    if let Some(rows) = wide_batch_rows {
        let config = crate::wide_batch::with_limit(bindings.context.session_config().clone(), rows);
        bindings.context =
            crate::wide_batch::task_context_with_config(&bindings.context, config.clone());
        bindings.state = datafusion::execution::session_state::SessionStateBuilder::new_from_existing(
            bindings.state,
        )
        .with_config(config)
        .build();
    }
    let plan = compile(logical, &bindings.state).await?;
    let plan = match wide_batch_rows {
        Some(rows) => crate::wide_batch::cap_filter_batches(plan, rows)?,
        None => plan,
    };
    Ok(crate::action_parameters::with_context(
        plan,
        bindings.context.clone(),
    ))
}

pub(crate) async fn compile(
    logical: LogicalPlan,
    state: &datafusion::execution::SessionState,
) -> Result<PhysicalPlan> {
    compile_mode(logical, state, false).await
}

pub(crate) async fn diagnostic(
    logical: LogicalPlan,
    target_partitions: usize,
) -> Result<PhysicalPlan> {
    compile_mode(logical, &session_state(target_partitions), true).await
}

async fn compile_mode(
    logical: LogicalPlan,
    state: &datafusion::execution::SessionState,
    diagnostic: bool,
) -> Result<PhysicalPlan> {
    let planner = DefaultPhysicalPlanner::with_extension_planners(vec![
        Arc::new(VailExtensionPlanner { diagnostic }),
        Arc::new(crate::logical_globals::GlobalsExtensionPlanner),
        Arc::new(crate::logical_same::SameExtensionPlanner),
    ]);
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut state = state.clone();
    // Keep large immutable Arrow constants opaque to expression optimization.
    // This runs after action parameters are resolved and before optimization.
    let logical = crate::logical_parameters::parameterize(logical, state.execution_props())?;
    let optimized = state.optimize(&logical)?;
    // DF55 physical filter pushdown can inline volatile projection expressions
    // and evaluate a checked cast before an earlier filter. Keep logical scan
    // pushdown, but omit that physical rewrite when evaluation order matters.
    let mut volatile = false;
    optimized.apply(|node| {
        volatile |= node.expressions().iter().any(Expr::is_volatile);
        Ok(TreeNodeRecursion::Continue)
    })?;
    if volatile {
        let rules = state
            .physical_optimizers()
            .iter()
            .filter(|rule| !rule.name().starts_with("FilterPushdown"))
            .cloned()
            .collect();
        state = datafusion::execution::session_state::SessionStateBuilder::new_from_existing(state)
            .with_physical_optimizer_rules(rules)
            .build();
    }
    let plan = planner.create_physical_plan(&optimized, &state).await?;
    let plan = crate::projection_broadcast::cache_nested_projection_broadcasts(plan)?;
    Ok(crate::rng_context::bind_consumers(plan)?)
}

#[derive(Debug)]
struct MetadataCrossJoin;

/// Rewrite each physical node once, retaining shared inputs across branches.
/// The original root is kept alive for the traversal, so pointer identities
/// cannot be recycled while their memoized replacements are in use.
fn rewrite_physical_dag(
    plan: PhysicalPlan,
    rewrite: &mut impl FnMut(PhysicalPlan) -> datafusion_common::Result<PhysicalPlan>,
) -> datafusion_common::Result<PhysicalPlan> {
    fn visit(
        plan: PhysicalPlan,
        memo: &mut std::collections::HashMap<usize, PhysicalPlan>,
        rewrite: &mut impl FnMut(PhysicalPlan) -> datafusion_common::Result<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        let identity = Arc::as_ptr(&plan) as *const () as usize;
        if let Some(result) = memo.get(&identity) {
            return Ok(result.clone());
        }
        let mut children = Vec::with_capacity(plan.children().len());
        let mut changed = false;
        for child in plan.children() {
            let result = visit(child.clone(), memo, rewrite)?;
            changed |= !Arc::ptr_eq(child, &result);
            children.push(result);
        }
        let plan = if changed {
            plan.replace_children(children, datafusion_physical_plan::ReplaceChildrenOptions::new(
                datafusion_physical_plan::ChildrenPropertiesMode::Recompute,
            ))?
        } else { plan };
        let result = rewrite(plan)?;
        memo.insert(identity, result.clone());
        Ok(result)
    }
    let original = plan.clone();
    let result = visit(plan, &mut std::collections::HashMap::new(), rewrite);
    drop(original);
    result
}

impl datafusion::physical_optimizer::PhysicalOptimizerRule for MetadataCrossJoin {
    fn name(&self) -> &str {
        "vail_arrow_metadata_cross_join"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(
        &self,
        plan: PhysicalPlan,
        _: &datafusion_common::config::ConfigOptions,
    ) -> datafusion_common::Result<PhysicalPlan> {
        rewrite_physical_dag(plan, &mut |plan| {
                if let Some(join) = plan
                    .as_ref()
                    .downcast_ref::<datafusion_physical_plan::joins::CrossJoinExec>()
                {
                    let metadata = join
                        .schema()
                        .fields()
                        .iter()
                        .any(|field| vail_ordering::has_type_metadata(field.data_type()));
                    if metadata {
                        return crate::plan::cross_join(
                            join.left().clone(),
                            join.right().clone(),
                        );
                    }
                }
                Ok(plan)
        })
    }
}

/// Count is insensitive to output order, even when its input happens to carry
/// an ordering certificate. Do not infer this for arbitrary user aggregates.
#[derive(Debug)]
struct CountOrderingDemand;
impl datafusion::physical_optimizer::PhysicalOptimizerRule for CountOrderingDemand {
    fn name(&self) -> &str { "vail_count_ordering_demand" }
    fn schema_check(&self) -> bool { true }
    fn optimize(&self, plan: PhysicalPlan, _: &datafusion_common::config::ConfigOptions)
        -> datafusion_common::Result<PhysicalPlan>
    {
        rewrite_physical_dag(plan, &mut |plan| {
            if crate::ordering_requirements::count_has_unordered_input(&plan) {
                let children = plan.children().into_iter()
                    .map(|child| crate::ordering_requirements::unordered(child.clone()))
                    .collect::<datafusion_common::Result<Vec<_>>>()?;
                return plan.replace_children(children,
                    datafusion_physical_plan::ReplaceChildrenOptions::new(
                        datafusion_physical_plan::ChildrenPropertiesMode::Recompute));
            }
            Ok(plan)
        })
    }
}

#[derive(Debug)]
struct EliminateIdentityRanges;
impl datafusion::physical_optimizer::PhysicalOptimizerRule for EliminateIdentityRanges {
    fn name(&self) -> &str {
        "vail_eliminate_identity_ranges"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(
        &self,
        plan: PhysicalPlan,
        _: &datafusion_common::config::ConfigOptions,
    ) -> datafusion_common::Result<PhysicalPlan> {
        rewrite_physical_dag(plan, &mut |plan| {
            if let Some(range) = plan.as_ref()
                .downcast_ref::<crate::partitioned::ProtectedRangeRepartitionExec>()
                && let Some(input) = range.identity_input()
            {
                return Ok(input);
            }
            Ok(plan)
        })
    }
}

fn session_state(target_partitions: usize) -> datafusion::execution::SessionState {
    // Vail source partitions are observable. DataFusion may otherwise insert
    // a round-robin repartition to the host's core count after a narrow filter.
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions.clamp(1, crate::EXECUTION_CONCURRENCY))
        .with_round_robin_repartition(false);
    with_metadata_optimizer(SessionContext::new_with_config(config).state())
}

fn with_metadata_optimizer(
    state: datafusion::execution::SessionState,
) -> datafusion::execution::SessionState {
    let mut rules: Vec<
        Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
    > = vec![Arc::new(MetadataCrossJoin), Arc::new(CountOrderingDemand)];
    rules.extend(state.physical_optimizers().iter().cloned());
    rules.push(Arc::new(CountOrderingDemand));
    rules.push(Arc::new(EliminateIdentityRanges));
    datafusion::execution::session_state::SessionStateBuilder::new_from_existing(state)
        .with_physical_optimizer_rules(rules)
        .build()
}

pub(crate) async fn physical_globals(globals: &crate::globals::GlobalsRef) -> Result<PhysicalPlan> {
    let target = match globals.node() {
        crate::globals::Node::Aggregate {
            table,
            expression,
            ..
        } => total_aggregate_target(table, expression),
        _ => 1,
    };
    physical_plan(crate::logical_globals::lower(globals)?, target).await
}

pub(crate) async fn sample(
    table: &TableRef,
    limit: usize,
) -> Result<Vec<arrow_array::RecordBatch>> {
    use datafusion_physical_plan::execution_plan::collect;
    let input = ordered(lower(table)?);
    let projected = (0..table.schema().fields().len())
        .map(|i| indexed_column(&input, i))
        .collect::<Vec<_>>();
    let logical = LogicalPlanBuilder::from(input)
        .project(projected)?
        .limit(0, Some(limit))?
        .build()?;
    let physical = physical_plan(logical, max_partitions(table)).await?;
    Ok(collect(
        physical,
        Arc::new(datafusion_execution::TaskContext::default()),
    )
    .await?)
}

pub(crate) async fn count(table: &TableRef) -> Result<usize> {
    use datafusion_physical_plan::execution_plan::collect;
    // COUNT observes cardinality, not output values or order. Remove only the
    // outer cardinality-preserving chain: crossing a filter, head, aggregate,
    // or other row-selection operation could change its semantics (for example
    // a filter on scan ordinals). force_count uses full execute() separately.
    let mut input = table;
    while let TableNode::OrderBy { child, .. }
    | TableNode::KeyBy { child, .. }
    | TableNode::MapRows { child, .. }
    | TableNode::MapGlobals { child } = input.node()
    {
        input = child;
    }
    let aggregate = datafusion_functions_aggregate::count::count_udaf()
        .call(vec![datafusion_expr::lit(1i32)])
        .alias("count");
    let logical = LogicalPlanBuilder::from(lower_for_expressions(input, std::iter::empty())?)
        .aggregate(Vec::<Expr>::new(), vec![aggregate])?
        .build()?;
    let physical = physical_plan(logical, max_partitions(table)).await?;
    let batches = collect(
        physical,
        Arc::new(datafusion_execution::TaskContext::default()),
    )
    .await?;
    let value = batches
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
        })
        .and_then(|column| {
            if column.len() == 1 {
                Some(column.value(0))
            } else {
                None
            }
        })
        .ok_or_else(|| {
            TableError::Invalid("count aggregate did not return one int64 value".into())
        })?;
    Ok(value as usize)
}

struct VailExtensionPlanner {
    diagnostic: bool,
}
#[async_trait]
impl ExtensionPlanner for VailExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[PhysicalPlan],
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> datafusion_common::Result<Option<PhysicalPlan>> {
        if self.diagnostic {
            let label = if node
                .as_any()
                .is::<crate::aggregate_singleton::AggregateSingleton>()
            {
                Some("shared aggregate singleton remains unbound".to_owned())
            } else if node
                .as_any()
                .is::<crate::conditional_globals::ConditionalNode>()
            {
                Some("conditional singleton selection remains unbound".to_owned())
            } else if node
                .as_any()
                .is::<crate::logical_aggregate_init::InitNode>()
            {
                Some("aggregate singleton parameters remain unbound".to_owned())
            } else if node
                .as_any()
                .is::<crate::logical_scan::ScanInitializationNode>()
            {
                Some("scan singleton parameters remain unbound".to_owned())
            } else {
                node.as_any().downcast_ref::<VailNode>().map(|node| {
                    format!(
                        "{} semantic operation; action preparation remains unbound",
                        node_name(node.table.node())
                    )
                })
            };
            if let Some(label) = label {
                return Ok(Some(crate::diagnostics::unbound(
                    label,
                    Arc::new(node.schema().as_arrow().clone()),
                    physical_inputs.to_vec(),
                )));
            }
        }
        if let Some(node) = node.as_any().downcast_ref::<crate::bgen_select::SelectNode>() {
            return Ok(Some(node.physical(physical_inputs)?));
        }
        if let Some(node) = node.as_any().downcast_ref::<crate::group_ranges::GroupRanges>() {
            return Ok(Some(node.physical(physical_inputs[0].clone())?));
        }
        if let Some(node) = node.as_any().downcast_ref::<SplitPartitionsNode>() {
            let input = physical_inputs.first().ok_or_else(|| {
                DataFusionError::Plan("partition split needs one input".into())
            })?;
            if input.properties().output_partitioning().partition_count() >= node.partitions {
                return Ok(Some(input.clone()));
            }
            return Ok(Some(Arc::new(
                crate::partitioned::SplitPartitionsExec::new(
                    input.clone(),
                    node.partitions,
                )?,
            )));
        }
        if let Some(node) = node.as_any().downcast_ref::<RestorePartitionsNode>() {
            let input = &physical_inputs[0];
            // Keep the restoration boundary even for one logical partition:
            // upstream window/join optimization can add physical partitions.
            let index = input.schema().index_of(&node.partition_name)?;
            let key: Arc<dyn datafusion_physical_expr::PhysicalExpr> = Arc::new(
                datafusion_physical_expr::expressions::Column::new(&node.partition_name, index),
            );
            let splits = (1..node.partitions)
                .map(|partition| SplitPoint::new(vec![ScalarValue::UInt64(Some(partition as u64))]))
                .collect();
            let ranges = RangePartitioning::try_new(
                LexOrdering::new(vec![PhysicalSortExpr::new(
                    key,
                    arrow_schema::SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                )])
                .expect("one partition sort key"),
                splits,
            )?;
            return Ok(Some(Arc::new(
                crate::partitioned::ProtectedRangeRepartitionExec::new(input.clone(), ranges)?,
            )));
        }
        if let Some(node) = node.as_any().downcast_ref::<crate::streaming_prev::Previous>() {
            return Ok(Some(node.physical(physical_inputs[0].clone())?));
        }
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::scan_ordinal::SourceOrdinal>()
        {
            return Ok(Some(node.physical(physical_inputs[0].clone())?));
        }
        if let Some(node) = node.as_any().downcast_ref::<OrderedNode>() {
            let input = &physical_inputs[0];
            if node.freeze {
                let input = if node.independent {
                    crate::partitioned::reset_plan_states(input.clone())?
                } else {
                    input.clone()
                };
                let input = DefaultPhysicalPlanner::default().optimize_physical_plan(
                    input.clone(),
                    session,
                    |_, _| {},
                )?;
                return Ok(Some(crate::rng_context::consumer(input)));
            }
            let parts = crate::partitioned::partition_views(
                input.clone(),
                input.properties().output_partitioning().partition_count(),
            );
            return Ok(Some(Arc::new(
                crate::partitioned::OrderedConcatExec::tagged(
                    parts,
                    input.schema(),
                    input.schema().fields().len() - 1,
                ),
            )));
        }
        let Some(node) = node.as_any().downcast_ref::<VailNode>() else {
            return Ok(None);
        };
        // Interval reads prune source partitions and expose the survivors as
        // a dense output partition sequence. Apply the user's partition IDs
        // to that output sequence, retaining the corresponding source plans.
        if let TableNode::FilterPartitions { child, parts, keep } = &node.table.node()
            && let TableNode::IntervalRead {
                child: source,
                intervals,
                point_type,
                filter: true,
            } = child.node()
        {
            let prepared = crate::interval_read::prepare_output_partitions(
                source, intervals, point_type, parts, *keep,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            return Ok(Some(prepared.physical_plan()));
        }
        // Prepared children become private inputs of deferred partition plans.
        // Finalize them before that boundary: the outer optimizer cannot reach
        // them to enforce window ordering/distribution or select a join mode.
        let physical_inputs = physical_inputs
            .iter()
            .zip(logical_inputs)
            .map(|(input, logical)| {
                // Each semantic operand is an independent consumer, even
                // when DF shares an upstream physical DAG between operands.
                let input = crate::partitioned::reset_plan_states(input.clone())?;
                use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
                let mut needs_finalization = false;
                logical.apply(|plan| {
                    // These extensions already own executable private plans.
                    // Do not re-optimize them as if they were unfinished DF
                    // operators (notably unions with replayed shared sources).
                    if let LogicalPlan::Extension(extension) = plan
                        && extension.node.as_any().is::<VailNode>()
                    {
                        return Ok(TreeNodeRecursion::Jump);
                    }
                    needs_finalization |= matches!(
                        plan,
                        LogicalPlan::Window(_)
                            | LogicalPlan::Aggregate(_)
                            | LogicalPlan::Join(_)
                            | LogicalPlan::Sort(_)
                            | LogicalPlan::Limit(_)
                            | LogicalPlan::Repartition(_)
                    );
                    Ok(TreeNodeRecursion::Continue)
                })?;
                if needs_finalization {
                    DefaultPhysicalPlanner::default().optimize_physical_plan(
                        input.clone(),
                        session,
                        |_, _| {},
                    )
                } else {
                    Ok(input.clone())
                }
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let children = children(&node.table);
        // Equal immutable source identity in one zip action has one prepared
        // owner, including its bound globals; no digest proves equality.
        let share_zip = matches!(node.table.node(), TableNode::MultiWayZipJoin { .. });
        let mut prepared_zip = std::collections::HashMap::<usize, TableRef>::new();
        let prepared = children
            .iter()
            .enumerate()
            .map(|(i, child)| {
                let physical = &physical_inputs[node.row_input_indices[i]];
                let id = Arc::as_ptr(child) as usize;
                if share_zip {
                    if let Some(prepared) = prepared_zip.get(&id) {
                        return prepared.clone();
                    }
                }
                let n = physical
                    .properties()
                    .output_partitioning()
                    .partition_count();
                let partition_counts = child.partition_rows().filter(|counts| counts.len() == n);
                let count = partition_counts.as_ref().map(|counts| counts.iter().sum());
                let prepared = TableIR::prepared(child, physical.clone(), count, n)
                    .replacing_node(TableNode::Prepared {
                        rows: physical.clone(),
                        count,
                        n_partitions: n,
                        partition_counts,
                    })
                    .replacing_globals(
                        node.child_global_inputs[i]
                            .map(|index| {
                                crate::globals::GlobalsIR::prepared(physical_inputs[index].clone())
                            })
                            .unwrap_or_else(|| child.globals().clone()),
                    );
                if share_zip {
                    prepared_zip.insert(id, prepared.clone());
                }
                prepared
            })
            .collect::<Vec<_>>();
        let mut table = replace_children(&node.table, prepared)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if let Some(index) = node.output_global_input {
            table = table.replacing_globals(crate::globals::GlobalsIR::prepared(
                physical_inputs[index].clone(),
            ));
        }
        let binding_start = node.row_input_count
            + node
                .child_global_inputs
                .iter()
                .filter(|index| index.is_some())
                .count()
            + usize::from(node.output_global_input.is_some());
        if !node.binding_names.is_empty() {
            let bindings = node
                .binding_names
                .iter()
                .zip(&physical_inputs[binding_start..])
                .map(|(name, physical)| {
                    (
                        name.clone(),
                        crate::globals::GlobalsIR::prepared(physical.clone()),
                    )
                })
                .collect();
            table = replace_bindings(&table, bindings)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        if let TableNode::Expression {
            expression,
            bindings,
            n_partitions,
        } = table.node()
        {
            let input = crate::plan::expression_array(expression, bindings)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            return Ok(Some(crate::partitioned::expression_source_plan(
                input,
                table.clone(),
                *n_partitions,
            )));
        }
        let prepared = crate::partitioned::prepare_operation(table)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(prepared.physical_plan()))
    }
}

fn replace_bindings(
    table: &TableRef,
    bindings: crate::globals::GlobalBindings,
) -> Result<TableRef> {
    let mut node = table.node().clone();
    let target = match &mut node {
        TableNode::Expression { bindings, .. }
        | TableNode::MapRows { bindings, .. }
        | TableNode::Filter { bindings, .. }
        | TableNode::MapPartitions { bindings, .. }
        | TableNode::Lookup { bindings, .. }
        | TableNode::Aggregate { bindings, .. } => bindings,
        _ => {
            return Err(TableError::Invalid(
                "unexpected logical extension global bindings".into(),
            ));
        }
    };
    *target = bindings;
    Ok(table.replacing_node(node))
}

fn replace_children(table: &TableRef, mut children: Vec<TableRef>) -> Result<TableRef> {
    if children.is_empty() {
        return Ok(table.clone());
    }
    let mut node = table.node().clone();
    match &mut node {
        TableNode::Join { left, right, .. } | TableNode::Lookup { left, right, .. } => {
            *left = children.remove(0);
            *right = children.remove(0);
        }
        TableNode::MultiWayZipJoin { tables, .. } => *tables = children,
        TableNode::Union { children: slots } => *slots = children,
        TableNode::IntervalRead { child, .. }
        | TableNode::Repartition { child, .. }
        | TableNode::FilterPartitions { child, .. }
        | TableNode::GroupWithinPartitions { child, .. }
        | TableNode::MatrixEntriesByKeyRuns { child, .. }
        | TableNode::MapPartitions { child, .. }
        | TableNode::MapGlobals { child }
        | TableNode::MapRows { child, .. }
        | TableNode::Filter { child, .. }
        | TableNode::Explode { child, .. }
        | TableNode::KeyBy { child, .. }
        | TableNode::OrderBy { child, .. }
        | TableNode::Head { child, .. }
        | TableNode::Tail { child, .. }
        | TableNode::Distinct { child }
        | TableNode::Aggregate { child, .. } => *child = children.remove(0),
        _ => {
            return Err(TableError::Invalid(
                "unexpected logical extension children".into(),
            ));
        }
    }
    Ok(table.replacing_node(node))
}
