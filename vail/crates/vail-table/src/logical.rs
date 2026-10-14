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
        "VailOrderedPartitions"
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
        write!(f, "VailOrderedPartitions")
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
        Ok(Self {
            input: inputs.remove(0),
            ..self.clone()
        })
    }
}

pub(crate) fn ordered(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(OrderedNode {
            id: NEXT_NODE.fetch_add(1, Ordering::Relaxed),
            schema: input.schema().clone(),
            input,
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
        Ok(Self {
            input: inputs.remove(0),
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
        TableNode::MapPartitions { .. } => "MapPartitions",
        TableNode::Read { .. } => "Read",
        TableNode::TextRead { .. } => "TextRead",
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

fn children(table: &TableRef) -> Vec<TableRef> {
    match table.node() {
        TableNode::Read { .. }
        | TableNode::IntervalRead { .. }
        | TableNode::TextRead { .. }
        | TableNode::Range { .. }
        | TableNode::Parallelize { .. }
        | TableNode::Expression { .. }
        | TableNode::Prepared { .. } => vec![],
        TableNode::Join { left, right, .. } | TableNode::Lookup { left, right, .. } => {
            vec![left.clone(), right.clone()]
        }
        TableNode::MultiWayZipJoin { tables, .. } => tables.clone(),
        TableNode::Union { children } => children.clone(),
        TableNode::Repartition { child, .. }
        | TableNode::FilterPartitions { child, .. }
        | TableNode::GroupWithinPartitions { child, .. }
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

fn extension(table: &TableRef) -> Result<LogicalPlan> {
    let row_children = children(table);
    let mut inputs = row_children.iter().map(lower).collect::<Result<Vec<_>>>()?;
    let row_input_count = inputs.len();
    let mut child_global_inputs = Vec::with_capacity(row_children.len());
    for child in &row_children {
        if matches!(child.globals().node(), crate::globals::Node::Empty) {
            child_global_inputs.push(None);
        } else {
            child_global_inputs.push(Some(inputs.len()));
            inputs.push(crate::logical_globals::lower(child.globals())?);
        }
    }
    let output_global_input = if matches!(table.globals().node(), crate::globals::Node::Empty) {
        None
    } else {
        let index = inputs.len();
        inputs.push(crate::logical_globals::lower(table.globals())?);
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
                .map(crate::logical_globals::lower)
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

fn is_scan(expr: &vail_execution::expressions::Expr) -> bool {
    matches!(expr, vail_execution::expressions::Expr::Aggregate { options, .. }
        if options.get("__vail_scan").and_then(serde_json::Value::as_bool) == Some(true))
}

fn table_has_scan(table: &TableRef) -> bool {
    (match table.node() {
        TableNode::MapRows { fields, .. } => fields.iter().any(|(_, expr)| {
            let mut found = false;
            expr.visit(&mut |node| found |= is_scan(node));
            found
        }),
        _ => false,
    }) || children(table).iter().any(table_has_scan)
}

fn lower_scan_tail(table: &TableRef, child: &TableRef, n: usize) -> Result<LogicalPlan> {
    use datafusion_expr::{Operator, WindowFunctionDefinition, expr::WindowFunction};

    let input = ordered(lower(child)?);
    let used = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<HashSet<_>>();
    let mut ordinal_name = "__vail_scan_tail_ordinal".to_owned();
    while used.contains(&ordinal_name) {
        ordinal_name.push('_');
    }
    let mut total_name = "__vail_scan_tail_total".to_owned();
    while used.contains(&total_name) || total_name == ordinal_name {
        total_name.push('_');
    }
    if n == 0 {
        return Ok(LogicalPlanBuilder::from(input).limit(0, Some(0))?.build()?);
    }

    let mut ordinal = WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        vec![],
    );
    ordinal.params.window_frame = WindowFrame::new(Some(true));
    let mut total = WindowFunction::new(
        WindowFunctionDefinition::AggregateUDF(datafusion_functions_aggregate::count::count_udaf()),
        vec![datafusion_expr::lit(1u64)],
    );
    let mut full_rows = WindowFrame::new(Some(true));
    full_rows.end_bound = WindowFrameBound::Following(ScalarValue::UInt64(None));
    total.params.window_frame = full_rows;
    let numbered = LogicalPlanBuilder::from(input)
        .window(vec![
            Expr::WindowFunction(Box::new(ordinal)).alias(&ordinal_name),
            Expr::WindowFunction(Box::new(total)).alias(&total_name),
        ])?
        .build()?;
    // ordinal + n > total includes every row when n exceeds the table size
    // and otherwise selects precisely the final n rows.
    let keep = Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr::new(
        Box::new(datafusion_expr::col(&ordinal_name) + datafusion_expr::lit(n as u64)),
        Operator::Gt,
        Box::new(datafusion_expr::col(&total_name)),
    ));
    let filtered = LogicalPlanBuilder::from(numbered)
        .filter(keep)?
        .sort(vec![datafusion_expr::col(&ordinal_name).sort(true, true)])?
        .build()?;
    let projected = (0..child.schema().fields().len() + 1)
        .map(|i| indexed_column(&filtered, i))
        .collect::<Vec<_>>();
    let output = LogicalPlanBuilder::from(filtered)
        .project(projected)?
        .build()?;
    let _ = table;
    Ok(ordered(output))
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
            let logical_value = match value {
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

pub(crate) fn lower(table: &TableRef) -> Result<LogicalPlan> {
    match table.node() {
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
        TableNode::MapGlobals { child } => lower(child),
        // Vail union concatenates observable execution partitions. Native SQL
        // Union permits branch interleaving and repartitioning; the extension
        // keeps each optimized child partition as an independent output.
        TableNode::Union { .. } => extension(table),
        TableNode::Explode { child, path } if path.len() == 1 => {
            let input = lower(child)?;
            Ok(LogicalPlanBuilder::from(input)
                .unnest_column_with_options(
                    datafusion_common::Column::new_unqualified(&path[0]),
                    datafusion_common::UnnestOptions::new()
                        .with_null_handling(datafusion_common::NullHandling::Drop),
                )?
                .build()?)
        }
        TableNode::Aggregate { child, keys, .. } => {
            let input = lower(child)?;
            match crate::logical_aggregation::lower_logical_grouped(table, input)? {
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
                    Ok(LogicalPlanBuilder::from(plan).project(fields)?.build()?)
                }
                None => extension(table),
            }
        }
        TableNode::MapRows {
            child,
            fields,
            bindings,
        } => {
            let input = crate::logical_globals::bind_globals(lower(child)?, child)?;
            let (input, indices) = crate::logical_globals::bind_external(input, bindings)?;
            if fields.iter().any(|(_, e)| {
                let mut found = false;
                e.visit(&mut |n| found |= is_scan(n));
                found
            }) {
                if let Some(plan) =
                    crate::logical_scan::initialize(table, child, input.clone(), fields, &indices)?
                {
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
                    lower_logical_with_bindings(
                        expr,
                        input.schema(),
                        child.globals().schema(),
                        &indices,
                    )
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
                    let partition_index =
                        child.globals().schema().fields().len() + child.schema().fields().len();
                    projected.push(indexed_column(&input, partition_index));
                    Ok(LogicalPlanBuilder::from(input)
                        .project(projected)?
                        .build()?)
                }
                Err(vail_execution::expressions::ExprError::Invalid(message))
                    if message.contains("needs a Vail logical kernel") =>
                {
                    extension(table)
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
            let input = crate::logical_globals::bind_globals(lower(child)?, child)?;
            let (input, indices) = crate::logical_globals::bind_external(input, bindings)?;
            match lower_logical_with_bindings(
                predicate,
                input.schema(),
                child.globals().schema(),
                &indices,
            ) {
                Ok(predicate) => {
                    let predicate = if *keep { predicate } else { predicate.not() };
                    let filtered = LogicalPlanBuilder::from(input).filter(predicate)?.build()?;
                    let offset = child.globals().schema().fields().len();
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
                    extension(table)
                }
                Err(error) => Err(error.into()),
            }
        }
        TableNode::Tail { child, n } if table_has_scan(child) => lower_scan_tail(table, child, *n),
        _ => extension(table),
    }
}

/// Display the plan DataFusion will optimize and lower for an action.
pub(crate) fn display(table: &TableRef) -> Result<String> {
    let state = session_state(max_partitions(table));
    Ok(state.optimize(&lower(table)?)?.display_indent().to_string())
}

pub(crate) async fn physical(table: &TableRef) -> Result<PhysicalPlan> {
    physical_plan(lower(table)?, max_partitions(table)).await
}

fn max_partitions(table: &TableRef) -> usize {
    children(table)
        .iter()
        .map(max_partitions)
        .chain(table.known_n_partitions())
        .max()
        .unwrap_or(1)
        .max(1)
}

pub(crate) async fn physical_plan(
    logical: LogicalPlan,
    target_partitions: usize,
) -> Result<PhysicalPlan> {
    let planner = DefaultPhysicalPlanner::with_extension_planners(vec![
        Arc::new(VailExtensionPlanner),
        Arc::new(crate::logical_globals::GlobalsExtensionPlanner),
        Arc::new(crate::logical_same::SameExtensionPlanner),
    ]);
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut state = session_state(target_partitions);
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
    Ok(planner.create_physical_plan(&optimized, &state).await?)
}

#[derive(Debug)]
struct MetadataCrossJoin;
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
        use datafusion_common::tree_node::{Transformed, TreeNode};
        Ok(plan
            .transform_up(|plan| {
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
                        return Ok(Transformed::yes(crate::plan::cross_join(
                            join.left().clone(),
                            join.right().clone(),
                        )?));
                    }
                }
                Ok(Transformed::no(plan))
            })?
            .data)
    }
}

fn session_state(target_partitions: usize) -> datafusion::execution::SessionState {
    // Vail source partitions are observable. DataFusion may otherwise insert
    // a round-robin repartition to the host's core count after a narrow filter.
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions.clamp(1, crate::EXECUTION_CONCURRENCY))
        .with_round_robin_repartition(false);
    let state = SessionContext::new_with_config(config).state();
    let mut rules: Vec<
        Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
    > = vec![Arc::new(MetadataCrossJoin)];
    rules.extend(state.physical_optimizers().iter().cloned());
    datafusion::execution::session_state::SessionStateBuilder::new_from_existing(state)
        .with_physical_optimizer_rules(rules)
        .build()
}

pub(crate) async fn physical_globals(globals: &crate::globals::GlobalsRef) -> Result<PhysicalPlan> {
    let target = match globals.node() {
        crate::globals::Node::Aggregate { table, .. } => max_partitions(table),
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
    let logical = LogicalPlanBuilder::from(lower(input)?)
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

struct VailExtensionPlanner;
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
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::logical_scan::ScanInitializationNode>()
        {
            return Ok(Some(
                crate::logical_scan::plan_initialized(node.clone(), physical_inputs[0].clone())
                    .await?,
            ));
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
                    arrow_schema::SortOptions::default(),
                )])
                .expect("one partition sort key"),
                splits,
            )?;
            return Ok(Some(Arc::new(
                crate::partitioned::ProtectedRangeRepartitionExec::new(input.clone(), ranges)?,
            )));
        }
        if node.as_any().downcast_ref::<OrderedNode>().is_some() {
            let input = &physical_inputs[0];
            let parts = (0..input.properties().output_partitioning().partition_count())
                .map(|p| {
                    Arc::new(crate::partitioned::PartitionViewExec::new(input.clone(), p))
                        as PhysicalPlan
                })
                .collect();
            return Ok(Some(Arc::new(crate::partitioned::OrderedConcatExec::new(
                parts,
                input.schema(),
            ))));
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
        let prepared = children
            .iter()
            .zip(&physical_inputs[..node.row_input_count])
            .enumerate()
            .map(|(i, (child, physical))| {
                let n = physical
                    .properties()
                    .output_partitioning()
                    .partition_count();
                let partition_counts = child.partition_rows().filter(|counts| counts.len() == n);
                let count = partition_counts.as_ref().map(|counts| counts.iter().sum());
                TableIR::prepared(child, physical.clone(), count, n)
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
                    )
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
        let prepared = crate::partitioned::prepare_legacy(table)
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
