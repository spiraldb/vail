//! Parallel scans over logical execution partitions. Local row prefixes and
//! partition summaries use DataFusion windows/aggregates; accumulator states
//! flow between partitions through the shared aggregate UDAFs.
use crate::{Result, TableError, TableRef};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{
    Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunctionDefinition, expr::WindowFunction, expr_fn::ExprFunctionExt,
};
use std::{collections::HashSet, sync::Arc};
use vail_execution::expressions::Expr as VExpr;

#[derive(Clone)]
struct Leaf {
    expr: VExpr,
    filter: Option<VExpr>,
    local: String,
    total: String,
    carry: String,
    output: String,
    descriptor: Arc<vail_execution::scan_state::ScanState>,
    argument: VExpr,
    finish: VExpr,
}

fn marked_scan(expr: &VExpr) -> bool {
    matches!(expr, VExpr::Aggregate { options, .. }
        if options.get("__vail_scan").and_then(serde_json::Value::as_bool) == Some(true))
}

fn collect_scans(
    expr: &VExpr,
    filter: Option<VExpr>,
    leaves: &mut Vec<(VExpr, Option<VExpr>)>,
) -> std::result::Result<(), vail_execution::expressions::ExprError> {
    if let VExpr::AggFilter { predicate, body } = expr {
        let combined = match filter {
            Some(previous) => VExpr::Binary {
                operator: vail_execution::expressions::BinaryOperator::And,
                left: Box::new(previous),
                right: predicate.clone(),
            },
            None => (**predicate).clone(),
        };
        return collect_scans(body, Some(combined), leaves);
    }
    if marked_scan(expr) {
        let leaf = (expr.clone(), filter);
        if !leaves.contains(&leaf) {
            leaves.push(leaf);
        }
        return Ok(());
    }
    let mut nested_error = None;
    expr.try_map_children(|child| {
        if let Err(error) = collect_scans(child, filter.clone(), leaves) {
            nested_error = Some(error);
        }
        Ok::<_, vail_execution::expressions::ExprError>(child.clone())
    })?;
    if let Some(error) = nested_error {
        return Err(error);
    }
    Ok(())
}

fn unique(used: &mut HashSet<String>, base: &str) -> String {
    let mut name = base.to_owned();
    while !used.insert(name.clone()) {
        name.push('_');
    }
    name
}

fn column(plan: &LogicalPlan, index: usize) -> Expr {
    Expr::Column(Column::from(plan.schema().qualified_field(index)))
}

fn frame() -> WindowFrame {
    let mut frame = WindowFrame::new(Some(true));
    frame.units = WindowFrameUnits::Rows;
    frame.end_bound = WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1)));
    frame
}

fn guard_argument(
    argument: Expr,
    filter: Option<&VExpr>,
    schema: &datafusion_common::DFSchema,
    globals: &arrow_schema::Schema,
    bindings: &std::collections::BTreeMap<String, usize>,
) -> Result<Expr> {
    let Some(filter) = filter else {
        return Ok(argument);
    };
    let predicate = vail_execution::logical_expr::lower_logical_with_bindings(
        filter, schema, globals, bindings,
    )?;
    let null = ScalarValue::try_from(argument.get_type(schema)?)?;
    Ok(datafusion_expr::when(predicate, argument).otherwise(datafusion_expr::lit(null))?)
}

/// Lower a row projection containing scan aggregate leaves. The only global
/// window is over one row per non-empty input partition; row-level windows are
/// partitioned by the original execution partition id.
pub(crate) fn lower(
    table: &TableRef,
    child: &TableRef,
    input: LogicalPlan,
    fields: &[(String, VExpr)],
    bindings: &std::collections::BTreeMap<String, usize>,
) -> Result<Option<LogicalPlan>> {
    let mut nodes = Vec::<(VExpr, Option<VExpr>)>::new();
    for (_, expr) in fields {
        collect_scans(expr, None, &mut nodes)?;
    }
    if nodes.is_empty() {
        return Ok(None);
    }
    if nodes
        .iter()
        .any(|(node, _)| matches!(node, VExpr::Aggregate { init, .. } if !init.is_empty()))
    {
        return Ok(None); // The existing serial lowering reports the unsupported initializer.
    }
    let mut used = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .chain(fields.iter().map(|(name, _)| name.clone()))
        .collect::<HashSet<_>>();
    let part_index = child.globals().schema().fields().len() + child.schema().fields().len();
    let part = column(&input, part_index);
    let part_name = input.schema().field(part_index).name().clone();

    let mut leaves = Vec::with_capacity(nodes.len());
    for (i, (node, filter)) in nodes.into_iter().enumerate() {
        let VExpr::Aggregate { function, value, options, .. } = &node else {
            return Err(TableError::Invalid("unsupported scan scope".into()));
        };
        let argument = value.as_deref().cloned().unwrap_or(VExpr::Literal {
            dtype: vail_execution::expressions::Type::Primitive(vail_execution::expressions::PrimitiveType::Int64),
            value: serde_json::json!(1),
        });
        let input_type = argument.dtype_with_globals(input.schema().as_arrow(), child.globals().schema())?.to_arrow()?;
        let mut options = options.clone();
        if let Some(options) = options.as_object_mut() { options.remove("__vail_scan"); }
        let reducer = crate::aggregation::Reducer { name: "scan".into(), function: function.clone(), value: value.as_deref().cloned(), options };
        let udf = crate::logical_aggregation::factory(&reducer, Some(&input_type))?;
        let descriptor = Arc::new(vail_execution::scan_state::ScanState::new(udf, vec![input_type])?);
        let output = unique(&mut used, &format!("__scan_value_{i}"));
        let finish = VExpr::Column { name: output.clone() };
        leaves.push(Leaf {
            expr: node,
            filter,
            local: unique(&mut used, &format!("__scan_local_{i}")),
            total: unique(&mut used, &format!("__scan_total_{i}")),
            carry: unique(&mut used, &format!("__scan_carry_{i}")),
            output,
            descriptor,
            argument,
            finish,
        });
    }

    let mut local_ordinal = WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        vec![],
    );
    local_ordinal.params.partition_by = vec![part.clone()];
    local_ordinal.params.window_frame = WindowFrame::new(Some(true));
    let ordinal_name = unique(&mut used, "__scan_local_ordinal");
    let mut local = LogicalPlanBuilder::from(input.clone())
        .window(vec![
            Expr::WindowFunction(Box::new(local_ordinal)).alias(&ordinal_name),
        ])?
        .build()?;
    let mut local_windows = Vec::with_capacity(leaves.len());
    for leaf in &leaves {
        let arg = vail_execution::logical_expr::lower_logical_with_bindings(
            &leaf.argument,
            input.schema(),
            child.globals().schema(),
            bindings,
        )?;
        let arg = guard_argument(
            arg,
            leaf.filter.as_ref(),
            input.schema(),
            child.globals().schema(),
            bindings,
        )?;
        let mut window = WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(leaf.descriptor.state_udaf()),
            vec![arg],
        );
        window.params.partition_by = vec![part.clone()];
        window.params.order_by = vec![datafusion_expr::col(&ordinal_name).sort(true, true)];
        window.params.window_frame = frame();
        if let Some(predicate) = &leaf.filter {
            if predicate.dtype_with_globals(input.schema().as_arrow(), child.globals().schema())?
                != vail_execution::expressions::Type::Primitive(
                    vail_execution::expressions::PrimitiveType::Bool,
                )
            {
                return Err(TableError::Invalid(
                    "scan.filter predicate must be Boolean".into(),
                ));
            }
            window.params.filter = Some(Box::new(
                vail_execution::logical_expr::lower_logical_with_bindings(
                    predicate,
                    input.schema(),
                    child.globals().schema(),
                    bindings,
                )?,
            ));
        }
        local_windows.push(Expr::WindowFunction(Box::new(window)).alias(&leaf.local));
    }
    local = LogicalPlanBuilder::from(local)
        .window(local_windows)?
        .build()?;

    // One complete accumulator state per execution partition. DataFusion's
    // aggregate nodes own all reducer behavior, including missing values.
    let mut summary_aggregates = Vec::with_capacity(leaves.len());
    for leaf in &leaves {
        let arg = vail_execution::logical_expr::lower_logical_with_bindings(
            &leaf.argument,
            input.schema(),
            child.globals().schema(),
            bindings,
        )?;
        let arg = guard_argument(
            arg,
            leaf.filter.as_ref(),
            input.schema(),
            child.globals().schema(),
            bindings,
        )?;
        let mut aggregate = leaf
            .descriptor
            .state_udaf()
            .call(vec![arg])
            .order_by(vec![datafusion_expr::col(&ordinal_name).sort(true, true)]);
        if let Some(predicate) = &leaf.filter {
            aggregate =
                aggregate.filter(vail_execution::logical_expr::lower_logical_with_bindings(
                    predicate,
                    input.schema(),
                    child.globals().schema(),
                    bindings,
                )?);
        }
        summary_aggregates.push(aggregate.build()?.alias(&leaf.total));
    }
    let summary_part = unique(&mut used, "__scan_partition_id");
    let summary = LogicalPlanBuilder::from(local.clone())
        .aggregate(vec![part.alias(&summary_part)], summary_aggregates)?
        .build()?;
    let mut carry_windows = Vec::with_capacity(leaves.len());
    for leaf in &leaves {
        let mut window = WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(leaf.descriptor.merge_udaf()),
            vec![datafusion_expr::col(&leaf.total)],
        );
        window.params.order_by = vec![datafusion_expr::col(&summary_part).sort(true, true)];
        window.params.window_frame = frame();
        carry_windows.push(Expr::WindowFunction(Box::new(window)).alias(&leaf.carry));
    }
    let summary = LogicalPlanBuilder::from(summary)
        .window(carry_windows)?
        .build()?;
    let right_key = unique(&mut used, "__scan_right_partition_id");
    let mut right_projection = vec![datafusion_expr::col(&summary_part).alias(&right_key)];
    right_projection.extend(leaves.iter().map(|leaf| datafusion_expr::col(&leaf.carry)));
    let right = LogicalPlanBuilder::from(summary)
        .project(right_projection)?
        .build()?;
    let joined = LogicalPlanBuilder::from(local)
        .join(
            right,
            JoinType::Left,
            (vec![part_name.clone()], vec![right_key]),
            None,
        )?
        .build()?;

    let mut completed = joined
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    for leaf in &leaves {
        completed.push(
            leaf.descriptor
                .finish_udf()
                .call(vec![
                    datafusion_expr::col(&leaf.carry),
                    datafusion_expr::col(&leaf.local),
                ])
                .alias(&leaf.output),
        );
    }
    let completed = LogicalPlanBuilder::from(joined)
        .project(completed)?
        .build()?;
    let completed = LogicalPlanBuilder::from(completed)
        .sort(vec![
            datafusion_expr::col(&part_name).sort(true, true),
            datafusion_expr::col(&ordinal_name).sort(true, true),
        ])?
        .build()?;
    let completed = crate::logical::restore_partitions(
        completed,
        part_name,
        table.known_n_partitions().unwrap_or(1).max(1),
    );
    let rewritten = fields
        .iter()
        .map(|(name, expr)| Ok((name.clone(), replace_scans(expr, &leaves, None)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut output = Vec::with_capacity(fields.len() + 1);
    for (name, expr) in &rewritten {
        output.push(
            vail_execution::logical_expr::lower_logical_with_bindings(
                expr,
                completed.schema(),
                child.globals().schema(),
                bindings,
            )?
            .alias(name),
        );
    }
    output.push(column(&completed, part_index));
    let output = LogicalPlanBuilder::from(completed)
        .project(output)?
        .build()?;
    Ok(Some(output))
}

fn replace_scans(expr: &VExpr, leaves: &[Leaf], filter: Option<VExpr>) -> Result<VExpr> {
    if let Some(leaf) = leaves
        .iter()
        .find(|leaf| leaf.expr == *expr && leaf.filter == filter)
    {
        return Ok(leaf.finish.clone());
    }

    if let VExpr::AggFilter { predicate, body } = expr {
        let combined = match filter {
            Some(previous) => VExpr::Binary {
                operator: vail_execution::expressions::BinaryOperator::And,
                left: Box::new(previous),
                right: predicate.clone(),
            },
            None => (**predicate).clone(),
        };
        return replace_scans(body, leaves, Some(combined));
    }
    if marked_scan(expr) {
        let leaf = leaves
            .iter()
            .find(|leaf| leaf.expr == *expr && leaf.filter == filter)
            .ok_or_else(|| TableError::Invalid("missing scan accumulator state".into()))?;
        return Ok(leaf.finish.clone());
    }
    if matches!(
        expr,
        VExpr::AggFilter { .. }
            | VExpr::AggExplode { .. }
            | VExpr::AggGroupBy { .. }
            | VExpr::AggArray { .. }
            | VExpr::AggregateFold { .. }
    ) {
        return Err(TableError::Invalid(
            "scan.filter, scan.explode, and nested scan combinators are unsupported".into(),
        ));
    }
    expr.try_map_children(&mut |child: &VExpr| replace_scans(child, leaves, filter.clone()))
        .map_err(Into::into)
}
