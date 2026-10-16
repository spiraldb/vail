//! Parallel scans over logical execution partitions. Local row prefixes and
//! partition summaries use DataFusion windows/aggregates; accumulator states
//! flow between partitions through the shared aggregate UDAFs.
use crate::{Result, TableError, TableRef};
use arrow_schema::DataType;
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
    validation: Option<String>,
}

fn complex_scope(expr: &VExpr) -> bool {
    let mut found = false;
    expr.visit_unique(&mut |node| {
        found |= matches!(
            node,
            VExpr::AggGroupBy { .. } | VExpr::AggArray { .. } | VExpr::AggFilter { .. }
        )
    });
    found
}

fn normalize_explodes(expr: &VExpr) -> Result<VExpr> {
    if let VExpr::AggExplode {
        value,
        parameter,
        body,
    } = expr
    {
        // Keep selected-only nested filter scopes intact. Boolean flattening
        // changes missing-filter short circuit and repeats predicate work.
        if complex_scope(body) {
            return Ok(expr.clone());
        }
        let body = normalize_explodes(body)?;
        return explode_leaves(&body, value, parameter, None);
    }
    expr.try_map_children(&mut |child: &VExpr| normalize_explodes(child))
        .map_err(Into::into)
}

fn explode_leaves(
    expr: &VExpr,
    array: &VExpr,
    parameter: &str,
    filter: Option<VExpr>,
) -> Result<VExpr> {
    if let VExpr::AggFilter { predicate, body } = expr {
        let filter = match filter {
            Some(previous) => VExpr::Binary {
                operator: vail_execution::expressions::BinaryOperator::And,
                left: Arc::new(previous),
                right: predicate.clone(),
            },
            None => (**predicate).clone(),
        };
        return explode_leaves(body, array, parameter, Some(filter));
    }
    if let VExpr::Aggregate {
        function,
        value,
        options,
        result_type,
        init,
    } = expr
    {
        if marked_scan(expr) {
            let source = match filter {
                Some(predicate) => VExpr::ArrayFilter {
                    value: Arc::new(array.clone()),
                    parameter: parameter.into(),
                    predicate: Arc::new(predicate),
                },
                None => array.clone(),
            };
            let element = value.as_deref().cloned().unwrap_or(VExpr::Literal {
                dtype: vail_execution::expressions::Type::Primitive(
                    vail_execution::expressions::PrimitiveType::Int64,
                ),
                value: serde_json::json!(1),
            });
            let mapped = VExpr::ArrayMap {
                value: Arc::new(source),
                parameter: parameter.into(),
                body: Arc::new(element),
            };
            let argument = if options
                .get("__vail_exploded")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                VExpr::ArrayFlatten {
                    value: Arc::new(mapped),
                }
            } else {
                mapped
            };
            let mut options = options.clone();
            options["__vail_exploded"] = serde_json::Value::Bool(true);
            return Ok(VExpr::Aggregate {
                function: function.clone(),
                value: Some(Arc::new(argument)),
                options,
                result_type: result_type.clone(),
                init: init.clone(),
            });
        }
    }
    expr.try_map_children(&mut |child: &VExpr| {
        explode_leaves(child, array, parameter, filter.clone())
    })
    .map_err(Into::into)
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
    if complex_scope(expr)
        && expr.contains_scan()
        && matches!(
            expr,
            VExpr::AggGroupBy { .. }
                | VExpr::AggArray { .. }
                | VExpr::AggExplode { .. }
                | VExpr::AggFilter { .. }
        )
    {
        let leaf = (expr.clone(), filter);
        if !leaves.contains(&leaf) {
            leaves.push(leaf);
        }
        return Ok(());
    }
    if let VExpr::AggFilter { predicate, body } = expr {
        let combined = match filter {
            Some(previous) => VExpr::Binary {
                operator: vail_execution::expressions::BinaryOperator::And,
                left: Arc::new(previous),
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

fn native_prefix_kind(expr: &VExpr) -> Option<(&'static str, Option<&VExpr>)> {
    let VExpr::Aggregate {
        function,
        value,
        options,
        init,
        ..
    } = expr
    else {
        return None;
    };
    if !init.is_empty()
        || options
            .get("__vail_exploded")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        || options.as_object().is_some_and(|options| {
            options
                .keys()
                .any(|key| !matches!(key.as_str(), "__vail_scan" | "__vail_result_type"))
        })
    {
        return None;
    }
    match function.as_str() {
        "count" if value.is_none() => Some(("count", None)),
        "sum" => Some(("sum", Some(value.as_deref()?))),
        _ => None,
    }
}

fn replace_native_scans(
    expr: &VExpr,
    scans: &[(VExpr, String)],
) -> std::result::Result<VExpr, vail_execution::expressions::ExprError> {
    if let Some((_, name)) = scans.iter().find(|(scan, _)| scan == expr) {
        return Ok(VExpr::Column { name: name.clone() });
    }
    expr.try_map_children(&mut |child: &VExpr| replace_native_scans(child, scans))
}

/// Fast path for plain numeric sums and row counts. It keeps values as native
/// DataFusion window aggregates; the generic ScanState path remains for custom,
/// filtered, and nested scan scopes.
fn lower_native_prefix(
    table: &TableRef,
    child: &TableRef,
    input: LogicalPlan,
    fields: &[(String, VExpr)],
    nodes: &[(VExpr, Option<VExpr>)],
    bindings: &std::collections::BTreeMap<String, usize>,
    part_index: usize,
) -> Result<Option<LogicalPlan>> {
    let kinds = nodes
        .iter()
        .map(|(node, filter)| {
            if filter.is_some() {
                return None;
            }
            let (kind, value) = native_prefix_kind(node)?;
            if let Some(value) = value {
                let dtype = value
                    .dtype_with_globals(input.schema().as_arrow(), child.globals().schema())
                    .ok()?
                    .to_arrow()
                    .ok()?;
                // DataFusion widens SUM(Int32) to Int64 and SUM(Float32) to
                // Float64, matching Hail's scan.sum result types. Include the
                // narrow input types used by range_table(idx), while leaving
                // unsupported numeric layouts on the generic scan path.
                if kind == "sum"
                    && !matches!(
                        dtype,
                        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
                    )
                {
                    return None;
                }
            }
            Some((kind, value))
        })
        .collect::<Option<Vec<_>>>();
    let Some(kinds) = kinds else {
        return Ok(None);
    };
    if kinds.is_empty() {
        return Ok(None);
    }

    let mut used = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .chain(fields.iter().map(|(name, _)| name.clone()))
        .collect::<HashSet<_>>();
    let part = column(&input, part_index);
    let part_name = input.schema().field(part_index).name().clone();
    let ordinal_name = unique(&mut used, "__scan_native_ordinal");
    let mut local_ordinal = WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(
            datafusion_functions_window::row_number::row_number_udwf(),
        ),
        vec![],
    );
    local_ordinal.params.partition_by = vec![part.clone()];
    local_ordinal.params.window_frame = WindowFrame::new(Some(true));
    let mut local = LogicalPlanBuilder::from(input.clone())
        .window(vec![
            Expr::WindowFunction(Box::new(local_ordinal)).alias(&ordinal_name),
        ])?
        .build()?;

    let mut local_windows = Vec::with_capacity(nodes.len());
    let mut local_names = Vec::with_capacity(nodes.len());
    let mut total_aggregates = Vec::with_capacity(nodes.len());
    let mut total_names = Vec::with_capacity(nodes.len());
    for (i, (kind, value)) in kinds.iter().enumerate() {
        // A row count is the sum of one per input row; the same native sum
        // kernel handles its exclusive local prefix and partition total.
        let udf = match *kind {
            "sum" | "count" => datafusion_functions_aggregate::sum::sum_udaf(),
            _ => unreachable!(),
        };
        let mut arg = match value {
            Some(value) => vail_execution::logical_expr::lower_logical_with_bindings(
                value,
                input.schema(),
                child.globals().schema(),
                bindings,
            )?,
            None => datafusion_expr::lit(1_i64),
        };
        if *kind == "sum" {
            let value = value.expect("sum prefix has an argument");
            let dtype = value
                .dtype_with_globals(input.schema().as_arrow(), child.globals().schema())?
                .to_arrow()?;
            let widened = match dtype {
                DataType::Int32 => Some(DataType::Int64),
                DataType::Float32 => Some(DataType::Float64),
                _ => None,
            };
            if let Some(dtype) = widened {
                // DataFusion's SUM kernel accepts Int64 and Float64, and
                // Hail widens narrower numeric sums to those same result types.
                arg = Expr::Cast(datafusion_expr::expr::Cast::new(Box::new(arg), dtype));
            }
        }
        let local_name = unique(&mut used, &format!("__scan_native_local_{i}"));
        let mut local_window = WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(udf.clone()),
            vec![arg.clone()],
        );
        local_window.params.partition_by = vec![part.clone()];
        local_window.params.order_by = vec![datafusion_expr::col(&ordinal_name).sort(true, true)];
        local_window.params.window_frame = frame();
        local_windows.push(Expr::WindowFunction(Box::new(local_window)).alias(&local_name));
        local_names.push(local_name);

        let total_name = unique(&mut used, &format!("__scan_native_total_{i}"));
        total_aggregates.push(udf.call(vec![arg]).alias(&total_name));
        total_names.push(total_name);
    }
    local = LogicalPlanBuilder::from(local)
        .window(local_windows)?
        .build()?;

    let summary_part = unique(&mut used, "__scan_native_partition");
    let summary = LogicalPlanBuilder::from(input)
        .aggregate(vec![part.alias(&summary_part)], total_aggregates)?
        .build()?;
    let mut carry_windows = Vec::with_capacity(total_names.len());
    let mut carry_names = Vec::with_capacity(total_names.len());
    for (i, (kind, _)) in kinds.iter().enumerate() {
        // Partition totals are already sums or counts. Add the totals to form
        // the exclusive prefix across partitions.
        let udf = match *kind {
            "sum" | "count" => datafusion_functions_aggregate::sum::sum_udaf(),
            _ => unreachable!(),
        };
        let carry_name = unique(&mut used, &format!("__scan_native_carry_{i}"));
        let mut carry = WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(udf),
            vec![datafusion_expr::col(&total_names[i])],
        );
        carry.params.order_by = vec![datafusion_expr::col(&summary_part).sort(true, true)];
        carry.params.window_frame = frame();
        carry_windows.push(Expr::WindowFunction(Box::new(carry)).alias(&carry_name));
        carry_names.push(carry_name);
    }
    let summary = LogicalPlanBuilder::from(summary)
        .window(carry_windows)?
        .build()?;
    let right_key = unique(&mut used, "__scan_native_right_partition");
    let mut right_projection = vec![datafusion_expr::col(&summary_part).alias(&right_key)];
    right_projection.extend(carry_names.iter().map(datafusion_expr::col));
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
    let mut scans = Vec::with_capacity(nodes.len());
    for (i, ((node, _), _)) in nodes.iter().zip(kinds.iter()).enumerate() {
        let total_type = joined
            .schema()
            .field_with_name(None, &local_names[i])?
            .data_type()
            .clone();
        let zero = match total_type {
            DataType::Int64 => ScalarValue::Int64(Some(0)),
            DataType::Float64 => ScalarValue::Float64(Some(0.0)),
            other => {
                return Err(TableError::Invalid(format!(
                    "native scan prefix does not support output type {other:?}"
                )));
            }
        };
        let local_value = datafusion_expr::col(&local_names[i]);
        let local = datafusion_expr::when(
            local_value.clone().is_null(),
            datafusion_expr::lit(zero.clone()),
        )
        .otherwise(local_value)?;
        let carry_value = datafusion_expr::col(&carry_names[i]);
        let carry =
            datafusion_expr::when(carry_value.clone().is_null(), datafusion_expr::lit(zero))
                .otherwise(carry_value)?;
        let output_name = unique(&mut used, &format!("__scan_native_value_{i}"));
        completed.push((carry + local).alias(&output_name));
        scans.push((node.clone(), output_name));
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
        .map(|(name, expr)| Ok((name.clone(), replace_native_scans(expr, &scans)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut output = rewritten
        .iter()
        .map(|(name, expr)| {
            Ok(vail_execution::logical_expr::lower_logical_with_bindings(
                expr,
                completed.schema(),
                child.globals().schema(),
                bindings,
            )?
            .alias(name))
        })
        .collect::<Result<Vec<_>>>()?;
    output.push(column(&completed, part_index));
    Ok(Some(
        LogicalPlanBuilder::from(completed)
            .project(output)?
            .build()?,
    ))
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
    let normalized = fields
        .iter()
        .map(|(name, expr)| {
            Ok((
                name.clone(),
                if complex_scope(expr) {
                    expr.clone()
                } else {
                    normalize_explodes(expr)?
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let fields = normalized.as_slice();
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
    let part_index = child.globals().schema().fields().len() + child.schema().fields().len();
    if let Some(plan) = lower_native_prefix(
        table,
        child,
        input.clone(),
        fields,
        &nodes,
        bindings,
        part_index,
    )? {
        return Ok(Some(plan));
    }
    let mut used = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .chain(fields.iter().map(|(name, _)| name.clone()))
        .collect::<HashSet<_>>();
    let part = column(&input, part_index);
    let part_name = input.schema().field(part_index).name().clone();

    let mut leaves = Vec::with_capacity(nodes.len());
    for (i, (node, filter)) in nodes.into_iter().enumerate() {
        let built =
            crate::nested_scan::build(&node, input.schema(), child.globals().schema(), bindings)?;
        let output = unique(&mut used, &format!("__scan_value_{i}"));
        let finish = built.finish(VExpr::Column {
            name: output.clone(),
        })?;
        let descriptor = Arc::new(vail_execution::scan_state::ScanState::new(
            built.udf,
            vec![built.input],
        )?);
        let mut array_scope = false;
        node.visit_unique(&mut |expr| array_scope |= matches!(expr, VExpr::AggArray { .. }));
        let validation = array_scope.then(|| unique(&mut used, &format!("__scan_validated_{i}")));
        leaves.push(Leaf {
            expr: node,
            filter,
            local: unique(&mut used, &format!("__scan_local_{i}")),
            total: unique(&mut used, &format!("__scan_total_{i}")),
            carry: unique(&mut used, &format!("__scan_carry_{i}")),
            output,
            descriptor,
            argument: built.argument,
            finish,
            validation,
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
        if let Some(name) = &leaf.validation {
            let mut inclusive = window.clone();
            inclusive.params.window_frame.end_bound = WindowFrameBound::CurrentRow;
            carry_windows.push(Expr::WindowFunction(Box::new(inclusive)).alias(name));
        }
        carry_windows.push(Expr::WindowFunction(Box::new(window)).alias(&leaf.carry));
    }
    let summary = LogicalPlanBuilder::from(summary)
        .window(carry_windows)?
        .build()?;
    let right_key = unique(&mut used, "__scan_right_partition_id");
    let mut right_projection = vec![datafusion_expr::col(&summary_part).alias(&right_key)];
    right_projection.extend(leaves.iter().map(|leaf| datafusion_expr::col(&leaf.carry)));
    right_projection.extend(
        leaves
            .iter()
            .filter_map(|leaf| leaf.validation.as_ref())
            .map(datafusion_expr::col),
    );
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
        let mut arguments = vec![
            datafusion_expr::col(&leaf.carry),
            datafusion_expr::col(&leaf.local),
        ];
        let finisher = if let Some(name) = &leaf.validation {
            arguments.push(datafusion_expr::col(name));
            leaf.descriptor.validated_finish_udf()
        } else {
            leaf.descriptor.finish_udf()
        };
        completed.push(finisher.call(arguments).alias(&leaf.output));
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
                left: Arc::new(previous),
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

#[derive(Debug, Clone)]
pub(crate) struct ScanInitializationNode {
    pub(crate) id: u64,
    pub(crate) initializer: LogicalPlan,
    template: LogicalPlan,
    pub(crate) input: LogicalPlan,
    table: TableRef,
    child: TableRef,
    fields: Vec<(String, VExpr)>,
    bindings: std::collections::BTreeMap<String, usize>,
}
impl PartialEq for ScanInitializationNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for ScanInitializationNode {}
impl PartialOrd for ScanInitializationNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl std::hash::Hash for ScanInitializationNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl datafusion_expr::UserDefinedLogicalNodeCore for ScanInitializationNode {
    fn name(&self) -> &str {
        "VailInitializeScan"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.initializer, &self.template]
    }
    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.template.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "VailInitializeScan")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 2 {
            return Err(datafusion_common::DataFusionError::Plan(
                "scan initialization requires initializer and data inputs".into(),
            ));
        }
        let mut result = self.clone();
        result.initializer = inputs[0].clone();
        result.template = inputs[1].clone();
        Ok(result)
    }
}

fn bind_configuration(
    expr: &VExpr,
    bind: &mut impl FnMut(&str, &VExpr) -> Result<ScalarValue>,
) -> Result<VExpr> {
    fn globals(
        expr: &VExpr,
        bind: &mut impl FnMut(&str, &VExpr) -> Result<ScalarValue>,
    ) -> Result<VExpr> {
        if matches!(expr, VExpr::Global { .. } | VExpr::ExternalGlobal { .. }) {
            return Ok(VExpr::RuntimeLiteral {
                value: bind("__global", expr)?,
            });
        }
        expr.try_map_children(&mut |child: &VExpr| globals(child, bind))
            .map_err(Into::into)
    }
    globals(&crate::aggregation::bind_initializers(expr, bind)?, bind)
}

pub(crate) fn initialize(
    table: &TableRef,
    child: &TableRef,
    input: LogicalPlan,
    fields: &[(String, VExpr)],
    bindings: &std::collections::BTreeMap<String, usize>,
) -> Result<Option<LogicalPlan>> {
    let mut parameters = Vec::new();
    let placeholders = fields
        .iter()
        .map(|(name, expr)| {
            let expr = bind_configuration(expr, &mut |name, value| {
                let dtype = value
                    .dtype_with_globals(&arrow_schema::Schema::empty(), child.globals().schema())?
                    .to_arrow()?;
                parameters.push(value.clone());
                if name == "__global" {
                    Ok(ScalarValue::try_from(&dtype)?)
                } else {
                    crate::aggregation::initializer_placeholder(name, &dtype)
                }
            })?;
            Ok((name.clone(), expr))
        })
        .collect::<Result<Vec<_>>>()?;
    if parameters.is_empty() {
        return Ok(None);
    }
    let raw_bindings = match table.node() {
        crate::TableNode::MapRows { bindings, .. } => bindings,
        _ => unreachable!(),
    };
    let (globals, global_indices) = crate::logical_globals::bind_external(
        crate::logical_globals::lower(child.globals())?,
        raw_bindings,
    )?;
    let projection = parameters
        .iter()
        .enumerate()
        .map(|(i, value)| {
            Ok(vail_execution::logical_expr::lower_logical_with_bindings(
                value,
                globals.schema(),
                child.globals().schema(),
                &global_indices,
            )?
            .alias(format!("__scan_init_{i}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let initializer = LogicalPlanBuilder::from(globals)
        .project(projection)?
        .build()?;
    let template = lower(table, child, input.clone(), &placeholders, bindings)?
        .ok_or_else(|| TableError::Invalid("scan initializer has no scan aggregate".into()))?;
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    Ok(Some(LogicalPlan::Extension(datafusion_expr::Extension {
        node: Arc::new(ScanInitializationNode {
            id: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            initializer,
            template,
            input,
            table: table.clone(),
            child: child.clone(),
            fields: fields.to_vec(),
            bindings: bindings.clone(),
        }),
    })))
}

/// Resolve singleton scan configuration in the action's scalar binding phase.
/// The resulting ordinary logical scan is optimized and compiled once.
pub(crate) fn bind_initialized(
    node: &ScanInitializationNode,
    batch: &arrow_array::RecordBatch,
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    let mut index = 0;
    let fields = node
        .fields
        .iter()
        .map(|(name, expr)| {
            let expr = bind_configuration(expr, &mut |_, _| {
                let value = vail_execution::expressions::scalar_at(batch.column(index), 0)?;
                index += 1;
                Ok(value)
            })?;
            Ok((name.clone(), expr))
        })
        .collect::<Result<Vec<_>>>()?;
    if index != batch.num_columns() {
        return Err(TableError::Invalid(
            "scan initializer arity mismatch".into(),
        ));
    }
    lower(&node.table, &node.child, input, &fields, &node.bindings)?
        .ok_or_else(|| TableError::Invalid("initialized scan has no reducer".into()))
}
