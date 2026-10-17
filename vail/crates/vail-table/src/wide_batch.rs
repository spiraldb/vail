//! Advisory source batch cap for closed primitive `array_repeat` annotations.
//!
//! This is deliberately narrow: it estimates a known materialized list only
//! to avoid broadcasting that list across a large input batch. It is not a
//! query-memory guarantee; a single output row may exceed the target. The
//! estimate covers demanded primitive `array_repeat` expressions in row maps.
//! Range sources and Vail-built filters honor the advice explicitly; other
//! source implementations must honor the session batch size themselves.

use crate::{TableNode, TableRef};
use datafusion_common::ScalarValue;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::{Expr, PrimitiveType, Type};

const TARGET_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct WideBatchLimit {
    pub(crate) rows: usize,
}

/// Return an opt-in limit only when a closed primitive repeat is statically
/// known to exceed the configured batch's advisory broadcast target.
pub(crate) fn for_table(table: &TableRef, configured_rows: usize) -> Option<usize> {
    let mut seen_tables: HashMap<usize, HashSet<String>> = HashMap::new();
    let mut bytes_per_row = 0u64;
    let mut found = false;
    let root_columns: HashSet<String> = table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut pending = vec![(table.clone(), root_columns)];
    while let Some((table, demanded)) = pending.pop() {
        let identity = Arc::as_ptr(&table) as usize;
        let processed = seen_tables.entry(identity).or_default();
        let demanded: HashSet<_> = demanded.difference(processed).cloned().collect();
        if demanded.is_empty() {
            continue;
        }
        processed.extend(demanded.iter().cloned());
        let mut child_demand = demanded.clone();
        if let TableNode::MapRows { child, fields, .. } = table.node() {
            child_demand.clear();
            for (name, expression) in fields {
                // The expression's output is live only if the action still
                // demands that field. This avoids capping a wide annotation
                // that a following projection has removed.
                if !demanded.contains(name) {
                    continue;
                }
                visit_reachable(expression, &mut |node| {
                    if let Expr::Column { name } = node {
                        child_demand.insert(name.clone());
                    }
                    let Expr::NestedFunction {
                        function,
                        args,
                        dtype,
                    } = node
                    else {
                        return;
                    };
                    if function != "array_repeat" {
                        return;
                    }
                    match repeat_bytes(args, dtype) {
                        Some(bytes) => {
                            bytes_per_row = bytes_per_row.saturating_add(bytes);
                            found = true;
                        }
                        // Unknown or invalid closed repeats contribute no
                        // estimate; they are never evaluated during planning.
                        None => {}
                    }
                });
            }
            pending.push((child.clone(), child_demand));
            continue;
        }
        let children = crate::logical::children(&table);
        if let TableNode::Filter { predicate, .. } = table.node() {
            visit_reachable(predicate, &mut |node| {
                if let Expr::Column { name } = node {
                    child_demand.insert(name.clone());
                }
            });
        }
        for child in children {
            let child_columns: HashSet<_> = child
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let selected = if matches!(table.node(), TableNode::Filter { .. }) {
                child_demand.intersection(&child_columns).cloned().collect()
            } else {
                // Other node types may consume fields that are not present in
                // their output schema (for example ordering, aggregation, or
                // callbacks). Keep their estimate conservative.
                child_columns
            };
            pending.push((child, selected));
        }
    }
    let configured_rows = configured_rows.max(1);
    let configured_bytes = bytes_per_row.saturating_mul(configured_rows as u64);
    if !found || configured_bytes <= TARGET_BYTES {
        return None;
    }
    let advised_rows = usize::try_from(TARGET_BYTES / bytes_per_row)
        .unwrap_or(usize::MAX)
        .max(1);
    Some(advised_rows.min(configured_rows))
}

fn visit_reachable(expression: &Expr, f: &mut impl FnMut(&Expr)) {
    fn walk(expression: &Expr, seen: &mut HashSet<usize>, f: &mut impl FnMut(&Expr)) {
        if !seen.insert(expression as *const Expr as usize) {
            return;
        }
        f(expression);
        // A closed array length produces a scalar per row and does not
        // broadcast the nested array into the output batches.
        if let Expr::ArrayLength { value } = expression
            && is_closed_repeat(value)
        {
            return;
        }
        if let Expr::IfElse {
            condition,
            consequent,
            alternate,
        } = expression
        {
            let selected = match condition.as_ref() {
                Expr::Literal {
                    dtype: Type::Primitive(PrimitiveType::Bool),
                    value,
                } => value.as_bool(),
                Expr::RuntimeLiteral {
                    value: ScalarValue::Boolean(value),
                } => *value,
                _ => None,
            };
            if let Some(selected) = selected {
                walk(condition, seen, f);
                walk(if selected { consequent } else { alternate }, seen, f);
                return;
            }
        }
        expression.visit_children(&mut |child| walk(child, seen, f));
    }
    walk(expression, &mut HashSet::new(), f);
}

fn is_closed_repeat(expression: &Expr) -> bool {
    let Expr::NestedFunction { function, args, .. } = expression else {
        return false;
    };
    function == "array_repeat"
        && args.len() == 2
        && known_positive_count(&args[1]).is_some()
        && matches!(args[0], Expr::Literal { .. } | Expr::RuntimeLiteral { .. })
}

fn repeat_bytes(args: &[Expr], dtype: &Type) -> Option<u64> {
    let [_, count] = args else {
        return None;
    };
    let Type::Array { array } = dtype else {
        return None;
    };
    let width = match array.as_ref() {
        Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Float32) => 4,
        Type::Primitive(PrimitiveType::Int64 | PrimitiveType::Float64) => 8,
        // Boolean storage is bit-packed. Charging one byte per element is a
        // conservative estimate without inspecting or allocating any values.
        Type::Primitive(PrimitiveType::Bool) => 1,
        _ => return None,
    };
    let elements = known_positive_count(count)?;
    elements
        .checked_mul(width)
        .and_then(|bytes| bytes.checked_add(elements.div_ceil(8)))
        .and_then(|bytes| bytes.checked_add(8)) // two List32 offsets
}

fn known_positive_count(expression: &Expr) -> Option<u64> {
    let value = match expression {
        Expr::Literal { dtype, value }
            if matches!(
                dtype,
                Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64)
            ) =>
        {
            value.as_i64()?
        }
        Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some(value)),
        } => i64::from(*value),
        Expr::RuntimeLiteral {
            value: ScalarValue::Int64(Some(value)),
        } => *value,
        Expr::Cast {
            dtype: Type::Primitive(PrimitiveType::Int64),
            value,
        } => match value.as_ref() {
            Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value,
            } => value.as_i64()?,
            Expr::RuntimeLiteral {
                value: ScalarValue::Int32(Some(value)),
            } => i64::from(*value),
            _ => return None,
        },
        _ => return None,
    };
    u64::try_from(value).ok().filter(|&value| value > 0)
}

/// Adjust only the typed opt-in extension; unrelated caller extensions remain.
pub(crate) fn with_limit(config: SessionConfig, rows: usize) -> SessionConfig {
    let existing_limit = config
        .get_extension::<WideBatchLimit>()
        .map_or(usize::MAX, |limit| limit.rows);
    let rows = config.batch_size().min(rows.max(1)).min(existing_limit);
    config
        .with_batch_size(rows)
        .with_extension(Arc::new(WideBatchLimit { rows }))
}

pub(crate) fn task_context_with_config(
    context: &TaskContext,
    config: SessionConfig,
) -> Arc<TaskContext> {
    Arc::new(TaskContext::new(
        context.task_id(),
        context.session_id(),
        config,
        context.scalar_functions().clone(),
        context.higher_order_functions().clone(),
        context.aggregate_functions().clone(),
        context.window_functions().clone(),
        context.runtime_env(),
    ))
}

/// DF55's FilterExec owns an internal coalescer and does not consult the task
/// batch size. Cap it only in actions carrying the explicit wide-list advice.
pub(crate) fn cap_filter_batches(
    plan: crate::plan::PhysicalPlan,
    rows: usize,
) -> crate::Result<crate::plan::PhysicalPlan> {
    use datafusion_physical_plan::filter::FilterExec;

    fn rewrite(
        plan: crate::plan::PhysicalPlan,
        rows: usize,
        memo: &mut HashMap<usize, crate::plan::PhysicalPlan>,
    ) -> datafusion_common::Result<crate::plan::PhysicalPlan> {
        let identity = Arc::as_ptr(&plan) as *const () as usize;
        if let Some(rewritten) = memo.get(&identity) {
            return Ok(rewritten.clone());
        }

        let children = plan.children();
        let mut rewritten_children = Vec::with_capacity(children.len());
        let mut changed = false;
        for child in children {
            let rewritten = rewrite(child.clone(), rows, memo)?;
            changed |= !Arc::ptr_eq(child, &rewritten);
            rewritten_children.push(rewritten);
        }
        let mut rewritten = if changed {
            plan.clone().with_new_children(rewritten_children)?
        } else {
            plan
        };
        if let Some(filter) = rewritten.as_ref().downcast_ref::<FilterExec>() {
            let batch_size = filter.batch_size().min(rows.max(1));
            if batch_size < filter.batch_size() {
                rewritten = Arc::new(filter.with_batch_size(batch_size)?);
            }
        }
        memo.insert(identity, rewritten.clone());
        Ok(rewritten)
    }

    rewrite(plan, rows, &mut HashMap::new()).map_err(Into::into)
}
