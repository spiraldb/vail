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
    use datafusion_common::tree_node::{Transformed, TreeNode};
    use datafusion_physical_plan::filter::FilterExec;
    Ok(plan
        .transform_up(|plan| {
            let Some(filter) = plan.as_ref().downcast_ref::<FilterExec>() else {
                return Ok(Transformed::no(plan));
            };
            let batch_size = filter.batch_size().min(rows.max(1));
            if batch_size == filter.batch_size() {
                return Ok(Transformed::no(plan));
            }
            let capped: crate::plan::PhysicalPlan = Arc::new(filter.with_batch_size(batch_size)?);
            Ok(Transformed::yes(capped))
        })?
        .data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TableIR;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use serde_json::json;

    #[derive(Debug)]
    struct CallerExtension;

    fn literal_i32(value: i32) -> Expr {
        Expr::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: json!(value),
        }
    }

    fn repeat(count: Expr) -> Expr {
        Expr::NestedFunction {
            function: "array_repeat".into(),
            args: vec![literal_i32(0), count],
            dtype: Type::Array {
                array: Box::new(Type::Primitive(PrimitiveType::Int32)),
            },
        }
    }

    fn int64_from_i32(value: Expr) -> Expr {
        Expr::Cast {
            dtype: Type::Primitive(PrimitiveType::Int64),
            value: Arc::new(value),
        }
    }

    fn mapped(expression: Expr) -> TableRef {
        TableIR::range(4, 1)
            .unwrap()
            .map_rows(vec![
                ("idx".into(), Expr::Column { name: "idx".into() }),
                ("items".into(), expression),
            ])
            .unwrap()
    }

    #[test]
    fn advises_only_for_known_positive_primitive_repeats() {
        assert_eq!(
            for_table(
                &mapped(repeat(int64_from_i32(literal_i32(50_000_000)))),
                8192
            ),
            Some(1)
        );
        assert_eq!(
            for_table(&mapped(repeat(int64_from_i32(literal_i32(1_000_000)))), 8192),
            Some(16)
        );

        assert_eq!(
            for_table(&mapped(repeat(int64_from_i32(literal_i32(-1)))), 8192),
            None
        );
        assert_eq!(
            for_table(&mapped(repeat(int64_from_i32(literal_i32(0)))), 8192),
            None
        );
        assert_eq!(
            for_table(
                &mapped(repeat(int64_from_i32(Expr::Column { name: "idx".into() }))),
                8192
            ),
            None
        );
        assert_eq!(
            for_table(&mapped(repeat(int64_from_i32(literal_i32(50_000_000)))), 1),
            Some(1)
        );
    }

    #[test]
    fn unknown_repeat_does_not_erase_known_wide_repeat_advice() {
        let known = repeat(int64_from_i32(literal_i32(50_000_000)));
        let bad_unselected = repeat(int64_from_i32(literal_i32(-1)));
        let expression = Expr::IfElse {
            condition: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Bool),
                value: json!(false),
            }),
            consequent: Arc::new(bad_unselected),
            alternate: Arc::new(known),
        };
        assert_eq!(for_table(&mapped(expression), 8192), Some(1));
        assert_eq!(
            for_table(
                &mapped(repeat(int64_from_i32(Expr::Column { name: "idx".into() }))),
                8192
            ),
            None
        );
    }

    #[test]
    fn dropped_projection_and_literal_unselected_repeat_do_not_cap() {
        let source = TableIR::range(4, 1)
            .unwrap()
            .map_rows(vec![
                ("idx".into(), Expr::Column { name: "idx".into() }),
                (
                    "items".into(),
                    repeat(int64_from_i32(literal_i32(50_000_000))),
                ),
            ])
            .unwrap();
        let selected = source
            .map_rows(vec![("idx".into(), Expr::Column { name: "idx".into() })])
            .unwrap();
        assert_eq!(for_table(&selected, 8192), None);

        let expression = Expr::IfElse {
            condition: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Bool),
                value: json!(true),
            }),
            consequent: Arc::new(repeat(int64_from_i32(literal_i32(1)))),
            alternate: Arc::new(repeat(int64_from_i32(literal_i32(50_000_000)))),
        };
        assert_eq!(for_table(&mapped(expression), 8192), None);
    }

    #[test]
    fn advised_context_preserves_caller_identity_runtime_and_extensions() {
        let udf = Arc::new(datafusion_expr::expr_fn::create_udf(
            "caller_identity",
            vec![arrow_schema::DataType::Int32],
            arrow_schema::DataType::Int32,
            datafusion_expr::Volatility::Immutable,
            Arc::new(|values: &[datafusion_expr::ColumnarValue]| Ok(values[0].clone())),
        ));
        let scalar_functions = HashMap::from([("caller_identity".into(), udf.clone())]);
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new()
            .with_batch_size(4096)
            .with_extension(Arc::new(CallerExtension));
        let context = TaskContext::new(
            Some("caller-task".into()),
            "caller-session".into(),
            config,
            scalar_functions,
            Default::default(),
            Default::default(),
            Default::default(),
            runtime.clone(),
        );

        let adjusted_config = with_limit(context.session_config().clone(), 1);
        let adjusted = task_context_with_config(&context, adjusted_config);
        assert_eq!(adjusted.task_id().as_deref(), Some("caller-task"));
        assert_eq!(adjusted.session_id(), "caller-session");
        assert!(Arc::ptr_eq(&runtime, &adjusted.runtime_env()));
        assert_eq!(adjusted.session_config().batch_size(), 1);
        assert!(
            adjusted
                .session_config()
                .get_extension::<CallerExtension>()
                .is_some()
        );
        assert!(
            adjusted
                .session_config()
                .get_extension::<WideBatchLimit>()
                .is_some()
        );
        assert_eq!(
            adjusted.scalar_functions().len(),
            context.scalar_functions().len()
        );
        assert!(Arc::ptr_eq(
            adjusted.scalar_functions().get("caller_identity").unwrap(),
            &udf
        ));
        assert_eq!(
            adjusted.higher_order_functions().len(),
            context.higher_order_functions().len()
        );
        assert_eq!(
            adjusted.aggregate_functions().len(),
            context.aggregate_functions().len()
        );
        assert_eq!(
            adjusted.window_functions().len(),
            context.window_functions().len()
        );

        let already_capped = with_limit(SessionConfig::new(), 32);
        let still_capped = with_limit(already_capped, 64);
        assert_eq!(
            still_capped
                .get_extension::<WideBatchLimit>()
                .unwrap()
                .rows,
            32
        );
    }
}
