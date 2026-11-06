//! Action-local permission to read ahead in a completely consumed row plan.
//!
//! This certifies the existing planned output. It does not request additional
//! source fields or move a value expression across a predicate.
use crate::{TableNode, TableRef, globals::Node as GlobalsNode};
use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::Expr;

/// Explicit experiments remain available to actions without a full-demand proof.
/// Call once when creating their lowering context, never while executing batches.
pub(crate) fn explicit_prefetch() -> bool {
    std::env::var_os("VAIL_DISABLE_BOUNDED_PREFETCH").is_none()
        && std::env::var_os("VAIL_EXPERIMENT_BOUNDED_PREFETCH").is_some()
}

/// Only force_count supplies the complete-consumption part of this certificate.
pub(crate) fn for_force_count(table: &TableRef) -> bool {
    let disabled = std::env::var_os("VAIL_DISABLE_BOUNDED_PREFETCH").is_some();
    let explicit = std::env::var_os("VAIL_EXPERIMENT_BOUNDED_PREFETCH").is_some();
    let checked = !disabled && !explicit;
    let safe = checked && safe_planned_values(table);
    let admitted = !disabled && (explicit || safe);
    if std::env::var_os("VAIL_TRACE_BOUNDED_PREFETCH").is_some() {
        eprintln!(
            "VAIL_AUTO_PREFETCH_ADMISSION admitted={admitted} explicit={explicit} certificate_checked={checked} certificate_safe={safe}"
        );
    }
    admitted
}

fn safe_expression(
    expression: &Expr,
    seen: &mut HashSet<usize>,
    contracts: &mut HashMap<String, bool>,
) -> bool {
    let mut pending = vec![expression];
    while let Some(node) = pending.pop() {
        if !seen.insert(node as *const Expr as usize) {
            continue;
        }
        if !node.can_prefetch_node(contracts) {
            return false;
        }
        node.visit_children(&mut |child| pending.push(child));
    }
    true
}

fn safe_planned_values(root: &TableRef) -> bool {
    let mut tables = vec![root.clone()];
    let mut globals = Vec::new();
    let mut seen_tables = HashSet::new();
    let mut seen_globals = HashSet::new();
    let mut seen_expressions = HashSet::new();
    let mut contracts = HashMap::new();
    loop {
        if let Some(table) = tables.pop() {
            if !seen_tables.insert(Arc::as_ptr(&table) as usize) {
                continue;
            }
            match table.node() {
                TableNode::Read { .. }
                | TableNode::Range { .. }
                | TableNode::Parallelize { .. }
                | TableNode::MapGlobals { .. }
                | TableNode::Explode { .. }
                | TableNode::KeyBy { .. }
                | TableNode::OrderBy { .. }
                | TableNode::Union { .. }
                | TableNode::OrderedUnion { .. } => {}
                TableNode::Prepared { rows, .. } => {
                    // cache() produces this exact immutable source, without a
                    // retained producer. Opaque prepared operators stay barriers.
                    let Some(source) = rows.as_ref().downcast_ref::<DataSourceExec>() else {
                        return false;
                    };
                    if source.data_source().fetch().is_some()
                        || source
                            .data_source()
                            .downcast_ref::<MemorySourceConfig>()
                            .is_none()
                    {
                        return false;
                    }
                }
                TableNode::Expression { expression, .. } => {
                    if !safe_expression(expression, &mut seen_expressions, &mut contracts) {
                        return false;
                    }
                }
                TableNode::MapRows { fields, .. } => {
                    if fields.iter().any(|(_, value)| {
                        !safe_expression(value, &mut seen_expressions, &mut contracts)
                    }) {
                        return false;
                    }
                }
                TableNode::Filter { predicate, .. } => {
                    if !safe_expression(predicate, &mut seen_expressions, &mut contracts) {
                        return false;
                    }
                }
                // Includes prefix/partition selection, stateful scans,
                // partition callbacks, joins and unknown source kinds.
                _ => return false,
            }
            globals.push(table.globals().clone());
            if let Some(bindings) = crate::logical::bindings_for_node(table.node()) {
                globals.extend(bindings.values().cloned());
            }
            tables.extend(crate::logical::children(&table));
        } else if let Some(owner) = globals.pop() {
            if !seen_globals.insert(Arc::as_ptr(&owner) as usize) {
                continue;
            }
            match owner.node() {
                GlobalsNode::Empty | GlobalsNode::ArrowLiteral(_) | GlobalsNode::Read(_) => {}
                GlobalsNode::Map {
                    child,
                    fields,
                    bindings,
                } => {
                    if fields.iter().any(|(_, value)| {
                        !safe_expression(value, &mut seen_expressions, &mut contracts)
                    }) {
                        return false;
                    }
                    globals.push(child.clone());
                    globals.extend(bindings.values().cloned());
                }
                // Conditional/aggregate/prepared owners do not establish
                // complete demand merely because the outer action counts.
                _ => return false,
            }
        } else {
            return true;
        }
    }
}
