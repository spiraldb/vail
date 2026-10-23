//! Consumer demand is distinct from available ordering.
//!
//! An unordered action can discard ordering-only work, but cannot cross an
//! operator that uses sequence to choose rows or compute values. Unknown custom
//! operators are barriers, even when they omit `required_input_ordering`.
use crate::plan::PhysicalPlan;
use datafusion_common::Result;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_plan::{
    ChildrenPropertiesMode, ReplaceChildrenOptions,
    coalesce_batches::CoalesceBatchesExec,
    coalesce_partitions::CoalescePartitionsExec,
    projection::ProjectionExec,
    sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
};
use std::{collections::HashMap, sync::Arc};

/// Weaken only output ordering. Preserve the number of partitions, evaluation
/// of upstream expressions, and shared execution DAGs. In particular a global
/// sort is replaced by a coalesce, not by its multi-partition input.
pub(crate) fn unordered(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    visit(plan, &mut HashMap::new())
}

fn visit(plan: PhysicalPlan, memo: &mut HashMap<usize, PhysicalPlan>) -> Result<PhysicalPlan> {
    let key = Arc::as_ptr(&plan) as *const () as usize;
    if let Some(result) = memo.get(&key) {
        return Ok(result.clone());
    }
    let result = if let Some(sort) = plan.downcast_ref::<SortExec>() {
        // Computed sort keys can fail or carry volatile state. Do not eliminate
        // their evaluation. Fetch is a row-selection operation, not ordering.
        if sort.fetch().is_none() && sort.expr().iter().all(|s| s.expr.is::<Column>()) {
            let input = visit(sort.input().clone(), memo)?;
            if sort.preserve_partitioning() {
                input
            } else {
                Arc::new(CoalescePartitionsExec::new(input))
            }
        } else {
            plan.clone()
        }
    } else if let Some(merge) = plan.downcast_ref::<SortPreservingMergeExec>() {
        if merge.fetch().is_none() && merge.expr().iter().all(|s| s.expr.is::<Column>()) {
            Arc::new(CoalescePartitionsExec::new(visit(
                merge.input().clone(),
                memo,
            )?))
        } else {
            plan.clone()
        }
    } else {
        // Deliberately excludes filters, limits, scans, windows, deduplication,
        // joins, RNG lineage, range exchanges and opaque custom operators.
        // Even a deterministic projection may encode sequence-sensitive Vail
        // kernels, so initially permit only infallible column/literal access.
        let transparent = plan
            .downcast_ref::<ProjectionExec>()
            .is_some_and(|projection| {
                projection
                    .expr()
                    .iter()
                    .all(|e| e.expr.is::<Column>() || e.expr.is::<Literal>())
            })
            || plan.is::<CoalesceBatchesExec>()
            || plan.is::<CoalescePartitionsExec>()
            || crate::partitioned::ordering_transparent_container(&plan)
            || crate::action_parameters::repeatable_node(&plan)
            || plan.is::<crate::rng_context::ConsumerScopeExec>();
        if transparent {
            let children = plan
                .children()
                .into_iter()
                .map(|child| visit(child.clone(), memo))
                .collect::<Result<Vec<_>>>()?;
            plan.clone().replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?
        } else {
            plan.clone()
        }
    };
    memo.insert(key, result.clone());
    Ok(result)
}

/// Built-in count over field/literal arguments is a certified unordered consumer.
/// Grouped/streaming aggregates, filtered aggregates and arbitrary UDAFs keep
/// their original contracts until an explicit semantic proof is supplied.
pub(crate) fn count_has_unordered_input(plan: &PhysicalPlan) -> bool {
    let Some(aggregate) =
        plan.downcast_ref::<datafusion_physical_plan::aggregates::AggregateExec>()
    else {
        return false;
    };
    aggregate.group_expr().is_true_no_grouping()
        && !aggregate.aggr_expr().is_empty()
        && aggregate.filter_expr().iter().all(Option::is_none)
        && plan.required_input_ordering().iter().all(Option::is_none)
        && aggregate.aggr_expr().iter().all(|expression| {
            expression
                .fun()
                .inner()
                .is::<datafusion_functions_aggregate::count::Count>()
                && expression.order_bys().is_empty()
                && expression
                    .expressions()
                    .iter()
                    .all(|expr| expr.is::<Column>() || expr.is::<Literal>())
        })
}

