//! Consumer demand is distinct from available ordering.
//!
//! An unordered action can discard ordering-only work, but cannot cross an
//! operator that uses sequence to choose rows or compute values. Unknown custom
//! operators are barriers, even when they omit `required_input_ordering`.
use crate::plan::PhysicalPlan;
use datafusion_common::Result;
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{Column, IsNotNullExpr, IsNullExpr, Literal},
};
use datafusion_physical_plan::{
    ChildrenPropertiesMode, ReplaceChildrenOptions,
    coalesce_batches::CoalesceBatchesExec,
    coalesce_partitions::CoalescePartitionsExec,
    projection::ProjectionExec,
    scalar_subquery::ScalarSubqueryExec,
    sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Weaken only output ordering. Preserve the number of partitions, evaluation
/// of upstream expressions, and shared execution DAGs. In particular a global
/// sort is replaced by a coalesce, not by its multi-partition input.
pub(crate) fn unordered(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    unordered_with_subqueries(plan, false)
}

pub(crate) fn unordered_with_subqueries(
    plan: PhysicalPlan,
    scalar_subqueries: bool,
) -> Result<PhysicalPlan> {
    visit(plan, &mut HashMap::new(), scalar_subqueries)
}

/// Replacing a scalar-subquery coordinator creates a fresh one-time future.
/// Do not fork that future away from another consumer of its result slots.
/// Counts are graph edges, not unrelated external Arc owners.
pub(crate) fn can_rewrite_scalar_subqueries(root: &PhysicalPlan) -> bool {
    let mut edges = HashMap::<usize, usize>::new();
    let mut expanded = HashSet::new();
    let mut contains = HashMap::<usize, bool>::new();
    let mut pending = vec![(root.clone(), false)];
    while let Some((plan, finish)) = pending.pop() {
        let key = Arc::as_ptr(&plan) as *const () as usize;
        if finish {
            let has = plan.is::<ScalarSubqueryExec>()
                || plan
                    .children()
                    .into_iter()
                    .any(|child| contains[&(Arc::as_ptr(child) as *const () as usize)]);
            contains.insert(key, has);
            continue;
        }
        *edges.entry(key).or_default() += 1;
        if !expanded.insert(key) {
            if !contains.contains_key(&key) {
                return false;
            }
            continue;
        }
        if let Some(subquery) = plan.downcast_ref::<ScalarSubqueryExec>() {
            if subquery
                .subqueries()
                .iter()
                .any(|link| subquery.results().get(link.index).is_some())
            {
                return false;
            }
        }
        pending.push((plan.clone(), true));
        pending.extend(
            plan.children()
                .into_iter()
                .rev()
                .map(|child| (child.clone(), false)),
        );
    }
    !edges
        .into_iter()
        .any(|(key, count)| count > 1 && contains[&key])
}

// Null markers of a column cannot fail or observe execution context.
// Arbitrary computed expressions retain their evaluation barrier.
fn infallible_order_key(expression: &Arc<dyn PhysicalExpr>) -> bool {
    expression.is::<Column>()
        || expression
            .downcast_ref::<IsNullExpr>()
            .is_some_and(|value| value.arg().is::<Column>())
        || expression
            .downcast_ref::<IsNotNullExpr>()
            .is_some_and(|value| value.arg().is::<Column>())
}

fn visit(
    plan: PhysicalPlan,
    memo: &mut HashMap<usize, PhysicalPlan>,
    scalar_subqueries: bool,
) -> Result<PhysicalPlan> {
    let key = Arc::as_ptr(&plan) as *const () as usize;
    if let Some(result) = memo.get(&key) {
        return Ok(result.clone());
    }
    let order_key = |expression: &Arc<dyn PhysicalExpr>| {
        if scalar_subqueries {
            infallible_order_key(expression)
        } else {
            expression.is::<Column>()
        }
    };
    let result = if let Some(sort) = plan.downcast_ref::<SortExec>() {
        // Computed sort keys can fail or carry volatile state. Do not eliminate
        // their evaluation. Fetch is a row-selection operation, not ordering.
        if sort.fetch().is_none() && sort.expr().iter().all(|s| order_key(&s.expr)) {
            let input = visit(sort.input().clone(), memo, scalar_subqueries)?;
            if sort.preserve_partitioning() {
                input
            } else {
                Arc::new(CoalescePartitionsExec::new(input))
            }
        } else {
            plan.clone()
        }
    } else if let Some(merge) = plan.downcast_ref::<SortPreservingMergeExec>() {
        if merge.fetch().is_none() && merge.expr().iter().all(|s| order_key(&s.expr)) {
            Arc::new(CoalescePartitionsExec::new(visit(
                merge.input().clone(),
                memo,
                scalar_subqueries,
            )?))
        } else {
            plan.clone()
        }
    } else if scalar_subqueries && plan.is::<ScalarSubqueryExec>() {
        // Only the main data input has unordered demand. Keep singleton plans,
        // result indices, and their shared container exactly as planned.
        let mut children = plan.children().into_iter().cloned().collect::<Vec<_>>();
        children[0] = visit(children[0].clone(), memo, scalar_subqueries)?;
        plan.clone().replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?
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
                .map(|child| visit(child.clone(), memo, scalar_subqueries))
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
