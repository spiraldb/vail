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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_execution::TaskContext;
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::{ExecutionPlanProperties, collect, limit::GlobalLimitExec};

    fn source() -> PhysicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = |values| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
        };
        MemorySourceConfig::try_new_exec(
            &[vec![batch(vec![3, 1])], vec![batch(vec![4, 2])]],
            schema,
            None,
        )
        .unwrap()
    }
    fn sorted(input: PhysicalPlan, local: bool, fetch: Option<usize>) -> PhysicalPlan {
        // A global sort requires a singleton input, as enforced by the planner.
        let input = if local {
            input
        } else {
            Arc::new(CoalescePartitionsExec::new(input))
        };
        Arc::new(
            SortExec::new(
                LexOrdering::new(vec![PhysicalSortExpr::new(
                    Arc::new(Column::new("x", 0)),
                    SortOptions::default(),
                )])
                .unwrap(),
                input,
            )
            .with_preserve_partitioning(local)
            .with_fetch(fetch),
        )
    }
    async fn values(plan: PhysicalPlan) -> Vec<i32> {
        collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap()
            .into_iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }
    #[tokio::test]
    async fn unordered_global_sort_retains_all_partitions_and_multiplicity() {
        let plan = unordered(sorted(source(), false, None)).unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 1);
        assert!(!plan.is::<SortExec>());
        let mut result = values(plan).await;
        result.sort_unstable();
        assert_eq!(result, [1, 2, 3, 4]);
    }
    #[tokio::test]
    async fn unordered_local_sort_retains_partition_count() {
        let plan = unordered(sorted(source(), true, None)).unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 2);
        let mut result = values(plan).await;
        result.sort_unstable();
        assert_eq!(result, [1, 2, 3, 4]);
    }
    #[tokio::test]
    async fn head_and_topk_retain_selection_order() {
        let sorted = sorted(source(), false, None);
        let limited: PhysicalPlan = Arc::new(GlobalLimitExec::new(sorted, 0, Some(2)));
        assert_eq!(values(unordered(limited).unwrap()).await, [1, 2]);
        assert_eq!(
            values(unordered(super::tests::sorted(source(), false, Some(2))).unwrap()).await,
            [1, 2]
        );
    }
    #[test]
    fn count_is_unordered_but_collection_is_not() {
        use datafusion_physical_expr::aggregate::AggregateExprBuilder;
        use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
        let input = sorted(source(), false, None);
        let aggregate = |udaf| -> PhysicalPlan {
            let expression = AggregateExprBuilder::new(udaf, vec![Arc::new(Column::new("x", 0))])
                .schema(input.schema())
                .alias("answer")
                .build()
                .unwrap();
            Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Single,
                    PhysicalGroupBy::new_single(vec![]),
                    vec![Arc::new(expression)],
                    vec![None],
                    input.clone(),
                    input.schema(),
                )
                .unwrap(),
            )
        };
        assert!(count_has_unordered_input(&aggregate(
            datafusion_functions_aggregate::count::count_udaf()
        )));
        assert!(!count_has_unordered_input(&aggregate(
            datafusion_functions_aggregate::array_agg::array_agg_udaf()
        )));
    }
}
