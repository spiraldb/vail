//! Bounded partition metadata planning; DataFusion performs redistribution.
use crate::RepartitionStrategy;
use crate::plan::{PhysicalPlan, column};
use arrow_array::ArrayRef;
use arrow_schema::SortOptions;
use datafusion_common::{DataFusionError, ScalarValue, SplitPoint};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use futures::TryStreamExt;
use std::sync::Arc;
use vail_storage::fragment_index::make_typed_key_comparators;

pub(crate) fn coalesce_ends(
    counts: &[(usize, usize)],
    n: usize,
    strategy: RepartitionStrategy,
) -> Vec<usize> {
    if counts.is_empty() {
        return vec![];
    }
    if n >= counts.len() {
        return (0..counts.len()).collect();
    }
    if strategy == RepartitionStrategy::NaiveCoalesce {
        let base = counts.len() / n;
        let extra = counts.len() % n;
        let mut end = 0;
        return (0..n)
            .map(|p| {
                end += base + usize::from(p < extra);
                end - 1
            })
            .collect();
    }
    let mut sum = 0usize;
    let cumulative = counts
        .iter()
        .map(|(_, count)| {
            sum += count;
            sum
        })
        .collect::<Vec<_>>();
    let mut ends = Vec::new();
    for p in 1..=n {
        let target = (sum as u128 * p as u128 / n as u128) as usize;
        let mut end = cumulative
            .partition_point(|&s| s < target)
            .min(counts.len() - 1);
        while end + 1 < counts.len() && cumulative[end + 1] == target {
            end += 1;
        }
        if ends.last() != Some(&end) {
            ends.push(end);
        }
    }
    ends
}
pub(crate) struct KeyRanges {
    pub ordering: LexOrdering,
    pub splits: Vec<SplitPoint>,
}
pub(crate) async fn key_ranges(
    input: PhysicalPlan,
    keys: &[String],
    total: usize,
    n: usize,
    context: Arc<TaskContext>,
) -> datafusion_common::Result<KeyRanges> {
    let indices = keys
        .iter()
        .map(|name| input.schema().index_of(name).map_err(DataFusionError::from))
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    let ordering = LexOrdering::new(
        indices
            .iter()
            .map(|&index| {
                PhysicalSortExpr::new(
                    column(&input, index),
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                )
            })
            .collect::<Vec<_>>(),
    )
    .ok_or_else(|| DataFusionError::Plan("range partitioning requires at least one key".into()))?;
    let mut stream = input.execute(0, context)?;
    let mut ordinal = 0usize;
    let mut next = 1usize;
    let mut previous: Option<(Vec<ArrayRef>, usize)> = None;
    let mut splits = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let columns = indices
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect::<Vec<_>>();
        let options = sort_options(keys.len());
        let within = make_typed_key_comparators(&columns, &columns, &options)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let across = previous
            .as_ref()
            .map(|(old, _)| make_typed_key_comparators(old, &columns, &options))
            .transpose()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        for row in 0..batch.num_rows() {
            let ordering = if row > 0 {
                compare_rows(&within, row - 1, row)
            } else if let (Some((_, previous_row)), Some(comparators)) = (&previous, &across) {
                compare_rows(comparators, *previous_row, row)
            } else {
                None
            };
            if ordering == Some(std::cmp::Ordering::Greater) {
                return Err(DataFusionError::Execution(
                    "range partition input is not sorted by its key".into(),
                ));
            }
            let changed = ordering != Some(std::cmp::Ordering::Equal);
            let target = next < n && ordinal >= (total as u128 * next as u128 / n as u128) as usize;
            if changed && target && ordinal > 0 {
                let values = indices
                    .iter()
                    .map(|&i| ScalarValue::try_from_array(batch.column(i), row))
                    .collect::<datafusion_common::Result<Vec<_>>>()?;
                splits.push(SplitPoint::new(values));
                while next < n && ordinal >= (total as u128 * next as u128 / n as u128) as usize {
                    next += 1;
                }
            }
            ordinal += 1;
        }
        previous = Some((columns, batch.num_rows() - 1));
    }
    if ordinal != total {
        return Err(DataFusionError::Execution(
            "input cardinality changed during range partition planning".into(),
        ));
    }
    Ok(KeyRanges { ordering, splits })
}

/// Count distinct sorted keys while retaining only the previous typed key row.
/// This is metadata planning, not a Table distinct/aggregation implementation.
pub(crate) async fn distinct_key_positions(
    input: PhysicalPlan,
    keys: &[String],
    wanted: &[usize],
) -> datafusion_common::Result<(usize, Vec<(usize, Vec<ScalarValue>)>)> {
    let fields = keys
        .iter()
        .map(|k| input.schema().index_of(k))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut stream = input.execute(0, Arc::new(TaskContext::default()))?;
    let mut previous: Option<(Vec<ArrayRef>, usize)> = None;
    let mut index = 0usize;
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let columns = fields
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect::<Vec<_>>();
        let options = sort_options(keys.len());
        let within = make_typed_key_comparators(&columns, &columns, &options)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let across = previous
            .as_ref()
            .map(|(old, _)| make_typed_key_comparators(old, &columns, &options))
            .transpose()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        for row in 0..batch.num_rows() {
            let ordering = if row > 0 {
                compare_rows(&within, row - 1, row)
            } else if let (Some((_, previous_row)), Some(comparators)) = (&previous, &across) {
                compare_rows(comparators, *previous_row, row)
            } else {
                None
            };
            if ordering == Some(std::cmp::Ordering::Equal) {
                continue;
            }
            if ordering == Some(std::cmp::Ordering::Greater) {
                return Err(DataFusionError::Execution(
                    "partition planner input is not in key order".into(),
                ));
            }
            if wanted.binary_search(&index).is_ok() {
                values.push((
                    index,
                    fields
                        .iter()
                        .map(|&i| ScalarValue::try_from_array(batch.column(i), row))
                        .collect::<datafusion_common::Result<Vec<_>>>()?,
                ));
            }
            index = index
                .checked_add(1)
                .ok_or_else(|| DataFusionError::Execution("distinct key count overflow".into()))?;
        }
        previous = Some((columns, batch.num_rows() - 1));
    }
    Ok((index, values))
}

fn sort_options(n: usize) -> Vec<SortOptions> {
    vec![
        SortOptions {
            descending: false,
            nulls_first: false
        };
        n
    ]
}

fn compare_rows(
    comparators: &[arrow_ord::ord::DynComparator],
    left: usize,
    right: usize,
) -> Option<std::cmp::Ordering> {
    for comparator in comparators {
        let ordering = comparator(left, right);
        if ordering != std::cmp::Ordering::Equal {
            return Some(ordering);
        }
    }
    Some(std::cmp::Ordering::Equal)
}
