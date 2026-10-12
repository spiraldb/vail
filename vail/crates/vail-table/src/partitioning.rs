//! Bounded partition metadata planning; DataFusion performs redistribution.
use crate::RepartitionStrategy;
use crate::plan::{PhysicalPlan, column};
use arrow_array::Array;
use datafusion_common::{DataFusionError, ScalarValue, SplitPoint, config::ConfigOptions};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::{PhysicalExpr, expressions::Literal};
use futures::TryStreamExt;
use std::sync::Arc;

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
/// Range keys always use Vail's canonical encoder, including nested values,
/// missing values and IEEE totalOrder floats; Arrow's native float comparator
/// is deliberately not used to choose or route split points.
pub(crate) struct KeyRanges {
    pub expression: Arc<dyn PhysicalExpr>,
    pub splits: Vec<SplitPoint>,
}
pub(crate) fn key_expression(
    input: &PhysicalPlan,
    keys: &[String],
) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
    let schema = input.schema();
    let mut args = Vec::<Arc<dyn PhysicalExpr>>::new();
    for name in keys {
        args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
            name.clone(),
        )))));
        args.push(column(input, schema.index_of(name)?));
    }
    let value = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::core::named_struct(),
        args,
        &schema,
        Arc::new(ConfigOptions::default()),
    )?);
    Ok(vail_execution::ordering::canonical_key(value))
}
pub(crate) async fn key_ranges(
    input: PhysicalPlan,
    keys: &[String],
    total: usize,
    n: usize,
    context: Arc<TaskContext>,
) -> datafusion_common::Result<KeyRanges> {
    let expression = key_expression(&input, keys)?;
    let mut stream = input.execute(0, context)?;
    let mut ordinal = 0usize;
    let mut next = 1usize;
    let mut previous: Option<Vec<u8>> = None;
    let mut splits = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let encoded = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
        let encoded = encoded
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("canonical key expression did not produce binary".into())
            })?;
        for row in 0..batch.num_rows() {
            let key = encoded.value(row);
            let changed = previous.as_deref() != Some(key);
            let target = next < n && ordinal >= (total as u128 * next as u128 / n as u128) as usize;
            if changed && target && ordinal > 0 {
                splits.push(SplitPoint::new(vec![ScalarValue::Binary(Some(
                    key.to_vec(),
                ))]));
                while next < n && ordinal >= (total as u128 * next as u128 / n as u128) as usize {
                    next += 1;
                }
            }
            if changed {
                previous = Some(key.to_vec());
            }
            ordinal += 1;
        }
    }
    if ordinal != total {
        return Err(DataFusionError::Execution(
            "input cardinality changed during range partition planning".into(),
        ));
    }
    Ok(KeyRanges { expression, splits })
}

/// Count distinct sorted keys while retaining only the previous encoded key.
/// This is metadata planning, not a Table distinct/aggregation implementation.
pub(crate) async fn distinct_key_positions(
    input: PhysicalPlan,
    keys: &[String],
    wanted: &[usize],
) -> datafusion_common::Result<(usize, Vec<(usize, Vec<ScalarValue>)>)> {
    let expression = key_expression(&input, keys)?;
    let fields = keys
        .iter()
        .map(|k| input.schema().index_of(k))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut stream = input.execute(0, Arc::new(TaskContext::default()))?;
    let mut previous: Option<Vec<u8>> = None;
    let mut index = 0usize;
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let encoded = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
        let encoded = encoded
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("canonical key must be binary".into()))?;
        for row in 0..batch.num_rows() {
            let key = encoded.value(row);
            if previous.as_deref() == Some(key) {
                continue;
            }
            if previous.as_deref().is_some_and(|p| p > key) {
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
            previous = Some(key.to_vec());
            index = index
                .checked_add(1)
                .ok_or_else(|| DataFusionError::Execution("distinct key count overflow".into()))?;
        }
    }
    Ok((index, values))
}
