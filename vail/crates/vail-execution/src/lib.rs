//! Canonical Hail key ordering for embedded Arrow partitions.
//!
//! Ordering expressions use the same byte encoding as keyed storage metadata.
//! It always sorts the provided in-memory partitions before merging them. It
//! does not yet propagate an order proof through arbitrary DataFusion plans or
//! provide a streaming storage scan; the caller's memory limit applies to the
//! sort and merge operators, not to batches already held by the caller.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
#[cfg(test)]
use arrow_array::StringArray;
use arrow_schema::{DataType, SchemaRef, SortOptions};
use datafusion_common::DataFusionError;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::{Column, IsNullExpr};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::execution_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use thiserror::Error;

mod aggregate_identity;
pub mod aggregates;
mod array_fold;
pub mod calls;
pub mod collect_aggregate;
mod conditionals;
pub mod csv_line;
pub mod expressions;
pub mod logical_expr;
mod matrix_arrays;
pub mod reference;
pub mod scan_scopes;
pub mod scan_state;
mod string_kernels;

#[derive(Debug, Error)]
pub enum OrderError {
    #[error("invalid order requirement: {0}")]
    InvalidRequirement(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("DataFusion execution failed: {0}")]
    DataFusion(#[from] DataFusionError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    pub column: String,
    pub descending: bool,
    pub nulls_last: bool,
}

impl OrderKey {
    pub fn new(column: impl Into<String>, descending: bool, nulls_last: bool) -> Self {
        Self {
            column: column.into(),
            descending,
            nulls_last,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRequirement {
    keys: Vec<OrderKey>,
}

impl OrderRequirement {
    pub fn new(keys: Vec<OrderKey>) -> Result<Self, OrderError> {
        if keys.is_empty() {
            return Err(OrderError::InvalidRequirement(
                "at least one sort key is required".into(),
            ));
        }
        let mut names = HashSet::new();
        for key in &keys {
            if key.column.is_empty() {
                return Err(OrderError::InvalidRequirement(
                    "sort column name cannot be empty".into(),
                ));
            }
            if !names.insert(key.column.as_str()) {
                return Err(OrderError::InvalidRequirement(format!(
                    "duplicate sort column {:?}",
                    key.column
                )));
            }
        }
        Ok(Self { keys })
    }

    pub fn keys(&self) -> &[OrderKey] {
        &self.keys
    }

    pub fn physical_ordering(&self, schema: &SchemaRef) -> Result<LexOrdering, OrderError> {
        let mut sort_exprs = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            let index = schema.index_of(&key.column).map_err(|_| {
                OrderError::InvalidRequirement(format!(
                    "sort column {:?} is absent from the input schema",
                    key.column
                ))
            })?;
            let data_type = schema.field(index).data_type();
            if !supports_arrow_comparator(data_type) {
                return Err(OrderError::InvalidRequirement(format!(
                    "sort column {:?} has unsupported comparator domain {data_type:?}",
                    key.column
                )));
            }
            let column = Arc::new(Column::new(&key.column, index));
            sort_exprs.push(PhysicalSortExpr::new(
                Arc::new(IsNullExpr::new(column.clone())),
                SortOptions {
                    descending: !key.nulls_last,
                    nulls_first: false,
                },
            ));
            sort_exprs.push(PhysicalSortExpr::new(
                column,
                SortOptions {
                    descending: key.descending,
                    nulls_first: false,
                },
            ));
        }
        LexOrdering::new(sort_exprs).ok_or_else(|| {
            OrderError::InvalidRequirement("sort keys produce no physical ordering".into())
        })
    }
}

fn supports_arrow_comparator(data_type: &DataType) -> bool {
    vail_ordering::supports(data_type)
}

/// A reusable execution context for ordered collection.
#[derive(Debug, Clone)]
pub struct OrderedSession {
    context: Arc<TaskContext>,
}

impl Default for OrderedSession {
    fn default() -> Self {
        Self {
            context: Arc::new(TaskContext::default()),
        }
    }
}

impl OrderedSession {
    pub fn new(context: Arc<TaskContext>) -> Self {
        Self { context }
    }

    /// Sort each input partition, merge them in key order, and optionally take
    /// the first `limit` rows. All input batches must match `schema` exactly.
    pub async fn collect(
        &self,
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        requirement: &OrderRequirement,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>, OrderError> {
        let plan = plan_ordered_collect(partitions, schema, requirement, limit)?;
        Ok(collect(plan, Arc::clone(&self.context)).await?)
    }
}

/// Builds a physical plan with an explicit single-partition ordering proof.
/// The returned plan can be inspected or executed by a host session.
pub fn plan_ordered_collect(
    partitions: &[Vec<RecordBatch>],
    schema: SchemaRef,
    requirement: &OrderRequirement,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>, OrderError> {
    let ordering = requirement.physical_ordering(&schema)?;
    for (partition_index, partition) in partitions.iter().enumerate() {
        for (batch_index, batch) in partition.iter().enumerate() {
            if batch.schema() != schema {
                return Err(OrderError::InvalidInput(format!(
                    "partition {partition_index} batch {batch_index} schema differs from the declared schema"
                )));
            }
        }
    }
    // A zero-partition source cannot feed SortPreservingMergeExec. One empty
    // partition still produces the correct empty result and sorted properties.
    let empty = vec![Vec::new()];
    let partitions = if partitions.is_empty() {
        &empty
    } else {
        partitions
    };
    let source: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(partitions, schema, None)?;
    let local_sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering.clone(), source).with_preserve_partitioning(true));
    if !local_sort
        .equivalence_properties()
        .ordering_satisfy(ordering.clone())?
    {
        return Err(OrderError::InvalidInput(
            "local sort failed to advertise the required ordering".into(),
        ));
    }
    let merged: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(ordering.clone(), local_sort).with_fetch(limit));
    if merged.output_partitioning().partition_count() != 1
        || !merged.equivalence_properties().ordering_satisfy(ordering)?
    {
        return Err(OrderError::InvalidInput(
            "sorted merge failed to advertise one ordered partition".into(),
        ));
    }
    Ok(merged)
}

/// Convenience API using a default embedded execution context.
pub async fn ordered_collect(
    partitions: &[Vec<RecordBatch>],
    schema: SchemaRef,
    requirement: &OrderRequirement,
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>, OrderError> {
    OrderedSession::default()
        .collect(partitions, schema, requirement, limit)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, Int32Array, Int64Array};
    use arrow_schema::{Field, Schema};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;

    fn int_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]))
    }

    fn int_batch(schema: &SchemaRef, values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn flatten_i64(batches: &[RecordBatch]) -> Vec<Option<i64>> {
        batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..values.len()).map(move |i| (!values.is_null(i)).then(|| values.value(i)))
            })
            .collect()
    }

    #[tokio::test]
    async fn interleaved_partitions_merge_in_global_order() {
        let schema = int_schema();
        let partitions = vec![
            vec![
                int_batch(&schema, vec![Some(4), Some(1)]),
                int_batch(&schema, vec![Some(3)]),
            ],
            vec![int_batch(&schema, vec![Some(2), Some(5)])],
        ];
        let order = OrderRequirement::new(vec![OrderKey::new("k", false, true)]).unwrap();
        let plan = plan_ordered_collect(&partitions, Arc::clone(&schema), &order, None).unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 1);
        assert!(plan.output_ordering().is_some());
        let result = ordered_collect(&partitions, schema, &order, None)
            .await
            .unwrap();
        assert_eq!(
            flatten_i64(&result),
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)]
        );
    }

    #[tokio::test]
    async fn descending_nulls_last_and_limit_apply_after_merge() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let make_batch = |values: Vec<Option<i32>>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(values))],
            )
            .unwrap()
        };
        let partitions = vec![
            vec![make_batch(vec![None, Some(2), Some(9)])],
            vec![make_batch(vec![Some(5), None, Some(7)])],
        ];
        let order = OrderRequirement::new(vec![OrderKey::new("k", true, true)]).unwrap();
        let result = ordered_collect(&partitions, Arc::clone(&schema), &order, None)
            .await
            .unwrap();
        let values = result
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..array.len()).map(move |i| (!array.is_null(i)).then(|| array.value(i)))
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Some(9), Some(7), Some(5), Some(2), None, None]);
        let limited = ordered_collect(&partitions, schema, &order, Some(3))
            .await
            .unwrap();
        let limited_values = limited
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..array.len()).map(move |i| (!array.is_null(i)).then(|| array.value(i)))
            })
            .collect::<Vec<_>>();
        assert_eq!(limited_values, vec![Some(9), Some(7), Some(5)]);
        let zero = ordered_collect(
            &partitions,
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)])),
            &order,
            Some(0),
        )
        .await
        .unwrap();
        assert_eq!(zero.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn compound_mixed_directions_ignore_input_partition_layout() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let make_batch = |rows: &[(Option<i32>, Option<&str>)]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(
                        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };
        let layouts = [
            vec![
                vec![
                    make_batch(&[(Some(2), Some("a")), (Some(1), Some("z"))]),
                    make_batch(&[(None, Some("b")), (Some(1), None)]),
                ],
                vec![make_batch(&[
                    (Some(1), Some("c")),
                    (Some(2), Some("d")),
                    (Some(1), Some("a")),
                    (None, Some("a")),
                ])],
            ],
            vec![
                vec![make_batch(&[(None, Some("a")), (Some(2), Some("d"))])],
                vec![make_batch(&[(Some(1), None), (Some(1), Some("z"))])],
                vec![
                    make_batch(&[(Some(2), Some("a")), (None, Some("b"))]),
                    make_batch(&[(Some(1), Some("a")), (Some(1), Some("c"))]),
                ],
            ],
        ];
        let requirement = OrderRequirement::new(vec![
            OrderKey::new("a", false, true),
            OrderKey::new("b", true, true),
        ])
        .unwrap();
        let expected = vec![
            (Some(1), Some("z".to_string())),
            (Some(1), Some("c".to_string())),
            (Some(1), Some("a".to_string())),
            (Some(1), None),
            (Some(2), Some("d".to_string())),
            (Some(2), Some("a".to_string())),
            (None, Some("b".to_string())),
            (None, Some("a".to_string())),
        ];
        for partitions in layouts {
            let output = ordered_collect(&partitions, Arc::clone(&schema), &requirement, None)
                .await
                .unwrap();
            let rows = output
                .iter()
                .flat_map(|batch| {
                    let a = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let b = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    (0..batch.num_rows()).map(move |row| {
                        (
                            (!a.is_null(row)).then(|| a.value(row)),
                            (!b.is_null(row)).then(|| b.value(row).to_string()),
                        )
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(rows, expected);
        }
    }

    #[tokio::test]
    async fn caller_memory_limit_spills_local_sort() {
        let schema = int_schema();
        let session_config = SessionConfig::new();
        let reserve = session_config
            .options()
            .execution
            .sort_spill_reservation_bytes;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(reserve + 12_288, 1.0)
            .build_arc()
            .unwrap();
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(Arc::clone(&runtime)),
        );
        let partitions = vec![
            (0..100)
                .map(|batch| {
                    int_batch(
                        &schema,
                        (0..100)
                            .map(|row| Some((10_000 - batch * 100 - row) as i64))
                            .collect(),
                    )
                })
                .collect::<Vec<_>>(),
        ];
        let order = OrderRequirement::new(vec![OrderKey::new("k", false, true)]).unwrap();
        let plan = plan_ordered_collect(&partitions, schema, &order, None).unwrap();
        let result = collect(Arc::clone(&plan), context).await.unwrap();
        let values = flatten_i64(&result);
        assert_eq!(values.len(), 10_000);
        assert_eq!(values.first(), Some(&Some(1)));
        assert_eq!(values.last(), Some(&Some(10_000)));
        let local_sort = plan.children()[0];
        assert!(
            local_sort.metrics().unwrap().spill_count().unwrap_or(0) > 0,
            "test input should force a spill under the caller's memory limit"
        );
        assert_eq!(runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn empty_and_duplicate_keys_are_handled() {
        let schema = int_schema();
        let order = OrderRequirement::new(vec![OrderKey::new("k", false, true)]).unwrap();
        assert!(
            ordered_collect(&[], Arc::clone(&schema), &order, None)
                .await
                .unwrap()
                .iter()
                .all(|batch| batch.num_rows() == 0)
        );
        let partitions = vec![
            vec![int_batch(&schema, vec![Some(2), Some(1), Some(2)])],
            vec![],
            vec![int_batch(&schema, vec![Some(1), Some(3)])],
        ];
        let result = ordered_collect(&partitions, schema, &order, None)
            .await
            .unwrap();
        assert_eq!(
            flatten_i64(&result),
            vec![Some(1), Some(1), Some(2), Some(2), Some(3)]
        );
    }

    #[test]
    fn invalid_inputs_fail_before_execution() {
        let schema = int_schema();
        assert!(OrderRequirement::new(vec![]).is_err());
        assert!(
            OrderRequirement::new(vec![
                OrderKey::new("k", false, true),
                OrderKey::new("k", true, true)
            ])
            .is_err()
        );
        let missing = OrderRequirement::new(vec![OrderKey::new("absent", false, true)]).unwrap();
        assert!(plan_ordered_collect(&[], Arc::clone(&schema), &missing, None).is_err());
        let order = OrderRequirement::new(vec![OrderKey::new("k", false, true)]).unwrap();
        let wrong_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = int_batch(&wrong_schema, vec![Some(1)]);
        assert!(plan_ordered_collect(&[vec![batch]], Arc::clone(&schema), &order, None).is_err());
        let float_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Float64, false)]));
        let float_batch = RecordBatch::try_new(
            Arc::clone(&float_schema),
            vec![Arc::new(Float64Array::from(vec![1.0]))],
        )
        .unwrap();
        assert!(plan_ordered_collect(&[vec![float_batch]], float_schema, &order, None).is_ok());
        let string_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let string_batch = RecordBatch::try_new(
            Arc::clone(&string_schema),
            vec![Arc::new(StringArray::from(vec!["é"]))],
        )
        .unwrap();
        assert!(plan_ordered_collect(&[vec![string_batch]], string_schema, &order, None).is_ok());
    }
}

pub mod advanced_collection_aggregates;
pub mod advanced_genomic_aggregates;
pub mod advanced_numeric_aggregates;
pub mod array_aggregate;
pub mod array_length_aggregate;

pub mod fold_aggregate;

mod collection_groups;

mod collection_lookup;

mod matrix_transpose;

mod randomness;

mod hwe_counts;
