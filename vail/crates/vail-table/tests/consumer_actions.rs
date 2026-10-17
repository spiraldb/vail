//! Public prepared-action behavior across independent multipartition consumers.
use arrow_array::Int32Array;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::ExecutionPlanProperties;
use futures::TryStreamExt;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_table::{RepartitionStrategy, ScalarIR, TableIR, partitioned};

fn i32_literal(value: i32) -> ScalarIR {
    ScalarIR::Literal {
        dtype: Type::Primitive(PrimitiveType::Int32),
        value: json!(value),
    }
}

#[tokio::test]
async fn independent_prepared_actions_accept_interleaved_partition_requests() {
    let table = TableIR::range(37, 3)
        .unwrap()
        .repartition(4, RepartitionStrategy::Shuffle)
        .unwrap();
    let prepared = partitioned::prepare(table).await.unwrap();
    let first = prepared.start_action().unwrap();
    let second = prepared.start_action().unwrap();
    let partitions = first.output_partitioning().partition_count();
    assert_eq!(second.output_partitioning().partition_count(), partitions);
    let mut outputs = [Vec::new(), Vec::new()];
    for partition in (0..partitions).rev() {
        for (index, action) in [&first, &second].into_iter().enumerate() {
            let batches = action
                .execute(partition, Arc::new(TaskContext::default()))
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut values = Vec::new();
            for batch in batches {
                values.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                );
            }
            assert!(values.windows(2).all(|pair| pair[0] <= pair[1]));
            outputs[index].extend(values);
        }
    }
    for values in &mut outputs {
        values.sort_unstable();
        assert_eq!(*values, (0..37).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn dropping_a_partial_public_action_does_not_poison_the_next_action() {
    let table = TableIR::range_batched(100_000, 4, 1024).unwrap();
    let mut partial = table.execute();
    assert!(partial.try_next().await.unwrap().is_some());
    drop(partial);
    assert_eq!(table.force_count().await.unwrap(), 100_000);
    assert_eq!(table.head(7).count().await.unwrap(), 7);
}

#[tokio::test]
async fn an_exchange_requires_an_independent_action_for_replay() {
    let table = TableIR::range(37, 3)
        .unwrap()
        .repartition(4, RepartitionStrategy::Shuffle)
        .unwrap();
    let prepared = partitioned::prepare(table).await.unwrap();
    let action = prepared.start_action().unwrap();
    let context = Arc::new(TaskContext::default());
    let first = datafusion_physical_plan::collect(action.clone(), context.clone())
        .await
        .unwrap();
    assert_eq!(
        first.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        37
    );
    let error = datafusion_physical_plan::collect(action, context.clone())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("explicit new action"));
    let repeated = datafusion_physical_plan::collect(prepared.start_action().unwrap(), context)
        .await
        .unwrap();
    assert_eq!(
        repeated.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        37
    );
}

#[tokio::test]
async fn force_count_preserves_ordered_rows_and_limit_semantics() {
    let ordered = TableIR::range(100, 4)
        .unwrap()
        .order_by(vec![("idx".into(), true)])
        .unwrap();
    assert_eq!(ordered.force_count().await.unwrap(), 100);
    let batches = ordered.collect_partitions().await.unwrap();
    let values = batches[0]
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (0..100).rev().collect::<Vec<_>>());

    let limited = ordered.head(5);
    assert_eq!(limited.force_count().await.unwrap(), 5);
    let batches = limited.collect_partitions().await.unwrap();
    let values = batches[0]
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, [99, 98, 97, 96, 95]);
}

#[tokio::test]
async fn force_count_evaluates_projected_fields_in_every_partition() {
    let invalid_array = ScalarIR::StreamRange {
        start: Arc::new(i32_literal(0)),
        stop: Arc::new(i32_literal(100)),
        step: Arc::new(i32_literal(0)),
    };
    let table = TableIR::range(100, 4)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "checked".into(),
                ScalarIR::IfElse {
                    condition: Arc::new(ScalarIR::Binary {
                        operator: BinaryOperator::Eq,
                        left: Arc::new(ScalarIR::Column { name: "idx".into() }),
                        right: Arc::new(i32_literal(99)),
                    }),
                    consequent: Arc::new(ScalarIR::ArrayLength {
                        value: Arc::new(invalid_array),
                    }),
                    alternate: Arc::new(i32_literal(0)),
                },
            ),
        ])
        .unwrap()
        .order_by(vec![("idx".into(), true)])
        .unwrap();
    let error = table
        .force_count()
        .await
        .unwrap_err()
        .to_string()
        .to_lowercase();
    assert!(error.contains("step must not be zero"), "{error}");
}
