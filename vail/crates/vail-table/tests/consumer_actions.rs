//! Public prepared-action behavior across independent multipartition consumers.
use arrow_array::Int32Array;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::ExecutionPlanProperties;
use futures::TryStreamExt;
use std::sync::Arc;
use vail_table::{RepartitionStrategy, TableIR, partitioned};

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
