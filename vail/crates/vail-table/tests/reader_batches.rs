use arrow_array::{Array, Int32Array, Int64Array, RecordBatch};
use datafusion_execution::{TaskContext, config::SessionConfig};
use datafusion_physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_table::{ScalarIR, TableIR, TableRef, partitioned};

// Execute the supported prepared-table API using a caller-owned session.
// No substitute reader or execution operator is installed by these tests.
struct ReadOutput {
    partitions: Vec<Vec<RecordBatch>>,
    observed_splitting: bool,
}

// This is an execution metric from DataFusion's public metrics API. It records
// real splitting work; it does not assert an operator name or optimizer layout.
fn observed_splitting(plan: &dyn ExecutionPlan) -> bool {
    plan.metrics()
        .and_then(|metrics| metrics.sum_by_name("batches_split"))
        .is_some_and(|value| value.as_usize() > 0)
        || plan
            .children()
            .iter()
            .any(|child| observed_splitting(child.as_ref()))
}

async fn batches(table: &TableRef, batch_size: usize) -> ReadOutput {
    let prepared = partitioned::prepare(table.clone()).await.unwrap();
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(batch_size)),
    );
    let mut partitions = Vec::new();
    let mut split = false;
    for part in prepared.parts {
        partitions.push(
            part.execute(0, context.clone())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
        );
        split |= observed_splitting(part.as_ref());
    }
    ReadOutput {
        partitions,
        observed_splitting: split,
    }
}

fn keys(partitions: &[Vec<RecordBatch>]) -> Vec<Vec<i32>> {
    partitions
        .iter()
        .map(|partition| {
            partition
                .iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name("idx")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect()
        })
        .collect()
}

fn comparison(operator: BinaryOperator, boundary: i32) -> ScalarIR {
    ScalarIR::Binary {
        operator,
        left: Arc::new(ScalarIR::Column { name: "idx".into() }),
        right: Arc::new(ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: json!(boundary),
        }),
    }
}

#[tokio::test]
async fn real_readers_bound_batches_and_preserve_order_and_partition_membership() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("table");
    let source = TableIR::range(257, 3).unwrap();
    let expected = keys(&source.collect_partitions().await.unwrap());
    source.write(path.to_str().unwrap(), false).await.unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();

    let unsplit = batches(&read, 1024).await;
    assert!(
        unsplit
            .partitions
            .iter()
            .flatten()
            .any(|batch| batch.num_rows() > 7)
    );
    let actual = batches(&read, 7).await;
    assert!(actual.observed_splitting);
    assert!(
        actual
            .partitions
            .iter()
            .flatten()
            .all(|batch| batch.num_rows() <= 7)
    );
    assert_eq!(keys(&actual.partitions), expected);

    // Both bounds fall inside real stored partitions. This exercises the
    // key-predicate reader as well as its residual DataFusion filter.
    let filtered = read
        .filter(comparison(BinaryOperator::Ge, 30), true)
        .unwrap()
        .filter(comparison(BinaryOperator::Lt, 210), true)
        .unwrap();
    let unsplit = batches(&filtered, 1024).await;
    assert!(
        unsplit
            .partitions
            .iter()
            .flatten()
            .any(|batch| batch.num_rows() > 7)
    );
    let actual = batches(&filtered, 7).await;
    // The residual filter may coalesce its output. The metric proves that the
    // reader did split, without claiming a bound on those final filter batches.
    assert!(actual.observed_splitting);
    let expected = expected
        .into_iter()
        .map(|partition| {
            partition
                .into_iter()
                .filter(|key| (30..210).contains(key))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(keys(&actual.partitions), expected);
}

#[tokio::test]
async fn stored_scan_state_survives_reader_batch_and_partition_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("table");
    TableIR::range(257, 3)
        .unwrap()
        .write(path.to_str().unwrap(), false)
        .await
        .unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let scanned = read
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "prefix".into(),
                ScalarIR::Aggregate {
                    function: "sum".into(),
                    value: Some(Arc::new(ScalarIR::Cast {
                        value: Arc::new(ScalarIR::Column { name: "idx".into() }),
                        dtype: Type::Primitive(PrimitiveType::Int64),
                    })),
                    options: json!({"__vail_scan": true}),
                    init: vec![],
                },
            ),
        ])
        .unwrap();
    let actual = batches(&scanned, 7).await;
    assert!(actual.observed_splitting);
    let mut observed = Vec::new();
    for batch in actual.partitions.iter().flatten() {
        let index = batch
            .column_by_name("idx")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let prefix = batch
            .column_by_name("prefix")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(prefix.null_count(), 0);
        observed.extend(
            index
                .values()
                .iter()
                .copied()
                .zip(prefix.values().iter().copied()),
        );
    }
    assert_eq!(
        observed,
        (0..257i32)
            .map(|i| (i, i64::from(i) * i64::from(i - 1) / 2))
            .collect::<Vec<_>>()
    );
}
