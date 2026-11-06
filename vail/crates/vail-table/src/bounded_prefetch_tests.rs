//! Public physical sources/expressions exercise values and stream ownership.
use super::wrap;
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::DataFusionError;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::{ColumnarValue, Volatility, expr_fn::create_udf};
use datafusion_physical_expr::{ScalarFunctionExpr, expressions::Column};
use datafusion_physical_plan::{
    ExecutionPlan, empty::EmptyExec, limit::GlobalLimitExec, projection::ProjectionExec,
};
use futures::TryStreamExt;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

fn schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, true)],
        HashMap::from([("meaning".into(), "ordered rows".into())]),
    ))
}
fn batch(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
}
fn source(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&partitions, schema(), None).unwrap()
}
fn values(batch: &RecordBatch) -> Vec<Option<i64>> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[tokio::test]
async fn ordered_rows_nulls_metadata_and_repeated_execution() {
    let original = vec![
        batch(vec![Some(3), None]),
        batch(vec![]),
        batch(vec![Some(1), Some(2)]),
        batch(vec![None]),
    ];
    let plan = wrap(source(vec![original.clone()]));
    for _ in 0..2 {
        let mut output = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert_eq!(output.schema(), schema());
        let mut received = Vec::new();
        while let Some(batch) = output.try_next().await.unwrap() {
            received.push(batch);
        }
        assert_eq!(received, original);
        assert_eq!(
            received.iter().flat_map(values).collect::<Vec<_>>(),
            vec![Some(3), None, Some(1), Some(2), None]
        );
    }
}

#[derive(Default)]
struct Released {
    done: AtomicBool,
    changed: Notify,
}
impl Released {
    async fn wait(&self) {
        timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.changed.notified();
                if self.done.load(Ordering::SeqCst) {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("upstream expression did not release its owned resource");
    }
}
struct Resource(Arc<Released>);
impl Drop for Resource {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}
#[derive(Clone, Copy)]
enum Failure {
    None,
    Error,
    Panic,
}

// A real expression owns a resource until its execution stream is released.
// Errors/panics occur in expression evaluation, not in a mocked stream.
fn projected(failure: Failure, rows: usize) -> (Arc<dyn ExecutionPlan>, Arc<Released>) {
    let input = source(vec![
        (0..rows).map(|id| batch(vec![Some(id as i64)])).collect(),
    ]);
    let released = Arc::new(Released::default());
    let resource = Arc::new(Resource(released.clone()));
    let function = create_udf(
        "prefetch_public_expression",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |arguments| {
            let _owned = resource.clone();
            let ColumnarValue::Array(array) = &arguments[0] else {
                unreachable!()
            };
            let id = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            if id == 2 {
                match failure {
                    Failure::None => {}
                    Failure::Error => {
                        return Err(DataFusionError::Execution(
                            "intentional prefetch expression error".into(),
                        ));
                    }
                    Failure::Panic => panic!("intentional prefetch expression panic"),
                }
            }
            Ok(arguments[0].clone())
        }),
    );
    let expression = Arc::new(
        ScalarFunctionExpr::try_new(
            Arc::new(function),
            vec![Arc::new(Column::new("id", 0))],
            &input.schema(),
            Arc::new(datafusion_common::config::ConfigOptions::default()),
        )
        .unwrap(),
    );
    let projected = ProjectionExec::try_new_with_schema_metadata(
        vec![(
            expression as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
            "id".into(),
        )],
        input,
        &schema(),
    )
    .unwrap();
    (wrap(Arc::new(projected)), released)
}

#[tokio::test]
async fn expression_error_follows_preceding_rows() {
    let (plan, released) = projected(Failure::Error, 4);
    let mut output = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    drop(plan);
    assert_eq!(
        values(&output.try_next().await.unwrap().unwrap()),
        vec![Some(0)]
    );
    assert_eq!(
        values(&output.try_next().await.unwrap().unwrap()),
        vec![Some(1)]
    );
    let error = output.try_next().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("intentional prefetch expression error")
    );
    assert!(output.try_next().await.unwrap().is_none());
    drop(output);
    released.wait().await;
}

#[tokio::test]
async fn expression_panic_is_reported_after_preceding_rows() {
    let (plan, released) = projected(Failure::Panic, 4);
    let mut output = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    drop(plan);
    assert_eq!(
        values(&output.try_next().await.unwrap().unwrap()),
        vec![Some(0)]
    );
    assert_eq!(
        values(&output.try_next().await.unwrap().unwrap()),
        vec![Some(1)]
    );
    let error = output.try_next().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("intentional prefetch expression panic")
    );
    assert!(output.try_next().await.unwrap().is_none());
    drop(output);
    released.wait().await;
}

#[tokio::test]
async fn dropping_unpolled_or_prefix_output_releases_upstream() {
    for take_first in [false, true] {
        // More than the queue holds, so completion cannot explain release.
        let (plan, released) = projected(Failure::None, 100);
        let mut output = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        drop(plan);
        if take_first {
            assert_eq!(
                values(&output.try_next().await.unwrap().unwrap()),
                vec![Some(0)]
            );
        }
        drop(output);
        released.wait().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_higher_partition_does_not_exhaust_lower_partition_credit() {
    let large = batch(vec![Some(1); (32 * 1024 * 1024) / 8 + 1]);
    assert!(large.get_array_memory_size() > 32 * 1024 * 1024);
    let plan = wrap(source(vec![vec![large.clone(); 3], vec![large.clone(); 3]]));
    let context = SessionContext::new_with_config(
        SessionConfig::new().with_batch_size(large.num_rows()),
    ).task_ctx();
    let mut higher = plan.execute(1, context.clone()).unwrap();
    let held = higher.try_next().await.unwrap().unwrap();
    assert_eq!(held, large);
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    let mut lower = plan.execute(0, context).unwrap();
    let first = timeout(Duration::from_secs(2), lower.try_next())
        .await
        .expect("higher partition prevented lower partition progress")
        .unwrap()
        .unwrap();
    assert_eq!(first, large);
    timeout(Duration::from_secs(5), async {
        while let Some(batch) = lower.try_next().await.unwrap() {
            assert_eq!(batch, large);
        }
        while let Some(batch) = higher.try_next().await.unwrap() {
            assert_eq!(batch, large);
        }
    })
    .await
    .expect("partition queues did not complete their independent drains");
}

#[tokio::test]
async fn public_limit_stops_at_prefix_and_releases_upstream() {
    let (prefetched, released) = projected(Failure::None, 100);
    let limited = GlobalLimitExec::new(prefetched, 0, Some(3));
    let mut output = limited
        .execute(0, Arc::new(TaskContext::default()))
        .unwrap();
    drop(limited);
    let mut rows = Vec::new();
    while let Some(batch) = output.try_next().await.unwrap() {
        rows.extend(values(&batch));
    }
    assert_eq!(rows, vec![Some(0), Some(1), Some(2)]);
    drop(output);
    released.wait().await;
}

#[test]
fn synchronous_input_execution_error_stays_synchronous() {
    let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
    let context = Arc::new(TaskContext::default());
    let original = match input.execute(1, context.clone()) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("invalid empty-source partition unexpectedly succeeded"),
    };
    let error = match wrap(input).execute(1, context) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("prefetch deferred a synchronous input error"),
    };
    assert_eq!(error, original);
}
