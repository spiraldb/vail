//! A snapshot reader can outlive the runtime which first polled it.
use std::sync::{Arc, Barrier};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use serde_json::json;
use vail_storage::local_table::{LocalRead, PartitionEvent, open_local, write_local};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn fixture(path: &str) -> LocalRead {
    runtime().block_on(async {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let partitions = (0..4)
            .map(|p| {
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int32Array::from(vec![
                            Some(p * 10),
                            None,
                            Some(p * 10 + 2),
                        ]))],
                    )
                    .unwrap(),
                ]
            })
            .collect();
        write_local(
            path,
            false,
            schema,
            partitions,
            json!([["x", "int32"]]),
            vec![],
        )
        .await
        .unwrap();
        open_local(path).await.unwrap()
    })
}

async fn assert_rows(source: &LocalRead) {
    for partition in 0..4 {
        let events = source
            .partition_batches(partition)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let got = events
            .iter()
            .filter_map(|event| match event {
                PartitionEvent::Batch(batch) => Some(batch),
                PartitionEvent::StartPartition => None,
            })
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            got,
            vec![
                Some(partition as i32 * 10),
                None,
                Some(partition as i32 * 10 + 2)
            ]
        );
    }
}

#[test]
fn reopened_snapshot_reads_after_previous_runtimes_are_destroyed() {
    let dir = tempfile::tempdir().unwrap();
    let source = fixture(dir.path().join("native").to_str().unwrap());
    // Opening the source used a runtime which is already destroyed. Every
    // subsequent read creates and drops another, retaining the same source.
    for _ in 0..4 {
        runtime().block_on(assert_rows(&source));
    }
}

#[test]
fn cloned_snapshot_reads_on_concurrent_independent_runtimes() {
    let dir = tempfile::tempdir().unwrap();
    let source = fixture(dir.path().join("native").to_str().unwrap());
    let barrier = Arc::new(Barrier::new(2));
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let source = source.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                let runtime = runtime();
                for _ in 0..4 {
                    barrier.wait();
                    runtime.block_on(assert_rows(&source));
                }
            });
        }
    });
    runtime().block_on(assert_rows(&source));
}
