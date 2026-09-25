use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use vail_storage::{
    StorageError,
    local_table::{PartitionEvent, WriteOptions, read_local, write_local_stream},
};

fn files(root: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if let Ok(entries) = fs::read_dir(root) {
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                result.extend(files(&path));
            } else if path.extension().is_some_and(|s| s == "vortex") {
                result.push(path);
            }
        }
    }
    result
}
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
}
fn batch(values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(values))]).unwrap()
}
fn values(parts: Vec<Vec<RecordBatch>>) -> Vec<i32> {
    parts
        .iter()
        .flatten()
        .flat_map(|b| {
            let a = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn row_and_byte_limits_flush_before_requesting_more_input() {
    for options in [
        WriteOptions {
            max_buffer_bytes: usize::MAX,
            max_rows_per_file: 2,
        },
        WriteOptions {
            max_buffer_bytes: 1,
            max_rows_per_file: 100,
        },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("table");
        let root_ref = &root;
        let input = async_stream::try_stream! {
            yield PartitionEvent::StartPartition;
            for i in 0..3 {
                // This is executed when the consumer asks for the next batch.
                // Each preceding file must already have been flushed and closed.
                let written = files(root_ref);
                assert_eq!(written.len(), i);
                assert!(written.iter().all(|p| fs::metadata(p).unwrap().len() > 0));
                assert!(!root_ref.join("_vail_table.json").exists());
                yield PartitionEvent::Batch(batch(vec![(2 * i) as i32, (2 * i + 1) as i32]));
            }
            assert_eq!(files(root_ref).len(), 3);
        }
        .boxed();
        write_local_stream(
            root.to_str().unwrap(),
            false,
            schema(),
            input,
            json!([["x", "int32"]]),
            vec!["x".into()],
            options,
        )
        .await
        .unwrap();
        assert_eq!(
            values(read_local(root.to_str().unwrap()).await.unwrap().partitions),
            (0..6).collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn oversized_batches_split_at_row_limit_and_unkeyed_order_is_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("table");
    let input = futures::stream::iter(vec![
        Ok(PartitionEvent::StartPartition), // empty partition
        Ok(PartitionEvent::StartPartition),
        Ok(PartitionEvent::Batch(batch(vec![9, 1, 8, 2, 7, 3, 6]))),
        Ok(PartitionEvent::StartPartition), // empty partition
    ])
    .boxed();
    write_local_stream(
        root.to_str().unwrap(),
        false,
        schema(),
        input,
        json!([["x", "int32"]]),
        vec![],
        WriteOptions {
            max_buffer_bytes: usize::MAX,
            max_rows_per_file: 2,
        },
    )
    .await
    .unwrap();
    assert_eq!(files(&root).len(), 4);
    let read = read_local(root.to_str().unwrap()).await.unwrap();
    assert_eq!(read.partitions.len(), 3);
    assert!(read.partitions[0].is_empty() && read.partitions[2].is_empty());
    assert_eq!(values(read.partitions), vec![9, 1, 8, 2, 7, 3, 6]);
}

#[tokio::test]
async fn input_failure_after_a_flush_never_publishes_a_partial_snapshot() {
    for overwrite in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("table");
        let pointer = root.join("_vail_table.json");
        let before = if overwrite {
            vail_storage::local_table::write_local(
                root.to_str().unwrap(),
                false,
                schema(),
                vec![vec![batch(vec![10, 11])]],
                json!([["x", "int32"]]),
                vec!["x".into()],
            )
            .await
            .unwrap();
            Some(fs::read(&pointer).unwrap())
        } else {
            None
        };
        let root_ref = &root;
        let input = async_stream::try_stream! {
            yield PartitionEvent::StartPartition;
            yield PartitionEvent::Batch(batch(vec![1, 2]));
            assert_eq!(files(root_ref).len(), if overwrite { 2 } else { 1 });
            Err(StorageError::Input("injected after flush".into()))?;
        }
        .boxed();
        let result = write_local_stream(
            root.to_str().unwrap(),
            overwrite,
            schema(),
            input,
            json!([["x", "int32"]]),
            vec!["x".into()],
            WriteOptions {
                max_buffer_bytes: usize::MAX,
                max_rows_per_file: 2,
            },
        )
        .await;
        assert!(matches!(result, Err(StorageError::Input(_))));
        if let Some(before) = before {
            assert_eq!(fs::read(pointer).unwrap(), before);
            assert_eq!(files(&root).len(), 1);
            assert_eq!(
                values(read_local(root.to_str().unwrap()).await.unwrap().partitions),
                vec![10, 11]
            );
            assert!(!root.join(".write.lock").exists());
        } else {
            assert!(!root.exists());
        }
    }
}

#[tokio::test]
async fn invalid_limits_and_existing_destination_do_not_poll_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("table");
    for options in [
        WriteOptions {
            max_buffer_bytes: 0,
            max_rows_per_file: 2,
        },
        WriteOptions {
            max_buffer_bytes: 100,
            max_rows_per_file: 0,
        },
        WriteOptions::default(),
    ] {
        if options.max_buffer_bytes > 0 && options.max_rows_per_file > 0 {
            fs::create_dir(&root).unwrap();
        }
        let input = futures::stream::poll_fn(
            |_| -> std::task::Poll<Option<vail_storage::Result<PartitionEvent>>> {
                panic!("validation must happen before polling input");
            },
        )
        .boxed();
        assert!(
            write_local_stream(
                root.to_str().unwrap(),
                false,
                schema(),
                input,
                json!([["x", "int32"]]),
                vec![],
                options
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn duplicate_and_missing_keys_keep_arrival_order_across_file_rollover() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("table");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, true),
        Field::new("ordinal", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                None,
                None,
                None,
                None,
            ])),
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5])),
        ],
    )
    .unwrap();
    let input = futures::stream::iter(vec![
        Ok(PartitionEvent::StartPartition),
        Ok(PartitionEvent::Batch(batch)),
    ])
    .boxed();
    write_local_stream(
        root.to_str().unwrap(),
        false,
        schema,
        input,
        json!([["key", "int32"], ["ordinal", "int32"]]),
        vec!["key".into()],
        WriteOptions {
            max_buffer_bytes: usize::MAX,
            max_rows_per_file: 2,
        },
    )
    .await
    .unwrap();
    assert_eq!(files(&root).len(), 3);
    let read = read_local(root.to_str().unwrap()).await.unwrap();
    let ordinals = read
        .partitions
        .iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(ordinals, vec![0, 1, 2, 3, 4, 5]);
}
