use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use vail_table::{TableIR, TableJoinKind};

fn table(key: &str, value: &str, keys: Vec<Option<i32>>, values: Vec<i32>) -> vail_table::TableRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new(key, DataType::Int32, true),
        Field::new(value, DataType::Int32, true),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap();
    TableIR::parallelize(rows, 2)
        .unwrap()
        .key_by(vec![key.into()])
        .unwrap()
}

#[tokio::test]
async fn join_modes_preserve_cartesian_duplicates_and_replay() {
    let left = table("k", "l", vec![Some(1), Some(1), None], vec![10, 11, 12]);
    let right = table("rk", "r", vec![Some(1), Some(1), None], vec![20, 21, 22]);
    for (kind, count) in [
        (TableJoinKind::Inner, 4),
        (TableJoinKind::Left, 5),
        (TableJoinKind::Right, 5),
        (TableJoinKind::Outer, 6),
    ] {
        let joined = left.join(right.clone(), kind, 1).unwrap();
        assert_eq!(joined.known_count(), None);
        assert_eq!(joined.explain()["node"], "TableJoin");
        joined.physical_plan().unwrap();
        assert!(joined.logical_plan().unwrap().contains("Join"));
        assert_eq!(joined.count().await.unwrap(), count);
        assert_eq!(joined.count().await.unwrap(), count);
    }
}

#[tokio::test]
async fn membership_joins_preserve_independent_left_partitions() {
    let left = table(
        "k",
        "l",
        vec![Some(1), Some(2), None, Some(1)],
        vec![10, 11, 12, 13],
    );
    let right = table("rk", "r", vec![Some(1), Some(1), None], vec![20, 21, 22]);
    for kind in [TableJoinKind::Semi, TableJoinKind::Anti] {
        let joined = left.join(right.clone(), kind, 1).unwrap();
        assert_eq!(
            joined.n_partitions().await.unwrap(),
            left.n_partitions().await.unwrap()
        );
        assert_eq!(joined.count().await.unwrap(), 2);
        assert_eq!(joined.schema(), left.schema());
    }
}

#[tokio::test]
async fn join_reads_are_lazy_and_replayed_after_storage_returns() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let moved = directory.path().join("moved");
    TableIR::range(12, 3)
        .unwrap()
        .write(path.to_str().unwrap(), false)
        .await
        .unwrap();
    let source = TableIR::read(path.to_str().unwrap()).await.unwrap();
    std::fs::rename(&path, &moved).unwrap();
    let joined = source
        .join(TableIR::range(12, 4).unwrap(), TableJoinKind::Outer, 1)
        .unwrap();
    assert_eq!(joined.head(0).count().await.unwrap(), 0);
    assert!(joined.count().await.is_err());
    std::fs::rename(&moved, &path).unwrap();
    assert_eq!(joined.count().await.unwrap(), 12);
    std::fs::rename(&path, &moved).unwrap();
    assert!(joined.count().await.is_err());
}
#[test]
fn join_key_admission_rejects_overshoot() {
    let left = TableIR::range(2, 1).unwrap();
    assert!(left.join(left.clone(), TableJoinKind::Inner, 2).is_err());
}

#[tokio::test]
async fn prepared_zip_actions_reread_after_success_and_failure() {
    use vail_table::partitioned;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let moved = directory.path().join("moved");
    TableIR::range(12, 3)
        .unwrap()
        .write(path.to_str().unwrap(), false)
        .await
        .unwrap();
    let source = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let zipped = TableIR::multi_way_zip_join(
        vec![source.clone(), source],
        "data".into(),
        "globals".into(),
    )
    .unwrap();
    let prepared = partitioned::prepare(zipped).await.unwrap();
    for missing in [false, true, false, true, false] {
        if missing {
            std::fs::rename(&path, &moved).unwrap();
        } else if moved.exists() {
            std::fs::rename(&moved, &path).unwrap();
        }
        let result = datafusion_physical_plan::collect(
            prepared.start_action().unwrap(),
            Arc::new(datafusion_execution::TaskContext::default()),
        )
        .await;
        if missing {
            assert!(result.is_err());
        } else {
            assert_eq!(
                result
                    .unwrap()
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>(),
                12
            );
        }
    }
}

#[tokio::test]
async fn wide_zip_replay_survives_cancelled_reader() {
    use futures::TryStreamExt;
    use vail_table::partitioned;
    let count = 2048;
    let values = (0..count)
        .map(|i| format!("{i}:{}", "x".repeat(8192)))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(0..count)),
            Arc::new(arrow_array::StringArray::from(values)),
        ],
    )
    .unwrap();
    let source = TableIR::parallelize(batch, 1)
        .unwrap()
        .key_by(vec!["k".into()])
        .unwrap();
    let zipped = TableIR::multi_way_zip_join(
        vec![source.clone(), source],
        "data".into(),
        "globals".into(),
    )
    .unwrap();
    let prepared = partitioned::prepare(zipped).await.unwrap();
    let context = Arc::new(datafusion_execution::TaskContext::default());
    let action = prepared.start_action().unwrap();
    let mut reader = action.execute(0, context.clone()).unwrap();
    // A reader can abandon its demand while another shared consumer still
    // requires the same batches, including an in-flight spill write.
    let _ = tokio::time::timeout(std::time::Duration::from_millis(1), reader.try_next()).await;
    drop(reader);
    let actual = datafusion_physical_plan::collect(action, context)
        .await
        .unwrap();
    assert_eq!(
        actual.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        count as usize
    );
    let mut keys = Vec::new();
    for batch in actual {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let data = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = k.value(row);
            keys.push(key);
            let slots = data.value(row);
            let slots = slots
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap();
            let values = slots
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            assert_eq!(values.len(), 2);
            let expected = format!("{key}:{}", "x".repeat(8192));
            for slot in 0..2 {
                assert!(
                    values.value(slot) == expected,
                    "payload mismatch for key {key}, slot {slot}"
                );
            }
        }
    }
    keys.sort_unstable();
    assert_eq!(keys, (0..count).collect::<Vec<_>>());
}
