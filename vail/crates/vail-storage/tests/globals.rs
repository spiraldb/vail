//! Public global IPC and snapshot writer boundaries.
use arrow_array::{Int32Array, RecordBatch, StringArray, StringViewArray};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::{fs::File, sync::Arc};
use vail_storage::{
    globals,
    local_table::{WriteGlobals, WriteOptions, write_local_partition_streams_with_globals},
};
fn integers(values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("g", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(values))],
    )
    .unwrap()
}
#[test]
fn singleton_ipc_accepts_valid_values_and_rejects_invalid_rows_or_batch_counts() {
    let directory = tempfile::tempdir().unwrap();
    let batch = integers(vec![7]);
    let pointer = globals::write(directory.path(), &batch).unwrap();
    assert_eq!(globals::read(&pointer).unwrap(), batch);
    for rows in [vec![], vec![1, 2]] {
        let bad = integers(rows);
        assert!(globals::write(&directory.path().join("bad"), &bad).is_err());
        let path = directory.path().join(format!("rows{}", bad.num_rows()));
        let mut writer = FileWriter::try_new(File::create(&path).unwrap(), &bad.schema()).unwrap();
        writer.write(&bad).unwrap();
        writer.finish().unwrap();
        assert!(
            globals::read(
                &json!({"format":"arrow-ipc-v1","path":format!("file://{}",path.display())})
            )
            .unwrap_err()
            .to_string()
            .contains("one row")
        );
    }
    for count in [0, 1, 2] {
        let path = directory.path().join(format!("ipc{count}"));
        let mut writer =
            FileWriter::try_new(File::create(&path).unwrap(), &batch.schema()).unwrap();
        for _ in 0..count {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        let result = globals::read(
            &json!({"format":"arrow-ipc-v1","path":format!("file://{}",path.display())}),
        );
        if count == 1 {
            assert_eq!(result.unwrap(), batch);
        } else {
            assert!(result.unwrap_err().to_string().contains("one batch"));
        }
    }
    let path = directory.path().join("corrupt");
    std::fs::write(&path, b"corrupt").unwrap();
    assert!(
        globals::read(&json!({"format":"arrow-ipc-v1","path":format!("file://{}",path.display())}))
            .is_err()
    );
}
#[tokio::test]
async fn snapshot_writer_rejects_globals_disagreeing_with_advertised_schema_before_publication() {
    let directory = tempfile::tempdir().unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("h", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["7"]))],
    )
    .unwrap();
    for (index, logical_type) in [
        json!([["g", "int32"]]),
        json!([["g", "str"]]),
        json!([["h", "int32"]]),
        json!([["h", "str"], ["h", "str"]]),
        json!([["", "str"]]),
    ]
    .into_iter()
    .enumerate()
    {
        let path = directory.path().join(format!("bad{index}"));
        let result = write_local_partition_streams_with_globals(
            path.to_str().unwrap(),
            false,
            integers(vec![]).schema(),
            vec![],
            json!([["g", "int32"]]),
            vec![],
            WriteOptions::default(),
            Some(WriteGlobals {
                logical_type,
                batch: batch.clone(),
            }),
        )
        .await;
        assert!(result.is_err());
        assert!(!path.exists());
    }
}
#[test]
fn global_ipc_keeps_view_strings_and_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("text", DataType::Utf8View, false)
                .with_metadata([("custom".into(), "value".into())].into()),
        ],
        [("source".into(), "test".into())].into(),
    ));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringViewArray::from(vec!["β"]))]).unwrap();
    let pointer = globals::write(directory.path(), &batch).unwrap();
    assert_eq!(globals::read(&pointer).unwrap(), batch);
}
