//! Public ALL behavior through list-reducer and streaming scan paths.
use arrow_array::{Array, BooleanArray, Int32Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{PrimitiveType, Type};
use vail_table::{ScalarIR, TableIR};

#[tokio::test]
async fn list_all_uses_native_bool_and_for_empty_and_nullable_lists() {
    let field = Arc::new(Field::new("item", DataType::Boolean, true));
    let lists = ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 0, 2, 5, 5])),
        Arc::new(BooleanArray::from(vec![
            None,
            None,
            Some(true),
            None,
            Some(false),
        ])),
        Some(NullBuffer::from(vec![true, true, true, false])),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "values",
        lists.data_type().clone(),
        true,
    )]));
    let rows = RecordBatch::try_new(schema, vec![Arc::new(lists)]).unwrap();
    let source = TableIR::parallelize(rows, 1).unwrap();
    let result = source
        .map_rows(vec![(
            "all_values".into(),
            ScalarIR::NestedFunction {
                function: "__vail_list_reduce_all".into(),
                args: vec![ScalarIR::Column {
                    name: "values".into(),
                }],
                dtype: Type::Primitive(PrimitiveType::Bool),
            },
        )])
        .unwrap()
        .collect_partitions()
        .await
        .unwrap();
    let result = result.into_iter().flatten().next().unwrap();
    let values = result
        .column_by_name("all_values")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(true), Some(true), Some(false), None]
    );
}

#[tokio::test]
async fn all_scan_uses_true_identity_across_streamed_storage_batches() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("all-scan");
    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int32, false),
        Field::new("value", DataType::Boolean, true),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
        ],
    )
    .unwrap();
    TableIR::parallelize(rows, 2)
        .unwrap()
        .key_by(vec!["idx".into()])
        .unwrap()
        .write(path.to_str().unwrap(), false)
        .await
        .unwrap();
    let scanned = TableIR::read(path.to_str().unwrap())
        .await
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "prefix_all".into(),
                ScalarIR::Aggregate {
                    result_type: Some(Type::Primitive(PrimitiveType::Bool)),
                    function: "all".into(),
                    value: Some(Arc::new(ScalarIR::Column {
                        name: "value".into(),
                    })),
                    options: json!({"__vail_scan": true}),
                    init: vec![],
                },
            ),
        ])
        .unwrap();
    let partitions = scanned.collect_partitions().await.unwrap();
    let mut observed = Vec::new();
    for batch in partitions.into_iter().flatten() {
        let indices = batch
            .column_by_name("idx")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("prefix_all")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        observed.extend((0..batch.num_rows()).map(|row| {
            assert!(!values.is_null(row));
            (indices.value(row), values.value(row))
        }));
    }
    observed.sort_unstable_by_key(|(index, _)| *index);
    assert_eq!(observed, vec![(0, true), (1, true), (2, true), (3, false)]);
}
