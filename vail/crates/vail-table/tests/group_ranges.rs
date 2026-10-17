//! Public grouped actions over persisted, metadata-certified key partitions.
use arrow_array::{Array, Int32Array, Int64Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_storage::local_table::WriteOptions;
use vail_table::{ScalarIR, TableIR, TableRef, globals::GlobalBindings};

fn col(name: &str) -> ScalarIR {
    ScalarIR::Column { name: name.into() }
}
fn aggregate(function: &str, value: Option<&str>) -> ScalarIR {
    ScalarIR::Aggregate {
        result_type: None,
        function: function.into(),
        value: value.map(|value| Arc::new(col(value))),
        options: json!({}),
        init: vec![],
    }
}
fn grouped(table: &TableRef, names: &[&str]) -> TableRef {
    table
        .grouped_aggregate_bound(
            names
                .iter()
                .map(|name| (format!("out_{name}"), col(name)))
                .collect(),
            vec![
                ("values".into(), aggregate("collect", Some("value"))),
                ("total".into(), aggregate("sum", Some("value"))),
                ("n".into(), aggregate("count", None)),
            ],
            GlobalBindings::new(),
        )
        .unwrap()
}
async fn stored(
    keys: Vec<i32>,
    subkeys: Vec<i32>,
    file_rows: usize,
) -> (tempfile::TempDir, TableRef) {
    let n = keys.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("sub", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(subkeys)),
            Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let table = TableIR::parallelize(rows, 1)
        .unwrap()
        .with_keys(vec!["key".into(), "sub".into()])
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("table");
    table
        .write_with_options(
            path.to_str().unwrap(),
            false,
            WriteOptions {
                max_rows_per_file: file_rows,
                max_buffer_bytes: usize::MAX,
            },
        )
        .await
        .unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();
    (temp, read)
}
async fn check(table: &TableRef, names: &[&str], expected: &[(i32, Vec<i32>)]) {
    let grouped = grouped(table, names);
    let mut actual = Vec::new();
    for batch in grouped
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
    {
        let keys = batch
            .column_by_name(&format!("out_{}", names[0]))
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let lists = batch
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let sums = batch
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let array = lists.value(row);
            let mut values = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();
            values.sort();
            assert_eq!(
                sums.value(row),
                values.iter().map(|v| *v as i64).sum::<i64>()
            );
            assert_eq!(counts.value(row), values.len() as i64);
            actual.push((keys.value(row), values));
        }
    }
    actual.sort();
    assert_eq!(actual, expected);
}
#[tokio::test]
async fn disjoint_multifile_groups_stream_and_preserve_duplicate_rows() {
    let (_temp, table) = stored(vec![0, 0, 1, 1, 2, 2, 3, 3], vec![0; 8], 4).await;
    check(
        &table,
        &["key"],
        &[
            (0, vec![0, 1]),
            (1, vec![2, 3]),
            (2, vec![4, 5]),
            (3, vec![6, 7]),
        ],
    )
    .await;
}
#[tokio::test]
async fn equal_prefix_boundaries_fall_back_and_merge_all_values() {
    let (_temp, table) = stored(vec![0, 0, 0, 0, 1, 1], vec![0, 1, 2, 3, 0, 1], 3).await;
    check(&table, &["key"], &[(0, vec![0, 1, 2, 3]), (1, vec![4, 5])]).await;
}
#[tokio::test]
async fn compound_key_ranges_accept_equal_first_components() {
    let (_temp, table) = stored(vec![0, 0, 0, 0, 1, 1], vec![0, 1, 2, 3, 0, 1], 3).await;
    check(
        &table,
        &["key", "sub"],
        &[
            (0, vec![0]),
            (0, vec![1]),
            (0, vec![2]),
            (0, vec![3]),
            (1, vec![4]),
            (1, vec![5]),
        ],
    )
    .await;
}
#[tokio::test]
async fn filters_keep_source_partition_assignment_and_empty_output_is_valid() {
    let (_temp, table) = stored(vec![0, 0, 1, 1, 2, 2, 3, 3], vec![0; 8], 4).await;
    let table = table
        .filter(
            ScalarIR::Binary {
                operator: BinaryOperator::Gt,
                left: Arc::new(col("value")),
                right: Arc::new(ScalarIR::Literal {
                    dtype: Type::Primitive(PrimitiveType::Int32),
                    value: json!(3),
                }),
            },
            true,
        )
        .unwrap();
    check(&table, &["key"], &[(2, vec![4, 5]), (3, vec![6, 7])]).await;
    let empty = table
        .filter(
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Bool),
                value: json!(false),
            },
            true,
        )
        .unwrap();
    assert_eq!(grouped(&empty, &["key"]).count().await.unwrap(), 0);
}
#[tokio::test]
async fn range_sources_use_the_same_native_aggregation() {
    let table = TableIR::range(12, 3)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), col("idx")),
            ("value".into(), col("idx")),
        ])
        .unwrap();
    let expected = (0..12).map(|v| (v, vec![v])).collect::<Vec<_>>();
    check(&table, &["idx"], &expected).await;
}

async fn check_key_array(keys: arrow_array::ArrayRef) {
    use datafusion_common::ScalarValue;
    let n = keys.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", keys.data_type().clone(), true),
        Field::new("value", DataType::Int32, false),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            keys.clone(),
            Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let table = TableIR::parallelize(rows, 1)
        .unwrap()
        .with_keys(vec!["key".into()])
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("keys");
    table
        .write_with_options(
            path.to_str().unwrap(),
            false,
            WriteOptions {
                max_rows_per_file: 2,
                max_buffer_bytes: usize::MAX,
            },
        )
        .await
        .unwrap();
    let table = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let keys = arrow_cast::cast(
        &keys,
        table.schema().field_with_name("key").unwrap().data_type(),
    )
    .unwrap();
    let grouped = grouped(&table, &["key"]);
    let mut actual = Vec::new();
    for batch in grouped
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
    {
        assert_eq!(
            batch.column_by_name("out_key").unwrap().data_type(),
            keys.data_type()
        );
        let lists = batch
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let values = lists.value(row);
            let mut values = values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();
            values.sort();
            actual.push((
                values,
                ScalarValue::try_from_array(batch.column_by_name("out_key").unwrap(), row).unwrap(),
            ));
        }
    }
    actual.sort_by(|left, right| left.0.cmp(&right.0));
    let expected = (0..n)
        .step_by(2)
        .map(|i| {
            (
                vec![i as i32, i as i32 + 1],
                ScalarValue::try_from_array(&keys, i).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
#[tokio::test]
async fn persisted_null_keys_sort_last_and_group_all_occurrences() {
    check_key_array(Arc::new(Int32Array::from(vec![
        Some(0),
        Some(0),
        Some(1),
        Some(1),
        None,
        None,
    ])))
    .await;
}
#[tokio::test]
async fn persisted_strings_and_nullable_struct_keys_keep_their_values() {
    use arrow_array::{StringArray, StructArray};
    use arrow_buffer::NullBuffer;
    check_key_array(Arc::new(StringArray::from(vec![
        Some("a"),
        Some("a"),
        Some("b"),
        Some("b"),
        None,
        None,
    ])))
    .await;
    let fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, true)),
        Arc::new(Field::new("position", DataType::Int32, true)),
    ];
    let key = StructArray::new(
        fields.into(),
        vec![
            Arc::new(StringArray::from(vec![
                "a", "a", "b", "b", "c", "c", "z", "z",
            ])),
            Arc::new(Int32Array::from(vec![0, 0, 1, 1, 2, 2, 3, 3])),
        ],
        Some(NullBuffer::from(vec![
            true, true, true, true, true, true, false, false,
        ])),
    );
    check_key_array(Arc::new(key)).await;
}
#[tokio::test]
async fn empty_source_partitions_use_fallback_without_losing_empty_semantics() {
    let table = TableIR::range(0, 3)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), col("idx")),
            ("value".into(), col("idx")),
        ])
        .unwrap();
    assert_eq!(grouped(&table, &["idx"]).count().await.unwrap(), 0);
}

#[tokio::test]
async fn persisted_globals_join_preserves_grouped_values() {
    let table = TableIR::range(8, 2)
        .unwrap()
        .map_globals(vec![(
            "offset".into(),
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: json!(10),
            },
        )])
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("globals");
    table.write(path.to_str().unwrap(), false).await.unwrap();
    let table = TableIR::read(path.to_str().unwrap())
        .await
        .unwrap()
        .map_rows(vec![
            ("idx".into(), col("idx")),
            (
                "value".into(),
                ScalarIR::Binary {
                    operator: BinaryOperator::Add,
                    left: Arc::new(col("idx")),
                    right: Arc::new(ScalarIR::Global {
                        name: "offset".into(),
                    }),
                },
            ),
        ])
        .unwrap();
    check(
        &table,
        &["idx"],
        &(0..8).map(|i| (i, vec![i + 10])).collect::<Vec<_>>(),
    )
    .await;
}

#[tokio::test]
async fn persisted_call_keys_normalize_iceberg_widened_bound_children() {
    use arrow_array::{BooleanArray, StructArray, UInt8Array, UInt16Array};
    let dtype = Type::Primitive(PrimitiveType::Call).to_arrow().unwrap();
    let DataType::Struct(fields) = dtype else {
        panic!("Call storage must be a struct");
    };
    let keys = StructArray::new(
        fields,
        vec![
            Arc::new(UInt16Array::from(vec![0, 0, 0, 0, 1, 1])),
            Arc::new(UInt16Array::from(vec![0, 0, 1, 1, 1, 1])),
            Arc::new(UInt8Array::from(vec![2; 6])),
            Arc::new(BooleanArray::from(vec![false; 6])),
        ],
        None,
    );
    check_key_array(Arc::new(keys)).await;
}
#[tokio::test]
async fn alternate_string_storage_preserves_persisted_group_keys() {
    use arrow_array::{LargeStringArray, StringViewArray};
    let values = vec![Some("a"), Some("a"), Some("b"), Some("b"), None, None];
    check_key_array(Arc::new(LargeStringArray::from(values.clone()))).await;
    check_key_array(Arc::new(StringViewArray::from(values))).await;
}
