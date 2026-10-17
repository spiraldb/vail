use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_table::{ScalarIR, TableIR, globals::GlobalBindings};

fn collect_expr(field: &str) -> ScalarIR {
    ScalarIR::Aggregate {
        result_type: None,
        function: "collect".into(),
        value: Some(Arc::new(ScalarIR::Column { name: field.into() })),
        options: json!({}),
        init: vec![],
    }
}

fn grouped(rows: RecordBatch, value: &str) -> vail_table::TableRef {
    TableIR::parallelize(rows, 2)
        .unwrap()
        .grouped_aggregate_bound(
            vec![("key".into(), ScalarIR::Column { name: "key".into() })],
            vec![("values".into(), collect_expr(value))],
            GlobalBindings::new(),
        )
        .unwrap()
}

#[tokio::test]
async fn collect_group_accumulator_preserves_nullable_nested_values_and_duplicates() {
    let nested = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", nested, true),
    ]));
    let values = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(7), None]),
        None,
        Some(vec![Some(9)]),
        Some(vec![Some(7), None]),
        Some(vec![]),
        Some(vec![]),
    ]);
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0, 1])),
            Arc::new(values),
        ],
    )
    .unwrap();
    let batches = grouped(rows, "value").collect_partitions().await.unwrap();
    let mut groups = Vec::new();
    for batch in batches.into_iter().flatten() {
        let keys = batch
            .column_by_name("key")
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
        for row in 0..batch.num_rows() {
            assert!(
                lists.is_valid(row),
                "collect must represent an empty group as [] rather than null"
            );
            groups.push((keys.value(row), lists.value(row)));
        }
    }
    groups.sort_by_key(|(key, _)| *key);
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].0, 0);
    let first = groups.remove(0).1;
    let first = first.as_any().downcast_ref::<ListArray>().unwrap();
    let mut actual = Vec::new();
    for row in 0..first.len() {
        if first.is_null(row) {
            actual.push(None);
        } else {
            let item = first.value(row);
            let item = item.as_any().downcast_ref::<Int32Array>().unwrap();
            actual.push(Some(item.iter().collect::<Vec<_>>()));
        }
    }
    actual.sort();
    assert_eq!(
        actual,
        vec![
            None,
            Some(vec![]),
            Some(vec![Some(7), None]),
            Some(vec![Some(7), None]),
            Some(vec![Some(9)]),
        ]
    );
    assert_eq!(groups[0].0, 1);
    let second = groups.remove(0).1;
    assert_eq!(
        second
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_length(0),
        0
    );
}

#[tokio::test]
async fn collect_group_accumulator_handles_empty_struct_values() {
    let value_type = DataType::Struct(Default::default());
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", value_type, false),
    ]));
    let values: ArrayRef = Arc::new(StructArray::new_empty_fields(3, None));
    let rows = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![0, 0, 1])), values],
    )
    .unwrap();
    let batches = grouped(rows, "value").collect_partitions().await.unwrap();
    let mut sizes = Vec::new();
    for batch in batches.into_iter().flatten() {
        let lists = batch
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..lists.len() {
            assert!(lists.is_valid(row));
            sizes.push(lists.value_length(row));
        }
    }
    sizes.sort_unstable();
    assert_eq!(sizes, [1, 2]);
}

#[tokio::test]
async fn filtered_collect_groups_emit_valid_empty_lists() {
    use vail_execution::expressions::{PrimitiveType, Type};
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::Int32, true),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 0, 1])),
            Arc::new(Int32Array::from(vec![Some(5), None, Some(8)])),
        ],
    )
    .unwrap();
    let filtered = ScalarIR::AggFilter {
        predicate: Arc::new(ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Bool),
            value: json!(false),
        }),
        body: Arc::new(collect_expr("value")),
    };
    let table = TableIR::parallelize(rows, 2)
        .unwrap()
        .grouped_aggregate_bound(
            vec![("key".into(), ScalarIR::Column { name: "key".into() })],
            vec![("values".into(), filtered)],
            GlobalBindings::new(),
        )
        .unwrap();
    let batches = table.collect_partitions().await.unwrap();
    let mut seen = 0;
    for batch in batches.into_iter().flatten() {
        let lists = batch
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..lists.len() {
            seen += 1;
            assert!(lists.is_valid(row));
            assert_eq!(lists.value_length(row), 0);
        }
    }
    assert_eq!(seen, 2);
}
