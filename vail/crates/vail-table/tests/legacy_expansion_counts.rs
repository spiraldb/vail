use arrow_array::{Array, ArrayRef, Int32Array, ListArray, NullArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType, Type};
use vail_table::{TableIR, TableRef};

fn col(name: &str) -> Expr {
    Expr::Column { name: name.into() }
}
fn field(value: Expr, name: &str) -> Expr {
    Expr::GetField {
        value: Arc::new(value),
        name: name.into(),
    }
}
fn source(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> TableRef {
    let list = Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    )) as ArrayRef;
    TableIR::parallelize(RecordBatch::try_from_iter([("items", list)]).unwrap(), 2).unwrap()
}
async fn assert_count(table: TableRef, expected: usize) {
    for _ in 0..2 {
        let batches = table.collect_partitions().await.unwrap();
        assert_eq!(
            batches
                .iter()
                .flatten()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            expected
        );
        assert_eq!(table.force_count().await.unwrap(), expected);
    }
}
fn filtered(table: TableRef, selected: Expr) -> TableRef {
    table
        .filter(
            Expr::IsMissing {
                value: Arc::new(selected),
            },
            false,
        )
        .unwrap()
}
#[tokio::test]
async fn null_array_slots_and_supported_nullable_values_preserve_public_semantics() {
    let table = source(Arc::new(NullArray::new(4)), vec![0, 2, 3, 4], None)
        .explode(vec!["items".into()])
        .unwrap();
    assert_count(table.clone(), 4).await;
    // Arrow Null slots can be expanded, but this frontend does not expose a
    // Hail value type for them. Keep that existing public typing boundary.
    let error = table
        .filter(
            Expr::IsMissing {
                value: Arc::new(col("items")),
            },
            false,
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("unsupported expression type Null"),
        "{error}"
    );
    let table = source(
        Arc::new(Int32Array::from(vec![Some(3), None, Some(7), Some(9)])),
        vec![0, 2, 3, 4],
        Some(vec![true, false, true]),
    )
    .explode(vec!["items".into()])
    .unwrap();
    assert_count(filtered(table, col("items")), 2).await;
}
#[tokio::test]
async fn defined_all_null_structs_are_distinct_from_missing_entries_and_leaves() {
    let values = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(vec![
            None,
            Some(2),
            Some(3),
            Some(4),
        ]))],
        Some(NullBuffer::from(vec![true, false, true, true])),
    )) as ArrayRef;
    let table = source(values, vec![0, 2, 3, 4], Some(vec![true, false, true]))
        .explode(vec!["items".into()])
        .unwrap();
    assert_count(filtered(table.clone(), col("items")), 2).await;
    assert_count(filtered(table, field(col("items"), "x")), 1).await;
}
fn selected(indices: Vec<Option<i32>>, absent_owner: bool) -> TableRef {
    let entries = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(vec![None, Some(2), Some(3)]))],
        Some(NullBuffer::from(vec![true, false, true])),
    )) as ArrayRef;
    let cells = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "value",
            entries.data_type().clone(),
            true,
        ))]
        .into(),
        vec![entries],
        None,
    )) as ArrayRef;
    let table = source(cells, vec![0, 3], absent_owner.then(|| vec![false]));
    let list_type = Type::from_arrow(table.schema().field(0).data_type()).unwrap();
    let Type::Array { array: element } = &list_type else {
        unreachable!()
    };
    let take = Expr::NestedFunction {
        function: "__vail_list_take".into(),
        args: vec![
            col("items"),
            Expr::Literal {
                dtype: Type::Array {
                    array: Box::new(Type::Primitive(PrimitiveType::Int32)),
                },
                value: serde_json::json!(indices),
            },
        ],
        dtype: list_type.clone(),
    };
    let zip = Expr::NestedFunction {
        function: "arrays_zip".into(),
        args: vec![take, col("items")],
        dtype: Type::Array {
            array: Box::new(Type::Struct {
                r#struct: vec![
                    ("1".into(), element.as_ref().clone()),
                    ("2".into(), element.as_ref().clone()),
                ],
            }),
        },
    };
    let table = table
        .map_rows(vec![("items".into(), zip)])
        .unwrap()
        .explode(vec!["items".into()])
        .unwrap();
    filtered(table, field(field(col("items"), "1"), "value"))
}
#[tokio::test]
async fn selected_zip_count_preserves_duplicates_missing_entries_and_nullable_leaves() {
    assert_count(selected(vec![Some(0), Some(0), Some(1), Some(2)], false), 3).await;
    assert_count(selected(vec![], false), 0).await;
}
#[tokio::test]
async fn selected_zip_count_keeps_index_and_owner_errors() {
    for (indices, owner, message) in [
        (vec![Some(0), None], false, "positions must not be missing"),
        (vec![Some(-1)], false, "out of bounds"),
        (vec![Some(3)], false, "out of bounds"),
        (vec![Some(0)], true, "defined arrays and positions"),
    ] {
        let table = selected(indices, owner);
        for _ in 0..2 {
            let error = table.force_count().await.unwrap_err().to_string();
            assert!(error.contains(message), "{error}");
            let error = table.collect_partitions().await.unwrap_err().to_string();
            assert!(error.contains(message), "{error}");
        }
    }
}
#[tokio::test]
async fn forced_expansion_count_still_evaluates_computed_errors() {
    let table = source(Arc::new(Int32Array::from(vec![1, 2])), vec![0, 2], None)
        .map_rows(vec![
            ("items".into(), col("items")),
            (
                "error".into(),
                Expr::Die {
                    message: Arc::new(Expr::Literal {
                        dtype: Type::Primitive(PrimitiveType::Str),
                        value: serde_json::json!("retained upstream failure"),
                    }),
                    dtype: Type::Primitive(PrimitiveType::Int32),
                },
            ),
        ])
        .unwrap()
        .explode(vec!["items".into()])
        .unwrap();
    for _ in 0..2 {
        assert!(
            table
                .force_count()
                .await
                .unwrap_err()
                .to_string()
                .contains("retained upstream failure")
        );
    }
}
