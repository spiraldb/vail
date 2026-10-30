use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, types::Int32Type,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, project_batch};
fn select(values: ArrayRef, indices: Int32Array) -> Result<ArrayRef, String> {
    let batch =
        RecordBatch::try_from_iter([("values", values), ("index", Arc::new(indices) as ArrayRef)])
            .unwrap();
    let expression: Expr = serde_json::from_value(json!({"op":"array_index", "value":{"op":"column","name":"values"},"index":{"op":"column","name":"index"}})).unwrap();
    project_batch(&batch, &[("selected".into(), expression)])
        .map(|r| r.column(0).clone())
        .map_err(|e| e.to_string())
}
#[test]
fn missing_values_negative_indices_and_bounds() {
    let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(10)]),
        Some(vec![None]),
        None,
        Some(vec![]),
        Some(vec![Some(40), Some(41)]),
        Some(vec![Some(50), Some(51)]),
    ]);
    let output = select(
        Arc::new(values),
        Int32Array::from(vec![Some(0), Some(-1), Some(999), None, Some(-1), Some(0)]),
    )
    .unwrap();
    assert_eq!(
        output.as_any().downcast_ref::<Int32Array>().unwrap(),
        &Int32Array::from(vec![Some(10), None, None, None, Some(41), Some(50)])
    );
    for index in [-3, 2] {
        let values =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1), Some(2)])]);
        assert!(
            select(Arc::new(values), Int32Array::from(vec![index]))
                .unwrap_err()
                .contains("out of bounds")
        );
    }
}
#[test]
fn sliced_strings_preserve_offsets_and_child_nulls() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("unused"),
        Some("one"),
        None,
        Some("three"),
        Some("tail"),
    ]));
    let values = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3, 4, 5])),
        strings,
        None,
    )
    .slice(1, 3);
    let output = select(
        Arc::new(values.clone()),
        Int32Array::from(vec![99, 0, -1, 0, 99]).slice(1, 3),
    )
    .unwrap();
    assert_eq!(
        output.as_any().downcast_ref::<StringArray>().unwrap(),
        &StringArray::from(vec![Some("one"), None, Some("three")])
    );
    assert_eq!(
        select(
            Arc::new(values.slice(0, 0)),
            Int32Array::from(Vec::<i32>::new())
        )
        .unwrap()
        .len(),
        0
    );
}
#[test]
fn missing_parent_hides_nonempty_child_span() {
    let values = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3, 4])),
        Arc::new(Int32Array::from(vec![10, 998, 999, 30])),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let output = select(Arc::new(values), Int32Array::from(vec![0, 0, 0])).unwrap();
    assert_eq!(
        output.as_any().downcast_ref::<Int32Array>().unwrap(),
        &Int32Array::from(vec![Some(10), None, Some(30)])
    );
}
#[test]
fn scalar_list_supports_reordered_and_repeated_indices() {
    let batch = RecordBatch::try_from_iter([(
        "index",
        Arc::new(Int32Array::from(vec![1, 0, -1, 1])) as ArrayRef,
    )])
    .unwrap();
    let expression: Expr=serde_json::from_value(json!({"op":"array_index","value":{"op":"literal","dtype":{"array":"str"},"value":["a","b","c"]},"index":{"op":"column","name":"index"}})).unwrap();
    let result = project_batch(&batch, &[("selected".into(), expression)]).unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec!["b", "a", "c", "b"])
    );
}
