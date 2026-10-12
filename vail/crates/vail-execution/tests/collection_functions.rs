use arrow_array::{Array, Int32Array, ListArray, RecordBatch};
use arrow_schema::Schema;
use serde_json::json;
use vail_execution::expressions::{Expr, lower, project_batch};

#[test]
fn set_constructor_executes_datafusion_distinct() {
    let expr: Expr = serde_json::from_value(json!({
        "op": "nested_function", "function": "array_distinct", "dtype": {"set": "int32"},
        "args": [{"op": "literal", "dtype": {"array": "int32"}, "value": [2, 1, 2, null, null]}]
    }))
    .unwrap();
    assert!(
        lower(&expr, &Schema::empty())
            .unwrap()
            .to_string()
            .contains("array_distinct")
    );
    let source = RecordBatch::try_from_iter([(
        "x",
        std::sync::Arc::new(Int32Array::from(vec![0])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let output = project_batch(&source, &[("s".into(), expr)]).unwrap();
    let list = output
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = list.value(0);
    assert_eq!(values.len(), 3);
    assert_eq!(values.null_count(), 1);
}

#[test]
fn malformed_collection_calls_are_planning_errors() {
    for (function, args, dtype) in [
        ("array_distinct", json!([]), json!({"set": "int32"})),
        ("array_union", json!([]), json!({"set": "int32"})),
        ("unknown", json!([]), json!("int32")),
        (
            "array_distinct",
            json!([{"op":"literal","dtype":"int32","value":1}]),
            json!({"set":"int32"}),
        ),
        (
            "array_distinct",
            json!([{"op":"literal","dtype":{"array":"int32"},"value":[1]}]),
            json!({"set":"str"}),
        ),
        (
            "array_element",
            json!([
                {"op":"literal","dtype":{"array":"int32"},"value":[1]},
                {"op":"literal","dtype":"int32","value":1}
            ]),
            json!("int32"),
        ),
    ] {
        let expr: Expr = serde_json::from_value(json!({
            "op":"nested_function", "function":function, "args":args, "dtype":dtype,
        }))
        .unwrap();
        assert!(
            expr.dtype(&Schema::empty()).is_err(),
            "{function} accepted malformed arguments"
        );
    }
}

#[test]
fn collection_tag_cast_cannot_change_element_storage() {
    let expr: Expr = serde_json::from_value(json!({
        "op":"collection_cast", "dtype":{"set":"str"},
        "value":{"op":"literal","dtype":{"array":"int32"},"value":[1]}
    }))
    .unwrap();
    assert!(expr.dtype(&Schema::empty()).is_err());
}

#[test]
fn distinct_rejects_float_equivalence_mismatch() {
    let expr: Expr = serde_json::from_value(json!({
        "op":"nested_function", "function":"array_distinct", "dtype":{"set":"float64"},
        "args":[{"op":"literal","dtype":{"array":"float64"},"value":[0.0]}]
    }))
    .unwrap();
    assert!(
        expr.dtype(&Schema::empty())
            .unwrap_err()
            .to_string()
            .contains("key equality")
    );
}
