use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::Schema;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, Type, project_batch};

#[test]
fn tuple_construction_and_projection_preserve_values_and_missingness() {
    let tuple: Expr = serde_json::from_value(json!({
        "op":"tuple", "elements":[
            {"op":"column","name":"x"},
            {"op":"literal","dtype":"str","value":"value"}
        ]
    }))
    .unwrap();
    let source = RecordBatch::try_from_iter([(
        "x",
        Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
    )])
    .unwrap();
    let element: Expr = serde_json::from_value(json!({
        "op":"get_field","value":tuple,"name":"__vail_tuple_0"
    }))
    .unwrap();
    let result = project_batch(&source, &[("element".into(), element)]).unwrap();
    let values = result
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), 1);
    assert!(values.is_null(1));
}

#[test]
fn empty_tuple_has_a_distinct_arrow_type_and_executes_for_each_row() {
    let tuple: Expr = serde_json::from_value(json!({"op":"tuple","elements":[]})).unwrap();
    let schema = Schema::empty();
    let dtype = tuple.dtype(&schema).unwrap();
    assert_eq!(dtype, Type::Tuple { tuple: vec![] });
    assert_eq!(Type::from_arrow(&dtype.to_arrow().unwrap()).unwrap(), dtype);
    assert_ne!(
        dtype.to_arrow().unwrap(),
        Type::Struct { r#struct: vec![] }.to_arrow().unwrap()
    );
    let source =
        RecordBatch::try_from_iter([("x", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef)])
            .unwrap();
    let result = project_batch(&source, &[("tuple".into(), tuple)]).unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.column(0).null_count(), 0);
    assert_eq!(
        Type::from_arrow(result.column(0).data_type()).unwrap(),
        dtype
    );
}
