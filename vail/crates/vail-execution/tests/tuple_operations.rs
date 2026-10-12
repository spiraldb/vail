use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::Schema;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, Type, lower, project_batch};

fn contains_function(expr: &Arc<dyn PhysicalExpr>, name: &str) -> bool {
    expr.downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|f| f.name() == name)
        || expr
            .children()
            .iter()
            .any(|child| contains_function(child, name))
}

#[test]
fn tuple_construction_and_projection_use_datafusion_functions() {
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
    let plan = lower(&tuple, &source.schema()).unwrap();
    assert!(contains_function(&plan, "named_struct"));
    let element: Expr = serde_json::from_value(json!({
        "op":"get_field","value":tuple,"name":"__vail_tuple_0"
    }))
    .unwrap();
    let plan = lower(&element, &source.schema()).unwrap();
    assert!(contains_function(&plan, "get_field"));
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
fn empty_tuple_has_a_distinct_arrow_type_and_builtin_constructor() {
    let tuple: Expr = serde_json::from_value(json!({"op":"tuple","elements":[]})).unwrap();
    let schema = Schema::empty();
    let dtype = tuple.dtype(&schema).unwrap();
    assert_eq!(dtype, Type::Tuple { tuple: vec![] });
    assert_eq!(Type::from_arrow(&dtype.to_arrow().unwrap()).unwrap(), dtype);
    assert_ne!(
        dtype.to_arrow().unwrap(),
        Type::Struct { r#struct: vec![] }.to_arrow().unwrap()
    );
    assert!(contains_function(
        &lower(&tuple, &schema).unwrap(),
        "named_struct"
    ));
}
