use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::Schema;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, lower, project_batch};

fn assert_builtin_functions(expr: &Arc<dyn PhysicalExpr>) {
    if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() {
        assert!(
            matches!(function.name(), "lower" | "character_length"),
            "unexpected function {}",
            function.name()
        );
    }
    for child in expr.children() {
        assert_builtin_functions(child);
    }
}

#[test]
fn string_length_uses_codepoints_and_only_datafusion_kernels() {
    let expr: Expr = serde_json::from_value(json!({
        "op":"string_function","function":"character_length",
        "args":[{"op":"column","name":"text"}]
    }))
    .unwrap();
    let source = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec![Some("a😀b"), None])) as ArrayRef,
    )])
    .unwrap();
    assert_builtin_functions(&lower(&expr, &source.schema()).unwrap());
    let output = project_batch(&source, &[("size".into(), expr)]).unwrap();
    assert_eq!(
        datafusion_common::ScalarValue::try_from_array(output.column(0), 0).unwrap(),
        datafusion_common::ScalarValue::Int32(Some(3))
    );
    assert_eq!(
        datafusion_common::ScalarValue::try_from_array(output.column(0), 1).unwrap(),
        datafusion_common::ScalarValue::Int32(None)
    );
}

#[test]
fn malformed_string_functions_are_rejected_during_planning() {
    for (name, args) in [
        ("lower", json!([])),
        ("unknown", json!([])),
        ("lower", json!([{"op":"literal","dtype":"int32","value":1}])),
        (
            "repeat",
            json!([
                {"op":"literal","dtype":"str","value":"a"},
                {"op":"literal","dtype":"int32","value":1}
            ]),
        ),
    ] {
        let expr: Expr =
            serde_json::from_value(json!({"op":"string_function","function":name,"args":args}))
                .unwrap();
        assert!(expr.dtype(&Schema::empty()).is_err());
    }
}
