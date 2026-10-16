use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::Schema;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, project_batch};

#[test]
fn string_length_uses_codepoints() {
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

#[test]
fn unicode_kernels_accept_arrow_string_storage_and_shared_scalar_arguments() {
    use arrow_array::{Int32Array, LargeStringArray, StringViewArray};
    let sources: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("a😀"), None])),
        Arc::new(LargeStringArray::from(vec![Some("a😀"), None])),
        Arc::new(StringViewArray::from(vec![Some("a😀"), None])),
    ];
    let expr: Expr =
        serde_json::from_value(json!({"op":"string_function","function":"hamming","args":[
            {"op":"column","name":"text"},{"op":"literal","dtype":"str","value":"a🚀"}
        ]}))
        .unwrap();
    for source in sources {
        let batch = RecordBatch::try_from_iter([("text", source)]).unwrap();
        let result = project_batch(&batch, &[("h".into(), expr.clone())]).unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(1), None])
        );
        assert_eq!(
            project_batch(&batch.slice(0, 0), &[("h".into(), expr.clone())])
                .unwrap()
                .num_rows(),
            0
        );
    }
}
