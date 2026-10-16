use arrow_array::{
    Array, ArrayRef, Int32Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    StructArray,
};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, project_batch};

#[test]
fn normalization_accepts_shared_scalars_and_arrow_string_storage() {
    let text = "é".repeat(8192);
    let reference = format!("{text}A");
    let alternate = format!("{text}T");
    let expr: Expr = serde_json::from_value(json!({
        "op":"string_function", "function":"variant_min_rep", "args":[
            {"op":"literal","dtype":"str","value":reference},
            {"op":"column","name":"alternate"}
        ]
    }))
    .unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![alternate.as_str(); 64])),
        Arc::new(LargeStringArray::from(vec![alternate.as_str(); 64])),
        Arc::new(StringViewArray::from(vec![alternate.as_str(); 64])),
    ];
    for array in arrays {
        let source = RecordBatch::try_from_iter([("alternate", array)]).unwrap();
        let result = project_batch(&source, &[("variant".into(), expr.clone())]).unwrap();
        let variant = result
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let shift = variant
            .column_by_name("shift")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(shift, &Int32Array::from(vec![8192; 64]));
        assert_eq!(
            project_batch(&source.slice(0, 0), &[("variant".into(), expr.clone())])
                .unwrap()
                .num_rows(),
            0
        );
    }
}

#[test]
fn entry_preparation_reuses_scalar_likelihoods_for_batch_rows() {
    let n = 128;
    let pl = (0..n * (n + 1) / 2).collect::<Vec<i32>>();
    let expr: Expr = serde_json::from_value(json!({
        "op":"string_function","function":"variant_entry", "args":[
            {"op":"column","name":"n"},
            {"op":"literal","dtype":{"array":"int32"},"value":[0,1]},
            {"op":"literal","dtype":{"array":"int32"},"value":null},
            {"op":"literal","dtype":{"array":"int32"},"value":null},
            {"op":"literal","dtype":{"array":"int32"},"value":pl}
        ]
    }))
    .unwrap();
    let source =
        RecordBatch::try_from_iter([("n", Arc::new(Int32Array::from(vec![n; 256])) as ArrayRef)])
            .unwrap();
    let result = project_batch(&source, &[("entry".into(), expr.clone())]).unwrap();
    assert_eq!(result.num_rows(), 256);
    let first = datafusion_common::ScalarValue::try_from_array(result.column(0), 0).unwrap();
    for row in 1..256 {
        assert_eq!(
            datafusion_common::ScalarValue::try_from_array(result.column(0), row).unwrap(),
            first
        );
    }
    assert_eq!(
        project_batch(&source.slice(0, 0), &[("entry".into(), expr)])
            .unwrap()
            .num_rows(),
        0
    );
}
