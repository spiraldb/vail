use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, project_batch};

#[test]
fn quality_reads_sliced_nullable_likelihoods() {
    let lists = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(99)]),
        Some(vec![Some(0), Some(69), Some(1035)]),
        None,
        Some(vec![None, Some(8), Some(3)]),
        Some(vec![]),
        Some(vec![Some(120), Some(150)]),
    ]);
    let source =
        RecordBatch::try_from_iter([("PL", Arc::new(lists.slice(1, 5)) as ArrayRef)]).unwrap();
    let expression: Expr = serde_json::from_value(json!({
        "op": "string_function", "function": "gq_from_pl",
        "args": [{"op": "column", "name": "PL"}]
    }))
    .unwrap();
    let output = project_batch(&source, &[("GQ".into(), expression.clone())]).unwrap();
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![Some(69), None, Some(5), Some(0), Some(0)])
    );
    let empty = project_batch(&source.slice(0, 0), &[("GQ".into(), expression)]).unwrap();
    assert_eq!(empty.num_rows(), 0);
}
