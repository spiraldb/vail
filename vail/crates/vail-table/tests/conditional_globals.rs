use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use vail_table::globals::{GlobalsIR, GlobalsRef};

fn selector(value: Option<bool>) -> GlobalsRef {
    GlobalsIR::arrow_literal(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "condition",
                DataType::Boolean,
                true,
            )])),
            vec![Arc::new(BooleanArray::from(vec![value]))],
        )
        .unwrap(),
    )
    .unwrap()
}
fn value(value: i32) -> GlobalsRef {
    GlobalsIR::arrow_literal(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![value]))],
        )
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn conditional_globals_promote_nullable_outputs_and_choose_nonnullable_branches() {
    for (condition, expected) in [(Some(true), Some(3)), (Some(false), Some(7)), (None, None)] {
        let globals = GlobalsIR::conditional(selector(condition), value(3), value(7)).unwrap();
        assert!(globals.schema().field(0).is_nullable());
        let output = globals.evaluate().await.unwrap();
        assert!(output.schema().field(0).is_nullable());
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(
            if values.is_null(0) {
                None
            } else {
                Some(values.value(0))
            },
            expected
        );
    }
}
