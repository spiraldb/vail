//! Public TableIR/action and DataFusion execution boundaries: cancellation,
//! out-of-order partition demand, and compact view-backed nested values.
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringViewArray, StructArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_execution::TaskContext;
use futures::TryStreamExt;
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType as P, Type, UnaryOperator};
use vail_table::{TableIR, partitioned};

fn previous(value: Expr, dtype: Type) -> Expr {
    let variable = |name: &str| Expr::LambdaVariable {
        name: name.into(),
        dtype: None,
    };
    let select = |value: Expr, fallback: Expr| Expr::IfElse {
        condition: Arc::new(Expr::Unary {
            operator: UnaryOperator::Not,
            value: Arc::new(Expr::IsMissing {
                value: Arc::new(value.clone()),
            }),
        }),
        consequent: Arc::new(value),
        alternate: Arc::new(fallback),
    };
    Expr::Aggregate {
        function: "__scan_fold".into(),
        value: Some(Arc::new(Expr::AggregateFold {
            initial: Arc::new(Expr::Literal {
                dtype: dtype.clone(),
                value: serde_json::Value::Null,
            }),
            seq: Arc::new(select(value, variable("state"))),
            combine: Arc::new(select(variable("right"), variable("left"))),
            state_parameter: "state".into(),
            left_parameter: "left".into(),
            right_parameter: "right".into(),
        })),
        options: serde_json::json!({"__vail_scan":true}),
        result_type: Some(dtype),
        init: vec![],
    }
}
fn column(name: &str) -> Expr {
    Expr::Column { name: name.into() }
}

#[tokio::test]
async fn cancelled_partition_reader_can_retry_same_prepared_action() {
    let n = 4 * 1024 * 1024;
    let table = TableIR::range_batched(n, 4, 128)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), column("idx")),
            (
                "previous".into(),
                previous(column("idx"), Type::Primitive(P::Int32)),
            ),
        ])
        .unwrap();
    let template = partitioned::prepare_action(&table).unwrap();
    let action = template.start_action().await.unwrap();
    let context = Arc::new(TaskContext::default());
    let mut cancelled = action.execute(3, context.clone()).unwrap();
    // Interrupt an actual consumer, without asserting a particular pending
    // state or scheduler sequence. Completion before interruption is valid too.
    let _ = tokio::time::timeout(std::time::Duration::from_millis(1), cancelled.try_next()).await;
    drop(cancelled);
    let mut replay = action.execute(3, context.clone()).unwrap();
    let batch = tokio::time::timeout(std::time::Duration::from_secs(15), replay.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let previous = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(previous.value(0), (3 * n / 4 - 1) as i32);
    assert_eq!(previous.value(1), (3 * n / 4) as i32);
    // Another reader of the same action demands an earlier partition later.
    let mut earlier = action.execute(1, context).unwrap();
    let batch = earlier.try_next().await.unwrap().unwrap();
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        (n / 4 - 1) as i32
    );
    let fresh = template.start_action().await.unwrap();
    let mut first = fresh.execute(0, Arc::new(TaskContext::default())).unwrap();
    let batch = first.try_next().await.unwrap().unwrap();
    assert!(batch.column(1).is_null(0));
}

#[tokio::test]
async fn retained_nested_view_value_is_canonical_and_does_not_pin_large_backing_buffer() {
    let huge = "x".repeat(16 * 1024 * 1024);
    let strings: ArrayRef = Arc::new(StringViewArray::from(vec![
        huge.as_str(),
        "retained non-inline value",
        "tail one",
        "tail two",
    ]));
    let fields = vec![Field::new("text", DataType::Utf8View, false)].into();
    let values: ArrayRef = Arc::new(StructArray::new(fields, vec![strings], None));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        values.data_type().clone(),
        false,
    )]));
    let table =
        TableIR::parallelize(RecordBatch::try_new(schema, vec![values]).unwrap(), 2).unwrap();
    let dtype = Type::Struct {
        r#struct: vec![("text".into(), Type::Primitive(P::Str))],
    };
    let scanned = table
        .map_rows(vec![
            ("v".into(), column("v")),
            ("previous".into(), previous(column("v"), dtype)),
        ])
        .unwrap();
    let action = partitioned::prepare_action(&scanned)
        .unwrap()
        .start_action()
        .await
        .unwrap();
    let mut stream = action.execute(1, Arc::new(TaskContext::default())).unwrap();
    let batch = stream.try_next().await.unwrap().unwrap();
    let previous = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(previous.column(0).data_type(), &DataType::Utf8);
    assert!(
        previous.get_buffer_memory_size() < 16 * 1024,
        "retained prefix pinned a source buffer"
    );
    let text = previous
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(text.value(0), "retained non-inline value");
}
