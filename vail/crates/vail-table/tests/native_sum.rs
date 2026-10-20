//! Public coordinate SUM execution uses the actual element type, independent
//! of optional frontend result annotations.
use arrow_array::{Array, BooleanArray, Int64Array, ListArray, RecordBatch};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType, Type};
use vail_table::{TableIR, globals::GlobalsIR};

fn column(name: &str) -> Expr {
    Expr::Column { name: name.into() }
}

fn coordinate_sum(result_type: Option<Type>) -> Expr {
    Expr::AggArray {
        value: Arc::new(column("xs")),
        parameter: "item".into(),
        body: Arc::new(Expr::Aggregate {
            function: "sum".into(),
            value: Some(Arc::new(Expr::LambdaVariable {
                name: "item".into(),
                dtype: None,
            })),
            options: json!({}),
            result_type,
            init: vec![],
        }),
    }
}

#[tokio::test]
async fn coordinate_sum_preserves_large_integer_with_absent_or_mismatched_annotation() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "xs",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
        true,
    )]));
    let values = Arc::new(Int64Array::from(vec![9_007_199_254_740_993, 1]));
    let lists = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int64, true)),
        OffsetBuffer::from_lengths([1, 1]),
        values,
        None,
    ));
    let batch = RecordBatch::try_new(schema, vec![lists]).unwrap();
    let source = TableIR::parallelize(batch, 2).unwrap();

    for annotation in [None, Some(Type::Primitive(PrimitiveType::Float64))] {
        let result = GlobalsIR::aggregate(
            source.clone(),
            coordinate_sum(annotation),
            Default::default(),
        )
        .unwrap()
        .evaluate()
        .await
        .unwrap();
        let list = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            list.value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[9_007_199_254_740_994]
        );
        assert_eq!(
            list.value_type(),
            DataType::Int64,
            "SUM output type comes from its actual Int64 input"
        );
    }
}

#[tokio::test]
async fn coordinate_boolean_sum_uses_int64_empty_identity() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "xs",
        DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true))),
        true,
    )]));
    let lists = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Boolean, true)),
        OffsetBuffer::from_lengths([2, 2]),
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            None,
        ])),
        None,
    ));
    let batch = RecordBatch::try_new(schema, vec![lists]).unwrap();
    let source = TableIR::parallelize(batch, 2).unwrap();
    let result = GlobalsIR::aggregate(source, coordinate_sum(None), Default::default())
        .unwrap()
        .evaluate()
        .await
        .unwrap();
    let list = result
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_type(), DataType::Int64);
    assert_eq!(
        list.value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 0],
        "the missing-only coordinate has a typed zero identity"
    );
}
