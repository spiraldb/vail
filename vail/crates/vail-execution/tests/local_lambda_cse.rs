use arrow_array::{Array, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, Expr, PrimitiveType, Type, lower};

#[test]
fn local_binding_avoids_unused_enclosing_lambda_parameter() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 9]))]).unwrap();
    let repeated = || Expr::Binary {
        operator: BinaryOperator::Add,
        left: Arc::new(Expr::Column { name: "x".into() }),
        right: Arc::new(Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some(1)),
        }),
    };
    let expression = Expr::ArrayMap {
        value: Arc::new(Expr::Array {
            element_type: Type::Primitive(PrimitiveType::Int32),
            elements: vec![Expr::RuntimeLiteral {
                value: ScalarValue::Int32(Some(0)),
            }],
        }),
        parameter: "__vail_local_cse_0".into(),
        body: Arc::new(Expr::Struct {
            fields: vec![("a".into(), repeated()), ("b".into(), repeated())],
        }),
    };
    let program = lower(&expression, &schema).unwrap();
    let result = program.evaluate(&batch).unwrap().into_array(2).unwrap();
    let lists = result.as_any().downcast_ref::<ListArray>().unwrap();
    let values = lists
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    for field in ["a", "b"] {
        let column = values
            .column_by_name(field)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(column.values().as_ref(), &[5, 10]);
    }
}

#[test]
fn nested_struct_projection_retains_public_values() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 9]))]).unwrap();
    let mut expression = Expr::Column { name: "x".into() };
    for _ in 0..96 {
        expression = Expr::Struct {
            fields: vec![("value".into(), expression)],
        };
    }
    let program = lower(&expression, &schema).unwrap();
    let mut result = program.evaluate(&batch).unwrap().into_array(2).unwrap();
    for _ in 0..96 {
        result = result
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .clone();
    }
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[4, 9]
    );
}
