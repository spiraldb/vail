use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use serde_json::json;
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

#[test]
fn borrowed_bindings_keep_sliced_null_values_and_tuple_fields() {
    let x: ArrayRef = Arc::new(Int32Array::from(vec![Some(99), Some(4), None, Some(9)]));
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("skip"),
        None,
        Some("AbC"),
        Some("Π"),
    ]));
    let batch = RecordBatch::try_from_iter([("x", x), ("text", text)])
        .unwrap()
        .slice(1, 3);
    let expression: Expr = serde_json::from_value(json!({"op":"switch",
        "value":{"op":"tuple","elements":[{"op":"column","name":"x"},{"op":"column","name":"text"}]},
        "parameter":"v","cases":[],"missing":null,
        "default":{"op":"struct","fields":[
            ["a",{"op":"get_field","value":{"op":"lambda_variable","name":"v"},"name":"__vail_tuple_0"}],
            ["b",{"op":"get_field","value":{"op":"lambda_variable","name":"v"},"name":"__vail_tuple_1"}],
            ["again",{"op":"get_field","value":{"op":"lambda_variable","name":"v"},"name":"__vail_tuple_0"}]]}})).unwrap();
    let program = lower(&expression, batch.schema_ref()).unwrap();
    for batch in [batch.clone(), batch.slice(0, 0), batch.slice(1, 2)] {
        let result = program
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(result.column(0), batch.column(0));
        assert_eq!(result.column(1), batch.column(1));
        assert_eq!(result.column(2), batch.column(0));
    }
}

#[test]
fn repeated_text_conversion_preserves_unicode_nulls_and_bindings() {
    let batch = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec![
            Some("skip"),
            Some("AbC"),
            None,
            Some("Π"),
        ])) as ArrayRef,
    )])
    .unwrap()
    .slice(1, 3);
    for function in ["lower", "upper"] {
        let convert = json!({"op":"string_function","function":function,
            "args":[{"op":"column","name":"text"}]});
        let fields = |value: serde_json::Value| {
            json!({"op":"struct","fields":[
            ["first",value.clone()],["second",value.clone()],["third",value]]})
        };
        let direct: Expr = serde_json::from_value(fields(convert.clone())).unwrap();
        let bound: Expr = serde_json::from_value(json!({"op":"switch","value":convert,
            "parameter":"converted","cases":[],"missing":null,
            "default":fields(json!({"op":"lambda_variable","name":"converted"}))}))
        .unwrap();
        let expected: ArrayRef = Arc::new(StringArray::from(if function == "lower" {
            vec![Some("abc"), None, Some("π")]
        } else {
            vec![Some("ABC"), None, Some("Π")]
        }));
        for expression in [direct, bound] {
            let program = lower(&expression, batch.schema_ref()).unwrap();
            let result = program.evaluate(&batch).unwrap().into_array(3).unwrap();
            let result = result.as_any().downcast_ref::<StructArray>().unwrap();
            for column in result.columns() {
                assert_eq!(column, &expected);
            }
        }
    }
}

#[test]
fn bindings_retain_eager_errors_and_lexical_scope() {
    let batch =
        RecordBatch::try_from_iter([("x", Arc::new(Int32Array::from(vec![4, 9])) as ArrayRef)])
            .unwrap();
    let eager: Expr = serde_json::from_value(json!({"op":"switch",
        "value":{"op":"tuple","elements":[{"op":"column","name":"x"},
            {"op":"die","dtype":"int32","message":{"op":"literal","dtype":"str","value":"unused eager argument"}}]},
        "parameter":"v","cases":[],"missing":null,
        "default":{"op":"get_field","value":{"op":"lambda_variable","name":"v"},"name":"__vail_tuple_0"}})).unwrap();
    let error = lower(&eager, batch.schema_ref())
        .unwrap()
        .evaluate(&batch)
        .unwrap_err();
    assert!(
        error.to_string().contains("unused eager argument"),
        "{error}"
    );
    let nested = |inner: &str| -> Expr {
        serde_json::from_value(json!({"op":"switch","value":{"op":"column","name":"x"},
        "parameter":"v","cases":[],"missing":null,
        "default":{"op":"struct","fields":[["outer",{"op":"lambda_variable","name":"v"}],
            ["inner",{"op":"switch","value":{"op":"literal","dtype":"int32","value":42},
                "parameter":inner,"cases":[],"missing":null,"default":{"op":"lambda_variable","name":inner}}]]}})).unwrap()
    };
    assert!(
        lower(&nested("v"), batch.schema_ref())
            .unwrap_err()
            .to_string()
            .contains("duplicate switch parameter identity")
    );
    let output = lower(&nested("inner"), batch.schema_ref())
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(2)
        .unwrap();
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(output.column(0), batch.column(0));
    assert_eq!(output.column(1).as_ref(), &Int32Array::from(vec![42, 42]));
}
