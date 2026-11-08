use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::Schema;
use serde_json::{Value, json};
use vail_execution::expressions::{Expr, project_batch};

#[test]
fn boolean_null_fallbacks_preserve_truth_values_and_slices() {
    use std::sync::Arc;
    use arrow_array::ArrayRef;
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr::{execution_props::ExecutionProps, physical_planning_context::PhysicalPlanningContext};
    use datafusion_physical_expr::create_physical_expr;
    use vail_execution::{expressions::{PrimitiveType, Type, UnaryOperator, lower}, logical_expr::lower_logical};
    let batch = RecordBatch::try_from_iter([("p", Arc::new(BooleanArray::from(vec![Some(true), Some(false), None, Some(true), None])) as ArrayRef)]).unwrap();
    let predicate = Arc::new(Expr::Column { name: "p".into() });
    for fallback in [false, true] {
        for reversed in [false, true] {
            for runtime in [false, true] {
                let fallback_value = Arc::new(if runtime {
                    Expr::RuntimeLiteral { value: ScalarValue::Boolean(Some(fallback)) }
                } else {
                    Expr::Literal { dtype: Type::Primitive(PrimitiveType::Bool), value: json!(fallback) }
                });
                let missing = Arc::new(Expr::IsMissing { value: predicate.clone() });
                let expression = if reversed {
                    Expr::IfElse { condition: missing, consequent: fallback_value, alternate: predicate.clone() }
                } else {
                    Expr::IfElse { condition: Arc::new(Expr::Unary { operator: UnaryOperator::Not, value: missing }), consequent: predicate.clone(), alternate: fallback_value }
                };
                let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
                let logical = lower_logical(&expression, &schema).unwrap();
                let programs = [lower(&expression, batch.schema_ref()).unwrap(), create_physical_expr(&logical, &schema, &ExecutionProps::new(), &PhysicalPlanningContext::default()).unwrap()];
                let expected = BooleanArray::from(vec![true, false, fallback, true, fallback]);
                for program in programs {
                    for (start, len) in [(0, 5), (1, 3), (0, 0)] {
                        let actual = program.evaluate(&batch.slice(start, len)).unwrap().into_array(len).unwrap();
                        assert_eq!(actual.as_ref(), &expected.slice(start, len));
                    }
                }
            }
        }
    }
}

#[test]
fn scalar_boolean_fallbacks_broadcast_over_nonempty_and_empty_batches() {
    use std::sync::Arc;
    use arrow_array::{ArrayRef, Int32Array};
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr::{execution_props::ExecutionProps, physical_planning_context::PhysicalPlanningContext};
    use datafusion_physical_expr::create_physical_expr;
    use vail_execution::{expressions::{PrimitiveType, Type, lower}, logical_expr::lower_logical};
    let batch = RecordBatch::try_from_iter([("unused", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef)]).unwrap();
    for value in [Some(true), Some(false), None] {
        for fallback in [false, true] {
            for runtime in [false, true] {
                let scalar = Arc::new(if runtime { Expr::RuntimeLiteral { value: ScalarValue::Boolean(value) } } else {
                    Expr::Literal { dtype: Type::Primitive(PrimitiveType::Bool), value: json!(value) }
                });
                let expression = Expr::IfElse { condition: Arc::new(Expr::IsMissing { value: scalar.clone() }),
                    consequent: Arc::new(Expr::Literal { dtype: Type::Primitive(PrimitiveType::Bool), value: json!(fallback) }), alternate: scalar };
                let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
                let logical = lower_logical(&expression, &schema).unwrap();
                for program in [lower(&expression, batch.schema_ref()).unwrap(), create_physical_expr(&logical, &schema, &ExecutionProps::new(), &PhysicalPlanningContext::default()).unwrap()] {
                    for len in [0, 3] {
                        let actual = program.evaluate(&batch.slice(0, len)).unwrap().into_array(len).unwrap();
                        assert_eq!(actual.as_ref(), &BooleanArray::from(vec![value.unwrap_or(fallback); len]));
                    }
                }
            }
        }
    }
}

fn coalesce(args: Value, dtype: &str) -> Expr {
    serde_json::from_value(
        json!({"op":"nested_function","function":"coalesce","args":args,"dtype":dtype}),
    )
    .unwrap()
}
#[test]
fn boolean_fallback_validates_arity_input_and_output_types() {
    let boolean = json!({"op":"literal","dtype":"bool","value":true});
    let integer = json!({"op":"literal","dtype":"int32","value":1});
    for args in [
        json!([]),
        json!([boolean]),
        json!([boolean, boolean, boolean]),
        json!([boolean, integer]),
    ] {
        assert!(coalesce(args, "bool").dtype(&Schema::empty()).is_err());
    }
    assert!(
        coalesce(json!([boolean, boolean]), "int32")
            .dtype(&Schema::empty())
            .is_err()
    );
    let batch = RecordBatch::try_from_iter([(
        "p",
        std::sync::Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]))
            as arrow_array::ArrayRef,
    )])
    .unwrap();
    let expr = coalesce(
        json!([{"op":"column","name":"p"},{"op":"literal","dtype":"bool","value":false}]),
        "bool",
    );
    let result = project_batch(&batch, &[("result".into(), expr)]).unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap(),
        &BooleanArray::from(vec![true, false, false])
    );
    let error = json!({"op":"die","dtype":"bool","message":{"op":"literal","dtype":"str","value":"selected fallback"}});
    let lazy = coalesce(json!([{"op":"column","name":"p"},error]), "bool");
    assert!(project_batch(&batch.slice(0, 2), &[("result".into(), lazy.clone())]).is_ok());
    assert!(project_batch(&batch, &[("result".into(), lazy)]).is_err());
}

#[test]
fn scoped_switch_projects_ordered_nullable_cases_and_checks_callback_types() {
    use arrow_array::{ArrayRef, Int32Array, StringArray};
    let batch = RecordBatch::try_from_iter([(
        "text",
        std::sync::Arc::new(StringArray::from(vec![Some("0"), None, Some("2")])) as ArrayRef,
    )])
    .unwrap();
    let variable = json!({"op":"lambda_variable","name":"base"});
    let int = |value| json!({"op":"literal","dtype":"int32","value":value});
    let condition = |key| json!({"op":"binary","operator":"eq","left":variable,"right":int(key)});
    let wire = json!({"op":"switch","parameter":"base",
        "value":{"op":"cast","dtype":"int32","value":{"op":"column","name":"text"}},
        "cases":[[condition(0),int(7)],[condition(1),{"op":"cast","dtype":"int32","value":{"op":"literal","dtype":"str","value":"unused invalid value"}}]],
        "default":int(9),"missing":int(-1)});
    let expression: Expr = serde_json::from_value(wire.clone()).unwrap();
    let output = project_batch(&batch, &[("value".into(), expression.clone())]).unwrap();
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![7, -1, 9])
    );
    assert_eq!(
        project_batch(&batch.slice(0, 0), &[("value".into(), expression)])
            .unwrap()
            .num_rows(),
        0
    );
    let mut wrong_condition = wire.clone();
    wrong_condition["cases"][0][0] = int(1);
    let mut wrong_result = wire.clone();
    wrong_result["cases"][0][1] = json!({"op":"literal","dtype":"str","value":"wrong"});
    let mut unbound = wire;
    unbound["missing"] = json!({"op":"lambda_variable","name":"unbound"});
    for invalid in [wrong_condition, wrong_result, unbound] {
        let invalid: Expr = serde_json::from_value(invalid).unwrap();
        assert!(invalid.dtype(batch.schema().as_ref()).is_err());
    }
}

#[test]
fn switch_canonicalizes_arrow_string_storage_once_for_its_scope() {
    use arrow_array::{ArrayRef, Int32Array, LargeStringArray, StringArray, StringViewArray};
    let inputs: Vec<ArrayRef> = vec![
        std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
        std::sync::Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("b")])),
        std::sync::Arc::new(StringViewArray::from(vec![Some("a"), None, Some("b")])),
    ];
    for input in inputs {
        let batch = RecordBatch::try_from_iter([("text", input)]).unwrap();
        for missing in [
            json!(null),
            json!({"op":"literal","dtype":"int32","value":-1}),
        ] {
            let has_missing = !missing.is_null();
            let expression:Expr=serde_json::from_value(json!({"op":"switch","parameter":"base","value":{"op":"column","name":"text"},
                "cases":[[{"op":"binary","operator":"eq","left":{"op":"lambda_variable","name":"base"},"right":{"op":"literal","dtype":"str","value":"a"}},
                          {"op":"literal","dtype":"int32","value":1}]],
                "default":{"op":"literal","dtype":"int32","value":2},"missing":missing})).unwrap();
            let result = project_batch(&batch, &[("result".into(), expression)]).unwrap();
            assert_eq!(
                result
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap(),
                &Int32Array::from(vec![
                    Some(1),
                    if has_missing { Some(-1) } else { None },
                    Some(2)
                ])
            );
        }
    }
}

#[test]
fn switch_selected_result_and_default_storage_match_the_declared_output() {
    use arrow_array::{ArrayRef, Int32Array, LargeStringArray, StringArray, StringViewArray};
    for values in [
        std::sync::Arc::new(StringArray::from(vec!["first", "second", "third"])) as ArrayRef,
        std::sync::Arc::new(LargeStringArray::from(vec!["first", "second", "third"])) as ArrayRef,
        std::sync::Arc::new(StringViewArray::from(vec!["first", "second", "third"])) as ArrayRef,
    ] {
        let batch = RecordBatch::try_from_iter([
            (
                "choice",
                std::sync::Arc::new(Int32Array::from(vec![Some(0), Some(1), None])) as ArrayRef,
            ),
            ("text", values),
        ])
        .unwrap();
        let column = json!({"op":"column","name":"text"});
        let literal = json!({"op":"literal","dtype":"str","value":"literal"});
        for (then, default, expected) in [
            (
                column.clone(),
                literal.clone(),
                vec!["first", "literal", "third"],
            ),
            (
                literal.clone(),
                column.clone(),
                vec!["literal", "second", "third"],
            ),
        ] {
            let expr:Expr=serde_json::from_value(json!({"op":"switch","parameter":"base","value":{"op":"column","name":"choice"},
                "cases":[[{"op":"binary","operator":"eq","left":{"op":"lambda_variable","name":"base"},"right":{"op":"literal","dtype":"int32","value":0}},then]],
                "default":default,"missing":column})).unwrap();
            let result = project_batch(&batch, &[("result".into(), expr)]).unwrap();
            assert_eq!(
                result
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap(),
                &StringArray::from(expected)
            );
        }
    }
}
