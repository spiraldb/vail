//! Public checked-expression/type boundaries, independent of execution plans.
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType as P, Type};

fn primitive(p: P) -> Type {
    Type::Primitive(p)
}
fn literal() -> Expr {
    Expr::Literal {
        dtype: primitive(P::Int32),
        value: json!(3),
    }
}
fn scan(function: &str, value: Option<Expr>, annotation: Option<Type>) -> Expr {
    Expr::Aggregate {
        function: function.into(),
        value: value.map(Arc::new),
        options: json!({"__vail_scan":true}),
        result_type: annotation,
        init: vec![],
    }
}
#[test]
fn casts_check_semantic_compatibility_before_promising_a_type() {
    let target = Type::Array {
        array: Box::new(primitive(P::Int32)),
    };
    let expression = Expr::Cast {
        dtype: target,
        value: Arc::new(literal()),
    };
    assert!(expression.dtype(&Schema::empty()).is_err());
    let expression = Expr::Cast {
        dtype: primitive(P::Float64),
        value: Arc::new(literal()),
    };
    assert_eq!(
        expression.dtype(&Schema::empty()).unwrap(),
        primitive(P::Float64)
    );
}
#[test]
fn scan_annotations_cannot_hide_invalid_inputs_functions_or_results() {
    for expression in [
        scan("sum", Some(literal()), Some(primitive(P::Bool))),
        scan("unknown", Some(literal()), Some(primitive(P::Int64))),
        scan(
            "sum",
            Some(Expr::Column {
                name: "absent".into(),
            }),
            Some(primitive(P::Int64)),
        ),
        scan("count_where", Some(literal()), Some(primitive(P::Int64))),
        scan("count", Some(literal()), Some(primitive(P::Int64))),
    ] {
        assert!(expression.dtype(&Schema::empty()).is_err());
    }
    assert_eq!(
        scan("sum", Some(literal()), Some(primitive(P::Int64)))
            .dtype(&Schema::empty())
            .unwrap(),
        primitive(P::Int64)
    );
}
#[test]
fn fold_state_and_lexical_scopes_are_checked() {
    let fold = |seq: Expr, combine: Expr| {
        scan(
            "__scan_fold",
            Some(Expr::AggregateFold {
                initial: Arc::new(literal()),
                seq: Arc::new(seq),
                combine: Arc::new(combine),
                state_parameter: "state".into(),
                left_parameter: "left".into(),
                right_parameter: "right".into(),
            }),
            None,
        )
    };
    let variable = |name: &str| Expr::LambdaVariable { name: name.into() };
    assert_eq!(
        fold(variable("state"), variable("left"))
            .dtype(&Schema::empty())
            .unwrap(),
        primitive(P::Int32)
    );
    assert!(
        fold(
            Expr::Literal {
                dtype: primitive(P::Bool),
                value: json!(true)
            },
            variable("left")
        )
        .dtype(&Schema::empty())
        .is_err()
    );
    assert!(
        fold(variable("state"), variable("state"))
            .dtype(&Schema::empty())
            .is_err()
    );
    let schema = Schema::new(vec![Field::new("row", DataType::Int32, true)]);
    assert!(
        fold(variable("state"), Expr::Column { name: "row".into() })
            .dtype(&schema)
            .is_err()
    );
}
#[test]
fn arrow_struct_import_rejects_duplicate_nested_names() {
    let dtype = DataType::Struct(
        vec![
            Arc::new(Field::new("x", DataType::Int32, true)),
            Arc::new(Field::new("x", DataType::Utf8, true)),
        ]
        .into(),
    );
    assert!(Type::from_arrow(&dtype).is_err());
}

#[test]
fn required_initializers_and_ordered_take_layout_are_checked() {
    use vail_execution::aggregate_contract::result_type;
    let number = primitive(P::Float64);
    assert!(result_type("hist", Some(&number), &json!({}), &[]).is_err());
    assert!(
        result_type(
            "hist",
            Some(&number),
            &json!({"start":0.,"end":1.,"bins":2}),
            &[("start".into(), number.clone())]
        )
        .is_err()
    );
    let reversed = Type::Struct {
        r#struct: vec![
            ("ordering".into(), primitive(P::Int64)),
            ("value".into(), primitive(P::Str)),
        ],
    };
    assert!(result_type("take", Some(&reversed), &json!({"n":1,"ordered":true}), &[]).is_err());
    let ordered = Type::Struct {
        r#struct: vec![
            ("value".into(), primitive(P::Str)),
            ("ordering".into(), primitive(P::Int64)),
        ],
    };
    assert_eq!(
        result_type("take", Some(&ordered), &json!({"n":1,"ordered":true}), &[]).unwrap(),
        Type::Array {
            array: Box::new(primitive(P::Str))
        }
    );
    // Quantiles are optional at the native boundary and default to the median.
    assert_eq!(
        result_type(
            "approx_median",
            Some(&number),
            &json!({"k":0,"scalar":true}),
            &[]
        )
        .unwrap(),
        number
    );
}

#[test]
fn unsupported_cdf_is_rejected_at_public_signature_boundary() {
    assert!(
        vail_execution::aggregate_contract::result_type(
            "approx_cdf",
            Some(&primitive(P::Int32)),
            &json!({"k":100,"raw":false}),
            &[]
        )
        .is_err()
    );
}

#[test]
fn packed_numeric_arguments_match_executor_positions_and_scalar_regression_form() {
    use vail_execution::aggregate_contract::result_type;
    let double = primitive(P::Float64);
    let reversed = Type::Struct {
        r#struct: vec![("y".into(), double.clone()), ("x".into(), double.clone())],
    };
    assert!(
        result_type(
            "downsample",
            Some(&reversed),
            &json!({"n_divisions":4}),
            &[]
        )
        .is_err()
    );
    let scalar_regression = Type::Struct {
        r#struct: vec![("y".into(), double.clone()), ("x".into(), double)],
    };
    assert!(
        result_type(
            "linreg",
            Some(&scalar_regression),
            &json!({"n_predictors":1,"nested_dim":0}),
            &[]
        )
        .is_ok()
    );
    assert!(
        result_type(
            "linreg",
            Some(&scalar_regression),
            &json!({"n_predictors":2,"nested_dim":0}),
            &[]
        )
        .is_err()
    );
}

#[test]
fn shared_callback_nodes_are_checked_in_each_lexical_type_context() {
    use vail_execution::expressions::UnaryOperator;
    let variable = Arc::new(Expr::LambdaVariable { name: "v".into() });
    let map = |dtype, value, body| Expr::ArrayMap {
        value: Arc::new(Expr::Literal {
            dtype: Type::Array {
                array: Box::new(dtype),
            },
            value,
        }),
        parameter: "v".into(),
        body,
    };
    let mixed = Expr::Struct {
        fields: vec![
            (
                "ints".into(),
                map(primitive(P::Int32), json!([1]), variable.clone()),
            ),
            (
                "strings".into(),
                map(primitive(P::Str), json!(["x"]), variable.clone()),
            ),
        ],
    };
    assert_eq!(
        mixed.dtype(&Schema::empty()).unwrap(),
        Type::Struct {
            r#struct: vec![
                (
                    "ints".into(),
                    Type::Array {
                        array: Box::new(primitive(P::Int32))
                    }
                ),
                (
                    "strings".into(),
                    Type::Array {
                        array: Box::new(primitive(P::Str))
                    }
                )
            ]
        }
    );
    let numeric = Arc::new(Expr::Unary {
        operator: UnaryOperator::Neg,
        value: variable,
    });
    let invalid = Expr::Struct {
        fields: vec![
            (
                "ints".into(),
                map(primitive(P::Int32), json!([1]), numeric.clone()),
            ),
            (
                "strings".into(),
                map(primitive(P::Str), json!(["x"]), numeric),
            ),
        ],
    };
    assert!(invalid.dtype(&Schema::empty()).is_err());
}
#[test]
fn shared_row_reference_cannot_escape_into_closed_scan_initializer() {
    let column = Arc::new(Expr::Column { name: "x".into() });
    let expression = Expr::Struct {
        fields: vec![
            (
                "row".into(),
                Expr::Unary {
                    operator: vail_execution::expressions::UnaryOperator::Pos,
                    value: column.clone(),
                },
            ),
            (
                "prefix".into(),
                Expr::Aggregate {
                    function: "__scan_fold".into(),
                    value: Some(Arc::new(Expr::AggregateFold {
                        initial: column,
                        seq: Arc::new(Expr::LambdaVariable {
                            name: "state".into(),
                        }),
                        combine: Arc::new(Expr::LambdaVariable {
                            name: "left".into(),
                        }),
                        state_parameter: "state".into(),
                        left_parameter: "left".into(),
                        right_parameter: "right".into(),
                    })),
                    options: json!({"__vail_scan":true}),
                    result_type: Some(primitive(P::Int32)),
                    init: vec![],
                },
            ),
        ],
    };
    assert!(
        expression
            .dtype(&Schema::new(vec![Field::new("x", DataType::Int32, false)]))
            .is_err()
    );
}

#[test]
fn shared_scan_bodies_remain_valid_through_each_collection_wrapper() {
    let mut body = Arc::new(scan("sum", Some(literal()), Some(primitive(P::Int64))));
    for _ in 0..20 {
        body = Arc::new(Expr::Binary {
            operator: vail_execution::expressions::BinaryOperator::Add,
            left: body.clone(),
            right: body,
        });
    }
    let input = Arc::new(Expr::Literal {
        dtype: Type::Array {
            array: Box::new(primitive(P::Int32)),
        },
        value: json!([1]),
    });
    let wrappers = [
        (
            Expr::AggFilter {
                predicate: Arc::new(Expr::Literal {
                    dtype: primitive(P::Bool),
                    value: json!(true),
                }),
                body: body.clone(),
            },
            primitive(P::Int64),
        ),
        (
            Expr::AggArray {
                value: input.clone(),
                parameter: "element".into(),
                body: body.clone(),
            },
            Type::Array {
                array: Box::new(primitive(P::Int64)),
            },
        ),
        (
            Expr::AggExplode {
                value: input,
                parameter: "element".into(),
                body: body.clone(),
            },
            primitive(P::Int64),
        ),
        (
            Expr::AggGroupBy {
                key: Arc::new(literal()),
                body,
            },
            Type::Dict {
                dict: Box::new((primitive(P::Int32), primitive(P::Int64))),
            },
        ),
    ];
    for (expression, expected) in wrappers {
        assert!(expression.contains_scan());
        assert_eq!(expression.dtype(&Schema::empty()).unwrap(), expected);
    }
}
