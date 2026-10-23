use arrow_array::Int64Array;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, Expr, PrimitiveType, Type};
use vail_table::{
    TableIR,
    globals::{GlobalsIR, GlobalsRef},
};

fn count(n: usize) -> GlobalsRef {
    GlobalsIR::aggregate(
        TableIR::range(n, 3).unwrap(),
        Expr::Aggregate {
            function: "count".into(),
            value: None,
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap()
}
fn reference(globals: &GlobalsRef) -> Expr {
    Expr::GetField {
        value: Arc::new(Expr::ExternalGlobal {
            reference: "shared".into(),
            dtype: Type::from_arrow(&arrow_schema::DataType::Struct(
                globals.schema().fields().clone(),
            ))
            .unwrap(),
        }),
        name: "value".into(),
    }
}
#[tokio::test]
async fn aggregate_dependency_can_be_shared_by_selector_and_selected_branch() {
    let total = count(17);
    // Inspecting a deferred aggregate must not execute it or require binding.
    total.physical_plan().unwrap();
    let selector = GlobalsIR::empty()
        .map_bound(
            vec![(
                "condition".into(),
                Expr::Binary {
                    operator: BinaryOperator::Gt,
                    left: Arc::new(reference(&total)),
                    right: Arc::new(Expr::Literal {
                        dtype: Type::Primitive(PrimitiveType::Int64),
                        value: json!(0),
                    }),
                },
            )],
            [("shared".into(), total.clone())].into(),
        )
        .unwrap();
    let selected = GlobalsIR::conditional(selector, total, count(0)).unwrap();
    for _ in 0..2 {
        let result = selected.evaluate().await.unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            17
        );
    }
}

#[tokio::test]
async fn scalar_subqueries_share_aggregate_dependencies_and_independent_actions() {
    let total = count(17);
    let source = TableIR::range(5, 2).unwrap();
    let output = source
        .map_rows_bound(
            vec![
                ("idx".into(), Expr::Column { name: "idx".into() }),
                ("total".into(), reference(&total)),
            ],
            [("shared".into(), total)].into(),
        )
        .unwrap();
    for _ in 0..2 {
        let batches = output.collect_partitions().await.unwrap();
        let mut rows = 0;
        for batch in batches.into_iter().flatten() {
            let values = batch
                .column_by_name("total")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(values.iter().all(|value| value == Some(17)));
            rows += batch.num_rows();
        }
        assert_eq!(rows, 5);
    }
}

#[tokio::test]
async fn unselected_aggregate_dependency_remains_lazy() {
    let good = count(17);
    let bad_table = TableIR::range(1, 1)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), Expr::Column { name: "idx".into() }),
            (
                "bad".into(),
                Expr::Die {
                    message: Arc::new(Expr::Literal {
                        dtype: Type::Primitive(PrimitiveType::Str),
                        value: json!("unselected aggregate"),
                    }),
                    dtype: Type::Primitive(PrimitiveType::Int32),
                },
            ),
        ])
        .unwrap();
    let bad = GlobalsIR::aggregate(
        bad_table,
        Expr::Aggregate {
            function: "sum".into(),
            value: Some(Arc::new(Expr::Column { name: "bad".into() })),
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let selector = GlobalsIR::empty()
        .map(vec![(
            "condition".into(),
            Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Bool),
                value: json!(true),
            },
        )])
        .unwrap();
    let selected = GlobalsIR::conditional(selector, good, bad).unwrap();
    assert_eq!(
        selected
            .evaluate()
            .await
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        17
    );
}

#[tokio::test]
async fn discarded_scalar_aggregate_is_not_evaluated() {
    let source = TableIR::range(1, 1)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), Expr::Column { name: "idx".into() }),
            (
                "bad".into(),
                Expr::Die {
                    message: Arc::new(Expr::Literal {
                        dtype: Type::Primitive(PrimitiveType::Str),
                        value: json!("discarded aggregate"),
                    }),
                    dtype: Type::Primitive(PrimitiveType::Int32),
                },
            ),
        ])
        .unwrap();
    let bad = GlobalsIR::aggregate(
        source,
        Expr::Aggregate {
            function: "sum".into(),
            value: Some(Arc::new(Expr::Column { name: "bad".into() })),
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let output = TableIR::range(5, 2)
        .unwrap()
        .map_rows_bound(
            vec![
                ("idx".into(), Expr::Column { name: "idx".into() }),
                ("unused".into(), reference(&bad)),
            ],
            [("shared".into(), bad)].into(),
        )
        .unwrap()
        .map_rows(vec![("idx".into(), Expr::Column { name: "idx".into() })])
        .unwrap();
    assert_eq!(
        output
            .collect_partitions()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        5
    );
}

#[tokio::test]
async fn unreachable_case_aggregate_is_not_evaluated() {
    let source = TableIR::range(1, 1)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), Expr::Column { name: "idx".into() }),
            (
                "bad".into(),
                Expr::Die {
                    message: Arc::new(Expr::Literal {
                        dtype: Type::Primitive(PrimitiveType::Str),
                        value: json!("unreachable aggregate"),
                    }),
                    dtype: Type::Primitive(PrimitiveType::Int32),
                },
            ),
        ])
        .unwrap();
    let bad = GlobalsIR::aggregate(
        source,
        Expr::Aggregate {
            function: "sum".into(),
            value: Some(Arc::new(Expr::Column { name: "bad".into() })),
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let output = TableIR::range(5, 2)
        .unwrap()
        .map_rows_bound(
            vec![
                ("idx".into(), Expr::Column { name: "idx".into() }),
                (
                    "selected".into(),
                    Expr::IfElse {
                        condition: Arc::new(Expr::Literal {
                            dtype: Type::Primitive(PrimitiveType::Bool),
                            value: json!(false),
                        }),
                        consequent: Arc::new(reference(&bad)),
                        alternate: Arc::new(Expr::Literal {
                            dtype: Type::Primitive(PrimitiveType::Int64),
                            value: json!(7),
                        }),
                    },
                ),
            ],
            [("shared".into(), bad)].into(),
        )
        .unwrap();
    let batches = output.collect_partitions().await.unwrap();
    let mut rows = 0;
    for batch in batches.into_iter().flatten() {
        let values = batch
            .column_by_name("selected")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(values.iter().all(|value| value == Some(7)));
        rows += batch.num_rows();
    }
    assert_eq!(rows, 5);
}

#[tokio::test]
async fn unselected_conditional_aggregate_does_not_fold_invalid_cast() {
    let bad = GlobalsIR::aggregate(
        TableIR::range(1, 1).unwrap(),
        Expr::Aggregate {
            function: "sum".into(),
            value: Some(Arc::new(Expr::Cast {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: Arc::new(Expr::Literal {
                    dtype: Type::Primitive(PrimitiveType::Str),
                    value: json!("not an integer"),
                }),
            })),
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let selector = GlobalsIR::empty()
        .map(vec![(
            "condition".into(),
            Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Bool),
                value: json!(true),
            },
        )])
        .unwrap();
    let selected = GlobalsIR::conditional(selector, count(17), bad).unwrap();
    assert_eq!(
        selected
            .evaluate()
            .await
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        17
    );
}
