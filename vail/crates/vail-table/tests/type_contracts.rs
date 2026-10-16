//! Public aggregate construction/evaluation checks the shared type contract.
use arrow_array::{Array, Float64Array, Int64Array, ListArray, StringArray, StructArray};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType as P, Type};
use vail_table::{TableIR, globals::GlobalsIR};

fn number(value: f64) -> Expr {
    Expr::Literal {
        dtype: Type::Primitive(P::Float64),
        value: json!(value),
    }
}
fn aggregate(function: &str, input: Expr, options: serde_json::Value) -> Expr {
    Expr::Aggregate {
        function: function.into(),
        value: Some(Arc::new(input)),
        options,
        result_type: None,
        init: vec![],
    }
}
#[tokio::test]
async fn ordered_take_named_output_agrees_with_executable_values() {
    let source = TableIR::range(3, 2).unwrap();
    let value = Expr::Literal {
        dtype: Type::Primitive(P::Str),
        value: json!("label"),
    };
    let ordering = Expr::Column { name: "idx".into() };
    let reversed = Expr::Struct {
        fields: vec![
            ("ordering".into(), ordering.clone()),
            ("value".into(), value.clone()),
        ],
    };
    assert!(
        GlobalsIR::aggregate(
            source.clone(),
            aggregate("take", reversed, json!({"n":1,"ordered":true})),
            Default::default()
        )
        .is_err()
    );
    let packed = Expr::Struct {
        fields: vec![("value".into(), value), ("ordering".into(), ordering)],
    };
    let result = GlobalsIR::aggregate(
        source,
        aggregate("take", packed, json!({"n":1,"ordered":true})),
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
        .unwrap()
        .value(0);
    assert_eq!(
        list.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "label"
    );
}
#[tokio::test]
async fn scalar_regression_predictor_and_weight_remain_executable() {
    let source = TableIR::range(5, 2).unwrap();
    let y = Expr::Cast {
        dtype: Type::Primitive(P::Float64),
        value: Arc::new(Expr::Column { name: "idx".into() }),
    };
    let input = Expr::Struct {
        fields: vec![
            ("y".into(), y),
            ("x".into(), number(1.)),
            ("weight".into(), number(2.)),
        ],
    };
    let result = GlobalsIR::aggregate(
        source,
        aggregate("linreg", input, json!({"n_predictors":1,"nested_dim":1})),
        Default::default(),
    )
    .unwrap()
    .evaluate()
    .await
    .unwrap();
    let values = result
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let beta = values
        .column_by_name("beta")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    assert!(
        (beta
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            - 2.)
            .abs()
            < 1e-10
    );
    assert_eq!(
        values
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
}
