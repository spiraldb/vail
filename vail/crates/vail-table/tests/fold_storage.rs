//! Public folds over raw alternate Arrow string storage.
use arrow_array::{ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::{Field, Schema};
use datafusion_common::ScalarValue;
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType as P, Type};
use vail_table::{TableIR, globals::GlobalsIR};

fn fold(initial: Expr, seq: Expr) -> Expr {
    Expr::AggregateFold {
        initial: Arc::new(initial),
        seq: Arc::new(seq),
        combine: Arc::new(Expr::LambdaVariable {
            name: "left".into(),
        }),
        state_parameter: "state".into(),
        left_parameter: "left".into(),
        right_parameter: "right".into(),
    }
}
#[tokio::test]
async fn alternate_storage_initializers_and_capture_returns_use_canonical_state() {
    for view in [false, true] {
        let initial_array: ArrayRef = if view {
            Arc::new(StringViewArray::from(vec!["seed"]))
        } else {
            Arc::new(LargeStringArray::from(vec!["seed"]))
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "initial",
            initial_array.data_type().clone(),
            true,
        )]));
        let globals =
            GlobalsIR::arrow_literal(RecordBatch::try_new(schema, vec![initial_array]).unwrap())
                .unwrap();
        let initial = Expr::GetField {
            value: Arc::new(Expr::ExternalGlobal {
                reference: "g".into(),
                dtype: Type::Struct {
                    r#struct: vec![("initial".into(), Type::Primitive(P::Str))],
                },
            }),
            name: "initial".into(),
        };
        let result = GlobalsIR::aggregate(
            TableIR::range(5, 3).unwrap(),
            fold(
                initial,
                Expr::LambdaVariable {
                    name: "state".into(),
                },
            ),
            [("g".into(), globals)].into(),
        )
        .unwrap()
        .evaluate()
        .await
        .unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "seed"
        );
        let values: ArrayRef = if view {
            Arc::new(StringViewArray::from(vec!["a", "b", "c"]))
        } else {
            Arc::new(LargeStringArray::from(vec!["a", "b", "c"]))
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            values.data_type().clone(),
            true,
        )]));
        let source =
            TableIR::parallelize(RecordBatch::try_new(schema, vec![values]).unwrap(), 1).unwrap();
        let initial = Expr::RuntimeLiteral {
            value: if view {
                ScalarValue::Utf8View(Some("seed".into()))
            } else {
                ScalarValue::LargeUtf8(Some("seed".into()))
            },
        };
        let result = GlobalsIR::aggregate(
            source.clone(),
            fold(initial.clone(), Expr::Column { name: "x".into() }),
            Default::default(),
        )
        .unwrap()
        .evaluate()
        .await
        .unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "c"
        );
        let scan = Expr::Aggregate {
            function: "__scan_fold".into(),
            value: Some(Arc::new(fold(initial, Expr::Column { name: "x".into() }))),
            options: serde_json::json!({"__vail_scan":true}),
            result_type: Some(Type::Primitive(P::Str)),
            init: vec![],
        };
        let scanned = source
            .map_rows(vec![("prefix".into(), scan)])
            .unwrap()
            .collect_partitions()
            .await
            .unwrap();
        let values = scanned
            .iter()
            .flatten()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned())
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec!["seed", "a", "b"]);
    }
}
