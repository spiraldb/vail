//! Replay a prepared configured scan through the public action boundary.
use arrow_array::Int32Array;
use datafusion_execution::TaskContext;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, Expr, PrimitiveType as P, Type};
use vail_table::{TableIR, partitioned};
fn integer(value: i32) -> Expr {
    Expr::Literal {
        dtype: Type::Primitive(P::Int32),
        value: serde_json::json!(value),
    }
}
fn variable(name: &str) -> Expr {
    Expr::LambdaVariable {
        name: name.into(),
        dtype: None,
    }
}
fn binary(operator: BinaryOperator, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        operator,
        left: Arc::new(left),
        right: Arc::new(right),
    }
}
#[tokio::test]
async fn prepared_scan_actions_reread_configuration_after_success_and_failure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("configuration");
    let moved = directory.path().join("moved");
    TableIR::range(0, 1)
        .unwrap()
        .map_globals(vec![("initial".into(), integer(7))])
        .unwrap()
        .write(path.to_str().unwrap(), false)
        .await
        .unwrap();
    let config = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let initial = Expr::GetField {
        value: Arc::new(Expr::ExternalGlobal {
            reference: "configuration".into(),
            dtype: Type::Struct {
                r#struct: vec![("initial".into(), Type::Primitive(P::Int32))],
            },
        }),
        name: "initial".into(),
    };
    let fold = Expr::AggregateFold {
        initial: Arc::new(initial),
        seq: Arc::new(binary(BinaryOperator::Add, variable("state"), integer(1))),
        combine: Arc::new(binary(
            BinaryOperator::Sub,
            binary(BinaryOperator::Add, variable("left"), variable("right")),
            integer(7),
        )),
        state_parameter: "state".into(),
        left_parameter: "left".into(),
        right_parameter: "right".into(),
    };
    let scan = Expr::Aggregate {
        function: "__scan_fold".into(),
        value: Some(Arc::new(fold)),
        options: serde_json::json!({"__vail_scan":true}),
        result_type: Some(Type::Primitive(P::Int32)),
        init: vec![],
    };
    let table = TableIR::range(4, 3)
        .unwrap()
        .map_rows_bound(
            vec![
                ("idx".into(), Expr::Column { name: "idx".into() }),
                ("prefix".into(), scan),
            ],
            [("configuration".into(), config.globals().clone())].into(),
        )
        .unwrap();
    let prepared = partitioned::prepare_action(&table).unwrap();
    for missing in [false, true, false] {
        if missing {
            std::fs::rename(&path, &moved).unwrap();
        } else if moved.exists() {
            std::fs::rename(&moved, &path).unwrap();
        }
        let result = match prepared.start_action().await {
            Ok(action) => {
                datafusion_physical_plan::collect(action, Arc::new(TaskContext::default())).await
            }
            Err(error) => Err(datafusion_common::DataFusionError::External(Box::new(
                error,
            ))),
        };
        if missing {
            assert!(result.unwrap_err().to_string().contains("No such file"));
            continue;
        }
        let mut values = Vec::new();
        for batch in result.unwrap() {
            let indices = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let prefixes = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            values.extend((0..batch.num_rows()).map(|i| (indices.value(i), prefixes.value(i))));
        }
        values.sort_unstable();
        assert_eq!(values, vec![(0, 7), (1, 8), (2, 9), (3, 10)]);
    }
}

#[tokio::test]
async fn wide_scan_projections_preserve_distinct_arguments_and_shared_aliases() {
    for generic in [false, true] {
        let mut fields = vec![("idx".into(), Expr::Column { name: "idx".into() })];
        for offset in 0..64 {
            let scan = Expr::Aggregate {
                function: "sum".into(),
                value: Some(Arc::new(binary(
                    BinaryOperator::Add,
                    Expr::Column { name: "idx".into() },
                    integer(offset),
                ))),
                options: serde_json::json!({"__vail_scan": true}),
                result_type: None,
                init: vec![],
            };
            fields.push((format!("sum_{offset}"), scan.clone()));
            if offset % 8 == 0 {
                fields.push((format!("shared_{offset}"), scan));
            }
        }
        if generic {
            // A different reducer exercises the general configured-state path.
            fields.push((
                "minimum".into(),
                Expr::Aggregate {
                    function: "min".into(),
                    value: Some(Arc::new(Expr::Column { name: "idx".into() })),
                    options: serde_json::json!({"__vail_scan": true}),
                    result_type: None,
                    init: vec![],
                },
            ));
        }
        let table = TableIR::range(9, 4).unwrap().map_rows(fields).unwrap();
        let batches = table.collect_partitions().await.unwrap();
        let mut seen = Vec::new();
        for batch in batches.into_iter().flatten() {
            let indices = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let index = i64::from(indices.value(row));
                seen.push(index);
                for (column, field) in batch.schema().fields().iter().enumerate() {
                    let name = field.name();
                    let Some(offset) = name
                        .strip_prefix("sum_")
                        .or_else(|| name.strip_prefix("shared_"))
                    else {
                        continue;
                    };
                    let offset = offset.parse::<i64>().unwrap();
                    let values = batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .unwrap();
                    assert_eq!(values.value(row), index * (index - 1) / 2 + offset * index);
                }
            }
        }
        seen.sort_unstable();
        assert_eq!(seen, (0..9).collect::<Vec<_>>());
    }
}
