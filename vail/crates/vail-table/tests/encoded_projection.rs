//! Native scans reduce encoded lists before Arrow while DF owns global SUM.
use arrow_array::{Array, Int32Array, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{ArrayReduction, Expr, PrimitiveType, Type};
use vail_table::{TableIR, globals::GlobalsIR};

fn reduction(filter_missing: bool) -> Expr {
    Expr::ArrayReduce {
        operation: ArrayReduction::Sum,
        filter_missing,
        value: Arc::new(Expr::ArrayMap {
            value: Arc::new(Expr::Column {
                name: "entries".into(),
            }),
            parameter: "e".into(),
            body: Arc::new(Expr::Cast {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: Arc::new(Expr::GetField {
                    value: Arc::new(Expr::LambdaVariable {
                        name: "e".into(),
                        dtype: None,
                    }),
                    name: "DP".into(),
                }),
            }),
        }),
    }
}
fn has(plan: &dyn datafusion_physical_plan::ExecutionPlan, name: &str) -> bool {
    plan.name() == name || plan.children().iter().any(|p| has(p.as_ref(), name))
}
#[tokio::test]
async fn native_nested_projection_and_global_sum_preserve_nulls_and_widening() {
    let values = Arc::new(Int32Array::from(vec![
        Some(i32::MAX),
        Some(i32::MAX),
        Some(999),
        None,
        Some(-5),
        Some(8),
    ]));
    let fields = vec![Arc::new(Field::new("DP", DataType::Int32, true))];
    let entries = StructArray::new(
        fields.clone().into(),
        vec![values],
        Some(NullBuffer::from(vec![true, true, false, true, true, true])),
    );
    let lists = ListArray::new(
        Arc::new(Field::new("item", entries.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4, 4, 6, 6])),
        Arc::new(entries),
        Some(NullBuffer::from(vec![true, true, true, true, false])),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int32, false),
        Field::new("entries", lists.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
            Arc::new(lists),
        ],
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native");
    let path = path.to_str().unwrap();
    TableIR::parallelize(batch, 2)
        .unwrap()
        .key_by(vec!["idx".into()])
        .unwrap()
        .write(path, false)
        .await
        .unwrap();
    let source = TableIR::read(path).await.unwrap();
    for filter_missing in [true, false] {
        let mapped = source
            .map_rows(vec![
                ("idx".into(), Expr::Column { name: "idx".into() }),
                ("sum".into(), reduction(filter_missing)),
            ])
            .unwrap();
        let physical = vail_table::plan::physical(&mapped).await.unwrap();
        assert_eq!(
            has(physical.as_ref(), "VortexComputedProjectionExec"),
            filter_missing
        );
        let batches = mapped.collect_partitions().await.unwrap();
        let got = batches
            .iter()
            .flatten()
            .flat_map(|b| {
                b.column_by_name("sum")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            got,
            if filter_missing {
                vec![
                    Some(i64::from(i32::MAX) * 2),
                    Some(0),
                    Some(0),
                    Some(3),
                    None,
                ]
            } else {
                vec![Some(i64::from(i32::MAX) * 2), None, Some(0), Some(3), None]
            }
        );
        let aggregate = GlobalsIR::aggregate(
            mapped,
            Expr::Aggregate {
                function: "sum".into(),
                value: Some(Arc::new(Expr::Column { name: "sum".into() })),
                options: json!({}),
                result_type: None,
                init: vec![],
            },
            Default::default(),
        )
        .unwrap();
        let result = aggregate.evaluate().await.unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::from(i32::MAX) * 2 + 3
        );
    }
    let direct = GlobalsIR::aggregate(
        source,
        Expr::Aggregate {
            function: "sum".into(),
            value: Some(Arc::new(reduction(true))),
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let result = direct.evaluate().await.unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::from(i32::MAX) * 2 + 3
    );
}
