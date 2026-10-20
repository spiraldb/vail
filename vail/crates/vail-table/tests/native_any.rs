//! Hail `any` stays total through public table aggregate and scan paths.
use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::Expr;
use vail_table::{ScalarIR, TableIR, globals::GlobalBindings};

fn column(name: &str) -> Expr {
    Expr::Column { name: name.into() }
}

fn any(value: &str) -> Expr {
    Expr::Aggregate {
        result_type: None,
        function: "any".into(),
        value: Some(Arc::new(column(value))),
        options: json!({}),
        init: vec![],
    }
}

fn bool_rows() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int32, false),
        Field::new("value", DataType::Boolean, true),
        Field::new("keep", DataType::Boolean, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 1, 2, 4, 4])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                None,
                None,
                Some(true),
                Some(false),
            ])),
            Arc::new(BooleanArray::from(vec![
                true, true, true, true, true, false, true,
            ])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn grouped_filtered_any_uses_false_for_all_missing_groups() {
    let source = TableIR::parallelize(bool_rows(), 2).unwrap();
    let filtered_any = ScalarIR::AggFilter {
        predicate: Arc::new(column("keep")),
        body: Arc::new(any("value")),
    };
    let result = source
        .grouped_aggregate_bound(
            vec![("group".into(), column("group"))],
            vec![("any".into(), filtered_any)],
            GlobalBindings::new(),
        )
        .unwrap();
    let batches = result.collect_partitions().await.unwrap();
    let mut rows = Vec::new();
    for batch in batches.into_iter().flatten() {
        let groups = batch
            .column_by_name("group")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("any")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!values.is_null(row));
            rows.push((groups.value(row), values.value(row)));
        }
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, true), (2, false), (3, false), (4, false)]);
}

#[tokio::test]
async fn any_table_scan_applies_aggregate_filter_and_starts_at_false() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Boolean, true),
        Field::new("keep", DataType::Boolean, false),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
            ])),
            Arc::new(BooleanArray::from(vec![false, true, true, true])),
        ],
    )
    .unwrap();
    let scanned = TableIR::parallelize(rows, 1)
        .unwrap()
        .map_rows(vec![(
            "prefix".into(),
            Expr::AggFilter {
                predicate: Arc::new(column("keep")),
                body: Arc::new(Expr::Aggregate {
                    result_type: None,
                    function: "any".into(),
                    value: Some(Arc::new(column("value"))),
                    options: json!({"__vail_scan": true}),
                    init: vec![],
                }),
            },
        )])
        .unwrap();
    let batches = scanned.collect_partitions().await.unwrap();
    let values = batches
        .into_iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column_by_name("prefix")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![Some(false), Some(false), Some(false), Some(true)]
    );
}

#[tokio::test]
async fn empty_table_any_is_false() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Boolean,
        true,
    )]));
    let empty = RecordBatch::new_empty(schema);
    let source = TableIR::parallelize(empty, 1).unwrap();
    let result =
        vail_table::globals::GlobalsIR::aggregate(source, any("value"), GlobalBindings::new())
            .unwrap()
            .evaluate()
            .await
            .unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false)]
    );
}
