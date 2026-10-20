//! Public TableIR coverage for native fraction aggregation and scan state.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, ListArray, RecordBatch,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_execution::TaskContext;
use futures::TryStreamExt;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType as P, Type};
use vail_table::{ScalarIR, TableIR, globals::GlobalsIR, partitioned};

fn column(name: &str) -> Expr {
    Expr::Column { name: name.into() }
}

fn fraction(value: Expr, scan: bool) -> Expr {
    Expr::Aggregate {
        function: "fraction".into(),
        value: Some(Arc::new(value)),
        options: if scan {
            json!({"__vail_scan": true})
        } else {
            json!({})
        },
        result_type: None,
        init: vec![],
    }
}

fn table(schema: Arc<Schema>, columns: Vec<ArrayRef>, partitions: usize) -> Arc<TableIR> {
    TableIR::parallelize(RecordBatch::try_new(schema, columns).unwrap(), partitions).unwrap()
}

#[tokio::test]
async fn total_and_grouped_fraction_follow_hail_empty_and_null_semantics() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int32, false),
        Field::new("predicate", DataType::Boolean, true),
    ]));
    let source = table(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 0, 1, 1])),
            Arc::new(BooleanArray::from(vec![
                None,
                None,
                Some(true),
                Some(false),
            ])),
        ],
        2,
    );
    let grouped = source
        .grouped_aggregate_bound(
            vec![("group".into(), column("group"))],
            vec![("fraction".into(), fraction(column("predicate"), false))],
            Default::default(),
        )
        .unwrap();
    let mut rows = Vec::new();
    for batch in grouped
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
    {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (groups.value(row), values.value(row))));
    }
    rows.sort_by_key(|(group, _)| *group);
    assert_eq!(rows, vec![(0, 0.0), (1, 0.5)]);

    let total = GlobalsIR::aggregate(
        source.clone(),
        ScalarIR::Aggregate {
            function: "fraction".into(),
            value: Some(Arc::new(column("predicate"))),
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap()
    .evaluate()
    .await
    .unwrap();
    let values = total
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.value(0), 0.25);

    let empty = TableIR::range(0, 1).unwrap();
    let total = GlobalsIR::aggregate(
        empty,
        ScalarIR::Aggregate {
            function: "fraction".into(),
            value: Some(Arc::new(ScalarIR::Literal {
                dtype: Type::Primitive(P::Bool),
                value: json!(true),
            })),
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap()
    .evaluate()
    .await
    .unwrap();
    let values = total
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![None]);
}

#[tokio::test]
async fn fraction_list_reducer_keeps_missing_and_empty_lists_null() {
    // Rows are [true, missing], missing list, empty list, and [false].
    let lists = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Boolean, true)),
        OffsetBuffer::from_lengths([2, 0, 0, 1]),
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Some(NullBuffer::from(vec![true, false, true, true])),
    )) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("predicates", lists.data_type().clone(), true),
        Field::new("id", DataType::Int32, false),
    ]));
    let source = table(
        schema,
        vec![lists, Arc::new(Int32Array::from(vec![0, 1, 2, 3]))],
        2,
    );
    let reduced = source
        .map_rows(vec![
            ("id".into(), column("id")),
            (
                "fraction".into(),
                Expr::NestedFunction {
                    function: "__vail_list_reduce_fraction".into(),
                    args: vec![column("predicates")],
                    dtype: Type::Primitive(P::Float64),
                },
            ),
        ])
        .unwrap();
    let mut result = Vec::new();
    for batch in reduced
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
    {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let fractions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        result.extend((0..batch.num_rows()).map(|row| (ids.value(row), fractions.value(row))));
    }
    result.sort_by_key(|(id, _)| *id);
    assert_eq!(
        result,
        vec![(0, Some(0.5)), (1, None), (2, None), (3, Some(0.0))]
    );
}

#[tokio::test]
async fn filtered_exploded_table_scan_and_stream_partition_keep_exclusive_state() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("keep", DataType::Boolean, false),
        Field::new(
            "predicates",
            DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true))),
            true,
        ),
    ]));
    let lists = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Boolean, true)),
        OffsetBuffer::from_lengths([2, 2]),
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
        None,
    ));
    let source = table(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(BooleanArray::from(vec![true, false])),
            lists,
        ],
        2,
    );
    let exploded = source
        .filter(column("keep"), true)
        .unwrap()
        .explode(vec!["predicates".into()])
        .unwrap();
    let scanned = exploded
        .map_rows(vec![
            ("id".into(), column("id")),
            ("predicate".into(), column("predicates")),
            ("prefix".into(), fraction(column("predicates"), true)),
        ])
        .unwrap();
    let mut rows = Vec::new();
    for batch in scanned
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
    {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let predicates = batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let prefixes = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                ids.value(row),
                predicates.iter().nth(row).flatten(),
                prefixes.iter().nth(row).flatten(),
            )
        }));
    }
    rows.sort_by_key(|(id, _, _)| *id);
    assert_eq!(rows, vec![(0, Some(true), None), (0, None, Some(1.0))]);

    let scalar_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("predicate", DataType::Boolean, true),
    ]));
    let scalar_source = table(
        scalar_schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
        ],
        2,
    );
    let scalar_scan = scalar_source
        .map_rows(vec![
            ("id".into(), column("id")),
            ("prefix".into(), fraction(column("predicate"), true)),
        ])
        .unwrap();
    let action = partitioned::prepare_action(&scalar_scan)
        .unwrap()
        .start_action()
        .await
        .unwrap();
    let mut stream = action.execute(1, Arc::new(TaskContext::default())).unwrap();
    let batch = stream.try_next().await.unwrap().unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let prefixes = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2, 3]);
    assert_eq!(
        prefixes.iter().collect::<Vec<_>>(),
        vec![Some(0.5), Some(1.0 / 3.0)]
    );
}
