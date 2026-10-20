//! Public TableIR coverage for grouped configured histograms.
use arrow_array::{
    Array, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::Expr;
use vail_table::TableIR;

fn column(name: &str) -> Expr {
    Expr::Column { name: name.into() }
}

fn histogram(value: Expr) -> Expr {
    Expr::Aggregate {
        function: "hist".into(),
        value: Some(Arc::new(value)),
        options: json!({"start": 0.0, "end": 4.0, "bins": 2}),
        result_type: None,
        init: vec![],
    }
}

#[tokio::test]
async fn grouped_histogram_uses_native_group_state_across_partitions() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int32, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let source = TableIR::parallelize(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0, 1, 1, 1])),
                Arc::new(Float64Array::from(vec![
                    Some(-1.0),
                    Some(0.0),
                    Some(1.9),
                    Some(2.0),
                    Some(4.0),
                    Some(5.0),
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )
        .unwrap(),
        3,
    )
    .unwrap();
    let grouped = source
        .grouped_aggregate_bound(
            vec![("group".into(), column("group"))],
            vec![("histogram".into(), histogram(column("value")))],
            Default::default(),
        )
        .unwrap();

    let mut result = Vec::new();
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
        let histograms = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let frequencies = histograms
            .column_by_name("bin_freq")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let smaller = histograms
            .column_by_name("n_smaller")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let larger = histograms
            .column_by_name("n_larger")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let row_frequencies = frequencies
                .value(row)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec();
            result.push((
                groups.value(row),
                row_frequencies,
                smaller.value(row),
                larger.value(row),
            ));
        }
    }
    result.sort_by_key(|(group, ..)| *group);
    assert_eq!(result, vec![(0, vec![2, 2], 1, 0), (1, vec![0, 0], 0, 1)]);
}
