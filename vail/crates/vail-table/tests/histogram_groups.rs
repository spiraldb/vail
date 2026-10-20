//! Public TableIR coverage for grouped configured histograms.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
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

fn read_histogram_rows(array: &dyn Array) -> Vec<(Vec<f64>, Vec<i64>, i64, i64)> {
    let values = array.as_any().downcast_ref::<StructArray>().unwrap();
    let edges = values
        .column_by_name("bin_edges")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let frequencies = values
        .column_by_name("bin_freq")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let smaller = values
        .column_by_name("n_smaller")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let larger = values
        .column_by_name("n_larger")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..values.len())
        .map(|row| {
            let edges = edges
                .value(row)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .to_vec();
            let frequencies = frequencies
                .value(row)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec();
            (edges, frequencies, smaller.value(row), larger.value(row))
        })
        .collect()
}

#[test]
fn histogram_groups_filters_state_conversion_and_partial_emit_match() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        true,
    )]));
    let udf = vail_execution::advanced_numeric_aggregates::make_udaf(
        "hist",
        &DataType::Float64,
        &json!({"start": 0.0, "end": 4.0, "bins": 2}),
    )
    .unwrap()
    .unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 0))])
        .schema(schema)
        .alias("histogram")
        .build()
        .unwrap();
    assert!(aggregate.groups_accumulator_supported());
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-1.0),
        None,
        None,
        Some(2.0),
        Some(4.0),
    ]));
    let group_indices = [0, 1, 1, 2, 2];
    let filter = BooleanArray::from(vec![false, true, true, true, false]);

    let mut direct = aggregate.create_groups_accumulator().unwrap();
    direct
        .update_batch(&[Arc::clone(&values)], &group_indices, Some(&filter), 3)
        .unwrap();
    let mut converted = aggregate.create_groups_accumulator().unwrap();
    let converted_state = converted
        .convert_to_state(&[Arc::clone(&values)], Some(&filter))
        .unwrap();
    converted
        .merge_batch(&converted_state, &group_indices, 3)
        .unwrap();
    assert_eq!(
        read_histogram_rows(direct.evaluate(EmitTo::All).unwrap().as_ref()),
        read_histogram_rows(converted.evaluate(EmitTo::All).unwrap().as_ref()),
    );

    let mut emitted = aggregate.create_groups_accumulator().unwrap();
    emitted
        .update_batch(&[values], &group_indices, Some(&filter), 3)
        .unwrap();
    let first = emitted.evaluate(EmitTo::First(1)).unwrap();
    assert_eq!(
        read_histogram_rows(first.as_ref()),
        vec![(vec![0.0, 2.0, 4.0], vec![0, 0], 0, 0)]
    );
    let remaining_state = emitted.state(EmitTo::All).unwrap();
    let mut merged = aggregate.create_groups_accumulator().unwrap();
    merged.merge_batch(&remaining_state, &[0, 1], 2).unwrap();
    assert_eq!(
        read_histogram_rows(merged.evaluate(EmitTo::All).unwrap().as_ref()),
        vec![
            (vec![0.0, 2.0, 4.0], vec![0, 0], 0, 0),
            (vec![0.0, 2.0, 4.0], vec![0, 1], 0, 0),
        ]
    );
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
