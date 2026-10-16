//! Public native UDAF executions over nullable, sliced Arrow input records.
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{Field, Schema};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    execution_plan::{ExecutionPlan, collect},
};
use std::sync::Arc;

async fn aggregate(name: &str, input: ArrayRef, options: serde_json::Value) -> ArrayRef {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "packed",
        input.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![input.clone()]).unwrap();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let udf =
        vail_execution::advanced_numeric_aggregates::make_udaf(name, input.data_type(), &options)
            .unwrap()
            .unwrap();
    let expression = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("packed", 0))])
        .schema(schema)
        .alias("result")
        .build()
        .unwrap();
    let plan = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![]),
            vec![Arc::new(expression)],
            vec![None],
            source.clone(),
            source.schema(),
        )
        .unwrap(),
    );
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap()[0]
        .column(0)
        .clone()
}
fn packed(names: &[&str], columns: Vec<ArrayRef>, valid: Vec<bool>) -> ArrayRef {
    let fields = names
        .iter()
        .zip(&columns)
        .map(|(name, column)| Arc::new(Field::new(*name, column.data_type().clone(), true)))
        .collect::<Vec<_>>();
    Arc::new(StructArray::new(
        fields.into(),
        columns,
        Some(NullBuffer::from(valid)),
    ))
}
#[tokio::test]
async fn correlation_ignores_hidden_children_under_sliced_null_records() {
    let input = packed(
        &["x", "y"],
        vec![
            Arc::new(Float64Array::from(vec![999., 0., 1., 1000., 2.])),
            Arc::new(Float64Array::from(vec![-999., 0., 2., -1e100, 4.])),
        ],
        vec![true, true, true, false, true],
    )
    .slice(1, 4);
    let result = aggregate("corr", input, serde_json::json!({})).await;
    assert!(
        (result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            - 1.)
            .abs()
            < 1e-12
    );
}
#[tokio::test]
async fn regression_ignores_hidden_observations_under_null_records() {
    let xs: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
        Some(vec![Some(1.), Some(0.)]),
        Some(vec![Some(1.), Some(1.)]),
        Some(vec![Some(1.), Some(999.)]),
        Some(vec![Some(1.), Some(2.)]),
        Some(vec![Some(1.), Some(3.)]),
    ]));
    let input = packed(
        &["y", "x"],
        vec![
            Arc::new(Float64Array::from(vec![2., 5., 1e100, 8., 11.])),
            xs,
        ],
        vec![true, true, false, true, true],
    );
    let result = aggregate(
        "linreg",
        input,
        serde_json::json!({"n_predictors":2,"nested_dim":1}),
    )
    .await;
    let result = result.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        result
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4
    );
    let beta = result
        .column_by_name("beta")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let beta = beta.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((beta.value(0) - 2.).abs() < 1e-10);
    assert!((beta.value(1) - 3.).abs() < 1e-10);
}
#[tokio::test]
async fn downsample_ignores_hidden_finite_points_under_null_records() {
    let input = packed(
        &["x", "y"],
        vec![
            Arc::new(Float64Array::from(vec![0., 1000., 1.])),
            Arc::new(Float64Array::from(vec![0., 1000., 1.])),
        ],
        vec![true, false, true],
    );
    let result = aggregate("downsample", input, serde_json::json!({"n_divisions":4})).await;
    let values = result
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let points = values.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(points.len(), 2);
    let x = points
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(x.values(), &[0., 1.]);
}
