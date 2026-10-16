//! Public numeric UDAFs executed on sliced and nullable Arrow batches.
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{Field, Schema};
use datafusion_common::DataFusionError;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::AggregateUDF;
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    execution_plan::{ExecutionPlan, collect},
};
use std::sync::Arc;

async fn execute(
    udf: Arc<AggregateUDF>,
    batches: Vec<ArrayRef>,
) -> Result<ArrayRef, DataFusionError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        batches[0].data_type().clone(),
        true,
    )]));
    let batches = batches
        .into_iter()
        .map(|array| RecordBatch::try_new(schema.clone(), vec![array]).unwrap())
        .collect::<Vec<_>>();
    let source = MemorySourceConfig::try_new_exec(&[batches], schema.clone(), None)?;
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 0))])
        .schema(schema)
        .alias("result")
        .build()?;
    let plan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![]),
        vec![Arc::new(aggregate)],
        vec![None],
        source.clone(),
        source.schema(),
    )?);
    Ok(collect(plan, Arc::new(TaskContext::default())).await?[0]
        .column(0)
        .clone())
}
#[tokio::test]
async fn stable_moments_accept_sliced_nullable_batches() {
    let input: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(999.),
        Some(-1e308),
        None,
        Some(1e308),
        Some(999.),
    ]));
    let input = input.slice(1, 3);
    for (name, expected) in [("mean", 0.), ("stddev_pop", 1e308)] {
        let udf = vail_execution::aggregates::aggregate_udf(name, Some(input.data_type())).unwrap();
        let result = execute(udf, vec![input.slice(0, 2), input.slice(2, 1)])
            .await
            .unwrap();
        let actual = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(actual, expected);
    }
}
#[tokio::test]
async fn integer_sum_wraps_across_nullable_sliced_batches() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(99),
        Some(i64::MAX),
        None,
        Some(1),
    ]));
    let input = input.slice(1, 3);
    let udf = vail_execution::aggregates::aggregate_udf("sum", Some(input.data_type())).unwrap();
    let result = execute(udf, vec![input.slice(0, 1), input.slice(1, 2)])
        .await
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );
}
#[test]
fn moments_reject_missing_and_nonnumeric_input_types() {
    for name in ["mean", "stddev_pop"] {
        assert!(vail_execution::aggregates::aggregate_udf(name, None).is_err());
        assert!(
            vail_execution::aggregates::aggregate_udf(name, Some(&arrow_schema::DataType::Utf8))
                .is_err()
        );
    }
}
