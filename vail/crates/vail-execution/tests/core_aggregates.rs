//! Public numeric UDAFs executed on sliced and nullable Arrow batches.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use arrow_schema::{Field, Schema};
use datafusion_common::DataFusionError;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::{AggregateUDF, EmitTo, function::AccumulatorArgs};
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
#[tokio::test]
async fn integer_product_wraps_with_widened_output() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(i32::MAX), Some(2)]));
    let udf =
        vail_execution::aggregates::aggregate_udf("product", Some(input.data_type())).unwrap();
    let result = execute(udf, vec![input]).await.unwrap();
    assert_eq!(result.data_type(), &arrow_schema::DataType::Int64);
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::from(i32::MAX) * 2
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

#[test]
fn grouped_product_filters_merges_emits_and_keeps_empty_groups_at_one() {
    let input = Field::new("value", arrow_schema::DataType::Int32, true);
    let schema = Arc::new(Schema::new(vec![input.clone()]));
    let udf =
        vail_execution::aggregates::aggregate_udf("product", Some(input.data_type())).unwrap();
    let args = AccumulatorArgs {
        return_field: Arc::new(Field::new("product", arrow_schema::DataType::Int64, true)),
        schema: &schema,
        name: "product",
        is_distinct: false,
        exprs: &[],
        expr_fields: &[input.clone().into()],
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
    };
    assert!(udf.groups_accumulator_supported(args.clone()));
    let mut grouped = udf.create_groups_accumulator(args.clone()).unwrap();
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(3), None, Some(5)]));
    let filter = BooleanArray::from(vec![true, true, true, false]);
    grouped
        .update_batch(&[values], &[0, 1, 2, 2], Some(&filter), 4)
        .unwrap();
    let partial = grouped.state(EmitTo::First(2)).unwrap();
    let first = partial[1].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(first.iter().collect::<Vec<_>>(), vec![Some(2), Some(3)]);

    let mut merged = udf.create_groups_accumulator(args).unwrap();
    merged.merge_batch(&partial, &[0, 1], 4).unwrap();
    let rest = grouped.state(EmitTo::All).unwrap();
    merged.merge_batch(&rest, &[0, 1], 4).unwrap();
    let results = merged.evaluate(EmitTo::All).unwrap();
    let results = results.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        results.iter().collect::<Vec<_>>(),
        vec![Some(2), Some(3), Some(1), Some(1)]
    );
}

#[test]
fn grouped_float_product_convert_to_state_preserves_null_identity() {
    let input = Field::new("value", arrow_schema::DataType::Float32, true);
    let schema = Arc::new(Schema::new(vec![input.clone()]));
    let udf =
        vail_execution::aggregates::aggregate_udf("product", Some(input.data_type())).unwrap();
    let args = AccumulatorArgs {
        return_field: Arc::new(Field::new("product", arrow_schema::DataType::Float64, true)),
        schema: &schema,
        name: "product",
        is_distinct: false,
        exprs: &[],
        expr_fields: &[input.clone().into()],
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
    };
    let grouped = udf.create_groups_accumulator(args).unwrap();
    let values: ArrayRef = Arc::new(Float32Array::from(vec![Some(2.), None, Some(4.)]));
    let state = grouped.convert_to_state(&[values], None).unwrap();
    let products = state[1].as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(
        products.iter().collect::<Vec<_>>(),
        vec![Some(2.), Some(1.), Some(4.)]
    );
}
