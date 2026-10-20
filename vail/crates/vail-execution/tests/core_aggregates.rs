//! Public numeric UDAFs executed on sliced and nullable Arrow batches.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use arrow_schema::{Field, Schema};
use datafusion_common::DataFusionError;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::{AggregateUDF, EmitTo, function::AccumulatorArgs};
use datafusion_physical_expr::{
    PhysicalSortExpr, aggregate::AggregateExprBuilder, expressions::Column,
};
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
async fn ordinary_grouped_product_uses_native_groups_accumulator() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", arrow_schema::DataType::Int32, false),
        Field::new("value", arrow_schema::DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 1, 2])),
            Arc::new(Int32Array::from(vec![Some(2), Some(3), Some(4), Some(5)])),
        ],
    )
    .unwrap();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let udf =
        vail_execution::aggregates::aggregate_udf("product", Some(&arrow_schema::DataType::Int32))
            .unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 1))])
        .schema(schema.clone())
        .alias("product")
        .build()
        .unwrap();
    // This is the capability decision used by DataFusion's grouped hash stream;
    // with default aggregate options, false would select GroupsAccumulatorAdapter.
    assert!(aggregate.groups_accumulator_supported());
    let plan = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("group", 0)),
                "group".to_owned(),
            )]),
            vec![Arc::new(aggregate)],
            vec![None],
            source.clone(),
            source.schema(),
        )
        .unwrap(),
    );
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap();
    let result = &batches[0];
    let groups = result
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let products = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows = (0..result.num_rows())
        .map(|row| (groups.value(row), products.value(row)))
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 8), (2, 15)]);
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
#[tokio::test]
async fn int64_product_overflow_wraps() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(2)]));
    let udf =
        vail_execution::aggregates::aggregate_udf("product", Some(input.data_type())).unwrap();
    let result = execute(udf, vec![input]).await.unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -2
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
    let mut distinct = args.clone();
    distinct.is_distinct = true;
    assert!(!udf.groups_accumulator_supported(distinct.clone()));
    assert!(udf.create_groups_accumulator(distinct).is_err());
    let mut reversed = args.clone();
    reversed.is_reversed = true;
    assert!(!udf.groups_accumulator_supported(reversed));
    let order_by = PhysicalSortExpr::new_default(Arc::new(Column::new("value", 0)));
    let mut ordered = args.clone();
    ordered.order_bys = std::slice::from_ref(&order_by);
    assert!(!udf.groups_accumulator_supported(ordered));
    let wrong_field = Arc::new(Field::new("value", arrow_schema::DataType::Boolean, true));
    let mut wrong_type = args.clone();
    wrong_type.expr_fields = std::slice::from_ref(&wrong_field);
    assert!(!udf.groups_accumulator_supported(wrong_type));
    let mut grouped = udf.create_groups_accumulator(args.clone()).unwrap();
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(3), None, Some(5)]));
    let filter = BooleanArray::from(vec![true, true, true, false]);
    grouped
        .update_batch(&[values], &[0, 1, 2, 2], Some(&filter), 4)
        .unwrap();
    let partial = grouped.state(EmitTo::First(2)).unwrap();
    let first = partial[1].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(first.iter().collect::<Vec<_>>(), vec![Some(2), Some(3)]);

    let mut merged = udf.create_groups_accumulator(args.clone()).unwrap();
    merged.merge_batch(&partial, &[0, 1], 4).unwrap();
    let rest = grouped.state(EmitTo::All).unwrap();
    merged.merge_batch(&rest, &[0, 1], 4).unwrap();
    let results = merged.evaluate(EmitTo::All).unwrap();
    let results = results.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        results.iter().collect::<Vec<_>>(),
        vec![Some(2), Some(3), Some(1), Some(1)]
    );

    let mut combined = udf.create_groups_accumulator(args.clone()).unwrap();
    combined.merge_batch(&partial, &[0, 0], 1).unwrap();
    let combined = combined.evaluate(EmitTo::All).unwrap();
    assert_eq!(
        combined
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        6
    );

    let mut nullable = udf.create_groups_accumulator(args).unwrap();
    let nullable_state: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(5), None])),
        Arc::new(Int64Array::from(vec![None, Some(7)])),
    ];
    nullable.merge_batch(&nullable_state, &[0, 1], 2).unwrap();
    let nullable = nullable.evaluate(EmitTo::All).unwrap();
    assert_eq!(
        nullable
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 7]
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
    assert_eq!(
        state[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 0, 1]
    );
    let products = state[1].as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(
        products.iter().collect::<Vec<_>>(),
        vec![Some(2.), Some(1.), Some(4.)]
    );
}

#[test]
fn grouped_int64_product_overflow_wraps() {
    let input = Field::new("value", arrow_schema::DataType::Int64, true);
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
    let mut grouped = udf.create_groups_accumulator(args).unwrap();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(2)]));
    grouped.update_batch(&[values], &[0, 0], None, 1).unwrap();
    let result = grouped.evaluate(EmitTo::All).unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -2
    );
}
