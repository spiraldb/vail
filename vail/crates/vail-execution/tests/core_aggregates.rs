//! Public numeric UDAFs executed on sliced and nullable Arrow batches.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::{
    Accumulator, AggregateUDF, EmitTo, GroupsAccumulator, function::AccumulatorArgs,
};
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

fn min_accumulator(udf: &AggregateUDF, schema: &Schema) -> Box<dyn Accumulator> {
    let field = Arc::new(schema.field(0).clone());
    let exprs: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>> =
        vec![Arc::new(Column::new("value", 0))];
    udf.accumulator(AccumulatorArgs {
        return_field: field.clone(),
        schema,
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
        name: "min",
        is_distinct: false,
        exprs: &exprs,
        expr_fields: &[field],
    })
    .unwrap()
}

fn min_groups_accumulator(udf: &AggregateUDF, schema: &Schema) -> Box<dyn GroupsAccumulator> {
    let field = Arc::new(schema.field(0).clone());
    let exprs: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>> =
        vec![Arc::new(Column::new("value", 0))];
    let args = AccumulatorArgs {
        return_field: field.clone(),
        schema,
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
        name: "min",
        is_distinct: false,
        exprs: &exprs,
        expr_fields: &[field],
    };
    assert!(udf.groups_accumulator_supported(args.clone()));
    udf.create_groups_accumulator(args).unwrap()
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

#[tokio::test]
async fn ordinary_grouped_min_uses_native_groups_accumulator() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int32, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 1])),
            Arc::new(Float64Array::from(vec![
                Some(f64::INFINITY),
                Some(4.0),
                Some(f64::INFINITY),
            ])),
        ],
    )
    .unwrap();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let udf = vail_execution::aggregates::aggregate_udf("min", Some(&DataType::Float64)).unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 1))])
        .schema(schema.clone())
        .alias("min")
        .build()
        .unwrap();
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
    let minima = result
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut rows = (0..result.num_rows())
        .map(|row| (groups.value(row), minima.value(row)))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    assert_eq!(rows, vec![(1, f64::INFINITY), (2, 4.0)]);
}

#[tokio::test]
async fn native_grouped_max_preserves_negative_infinity() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int32, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 1])),
            Arc::new(Float64Array::from(vec![
                Some(f64::NEG_INFINITY),
                Some(-4.0),
                Some(f64::NEG_INFINITY),
            ])),
        ],
    )
    .unwrap();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let udf = datafusion_functions_aggregate::min_max::max_udaf();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 1))])
        .schema(schema.clone())
        .alias("max")
        .build()
        .unwrap();
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
    let maxima = result
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut rows = (0..result.num_rows())
        .map(|row| (groups.value(row), maxima.value(row)))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    assert_eq!(rows, vec![(1, f64::NEG_INFINITY), (2, -4.0)]);
}

fn assert_native_float_min_merge(
    data_type: DataType,
    first_partition: Vec<ScalarValue>,
    second_partition: Vec<ScalarValue>,
    expected_finite: ScalarValue,
    expected_infinity: ScalarValue,
) {
    let schema = Schema::new(vec![Field::new("value", data_type.clone(), true)]);
    let udf = vail_execution::aggregates::aggregate_udf("min", Some(&data_type)).unwrap();
    let first = ScalarValue::iter_to_array(first_partition).unwrap();
    let second = ScalarValue::iter_to_array(second_partition).unwrap();
    let mut left = min_groups_accumulator(&udf, &schema);
    left.update_batch(&[first], &[0, 1, 2], None, 3).unwrap();
    let left_state = left.state(EmitTo::All).unwrap();
    let mut right = min_groups_accumulator(&udf, &schema);
    right.update_batch(&[second], &[0, 1, 2], None, 3).unwrap();
    let right_state = right.state(EmitTo::All).unwrap();

    let mut merged = min_groups_accumulator(&udf, &schema);
    merged.merge_batch(&left_state, &[0, 1, 2], 3).unwrap();
    merged.merge_batch(&right_state, &[0, 1, 2], 3).unwrap();
    let result = merged.evaluate(EmitTo::All).unwrap();
    match ScalarValue::try_from_array(&result, 0).unwrap() {
        ScalarValue::Float32(Some(value)) => assert_eq!(value.to_bits(), (-0.0_f32).to_bits()),
        ScalarValue::Float64(Some(value)) => assert_eq!(value.to_bits(), (-0.0_f64).to_bits()),
        value => panic!("expected negative zero, got {value:?}"),
    }
    assert_eq!(
        ScalarValue::try_from_array(&result, 1).unwrap(),
        expected_finite
    );
    assert_eq!(
        ScalarValue::try_from_array(&result, 2).unwrap(),
        expected_infinity
    );
}

#[test]
fn native_float_min_groups_preserve_extreme_values_across_partitions() {
    assert_native_float_min_merge(
        DataType::Float32,
        vec![
            ScalarValue::Float32(Some(-0.0)),
            ScalarValue::Float32(Some(f32::NAN)),
            ScalarValue::Float32(Some(f32::INFINITY)),
        ],
        vec![
            ScalarValue::Float32(Some(0.0)),
            ScalarValue::Float32(Some(3.0)),
            ScalarValue::Float32(Some(f32::INFINITY)),
        ],
        ScalarValue::Float32(Some(3.0)),
        ScalarValue::Float32(Some(f32::INFINITY)),
    );
    assert_native_float_min_merge(
        DataType::Float64,
        vec![
            ScalarValue::Float64(Some(-0.0)),
            ScalarValue::Float64(Some(f64::NAN)),
            ScalarValue::Float64(Some(f64::INFINITY)),
        ],
        vec![
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some(3.0)),
            ScalarValue::Float64(Some(f64::INFINITY)),
        ],
        ScalarValue::Float64(Some(3.0)),
        ScalarValue::Float64(Some(f64::INFINITY)),
    );
}

#[tokio::test]
async fn native_min_preserves_numeric_type_and_skips_nulls() {
    let cases: Vec<(ArrayRef, ScalarValue)> = vec![
        (
            Arc::new(Int32Array::from(vec![
                Some(999),
                Some(8),
                None,
                Some(-3),
                Some(999),
            ])),
            ScalarValue::Int32(Some(-3)),
        ),
        (
            Arc::new(Int64Array::from(vec![
                Some(999),
                Some(8),
                None,
                Some(-3),
                Some(999),
            ])),
            ScalarValue::Int64(Some(-3)),
        ),
        (
            Arc::new(Float32Array::from(vec![
                Some(999.),
                Some(8.5),
                None,
                Some(-3.25),
                Some(999.),
            ])),
            ScalarValue::Float32(Some(-3.25)),
        ),
        (
            Arc::new(Float64Array::from(vec![
                Some(999.),
                Some(8.5),
                None,
                Some(-3.25),
                Some(999.),
            ])),
            ScalarValue::Float64(Some(-3.25)),
        ),
    ];
    for (input, expected) in cases {
        let input = input.slice(1, 3);
        let udf =
            vail_execution::aggregates::aggregate_udf("min", Some(input.data_type())).unwrap();
        let result = execute(udf, vec![input]).await.unwrap();
        assert_eq!(ScalarValue::try_from_array(&result, 0).unwrap(), expected);
    }
}

#[tokio::test]
async fn native_min_returns_null_for_empty_and_all_null_inputs() {
    let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<Option<i64>>::new()));
    let nulls: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
    let udf = vail_execution::aggregates::aggregate_udf("min", Some(&DataType::Int64)).unwrap();
    for input in [empty, nulls] {
        let result = execute(udf.clone(), vec![input]).await.unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&result, 0).unwrap(),
            ScalarValue::Int64(None)
        );
    }
}

#[tokio::test]
async fn configured_min_keeps_the_native_aggregate() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(11), Some(-5), None]));
    let udf = vail_execution::reducer::configured_udaf(
        "min",
        Some(&DataType::Int64),
        &serde_json::Value::Null,
    )
    .unwrap();
    let result = execute(udf, vec![input]).await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(&result, 0).unwrap(),
        ScalarValue::Int64(Some(-5))
    );
}

#[test]
fn native_min_merges_multiple_partitions_for_each_group() {
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
    let udf = vail_execution::aggregates::aggregate_udf("min", Some(&DataType::Int64)).unwrap();
    let mut partials = [
        min_accumulator(&udf, &schema),
        min_accumulator(&udf, &schema),
    ];
    partials[0]
        .update_batch(&[Arc::new(Int64Array::from(vec![Some(9), Some(4), None]))])
        .unwrap();
    partials[1]
        .update_batch(&[Arc::new(Int64Array::from(vec![Some(6), Some(2)]))])
        .unwrap();
    let mut merged = min_accumulator(&udf, &schema);
    let states = partials
        .iter_mut()
        .map(|accumulator| accumulator.state().unwrap()[0].clone())
        .collect::<Vec<_>>();
    let state = ScalarValue::iter_to_array(states).unwrap();
    merged.merge_batch(&[state]).unwrap();
    assert_eq!(merged.evaluate().unwrap(), ScalarValue::Int64(Some(2)));

    let mut other_group = min_accumulator(&udf, &schema);
    other_group
        .update_batch(&[Arc::new(Int64Array::from(vec![Some(100), Some(7)]))])
        .unwrap();
    assert_eq!(other_group.evaluate().unwrap(), ScalarValue::Int64(Some(7)));
}
