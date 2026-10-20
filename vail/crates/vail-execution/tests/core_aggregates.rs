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
    coalesce_partitions::CoalescePartitionsExec,
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
async fn any_uses_native_grouped_bool_or_and_hail_false_identity() {
    use arrow_schema::DataType;

    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int32, false),
        Field::new("value", DataType::Boolean, true),
        Field::new("keep", DataType::Boolean, false),
    ]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(BooleanArray::from(vec![true, true, true])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 4, 4])),
            Arc::new(BooleanArray::from(vec![
                None,
                None,
                Some(true),
                Some(false),
            ])),
            Arc::new(BooleanArray::from(vec![true, true, false, true])),
        ],
    )
    .unwrap();
    let source =
        MemorySourceConfig::try_new_exec(&[vec![first], vec![second]], schema.clone(), None)
            .unwrap();
    let udf = vail_execution::aggregates::aggregate_udf("any", Some(&DataType::Boolean)).unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 1))])
        .schema(schema.clone())
        .alias("any")
        .build()
        .unwrap();
    assert!(aggregate.groups_accumulator_supported());
    let partial = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("group", 0)),
                "group".to_owned(),
            )]),
            vec![Arc::new(aggregate)],
            vec![Some(Arc::new(Column::new("keep", 2)))],
            source.clone(),
            source.schema(),
        )
        .unwrap(),
    );
    let partial_schema = partial.schema();
    let final_aggregate = AggregateExprBuilder::new(
        vail_execution::aggregates::aggregate_udf("any", Some(&DataType::Boolean)).unwrap(),
        vec![Arc::new(Column::new(partial_schema.field(1).name(), 1))],
    )
    .schema(partial_schema.clone())
    .alias("any")
    .build()
    .unwrap();
    assert!(final_aggregate.groups_accumulator_supported());
    let partial_input: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(partial.clone()));
    let plan = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("group", 0)),
                "group".to_owned(),
            )]),
            vec![Arc::new(final_aggregate)],
            vec![None],
            partial_input.clone(),
            partial_input.schema(),
        )
        .unwrap(),
    );
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap();
    let mut rows = Vec::new();
    for result in &batches {
        let groups = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let any = result
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        rows.extend((0..result.num_rows()).map(|row| {
            assert!(!any.is_null(row));
            (groups.value(row), any.value(row))
        }));
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, true), (2, false), (3, false), (4, false)]);
}

#[tokio::test]
async fn any_empty_and_all_null_inputs_are_false() {
    use arrow_schema::DataType;

    let udf = vail_execution::aggregates::aggregate_udf("any", Some(&DataType::Boolean)).unwrap();
    for input in [
        BooleanArray::from(Vec::<Option<bool>>::new()),
        BooleanArray::from(vec![None, None]),
        BooleanArray::from(vec![Some(false), None]),
    ] {
        let native = execute(
            datafusion_functions_aggregate::bool_and_or::bool_or_udaf(),
            vec![Arc::new(input.clone())],
        )
        .await
        .unwrap();
        let native = native.as_any().downcast_ref::<BooleanArray>().unwrap();
        if input.iter().all(|value| value.is_none()) {
            // Native BOOL_OR correctly represents SQL's unknown result. Hail
            // `any` differs only at its total Boolean empty/all-missing identity.
            assert!(native.is_null(0));
        }
        let result = execute(udf.clone(), vec![Arc::new(input)]).await.unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![Some(false)]);
    }
}

#[test]
fn any_rejects_non_boolean_declared_type_and_keeps_boolean_signature() {
    use arrow_schema::DataType;

    assert!(vail_execution::aggregates::aggregate_udf("any", Some(&DataType::Int32)).is_err());
    let udf = vail_execution::aggregates::aggregate_udf("any", None).unwrap();
    assert_eq!(
        udf.return_type(&[DataType::Boolean]).unwrap(),
        DataType::Boolean
    );
    assert_eq!(
        udf.signature().type_signature,
        datafusion_expr::TypeSignature::Exact(vec![DataType::Boolean])
    );
}
#[tokio::test]
async fn mean_and_population_stdev_accept_sliced_nullable_batches() {
    let input: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(999.),
        Some(1.),
        None,
        Some(3.),
        Some(999.),
    ]));
    let input = input.slice(1, 3);
    for (name, expected) in [("mean", 2.), ("stddev_pop", 1.)] {
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
async fn population_stdev_maps_empty_and_nonfinite_results_to_nan() {
    for input in [
        Arc::new(Float64Array::from(vec![None, None])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(f64::NAN)])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(f64::INFINITY)])) as ArrayRef,
    ] {
        let udf = vail_execution::aggregates::aggregate_udf("stddev_pop", Some(input.data_type()))
            .unwrap();
        let result = execute(udf, vec![input]).await.unwrap();
        assert!(
            result
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .is_nan()
        );
    }
}

#[tokio::test]
async fn native_mean_casts_numeric_inputs_and_preserves_empty_nan() {
    for (input, expected) in [
        (
            Arc::new(Int32Array::from(vec![Some(1), None, Some(4)])) as ArrayRef,
            2.5,
        ),
        (
            Arc::new(Int64Array::from(vec![Some(-2), Some(3)])) as ArrayRef,
            0.5,
        ),
        (
            Arc::new(Float32Array::from(vec![Some(1.25), None, Some(2.75)])) as ArrayRef,
            2.0,
        ),
    ] {
        let udf =
            vail_execution::aggregates::aggregate_udf("mean", Some(input.data_type())).unwrap();
        let result = execute(udf, vec![input]).await.unwrap();
        let values = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0), expected);
    }

    let empty: ArrayRef = Arc::new(Float64Array::from(Vec::<Option<f64>>::new()));
    let udf = vail_execution::aggregates::aggregate_udf("mean", Some(empty.data_type())).unwrap();
    let result = execute(udf, vec![empty]).await.unwrap();
    assert!(
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .is_nan()
    );

    let missing: ArrayRef = Arc::new(Float64Array::from(vec![None, None]));
    let udf = vail_execution::aggregates::aggregate_udf("mean", Some(missing.data_type())).unwrap();
    let result = execute(udf, vec![missing]).await.unwrap();
    assert!(
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .is_nan()
    );
}

#[test]
fn native_mean_partial_state_and_grouped_filter_merge() {
    let input = Field::new("value", arrow_schema::DataType::Int32, true);
    let schema = Arc::new(Schema::new(vec![input.clone()]));
    let udf = vail_execution::aggregates::aggregate_udf("mean", Some(input.data_type())).unwrap();
    let args = AccumulatorArgs {
        return_field: Arc::new(Field::new("mean", arrow_schema::DataType::Float64, true)),
        schema: &schema,
        name: "mean",
        is_distinct: false,
        exprs: &[],
        expr_fields: &[input.clone().into()],
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
    };

    let mut partial = udf.accumulator(args.clone()).unwrap();
    partial
        .update_batch(&[Arc::new(Int32Array::from(vec![Some(2), None, Some(6)]))])
        .unwrap();
    let state = partial.state().unwrap();
    assert_eq!(state.len(), 2);
    let states = state
        .into_iter()
        .map(|value| datafusion_common::ScalarValue::iter_to_array(vec![value]).unwrap())
        .collect::<Vec<_>>();
    let mut final_accumulator = udf.accumulator(args.clone()).unwrap();
    final_accumulator.merge_batch(&states).unwrap();
    let datafusion_common::ScalarValue::Float64(Some(value)) =
        final_accumulator.evaluate().unwrap()
    else {
        panic!("mean finalizer should return a defined float64");
    };
    assert_eq!(value, 4.0);

    assert!(udf.groups_accumulator_supported(args.clone()));
    let mut grouped = udf.create_groups_accumulator(args.clone()).unwrap();
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(6), None, Some(9)]));
    let filter = BooleanArray::from(vec![true, true, true, false]);
    grouped
        .update_batch(&[values], &[0, 0, 1, 1], Some(&filter), 3)
        .unwrap();
    let partial = grouped.state(EmitTo::All).unwrap();
    let mut merged = udf.create_groups_accumulator(args).unwrap();
    merged.merge_batch(&partial, &[0, 1, 2], 3).unwrap();
    let result = merged.evaluate(EmitTo::All).unwrap();
    let values = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.value(0), 4.0);
    assert!(values.value(1).is_nan());
    assert!(values.value(2).is_nan());
}

#[test]
fn native_mean_keeps_sum_overflow_semantics_across_partial_merge() {
    let input = Field::new("value", arrow_schema::DataType::Float64, true);
    let schema = Arc::new(Schema::new(vec![input.clone()]));
    let udf = vail_execution::aggregates::aggregate_udf("mean", Some(input.data_type())).unwrap();
    let args = AccumulatorArgs {
        return_field: Arc::new(Field::new("mean", arrow_schema::DataType::Float64, true)),
        schema: &schema,
        name: "mean",
        is_distinct: false,
        exprs: &[],
        expr_fields: &[input.clone().into()],
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
    };

    let mut partials = Vec::new();
    for _ in 0..2 {
        let mut partial = udf.accumulator(args.clone()).unwrap();
        partial
            .update_batch(&[Arc::new(Float64Array::from(vec![Some(1e308)]))])
            .unwrap();
        partials.push(partial.state().unwrap());
    }
    let states = (0..partials[0].len())
        .map(|field| {
            datafusion_common::ScalarValue::iter_to_array(
                partials.iter().map(|state| state[field].clone()),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut merged = udf.accumulator(args).unwrap();
    merged.merge_batch(&states).unwrap();
    let datafusion_common::ScalarValue::Float64(Some(value)) = merged.evaluate().unwrap() else {
        panic!("mean finalizer should return a defined float64");
    };
    assert!(value.is_infinite() && value.is_sign_positive());
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
async fn fraction_delegates_null_as_false_and_keeps_empty_null() {
    assert!(vail_execution::aggregates::aggregate_udf(
        "fraction",
        Some(&arrow_schema::DataType::Int64),
    )
    .is_err());
    let udf = vail_execution::aggregates::aggregate_udf(
        "fraction",
        Some(&arrow_schema::DataType::Boolean),
    )
    .unwrap();
    let first: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None]));
    let second: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true)]));
    let result = execute(udf.clone(), vec![first, second]).await.unwrap();
    let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(result.value(0), 0.5);

    let empty: ArrayRef = Arc::new(BooleanArray::from(Vec::<Option<bool>>::new()));
    let result = execute(udf, vec![empty]).await.unwrap();
    let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(result.iter().collect::<Vec<_>>(), vec![None]);
}

#[tokio::test]
async fn grouped_fraction_invocation_uses_native_groups_accumulator() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", arrow_schema::DataType::Int32, false),
        Field::new("value", arrow_schema::DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 1, 2, 3])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(true),
            ])),
        ],
    )
    .unwrap();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let udf = vail_execution::aggregates::aggregate_udf(
        "fraction",
        Some(&arrow_schema::DataType::Boolean),
    )
    .unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 1))])
        .schema(schema.clone())
        .alias("fraction")
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
    let fractions = result
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut rows = (0..result.num_rows())
        .map(|row| (groups.value(row), fractions.value(row)))
        .collect::<Vec<_>>();
    rows.sort_by_key(|(group, _)| *group);
    assert_eq!(rows, vec![(1, 0.5), (2, 0.5), (3, 1.0)]);
}

#[test]
fn grouped_fraction_convert_to_state_applies_filter_and_merges_native_state() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow_schema::DataType::Boolean,
        true,
    )]));
    let udf = vail_execution::aggregates::aggregate_udf(
        "fraction",
        Some(&arrow_schema::DataType::Boolean),
    )
    .unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 0))])
        .schema(schema)
        .alias("fraction")
        .build()
        .unwrap();
    assert!(aggregate.groups_accumulator_supported());
    let groups = aggregate.create_groups_accumulator().unwrap();
    let values: ArrayRef = Arc::new(BooleanArray::from(vec![
        Some(true),
        None,
        Some(false),
        Some(true),
    ]));
    let filter = BooleanArray::from(vec![true, true, true, false]);
    let states = groups.convert_to_state(&[values], Some(&filter)).unwrap();
    let mut merged = aggregate.create_groups_accumulator().unwrap();
    merged.merge_batch(&states, &[0, 0, 1, 1], 3).unwrap();
    let result = merged.evaluate(EmitTo::All).unwrap();
    let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(
        result.iter().collect::<Vec<_>>(),
        vec![Some(0.5), Some(0.0), None]
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
#[tokio::test]
async fn configured_max_uses_mergeable_datafusion_accumulator() {
    use arrow_array::Int32Array;

    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(7), Some(3)]));
    let udf = vail_execution::reducer::configured_udaf(
        "max",
        Some(input.data_type()),
        &serde_json::Value::Null,
    )
    .unwrap();
    let result = execute(udf, vec![input.slice(0, 2), input.slice(2, 2)])
        .await
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );

    let empty: ArrayRef = Arc::new(Int32Array::from(Vec::<Option<i32>>::new()));
    let udf = vail_execution::reducer::configured_udaf(
        "max",
        Some(empty.data_type()),
        &serde_json::Value::Null,
    )
    .unwrap();
    let result = execute(udf, vec![empty]).await.unwrap();
    assert!(result.is_null(0));
}

#[tokio::test]
async fn configured_max_preserves_numeric_types_and_empty_results() {
    macro_rules! check_empty {
        ($array:expr) => {{
            let input: ArrayRef = Arc::new($array);
            let input_type = input.data_type().clone();
            let udf = vail_execution::reducer::configured_udaf(
                "max",
                Some(&input_type),
                &serde_json::Value::Null,
            )
            .unwrap();
            let result = execute(udf, vec![input]).await.unwrap();
            assert_eq!(result.data_type(), &input_type);
            assert!(result.is_null(0));
        }};
    }

    check_empty!(Int32Array::from(Vec::<Option<i32>>::new()));
    check_empty!(Int64Array::from(Vec::<Option<i64>>::new()));
    check_empty!(Float32Array::from(Vec::<Option<f32>>::new()));
    check_empty!(Float64Array::from(Vec::<Option<f64>>::new()));
}

#[tokio::test]
async fn ordinary_grouped_max_uses_native_groups_accumulator_across_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", arrow_schema::DataType::Int32, false),
        Field::new("value", arrow_schema::DataType::Int32, true),
    ]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![99, 1, 2, 3]).slice(1, 3)),
            Arc::new(Int32Array::from(vec![Some(99), Some(1), None, None]).slice(1, 3)),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![99, 1, 2, 3]).slice(1, 3)),
            Arc::new(Int32Array::from(vec![Some(99), Some(7), Some(4), None]).slice(1, 3)),
        ],
    )
    .unwrap();
    let source =
        MemorySourceConfig::try_new_exec(&[vec![first, second]], schema.clone(), None).unwrap();
    let udf = vail_execution::reducer::configured_udaf(
        "max",
        Some(&arrow_schema::DataType::Int32),
        &serde_json::Value::Null,
    )
    .unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 1))])
        .schema(schema.clone())
        .alias("maximum")
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
    assert_eq!(result.column(1).data_type(), &arrow_schema::DataType::Int32);
    let groups = result
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let maxima = result
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let mut rows = (0..result.num_rows())
        .map(|row| {
            (
                groups.value(row),
                maxima.is_valid(row).then(|| maxima.value(row)),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(group, _)| *group);
    assert_eq!(rows, vec![(1, Some(7)), (2, Some(4)), (3, None)]);
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
async fn direct_count_where_factory_remains_compatible() {
    // Vail planner routes use native COUNT; this covers the legacy public Rust factory.
    let udf = vail_execution::aggregates::aggregate_udf(
        "count_where",
        Some(&arrow_schema::DataType::Boolean),
    )
    .unwrap();
    let input: ArrayRef = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
    ]));
    let result = execute(udf, vec![input.slice(0, 2), input.slice(2, 2)])
        .await
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
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

#[tokio::test]
async fn all_uses_native_bool_and_with_hail_empty_identity() {
    let native = datafusion_functions_aggregate::bool_and_or::bool_and_udaf();
    let hail =
        vail_execution::aggregates::aggregate_udf("all", Some(&arrow_schema::DataType::Boolean))
            .unwrap();
    let cases = [
        BooleanArray::from(Vec::<Option<bool>>::new()),
        BooleanArray::from(vec![None, None]),
        BooleanArray::from(vec![Some(true), None]),
        BooleanArray::from(vec![Some(true), Some(true), None]),
        BooleanArray::from(vec![Some(true), Some(false), None]),
    ];
    for (index, input) in cases.into_iter().enumerate() {
        let input: ArrayRef = Arc::new(input);
        let native_result = execute(native.clone(), vec![input.clone()]).await.unwrap();
        let native_result = native_result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let result = execute(hail.clone(), vec![input]).await.unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        if index <= 1 {
            // DataFusion retains SQL's null for empty and all-null BOOL_AND;
            // Hail's `all` applies the true identity to those cases.
            assert!(native_result.is_null(0));
            assert_eq!(result.iter().collect::<Vec<_>>(), vec![Some(true)]);
        } else {
            assert_eq!(
                native_result.iter().collect::<Vec<_>>(),
                result.iter().collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn all_grouped_native_accumulator_filters_and_merges_partial_state() {
    use arrow_schema::DataType;

    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int32, false),
        Field::new("value", DataType::Boolean, true),
        Field::new("keep", DataType::Boolean, false),
    ]));
    let partitions = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    Some(false),
                ])),
                Arc::new(BooleanArray::from(vec![true, true, true, false])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(BooleanArray::from(vec![None, None, None, Some(false)])),
                Arc::new(BooleanArray::from(vec![true, true, true, false])),
            ],
        )
        .unwrap(),
    ];
    let source = MemorySourceConfig::try_new_exec(
        &[vec![partitions[0].clone()], vec![partitions[1].clone()]],
        schema.clone(),
        None,
    )
    .unwrap();
    let udf = vail_execution::aggregates::aggregate_udf("all", Some(&DataType::Boolean)).unwrap();
    let aggregate = Arc::new(
        AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 1))])
            .schema(schema.clone())
            .alias("all")
            .build()
            .unwrap(),
    );
    assert!(aggregate.groups_accumulator_supported());
    let group_by = PhysicalGroupBy::new_single(vec![(
        Arc::new(Column::new("group", 0)),
        "group".to_owned(),
    )]);
    let partial = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            vec![aggregate.clone()],
            vec![Some(Arc::new(Column::new("keep", 2)))],
            source,
            schema.clone(),
        )
        .unwrap(),
    );
    let merged_input = Arc::new(CoalescePartitionsExec::new(partial));
    let final_aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by.as_final(),
            vec![aggregate],
            vec![None],
            merged_input,
            schema,
        )
        .unwrap(),
    );
    let batches = collect(final_aggregate, Arc::new(TaskContext::default()))
        .await
        .unwrap();
    let result = &batches[0];
    let groups = result
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let values = result
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let mut rows = (0..result.num_rows())
        .map(|row| {
            assert!(!values.is_null(row));
            (groups.value(row), values.value(row))
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    // Group 2 has only missing inputs; group 4 has all values filtered out.
    assert_eq!(rows, vec![(1, true), (2, true), (3, false), (4, true)]);
}

#[test]
fn all_rejects_non_boolean_declared_type_and_keeps_boolean_signature() {
    use arrow_schema::DataType;
    use datafusion_expr::TypeSignature;

    assert!(vail_execution::aggregates::aggregate_udf("all", Some(&DataType::Int32)).is_err());
    let udf = vail_execution::aggregates::aggregate_udf("all", None).unwrap();
    assert_eq!(
        udf.return_type(&[DataType::Boolean]).unwrap(),
        DataType::Boolean
    );
    assert_eq!(
        udf.signature().type_signature,
        TypeSignature::Exact(vec![DataType::Boolean])
    );
    assert_eq!(
        udf.default_value(&DataType::Boolean).unwrap(),
        datafusion_common::ScalarValue::Boolean(Some(true))
    );
}
