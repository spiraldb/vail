//! Public native UDAF executions over nullable, sliced Arrow input records.
use arrow_array::types::Float64Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::{EmitTo, function::AccumulatorArgs};
use datafusion_physical_expr::{
    PhysicalExpr, aggregate::AggregateExprBuilder, expressions::Column,
};
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
async fn grouped_correlation_handles_sliced_parent_null_filter_and_empty_groups() {
    let input_type = DataType::Struct(
        vec![
            Arc::new(Field::new("x", DataType::Float64, true)),
            Arc::new(Field::new("y", DataType::Float64, true)),
        ]
        .into(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int32, false),
        Field::new("packed", input_type.clone(), true),
        Field::new("keep", DataType::Boolean, true),
    ]));
    let groups: ArrayRef = Arc::new(Int32Array::from(vec![99, 0, 0, 1, 1, 2]).slice(1, 5));
    let records = packed(
        &["x", "y"],
        vec![
            Arc::new(Float64Array::from(vec![777., 1., 2., 999., 3., 4.])),
            Arc::new(Float64Array::from(vec![-777., 2., 4., -999., 6., 8.])),
        ],
        vec![false, true, true, false, true, true],
    )
    .slice(1, 5);
    let keep: ArrayRef = Arc::new(
        BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
        ])
        .slice(1, 5),
    );
    let batch = RecordBatch::try_new(schema.clone(), vec![groups, records, keep]).unwrap();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let udf = vail_execution::advanced_numeric_aggregates::make_udaf(
        "corr",
        &input_type,
        &serde_json::json!({}),
    )
    .unwrap()
    .unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("packed", 1))])
        .schema(schema.clone())
        .alias("corr")
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
            vec![Some(Arc::new(Column::new("keep", 2)))],
            source.clone(),
            source.schema(),
        )
        .unwrap(),
    );
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap();
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
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
            (0..batch.num_rows())
                .map(|row| {
                    (
                        groups.value(row),
                        values.is_valid(row).then(|| values.value(row)),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(group, _)| *group);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (0, Some(1.0)));
    assert_eq!(rows[1].0, 1);
    assert!(rows[1].1.unwrap().is_nan());
    assert_eq!(rows[2], (2, None));
}

#[test]
fn grouped_correlation_converted_states_match_updates_across_emissions() {
    let input_type = DataType::Struct(
        vec![
            Arc::new(Field::new("x", DataType::Float64, true)),
            Arc::new(Field::new("y", DataType::Float64, true)),
        ]
        .into(),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "packed",
        input_type.clone(),
        true,
    )]));
    let packed_field = Arc::new(schema.field(0).clone());
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("packed", 0))];
    let expr_fields = [packed_field];
    let args = AccumulatorArgs {
        return_field: Arc::new(Field::new("corr", DataType::Float64, true)),
        schema: schema.as_ref(),
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
        name: "corr",
        is_distinct: false,
        exprs: &exprs,
        expr_fields: &expr_fields,
    };
    let udf = vail_execution::advanced_numeric_aggregates::make_udaf(
        "corr",
        &input_type,
        &serde_json::json!({}),
    )
    .unwrap()
    .unwrap();
    let records = packed(
        &["x", "y"],
        vec![
            Arc::new(Float64Array::from(vec![Some(1.), Some(2.), None, Some(2.)])),
            Arc::new(Float64Array::from(vec![
                Some(2.),
                Some(4.),
                Some(4.),
                Some(4.),
            ])),
        ],
        vec![true; 4],
    );
    let groups = [0, 0, 1, 1];
    let filter = BooleanArray::from(vec![Some(true), Some(true), None, Some(true)]);

    let mut direct = udf.create_groups_accumulator(args.clone()).unwrap();
    direct
        .update_batch(&[records.clone()], &groups, Some(&filter), 3)
        .unwrap();
    let direct = direct.evaluate(EmitTo::All).unwrap();
    let direct = direct.as_any().downcast_ref::<Float64Array>().unwrap();

    let mut converted = udf.create_groups_accumulator(args).unwrap();
    let states = converted
        .convert_to_state(&[records], Some(&filter))
        .unwrap();
    converted.merge_batch(&states, &groups, 3).unwrap();
    let first = converted.evaluate(EmitTo::First(1)).unwrap();
    let rest = converted.evaluate(EmitTo::All).unwrap();
    let first = first.as_any().downcast_ref::<Float64Array>().unwrap();
    let rest = rest.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(rest.len(), 2);
    assert_eq!(first.value(0), direct.value(0));
    assert!(rest.value(0).is_nan());
    assert!(rest.is_null(1));
    assert_eq!(direct.null_count(), 1);
}

#[test]
fn correlation_scalar_and_grouped_partial_states_cross_merge() {
    let input_type = DataType::Struct(
        vec![
            Arc::new(Field::new("x", DataType::Float64, true)),
            Arc::new(Field::new("y", DataType::Float64, true)),
        ]
        .into(),
    );
    let schema = Arc::new(Schema::new(vec![Field::new("packed", input_type, true)]));
    let packed_field = Arc::new(schema.field(0).clone());
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("packed", 0))];
    let expr_fields = [packed_field];
    let args = AccumulatorArgs {
        return_field: Arc::new(Field::new("corr", DataType::Float64, true)),
        schema: schema.as_ref(),
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
        name: "corr",
        is_distinct: false,
        exprs: &exprs,
        expr_fields: &expr_fields,
    };
    let udf = vail_execution::advanced_numeric_aggregates::make_udaf(
        "corr",
        schema.field(0).data_type(),
        &serde_json::json!({}),
    )
    .unwrap()
    .unwrap();
    let records = packed(
        &["x", "y"],
        vec![
            Arc::new(Float64Array::from(vec![1., 2., 3., 4.])),
            Arc::new(Float64Array::from(vec![3., 5., 7., 9.])),
        ],
        vec![true; 4],
    );
    // Scalar partial state must merge into the public grouped accumulator.
    let mut scalar_partial = udf.accumulator(args.clone()).unwrap();
    scalar_partial.update_batch(&[records.clone()]).unwrap();
    let scalar_state = scalar_partial
        .state()
        .unwrap()
        .into_iter()
        .map(|state| ScalarValue::iter_to_array([state]).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        scalar_state
            .iter()
            .map(|state| state.data_type().clone())
            .collect::<Vec<_>>(),
        vec![
            DataType::UInt64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ]
    );
    let mut grouped_final = udf.create_groups_accumulator(args.clone()).unwrap();
    grouped_final.merge_batch(&scalar_state, &[0], 1).unwrap();
    let grouped_result = grouped_final.evaluate(EmitTo::All).unwrap();
    let grouped_result = grouped_result
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(grouped_result.len(), 1);
    assert_eq!(grouped_result.value(0), 1.0);

    // Grouped partial states for two partitions must merge into the scalar
    // accumulator, preserving the same raw-sum native state layout.
    let mut grouped_partial = udf.create_groups_accumulator(args.clone()).unwrap();
    grouped_partial
        .update_batch(&[records.clone()], &[0, 0, 1, 1], None, 2)
        .unwrap();
    let grouped_state = grouped_partial.state(EmitTo::All).unwrap();
    let mut scalar_final = udf.accumulator(args).unwrap();
    scalar_final.merge_batch(&grouped_state).unwrap();
    let first_state = scalar_final.state().unwrap();
    let second_state = scalar_final.state().unwrap();
    assert_eq!(first_state, second_state);
    assert_eq!(
        scalar_final.evaluate().unwrap(),
        ScalarValue::Float64(Some(1.0))
    );
    assert_eq!(
        scalar_final.evaluate().unwrap(),
        ScalarValue::Float64(Some(1.0))
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

fn correlation_state_count(input: ArrayRef, expected: u64) -> u64 {
    let schema = Schema::new(vec![Field::new("packed", input.data_type().clone(), true)]);
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("packed", 0))];
    let fields = [Arc::new(schema.field(0).clone())];
    let udf = vail_execution::advanced_numeric_aggregates::make_udaf(
        "corr",
        input.data_type(),
        &serde_json::json!({}),
    )
    .unwrap()
    .unwrap();
    let args = AccumulatorArgs {
        return_field: Arc::new(Field::new("corr", DataType::Float64, true)),
        schema: &schema,
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
        name: "corr",
        is_distinct: false,
        exprs: &exprs,
        expr_fields: &fields,
    };
    let mut accumulator = udf.accumulator(args.clone()).unwrap();
    accumulator.update_batch(&[input.clone()]).unwrap();
    let state = accumulator.state().unwrap();
    assert_eq!(state.len(), 6);
    let ScalarValue::UInt64(Some(count)) = state[0] else {
        panic!("expected public UInt64 count state")
    };
    if expected != 0 {
        for (seed, succeeds) in [
            (u64::MAX - expected, true),
            (u64::MAX - expected + 1, false),
        ] {
            let mut near_limit = udf.accumulator(args.clone()).unwrap();
            let mut state: Vec<ArrayRef> =
                vec![Arc::new(arrow_array::UInt64Array::from(vec![seed]))];
            state.extend((0..5).map(|_| Arc::new(Float64Array::from(vec![0.0])) as ArrayRef));
            near_limit.merge_batch(&state).unwrap();
            assert_eq!(near_limit.update_batch(&[input.clone()]).is_ok(), succeeds);
        }
    }
    count
}

#[tokio::test]
async fn correlation_counts_offset_validities_and_parent_masks() {
    // Independent child offsets exercise Arrow's shifted bitmap words and tail.
    for &len in &[0usize, 1, 63, 64, 65, 129] {
        for &(x_offset, y_offset) in &[(0, 0), (1, 7), (7, 63), (63, 64), (64, 65)] {
            for masks in 0..8 {
                let x = Float64Array::from(
                    (0..len + x_offset)
                        .map(|i| {
                            (masks & 1 == 0 || i % 3 != 0).then_some((i - i.min(x_offset)) as f64)
                        })
                        .collect::<Vec<_>>(),
                )
                .slice(x_offset, len);
                let y = Float64Array::from(
                    (0..len + y_offset)
                        .map(|i| {
                            (masks & 2 == 0 || i % 5 != 0)
                                .then_some(2.0 * (i - i.min(y_offset)) as f64 + 1.0)
                        })
                        .collect::<Vec<_>>(),
                )
                .slice(y_offset, len);
                let parent = (0..len)
                    .map(|i| masks & 4 == 0 || i % 7 != 0)
                    .collect::<Vec<_>>();
                let valid = (0..len)
                    .filter(|&i| {
                        parent[i]
                            && (masks & 1 == 0 || (i + x_offset) % 3 != 0)
                            && (masks & 2 == 0 || (i + y_offset) % 5 != 0)
                    })
                    .count();
                let input = packed(&["x", "y"], vec![Arc::new(x), Arc::new(y)], parent);
                assert_eq!(
                    correlation_state_count(input.clone(), valid as u64),
                    valid as u64
                );
                let result = aggregate("corr", input, serde_json::json!({})).await;
                let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
                match valid {
                    0 => assert!(result.is_null(0)),
                    1 => assert!(result.value(0).is_nan()),
                    _ => assert!((result.value(0) - 1.0).abs() < 1e-12),
                }
            }
        }
    }
}
