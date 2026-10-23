//! Public reducer/coordinate accumulator partial-state interoperability.
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, UInt64Array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
use std::sync::Arc;
use vail_execution::{array_aggregate::coordinate_udaf, reducer::configured_udaf};

fn list(values: ArrayRef) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::from_lengths([values.len()]),
        values,
        None,
    ))
}

#[test]
fn stddev_coordinate_accepts_native_uint64_state_and_published_snapshots() {
    let native = configured_udaf(
        "stddev_pop",
        Some(&DataType::Float64),
        &serde_json::json!({}),
    )
    .unwrap();
    let udf = coordinate_udaf(
        native,
        DataType::Float64,
        ScalarValue::Float64(Some(f64::NAN)),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "xs",
        DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
        true,
    )]));
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
        .schema(schema)
        .alias("sd")
        .build()
        .unwrap();
    let mut state = aggregate.create_accumulator().unwrap();
    let count = 1_u64 << 63;
    let partial = vec![
        list(Arc::new(UInt64Array::from(vec![count]))),
        list(Arc::new(Float64Array::from(vec![3.]))),
        list(Arc::new(Float64Array::from(vec![count as f64 * 4.]))),
        list(Arc::new(UInt64Array::from(vec![0]))),
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    ];
    state.merge_batch(&partial).unwrap();
    let snapshot = state.evaluate().unwrap();
    let ScalarValue::List(values) = &snapshot else {
        panic!("stddev must return list")
    };
    assert_eq!(
        values
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.
    );
    let published = state.state().unwrap();
    let mut restored = aggregate.create_accumulator().unwrap();
    restored
        .merge_batch(
            &published
                .iter()
                .map(|v| v.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert_eq!(restored.evaluate().unwrap(), snapshot);
    state
        .update_batch(&[list(Arc::new(Float64Array::from(vec![f64::INFINITY])))])
        .unwrap();
    let ScalarValue::List(values) = state.evaluate().unwrap() else {
        panic!("stddev must return list")
    };
    assert!(
        values
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .is_nan()
    );
    let ScalarValue::List(values) = snapshot else {
        unreachable!()
    };
    assert_eq!(
        values
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.
    );
    assert_eq!(
        published[0].data_type(),
        DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true)))
    );
}

#[test]
fn collect_coordinate_reuses_repeatable_native_state_then_restores_on_update() {
    let native = configured_udaf(
        "collect",
        Some(&DataType::Int32),
        &serde_json::json!({"ordered":true}),
    )
    .unwrap();
    let empty = ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths([0]),
        Arc::new(arrow_array::Int32Array::from(Vec::<i32>::new())),
        None,
    )));
    let udf = coordinate_udaf(native, DataType::Int32, empty).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "xs",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
        true,
    )]));
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
        .schema(schema)
        .alias("collected")
        .build()
        .unwrap();
    let mut accumulator = aggregate.create_accumulator().unwrap();
    // Native snapshots may be requested before the first input establishes width.
    let _empty_snapshot = accumulator.state().unwrap();
    let input: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths([2, 2]),
        Arc::new(arrow_array::Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
        ])),
        None,
    ));
    accumulator.update_batch(&[input]).unwrap();
    let snapshot = accumulator.evaluate().unwrap();
    assert_eq!(accumulator.evaluate().unwrap(), snapshot);
    let published = accumulator.state().unwrap();
    // The layout adapter emits the existing native ARRAY_AGG wire protocol:
    // another public native grouped factory can consume it directly.
    let native =
        configured_udaf("collect", Some(&DataType::Int32), &serde_json::json!({})).unwrap();
    let native_aggregate =
        AggregateExprBuilder::new(native, vec![Arc::new(Column::new("value", 0))])
            .schema(Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                true,
            )])))
            .alias("native_collect")
            .build()
            .unwrap();
    let ScalarValue::List(wire) = &published[0] else {
        panic!("coordinate state must be a list")
    };
    let mut native_groups = native_aggregate.create_groups_accumulator().unwrap();
    native_groups
        .merge_batch(&[wire.values().clone()], &[0, 1], 2)
        .unwrap();
    assert_eq!(
        native_groups
            .evaluate(datafusion_expr::EmitTo::All)
            .unwrap()
            .to_data(),
        wire.values().to_data()
    );
    let mut restored = aggregate.create_accumulator().unwrap();
    restored
        .merge_batch(
            &published
                .iter()
                .map(|state| state.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let new_input = list(Arc::new(arrow_array::Int32Array::from(vec![5, 6])));
    accumulator.update_batch(&[new_input.clone()]).unwrap();
    restored.update_batch(&[new_input]).unwrap();
    let result = accumulator.evaluate().unwrap();
    assert_eq!(restored.evaluate().unwrap(), result);
    fn values(result: ScalarValue) -> Vec<Vec<Option<i32>>> {
        let ScalarValue::List(result) = result else {
            panic!("coordinate output must be list")
        };
        let columns = result
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        (0..columns.len())
            .map(|column| {
                columns
                    .value(column)
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap()
                    .iter()
                    .collect()
            })
            .collect()
    }
    assert_eq!(
        values(snapshot),
        vec![vec![Some(1), Some(3)], vec![None, Some(4)]]
    );
    assert_eq!(
        values(result),
        vec![
            vec![Some(1), Some(3), Some(5)],
            vec![None, Some(4), Some(6)]
        ]
    );
}

#[test]
fn collect_coordinate_preserves_view_and_nested_physical_descriptors() {
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(arrow_array::StringViewArray::from(vec![
            Some("hello"),
            None,
        ])),
        Arc::new(arrow_array::LargeStringArray::from(vec![
            Some("hello"),
            None,
        ])),
        Arc::new(arrow_array::StructArray::new(
            vec![Arc::new(Field::new("x", DataType::Int32, false))].into(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2]))],
            Some(arrow_buffer::NullBuffer::from(vec![true, false])),
        )),
        Arc::new(arrow_array::StructArray::new_empty_fields(
            2,
            Some(arrow_buffer::NullBuffer::from(vec![true, false])),
        )),
    ];
    for input in inputs {
        let dtype = input.data_type().clone();
        let native = configured_udaf("collect", Some(&dtype), &serde_json::json!({})).unwrap();
        let field = Arc::new(Field::new_list_field(dtype.clone(), true));
        let empty = ScalarValue::List(Arc::new(ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([0]),
            arrow_array::new_empty_array(&dtype),
            None,
        )));
        let udf = coordinate_udaf(native, dtype.clone(), empty).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "xs",
            DataType::List(field.clone()),
            true,
        )]));
        let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
            .schema(schema)
            .alias("collected")
            .build()
            .unwrap();
        let mut accumulator = aggregate.create_accumulator().unwrap();
        let rows: ArrayRef = Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths([1, 1]),
            input.clone(),
            None,
        ));
        accumulator.update_batch(&[rows]).unwrap();
        let ScalarValue::List(result) = accumulator.evaluate().unwrap() else {
            panic!("coordinate output must be list")
        };
        let columns = result
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns.value(0).to_data(), input.to_data());
    }
}

#[test]
fn histogram_coordinate_matches_native_groups_and_partial_state_protocol() {
    let native = configured_udaf(
        "hist",
        Some(&DataType::Int32),
        &serde_json::json!({"start":0.,"end":4.,"bins":2}),
    )
    .unwrap();
    let native_expression =
        AggregateExprBuilder::new(native.clone(), vec![Arc::new(Column::new("value", 0))])
            .schema(Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                true,
            )])))
            .alias("hist")
            .build()
            .unwrap();
    let empty = native_expression
        .create_accumulator()
        .unwrap()
        .evaluate()
        .unwrap();
    let udf = coordinate_udaf(native, DataType::Int32, empty).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "xs",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
        true,
    )]));
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
        .schema(schema)
        .alias("coordinate_hist")
        .build()
        .unwrap();
    let mut accumulator = aggregate.create_accumulator().unwrap();
    let flat: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![
        Some(0),
        Some(2),
        Some(4),
        None,
    ]));
    let input: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths([2, 2]),
        flat.clone(),
        None,
    ));
    accumulator.update_batch(&[input]).unwrap();
    let snapshot = accumulator.evaluate().unwrap();
    let ScalarValue::List(result) = &snapshot else {
        panic!("coordinate result must be a list")
    };
    let mut native_groups = native_expression.create_groups_accumulator().unwrap();
    native_groups
        .update_batch(&[flat], &[0, 1, 0, 1], None, 2)
        .unwrap();
    assert_eq!(
        native_groups
            .evaluate(datafusion_expr::EmitTo::All)
            .unwrap()
            .to_data(),
        result.values().to_data()
    );
    let published = accumulator.state().unwrap();
    let ScalarValue::List(wire) = &published[0] else {
        panic!("hist partial state must be a list")
    };
    let mut native_restored = native_expression.create_groups_accumulator().unwrap();
    native_restored
        .merge_batch(&[wire.values().clone()], &[0, 1], 2)
        .unwrap();
    assert_eq!(
        native_restored
            .evaluate(datafusion_expr::EmitTo::All)
            .unwrap()
            .to_data(),
        result.values().to_data()
    );
    let mut restored = aggregate.create_accumulator().unwrap();
    restored
        .merge_batch(
            &published
                .iter()
                .map(|state| state.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let additional = list(Arc::new(arrow_array::Int32Array::from(vec![1, 3])));
    accumulator.update_batch(&[additional.clone()]).unwrap();
    restored.update_batch(&[additional]).unwrap();
    assert_eq!(
        accumulator.evaluate().unwrap(),
        restored.evaluate().unwrap()
    );
    let list_state = wire.values().as_any().downcast_ref::<ListArray>().unwrap();
    let counts = list_state
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.values(), &[0, 1, 1, 0, 0, 0, 1, 0]);
}

#[test]
fn take_coordinate_bounded_snapshots_and_native_partial_state() {
    let native =
        configured_udaf("take", Some(&DataType::Int32), &serde_json::json!({"n":2})).unwrap();
    let item = Arc::new(Field::new_list_field(DataType::Int32, true));
    let empty = ScalarValue::List(Arc::new(ListArray::new(
        item.clone(),
        OffsetBuffer::from_lengths([0]),
        Arc::new(arrow_array::Int32Array::from(Vec::<i32>::new())),
        None,
    )));
    let udf = coordinate_udaf(native.clone(), DataType::Int32, empty).unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
        .schema(Arc::new(Schema::new(vec![Field::new(
            "xs",
            DataType::List(item.clone()),
            true,
        )])))
        .alias("taken")
        .build()
        .unwrap();
    let mut accumulator = aggregate.create_accumulator().unwrap();
    accumulator.state().unwrap();
    accumulator
        .update_batch(&[list(Arc::new(arrow_array::Int32Array::from(vec![
            Some(1),
            None,
        ])))])
        .unwrap();
    let first = accumulator.evaluate().unwrap();
    let published = accumulator.state().unwrap();
    let mut restored = aggregate.create_accumulator().unwrap();
    restored
        .merge_batch(
            &published
                .iter()
                .map(|v| v.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let next: ArrayRef = Arc::new(ListArray::new(
        item.clone(),
        OffsetBuffer::from_lengths([2, 2]),
        Arc::new(arrow_array::Int32Array::from(vec![3, 4, 5, 6])),
        None,
    ));
    accumulator.update_batch(&[next.clone()]).unwrap();
    restored.update_batch(&[next]).unwrap();
    let result = accumulator.evaluate().unwrap();
    assert_eq!(restored.evaluate().unwrap(), result);
    assert_eq!(accumulator.evaluate().unwrap(), result);
    let ScalarValue::List(outer) = result else {
        panic!("list")
    };
    let columns = outer.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        columns.value(0).to_data(),
        arrow_array::Int32Array::from(vec![1, 3]).to_data()
    );
    assert_eq!(
        columns.value(1).to_data(),
        arrow_array::Int32Array::from(vec![None, Some(4)]).to_data()
    );
    let ScalarValue::List(first) = first else {
        panic!("list")
    };
    let first = first.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(first.value_length(0), 1);
    // Consume coordinate wire values with the unchanged public native TAKE factory.
    let native_expr = AggregateExprBuilder::new(native, vec![Arc::new(Column::new("x", 0))])
        .schema(Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Int32,
            true,
        )])))
        .alias("native")
        .build()
        .unwrap();
    let mut native_state = native_expr.create_accumulator().unwrap();
    native_state
        .merge_batch(&[Arc::new(columns.slice(0, 1))])
        .unwrap();
    let ScalarValue::List(native_values) = native_state.evaluate().unwrap() else {
        panic!("list")
    };
    assert_eq!(native_values.values().to_data(), columns.value(0).to_data());
}

#[test]
fn take_coordinate_sparse_partial_states_have_linear_retained_memory() {
    fn retained_size(width: usize) -> usize {
        let native =
            configured_udaf("take", Some(&DataType::Int32), &serde_json::json!({"n":1})).unwrap();
        let item = Arc::new(Field::new_list_field(DataType::Int32, true));
        let empty = ScalarValue::List(Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths([0]),
            Arc::new(arrow_array::Int32Array::from(Vec::<i32>::new())),
            None,
        )));
        let udf = coordinate_udaf(native, DataType::Int32, empty).unwrap();
        let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
            .schema(Arc::new(Schema::new(vec![Field::new(
                "xs",
                DataType::List(item.clone()),
                true,
            )])))
            .alias("take")
            .build()
            .unwrap();
        let mut state = aggregate.create_accumulator().unwrap();
        for coordinate in 0..width {
            let lists = ListArray::new(
                item.clone(),
                OffsetBuffer::from_lengths((0..width).map(|i| usize::from(i == coordinate))),
                Arc::new(arrow_array::Int32Array::from(vec![coordinate as i32])),
                Some(arrow_buffer::NullBuffer::from(
                    (0..width).map(|i| i == coordinate).collect::<Vec<_>>(),
                )),
            );
            state
                .merge_batch(&[
                    list(Arc::new(lists)),
                    Arc::new(Int64Array::from(vec![width as i64])),
                ])
                .unwrap();
        }
        let size = state.size();
        let ScalarValue::List(result) = state.evaluate().unwrap() else {
            panic!("list")
        };
        let lists = result
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(lists.len(), width);
        assert_eq!(lists.values().len(), width);
        size
    }
    // Four times as many retained values must not require quadratic metadata.
    assert!(retained_size(1024) < retained_size(256) * 6);
}

#[test]
fn correlation_coordinate_uses_native_wire_full_uint64_domain_and_snapshots() {
    let packed = DataType::Struct(
        vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]
        .into(),
    );
    let native = configured_udaf("corr", Some(&packed), &serde_json::json!({})).unwrap();
    let udf = coordinate_udaf(native.clone(), packed.clone(), ScalarValue::Float64(None)).unwrap();
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
        .schema(Arc::new(Schema::new(vec![Field::new(
            "xs",
            DataType::List(Arc::new(Field::new_list_field(packed.clone(), true))),
            true,
        )])))
        .alias("corr")
        .build()
        .unwrap();
    let mut state = aggregate.create_accumulator().unwrap();
    let count = 1_u64 << 63;
    let partial = vec![
        list(Arc::new(UInt64Array::from(vec![count]))),
        list(Arc::new(Float64Array::from(vec![0.]))),
        list(Arc::new(Float64Array::from(vec![0.]))),
        list(Arc::new(Float64Array::from(vec![count as f64 * 2.]))),
        list(Arc::new(Float64Array::from(vec![count as f64]))),
        list(Arc::new(Float64Array::from(vec![count as f64 * 4.]))),
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    ];
    state.merge_batch(&partial).unwrap();
    let snapshot = state.evaluate().unwrap();
    assert_eq!(state.evaluate().unwrap(), snapshot);
    let ScalarValue::List(output) = &snapshot else {
        panic!("list")
    };
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.
    );
    let published = state.state().unwrap();
    let native_expr = AggregateExprBuilder::new(native, vec![Arc::new(Column::new("xy", 0))])
        .schema(Arc::new(Schema::new(vec![Field::new("xy", packed, true)])))
        .alias("native_corr")
        .build()
        .unwrap();
    let mut native_groups = native_expr.create_groups_accumulator().unwrap();
    let native_states = published[..6]
        .iter()
        .map(|s| {
            let ScalarValue::List(v) = s else {
                panic!("list")
            };
            v.values().clone()
        })
        .collect::<Vec<_>>();
    native_groups.merge_batch(&native_states, &[0], 1).unwrap();
    assert_eq!(
        native_groups
            .evaluate(datafusion_expr::EmitTo::All)
            .unwrap()
            .to_data(),
        output.values().to_data()
    );
    let mut restored = aggregate.create_accumulator().unwrap();
    restored
        .merge_batch(
            &published
                .iter()
                .map(|s| s.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert_eq!(restored.evaluate().unwrap(), snapshot);
    let extra = vec![
        list(Arc::new(UInt64Array::from(vec![1]))),
        list(Arc::new(Float64Array::from(vec![0.]))),
        list(Arc::new(Float64Array::from(vec![0.]))),
        list(Arc::new(Float64Array::from(vec![2.]))),
        list(Arc::new(Float64Array::from(vec![1.]))),
        list(Arc::new(Float64Array::from(vec![4.]))),
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    ];
    restored.merge_batch(&extra).unwrap();
    let ScalarValue::List(counts) = &restored.state().unwrap()[0] else {
        panic!("list")
    };
    assert_eq!(
        counts
            .values()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        count + 1
    );
    let ScalarValue::List(counts) = &published[0] else {
        panic!("list")
    };
    assert_eq!(
        counts
            .values()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        count
    );
}
