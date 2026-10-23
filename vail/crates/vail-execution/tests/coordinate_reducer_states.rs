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
