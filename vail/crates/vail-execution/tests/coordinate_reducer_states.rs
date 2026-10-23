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
