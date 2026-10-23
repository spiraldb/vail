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
