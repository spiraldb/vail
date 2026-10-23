//! Public Arrow→coordinate aggregate contracts; independent scalar oracles.
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, ListArray, StructArray};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
use std::sync::Arc;
use vail_execution::{
    array_aggregate::{CoordinateReducer, coordinate_bundle_udaf, coordinate_udaf},
    reducer::configured_udaf,
};

fn offset_bits(values: &[bool], offset: usize) -> BooleanBuffer {
    let mut padded = vec![true; offset];
    padded.extend_from_slice(values);
    BooleanBuffer::from(padded).slice(offset, values.len())
}
fn list(values: ArrayRef, width: usize, rows: usize) -> ListArray {
    ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
        values,
        None,
    )
}

#[test]
fn sliced_predicate_validity_filter_and_parent_masks_keep_column_positions() {
    for width in [1, 7, 31, 32, 63, 64, 65, 1000, 2535] {
        let rows = 263;
        let len = (rows + 1) * width;
        let predicate = (0..len).map(|i| i % 7 < 3).collect::<Vec<_>>();
        let valid = (0..len).map(|i| i % 11 != 0).collect::<Vec<_>>();
        let filter = (0..len).map(|i| i % 5 != 0).collect::<Vec<_>>();
        let filter_valid = (0..len).map(|i| i % 13 != 0).collect::<Vec<_>>();
        let parent = (0..len).map(|i| i % 17 != 0).collect::<Vec<_>>();
        let fields = vec![
            Arc::new(Field::new("predicate", DataType::Boolean, true)),
            Arc::new(Field::new("filter", DataType::Boolean, true)),
        ];
        let packed: ArrayRef = Arc::new(StructArray::new(
            fields.into(),
            vec![
                Arc::new(BooleanArray::new(
                    offset_bits(&predicate, 3),
                    Some(NullBuffer::new(offset_bits(&valid, 11))),
                )),
                Arc::new(BooleanArray::new(
                    offset_bits(&filter, 5),
                    Some(NullBuffer::new(offset_bits(&filter_valid, 7))),
                )),
            ],
            Some(NullBuffer::new(offset_bits(&parent, 9))),
        ));
        let udf = coordinate_bundle_udaf(
            packed.data_type().clone(),
            vec![CoordinateReducer {
                name: "yes".into(),
                aggregate: configured_udaf(
                    "count_where",
                    Some(&DataType::Boolean),
                    &serde_json::json!({}),
                )
                .unwrap(),
                input: DataType::Boolean,
                value_index: 0,
                filter_index: Some(1),
                empty: ScalarValue::Int64(Some(0)),
                identity: "count_where".into(),
            }],
        )
        .unwrap();
        let input: ArrayRef = Arc::new(list(packed, width, rows + 1).slice(1, rows));
        let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
            .schema(Arc::new(Schema::new(vec![Field::new(
                "xs",
                input.data_type().clone(),
                true,
            )])))
            .alias("counts")
            .build()
            .unwrap();
        let mut state = aggregate.create_accumulator().unwrap();
        state.update_batch(&[input.clone()]).unwrap();
        state.update_batch(&[input]).unwrap();
        let published = state.state().unwrap();
        let ScalarValue::List(result) = state.evaluate().unwrap() else {
            panic!("list result")
        };
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let counts = values
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for col in 0..width {
            let expected = (1..=rows)
                .filter(|r| {
                    let i = r * width + col;
                    predicate[i] && valid[i] && filter[i] && filter_valid[i] && parent[i]
                })
                .count() as i64
                * 2;
            assert_eq!(counts.value(col), expected, "width{width} col{col}");
        }
        let mut restored = aggregate.create_accumulator().unwrap();
        restored
            .merge_batch(
                &published
                    .iter()
                    .map(|v| v.to_array_of_size(1).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        assert_eq!(restored.evaluate().unwrap(), ScalarValue::List(result));
    }
}

#[test]
fn packed_counts_keep_signed64_partial_state_wrapping() {
    let width = 65;
    let rows = 263;
    let native = configured_udaf(
        "count_where",
        Some(&DataType::Boolean),
        &serde_json::json!({}),
    )
    .unwrap();
    let udf = coordinate_udaf(native, DataType::Boolean, ScalarValue::Int64(Some(0))).unwrap();
    let input: ArrayRef = Arc::new(list(
        Arc::new(BooleanArray::from(vec![true; width * rows])),
        width,
        rows,
    ));
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("xs", 0))])
        .schema(Arc::new(Schema::new(vec![Field::new(
            "xs",
            input.data_type().clone(),
            true,
        )])))
        .alias("counts")
        .build()
        .unwrap();
    let mut state = aggregate.create_accumulator().unwrap();
    state
        .merge_batch(&[
            Arc::new(list(
                Arc::new(Int64Array::from(vec![i64::MAX; width])),
                width,
                1,
            )),
            Arc::new(Int64Array::from(vec![width as i64])),
        ])
        .unwrap();
    state.update_batch(&[input]).unwrap();
    let ScalarValue::List(result) = state.evaluate().unwrap() else {
        panic!("list result")
    };
    let counts = result
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(
        counts
            .values()
            .iter()
            .all(|v| *v == i64::MAX.wrapping_add(rows as i64))
    );
}
