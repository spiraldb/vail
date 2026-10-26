//! Observable results and lifecycle of the owner-preserving coordinate adapter.
use std::sync::Arc;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::{aggregate::{AggregateExprBuilder, AggregateFunctionExpr}, expressions::Column};
use vail_execution::array_aggregate::owning_coordinate_sum_udaf;

thread_local! {
    static ALLOCATED_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}
struct CountingAllocator;
fn record(bytes: usize) {
    let _ = ALLOCATED_BYTES.try_with(|count| {
        if let Some(value) = count.get() { count.set(Some(value.saturating_add(bytes))); }
    });
}
// Only allocation byte volume on the invoking test thread is recorded. Input
// construction and query preparation are outside the measured API operation.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// Owners 0 and 3 are observed. Owner 1 is excluded by a missing outer list;
// owner 2 is an included missing inner list with an arbitrarily large hidden
// child span. The unused sibling grows too, but must never be copied.
fn hidden_input(hidden: usize) -> ArrayRef {
    let mut values = vec![10, 20, 999];
    values.resize(3 + hidden, 888);
    values.extend([1, 2]);
    let coordinates = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3, (3 + hidden) as i32, (5 + hidden) as i32])),
        Arc::new(Int32Array::from(values)),
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let unused = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths([0, 0, hidden, 0]),
        Arc::new(Int32Array::from(vec![7; hidden])), None,
    ));
    let entry = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("arbitrary", coordinates.data_type().clone(), true)), coordinates as ArrayRef),
        (Arc::new(Field::new("unused", unused.data_type().clone(), true)), unused as ArrayRef),
    ]));
    let cells = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("value", entry.data_type().clone(), true)), entry as ArrayRef),
    ]));
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(cells.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 4])), cells,
        Some(NullBuffer::from(vec![true, false, true])),
    ))
}
fn aggregate(input: &ArrayRef) -> AggregateFunctionExpr {
    let udf = owning_coordinate_sum_udaf(input.data_type().clone(), &["value".into(), "arbitrary".into()]).unwrap();
    AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("cells", 0))])
        .schema(Arc::new(Schema::new(vec![Field::new("cells", input.data_type().clone(), true)])))
        .alias("result").build().unwrap()
}
fn result(values: Option<Vec<Option<i64>>>) -> ScalarValue {
    let field = Arc::new(Field::new_list_field(DataType::Int64, true));
    match values {
        Some(values) => ScalarValue::List(Arc::new(ListArray::new(field,
            OffsetBuffer::from_lengths([values.len()]), Arc::new(Int64Array::from(values)), None))),
        None => ScalarValue::List(Arc::new(ListArray::new_null(field, 1))),
    }
}
#[test]
fn outer_gap_and_null_inner_hidden_children_do_not_participate() {
    for hidden in [0, 17, 262_145] {
        let input = hidden_input(hidden);
        let aggregate = aggregate(&input);
        let mut accumulator = aggregate.create_accumulator().unwrap();
        accumulator.update_batch(&[input]).unwrap();
        assert_eq!(accumulator.evaluate().unwrap(), result(Some(vec![Some(11), Some(22)])));
    }
}
fn ordinary(values: Vec<Option<Vec<Option<i32>>>>) -> ArrayRef {
    let lists = Arc::new(ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(values));
    let entry = Arc::new(StructArray::from(vec![(Arc::new(Field::new("arbitrary", lists.data_type().clone(), true)), lists as ArrayRef)]));
    let cells = Arc::new(StructArray::from(vec![(Arc::new(Field::new("value", entry.data_type().clone(), true)), entry as ArrayRef)]));
    Arc::new(ListArray::new(Arc::new(Field::new_list_field(cells.data_type().clone(), true)),
        OffsetBuffer::from_lengths([cells.len()]), cells, None))
}
#[test]
fn multiple_updates_and_partial_merge_orders_keep_unknown_distinct_from_empty() {
    let missing = ordinary(vec![None, None]);
    let empty = ordinary(vec![Some(vec![])]);
    let nonempty = ordinary(vec![Some(vec![Some(4), None]), Some(vec![Some(3), Some(2)])]);
    let aggregate = aggregate(&missing);
    for observed in [&empty, &nonempty] {
        for missing_first in [true, false] {
            let mut direct = aggregate.create_accumulator().unwrap();
            let mut merged = aggregate.create_accumulator().unwrap();
            let batches = if missing_first { [&missing, observed] } else { [observed, &missing] };
            for batch in batches {
                direct.update_batch(&[batch.clone()]).unwrap();
                let mut partial = aggregate.create_accumulator().unwrap();
                partial.update_batch(&[batch.clone()]).unwrap();
                let states = partial.state().unwrap().iter().map(|state| state.to_array_of_size(1).unwrap()).collect::<Vec<_>>();
                merged.merge_batch(&states).unwrap();
            }
            let expected = if Arc::ptr_eq(observed, &empty) { result(Some(vec![])) }
                else { result(Some(vec![Some(7), Some(2)])) };
            assert_eq!(direct.evaluate().unwrap(), expected);
            assert_eq!(merged.evaluate().unwrap(), expected);
        }
    }
    let mut absent = aggregate.create_accumulator().unwrap();
    absent.update_batch(&[missing]).unwrap();
    assert_eq!(absent.evaluate().unwrap(), result(None));
    for reverse in [false, true] {
        let mut merged = aggregate.create_accumulator().unwrap();
        let batches = if reverse { [&nonempty, &empty] } else { [&empty, &nonempty] };
        let mut failed = false;
        for batch in batches {
            let mut partial = aggregate.create_accumulator().unwrap();
            partial.update_batch(&[batch.clone()]).unwrap();
            let states = partial.state().unwrap().iter().map(|state| state.to_array_of_size(1).unwrap()).collect::<Vec<_>>();
            failed |= merged.merge_batch(&states).is_err();
        }
        assert!(failed, "defined empty and nonempty coordinate widths disagree");
    }
}

#[test]
fn ancestor_nulls_and_nonzero_slices_exclude_hidden_ragged_arrays() {
    let inner = Arc::new(ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(999)]), Some(vec![Some(3), None]), Some(vec![Some(999)]), Some(vec![Some(4), Some(8)]),
    ]));
    let entry = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("arbitrary", inner.data_type().clone(), true))].into(),
        vec![inner], Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let cells = Arc::new(StructArray::from(vec![(Arc::new(Field::new("value", entry.data_type().clone(), true)), entry as ArrayRef)]));
    let outer = ListArray::new(Arc::new(Field::new_list_field(cells.data_type().clone(), true)),
        OffsetBuffer::from_lengths([1, 2, 1]), cells, None);
    let input: ArrayRef = Arc::new(outer.slice(1, 2));
    let mut accumulator = aggregate(&input).create_accumulator().unwrap();
    accumulator.update_batch(&[input]).unwrap();
    assert_eq!(accumulator.evaluate().unwrap(), result(Some(vec![Some(7), Some(8)])));
}

#[test]
fn integer_sum_keeps_existing_widening_and_wrapping() {
    let inner = Arc::new(ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>([
        Some(vec![Some(i64::MAX), None]), Some(vec![Some(1), None]),
    ]));
    let entry = Arc::new(StructArray::from(vec![(Arc::new(Field::new("arbitrary", inner.data_type().clone(), true)), inner as ArrayRef)]));
    let cells = Arc::new(StructArray::from(vec![(Arc::new(Field::new("value", entry.data_type().clone(), true)), entry as ArrayRef)]));
    let input: ArrayRef = Arc::new(ListArray::new(Arc::new(Field::new_list_field(cells.data_type().clone(), true)),
        OffsetBuffer::from_lengths([2]), cells, None));
    let mut accumulator = aggregate(&input).create_accumulator().unwrap();
    accumulator.update_batch(&[input]).unwrap();
    assert_eq!(accumulator.evaluate().unwrap(), result(Some(vec![Some(i64::MIN), Some(0)])));
}

#[test]
fn hidden_coordinate_and_unused_sibling_growth_does_not_expand_query_allocations() {
    let mut bytes = Vec::new();
    for hidden in [1, 262_145] {
        let input = hidden_input(hidden);
        let mut accumulator = aggregate(&input).create_accumulator().unwrap();
        ALLOCATED_BYTES.with(|counter| counter.set(Some(0)));
        let update = accumulator.update_batch(&[input]);
        bytes.push(ALLOCATED_BYTES.with(|counter| counter.replace(None).unwrap()));
        update.unwrap();
        assert_eq!(accumulator.evaluate().unwrap(), result(Some(vec![Some(11), Some(22)])));
    }
    assert!(bytes[1] <= bytes[0] + 4096, "excluded input growth expanded query allocations: {bytes:?}");
}

#[test]
fn empty_domains_and_repeated_data_updates_preserve_coordinate_state() {
    let absent = ordinary(vec![]);
    let data = ordinary(vec![Some(vec![Some(3), None]), Some(vec![Some(4), Some(2)])]);
    let aggregate = aggregate(&data);
    let mut accumulator = aggregate.create_accumulator().unwrap();
    accumulator.update_batch(&[absent.clone()]).unwrap();
    assert_eq!(accumulator.evaluate().unwrap(), result(None));
    for _ in 0..2 {
        accumulator.update_batch(&[data.clone()]).unwrap();
        accumulator.update_batch(&[absent.clone()]).unwrap();
    }
    assert_eq!(accumulator.evaluate().unwrap(), result(Some(vec![Some(14), Some(4)])));
    let mut merged = aggregate.create_accumulator().unwrap();
    for input in [&absent, &data, &absent, &data, &absent] {
        let mut partial = aggregate.create_accumulator().unwrap();
        partial.update_batch(&[input.clone()]).unwrap();
        let states = partial.state().unwrap().iter().map(|state| state.to_array_of_size(1).unwrap()).collect::<Vec<_>>();
        merged.merge_batch(&states).unwrap();
    }
    assert_eq!(merged.evaluate().unwrap(), result(Some(vec![Some(14), Some(4)])));
}
