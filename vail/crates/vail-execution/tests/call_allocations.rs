//! Heap allocation counts for public expression execution, excluding input setup.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use vail_execution::expressions::{lower, Expr, PrimitiveType, Type};

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn count_allocation() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value.saturating_add(1)));
        }
    });
}

// This test binary delegates storage to System and counts only the calling
// test thread. Const-initialized TLS does not allocate while counting an alloc.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count_allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn execute(batch: &RecordBatch, expression: &Expr) -> (ArrayRef, usize) {
    let compiled = lower(expression, batch.schema().as_ref()).unwrap();
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let output = compiled
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    (output, allocations)
}

fn calls(rows: usize) -> RecordBatch {
    let dtype = Type::Primitive(PrimitiveType::Call).to_arrow().unwrap();
    let DataType::Struct(fields) = dtype else {
        unreachable!()
    };
    let words = Int32Array::from_iter_values((0..rows + 2).map(|i| {
        if i % 5 == 0 {
            6
        } else if i % 2 == 0 {
            12
        } else {
            21
        }
    }));
    let valid = NullBuffer::from((0..rows + 2).map(|i| i % 5 != 0).collect::<Vec<_>>());
    let calls = StructArray::new(fields, vec![Arc::new(words)], Some(valid)).slice(1, rows);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "c",
        calls.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(calls)]).unwrap()
}

#[test]
fn accessor_allocations_do_not_scale_with_number_of_calls() {
    for name in [
        "__vail_call_word",
        "__vail_call_alleles",
        "__vail_call_phased",
    ] {
        let expression = Expr::GetField {
            value: Arc::new(Expr::Column { name: "c".into() }),
            name: name.into(),
        };
        let small = calls(64);
        let large = calls(8192);
        let (_, small_allocations) = execute(&small, &expression);
        let (output, large_allocations) = execute(&large, &expression);
        assert_eq!(output.len(), large.num_rows());
        assert_eq!(output.null_count(), large.column(0).null_count());
        assert!(
            large_allocations <= small_allocations + 32,
            "{name}: allocations grew from {small_allocations} to {large_allocations}"
        );
        eprintln!(
            "{name}: 64 calls={small_allocations} allocations, 8192 calls={large_allocations}"
        );
        if name == "__vail_call_word" {
            let source = large
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let source = source
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let output = output.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(source.values().as_ptr(), output.values().as_ptr());
        }
    }
}

fn constructor_input(rows: usize) -> RecordBatch {
    let child = Arc::new(Int32Array::from_iter_values(
        (0..2 * (rows + 2)).map(|i| (i % 2) as i32),
    ));
    let alleles = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths(vec![2; rows + 2]),
        child,
        None,
    );
    let phased = BooleanArray::from((0..rows + 2).map(|i| i % 2 == 0).collect::<Vec<_>>());
    let fields = vec![
        Field::new("__vail_call_alleles", alleles.data_type().clone(), true),
        Field::new("__vail_call_phased", DataType::Boolean, true),
    ]
    .into();
    let input =
        StructArray::new(fields, vec![Arc::new(alleles), Arc::new(phased)], None).slice(1, rows);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "input",
        input.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(input)]).unwrap()
}

#[test]
fn constructor_borrows_sliced_alleles_without_per_call_wrappers() {
    let expression = Expr::Cast {
        dtype: Type::Primitive(PrimitiveType::Call),
        value: Arc::new(Expr::Column {
            name: "input".into(),
        }),
    };
    let (_, small_allocations) = execute(&constructor_input(64), &expression);
    let (output, large_allocations) = execute(&constructor_input(8192), &expression);
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    let words = output
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(words
        .values()
        .iter()
        .enumerate()
        .all(|(i, word)| *word == if i % 2 == 0 { 12 } else { 13 }));
    assert!(
        large_allocations <= small_allocations + 32,
        "constructor: allocations grew from {small_allocations} to {large_allocations}"
    );
    eprintln!(
        "constructor: 64 calls={small_allocations} allocations, 8192 calls={large_allocations}"
    );
}
