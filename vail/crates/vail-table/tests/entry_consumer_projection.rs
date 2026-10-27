//! Public entry projections preserve logical nulls and independent consumers.
use arrow_array::{Array, ArrayRef, Int32Array, LargeListArray, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema};
use std::{alloc::{GlobalAlloc, Layout, System}, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}}};

struct CountingAllocator;
static MEASURING: AtomicBool = AtomicBool::new(false);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static SERIAL: Mutex<()> = Mutex::new(());
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) { ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed); }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) { unsafe { System.dealloc(pointer, layout) } }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) { ALLOCATED.fetch_add(size, Ordering::Relaxed); }
        unsafe { System.realloc(pointer, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

use vail_execution::expressions::{Expr, Type};
use vail_table::{TableIR, TableRef, globals::{GlobalBindings, GlobalsIR}};

fn structure(names: &[&str], arrays: Vec<ArrayRef>, nulls: Option<NullBuffer>) -> StructArray {
    let fields: Fields = names.iter().zip(&arrays).map(|(name, array)|
        Arc::new(Field::new(*name, array.data_type().clone(), true))).collect::<Vec<_>>().into();
    StructArray::try_new(fields, arrays, nulls).unwrap()
}

fn list(values: ArrayRef, offsets: Vec<i32>, nulls: Option<NullBuffer>) -> ListArray {
    ListArray::try_new(Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)), values, nulls).unwrap()
}

fn fixture(hidden: usize) -> TableRef { fixture_batches(hidden, false) }

fn fixture_batches(hidden: usize, duplicate_batches: bool) -> TableRef { fixture_layout(hidden, duplicate_batches, false) }

fn fixture_layout(hidden: usize, duplicate_batches: bool, large: bool) -> TableRef {
    let unused = Arc::new(list(Arc::new(Int32Array::from(vec![17; hidden])),
        vec![0, hidden as i32, hidden as i32, hidden as i32, hidden as i32, hidden as i32, hidden as i32],
        Some(NullBuffer::from(vec![false, true, true, true, true, true])))) as ArrayRef;
    // Combine a defined, unused nested payload with a distinct logical-null
    // sibling sharing its large backing owner. Null ancestors remain below.
    let null_list = unused.as_any().downcast_ref::<ListArray>().unwrap();
    let defined_list = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, true)), null_list.offsets().clone(),
        null_list.values().clone(), None,
    ).unwrap()) as ArrayRef;
    let unused = Arc::new(structure(&["defined", "hidden"], vec![defined_list, unused], None)) as ArrayRef;
    let entries = Arc::new(structure(&["value", "unused"], vec![
        Arc::new(Int32Array::from(vec![10, 11, 20, 21, 30, 31])), unused,
    ], Some(NullBuffer::from(vec![true, false, true, true, true, true])))) as ArrayRef;
    let cells = Arc::new(structure(&["entry"], vec![entries], None)) as ArrayRef;
    let cells = Arc::new(list(cells, vec![0, 2, 4, 6],
        Some(NullBuffer::from(vec![true, false, true])))) as ArrayRef;
    let cells: ArrayRef = if large {
        let original = cells.as_any().downcast_ref::<ListArray>().unwrap();
        Arc::new(LargeListArray::try_new(
            Arc::new(Field::new("item", original.values().data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2, 4, 6])),
            original.values().clone(), original.nulls().cloned(),
        ).unwrap())
    } else { cells };
    let rows = Arc::new(structure(&["r"], vec![Arc::new(Int32Array::from(vec![0, 0, 0]))], None)) as ArrayRef;
    let key = Arc::new(Int32Array::from(vec![0, 0, 0])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row", rows.data_type().clone(), true),
        Field::new("cells", cells.data_type().clone(), true),
        Field::new("key", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![rows, cells, key]).unwrap();
    let batches = if duplicate_batches { vec![batch.clone(), batch] } else { vec![batch] };
    let table = TableIR::parallelize_batches(schema, batches, 1).unwrap();
    let cols = Arc::new(structure(&["c"], vec![Arc::new(Int32Array::from(vec![0, 1]))], None)) as ArrayRef;
    let cols = Arc::new(structure(&["column"], vec![cols], None)) as ArrayRef;
    let cols = Arc::new(list(cols, vec![0, 2], None)) as ArrayRef;
    let globals_schema = Arc::new(Schema::new(vec![Field::new("columns", cols.data_type().clone(), true)]));
    let dtype = Type::from_arrow(&DataType::Struct(globals_schema.fields().clone())).unwrap();
    let globals = GlobalsIR::arrow_literal(RecordBatch::try_new(globals_schema, vec![cols]).unwrap()).unwrap();
    table.map_globals_bound(vec![("columns".into(), Expr::GetField {
        value: Arc::new(Expr::ExternalGlobal { reference: "g".into(), dtype }), name: "columns".into(),
    })], GlobalBindings::from([("g".into(), globals)])).unwrap()
        .matrix_entries_by_key_runs("row".into(), "cells".into(), "columns".into(),
            vec!["key".into()], vec!["r".into()], "column".into(), vec!["c".into()], "entry".into()).unwrap()
}

async fn values(table: &TableRef) -> Vec<(Option<i32>, Option<i32>, Option<i32>)> {
    let mut result = Vec::new();
    for partition in table.collect_partitions().await.unwrap() {
        for batch in partition {
            let r = batch.column_by_name("r").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            let c = batch.column_by_name("c").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            let value = batch.column_by_name("value").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            result.extend((0..batch.num_rows()).map(|i| (
                (!r.is_null(i)).then(|| r.value(i)),
                (!c.is_null(i)).then(|| c.value(i)),
                (!value.is_null(i)).then(|| value.value(i)),
            )));
        }
    }
    result
}

#[tokio::test]
async fn selected_values_ignore_missing_ancestors_and_preserve_shared_wide_consumer() {
    let _serial = SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let source = fixture(1024);
    let projected = source.map_rows(["r", "c", "value"].into_iter()
        .map(|name| (name.into(), Expr::Column { name: name.into() })).collect()).unwrap();
    let expected = vec![(Some(0), Some(0), Some(10)), (Some(0), Some(0), Some(30)), (Some(0), Some(1), Some(31))];
    assert_eq!(values(&projected).await, expected);
    assert_eq!(values(&source).await, expected);
    let mut wide_payload = Vec::new();
    for partition in source.collect_partitions().await.unwrap() {
        for batch in partition {
            let unused = batch.column_by_name("unused").unwrap().as_any().downcast_ref::<StructArray>().unwrap();
            let defined = unused.column_by_name("defined").unwrap().as_any().downcast_ref::<ListArray>().unwrap();
            let hidden = unused.column_by_name("hidden").unwrap().as_any().downcast_ref::<ListArray>().unwrap();
            for index in 0..batch.num_rows() {
                assert!(!unused.is_null(index));
                wide_payload.push(((!defined.is_null(index)).then(|| defined.value_length(index)),
                                   (!hidden.is_null(index)).then(|| hidden.value_length(index))));
            }
        }
    }
    assert_eq!(wide_payload, vec![(Some(1024), None), (Some(0), Some(0)), (Some(0), Some(0))]);
    assert_eq!(values(&projected).await, expected);
}


#[tokio::test]
async fn growing_unused_hidden_owner_does_not_grow_query_allocations() {
    let _serial = SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    // Fixture construction/allocation is deliberately outside the measurement.
    // Only the public projection+collect action is counted; returned values
    // independently force the selected payload and missing ancestors.
    let mut observations = Vec::new();
    for hidden in [1024, 4_000_000] {
        let source = fixture(hidden);
        let selected = source.map_rows(["r", "c", "value"].into_iter()
            .map(|name| (name.into(), Expr::Column { name: name.into() })).collect()).unwrap();
        ALLOCATED.store(0, Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let actual = values(&selected).await;
        MEASURING.store(false, Ordering::Relaxed);
        let allocated = ALLOCATED.load(Ordering::Relaxed);
        assert_eq!(actual, vec![(Some(0), Some(0), Some(10)), (Some(0), Some(0), Some(30)), (Some(0), Some(1), Some(31))]);
        observations.push(allocated);
    }
    // The added16MB source owner belongs only to an unused nested Struct,
    // with both defined data and a null List sibling.
    // Allow ordinary planner/runtime variation, never a payload-sized copy.
    eprintln!("public allocation bytes (small, growing hidden owner): {observations:?}");
    assert!(observations[1] < observations[0].saturating_add(2 * 1024 * 1024),
        "public query allocations grew with unused hidden owner: {observations:?}");
}


#[cfg(target_os = "linux")]
fn process_written_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/io").unwrap().lines()
        .find_map(|line| line.strip_prefix("write_bytes: ").map(|value| value.parse().unwrap())).unwrap()
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn multi_batch_duplicate_run_still_spills_but_writes_only_selected_fields() {
    let _serial = SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    // Two original source batches each retain a40MB owner counted in
    // both unused nested siblings by the unchanged Arrow-size mechanism. Their
    // ORIGINAL charges exceed the unchanged64MiB budget in both artifacts.
    // Producer construction is outside the observed temporary-write interval.
    let source = fixture_batches(10_000_000, true);
    let selected = source.map_rows(["r", "c", "value"].into_iter()
        .map(|name| (name.into(), Expr::Column { name: name.into() })).collect()).unwrap();
    let before = process_written_bytes();
    let actual = values(&selected).await;
    let written = process_written_bytes().saturating_sub(before);
    assert_eq!(actual, vec![(Some(0), Some(0), Some(10)),(Some(0), Some(0), Some(30)),(Some(0), Some(0), Some(10)),(Some(0), Some(0), Some(30)),(Some(0), Some(1), Some(31)),(Some(0), Some(1), Some(31))]);
    assert!(written > 0, "original input budget must still cause observable spill writes");
    assert!(written < 8 * 1024 * 1024, "unused source owner reached temporary output: {written}");
}


#[tokio::test]
async fn selected_large_list_offsets_preserve_logical_missingness() {
    let _serial = SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let source = fixture_layout(1024, false, true);
    let projected = source.map_rows(["r", "c", "value"].into_iter()
        .map(|name| (name.into(), Expr::Column { name: name.into() })).collect()).unwrap();
    assert_eq!(values(&projected).await, vec![(Some(0), Some(0), Some(10)), (Some(0), Some(0), Some(30)), (Some(0), Some(1), Some(31))]);
}
