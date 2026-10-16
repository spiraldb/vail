//! Heap allocation counts for public expression execution, excluding input setup.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use vail_execution::expressions::{Expr, PrimitiveType, Type, lower};

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
        "__vail_call_ploidy",
        "__vail_call_n_alt_alleles",
        "__vail_call_is_haploid",
        "__vail_call_is_diploid",
        "__vail_call_is_hom_ref",
        "__vail_call_is_non_ref",
        "__vail_call_is_het",
        "__vail_call_is_het_ref",
        "__vail_call_is_het_non_ref",
        "__vail_call_is_hom_var",
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
        if name != "__vail_call_alleles" {
            assert_eq!(
                large_allocations, small_allocations,
                "packed accessors must allocate only batch outputs"
            );
        }
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
    assert!(
        words
            .values()
            .iter()
            .enumerate()
            .all(|(i, word)| *word == if i % 2 == 0 { 12 } else { 13 })
    );
    assert!(
        large_allocations <= small_allocations + 32,
        "constructor: allocations grew from {small_allocations} to {large_allocations}"
    );
    eprintln!(
        "constructor: 64 calls={small_allocations} allocations, 8192 calls={large_allocations}"
    );
}

/// Reproducible throughput probe, excluded from correctness test runs.
#[test]
#[ignore = "manual packed Call throughput measurement"]
fn packed_predicate_throughput() {
    let batch = calls(1_000_000);
    for name in [
        "__vail_call_is_het",
        "__vail_call_is_hom_var",
        "__vail_call_n_alt_alleles",
    ] {
        let expression = Expr::GetField {
            value: Arc::new(Expr::Column { name: "c".into() }),
            name: name.into(),
        };
        let compiled = lower(&expression, batch.schema().as_ref()).unwrap();
        std::hint::black_box(compiled.evaluate(&batch).unwrap());
        let start = std::time::Instant::now();
        for _ in 0..64 {
            std::hint::black_box(compiled.evaluate(&batch).unwrap());
        }
        eprintln!(
            "{name}: {:.3} ns/Call (64 x 1M mixed phased/unphased and missing Calls)",
            start.elapsed().as_nanos() as f64 / 64_000_000.0
        );
    }
}

#[test]
fn public_packed_properties_match_alleles_at_triangular_boundaries() {
    use vail_ordering::call::{decode, pack};
    let mut words = (0..4096)
        .filter(|word| decode(*word).is_ok())
        .collect::<Vec<_>>();
    for phased in [false, true] {
        words.push(pack(&[], phased).unwrap());
        for allele in [0, 1, 31, 32767, (1 << 29) - 1] {
            words.push(pack(&[allele], phased).unwrap());
        }
        for k in 1..=32767u32 {
            let triangular = k * (k + 1) / 2;
            for repr in [triangular - 1, triangular, triangular + 1] {
                if repr < 1 << 29 {
                    words.push(((repr << 3) | 4 | u32::from(phased)) as i32);
                }
            }
        }
    }
    let DataType::Struct(fields) = Type::Primitive(PrimitiveType::Call).to_arrow().unwrap() else {
        unreachable!()
    };
    let calls = StructArray::new(
        fields,
        vec![Arc::new(Int32Array::from(words.clone()))],
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "c",
        calls.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(calls)]).unwrap();
    for property in [
        "ploidy",
        "n_alt_alleles",
        "is_haploid",
        "is_diploid",
        "is_hom_ref",
        "is_non_ref",
        "is_het",
        "is_het_ref",
        "is_het_non_ref",
        "is_hom_var",
    ] {
        let expression = Expr::GetField {
            value: Arc::new(Expr::Column { name: "c".into() }),
            name: format!("__vail_call_{property}"),
        };
        let result = lower(&expression, batch.schema().as_ref())
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        for (row, word) in words.iter().enumerate() {
            let call = decode(*word).unwrap();
            let a = call.alleles();
            let het = a.len() == 2 && a[0] != a[1];
            let expected = match property {
                "ploidy" => a.len() as i32,
                "n_alt_alleles" => a.iter().filter(|a| **a > 0).count() as i32,
                "is_haploid" => i32::from(a.len() == 1),
                "is_diploid" => i32::from(a.len() == 2),
                "is_hom_ref" => i32::from(!a.is_empty() && a.iter().all(|a| *a == 0)),
                "is_non_ref" => i32::from(a.iter().any(|a| *a > 0)),
                "is_het" => i32::from(het),
                "is_het_ref" => i32::from(het && a.contains(&0)),
                "is_het_non_ref" => i32::from(het && !a.contains(&0)),
                "is_hom_var" => i32::from(!a.is_empty() && !het && a[0] > 0),
                _ => unreachable!(),
            };
            let actual = if let Some(values) = result.as_any().downcast_ref::<Int32Array>() {
                values.value(row)
            } else {
                i32::from(
                    result
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row),
                )
            };
            assert_eq!(actual, expected, "word={word} property={property}");
        }
    }
}
