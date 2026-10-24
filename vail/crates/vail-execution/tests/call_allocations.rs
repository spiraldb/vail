//! Heap allocation counts for public expression execution, excluding input setup.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StructArray,
    UInt8Array, UInt16Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use vail_execution::expressions::{Expr, PrimitiveType, Type, lower};

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
    static ALLOCATED_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn count_allocation(bytes: usize) {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value.saturating_add(1)));
        }
    });
    let _ = ALLOCATED_BYTES.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value.saturating_add(bytes)));
        }
    });
}

// This test binary delegates storage to System and counts only the calling
// test thread. Const-initialized TLS does not allocate while counting an alloc.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count_allocation(size);
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
    let first =
        UInt16Array::from_iter_values((0..rows + 2).map(|i| if i % 2 == 0 { 0 } else { 1 }));
    let second =
        UInt16Array::from_iter_values((0..rows + 2).map(|i| if i % 2 == 0 { 1 } else { 0 }));
    let ploidy =
        UInt8Array::from_iter_values((0..rows + 2).map(|i| if i % 5 == 0 { 3 } else { 2 }));
    let phased = BooleanArray::from((0..rows + 2).map(|i| i % 2 != 0).collect::<Vec<_>>());
    let valid = NullBuffer::from((0..rows + 2).map(|i| i % 5 != 0).collect::<Vec<_>>());
    let calls = StructArray::new(
        fields,
        vec![
            Arc::new(first),
            Arc::new(second),
            Arc::new(ploidy),
            Arc::new(phased),
        ],
        Some(valid),
    )
    .slice(1, rows);
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
    let alleles = output
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();
    assert!(alleles.values().iter().all(|a| *a == 0));
    let second = output
        .column(1)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();
    assert!(second.values().iter().all(|a| *a == 1));
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
fn public_direct_properties_cover_uint16_alleles() {
    use vail_ordering::call::Call;
    let mut calls = Vec::new();
    for phased in [false, true] {
        calls.push(Call::new(&[], phased).unwrap());
        for allele in 0..=65535 {
            calls.push(Call::new(&[allele], phased).unwrap());
        }
        for a in [0, 1, 255, 256, 32767, 65535] {
            for b in [0, 1, 255, 256, 32767, 65535] {
                calls.push(Call::new(&[a, b], phased).unwrap());
            }
        }
    }
    let DataType::Struct(fields) = Type::Primitive(PrimitiveType::Call).to_arrow().unwrap() else {
        unreachable!()
    };
    let data = StructArray::new(
        fields,
        vec![
            Arc::new(UInt16Array::from_iter_values(
                calls.iter().map(|c| c.first()),
            )),
            Arc::new(UInt16Array::from_iter_values(
                calls.iter().map(|c| c.second()),
            )),
            Arc::new(UInt8Array::from_iter_values(
                calls.iter().map(|c| c.ploidy()),
            )),
            Arc::new(BooleanArray::from(
                calls.iter().map(|c| c.phased()).collect::<Vec<_>>(),
            )),
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "c",
        data.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
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
        let expr = Expr::GetField {
            value: Arc::new(Expr::Column { name: "c".into() }),
            name: format!("__vail_call_{property}"),
        };
        let output = lower(&expr, batch.schema().as_ref())
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(calls.len())
            .unwrap();
        for (row, c) in calls.iter().enumerate() {
            let a = c.alleles();
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
            let actual = if let Some(values) = output.as_any().downcast_ref::<Int32Array>() {
                values.value(row)
            } else {
                i32::from(
                    output
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row),
                )
            };
            assert_eq!(actual, expected, "{c:?} {property}");
        }
    }
}

#[test]
fn call_execution_trusts_stored_fields_and_parent_validity() {
    let dtype = Type::Primitive(PrimitiveType::Call).to_arrow().unwrap();
    let DataType::Struct(fields) = dtype else {
        unreachable!()
    };
    // Accessors consume stored values, without checking canonical allele order,
    // unused padding, or child validity. Only parent validity defines missing Calls.
    let input = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(UInt16Array::from(vec![Some(2), Some(1), None, Some(0)])),
            Arc::new(UInt16Array::from(vec![Some(0), Some(9), Some(1), Some(0)])),
            Arc::new(UInt8Array::from(vec![2, 1, 2, 3])),
            Arc::new(BooleanArray::from(vec![false; 4])),
        ],
        Some(NullBuffer::from(vec![true, true, true, false])),
    ));
    let rows = vail_execution::calls::CallRows::from_struct(&input).unwrap();
    assert_eq!(rows.get(0).unwrap().unwrap().alleles(), &[2, 0]);
    assert_eq!(rows.get(1).unwrap().unwrap().alleles(), &[1]);
    assert!(rows.get(2).unwrap().is_some());
    assert!(rows.get(3).unwrap().is_none());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "c",
        input.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
    let expression = Expr::GetField {
        value: Arc::new(Expr::Column { name: "c".into() }),
        name: "__vail_call_is_het".into(),
    };
    let (output, _) = execute(&batch, &expression);
    let values = output.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(values.value(0));
    assert!(!values.value(1));
    assert!(values.is_null(3));
}

fn aligned_column_input(payload_bytes: usize) -> (RecordBatch, Expr) {
    let columns_count = 128;
    let row_count = 8;
    let int64 = Type::Primitive(PrimitiveType::Int64);
    let string = Type::Primitive(PrimitiveType::Str);
    let payload_type = Type::Struct {
        r#struct: vec![("marker".into(), int64.clone()), ("payload".into(), string)],
    };
    let column_record_type = Type::Struct {
        r#struct: vec![
            ("__vail_matrix_col_id".into(), int64.clone()),
            ("col".into(), payload_type.clone()),
        ],
    };
    let entry_value_type = Type::Struct { r#struct: vec![] };
    let entry_type = Type::Struct {
        r#struct: vec![("value".into(), entry_value_type)],
    };

    let entry_values: ArrayRef = Arc::new(StructArray::new_empty_fields(
        row_count * columns_count,
        None,
    ));
    let entries_child: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("value", entry_values.data_type().clone(), true)].into(),
        vec![entry_values],
        None,
    ));
    let entries: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", entries_child.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(
            (0..=row_count)
                .map(|row| (row * columns_count) as i32)
                .collect::<Vec<_>>(),
        )),
        entries_child,
        None,
    ));

    let payload = "x".repeat(payload_bytes);
    let columns = Expr::Literal {
        dtype: Type::Array {
            array: Box::new(column_record_type.clone()),
        },
        value: serde_json::Value::Array(
            (0..columns_count)
                .map(|index| {
                    serde_json::json!({
                        "__vail_matrix_col_id": index as i64,
                        "col": {"marker": index as i64, "payload": payload},
                    })
                })
                .collect(),
        ),
    };
    let rows: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("row_idx", DataType::Int64, true)].into(),
        vec![Arc::new(Int64Array::from_iter_values(0..row_count as i64))],
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("entries", entries.data_type().clone(), true),
        Field::new("rows", rows.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![entries, rows]).unwrap();

    let column_input = Expr::AlignedMapInput {
        axis: "col".into(),
        name: None,
        dtype: column_record_type,
    };
    let payload_value = Expr::GetField {
        value: Arc::new(Expr::GetField {
            value: Arc::new(column_input),
            name: "col".into(),
        }),
        name: "payload".into(),
    };
    let payload_length = Expr::StringFunction {
        function: "character_length".into(),
        args: vec![payload_value],
    };
    let entry_present = Expr::AlignedMapInput {
        axis: "entry_present".into(),
        name: None,
        dtype: Type::Primitive(PrimitiveType::Bool),
    };
    let body = Expr::IfElse {
        condition: Arc::new(entry_present),
        consequent: Arc::new(payload_length),
        alternate: Arc::new(Expr::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: serde_json::Value::Null,
        }),
    };
    let expression = Expr::AlignedMatrixMap {
        entries: Arc::new(Expr::Column {
            name: "entries".into(),
        }),
        columns: Arc::new(columns),
        rows: Arc::new(Expr::Column {
            name: "rows".into(),
        }),
        globals: Arc::new(Expr::Struct { fields: vec![] }),
        body: Arc::new(body),
        dtype: Type::Array {
            array: Box::new(Type::Primitive(PrimitiveType::Int32)),
        },
        preserve_cell: false,
    };
    let _ = (payload_type, entry_type);
    (batch, expression)
}

fn evaluate_aligned_allocated_bytes(batch: &RecordBatch, expression: &Expr) -> (ArrayRef, usize) {
    let compiled = lower(expression, batch.schema().as_ref()).unwrap();
    ALLOCATED_BYTES.with(|bytes| bytes.set(Some(0)));
    let output = compiled
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let bytes = ALLOCATED_BYTES.with(|bytes| bytes.replace(None).unwrap());
    (output, bytes)
}

#[test]
fn aligned_map_column_metadata_allocations_do_not_scale_with_payload_bytes() {
    let (small_batch, small_expr) = aligned_column_input(8);
    let (wide_batch, wide_expr) = aligned_column_input(64 * 1024);
    let (small_output, small_bytes) = evaluate_aligned_allocated_bytes(&small_batch, &small_expr);
    let (wide_output, wide_bytes) = evaluate_aligned_allocated_bytes(&wide_batch, &wide_expr);
    for (output, expected) in [(small_output, 8), (wide_output, 64 * 1024)] {
        let rows = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(rows.len(), 8);
        let lengths = rows.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(lengths.values().len(), 8 * 128);
        assert!(
            lengths.values().iter().all(|length| *length == expected),
            "expected each mapped payload length to be {expected}, found {:?}",
            &lengths.values()[..8]
        );
    }
    assert!(
        wide_bytes <= small_bytes + 1_000_000,
        "metadata payload grew allocations from {small_bytes} to {wide_bytes} bytes"
    );
}

#[test]
fn direct_call_index_allocates_only_primitive_output_not_allele_lists() {
    let batch = calls(8192);
    let expression = Expr::ArrayIndex {
        value: Arc::new(Expr::GetField {
            value: Arc::new(Expr::Column { name: "c".into() }),
            name: "__vail_call_alleles".into(),
        }),
        index: Arc::new(Expr::RuntimeLiteral {
            value: datafusion_common::ScalarValue::Int32(Some(0)),
        }),
    };
    let (output, bytes) = evaluate_aligned_allocated_bytes(&batch, &expression);
    assert_eq!(output.len(), batch.num_rows());
    assert_eq!(output.null_count(), batch.column(0).null_count());
    // Four bytes per result plus bounded expression bookkeeping. No child
    // allele values, list offsets, per-row take indices or expanded index literal.
    assert!(bytes <= 5 * batch.num_rows() + 8192,
            "direct Call index allocated {bytes} bytes for {} calls", batch.num_rows());
    eprintln!("direct Call index: {} calls, {bytes} allocated bytes", batch.num_rows());
}
