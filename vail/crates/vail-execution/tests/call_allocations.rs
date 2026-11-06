//! Heap allocation counts for public expression execution, excluding input setup.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt8Array, UInt16Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_physical_expr::PhysicalExpr;
use vail_execution::expressions::{ArrayReduction, Expr, PreparationOptions, PrimitiveType, Type, lower, lower_with_options};

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
    evaluate_prepared_allocated_bytes(batch, &compiled)
}

fn evaluate_selected_allocated_bytes(batch: &RecordBatch, expression: &Expr) -> (ArrayRef, usize) {
    let options = PreparationOptions::default().for_selected_region();
    let compiled = lower_with_options(expression, batch.schema().as_ref(), &options).unwrap();
    evaluate_prepared_allocated_bytes(batch, &compiled)
}

fn evaluate_prepared_allocated_bytes(batch: &RecordBatch, compiled: &Arc<dyn PhysicalExpr>) -> (ArrayRef, usize) {
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


fn nested_row_capture_input(payload_bytes: usize) -> (RecordBatch, Expr) {
    nested_row_capture_with_unused_call(payload_bytes, None)
}

fn nested_row_capture_with_unused_call(payload_bytes: usize, depth: Option<usize>) -> (RecordBatch, Expr) {
    let count = 128usize;
    let mut present = vec![true; count * 3]; present[1] = false;
    let entry_values: ArrayRef = Arc::new(StructArray::new_empty_fields(count * 3, Some(NullBuffer::from(present))));
    let slots: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("value", entry_values.data_type().clone(), true)), entry_values,
    )]));
    let entries: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", slots.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, count as i32, count as i32, (count*2) as i32, (count*3) as i32])),
        slots, None,
    ));
    let small: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0,2,4,6,8])),
        Arc::new(Int32Array::from(vec![1,2,3,4,5,6,7,8])), None,
    ));
    let payload = "x".repeat(payload_bytes);
    let meta: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("small", small.data_type().clone(), true), Field::new("unused", DataType::Utf8, true)].into(),
        vec![small, Arc::new(StringArray::from(vec![payload.as_str();4]))],
        Some(NullBuffer::from(vec![true,true,true,false])),
    ));
    let meta = if let Some(depth) = depth {
        let value = meta.as_any().downcast_ref::<StructArray>().unwrap();
        let call = calls(4).column(0).clone();
        let mut fields = value.fields().to_vec();
        fields.push(Arc::new(Field::new("unused_call", call.data_type().clone(), true)));
        let mut children = value.columns().to_vec();
        children.push(call);
        let mut nested: ArrayRef = Arc::new(StructArray::new(
            fields.into(), children, value.nulls().cloned()));
        for _ in 0..depth {
            nested = Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("child", nested.data_type().clone(), true)), nested,
            )]));
        }
        nested
    } else { meta };
    let meta_type = Type::from_arrow(meta.data_type()).unwrap();
    let fields: ArrayRef = Arc::new(StructArray::from(vec![(Arc::new(Field::new("meta", meta.data_type().clone(), true)),meta)]));
    let rows: ArrayRef = Arc::new(StructArray::from(vec![(Arc::new(Field::new("__vail_aligned_row", fields.data_type().clone(), true)),fields)]));
    let schema = Arc::new(Schema::new(vec![Field::new("entries",entries.data_type().clone(),true),Field::new("rows",rows.data_type().clone(),true)]));
    let batch = RecordBatch::try_new(schema,vec![entries,rows]).unwrap();
    let captured = Expr::AlignedMapInput { axis:"row".into(),name:Some("meta".into()),dtype:meta_type };
    let mut selected = captured;
    for _ in 0..depth.unwrap_or(0) {
        selected = Expr::GetField { value: Arc::new(selected), name: "child".into() };
    }
    let sum = Expr::ArrayReduce { operation:ArrayReduction::Sum,filter_missing:true,
        value:Arc::new(Expr::GetField { value:Arc::new(selected),name:"small".into() }) };
    let body = Expr::IfElse {
        condition:Arc::new(Expr::AlignedMapInput { axis:"entry_present".into(),name:None,dtype:Type::Primitive(PrimitiveType::Bool) }),
        consequent:Arc::new(sum),alternate:Arc::new(Expr::Literal { dtype:Type::Primitive(PrimitiveType::Int32),value:serde_json::Value::Null }),
    };
    let expression = Expr::AlignedMatrixMap {
        entries:Arc::new(Expr::Column { name:"entries".into() }),rows:Arc::new(Expr::Column { name:"rows".into() }),
        columns:Arc::new(Expr::Literal { dtype:Type::Array { array:Box::new(Type::Primitive(PrimitiveType::Int32)) },value:serde_json::json!(vec![0;count]) }),
        globals:Arc::new(Expr::Struct { fields:vec![] }),body:Arc::new(body),
        dtype:Type::Array { array:Box::new(Type::Primitive(PrimitiveType::Int32)) },preserve_cell:false,
    };
    (batch,expression)
}

#[test]
fn flat_nested_row_capture_drops_unused_payload_before_nonidentity_take_and_decode() {
    let mut sizes = Vec::new();
    for bytes in [8,64*1024] {
        // Input construction AND query preparation are outside runtime
        // allocation accounting; public lower/evaluate is the only boundary.
        let (batch,expression) = nested_row_capture_input(bytes);
        let (output,allocated) = evaluate_aligned_allocated_bytes(&batch,&expression);
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        let actual = lists.iter().map(|row| row.map(|array|
            array.as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>()
        )).collect::<Vec<_>>();
        let mut first = vec![Some(3);128]; first[1] = None;
        assert_eq!(actual, vec![Some(first),Some(vec![]),Some(vec![Some(11);128]),Some(vec![None;128])]);
        sizes.push(allocated);
    }
    eprintln!("public nested capture runtime allocations (small,wide): {sizes:?}");
    assert!(sizes[1] < sizes[0].saturating_add(1_000_000), "unused row sibling grew allocations: {sizes:?}");
}

#[test]
fn unused_call_siblings_at_any_struct_depth_do_not_grow_capture_allocations() {
    for depth in [0, 3] {
        let mut sizes = Vec::new();
        for bytes in [8, 64 * 1024] {
            let (batch, expression) = nested_row_capture_with_unused_call(bytes, Some(depth));
            let (output, allocated) = evaluate_aligned_allocated_bytes(&batch, &expression);
            let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
            let actual = lists.iter().map(|row| row.map(|array|
                array.as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>()
            )).collect::<Vec<_>>();
            let mut first = vec![Some(3); 128]; first[1] = None;
            assert_eq!(actual, vec![Some(first), Some(vec![]),
                                   Some(vec![Some(11); 128]), Some(vec![None; 128])]);
            sizes.push(allocated);
        }
        eprintln!("unused Call sibling depth {depth} runtime allocations: {sizes:?}");
        assert!(sizes[1] < sizes[0].saturating_add(1_000_000),
                "unused payload with Call sibling grew allocations at depth {depth}: {sizes:?}");
    }
}

fn mapped_array_capture_input(bytes: usize, start: usize, gap: usize) -> (RecordBatch, Expr) {
    let count = start * 2 + gap + 8;
    let mut numbers = vec![0; count];
    numbers[start..start+4].copy_from_slice(&[1, 2, 3, 4]);
    numbers[start+4+gap..start+8+gap].copy_from_slice(&[5, 6, 7, 8]);
    let mut valid = vec![true; count];
    valid[start+1] = false;
    let nulls = NullBuffer::from(valid);
        let (batch, mut expression) = nested_row_capture_input(8);
        let values: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("value", DataType::Int32, false), Field::new("unused", DataType::Utf8, true)].into(),
            vec![Arc::new(Int32Array::new(ScalarBuffer::from(numbers), Some(nulls.clone()))),
                 Arc::new(StringArray::from(vec!["x".repeat(bytes); count]))],
            Some(nulls),
        ));
        let items: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![start as i32, (start+2) as i32, (start+4+gap) as i32, (start+6+gap) as i32, (start+8+gap) as i32])), values,
            Some(NullBuffer::from(vec![true, true, true, false])),
        ));
        let ty = Type::from_arrow(items.data_type()).unwrap();
        let Type::Array { array: element_type } = &ty else { unreachable!() };
        let element_type = element_type.as_ref().clone();
        let fields: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("meta", items.data_type().clone(), true)), items,
        )]));
        let rows: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("__vail_aligned_row", fields.data_type().clone(), true)), fields,
        )]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![
            batch.schema().field(0).clone(), Field::new("rows", rows.data_type().clone(), true),
        ])), vec![batch.column(0).clone(), rows]).unwrap();
        let Expr::AlignedMatrixMap { body, .. } = &mut expression else { unreachable!() };
        let Expr::IfElse { consequent, .. } = Arc::make_mut(body) else { unreachable!() };
        *consequent = Arc::new(Expr::ArrayReduce {
            operation: ArrayReduction::Sum, filter_missing: true,
            value: Arc::new(Expr::ArrayMap {
                value: Arc::new(Expr::AlignedMapInput { axis: "row".into(), name: Some("meta".into()), dtype: ty }),
                parameter: "item".into(),
                body: Arc::new(Expr::GetField {
                    value: Arc::new(Expr::LambdaVariable { name: "item".into(), dtype: Some(element_type) }),
                    name: "value".into(),
                }),
            }),
        });
    (batch, expression)
}

#[test]
fn mapped_array_struct_capture_drops_unused_element_payload() {
    let mut sizes = Vec::new();
    for bytes in [8, 64 * 1024] {
        let (batch, expression) = mapped_array_capture_input(bytes, 0, 0);
        let (output, allocated) = evaluate_aligned_allocated_bytes(&batch, &expression);
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        let actual = lists.iter().map(|row| row.map(|array|
            array.as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>()
        )).collect::<Vec<_>>();
        let mut first = vec![Some(1); 128]; first[1] = None;
        assert_eq!(actual, vec![Some(first), Some(vec![]), Some(vec![Some(11); 128]), Some(vec![None; 128])]);
        sizes.push(allocated);
    }
    eprintln!("mapped array Struct runtime allocations: {sizes:?}");
    assert!(sizes[1] < sizes[0] + 1_000_000, "unused mapped element payload grew allocations: {sizes:?}");
}

#[test]
fn mapped_sliced_lists_preserve_nullable_elements_and_hidden_gap_values() {
    // The second row has no entries; its list span is hidden from evaluation.
    // A sliced owner and an interior gap exercise distinct constructor domains.
    for (owner, gap) in [(0, 0), (65_536, 0), (0, 65_536)] {
        let (batch, expression) = mapped_array_capture_input(8, owner, gap);
        let (output, _) = evaluate_aligned_allocated_bytes(&batch, &expression);
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        let actual = lists.iter().map(|row| row.map(|array|
            array.as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>()
        )).collect::<Vec<_>>();
        let mut first = vec![Some(1); 128]; first[1] = None;
        assert_eq!(actual, vec![Some(first), Some(vec![]), Some(vec![Some(11); 128]), Some(vec![None; 128])]);
    }
}

#[test]
fn indexing_uses_compact_indices_without_a_temporary_optional_vector() {
    let rows = 32768;
    let values: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from((0..=rows).collect::<Vec<i32>>())),
        Arc::new(Int32Array::from((0..rows).collect::<Vec<i32>>())), None));
    let indices: ArrayRef = Arc::new(Int32Array::from(vec![0; rows as usize]));
    let batch = RecordBatch::try_from_iter([("values",values),("index",indices)]).unwrap();
    let expression = Expr::ArrayIndex {
        value: Arc::new(Expr::Column { name: "values".into() }),
        index: Arc::new(Expr::Column { name: "index".into() }),
    };
    let (output, bytes) = evaluate_aligned_allocated_bytes(&batch, &expression);
    assert_eq!(output.as_any().downcast_ref::<Int32Array>().unwrap(),
        &Int32Array::from((0..rows).collect::<Vec<i32>>()));
    // Four-byte gather indices and four-byte results, plus validity and
    // bounded expression bookkeeping, should fit below ten bytes per row.
    assert!(bytes <= 10 * rows as usize + 8192, "indexing allocated {bytes} bytes");
    eprintln!("generic indexing: {rows} rows, {bytes} allocated bytes");
}

#[path = "common/indexed_batch.rs"]
mod indexed_fixture;

#[test]
fn batch_conditionals_do_not_copy_unused_entry_payload() {
    let mut costs = Vec::new();
    for bytes in [8, 1024 * 1024] {
        let (batch, expression) = indexed_fixture::fixture([1, -1], true, bytes);
        let (output, allocated) = evaluate_selected_allocated_bytes(&batch, &expression);
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        let values = lists.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(values.column(1).as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(1), None, None, Some(93)]));
        costs.push(allocated);
    }
    eprintln!("indexed batch runtime allocation small/large unused field: {costs:?}");
    assert!(costs[1] <= costs[0] + 64 * 1024, "unused entry payload was copied: {costs:?}");
}

#[test]
fn selected_lists_do_not_reserve_hidden_owner_payload() {
    let mut costs = Vec::new();
    for hidden in [0, 1024 * 1024] {
        let (batch, expression) = indexed_fixture::fixture_with_hidden([1, -1], true, 8, hidden);
        let (output, allocated) = evaluate_selected_allocated_bytes(&batch, &expression);
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        let values = lists.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(values.column(1).as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(1), None, None, Some(93)]));
        costs.push(allocated);
    }
    eprintln!("selected list allocation small/hidden owner: {costs:?}");
    assert!(costs[1] <= costs[0] + 64 * 1024, "hidden list owner grew allocation: {costs:?}");
}

#[test]
fn readonly_aligned_payload_values_survive_input_release() {
    use arrow_array::types::Int32Type;
    for width in [2usize, 200_000] {
        let (output,bytes)={
            let payload=ListArray::from_iter_primitive::<Int32Type,_,_>(
                (0..4).map(|_|Some((0..width).map(|i|Some(i as i32)))));
            let values=StructArray::from(vec![(Arc::new(Field::new("payload",payload.data_type().clone(),true)),Arc::new(payload) as ArrayRef)]);
            let cells=StructArray::from(vec![(Arc::new(Field::new("value",values.data_type().clone(),true)),Arc::new(values) as ArrayRef)]);
            let cell_type=Type::from_arrow(cells.data_type()).unwrap();
            let entries=ListArray::new(Arc::new(Field::new("item",cells.data_type().clone(),true)),OffsetBuffer::new(ScalarBuffer::from(vec![0,2,4])),Arc::new(cells),None);
            let batch=RecordBatch::try_from_iter([("entries",Arc::new(entries) as ArrayRef)]).unwrap();
            let int=Type::Primitive(PrimitiveType::Int32);
            let array_int=Type::Array {array:Box::new(int.clone())};
            let value=Expr::GetField {value:Arc::new(Expr::AlignedMapInput {axis:"entry".into(),name:None,dtype:cell_type}),name:"value".into()};
            let payload=Expr::GetField {value:Arc::new(value),name:"payload".into()};
            let output_type=Type::Struct {r#struct:vec![("payload".into(),array_int.clone()),("length".into(),int.clone())]};
            let expression=Expr::AlignedMatrixMap {
                entries:Arc::new(Expr::Column{name:"entries".into()}),
                columns:Arc::new(Expr::Literal{dtype:array_int,value:serde_json::json!([0,1])}),
                rows:Arc::new(Expr::Struct{fields:vec![]}),globals:Arc::new(Expr::Struct{fields:vec![]}),
                body:Arc::new(Expr::Struct{fields:vec![("payload".into(),payload.clone()),("length".into(),Expr::ArrayLength{value:Arc::new(payload)})]}),
                dtype:Type::Array{array:Box::new(output_type)},preserve_cell:false,
            };
            let compiled=lower(&expression,batch.schema().as_ref()).unwrap();
            ALLOCATED_BYTES.with(|count|count.set(Some(0)));
            let output=compiled.evaluate(&batch).unwrap().into_array(2).unwrap();
            let bytes=ALLOCATED_BYTES.with(|count|count.replace(None).unwrap());
            (output,bytes)
        };
        let outer=output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(outer.len(),2);assert_eq!(outer.null_count(),0);
        assert!((0..outer.len()).all(|i| outer.value_length(i)==2));
        for row_index in 0..outer.len() {
            assert!(outer.is_valid(row_index));
            let row=outer.value(row_index);
            let values=row.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(values.null_count(),0);
            let payload=values.column(0).as_any().downcast_ref::<ListArray>().unwrap();
            let lengths=values.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..values.len() {
                assert!(payload.is_valid(i));assert!(lengths.is_valid(i));assert_eq!(lengths.value(i),width as i32);
                let row=payload.value(i);let row=row.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(row.len(),width);assert_eq!(row.null_count(),0);
                assert!(row.values().iter().enumerate().all(|(i,value)|*value==i as i32));
            }
        }
        eprintln!("READONLY_PAYLOAD_ALLOCATION width={width} bytes={bytes}");
    }
}

#[test]
fn nested_struct_callback_preparation_preserves_all_values() {
    use arrow_array::types::Int32Type;
    use datafusion_common::DFSchema;
    use datafusion_expr::{execution_props::ExecutionProps, physical_planning_context::PhysicalPlanningContext};
    use datafusion_physical_expr::create_physical_expr;
    for depth in [8, 32] {
        let xs = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(7)]), Some(vec![]), None,
        ])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("xs", xs)]).unwrap();
        let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
        let mut body = Expr::LambdaVariable {name: "x".into(), dtype: None};
        for _ in 0..depth {
            body = Expr::Struct {fields: vec![
                ("value".into(), body),
                ("tag".into(), Expr::RuntimeLiteral {value: datafusion_common::ScalarValue::Int32(Some(11))}),
            ]};
        }
        let expression = Expr::ArrayMap {value: Arc::new(Expr::Column {name: "xs".into()}),
            parameter: "x".into(), body: Arc::new(body)};
        ALLOCATED_BYTES.with(|count| count.set(Some(0)));
        let logical = vail_execution::logical_expr::lower_logical(&expression, &schema).unwrap();
        let bytes = ALLOCATED_BYTES.with(|count| count.replace(None).unwrap());
        let program = create_physical_expr(&logical, &schema, &ExecutionProps::new(),
            &PhysicalPlanningContext::default()).unwrap();
        let output = program.evaluate(&batch).unwrap().into_array(3).unwrap();
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(lists.is_valid(0)); assert!(lists.is_valid(1)); assert!(lists.is_null(2));
        assert_eq!(lists.value_length(1), 0);
        let mut value = lists.value(0);
        for _ in 0..depth {
            let record = value.as_any().downcast_ref::<StructArray>().unwrap();
            assert!(record.is_valid(0));
            let tag = record.column_by_name("tag").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            assert!(tag.is_valid(0)); assert_eq!(tag.value(0), 11);
            value = record.column_by_name("value").unwrap().clone();
        }
        let value = value.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(value.is_valid(0)); assert_eq!(value.value(0), 7);
        eprintln!("NESTED_CALLBACK_PREPARATION depth={depth} bytes={bytes}");
    }
}

#[test]
fn alias_field_inference_preserves_metadata_and_qualification() {
    use datafusion_common::{Column, DFSchema, TableReference, metadata::FieldMetadata};
    use datafusion_expr::{Expr as LogicalExpr, ExprSchemable};
    use std::collections::HashMap;
    for nullable in [false, true] {
        let child = Arc::new(Field::new("child", DataType::Int32, true)
            .with_metadata(HashMap::from([("child-tag".into(), "retained".into())])));
        let input = Field::new("value", DataType::Struct(vec![child].into()), nullable)
            .with_metadata(HashMap::from([
                ("inherited".into(), "yes".into()), ("replace".into(), "source".into()),
            ]));
        let schema = Schema::new(vec![input.clone()]);
        let schema = DFSchema::try_from_qualified_schema("records", &schema).unwrap();
        for depth in [4, 8, 12] {
            let mut expression = LogicalExpr::Column(Column::new(Some("records"), "value"));
            for level in 0..depth {
                expression = expression.alias_qualified_with_metadata(Some("scope"),
                    format!("alias_{level}"), Some(FieldMetadata::from(HashMap::from([
                        ("replace".to_owned(), level.to_string()),
                        (format!("level_{level}"), "retained".to_owned()),
                    ]))));
            }
            ALLOCATED_BYTES.with(|count| count.set(Some(0)));
            let (relation, field) = expression.to_field(&schema).unwrap();
            let bytes = ALLOCATED_BYTES.with(|count| count.replace(None).unwrap());
            assert_eq!(relation, Some(TableReference::bare("scope")));
            assert_eq!(field.name(), &format!("alias_{}", depth - 1));
            assert_eq!(field.data_type(), input.data_type());
            assert_eq!(field.is_nullable(), nullable);
            assert_eq!(field.metadata().get("inherited").unwrap(), "yes");
            assert_eq!(field.metadata().get("replace").unwrap(), &(depth - 1).to_string());
            for level in 0..depth {
                assert_eq!(field.metadata().get(&format!("level_{level}")).unwrap(), "retained");
            }
            assert_eq!(input.metadata().get("replace").unwrap(), "source");
            eprintln!("ALIAS_FIELD_PREPARATION nullable={nullable} depth={depth} bytes={bytes}");
        }
    }
}

#[test]
fn integer_row_captures_preserve_values_across_entry_widths() {
    use arrow_array::types::Int32Type;
    use vail_execution::expressions::BinaryOperator;
    for (rows, width) in [(1usize, 16usize), (32, 4096)] {
        let count = rows * width;
        let xs = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            (0..count).map(|i| Some(vec![Some(i as i32), Some(50), Some(100)])),
        )) as ArrayRef;
        let values = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("xs", xs.data_type().clone(), true)), xs,
        )])) as ArrayRef;
        let cells = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("value", values.data_type().clone(), true)), values,
        )])) as ArrayRef;
        let cell_type = Type::from_arrow(cells.data_type()).unwrap();
        let entries = Arc::new(ListArray::new(
            Arc::new(Field::new("item", cells.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from((0..=rows).map(|i| (i * width) as i32).collect::<Vec<_>>())),
            cells, None,
        )) as ArrayRef;
        let row_fields = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("slot", DataType::Int32, true)), Arc::new(Int32Array::from_value(1, rows)) as ArrayRef),
            (Arc::new(Field::new("constant", DataType::Int64, true)), Arc::new(Int64Array::from_value(9, rows)) as ArrayRef),
            (Arc::new(Field::new("absent", DataType::Int64, true)), Arc::new(Int64Array::from(vec![None; rows])) as ArrayRef),
        ])) as ArrayRef;
        let row_records = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("__vail_aligned_row", row_fields.data_type().clone(), true)), row_fields,
        )])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("entries", entries), ("rows", row_records)]).unwrap();
        let int = Type::Primitive(PrimitiveType::Int32);
        let long = Type::Primitive(PrimitiveType::Int64);
        let input = |name: &str, dtype: Type| Expr::AlignedMapInput { axis: "row".into(), name: Some(name.into()), dtype };
        let payload = Expr::GetField { value: Arc::new(Expr::GetField {
            value: Arc::new(Expr::AlignedMapInput { axis: "entry".into(), name: None, dtype: cell_type }),
            name: "value".into(),
        }), name: "xs".into() };
        let output_type = Type::Struct { r#struct: vec![
            ("value".into(), int.clone()), ("sum".into(), long.clone()),
            ("missing".into(), Type::Primitive(PrimitiveType::Bool)),
        ] };
        let expression = Expr::AlignedMatrixMap {
            entries: Arc::new(Expr::Column { name: "entries".into() }),
            rows: Arc::new(Expr::Column { name: "rows".into() }),
            columns: Arc::new(Expr::Literal { dtype: Type::Array { array: Box::new(int.clone()) }, value: serde_json::json!(vec![0; width]) }),
            globals: Arc::new(Expr::Struct { fields: vec![] }),
            body: Arc::new(Expr::Struct { fields: vec![
                ("value".into(), Expr::ArrayIndex { value: Arc::new(payload), index: Arc::new(input("slot", int)) }),
                ("sum".into(), Expr::Binary { operator: BinaryOperator::Add,
                    left: Arc::new(input("constant", long.clone())),
                    right: Arc::new(Expr::Literal { dtype: long.clone(), value: serde_json::json!(3) }) }),
                ("missing".into(), Expr::IsMissing { value: Arc::new(input("absent", long)) }),
            ] }),
            dtype: Type::Array { array: Box::new(output_type) }, preserve_cell: false,
        };
        let (output, allocated) = evaluate_aligned_allocated_bytes(&batch, &expression);
        drop(batch);
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.len(), rows);
        for row in lists.iter() {
            let row = row.unwrap(); let fields = row.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(fields.len(), width);
            assert_eq!(fields.null_count(), 0);
            assert!(fields.column(0).as_any().downcast_ref::<Int32Array>().unwrap().iter().all(|v| v == Some(50)));
            assert!(fields.column(1).as_any().downcast_ref::<Int64Array>().unwrap().iter().all(|v| v == Some(12)));
            assert!(fields.column(2).as_any().downcast_ref::<BooleanArray>().unwrap().iter().all(|v| v == Some(true)));
        }
        eprintln!("UNIFORM_INTEGER_PUBLIC rows={rows} width={width} allocated={allocated}");
    }
}

#[test]
fn coalesce_quality_preserves_values_with_sparse_missing_and_unused_columns() {
    use arrow_array::types::Int32Type;
    use vail_execution::expressions::UnaryOperator;
    let rows=8192usize;
    for missing_period in [0usize,100,2,1] {
        for unused_bytes in [8usize,1024] {
            let likelihoods=ListArray::from_iter_primitive::<Int32Type,_,_>((0..rows).map(|i|
                if missing_period>0&&i%missing_period==0 {None}
                else {Some(vec![Some(0),Some((i%30+1) as i32),Some(60)])}));
            let fallback=Int32Array::from_iter_values((0..rows).map(|i|i as i32));
            let unused=StringArray::from(vec!["x".repeat(unused_bytes);rows]);
            let batch=RecordBatch::try_from_iter([
                ("pl",Arc::new(likelihoods) as ArrayRef),("fallback",Arc::new(fallback)),("unused",Arc::new(unused))]).unwrap();
            let quality=Arc::new(Expr::StringFunction{function:"gq_from_pl".into(),args:vec![Expr::Column{name:"pl".into()}]});
            let expression=Expr::IfElse{
                condition:Arc::new(Expr::Unary{operator:UnaryOperator::Not,value:Arc::new(Expr::IsMissing{value:quality.clone()})}),
                consequent:quality,alternate:Arc::new(Expr::Column{name:"fallback".into()})};
            let (output,bytes)=evaluate_aligned_allocated_bytes(&batch,&expression);
            drop(batch);
            let actual=output.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(actual.len(),rows);assert_eq!(actual.null_count(),0);
            for i in 0..rows {assert_eq!(actual.value(i),if missing_period>0&&i%missing_period==0{i as i32}else{(i%30+1) as i32});}
            eprintln!("coalesce quality missing_period={missing_period} unused_bytes={unused_bytes} rows={rows} allocated_bytes={bytes}");
        }
    }
}

#[test]
fn nested_coalesce_preparation_preserves_the_first_defined_value() {
    use vail_execution::expressions::UnaryOperator;
    use datafusion_common::ScalarValue;
    for depth in [8usize,12] {
        let batch=RecordBatch::try_from_iter((0..depth).map(|i|
            (format!("x{i}"),Arc::new(Int32Array::from(vec![None])) as ArrayRef))).unwrap();
        let mut expression=Expr::RuntimeLiteral{value:ScalarValue::Int32(Some(7))};
        for i in (0..depth).rev() {
            let value=Arc::new(Expr::Column{name:format!("x{i}")});
            expression=Expr::IfElse{
                condition:Arc::new(Expr::Unary{operator:UnaryOperator::Not,value:Arc::new(Expr::IsMissing{value:value.clone()})}),
                consequent:value,alternate:Arc::new(expression)};
        }
        ALLOCATED_BYTES.with(|count|count.set(Some(0)));
        let compiled=lower(&expression,batch.schema().as_ref()).unwrap();
        let bytes=ALLOCATED_BYTES.with(|count|count.replace(None).unwrap());
        let output=compiled.evaluate(&batch).unwrap().into_array(1).unwrap();
        assert_eq!(output.as_any().downcast_ref::<Int32Array>().unwrap(),&Int32Array::from(vec![7]));
        eprintln!("nested coalesce preparation depth={depth} allocated_bytes={bytes}");
    }
}

fn list_child_gather_fixture(width:usize,hidden_values:usize,hidden_list:bool,partial:bool,bitmap_offset:usize)->(RecordBatch,Expr) {
    use arrow_buffer::BooleanBuffer;
    let len=4*width;
    let values=Int32Array::from_iter_values((0..hidden_values+len).map(|i|i as i32));
    let values=values.slice(hidden_values,len);
    let nulls=NullBuffer::new(BooleanBuffer::from((0..bitmap_offset+len).map(|i|
        i<bitmap_offset||(i-bitmap_offset)%7!=1).collect::<Vec<_>>())).slice(bitmap_offset,len);
    let values=Int32Array::new(values.values().clone(),Some(nulls));
    let lists=ListArray::new(Arc::new(Field::new("item",DataType::Int32,true)),
        OffsetBuffer::new(ScalarBuffer::from((0..=4).map(|i|(i*width) as i32).collect::<Vec<_>>())),Arc::new(values),None);
    let choices=ListArray::new(Arc::new(Field::new("item",lists.data_type().clone(),true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0,1,2,3,4])),Arc::new(lists),
        hidden_list.then(||NullBuffer::from(vec![true,false,true,true])));
    let entries=StructArray::from(vec![(Arc::new(Field::new("choices",choices.data_type().clone(),true)),Arc::new(choices) as ArrayRef)]);
    let slots=StructArray::from(vec![(Arc::new(Field::new("value",entries.data_type().clone(),true)),Arc::new(entries) as ArrayRef)]);
    let slot_type=Type::from_arrow(slots.data_type()).unwrap();
    let entries=ListArray::new(Arc::new(Field::new("item",slots.data_type().clone(),true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0,2,4])),Arc::new(slots),None);
    let batch=RecordBatch::try_from_iter([("entries",Arc::new(entries) as ArrayRef)]).unwrap();
    let int=Type::Primitive(PrimitiveType::Int32);let array=Type::Array{array:Box::new(int.clone())};
    let read=Expr::ArrayIndex{value:Arc::new(Expr::GetField{
        value:Arc::new(Expr::GetField{value:Arc::new(Expr::AlignedMapInput{axis:"entry".into(),name:None,dtype:slot_type}),name:"value".into()}),name:"choices".into()}),
        index:Arc::new(Expr::Literal{dtype:int.clone(),value:serde_json::json!(0)})};
    let body=if partial{Expr::IfElse{
        condition:Arc::new(Expr::Binary{operator:vail_execution::expressions::BinaryOperator::Lt,
            left:Arc::new(Expr::AlignedMapInput{axis:"col".into(),name:None,dtype:int}),
            right:Arc::new(Expr::Literal{dtype:Type::Primitive(PrimitiveType::Int32),value:serde_json::json!(1)})}),
        consequent:Arc::new(read),alternate:Arc::new(Expr::Literal{dtype:array.clone(),value:serde_json::json!([])})}}
        else{read};
    let expression=Expr::AlignedMatrixMap{entries:Arc::new(Expr::Column{name:"entries".into()}),
        columns:Arc::new(Expr::Literal{dtype:array.clone(),value:serde_json::json!([0,1])}),
        rows:Arc::new(Expr::Struct{fields:vec![]}),globals:Arc::new(Expr::Struct{fields:vec![]}),
        body:Arc::new(body),dtype:Type::Array{array:Box::new(array)},preserve_cell:false};
    (batch,expression)
}

#[test]
fn selected_list_children_preserve_nulls_and_lifetime_with_hidden_backing() {
    for (name,hidden_values,hidden_list,partial,bitmap_offset) in [
        ("complete",0,false,false,0),("sliced",65_536,false,false,0),
        ("hidden-null-list",0,true,false,0),("partial",0,false,true,0),
        ("bitmap-bit-offset",0,false,false,1),("bitmap-large-backing",0,false,false,65_536)] {
        let width=4096;
        let (batch,expression)=list_child_gather_fixture(width,hidden_values,hidden_list,partial,bitmap_offset);
        let (output,bytes)=evaluate_aligned_allocated_bytes(&batch,&expression);drop(batch);
        let outer=output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(outer.len(),2);assert_eq!(outer.null_count(),0);
        for row in 0..2 {
            let entries=outer.value(row);let entries=entries.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(entries.len(),2);
            for column in 0..2 {
                let entry=row*2+column;
                if partial&&column==1 {assert!(entries.is_valid(column));assert_eq!(entries.value_length(column),0);continue;}
                if hidden_list&&entry==1 {assert!(entries.is_null(column));continue;}
                assert!(entries.is_valid(column));let values=entries.value(column);let values=values.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(values.len(),width);
                for i in 0..width {let index=entry*width+i;
                    assert_eq!(values.is_valid(i),index%7!=1);
                    if index%7!=1{assert_eq!(values.value(i),(hidden_values+index) as i32);}
                }
            }
        }
        eprintln!("selected list child case={name} allocated_bytes={bytes} reported_output_bytes={}",output.get_buffer_memory_size());
    }
}

#[test]
fn nested_list_selection_preserves_repeated_and_reordered_values() {
    for picks in [[0,0],[1,0]] {
        let width=2048;
        let (batch,mut expression)=list_child_gather_fixture(2*width,0,false,false,0);
        let original=batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let slots=original.values().as_any().downcast_ref::<StructArray>().unwrap();
        let entries=slots.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let choices=entries.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let lists=choices.values().as_any().downcast_ref::<ListArray>().unwrap();
        let lists=ListArray::new(match lists.data_type(){DataType::List(field)=>field.clone(),_=>unreachable!()},
            OffsetBuffer::new(ScalarBuffer::from((0..=8).map(|i|(i*width) as i32).collect::<Vec<_>>())),lists.values().clone(),None);
        let choices=ListArray::new(match choices.data_type(){DataType::List(field)=>field.clone(),_=>unreachable!()},OffsetBuffer::new(ScalarBuffer::from(vec![0,2,4,6,8])),Arc::new(lists),None);
        let entries=StructArray::new(entries.fields().clone(),vec![Arc::new(choices)],None);
        let slots=StructArray::new(slots.fields().clone(),vec![Arc::new(entries)],None);
        let entries=ListArray::new(match original.data_type(){DataType::List(field)=>field.clone(),_=>unreachable!()},original.offsets().clone(),Arc::new(slots),None);
        let batch=RecordBatch::try_new(batch.schema(),vec![Arc::new(entries)]).unwrap();
        let Expr::AlignedMatrixMap{body,dtype,..}=&mut expression else{unreachable!()};
        let Expr::ArrayIndex{value,..}=body.as_ref() else{unreachable!()};
        let int=Type::Primitive(PrimitiveType::Int32);
        *body=Arc::new(Expr::ArrayMap{value:Arc::new(Expr::Literal{dtype:Type::Array{array:Box::new(int.clone())},value:serde_json::json!(picks)}),parameter:"pick".into(),
            body:Arc::new(Expr::ArrayIndex{value:value.clone(),index:Arc::new(Expr::LambdaVariable{name:"pick".into(),dtype:Some(int.clone())})})});
        *dtype=Type::Array{array:Box::new(Type::Array{array:Box::new(Type::Array{array:Box::new(int)})})};
        let (output,bytes)=evaluate_aligned_allocated_bytes(&batch,&expression);drop(batch);
        let outer=output.as_any().downcast_ref::<ListArray>().unwrap();assert_eq!(outer.len(),2);
        for row in 0..2 {let entries=outer.value(row);let entries=entries.as_any().downcast_ref::<ListArray>().unwrap();assert_eq!(entries.len(),2);
            for column in 0..2 {let selected=entries.value(column);let selected=selected.as_any().downcast_ref::<ListArray>().unwrap();assert_eq!(selected.len(),2);
                for (position,pick) in picks.into_iter().enumerate(){let values=selected.value(position);let values=values.as_any().downcast_ref::<Int32Array>().unwrap();assert_eq!(values.len(),width);
                    for i in 0..width {let index=((row*2+column)*2+pick)*width+i;
                        assert_eq!(values.is_valid(i),index%7!=1);if index%7!=1{assert_eq!(values.value(i),index as i32);}
                    }
                }
            }
        }
        eprintln!("nested list selection picks={picks:?} allocated_bytes={bytes}");
    }
}
