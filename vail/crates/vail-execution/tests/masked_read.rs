//! Existing nested values can be masked without copying their payload.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::DFSchema;
use datafusion_expr::{
    execution_props::ExecutionProps, physical_planning_context::PhysicalPlanningContext,
};
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use serde_json::json;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::Arc,
};
use vail_execution::expressions::{Expr, lower};
thread_local! { static BYTES: Cell<Option<usize>> = const { Cell::new(None) }; }
struct Allocator;
fn charge(bytes: usize) {
    let _ = BYTES.try_with(|sum| {
        if let Some(old) = sum.get() {
            sum.set(Some(old + bytes));
        }
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        charge(bytes);
        unsafe { System.realloc(ptr, layout, bytes) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn programs(expr: &Expr, batch: &RecordBatch) -> Vec<Arc<dyn PhysicalExpr>> {
    let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
    let mut expressions = vec![expr.clone()];
    if let Expr::IfElse { alternate, .. } = expr {
        if let Expr::Literal { dtype, value } = alternate.as_ref() {
            if value.is_null() {
                let mut native = expr.clone();
                let Expr::IfElse { alternate, .. } = &mut native else {
                    unreachable!()
                };
                *alternate = Arc::new(Expr::RuntimeLiteral {
                    value: datafusion_common::ScalarValue::try_from(&dtype.to_arrow().unwrap())
                        .unwrap(),
                });
                expressions.push(native);
            }
        }
    }
    expressions
        .iter()
        .flat_map(|expression| {
            let logical = vail_execution::logical_expr::lower_logical(expression, &schema).unwrap();
            [
                lower(expression, batch.schema_ref()).unwrap(),
                create_physical_expr(
                    &logical,
                    &schema,
                    &ExecutionProps::new(),
                    &PhysicalPlanningContext::default(),
                )
                .unwrap(),
            ]
        })
        .collect()
}
fn masked(dtype: &serde_json::Value) -> Expr {
    serde_json::from_value(json!({"op":"if_else", "condition":{"op":"column","name":"keep"},
        "consequent":{"op":"column","name":"record"},"alternate":{"op":"literal","dtype":dtype,"value":null}})).unwrap()
}
#[test]
fn mask_combines_existing_nulls_and_preserves_sliced_nested_values() {
    let numbers: ArrayRef = Arc::new(Int32Array::from(vec![9, 10, 20, 30, 40, 50, 60]));
    let lists: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 1, 2, 2, 3, 5, 6, 7].into()),
        numbers,
        None,
    ));
    let fields = vec![Arc::new(Field::new("xs", lists.data_type().clone(), true))].into();
    let records = StructArray::new(
        fields,
        vec![lists],
        Some(NullBuffer::from(vec![
            true, true, true, true, false, true, true,
        ])),
    );
    let batch = RecordBatch::try_from_iter([
        ("record", Arc::new(records.slice(1, 5)) as ArrayRef),
        (
            "keep",
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(true),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    for program in programs(
        &masked(&json!({"struct":[["xs",{"array":"int32"}]]})),
        &batch,
    ) {
        let result = program.evaluate(&batch).unwrap().into_array(5).unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            (0..5).map(|i| result.is_valid(i)).collect::<Vec<_>>(),
            vec![true, false, false, false, true]
        );
        let input = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(result.column(0), input.column(0));
        assert_eq!(
            program
                .evaluate(&batch.slice(0, 0))
                .unwrap()
                .into_array(0)
                .unwrap()
                .len(),
            0
        );
    }
}
#[test]
fn empty_struct_length_and_nulls_survive_masking() {
    let batch = RecordBatch::try_from_iter([
        (
            "record",
            Arc::new(StructArray::new_empty_fields(
                3,
                Some(NullBuffer::from(vec![true, true, false])),
            )) as ArrayRef,
        ),
        (
            "keep",
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(true)])) as ArrayRef,
        ),
    ])
    .unwrap();
    for program in programs(&masked(&json!({"struct":[]})), &batch) {
        let result = program.evaluate(&batch).unwrap().into_array(3).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(
            (0..3).map(|i| result.is_valid(i)).collect::<Vec<_>>(),
            vec![true, false, false]
        );
    }
}
#[test]
fn masking_allocation_does_not_grow_with_payload_bytes() {
    let mut sizes = Vec::new();
    for bytes in [1024, 1 << 20] {
        let text = "x".repeat(bytes);
        let strings: ArrayRef = Arc::new(StringArray::from(vec![text.as_str(); 16]));
        let fields = vec![Arc::new(Field::new("text", DataType::Utf8, true))].into();
        let records = StructArray::new(fields, vec![strings], None);
        let batch = RecordBatch::try_from_iter([
            ("record", Arc::new(records) as ArrayRef),
            (
                "keep",
                Arc::new(BooleanArray::from_iter((0..16).map(|i| Some(i % 2 == 0)))) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut totals = Vec::new();
        for program in programs(&masked(&json!({"struct":[["text","str"]]})), &batch) {
            BYTES.with(|sum| sum.set(Some(0)));
            let result = program.evaluate(&batch).unwrap().into_array(16).unwrap();
            let total = BYTES.with(|sum| sum.replace(None).unwrap());
            assert_eq!(result.null_count(), 8);
            totals.push(total);
        }
        sizes.push(totals);
    }
    for i in 0..sizes[0].len() {
        assert!(
            sizes[1][i] < sizes[0][i] + 16384,
            "payload copying: {sizes:?}"
        );
    }
}
#[test]
fn unselected_computed_branch_stays_lazy() {
    let batch = RecordBatch::try_from_iter([(
        "keep",
        Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
    )])
    .unwrap();
    let expr:Expr=serde_json::from_value(json!({"op":"if_else","condition":{"op":"column","name":"keep"},
        "consequent":{"op":"die","dtype":"int32","message":{"op":"literal","dtype":"str","value":"unselected"}},
        "alternate":{"op":"literal","dtype":"int32","value":null}})).unwrap();
    for program in programs(&expr, &batch) {
        assert_eq!(
            program
                .evaluate(&batch)
                .unwrap()
                .into_array(2)
                .unwrap()
                .null_count(),
            2
        );
    }
}

#[test]
fn scalar_nested_capture_does_not_broadcast_into_unselected_rows() {
    use vail_execution::expressions::lower_with_globals;
    let mut costs = vec![];
    for bytes in [1024, 1 << 20] {
        let text = "x".repeat(bytes);
        let strings: ArrayRef = Arc::new(StringArray::from(vec![text]));
        let fields = vec![Arc::new(Field::new("text", DataType::Utf8, true))].into();
        let globals = RecordBatch::try_from_iter([(
            "payload",
            Arc::new(StructArray::new(fields, vec![strings], None)) as ArrayRef,
        )])
        .unwrap();
        let batch = RecordBatch::try_from_iter([(
            "keep",
            Arc::new(BooleanArray::from(vec![None; 64])) as ArrayRef,
        )])
        .unwrap();
        let expr:Expr=serde_json::from_value(json!({"op":"if_else","condition":{"op":"column","name":"keep"},
            "consequent":{"op":"global","name":"payload"},"alternate":{"op":"literal","dtype":{"struct":[["text","str"]]},"value":null}})).unwrap();
        let program = lower_with_globals(&expr, batch.schema_ref(), &globals).unwrap();
        BYTES.with(|sum| sum.set(Some(0)));
        let result = program.evaluate(&batch).unwrap().into_array(64).unwrap();
        let allocated = BYTES.with(|sum| sum.replace(None).unwrap());
        assert_eq!(result.null_count(), 64);
        costs.push(allocated);
        let mixed = RecordBatch::try_from_iter([(
            "keep",
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])) as ArrayRef,
        )])
        .unwrap();
        let result = program.evaluate(&mixed).unwrap().into_array(4).unwrap();
        assert_eq!(
            (0..4).map(|i| result.is_valid(i)).collect::<Vec<_>>(),
            vec![true, false, false, true]
        );
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        let strings = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.value(0).len(), bytes);
        assert_eq!(strings.value(3).len(), bytes);
    }
    assert!(
        costs[1] < costs[0] + 16384,
        "scalar payload was broadcast: {costs:?}"
    );
}

#[test]
fn borrowed_string_storage_preserves_declared_type() {
    let batch = RecordBatch::try_from_iter([
        (
            "record",
            Arc::new(arrow_array::StringViewArray::from(vec!["first", "second"])) as ArrayRef,
        ),
        (
            "keep",
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
        ),
    ])
    .unwrap();
    for program in programs(&masked(&json!("str")), &batch) {
        let result = program.evaluate(&batch).unwrap().into_array(2).unwrap();
        assert_eq!(result.data_type(), &DataType::Utf8);
        assert_eq!(
            result.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![Some("first"), None])
        );
    }
}

#[test]
fn representation_cast_does_not_copy_unselected_view_payload() {
    let text = "x".repeat(1 << 20);
    let values =
        arrow_array::StringViewArray::from_iter_values(std::iter::repeat_n(text.as_str(), 32));
    let batch = RecordBatch::try_from_iter([
        ("record", Arc::new(values) as ArrayRef),
        (
            "keep",
            Arc::new(BooleanArray::from_iter((0..32).map(|i| Some(i == 3)))) as ArrayRef,
        ),
    ])
    .unwrap();
    for program in programs(&masked(&json!("str")), &batch) {
        BYTES.with(|sum| sum.set(Some(0)));
        let result = program.evaluate(&batch).unwrap().into_array(32).unwrap();
        let total = BYTES.with(|sum| sum.replace(None).unwrap());
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.null_count(), 31);
        assert_eq!(strings.value(3), text);
        assert!(total < 8 << 20, "unselected view data was copied: {total}");
    }
}

#[test]
fn nested_view_conversion_keeps_unselected_payload_lazy() {
    let text = "x".repeat(1 << 20);
    let values: ArrayRef = Arc::new(arrow_array::StringViewArray::from_iter_values(
        std::iter::repeat_n(text.as_str(), 32),
    ));
    let records = StructArray::new(
        vec![Arc::new(Field::new("v", DataType::Utf8View, true))].into(),
        vec![values],
        None,
    );
    let batch = RecordBatch::try_from_iter([
        ("input", Arc::new(records) as ArrayRef),
        (
            "keep",
            Arc::new(BooleanArray::from_iter((0..32).map(|i| Some(i == 3)))) as ArrayRef,
        ),
    ])
    .unwrap();
    let expr: Expr = serde_json::from_value(
        json!({"op":"if_else","condition":{"op":"column","name":"keep"},
        "consequent":{"op":"get_field","value":{"op":"column","name":"input"},"name":"v"},
        "alternate":{"op":"literal","dtype":"str","value":null}}),
    )
    .unwrap();
    for program in programs(&expr, &batch) {
        BYTES.with(|sum| sum.set(Some(0)));
        let result = program.evaluate(&batch).unwrap().into_array(32).unwrap();
        let allocated = BYTES.with(|sum| sum.replace(None).unwrap());
        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.null_count(), 31);
        assert_eq!(strings.value(3), text);
        assert!(
            allocated < 8 << 20,
            "unselected nested view data was copied: {allocated}"
        );
    }
}
