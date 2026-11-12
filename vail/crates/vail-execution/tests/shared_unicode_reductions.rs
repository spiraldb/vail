//! Public Unicode semantics and allocation volume; input fixtures are excluded.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{
    execution_props::ExecutionProps, physical_planning_context::PhysicalPlanningContext,
};
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::Arc,
};
use vail_execution::expressions::{BinaryOperator, Expr, PrimitiveType, Type, lower};

thread_local! { static BYTES: Cell<Option<usize>> = const { Cell::new(None) }; }
struct Allocator;
fn charge(bytes: usize) {
    let _ = BYTES.try_with(|count| {
        if let Some(old) = count.get() {
            count.set(Some(old + bytes));
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

fn variable() -> Arc<Expr> {
    Arc::new(Expr::LambdaVariable {
        name: "entry".into(),
        dtype: None,
    })
}
fn field(name: &str) -> Expr {
    Expr::GetField {
        value: variable(),
        name: name.into(),
    }
}
fn expression(filter: u8) -> Expr {
    let source = Arc::new(Expr::Column { name: "xs".into() });
    let owner = if filter == 0 {
        source
    } else {
        Arc::new(Expr::ArrayFilter {
            value: source,
            parameter: "entry".into(),
            predicate: Arc::new(if filter == 1 {
                field("keep")
            } else {
                Expr::RuntimeLiteral {
                    value: ScalarValue::Boolean(Some(false)),
                }
            }),
        })
    };
    let length = Arc::new(Expr::StringFunction {
        function: "character_length".into(),
        args: vec![field("text")],
    });
    Expr::Struct {
        fields: [
            ("short", BinaryOperator::Le, false),
            ("long", BinaryOperator::Gt, false),
            ("fraction", BinaryOperator::Gt, true),
        ]
        .into_iter()
        .map(|(name, op, fraction)| {
            (
                name.into(),
                Expr::NestedFunction {
                    function: if fraction {
                        "__vail_list_reduce_fraction"
                    } else {
                        "__vail_list_reduce_count_where"
                    }
                    .into(),
                    args: vec![Expr::ArrayMap {
                        value: owner.clone(),
                        parameter: "entry".into(),
                        body: Arc::new(Expr::Binary {
                            operator: op,
                            left: length.clone(),
                            right: Arc::new(Expr::RuntimeLiteral {
                                value: ScalarValue::Int32(Some(2)),
                            }),
                        }),
                    }],
                    dtype: Type::Primitive(if fraction {
                        PrimitiveType::Float64
                    } else {
                        PrimitiveType::Int64
                    }),
                },
            )
        })
        .collect(),
    }
}
fn batch(bytes: usize) -> RecordBatch {
    // One selected large value and one equally large null-parent payload.
    // Prefix/suffix records make both list and string offsets nonzero.
    let text = "x".repeat(bytes);
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("prefix"),
        Some(text.as_str()),
        Some(text.as_str()),
        Some("e\u{301}"),
        None,
        Some("suffix"),
    ]));
    let records = StructArray::new(
        vec![
            Arc::new(Field::new("text", DataType::Utf8, true)),
            Arc::new(Field::new("keep", DataType::Boolean, true)),
        ]
        .into(),
        vec![
            strings,
            Arc::new(BooleanArray::from(vec![
                true, true, true, false, true, true,
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true, true, true])),
    );
    let lists = ListArray::new(
        Arc::new(Field::new("item", records.data_type().clone(), true)),
        OffsetBuffer::new(vec![0_i32, 1, 5, 5, 6].into()),
        Arc::new(records),
        Some(NullBuffer::from(vec![true, true, true, false])),
    )
    .slice(1, 3);
    RecordBatch::try_from_iter([("xs", Arc::new(lists) as ArrayRef)]).unwrap()
}
fn programs(expression: &Expr, batch: &RecordBatch) -> Vec<Arc<dyn PhysicalExpr>> {
    let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
    let logical = vail_execution::logical_expr::lower_logical(expression, &schema).unwrap();
    vec![
        lower(expression, batch.schema_ref()).unwrap(),
        create_physical_expr(
            &logical,
            &schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap(),
    ]
}
#[test]
fn unicode_shared_reductions_borrow_selected_and_null_hidden_payloads() {
    for filter in 0..=2 {
        let mut costs = Vec::new();
        for bytes in [16, 8 * 1024 * 1024] {
            let batch = batch(bytes);
            let expression = expression(filter);
            for program in programs(&expression, &batch) {
                BYTES.with(|n| n.set(Some(0)));
                let output = program.evaluate(&batch).unwrap().into_array(3).unwrap();
                let allocated = BYTES.with(|n| n.replace(None).unwrap());
                costs.push(allocated);
                let rows = output.as_any().downcast_ref::<StructArray>().unwrap();
                let short = rows
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let long = rows
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let fraction = rows
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(short.value(0), i64::from(filter == 0));
                assert_eq!(long.value(0), i64::from(filter != 2));
                if filter == 2 {
                    assert!(fraction.is_null(0));
                } else {
                    assert_eq!(fraction.value(0), if filter == 0 { 0.25 } else { 0.5 });
                }
                assert_eq!(short.value(1), 0);
                assert_eq!(long.value(1), 0);
                assert!(fraction.is_null(1));
                assert!(short.is_null(2) && long.is_null(2) && fraction.is_null(2));
            }
        }
        eprintln!("Unicode filter{filter} evaluation allocated bytes: {costs:?}");
        assert!(
            costs[2] < costs[0] + 512 * 1024 && costs[3] < costs[1] + 512 * 1024,
            "payload-sized allocation grew; fixtures are excluded: {costs:?}"
        );
    }
}
