//! Public map/index semantics and allocation-volume controls.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, ListArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};
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
use vail_execution::expressions::{BinaryOperator as Binary, Expr, Type, lower};
thread_local! {static BYTES:Cell<Option<usize>>=const {Cell::new(None)};}
struct Allocator;
fn charge(n: usize) {
    let _ = BYTES.try_with(|b| {
        if let Some(old) = b.get() {
            b.set(Some(old + n))
        }
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        charge(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        charge(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        charge(n);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn variable(name: &str) -> Expr {
    Expr::LambdaVariable {
        name: name.into(),
        dtype: None,
    }
}
fn number(n: f64) -> Expr {
    Expr::RuntimeLiteral {
        value: ScalarValue::Float64(Some(n)),
    }
}
fn binary(operator: Binary, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        operator,
        left: Arc::new(left),
        right: Arc::new(right),
    }
}
fn polynomial() -> Expr {
    binary(
        Binary::Add,
        binary(Binary::Mul, variable("x"), number(2.0)),
        number(1.0),
    )
}
fn indexed(input: Expr, body: Expr, index: i32) -> Expr {
    Expr::ArrayIndex {
        value: Arc::new(Expr::ArrayMap {
            value: Arc::new(input),
            parameter: "x".into(),
            body: Arc::new(body),
        }),
        index: Arc::new(Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some(index)),
        }),
    }
}
fn planned(expr: &Expr, batch: &RecordBatch) -> Arc<dyn PhysicalExpr> {
    let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
    let logical = vail_execution::logical_expr::lower_logical(expr, &schema).unwrap();
    create_physical_expr(
        &logical,
        &schema,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    )
    .unwrap()
}
fn check(expr: &Expr, batch: &RecordBatch, expected: &dyn Array) {
    for program in [
        lower(expr, batch.schema_ref()).unwrap(),
        planned(expr, batch),
    ] {
        let field = program.return_field(batch.schema_ref()).unwrap();
        assert_eq!(field.data_type(), expected.data_type());
        assert!(field.is_nullable());
        assert!(field.metadata().is_empty());
        let output = program
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        assert_eq!(output.as_ref(), expected);
    }
}
fn lists() -> ListArray {
    ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::new(vec![0i32, 1, 4, 7, 10, 13, 14].into()),
        Arc::new(Float64Array::from(vec![
            Some(99.0),
            Some(0.0),
            None,
            Some(2.0),
            Some(99.0),
            Some(99.0),
            Some(99.0),
            Some(4.0),
            Some(5.0),
            Some(6.0),
            Some(8.0),
            Some(9.0),
            Some(10.0),
            Some(99.0),
        ])),
        Some(NullBuffer::from(vec![true, true, false, true, true, true])),
    )
    .slice(1, 4)
}
#[test]
fn indexed_maps_keep_slices_missing_elements_and_retained_owners() {
    let input: ArrayRef = Arc::new(lists());
    let before = input.to_data();
    let batch = RecordBatch::try_from_iter([("xs", input.clone())]).unwrap();
    let source = || Expr::Column { name: "xs".into() };
    check(
        &indexed(source(), polynomial(), 0),
        &batch,
        &Float64Array::from(vec![Some(1.0), None, Some(9.0), Some(17.0)]),
    );
    check(
        &indexed(source(), polynomial(), 1),
        &batch,
        &Float64Array::from(vec![None, None, Some(11.0), Some(19.0)]),
    );
    check(
        &indexed(source(), polynomial(), -1),
        &batch,
        &Float64Array::from(vec![Some(5.0), None, Some(13.0), Some(21.0)]),
    );
    check(
        &indexed(
            source(),
            Expr::IsMissing {
                value: Arc::new(variable("x")),
            },
            1,
        ),
        &batch,
        &BooleanArray::from(vec![Some(true), None, Some(false), Some(false)]),
    );
    check(
        &indexed(source(), polynomial(), 0),
        &batch.slice(0, 0),
        &Float64Array::from(Vec::<Option<f64>>::new()),
    );
    assert_eq!(input.to_data(), before);
}
#[test]
fn indexed_maps_observe_struct_ancestor_nulls_without_touching_unused_fields() {
    let arrays: ArrayRef = Arc::new(lists());
    let dtype = arrays.data_type().clone();
    let fields: Fields = vec![
        Field::new("xs", dtype, true),
        Field::new("unused", DataType::Utf8, true),
    ]
    .into();
    let unused = StringArray::from(vec!["x".repeat(1024 * 1024); 4]);
    let row: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![arrays, Arc::new(unused)],
        Some(NullBuffer::from(vec![false, true, true, true])),
    ));
    let before = row.to_data();
    let batch = RecordBatch::try_from_iter([("row", row.clone())]).unwrap();
    let source = Expr::GetField {
        value: Arc::new(Expr::Column { name: "row".into() }),
        name: "xs".into(),
    };
    check(
        &indexed(source, polynomial(), 0),
        &batch,
        &Float64Array::from(vec![None, None, Some(9.0), Some(17.0)]),
    );
    assert_eq!(row.to_data(), before);
}
#[test]
fn bounds_remain_demanded_for_zero_products_constants_and_missingness() {
    let empty: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::from_lengths([0]),
        Arc::new(Float64Array::from(Vec::<f64>::new())),
        None,
    ));
    let batch = RecordBatch::try_from_iter([("xs", empty)]).unwrap();
    for body in [
        binary(Binary::Mul, variable("x"), number(0.0)),
        number(0.0),
        binary(
            Binary::Add,
            Expr::RuntimeLiteral {
                value: ScalarValue::Float64(None),
            },
            variable("x"),
        ),
        Expr::IsMissing {
            value: Arc::new(variable("x")),
        },
    ] {
        let expr = indexed(Expr::Column { name: "xs".into() }, body, 0);
        for program in [
            lower(&expr, batch.schema_ref()).unwrap(),
            planned(&expr, &batch),
        ] {
            assert!(program.evaluate(&batch).is_err());
        }
    }
    let absent: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::from_lengths([0]),
        Arc::new(Float64Array::from(Vec::<f64>::new())),
        Some(NullBuffer::from(vec![false])),
    ));
    let batch = RecordBatch::try_from_iter([("xs", absent)]).unwrap();
    check(
        &indexed(Expr::Column { name: "xs".into() }, polynomial(), 0),
        &batch,
        &Float64Array::from(vec![None]),
    );
    check(
        &indexed(
            Expr::Column { name: "xs".into() },
            Expr::IsMissing {
                value: Arc::new(variable("x")),
            },
            0,
        ),
        &batch,
        &BooleanArray::from(vec![None]),
    );
}
#[test]
fn unused_element_failures_still_run_and_row_captures_still_bind() {
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::from_lengths([2]),
        Arc::new(Float64Array::from(vec![1.0, -1.0])),
        None,
    ));
    let batch = RecordBatch::try_from_iter([
        ("xs", list),
        (
            "capture",
            Arc::new(Float64Array::from(vec![10.0])) as ArrayRef,
        ),
    ])
    .unwrap();
    let body = Expr::IfElse {
        condition: Arc::new(binary(Binary::Gt, variable("x"), number(0.0))),
        consequent: Arc::new(variable("x")),
        alternate: Arc::new(Expr::Die {
            message: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Utf8(Some("unused map element failed".into())),
            }),
            dtype: Type::from_arrow(&DataType::Float64).unwrap(),
        }),
    };
    let expr = indexed(Expr::Column { name: "xs".into() }, body, 0);
    for program in [
        lower(&expr, batch.schema_ref()).unwrap(),
        planned(&expr, &batch),
    ] {
        assert!(
            program
                .evaluate(&batch)
                .unwrap_err()
                .to_string()
                .contains("unused map element failed")
        );
    }
    let body = binary(
        Binary::Add,
        variable("x"),
        Expr::Column {
            name: "capture".into(),
        },
    );
    check(
        &indexed(Expr::Column { name: "xs".into() }, body, 0),
        &batch,
        &Float64Array::from(vec![Some(11.0)]),
    );
}

#[test]
fn indexed_float_arithmetic_keeps_bits_and_operand_order() {
    macro_rules! check_bits {
        ($native:ty,$array:ty,$variant:ident,$dtype:expr,$nan:expr,$constant:expr) => {{
            let values = vec![
                Some($nan),
                Some(-0.0),
                Some(<$native>::INFINITY),
                None,
                Some(<$native>::NEG_INFINITY),
                Some(0.0),
            ];
            let child: ArrayRef = Arc::new(<$array>::from(values.clone()));
            let lists: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new_list_field($dtype, true)),
                OffsetBuffer::from_lengths([2, 2, 2]),
                child,
                None,
            ));
            let batch = RecordBatch::try_from_iter([("xs", lists)]).unwrap();
            for idx in [0, 1] {
                for op in [Binary::Add, Binary::Sub, Binary::Mul] {
                    let expr = indexed(
                        Expr::Column { name: "xs".into() },
                        binary(
                            op,
                            Expr::RuntimeLiteral {
                                value: ScalarValue::$variant(Some($constant)),
                            },
                            variable("x"),
                        ),
                        idx,
                    );
                    for program in [
                        lower(&expr, batch.schema_ref()).unwrap(),
                        planned(&expr, &batch),
                    ] {
                        let output = program.evaluate(&batch).unwrap().into_array(3).unwrap();
                        let output = output.as_any().downcast_ref::<$array>().unwrap();
                        for row in 0..3 {
                            let expected = values[row * 2 + idx as usize].map(|v| {
                                // Model runtime operands, not a compile-time
                                // signed-NaN simplification of a constant zero.
                                let c = std::hint::black_box($constant);
                                let v = std::hint::black_box(v);
                                match op {
                                    Binary::Add => c + v,
                                    Binary::Sub => c - v,
                                    Binary::Mul => c * v,
                                    _ => unreachable!(),
                                }
                            });
                            assert_eq!(output.is_null(row), expected.is_none());
                            if let Some(expected) = expected {
                                assert_eq!(
                                    output.value(row).to_bits(),
                                    expected.to_bits(),
                                    "{op:?} idx{idx} row{row} {}",
                                    stringify!($array)
                                );
                            }
                        }
                    }
                }
            }
        }};
    }
    check_bits!(
        f32,
        Float32Array,
        Float32,
        DataType::Float32,
        f32::from_bits(0x7fc0_1234),
        -0.0f32
    );
    check_bits!(
        f64,
        Float64Array,
        Float64,
        DataType::Float64,
        f64::from_bits(0x7ff8_0000_0000_1234),
        -0.0f64
    );
}
#[test]
fn promotion_wrapping_and_shared_parameter_uses_preserve_values() {
    let arrays: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths([2, 2]),
        Arc::new(Int32Array::from(vec![
            Some(i32::MAX),
            None,
            Some(-1),
            Some(0),
        ])),
        None,
    ));
    let batch = RecordBatch::try_from_iter([("xs", arrays)]).unwrap();
    let body = binary(
        Binary::Add,
        variable("x"),
        Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some(1)),
        },
    );
    check(
        &indexed(Expr::Column { name: "xs".into() }, body, 0),
        &batch,
        &Int32Array::from(vec![Some(i32::MIN), Some(0)]),
    );
    check(
        &indexed(
            Expr::Column { name: "xs".into() },
            binary(Binary::Add, variable("x"), number(0.5)),
            0,
        ),
        &batch,
        &Float64Array::from(vec![Some(i32::MAX as f64 + 0.5), Some(-0.5)]),
    );
    let x = Arc::new(variable("x"));
    let body = Expr::Binary {
        operator: Binary::Add,
        left: x.clone(),
        right: x,
    };
    check(
        &indexed(Expr::Column { name: "xs".into() }, body, 0),
        &batch,
        &Int32Array::from(vec![Some(-2), Some(-2)]),
    );
}

#[test]
fn list_backed_sets_keep_their_logical_map_conversion() {
    let dtype = Type::Set {
        set: Box::new(Type::from_arrow(&DataType::Int32).unwrap()),
    }
    .to_arrow()
    .unwrap();
    let DataType::List(field) = dtype else {
        panic!("set storage is a list")
    };
    let values: ArrayRef = Arc::new(ListArray::new(
        field,
        OffsetBuffer::from_lengths([3]),
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        None,
    ));
    let batch = RecordBatch::try_from_iter([("xs", values)]).unwrap();
    let body = binary(
        Binary::Add,
        variable("x"),
        Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some(1)),
        },
    );
    check(
        &indexed(Expr::Column { name: "xs".into() }, body, 0),
        &batch,
        &Int32Array::from(vec![Some(2)]),
    );
}
#[test]
fn indexing_does_not_allocate_a_full_unused_mapped_payload() {
    fn allocated(n: usize, mapped: bool) -> usize {
        let input: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            OffsetBuffer::from_lengths([n, n]),
            Arc::new(Float64Array::from_iter(
                (0..2 * n).map(|i| Some((i % 11) as f64)),
            )),
            Some(NullBuffer::from(vec![true, false])),
        ));
        let (batch, expr, expected): (RecordBatch, Expr, ArrayRef) = if mapped {
            let outer: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(input.data_type().clone(), true)),
                OffsetBuffer::from_lengths([2]),
                input,
                None,
            ));
            (
                RecordBatch::try_from_iter([("xss", outer)]).unwrap(),
                Expr::ArrayMap {
                    value: Arc::new(Expr::Column { name: "xss".into() }),
                    parameter: "xs".into(),
                    body: Arc::new(indexed(variable("xs"), polynomial(), 0)),
                },
                Arc::new(ListArray::new(
                    Arc::new(Field::new_list_field(DataType::Float64, true)),
                    OffsetBuffer::from_lengths([2]),
                    Arc::new(Float64Array::from(vec![Some(1.0), None])),
                    None,
                )),
            )
        } else {
            (
                RecordBatch::try_from_iter([("xs", input)]).unwrap(),
                indexed(Expr::Column { name: "xs".into() }, polynomial(), 0),
                Arc::new(Float64Array::from(vec![Some(1.0), None])),
            )
        };
        let program = lower(&expr, batch.schema_ref()).unwrap();
        assert_eq!(
            program
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap()
                .as_ref(),
            expected.as_ref()
        );
        BYTES.with(|b| b.set(Some(0)));
        let output = program
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let bytes = BYTES.with(|b| b.replace(None).unwrap());
        assert_eq!(output.as_ref(), expected.as_ref());
        bytes
    }
    let observations = [false, true].map(|mapped| {
        let small = allocated(1024, mapped);
        let large = allocated(131072, mapped);
        eprintln!("map-index mapped{mapped} allocation: {small} -> {large}");
        (mapped, small, large)
    });
    assert!(
        observations
            .iter()
            .all(|(_, small, large)| large < &(small + 65536)),
        "unused mapped payload allocation grew: {observations:?}"
    );
}
