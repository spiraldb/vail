//! Public ordered float-fold semantics and allocation volume, excluding fixtures.
use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::ScalarValue;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::Arc,
};
use vail_execution::expressions::{BinaryOperator as Binary, Expr, Type, UnaryOperator as Unary, lower};

thread_local! { static BYTES: Cell<Option<usize>> = const { Cell::new(None) }; }
struct Allocator;
fn charge(n: usize) {
    let _ = BYTES.try_with(|sum| {
        if let Some(old) = sum.get() {
            sum.set(Some(old + n));
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
fn binary(operator: Binary, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        operator,
        left: Arc::new(left),
        right: Arc::new(right),
    }
}
fn mapped_fold(seed: ScalarValue, operator: Binary, flatmap: bool) -> Expr {
    let body = if flatmap {
        Expr::Array {
            element_type: Type::from_arrow(&seed.data_type()).unwrap(),
            elements: vec![
                variable("x"),
                Expr::Unary {
                    operator: Unary::Neg,
                    value: Arc::new(variable("x")),
                },
            ],
        }
    } else {
        variable("x")
    };
    let mapped = Expr::ArrayMap {
        value: Arc::new(Expr::Column { name: "xs".into() }),
        parameter: "x".into(),
        body: Arc::new(body),
    };
    Expr::ArrayFold {
        value: Arc::new(if flatmap {
            Expr::ArrayFlatten {
                value: Arc::new(mapped),
            }
        } else {
            mapped
        }),
        zero: Arc::new(Expr::RuntimeLiteral { value: seed }),
        accumulator: "acc".into(),
        parameter: "v".into(),
        body: Arc::new(binary(operator, variable("acc"), variable("v"))),
        scan: false,
    }
}

#[test]
fn float_folds_keep_seed_bits_precision_order_and_missingness_across_blocks() {
    macro_rules! check {
        ($native:ty,$array:ty,$variant:ident,$dtype:expr,$nan:expr) => {{
            let rows: Vec<(bool, Vec<Option<$native>>)> = vec![
                (true, vec![Some(99.0)]),
                (true, vec![]),
                (true, vec![Some(0.0), Some(-0.0)]),
                (true, vec![Some(1e20), Some(-1e20), Some(3.0)]),
                (
                    true,
                    vec![Some(<$native>::INFINITY), Some(<$native>::NEG_INFINITY)],
                ),
                (true, vec![Some($nan), Some(1.0)]),
                (true, vec![Some(1.0), None, Some(2.0)]),
                (false, vec![Some($nan); 5000]),
                (
                    true,
                    (0..16387)
                        .map(|i| {
                            Some(match i % 4 {
                                0 => 1e20,
                                1 => 1.0,
                                2 => -1e20,
                                _ => -0.0,
                            })
                        })
                        .collect(),
                ),
                (true, vec![Some(99.0)]),
            ];
            let child: ArrayRef = Arc::new(<$array>::from_iter(
                rows.iter().flat_map(|(_, values)| values.iter().copied()),
            ));
            let lists = ListArray::new(
                Arc::new(Field::new_list_field($dtype, true)),
                OffsetBuffer::from_lengths(rows.iter().map(|(_, v)| v.len())),
                child,
                Some(NullBuffer::from_iter(rows.iter().map(|(valid, _)| *valid))),
            )
            .slice(1, rows.len() - 2);
            let original = lists.to_data();
            let batch = RecordBatch::try_from_iter([("xs", Arc::new(lists) as ArrayRef)]).unwrap();
            for seed in [Some(-0.0), Some(1.0), Some($nan), None] {
                for operator in [Binary::Add, Binary::Mul] {
                    for flatmap in [false, true] {
                        let program = lower(
                            &mapped_fold(ScalarValue::$variant(seed), operator, flatmap),
                            batch.schema_ref(),
                        )
                        .unwrap();
                        let output = program
                            .evaluate(&batch)
                            .unwrap()
                            .into_array(batch.num_rows())
                            .unwrap();
                        let output = output.as_any().downcast_ref::<$array>().unwrap();
                        for (row, (valid, values)) in rows[1..rows.len() - 1].iter().enumerate() {
                            let mut expected = if *valid { seed } else { None };
                            if *valid {
                                for &value in values {
                                    for next in std::iter::once(value)
                                        .chain(flatmap.then_some(value.map(|v| -v)))
                                    {
                                        expected =
                                            expected.zip(next).map(|(a, b)| match operator {
                                                Binary::Add => a + b,
                                                Binary::Mul => a * b,
                                                _ => unreachable!(),
                                            });
                                    }
                                }
                            }
                            assert_eq!(output.is_null(row), expected.is_none());
                            if let Some(expected) = expected {
                                assert_eq!(
                                    output.value(row).to_bits(),
                                    expected.to_bits(),
                                    "{operator:?} flatmap{flatmap} row{row}"
                                );
                            }
                        }
                    }
                }
            }
            assert_eq!(batch.column(0).to_data(), original);
        }};
    }
    check!(
        f32,
        Float32Array,
        Float32,
        DataType::Float32,
        f32::from_bits(0x7fc0_1234)
    );
    check!(
        f64,
        Float64Array,
        Float64,
        DataType::Float64,
        f64::from_bits(0x7ff8_0000_0000_1234)
    );
}

#[test]
fn float_map_fold_does_not_allocate_a_mapped_list() {
    fn allocated(n: usize) -> usize {
        let lists = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            OffsetBuffer::from_lengths([n]),
            Arc::new(Float64Array::from(vec![1.0; n])),
            None,
        );
        let batch = RecordBatch::try_from_iter([("xs", Arc::new(lists) as ArrayRef)]).unwrap();
        let mut expr = mapped_fold(ScalarValue::Float64(Some(7.0)), Binary::Add, false);
        if let Expr::ArrayFold { value, .. } = &mut expr {
            *value = Arc::new(Expr::ArrayMap {
                value: Arc::new(Expr::Column { name: "xs".into() }),
                parameter: "x".into(),
                body: Arc::new(binary(
                    Binary::Add,
                    variable("x"),
                    Expr::RuntimeLiteral {
                        value: ScalarValue::Float64(Some(2.0)),
                    },
                )),
            });
        }
        let program = lower(&expr, batch.schema_ref()).unwrap();
        let expected = Float64Array::from(vec![7.0 + n as f64 * 3.0]);
        assert_eq!(
            program
                .evaluate(&batch)
                .unwrap()
                .into_array(1)
                .unwrap()
                .as_ref(),
            &expected
        );
        BYTES.with(|sum| sum.set(Some(0)));
        let output = program.evaluate(&batch).unwrap().into_array(1).unwrap();
        let bytes = BYTES.with(|sum| sum.replace(None).unwrap());
        assert_eq!(output.as_ref(), &expected);
        bytes
    }
    // Both inputs exceed the bounded workspace's initial block size.
    let small = allocated(16384);
    let large = allocated(65536);
    eprintln!("float mapped-fold evaluation allocation: {small} -> {large}");
    assert!(
        large < small + 256 * 1024,
        "mapped-fold temporary grew with input: {small} -> {large}"
    );
}
