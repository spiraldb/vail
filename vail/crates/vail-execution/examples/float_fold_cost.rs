//! Public float map/fold workloads; complete stepwise oracles are untimed.
use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::ScalarValue;
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use vail_execution::expressions::{
    BinaryOperator as Binary, Expr, Type, UnaryOperator as Unary, lower,
};

fn variable(name: &str) -> Expr {
    Expr::LambdaVariable {
        name: name.into(),
        dtype: None,
    }
}
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let case = args.get(1).map(String::as_str).unwrap_or("f64_sum");
    let rows: usize = args.get(2).unwrap().parse().unwrap();
    let width: usize = args.get(3).unwrap().parse().unwrap();
    let f32 = case == "f32_sum";
    let flatmap = case == "flat_sum";
    let dtype = if f32 {
        DataType::Float32
    } else {
        DataType::Float64
    };
    let source: Vec<Option<f64>> = (0..rows * width)
        .map(|i| {
            (!(i / width % 13 == 12 && i % width == width - 1))
                .then_some((i % 17) as f64 * 0.125 - 0.5)
        })
        .collect();
    let child: ArrayRef = if f32 {
        Arc::new(Float32Array::from_iter(
            source.iter().map(|v| v.map(|v| v as f32)),
        ))
    } else {
        Arc::new(Float64Array::from(source.clone()))
    };
    let lists = ListArray::new(
        Arc::new(Field::new_list_field(dtype.clone(), true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
        child,
        Some(NullBuffer::from_iter((0..rows).map(|i| i % 29 != 28))),
    );
    let batch = RecordBatch::try_from_iter([("xs", Arc::new(lists) as ArrayRef)]).unwrap();
    let body = if flatmap {
        Expr::Array {
            element_type: Type::from_arrow(&dtype).unwrap(),
            elements: vec![
                variable("x"),
                Expr::Unary {
                    operator: Unary::Neg,
                    value: Arc::new(variable("x")),
                },
            ],
        }
    } else {
        Expr::Binary {
            operator: Binary::Add,
            left: Arc::new(variable("x")),
            right: Arc::new(Expr::RuntimeLiteral {
                value: if f32 {
                    ScalarValue::Float32(Some(1.0))
                } else {
                    ScalarValue::Float64(Some(1.0))
                },
            }),
        }
    };
    let mapped = Expr::ArrayMap {
        value: Arc::new(Expr::Column { name: "xs".into() }),
        parameter: "x".into(),
        body: Arc::new(body),
    };
    let expression = Expr::ArrayFold {
        value: Arc::new(if flatmap {
            Expr::ArrayFlatten {
                value: Arc::new(mapped),
            }
        } else {
            mapped
        }),
        zero: Arc::new(Expr::RuntimeLiteral {
            value: if f32 {
                ScalarValue::Float32(Some(7.0))
            } else {
                ScalarValue::Float64(Some(7.0))
            },
        }),
        accumulator: "acc".into(),
        parameter: "v".into(),
        body: Arc::new(Expr::Binary {
            operator: Binary::Add,
            left: Arc::new(variable("acc")),
            right: Arc::new(variable("v")),
        }),
        scan: false,
    };
    let start = Instant::now();
    let program = lower(&expression, batch.schema_ref()).unwrap();
    let prepare = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let actual = program.evaluate(&batch).unwrap().into_array(rows).unwrap();
    let first = start.elapsed().as_secs_f64();
    for row in 0..rows {
        let mut expected = (row % 29 != 28).then_some(7.0);
        for &v in &source[row * width..(row + 1) * width] {
            let first = if flatmap {
                v
            } else {
                v.map(|v| {
                    if f32 {
                        (v as f32 + 1.0) as f64
                    } else {
                        v + 1.0
                    }
                })
            };
            for v in std::iter::once(first).chain(flatmap.then_some(v.map(|v| -v))) {
                expected = expected.zip(v).map(|(a, b)| {
                    if f32 {
                        (a as f32 + b as f32) as f64
                    } else {
                        a + b
                    }
                });
            }
        }
        assert_eq!(actual.is_null(row), expected.is_none());
        if let Some(expected) = expected {
            if f32 {
                assert_eq!(
                    actual
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap()
                        .value(row)
                        .to_bits(),
                    (expected as f32).to_bits()
                );
            } else {
                assert_eq!(
                    actual
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row)
                        .to_bits(),
                    expected.to_bits()
                );
            }
        }
    }
    drop(actual);
    let mut warmed = vec![];
    for _ in 0..5 {
        let start = Instant::now();
        let fresh = lower(&expression, batch.schema_ref()).unwrap();
        warmed.push(start.elapsed().as_secs_f64());
        std::hint::black_box(fresh.evaluate(&batch).unwrap());
    }
    let mut samples = vec![];
    for _ in 0..5 {
        let start = Instant::now();
        let mut n = 0;
        while start.elapsed() < Duration::from_millis(20) {
            std::hint::black_box(program.evaluate(&batch).unwrap());
            n += 1;
        }
        samples.push(start.elapsed().as_secs_f64() / n as f64);
    }
    println!(
        "{}",
        json!({"case":case,"rows":rows,"width":width,"validated":true,"prepare_seconds":prepare,"first_seconds":first,"prepare_plus_first_seconds":prepare+first,"warmed_preparation_seconds":warmed,"evaluation_wall_seconds":samples,"cpu_measured":false})
    );
}
