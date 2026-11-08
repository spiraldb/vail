//! Diagnostic: the same public prepared expression, whole-batch or tiled.
//! This is not an optimizer rule. Total fixed-width numeric fixtures only.
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, ListArray, RecordBatch, builder::Float64Builder,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::PhysicalExpr;
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use vail_execution::expressions::{
    BinaryOperator as Binary, Expr, Type, UnaryOperator as Unary, lower,
};
fn number(v: f64) -> Expr {
    Expr::RuntimeLiteral {
        value: ScalarValue::Float64(Some(v)),
    }
}
fn var(name: &str) -> Expr {
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
fn index(i: i32) -> Expr {
    Expr::ArrayIndex {
        value: Arc::new(var("mapped")),
        index: Arc::new(Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some(i)),
        }),
    }
}
fn run(program: &Arc<dyn PhysicalExpr>, batch: &RecordBatch, tiled: bool) -> ArrayRef {
    if !tiled {
        return program
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
    }
    let mut out = Float64Builder::with_capacity(batch.num_rows());
    for start in (0..batch.num_rows()).step_by(2048) {
        let tile = batch.slice(start, (batch.num_rows() - start).min(2048));
        let array = program
            .evaluate(&tile)
            .unwrap()
            .into_array(tile.num_rows())
            .unwrap();
        out.append_array(array.as_any().downcast_ref::<Float64Array>().unwrap());
    }
    Arc::new(out.finish())
}
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let case = args.get(1).unwrap().as_str();
    let rows: usize = args.get(2).unwrap().parse().unwrap();
    let mode = args.get(3).unwrap();
    assert!(
        matches!(case, "probability" | "polynomial" | "cheap")
            && matches!(mode.as_str(), "whole" | "tiled")
    );
    let tiled = mode == "tiled";
    let (batch, expression, expected): (RecordBatch, Expr, Float64Array) = if case == "cheap" {
        let input =
            Float64Array::from_iter((0..rows).map(|i| (i % 17 != 16).then_some(i as f64 / 16.0)));
        let expected = Float64Array::from_iter(input.iter().map(|v| v.map(|v| (v + 1.0) * 2.0)));
        (
            RecordBatch::try_from_iter([("x", Arc::new(input) as ArrayRef)]).unwrap(),
            binary(
                Binary::Mul,
                binary(Binary::Add, Expr::Column { name: "x".into() }, number(1.0)),
                number(2.0),
            ),
            expected,
        )
    } else {
        let source: Vec<Option<i32>> = (0..rows * 3)
            .map(|i| (!(i / 3 % 17 == 16 && i % 3 == 1)).then_some((i % 91) as i32))
            .collect();
        let lists = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(3, rows)),
            Arc::new(Int32Array::from(source.clone())),
            Some(NullBuffer::from_iter((0..rows).map(|i| i % 29 != 28))),
        );
        let x = Expr::Cast {
            dtype: Type::from_arrow(&DataType::Float64).unwrap(),
            value: Arc::new(var("x")),
        };
        let body = if case == "probability" {
            binary(
                Binary::Pow,
                number(10.0),
                binary(
                    Binary::Div,
                    Expr::Unary {
                        operator: Unary::Neg,
                        value: Arc::new(x),
                    },
                    number(10.0),
                ),
            )
        } else {
            binary(
                Binary::Add,
                binary(Binary::Mul, x, number(2.0)),
                number(1.0),
            )
        };
        let expression = Expr::Switch {
            value: Arc::new(Expr::ArrayMap {
                value: Arc::new(Expr::Column { name: "xs".into() }),
                parameter: "x".into(),
                body: Arc::new(body),
            }),
            parameter: "mapped".into(),
            cases: vec![],
            default: Arc::new(if case == "probability" {
                binary(
                    Binary::Div,
                    binary(
                        Binary::Add,
                        index(1),
                        binary(Binary::Mul, number(2.0), index(2)),
                    ),
                    binary(
                        Binary::Add,
                        binary(Binary::Add, index(0), index(1)),
                        index(2),
                    ),
                )
            } else {
                binary(
                    Binary::Add,
                    binary(Binary::Add, index(0), index(1)),
                    index(2),
                )
            }),
            missing: Some(Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Float64(None),
            })),
        };
        let expected = Float64Array::from_iter((0..rows).map(|row| {
            if row % 29 == 28 {
                return None;
            }
            let p = source[row * 3..row * 3 + 3]
                .iter()
                .map(|v| {
                    v.map(|v| {
                        if case == "probability" {
                            10.0f64.powf(-(v as f64) / 10.0)
                        } else {
                            v as f64 * 2.0 + 1.0
                        }
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(if case == "probability" {
                (p[1] + 2.0 * p[2]) / ((p[0] + p[1]) + p[2])
            } else {
                (p[0] + p[1]) + p[2]
            })
        }));
        (
            RecordBatch::try_from_iter([("xs", Arc::new(lists) as ArrayRef)]).unwrap(),
            expression,
            expected,
        )
    };
    let start = Instant::now();
    let program = lower(&expression, batch.schema_ref()).unwrap();
    let prepare = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let actual = run(&program, &batch, tiled);
    let first = start.elapsed().as_secs_f64();
    let actual = actual.as_any().downcast_ref::<Float64Array>().unwrap();
    let other = run(&program, &batch, !tiled);
    let other = other.as_any().downcast_ref::<Float64Array>().unwrap();
    let mut reference_max_ulp = 0;
    for (i, value) in expected.iter().enumerate() {
        assert_eq!(actual.is_null(i), value.is_none());
        assert_eq!(other.is_null(i), value.is_none());
        if let Some(value) = value {
            assert_eq!(
                actual.value(i).to_bits(),
                other.value(i).to_bits(),
                "whole/tiled row {i}"
            );
            if case == "probability" {
                // Constant-base pow and the runtime scalar kernel can differ
                // by an ulp. Keep the independent math check, and require the
                // two execution schedules themselves to be bit-identical.
                assert!(
                    (actual.value(i) - value).abs() <= 8.0 * f64::EPSILON * value.abs().max(1.0),
                    "reference row {i}"
                );
                reference_max_ulp =
                    reference_max_ulp.max(actual.value(i).to_bits().abs_diff(value.to_bits()));
            } else {
                assert_eq!(actual.value(i).to_bits(), value.to_bits(), "row {i}");
            }
        }
    }
    for _ in 0..3 {
        std::hint::black_box(run(&program, &batch, tiled));
    }
    let mut samples = vec![];
    for _ in 0..5 {
        let start = Instant::now();
        let mut n = 0;
        while start.elapsed() < Duration::from_millis(20) {
            std::hint::black_box(run(&program, &batch, tiled));
            n += 1;
        }
        samples.push(start.elapsed().as_secs_f64() / n as f64);
    }
    println!(
        "{}",
        json!({"case":case,"rows":rows,"mode":mode,"tile_rows":2048,"validated":true,"whole_tiled_bits_identical":true,"independent_reference":if case=="probability" {"8epsilon absolute/relative"}else{"bit-exact"},"reference_max_ulp":reference_max_ulp,"prepare_seconds":prepare,"first_seconds":first,"evaluation_wall_seconds":samples,"cpu_measured":false,"scope":"same prepared program; final assembly included; fixed3element total fixtures; no default rule or actualPL admission; source owners retained; row-bound not arbitrarylistbytebound"})
    );
}
