//! Public indexed-map costs; generated fixtures and full Arrow oracles untimed.
use arrow_array::{ArrayRef, BooleanArray, Float64Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::ScalarValue;
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use vail_execution::expressions::{BinaryOperator as Binary, Expr, lower};
fn variable(name: &str) -> Expr {
    Expr::LambdaVariable {
        name: name.into(),
        dtype: None,
    }
}
fn number(value: f64) -> Expr {
    Expr::RuntimeLiteral {
        value: ScalarValue::Float64(Some(value)),
    }
}
fn binary(operator: Binary, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        operator,
        left: Arc::new(left),
        right: Arc::new(right),
    }
}
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let case = args.get(1).unwrap().as_str();
    let rows: usize = args.get(2).unwrap().parse().unwrap();
    let width: usize = args.get(3).unwrap().parse().unwrap();
    assert!(
        width > 0
            && matches!(
                case,
                "selected" | "identity" | "missing" | "shared" | "nested"
            )
    );
    let nested = case == "nested";
    let source = (0..rows * width)
        .map(|i| {
            (!(i / width % 17 == 16 && i % width == width - 1)).then_some((i % 11) as f64 - 3.0)
        })
        .collect::<Vec<_>>();
    let input: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
        Arc::new(Float64Array::from(source.clone())),
        Some(NullBuffer::from_iter((0..rows).map(|i| i % 29 != 28))),
    ));
    let expected: ArrayRef = if case == "missing" {
        Arc::new(BooleanArray::from_iter((0..rows).map(|row| {
            (row % 29 != 28).then_some(source[row * width + width - 1].is_none())
        })))
    } else {
        Arc::new(Float64Array::from_iter((0..rows).map(|row| {
            if row % 29 == 28 {
                None
            } else {
                source[row * width + width - 1].map(|v| match case {
                    "identity" => v,
                    "shared" => v + v,
                    _ => v * 2.0 + 1.0,
                })
            }
        })))
    };
    let body = match case {
        "identity" => variable("x"),
        "missing" => Expr::IsMissing {
            value: Arc::new(variable("x")),
        },
        "shared" => {
            let x = Arc::new(variable("x"));
            Expr::Binary {
                operator: Binary::Add,
                left: x.clone(),
                right: x,
            }
        }
        _ => binary(
            Binary::Add,
            binary(Binary::Mul, variable("x"), number(2.0)),
            number(1.0),
        ),
    };
    let expression = Expr::ArrayIndex {
        value: Arc::new(Expr::ArrayMap {
            value: Arc::new(if nested {
                variable("xs")
            } else {
                Expr::Column { name: "xs".into() }
            }),
            parameter: "x".into(),
            body: Arc::new(body),
        }),
        index: Arc::new(Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some((width - 1).try_into().unwrap())),
        }),
    };
    let (batch, expression, expected) = if nested {
        let input: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(input.data_type().clone(), true)),
            OffsetBuffer::from_lengths([rows]),
            input,
            None,
        ));
        let expected: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            OffsetBuffer::from_lengths([rows]),
            expected,
            None,
        ));
        (
            RecordBatch::try_from_iter([("xss", input)]).unwrap(),
            Expr::ArrayMap {
                value: Arc::new(Expr::Column { name: "xss".into() }),
                parameter: "xs".into(),
                body: Arc::new(expression),
            },
            expected,
        )
    } else {
        (
            RecordBatch::try_from_iter([("xs", input)]).unwrap(),
            expression,
            expected,
        )
    };
    let start = Instant::now();
    let program = lower(&expression, batch.schema_ref()).unwrap();
    let prepare = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let actual = program
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let first = start.elapsed().as_secs_f64();
    assert_eq!(actual.as_ref(), expected.as_ref());
    drop(actual);
    let mut warmed = vec![];
    for _ in 0..5 {
        let start = Instant::now();
        let fresh = lower(&expression, batch.schema_ref()).unwrap();
        warmed.push(start.elapsed().as_secs_f64());
        assert_eq!(
            fresh
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap()
                .as_ref(),
            expected.as_ref()
        );
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
        json!({"case":case,"rows":rows,"width":width,"physical_rows":batch.num_rows(),"validated":true,"prepare_seconds":prepare,"first_seconds":first,"prepare_plus_first_seconds":prepare+first,"warmed_preparation_seconds":warmed,"evaluation_wall_seconds":samples,"cpu_measured":false})
    );
}
