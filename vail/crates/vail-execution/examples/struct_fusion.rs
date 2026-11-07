//! Tiny local controls for direct-output list composition. All output checks are untimed.
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{Value, json};
use std::{sync::Arc, time::Instant};
use vail_execution::expressions::{Expr, lower};
fn var(name: &str) -> Value {
    json!({"op":"lambda_variable","name":name})
}
fn num(n: i32) -> Value {
    json!({"op":"literal","dtype":"int32","value":n})
}
fn bin(op: &str, a: Value, b: Value) -> Value {
    json!({"op":"binary","operator":op,"left":a,"right":b})
}
fn map(value: Value, body: Value) -> Value {
    json!({"op":"array_map","value":value,"parameter":"x","body":body})
}
fn field(value: Value, name: &str) -> Value {
    json!({"op":"get_field","value":value,"name":name})
}
fn record(x: Value) -> Value {
    json!({"op":"struct","fields":[["a",x.clone()],["b",bin("mul",x,num(3))]]})
}
fn filt(value: Value, p: Value) -> Value {
    json!({"op":"array_filter","value":value,"parameter":"x","predicate":p})
}
fn expand(value: Value, records: bool) -> Value {
    let values = vec![var("x"), bin("add", var("x"), num(1))];
    let (dtype, values) = if records {
        (
            json!({"struct":[["a","int32"],["b","int32"]]}),
            values.into_iter().map(record).collect(),
        )
    } else {
        (json!("int32"), values)
    };
    json!({"op":"array_flatten","value":map(value,json!({"op":"array","element_type":dtype,"elements":values}))})
}
fn main() {
    let case = std::env::args().nth(1).unwrap_or("maps".into());
    let nullable = case.ends_with("-null");
    let base = case.strip_suffix("-null").unwrap_or(&case);
    let rows = 64;
    let width = 1024;
    let inputs = (0..rows * width)
        .map(|i| (!nullable || i % 31 != 0).then_some((i % 64) as i32))
        .collect::<Vec<_>>();
    let valid = (0..rows)
        .map(|i| !nullable || i % 13 != 0)
        .collect::<Vec<_>>();
    let xs = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
        Arc::new(Int32Array::from(inputs.clone())),
        nullable.then(|| NullBuffer::from(valid.clone())),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            xs.data_type().clone(),
            true,
        )])),
        vec![Arc::new(xs)],
    )
    .unwrap();
    let source = json!({"op":"column","name":"xs"});
    let expression = match base {
        "maps" => map(
            map(
                map(source, record(bin("add", var("x"), num(1)))),
                record(bin("add", field(var("x"), "b"), num(2))),
            ),
            record(bin("mul", field(var("x"), "a"), num(2))),
        ),
        "maps-flatmap" => map(
            map(
                expand(source, true),
                record(bin("add", field(var("x"), "b"), num(2))),
            ),
            record(bin("mul", field(var("x"), "a"), num(2))),
        ),
        "filters-flatmap" => map(
            expand(
                map(
                    filt(
                        map(source, record(var("x"))),
                        bin("gt", field(var("x"), "a"), num(32)),
                    ),
                    field(var("x"), "b"),
                ),
                true,
            ),
            record(field(var("x"), "a")),
        ),
        "filters" => map(
            filt(
                filt(
                    map(source, record(var("x"))),
                    bin("gt", field(var("x"), "a"), num(16)),
                ),
                bin("lt", field(var("x"), "b"), num(144)),
            ),
            record(bin("add", field(var("x"), "b"), num(2))),
        ),
        "flatmaps" => map(
            filt(
                expand(source, true),
                bin("gt", field(var("x"), "a"), num(32)),
            ),
            record(bin("add", field(var("x"), "b"), num(2))),
        ),
        "fold" => {
            json!({"op":"array_fold","value":filt(expand(map(source,bin("add",var("x"),num(1))),false),bin("gt",var("x"),num(32))),"zero":num(7),"accumulator":"acc","parameter":"v","body":bin("add",var("acc"),var("v")),"scan":false})
        }
        "simple" => map(source, bin("add", var("x"), num(1))),
        _ => panic!("unknown case"),
    };
    let expr: Expr = serde_json::from_value(expression).unwrap();
    let start = Instant::now();
    let program = lower(&expr, batch.schema_ref()).unwrap();
    let prepare_s = start.elapsed().as_secs_f64();
    let expected: ArrayRef = if base == "fold" {
        Arc::new(Int32Array::from(
            (0..rows)
                .map(|r| {
                    valid[r].then(|| {
                        inputs[r * width..(r + 1) * width]
                            .iter()
                            .flat_map(|x| [x.map(|x| x + 1), x.map(|x| x + 2)])
                            .flatten()
                            .filter(|x| *x > 32)
                            .fold(7i32, i32::wrapping_add)
                    })
                })
                .collect::<Vec<_>>(),
        ))
    } else {
        let mut a = vec![];
        let mut b = vec![];
        let mut lengths = vec![];
        for r in 0..rows {
            let begin = a.len();
            if valid[r] {
                for &x in &inputs[r * width..(r + 1) * width] {
                    let values = match base {
                        "maps" => vec![x.map(|x| ((x + 1) * 3 + 2) * 2)],
                        "maps-flatmap" => [x, x.map(|x| x + 1)]
                            .into_iter()
                            .map(|x| x.map(|x| (x * 3 + 2) * 2))
                            .collect(),
                        "filters-flatmap" => x
                            .filter(|x| *x > 32)
                            .into_iter()
                            .flat_map(|x| [Some(x * 3), Some(x * 3 + 1)])
                            .collect(),
                        "filters" => x
                            .filter(|x| *x > 16 && *x * 3 < 144)
                            .map(|x| Some(x * 3 + 2))
                            .into_iter()
                            .collect(),
                        "flatmaps" => [x, x.map(|x| x + 1)]
                            .into_iter()
                            .flatten()
                            .filter(|x| *x > 32)
                            .map(|x| Some(x * 3 + 2))
                            .collect(),
                        "simple" => vec![x.map(|x| x + 1)],
                        _ => unreachable!(),
                    };
                    for value in values {
                        a.push(value);
                        b.push(value.map(|x| x * 3));
                    }
                }
            }
            lengths.push(a.len() - begin);
        }
        let values: ArrayRef = if base == "simple" {
            Arc::new(Int32Array::from(a))
        } else {
            Arc::new(StructArray::new(
                vec![
                    Arc::new(Field::new("a", DataType::Int32, true)),
                    Arc::new(Field::new("b", DataType::Int32, true)),
                ]
                .into(),
                vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
                None,
            ))
        };
        Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            OffsetBuffer::from_lengths(lengths),
            values,
            nullable.then(|| NullBuffer::from(valid)),
        ))
    };
    let eval = || program.evaluate(&batch).unwrap().into_array(rows).unwrap();
    for _ in 0..3 {
        assert_eq!(eval().as_ref(), expected.as_ref());
    }
    let mut samples = vec![];
    for _ in 0..7 {
        let start = Instant::now();
        let out = eval();
        samples.push(start.elapsed().as_secs_f64());
        assert_eq!(out.as_ref(), expected.as_ref());
    }
    println!(
        "{}",
        json!({"case":case,"rows":rows,"width":width,"disabled":std::env::var_os("VAIL_DISABLE_LIST_FLATMAP").is_some(),"prepare_s":prepare_s,"samples":samples,"program":program.to_string(),"validated":true})
    );
}
