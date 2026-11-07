//! Prepared-expression controls; fixture creation and full validation are untimed.
//! Compare VAIL_DISABLE_LIST_FLATMAP=1 with the default using the same executable.
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{Value, json};
use std::{sync::Arc, time::Instant};
use vail_execution::expressions::{Expr, lower, lower_with_globals};

fn variable() -> Value {
    json!({"op":"lambda_variable","name":"x"})
}
fn number(value: i32) -> Value {
    json!({"op":"literal","dtype":"int32","value":value})
}
fn binary(operator: &str, left: Value, right: Value) -> Value {
    json!({"op":"binary","operator":operator,"left":left,"right":right})
}
fn map(value: Value, body: Value) -> Value {
    json!({"op":"array_map","value":value,"parameter":"x","body":body})
}
fn filter(value: Value, predicate: Value) -> Value {
    json!({"op":"array_filter","value":value,"parameter":"x","predicate":predicate})
}
fn flatmap(value: Value, elements: Vec<Value>) -> Value {
    json!({"op":"array_flatten","value":map(value,json!({"op":"array","element_type":"int32","elements":elements}))})
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let case = args.get(1).map(String::as_str).unwrap_or("flat2");
    let width = args
        .get(2)
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(1024);
    let rows = args
        .get(3)
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(128);
    let nullable = case.ends_with("-null");
    let base = case.strip_suffix("-null").unwrap_or(case);
    let source: Vec<Option<i32>> = (0..rows * width)
        .map(|i| (!nullable || i % 19 != 0).then_some((i % 64) as i32))
        .collect();
    let root_valid: Vec<bool> = (0..rows).map(|i| !nullable || i % 13 != 0).collect();
    let nulls = nullable.then(|| NullBuffer::from(root_valid.clone()));
    let mut inputs: Vec<Vec<Option<i32>>> = source.chunks(width).map(<[_]>::to_vec).collect();
    let lists = if base.starts_with("nested") {
        assert_eq!(width % 8, 0);
        let row_lengths = if base.ends_with("skew") {
            vec![0, width / 2, 1, width - width / 2 - 1]
        } else {
            vec![8; width / 8]
        };
        let lengths: Vec<usize> = (0..rows)
            .flat_map(|_| row_lengths.iter().copied())
            .collect();
        let inner_valid: Vec<bool> = (0..lengths.len())
            .map(|i| !nullable || i % 17 != 0)
            .collect();
        let mut first = 0;
        inputs = (0..rows)
            .map(|row| {
                let mut values = vec![];
                for (i, &len) in row_lengths.iter().enumerate() {
                    if inner_valid[row * row_lengths.len() + i] {
                        values.extend_from_slice(&source[first..first + len]);
                    }
                    first += len;
                }
                values
            })
            .collect();
        let inner = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::from_lengths(lengths),
            Arc::new(Int32Array::from(source)),
            nullable.then(|| NullBuffer::from(inner_valid)),
        );
        ListArray::new(
            Arc::new(Field::new_list_field(inner.data_type().clone(), true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(row_lengths.len(), rows)),
            Arc::new(inner),
            nulls,
        )
    } else {
        ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
            Arc::new(Int32Array::from(source)),
            nulls,
        )
    };
    let biases: Vec<Option<i32>> = (0..rows)
        .map(|r| (!nullable || r % 7 != 0).then_some(r as i32))
        .collect();
    let mut fields = vec![Field::new("xs", lists.data_type().clone(), true)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(lists)];
    if matches!(base, "capture" | "struct-capture" | "mixed-capture") {
        fields.push(Field::new("bias", DataType::Int32, true));
        columns.push(Arc::new(Int32Array::from(biases.clone())));
    }
    if base == "struct-capture" {
        let row = StructArray::new(
            fields.into_iter().map(Arc::new).collect::<Vec<_>>().into(),
            columns,
            None,
        );
        fields = vec![Field::new("row", row.data_type().clone(), false)];
        columns = vec![Arc::new(row)];
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let xs = if base == "struct-capture" {
        json!({"op":"get_field","value":{"op":"column","name":"row"},"name":"xs"})
    } else {
        json!({"op":"column","name":"xs"})
    };
    let expression = match base {
        "maps" => map(
            map(
                map(xs, binary("add", variable(), number(1))),
                binary("mul", variable(), number(3)),
            ),
            binary("add", variable(), number(2)),
        ),
        "chain" => map(
            filter(
                map(xs, binary("add", variable(), number(1))),
                binary("gt", variable(), number(32)),
            ),
            binary("mul", variable(), number(2)),
        ),
        "flat2" => map(
            flatmap(xs, vec![variable(), binary("add", variable(), number(1))]),
            binary("mul", variable(), number(3)),
        ),
        "filter-expand" | "filter-none" => map(
            flatmap(
                filter(
                    xs,
                    binary(
                        "lt",
                        variable(),
                        number(if base == "filter-none" { 0 } else { 4 }),
                    ),
                ),
                (0..8)
                    .map(|j| binary("add", variable(), number(j)))
                    .collect(),
            ),
            binary("mul", variable(), number(3)),
        ),
        "fanout32" => flatmap(
            xs,
            (0..32)
                .map(|j| binary("mul", binary("add", variable(), number(j)), number(3)))
                .collect(),
        ),
        "nested" => map(
            json!({"op":"array_flatten","value":map(xs,variable())}),
            binary("add", variable(), number(1)),
        ),
        "nested-work" | "nested-skew" => {
            json!({"op":"array_flatten","value":map(xs,json!({"op":"array_map","value":variable(),"parameter":"y","body":binary("add",json!({"op":"lambda_variable","name":"y"}),number(1))}))})
        }
        "nested-construct" | "nested-construct-skew" => {
            json!({"op":"array_flatten","value":map(xs,json!({"op":"array_flatten","value":{"op":"array_map","value":variable(),"parameter":"y","body":{"op":"array","element_type":"int32","elements":[binary("add",json!({"op":"lambda_variable","name":"y"}),number(1)),binary("mul",json!({"op":"lambda_variable","name":"y"}),number(2))]}}}))})
        }
        "capture" | "struct-capture" | "mixed-capture" | "global" => {
            let bias = if base == "struct-capture" {
                json!({"op":"get_field","value":{"op":"column","name":"row"},"name":"bias"})
            } else if base == "global" {
                number(0)
            } else {
                json!({"op":"column","name":"bias"})
            };
            let a = binary("add", variable(), bias.clone());
            let b = binary("add", binary("mul", variable(), number(2)), bias);
            let elements = if matches!(base, "mixed-capture" | "global") {
                vec![
                    binary("add", a, json!({"op":"global","name":"g"})),
                    binary("add", b, json!({"op":"global","name":"g"})),
                ]
            } else {
                vec![a, b]
            };
            flatmap(xs, elements)
        }
        _ => panic!("unknown case"),
    };
    let expected: Vec<Option<Vec<Option<i32>>>> = inputs
        .into_iter()
        .enumerate()
        .map(|(row, xs)| {
            root_valid[row].then(|| {
                xs.into_iter()
                    .flat_map(|x| match base {
                        "maps" => vec![x.map(|x| (x + 1) * 3 + 2)],
                        "chain" => x
                            .map(|x| x + 1)
                            .filter(|&x| x > 32)
                            .map(|x| Some(x * 2))
                            .into_iter()
                            .collect(),
                        "flat2" => vec![x.map(|x| x * 3), x.map(|x| (x + 1) * 3)],
                        "filter-expand" | "filter-none" => x
                            .filter(|&x| x < if base == "filter-none" { 0 } else { 4 })
                            .into_iter()
                            .flat_map(|x| (0..8).map(move |j| Some((x + j) * 3)))
                            .collect(),
                        "fanout32" => (0..32).map(|j| x.map(|x| (x + j) * 3)).collect(),
                        "nested" | "nested-work" | "nested-skew" => vec![x.map(|x| x + 1)],
                        "nested-construct" | "nested-construct-skew" => {
                            vec![x.map(|x| x + 1), x.map(|x| x * 2)]
                        }
                        "capture" | "struct-capture" | "mixed-capture" | "global" => {
                            let bias = if base == "global" {
                                Some(0)
                            } else {
                                biases[row]
                            };
                            let g = if matches!(base, "mixed-capture" | "global") {
                                100
                            } else {
                                0
                            };
                            vec![
                                x.zip(bias).map(|(x, b)| x + b + g),
                                x.zip(bias).map(|(x, b)| x * 2 + b + g),
                            ]
                        }
                        _ => unreachable!(),
                    })
                    .collect()
            })
        })
        .collect();
    let expression: Expr = serde_json::from_value(expression).unwrap();
    let prepare_start = Instant::now();
    let program = if matches!(base, "mixed-capture" | "global") {
        let globals = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("g", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![100]))],
        )
        .unwrap();
        lower_with_globals(&expression, batch.schema_ref(), &globals).unwrap()
    } else {
        lower(&expression, batch.schema_ref()).unwrap()
    };
    let prepare_s = prepare_start.elapsed().as_secs_f64();
    let evaluate = || program.evaluate(&batch).unwrap().into_array(rows).unwrap();
    let check = |out: &ArrayRef| {
        let lists = out.as_any().downcast_ref::<ListArray>().unwrap();
        for (row, expected) in expected.iter().enumerate() {
            assert_eq!(lists.is_valid(row), expected.is_some());
            if let Some(expected) = expected {
                let values = lists.value(row);
                let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(values.iter().collect::<Vec<_>>(), *expected);
            }
        }
    };
    for _ in 0..3 {
        check(&evaluate());
    }
    let mut samples = vec![];
    for _ in 0..7 {
        let start = Instant::now();
        let out = evaluate();
        samples.push(start.elapsed().as_secs_f64());
        check(&out);
    }
    println!(
        "{}",
        json!({"case":case,"width":width,"rows":rows,"disabled":std::env::var_os("VAIL_DISABLE_LIST_FLATMAP").is_some(),"samples":samples,"prepare_s":prepare_s,"prepared_expr":program.to_string(),"validated":true})
    );
}
