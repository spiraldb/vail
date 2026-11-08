//! Compare existing lexical binding with recomputation/current local CSE.
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{Field, Schema};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use vail_execution::expressions::{Expr, lower};
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let case = args.get(1).map(String::as_str).unwrap_or("cheap");
    let bound = args.get(2).map(String::as_str) == Some("bound");
    let n: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(65536);
    let text = "AbC012xyz".repeat(16);
    let values: ArrayRef = if case == "lower" {
        Arc::new(StringArray::from_iter(
            (0..n).map(|i| (i % 31 != 30).then_some(text.as_str())),
        ))
    } else {
        Arc::new(Int32Array::from_iter(
            (0..n).map(|i| (i % 31 != 30).then_some((i % 101) as i32)),
        ))
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "x",
            values.data_type().clone(),
            true,
        )])),
        vec![values],
    )
    .unwrap();
    let column = json!({"op":"column","name":"x"});
    let argument = match case {
        "cheap" => column,
        "arithmetic" => {
            json!({"op":"binary","operator":"add","left":{"op":"binary","operator":"mul","left":column,"right":{"op":"literal","dtype":"int32","value":3}},"right":{"op":"literal","dtype":"int32","value":1}})
        }
        "lower" => json!({"op":"string_function","function":"lower","args":[column]}),
        _ => panic!("unknown case"),
    };
    let selected = if bound {
        json!({"op":"lambda_variable","name":"v"})
    } else {
        argument.clone()
    };
    let record = |value: Value| json!({"op":"struct","fields":[["a",value.clone()],["b",value.clone()],["c",value]]});
    let expression = if bound {
        json!({"op":"switch","value":argument,"parameter":"v","cases":[],"default":record(selected),"missing":null})
    } else {
        record(selected)
    };
    let expression: Expr = serde_json::from_value(expression).unwrap();
    let start = Instant::now();
    let program = lower(&expression, batch.schema_ref()).unwrap();
    let preparation = start.elapsed().as_secs_f64();
    let output = program.evaluate(&batch).unwrap().into_array(n).unwrap();
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    let expected: ArrayRef = if case == "lower" {
        Arc::new(StringArray::from_iter(
            (0..n).map(|i| (i % 31 != 30).then(|| text.to_ascii_lowercase())),
        ))
    } else {
        Arc::new(Int32Array::from_iter((0..n).map(|i| {
            (i % 31 != 30).then_some(if case == "arithmetic" {
                (i % 101) as i32 * 3 + 1
            } else {
                (i % 101) as i32
            })
        })))
    };
    for column in output.columns() {
        assert_eq!(column.as_ref(), expected.as_ref());
    }
    let mut samples = vec![];
    for _ in 0..5 {
        let start = Instant::now();
        let mut evaluations = 0;
        while start.elapsed() < Duration::from_millis(20) {
            std::hint::black_box(program.evaluate(&batch).unwrap());
            evaluations += 1;
        }
        samples.push(start.elapsed().as_secs_f64() / evaluations as f64);
    }
    println!(
        "{}",
        json!({"case":case,"bound":bound,"rows":n,"validated":true,"prepare_seconds":preparation,"evaluation_wall_seconds":samples})
    );
}
