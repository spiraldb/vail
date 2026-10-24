//! Public DataFusion-only reproducer; no Vail expression lowering or private rules.
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::{SessionContext, col, lit};
use std::sync::Arc;

#[tokio::test]
async fn same_name_struct_schema_replacement_preserves_new_field() {
    let mut failures = Vec::new();
    for nested in [false, true] {
        for mode in ["replace", "extend", "distinct", "same_type"] {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![0, 1]))],
            )
            .unwrap();
            let frame = SessionContext::new().read_batch(batch).unwrap();
            let structure = |name: &str, value| {
                datafusion_functions::core::named_struct().call(vec![lit(name.to_owned()), value])
            };
            let get = |value, name: &str| {
                datafusion_functions::core::get_field().call(vec![value, lit(name.to_owned())])
            };
            let selected = if mode == "same_type" {
                "original"
            } else {
                "added"
            };
            let frame = if nested {
                let frame = frame
                    .select(vec![
                        col("idx"),
                        structure("record", structure("original", col("idx"))).alias("row"),
                    ])
                    .unwrap();
                let old = get(get(col("row"), "record"), "original");
                let replacement = if mode == "extend" {
                    datafusion_functions::core::named_struct().call(vec![
                        lit("original"),
                        old,
                        lit("added"),
                        col("idx") + lit(1),
                    ])
                } else {
                    structure(selected, col("idx") + lit(1))
                };
                let target = if mode == "distinct" { "fresh" } else { "row" };
                frame
                    .select(vec![structure("record", replacement).alias(target)])
                    .unwrap()
                    .select(vec![
                        get(get(col(target), "record"), selected).alias("value"),
                    ])
                    .unwrap()
            } else {
                let frame = frame
                    .select(vec![
                        col("idx"),
                        structure("original", col("idx")).alias("record"),
                    ])
                    .unwrap();
                let replacement = if mode == "extend" {
                    datafusion_functions::core::named_struct().call(vec![
                        lit("original"),
                        get(col("record"), "original"),
                        lit("added"),
                        col("idx") + lit(1),
                    ])
                } else {
                    structure(selected, col("idx") + lit(1))
                };
                let target = if mode == "distinct" {
                    "fresh"
                } else {
                    "record"
                };
                frame
                    .select(vec![replacement.alias(target)])
                    .unwrap()
                    .select(vec![get(col(target), selected).alias("value")])
                    .unwrap()
            };
            let batches = match frame.collect().await {
                Ok(batches) => batches,
                Err(error) => {
                    eprintln!("nested={nested} mode={mode}: ERROR {error}");
                    failures.push(format!("nested={nested} mode={mode}: {error}"));
                    continue;
                }
            };
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.schema().fields().len() == 1
                        && batch.schema().field(0).name() == "value"
                        && batch.schema().field(0).data_type() == &DataType::Int32)
            );
            let values: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    assert_eq!(values.null_count(), 0);
                    values.values().iter().copied().collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(values, vec![1, 2], "nested={nested} mode={mode}");
            eprintln!("nested={nested} mode={mode}: values={values:?}");
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn struct_leaf_values_survive_filter_sort_and_limit() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3]))],
    )
    .unwrap();
    let frame = SessionContext::new()
        .read_batch(batch)
        .unwrap()
        .select(vec![
            col("idx"),
            datafusion_functions::core::named_struct()
                .call(vec![lit("value"), col("idx") + lit(10)])
                .alias("record"),
        ])
        .unwrap()
        .filter(col("idx").gt(lit(0)))
        .unwrap()
        .sort(vec![col("idx").sort(false, false)])
        .unwrap()
        .limit(0, Some(2))
        .unwrap()
        .select(vec![
            datafusion_functions::core::get_field()
                .call(vec![col("record"), lit("value")])
                .alias("value"),
        ])
        .unwrap();
    let batches = frame.collect().await.unwrap();
    for batch in &batches {
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(batch.schema().field(0).name(), "value");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(batch.column(0).null_count(), 0);
    }
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![13, 12]);
}

#[tokio::test]
async fn renamed_struct_projection_keeps_selected_field_values() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![0, 1, 2]))],
    )
    .unwrap();
    let frame = SessionContext::new()
        .read_batch(batch)
        .unwrap()
        .select(vec![
            datafusion_functions::core::named_struct()
                .call(vec![lit("value"), col("idx") + lit(5)])
                .alias("original"),
        ])
        .unwrap()
        .select(vec![col("original").alias("renamed")])
        .unwrap()
        .select(vec![
            datafusion_functions::core::get_field()
                .call(vec![col("renamed"), lit("value")])
                .alias("value"),
        ])
        .unwrap();
    let batches = frame.collect().await.unwrap();
    for batch in &batches {
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(batch.schema().field(0).name(), "value");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(batch.column(0).null_count(), 0);
    }
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![5, 6, 7]);
}

/// Public, passing resource controls. Environment sizes affect only this test
/// driver; production optimizer code has no workload-name/dimension dispatch.
#[tokio::test]
async fn public_valid_projection_resource_control() {
    use std::time::Instant;
    let width = std::env::var("VAIL_DF_WIDTH")
        .ok()
        .map(|x| x.parse().unwrap())
        .unwrap_or(16usize);
    let rows = std::env::var("VAIL_DF_ROWS")
        .ok()
        .map(|x| x.parse().unwrap())
        .unwrap_or(32i32);
    let iterations = std::env::var("VAIL_DF_ITERATIONS")
        .ok()
        .map(|x| x.parse().unwrap())
        .unwrap_or(1usize);
    let mode = std::env::var("VAIL_DF_MODE").unwrap_or_else(|_| "rename".into());
    let phase = std::env::var("VAIL_DF_PHASE").unwrap_or_else(|_| "execute".into());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from_iter_values(0..rows))],
    )
    .unwrap();
    let context = SessionContext::new();
    let frame = context.read_batch(batch).unwrap();
    let structure = |shift: i32| {
        let mut args = Vec::with_capacity(width * 2);
        for index in 0..width {
            args.push(lit(format!("f{index}")));
            args.push(col("idx") + lit(index as i32 + shift));
        }
        datafusion_functions::core::named_struct().call(args)
    };
    let frame = frame
        .select(vec![col("idx"), structure(0).alias("original")])
        .unwrap();
    let (frame, base, shift) = match mode.as_str() {
        "rename" => (
            frame
                .select(vec![col("original").alias("renamed")])
                .unwrap(),
            "renamed",
            0i64,
        ),
        "wide" => (
            frame.select(vec![structure(1).alias("fresh")]).unwrap(),
            "fresh",
            1i64,
        ),
        _ => panic!("unknown public control mode"),
    };
    let frame = frame
        .select(vec![
            datafusion_functions::core::get_field()
                .call(vec![col(base), lit(format!("f{}", width - 1))])
                .alias("value"),
        ])
        .unwrap();
    let expected =
        i64::from(rows) * i64::from(rows - 1) / 2 + i64::from(rows) * (width as i64 - 1 + shift);
    let validate = |batches: Vec<RecordBatch>| {
        let mut sum = 0i64;
        let mut count = 0usize;
        for batch in batches {
            assert_eq!(batch.schema().fields().len(), 1);
            assert_eq!(batch.schema().field(0).name(), "value");
            assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(values.null_count(), 0);
            count += values.len();
            sum += values
                .values()
                .iter()
                .map(|value| i64::from(*value))
                .sum::<i64>();
        }
        assert_eq!(count, rows as usize);
        assert_eq!(sum, expected);
    };
    validate(frame.clone().collect().await.unwrap());
    let started = Instant::now();
    for _ in 0..iterations {
        if phase == "plan" {
            let plan = frame.clone().into_optimized_plan().unwrap();
            assert_eq!(plan.schema().fields().len(), 1);
            assert_eq!(plan.schema().field(0).name(), "value");
            assert_eq!(plan.schema().field(0).data_type(), &DataType::Int32);
        } else {
            validate(frame.clone().collect().await.unwrap());
        }
    }
    eprintln!(
        "PUBLIC_DF_RESOURCE mode={mode} phase={phase} width={width} rows={rows} iterations={iterations} wall_seconds={:.9}",
        started.elapsed().as_secs_f64()
    );
}
