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
