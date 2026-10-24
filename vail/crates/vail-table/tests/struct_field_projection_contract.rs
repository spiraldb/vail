//! Public DataFusion-only reproducer; no Vail expression lowering or private rules.
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::{SessionContext, col, lit};
use std::sync::Arc;

#[tokio::test]
async fn same_name_struct_schema_replacement_preserves_new_field() {
    for nested in [false, true] {
        for mode in ["replace", "extend", "distinct"] {
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
                    structure("added", col("idx") + lit(1))
                };
                let target = if mode == "distinct" { "fresh" } else { "row" };
                frame
                    .select(vec![structure("record", replacement).alias(target)])
                    .unwrap()
                    .select(vec![
                        get(get(col(target), "record"), "added").alias("value"),
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
                    structure("added", col("idx") + lit(1))
                };
                let target = if mode == "distinct" {
                    "fresh"
                } else {
                    "record"
                };
                frame
                    .select(vec![replacement.alias(target)])
                    .unwrap()
                    .select(vec![get(col(target), "added").alias("value")])
                    .unwrap()
            };
            let batches = frame.collect().await.unwrap();
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
        }
    }
}
