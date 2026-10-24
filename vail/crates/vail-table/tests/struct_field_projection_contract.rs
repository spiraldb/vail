//! Public DataFusion-only reproducer; no Vail expression lowering or private rules.
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::{SessionContext, col, lit};
use std::sync::Arc;

#[tokio::test]
async fn same_name_struct_schema_replacement_preserves_new_field() {
    for nested in [false, true] {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![0, 1]))],
        ).unwrap();
        let frame = SessionContext::new().read_batch(batch).unwrap();
        let structure = |name: &str, value| datafusion_functions::core::named_struct()
            .call(vec![lit(name.to_owned()), value]);
        let get = |value, name: &str| datafusion_functions::core::get_field()
            .call(vec![value, lit(name.to_owned())]);
        let frame = if nested {
            frame.select(vec![col("idx"), structure("record", structure("original", col("idx"))).alias("row")]).unwrap()
                .select(vec![structure("record", structure("added", col("idx") + lit(1))).alias("row")]).unwrap()
                .select(vec![get(get(col("row"), "record"), "added").alias("value")]).unwrap()
        } else {
            frame.select(vec![col("idx"), structure("original", col("idx")).alias("record")]).unwrap()
                .select(vec![structure("added", col("idx") + lit(1)).alias("record")]).unwrap()
                .select(vec![get(col("record"), "added").alias("value")]).unwrap()
        };
        let batches = frame.collect().await.unwrap();
        let values: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(values.null_count(), 0);
            values.values().iter().copied().collect::<Vec<_>>()
        }).collect();
        assert_eq!(values, vec![1, 2]);
    }
}
