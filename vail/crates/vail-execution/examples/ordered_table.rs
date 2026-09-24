//! Native counterpart to `examples/table.py` for the supported integer slice.

use std::error::Error;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vail_execution::{OrderKey, OrderRequirement, OrderedSession};

fn batch(schema: &SchemaRef, values: Vec<Option<i32>>) -> RecordBatch {
    RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int32Array::from(values))]).unwrap()
}

fn values(batches: &[RecordBatch]) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..array.len()).map(move |row| (!array.is_null(row)).then(|| array.value(row)))
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "position",
        DataType::Int32,
        true,
    )]));
    let partitions = vec![
        vec![batch(&schema, vec![Some(30), None])],
        vec![batch(&schema, vec![Some(10)])],
        vec![batch(&schema, vec![Some(20)])],
    ];
    let session = OrderedSession::default();
    let ascending = OrderRequirement::new(vec![OrderKey::new("position", false, true)])?;
    let descending = OrderRequirement::new(vec![OrderKey::new("position", true, true)])?;

    let first_three = session
        .collect(&partitions, Arc::clone(&schema), &ascending, Some(3))
        .await?;
    let reversed = session
        .collect(&partitions, schema, &descending, None)
        .await?;
    println!("keyed take(3): {:?}", values(&first_three));
    println!("descending collect: {:?}", values(&reversed));
    Ok(())
}
