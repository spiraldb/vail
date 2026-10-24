//! Untimed fixture preparation using public configurable native writer API.
use arrow_array::{Array, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use vail_storage::local_table::WriteOptions;
use vail_table::TableIR;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).expect("new native table directory");
    let rows = 500_000usize;
    let cols = 1000usize;
    let values = Int32Array::from_iter((0..rows * cols).map(|i| {
        let row = i / cols;
        let col = i % cols;
        (col % 13 != 0).then_some(((row + col) % 101) as i32)
    }));
    let entries = StructArray::new(
        vec![Arc::new(Field::new("DP", DataType::Int32, true))].into(),
        vec![Arc::new(values)],
        None,
    );
    let items = ListArray::new(
        Arc::new(Field::new("item", entries.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(
            (0..=rows).map(|r| (r * cols) as i32).collect::<Vec<_>>(),
        )),
        Arc::new(entries),
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        items.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(items)])?;
    TableIR::parallelize(batch, 1)?
        .write_with_options(
            &path,
            false,
            WriteOptions {
                max_buffer_bytes: usize::MAX,
                max_rows_per_file: rows,
            },
        )
        .await?;
    let read = TableIR::read(&path).await?;
    println!(
        "{}",
        serde_json::json!({"path":path,"rows":rows,"cols":cols,"source_partitions":read.known_n_partitions()})
    );
    Ok(())
}
