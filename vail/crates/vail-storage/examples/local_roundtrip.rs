//! Run in separate processes:
//! `cargo run -p vail-storage --example local_roundtrip -- write /tmp/vail-demo`
//! `cargo run -p vail-storage --example local_roundtrip -- read <printed-metadata-URI>`

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::table::StaticTable;
use iceberg::{
    Catalog, CatalogBuilder, MemoryCatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
};
use serde_json::json;
use vail_storage::{VailSemantics, read_current, write_initial_table};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    match args.as_slice() {
        [_, mode, base] if mode == "write" => {
            let base = std::fs::canonicalize(base)?;
            let warehouse = format!("file://{}", base.join("warehouse").display());
            let catalog = MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load("local", HashMap::from([("warehouse".into(), warehouse)]))
                .await?;
            let namespace = NamespaceIdent::new("demo".into());
            catalog.create_namespace(&namespace, HashMap::new()).await?;
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema)?;
            let table = catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("rows".into())
                        .location(format!("file://{}", base.join("table").display()))
                        .schema(iceberg_schema)
                        .build(),
                )
                .await?;
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])?;
            let semantics = VailSemantics {
                row_type: json!({"x":"int32"}),
                global_type: json!({"label":"str"}),
                globals: json!({"label":"example"}),
                key_field_ids: vec![1],
                comparator_version: "hail-v1".into(),
                reference_genomes: json!({}),
            };
            let committed = write_initial_table(&catalog, &table, vec![batch], semantics).await?;
            println!(
                "{}",
                committed
                    .metadata_location()
                    .expect("catalog returned a metadata location")
            );
        }
        [_, mode, metadata_uri] if mode == "read" => {
            let table = StaticTable::from_metadata_file(
                metadata_uri,
                TableIdent::new(NamespaceIdent::new("demo".into()), "rows".into()),
                FileIO::new_with_fs(),
            )
            .await?
            .into_table();
            let (batches, descriptor) = read_current(&table).await?;
            let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            println!("rows={rows} globals={}", descriptor.semantics.globals);
        }
        _ => {
            return Err("usage: local_roundtrip write <existing-dir> | read <metadata-uri>".into());
        }
    }
    Ok(())
}
