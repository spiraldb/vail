use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::table::StaticTable;
use iceberg::{
    Catalog, CatalogBuilder, MemoryCatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
};
use serde_json::json;
use vail_execution::{OrderKey, OrderRequirement, ordered_collect};
use vail_storage::fragment_index::{
    Endpoint, KeyField, KeyType, KeyValue, PRIMITIVE_COMPARATOR_V1, QueryInterval,
};
use vail_storage::{
    StorageError, VailSemantics, read_current, read_snapshot_range, write_initial_ordered_files,
    write_initial_table,
};

#[tokio::test]
async fn ordered_snapshot_reads_three_real_vortex_files_in_key_order() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().join("table").display());
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("local", HashMap::from([("warehouse".into(), warehouse)]))
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".into())
                .location(location)
                .schema(arrow_schema_to_schema_auto_assign_ids(&schema).unwrap())
                .build(),
        )
        .await
        .unwrap();
    let batch = |values: Vec<i32>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap()
    };
    // Writer/manifest input order intentionally differs from key order.
    let files = vec![
        vec![batch(vec![5, 6])],
        vec![batch(vec![1, 2]), batch(vec![2])],
        vec![batch(vec![2, 3, 4, 5])],
    ];
    let semantics = VailSemantics {
        row_type: json!({"x":"int32"}),
        global_type: json!({}),
        globals: json!({}),
        key_field_ids: vec![1],
        comparator_version: PRIMITIVE_COMPARATOR_V1.into(),
        reference_genomes: json!({}),
    };
    let key_fields = vec![KeyField {
        field_id: 1,
        data_type: KeyType::Int32,
        descending: false,
        nulls_last: true,
    }];
    let committed = write_initial_ordered_files(&catalog, &table, files, semantics, key_fields)
        .await
        .unwrap();
    let reopened = StaticTable::from_metadata_file(
        committed.metadata_location().unwrap(),
        TableIdent::new(namespace, "rows".into()),
        FileIO::new_with_fs(),
    )
    .await
    .unwrap()
    .into_table();
    let snapshot_id = reopened.metadata().current_snapshot_id().unwrap();
    let tasks = reopened
        .scan()
        .snapshot_id(snapshot_id)
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        tasks.len(),
        3,
        "Iceberg snapshot must contain three live files"
    );
    assert!(
        tasks
            .iter()
            .all(|task| task.data_file_format() == iceberg::spec::DataFileFormat::Vortex)
    );
    let values = |batches: Vec<RecordBatch>| {
        batches
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..array.len()).map(move |row| array.value(row))
            })
            .collect::<Vec<_>>()
    };
    let (full, descriptor) = read_current(&reopened).await.unwrap();
    assert_eq!(descriptor.files.len(), 3);
    assert!(descriptor.index_path.is_some());
    assert_eq!(values(full), vec![1, 2, 2, 2, 3, 4, 5, 5, 6]);
    let query = QueryInterval {
        lower: Some(Endpoint {
            key: vec![KeyValue::Int32(2)],
            inclusive: true,
        }),
        upper: Some(Endpoint {
            key: vec![KeyValue::Int32(5)],
            inclusive: false,
        }),
    };
    let (range, _) = read_snapshot_range(&reopened, snapshot_id, &query)
        .await
        .unwrap();
    assert_eq!(values(range), vec![2, 2, 2, 3, 4]);

    let index_path = descriptor.index_path.unwrap();
    std::fs::write(index_path.strip_prefix("file://").unwrap(), b"corrupt").unwrap();
    assert!(matches!(
        read_current(&reopened).await,
        Err(StorageError::Invalid(_))
    ));
}

#[tokio::test]
async fn pr_iceberg_reader_preserves_single_file_multisplit_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().join("table").display());
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("local", HashMap::from([("warehouse".into(), warehouse)]))
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".into())
                .location(location)
                .schema(iceberg_schema)
                .build(),
        )
        .await
        .unwrap();
    let batches = (0..1025)
        .step_by(73)
        .map(|start| {
            let end = (start + 73).min(1025);
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from_iter_values(
                    (start..end).map(|i| 1024 - i),
                ))],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let semantics = VailSemantics {
        row_type: json!({"x":"int32"}),
        global_type: json!({}),
        globals: json!({}),
        key_field_ids: vec![],
        comparator_version: "arrow-int32-ascending-v1".into(),
        reference_genomes: json!({}),
    };
    let committed = write_initial_table(&catalog, &table, batches, semantics)
        .await
        .unwrap();
    let metadata_uri = committed.metadata_location().unwrap();
    let reopened = StaticTable::from_metadata_file(
        metadata_uri,
        TableIdent::new(namespace, "rows".into()),
        FileIO::new_with_fs(),
    )
    .await
    .unwrap()
    .into_table();
    let scan = reopened
        .scan()
        .snapshot_id(reopened.metadata().current_snapshot_id().unwrap())
        .with_batch_size(Some(128))
        .with_data_file_concurrency_limit(1)
        .build()
        .unwrap();
    let tasks = scan
        .plan_files()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0].data_file_format(),
        iceberg::spec::DataFileFormat::Vortex
    );
    assert_eq!(tasks[0].record_count(), Some(1025));
    assert!(tasks[0].file_size_in_bytes() > 0);
    let direct = scan
        .to_arrow()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(
        direct.len() > 1,
        "batch size should split the PR Vortex reader output"
    );
    let values = direct
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..array.len()).map(move |i| array.value(i))
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (0..1025).rev().collect::<Vec<_>>());
}

#[tokio::test]
async fn empty_snapshot_preserves_globals_without_data_files() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().join("table").display());
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("local", HashMap::from([("warehouse".into(), warehouse)]))
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".into())
                .location(location)
                .schema(iceberg_schema)
                .build(),
        )
        .await
        .unwrap();
    let semantics = VailSemantics {
        row_type: json!({"x":"int32"}),
        global_type: json!({"label":"str"}),
        globals: json!({"label":"empty"}),
        key_field_ids: vec![1],
        comparator_version: "hail-v1".into(),
        reference_genomes: json!({}),
    };
    let committed = write_initial_table(&catalog, &table, vec![], semantics.clone())
        .await
        .unwrap();
    let metadata_uri = committed.metadata_location().unwrap();
    let reopened = StaticTable::from_metadata_file(
        metadata_uri,
        TableIdent::new(namespace, "rows".into()),
        FileIO::new_with_fs(),
    )
    .await
    .unwrap()
    .into_table();
    let (batches, descriptor) = read_current(&reopened).await.unwrap();
    assert!(batches.is_empty());
    assert!(descriptor.files.is_empty());
    assert_eq!(descriptor.semantics, semantics);
}

#[tokio::test]
async fn vortex_file_is_committed_and_reopened_from_iceberg_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().join("table").display());
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("local", HashMap::from([("warehouse".into(), warehouse)]))
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".into())
                .location(location)
                .schema(iceberg_schema)
                .build(),
        )
        .await
        .unwrap();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    let semantics = VailSemantics {
        row_type: json!({"x":"int32"}),
        global_type: json!({"label":"str"}),
        globals: json!({"label":"sample"}),
        key_field_ids: vec![1],
        comparator_version: "hail-v1".into(),
        reference_genomes: json!({}),
    };
    let committed = write_initial_table(&catalog, &table, vec![batch.clone()], semantics.clone())
        .await
        .unwrap();
    let metadata_location = committed.metadata_location().unwrap().to_string();
    let reopened = StaticTable::from_metadata_file(
        &metadata_location,
        TableIdent::new(namespace, "rows".into()),
        FileIO::new_with_fs(),
    )
    .await
    .unwrap()
    .into_table();
    let (read_batches, descriptor) = read_current(&reopened).await.unwrap();
    assert_eq!(
        read_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
    assert_eq!(read_batches[0].column(0).as_ref(), batch.column(0).as_ref());
    assert_eq!(descriptor.semantics, semantics);
    assert_eq!(descriptor.files.len(), 1);
    assert!(descriptor.files[0].path.ends_with(".vortex"));

    let descriptor_uri = reopened
        .metadata()
        .current_snapshot()
        .unwrap()
        .summary()
        .additional_properties
        .get("vail.descriptor.path")
        .unwrap();
    let descriptor_path = descriptor_uri.strip_prefix("file://").unwrap();
    std::fs::write(descriptor_path, b"corrupt").unwrap();
    assert!(matches!(
        read_current(&reopened).await,
        Err(StorageError::Invalid(_))
    ));
}

#[tokio::test]
async fn ordered_execution_survives_vortex_iceberg_snapshot_and_fresh_read() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().join("table").display());
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("local", HashMap::from([("warehouse".into(), warehouse)]))
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".into())
                .location(location)
                .schema(iceberg_schema)
                .build(),
        )
        .await
        .unwrap();
    let batch = |values: Vec<i32>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap()
    };
    let partitions = vec![
        vec![batch(vec![5, 1]), batch(vec![3])],
        vec![batch(vec![4, 2])],
    ];
    let order = OrderRequirement::new(vec![OrderKey::new("x", false, true)]).unwrap();
    let ordered = ordered_collect(&partitions, Arc::clone(&schema), &order, None)
        .await
        .unwrap();
    let semantics = VailSemantics {
        row_type: json!({"x":"int32"}),
        global_type: json!({"label":"str"}),
        globals: json!({"label":"combined"}),
        key_field_ids: vec![1],
        comparator_version: "arrow-int32-ascending-v1".into(),
        reference_genomes: json!({}),
    };
    let committed = write_initial_table(&catalog, &table, ordered, semantics.clone())
        .await
        .unwrap();
    let metadata_location = committed.metadata_location().unwrap().to_string();
    let reopened = StaticTable::from_metadata_file(
        &metadata_location,
        TableIdent::new(namespace, "rows".into()),
        FileIO::new_with_fs(),
    )
    .await
    .unwrap()
    .into_table();
    let (read_batches, descriptor) = read_current(&reopened).await.unwrap();
    let values = read_batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..array.len()).map(move |i| array.value(i))
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
    assert_eq!(descriptor.semantics, semantics);
    assert_eq!(
        descriptor.snapshot_id,
        reopened.metadata().current_snapshot_id().unwrap()
    );
}

#[tokio::test]
async fn stale_initial_writer_cannot_append_under_old_descriptor() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().join("table").display());
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("local", HashMap::from([("warehouse".into(), warehouse)]))
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".into())
                .location(location)
                .schema(iceberg_schema)
                .build(),
        )
        .await
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();
    let semantics = VailSemantics {
        row_type: json!({"x":"int32"}),
        global_type: json!({}),
        globals: json!({}),
        key_field_ids: vec![1],
        comparator_version: "arrow-int32-ascending-v1".into(),
        reference_genomes: json!({}),
    };
    let renamed_schema = Arc::new(Schema::new(vec![Field::new("y", DataType::Int32, false)]));
    let renamed =
        RecordBatch::try_new(renamed_schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    assert!(matches!(
        write_initial_table(&catalog, &table, vec![renamed], semantics.clone()).await,
        Err(StorageError::Invalid(_))
    ));
    let mut bad_key = semantics.clone();
    bad_key.key_field_ids = vec![1, 1];
    assert!(matches!(
        write_initial_table(&catalog, &table, vec![batch.clone()], bad_key).await,
        Err(StorageError::Invalid(_))
    ));
    let committed = write_initial_table(&catalog, &table, vec![batch.clone()], semantics.clone())
        .await
        .unwrap();
    let first_snapshot = committed.metadata().current_snapshot_id().unwrap();
    assert!(
        write_initial_table(&catalog, &table, vec![batch], semantics)
            .await
            .is_err()
    );
    let current = catalog
        .load_table(&TableIdent::new(namespace, "rows".into()))
        .await
        .unwrap();
    assert_eq!(
        current.metadata().current_snapshot_id(),
        Some(first_snapshot)
    );
    let (rows, _) = read_current(&current).await.unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn local_paths_reject_encoding_the_iceberg_fileio_cannot_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().join("table%20space").display());
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("local", HashMap::from([("warehouse".into(), warehouse)]))
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema).unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".into())
                .location(location)
                .schema(iceberg_schema)
                .build(),
        )
        .await
        .unwrap();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let semantics = VailSemantics {
        row_type: json!({"x":"int32"}),
        global_type: json!({}),
        globals: json!({}),
        key_field_ids: vec![1],
        comparator_version: "arrow-int32-ascending-v1".into(),
        reference_genomes: json!({}),
    };
    assert!(matches!(
        write_initial_table(&catalog, &table, vec![batch], semantics).await,
        Err(StorageError::Unsupported(_))
    ));
}
