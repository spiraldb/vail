use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use vail_table::TableIR;

#[tokio::test]
async fn csv_text_read_is_lazy_partitioned_and_streams_to_single_file() {
    let root = tempfile::tempdir().unwrap();
    let first = root.path().join("first.csv");
    let second = root.path().join("second.csv");
    let output = root.path().join("out.tsv");
    std::fs::write(&first, "id,name\n1,alpha\n").unwrap();
    std::fs::write(&second, "id,name\n2,beta\n").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let table = TableIR::read_text(
        schema,
        vec![
            first.to_string_lossy().into_owned(),
            second.to_string_lossy().into_owned(),
        ],
        "csv",
        &serde_json::json!({"has_header":true}),
    )
    .unwrap();
    assert_eq!(table.known_count(), None);
    assert_eq!(table.known_n_partitions(), Some(2));
    let sampled = table.sample(1).await.unwrap();
    assert_eq!(
        sampled.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert_eq!(sampled[0].num_columns(), 2);
    assert_eq!(table.count().await.unwrap(), 2);
    table
        .export_text(output.to_str().unwrap(), "text", &serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(output).unwrap(),
        "id\tname\n1\talpha\n2\tbeta\n"
    );
}

#[tokio::test]
async fn line_read_adds_source_path_as_file_column() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("raw.txt");
    std::fs::write(&path, "first\n\nlast").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("file", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let table = TableIR::read_text(
        schema,
        vec![path.to_string_lossy().into_owned()],
        "lines",
        &serde_json::json!({}),
    )
    .unwrap();
    let partitions = table.collect_partitions().await.unwrap();
    let rows = partitions.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 3);
    let file = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(file.value(0), format!("file:{}", path.display()));
}
