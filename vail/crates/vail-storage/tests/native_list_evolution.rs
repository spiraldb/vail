//! Public Iceberg/Vortex reader-writer list schema-evolution regressions.
use arrow_array::{Array, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::Runtime;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataFile, DataFileFormat, NestedField, PrimitiveType, Schema, SchemaRef, Type,
};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, VortexWriterBuilder};
use std::sync::Arc;
use tempfile::TempDir;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

fn test_scan_task(path: &str, schema: SchemaRef, size: u64) -> FileScanTask {
    FileScanTask::builder()
        .with_file_size_in_bytes(size)
        .with_start(0)
        .with_length(size)
        .with_data_file_path(path.to_owned())
        .with_data_file_format(DataFileFormat::Vortex)
        .with_schema(schema.clone())
        .with_project_field_ids(schema.as_struct().fields().iter().map(|f| f.id).collect())
        .with_case_sensitive(false)
        .with_predicate(None)
        .with_deletes(vec![])
        .build()
        .unwrap()
}
async fn scan_batches(io: FileIO, task: FileScanTask) -> Vec<RecordBatch> {
    ArrowReaderBuilder::new(io, Runtime::current())
        .with_batch_size(2)
        .build()
        .read(Box::pin(futures::stream::iter([Ok(task)])))
        .unwrap()
        .stream()
        .try_collect()
        .await
        .unwrap()
}
async fn write_batch_file(
    path: &str,
    io: &FileIO,
    schema: SchemaRef,
    batch: &RecordBatch,
) -> DataFile {
    let mut writer = VortexWriterBuilder::new(schema, VortexSession::default())
        .build(io.new_output(path).unwrap())
        .await
        .unwrap();
    writer.write(batch).await.unwrap();
    writer
        .close()
        .await
        .unwrap()
        .pop()
        .unwrap()
        .partition_spec_id(0)
        .build()
        .unwrap()
}

#[tokio::test]
async fn test_vortex_list_evolution_preserves_sliced_parent_and_child_nulls() {
    use arrow_array::{ArrayRef, Int32Array, LargeListArray, ListArray, StringViewArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    for large in [false, true] {
        for view in [false, true] {
            let tmp = TempDir::new().unwrap();
            let path = tmp
                .path()
                .join("evolved-list.vortex")
                .to_str()
                .unwrap()
                .to_string();
            let io = FileIO::new_with_fs();
            let source = Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(
                            1,
                            "items",
                            Type::List(iceberg::spec::ListType::new(Arc::new(
                                NestedField::optional(
                                    2,
                                    "element",
                                    Type::Struct(iceberg::spec::StructType::new(vec![
                                        NestedField::optional(
                                            3,
                                            "value",
                                            Type::Primitive(PrimitiveType::Int),
                                        )
                                        .into(),
                                        NestedField::optional(
                                            4,
                                            "label",
                                            Type::Primitive(PrimitiveType::String),
                                        )
                                        .into(),
                                    ])),
                                ),
                            ))),
                        )
                        .into(),
                    ])
                    .build()
                    .unwrap(),
            );
            let target = Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::optional(
                            1,
                            "renamed_items",
                            Type::List(iceberg::spec::ListType::new(Arc::new(
                                NestedField::optional(
                                    2,
                                    "element",
                                    Type::Struct(iceberg::spec::StructType::new(vec![
                                        NestedField::optional(
                                            5,
                                            "added",
                                            Type::Primitive(PrimitiveType::Long),
                                        )
                                        .into(),
                                        NestedField::optional(
                                            4,
                                            "renamed_label",
                                            Type::Primitive(PrimitiveType::String),
                                        )
                                        .into(),
                                        NestedField::optional(
                                            3,
                                            "renamed_value",
                                            Type::Primitive(PrimitiveType::Long),
                                        )
                                        .into(),
                                    ])),
                                ),
                            ))),
                        )
                        .into(),
                    ])
                    .build()
                    .unwrap(),
            );
            let source_arrow: ArrowSchema = source.as_ref().try_into().unwrap();
            let DataType::List(element) = source_arrow.field(0).data_type() else {
                panic!("list")
            };
            let DataType::Struct(fields) = element.data_type() else {
                panic!("struct")
            };
            let mut fields = fields.to_vec();
            if view {
                fields[1] = Arc::new(
                    fields[1]
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Utf8View),
                );
            }
            let labels = vec![
                Some("discard"),
                Some("seven"),
                Some("masked"),
                Some("hidden"),
                None,
                Some("nine"),
                Some("discard"),
            ];
            let labels: ArrayRef = if view {
                Arc::new(StringViewArray::from(labels))
            } else {
                Arc::new(StringArray::from(labels))
            };
            let values: ArrayRef = Arc::new(StructArray::new(
                fields.into(),
                vec![
                    Arc::new(Int32Array::from(vec![
                        Some(88),
                        Some(7),
                        Some(22),
                        Some(99),
                        None,
                        Some(9),
                        Some(77),
                    ])),
                    labels,
                ],
                Some(NullBuffer::from(vec![
                    true, true, false, true, true, true, true,
                ])),
            ));
            let element = Arc::new(
                element
                    .as_ref()
                    .clone()
                    .with_data_type(values.data_type().clone()),
            );
            let nulls = Some(NullBuffer::from(vec![true, true, false, true, true, true]));
            let input: ArrayRef = if large {
                Arc::new(
                    LargeListArray::new(
                        element,
                        OffsetBuffer::new(vec![0i64, 1, 3, 4, 4, 6, 7].into()),
                        values,
                        nulls,
                    )
                    .slice(1, 4),
                )
            } else {
                Arc::new(
                    ListArray::new(
                        element,
                        OffsetBuffer::new(vec![0i32, 1, 3, 4, 4, 6, 7].into()),
                        values,
                        nulls,
                    )
                    .slice(1, 4),
                )
            };
            let batch = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    source_arrow
                        .field(0)
                        .clone()
                        .with_data_type(input.data_type().clone()),
                ])),
                vec![input],
            )
            .unwrap();
            let mut writer = VortexWriterBuilder::new(source, VortexSession::default())
                .build(io.new_output(&path).unwrap())
                .await
                .unwrap();
            writer.write(&batch.slice(0, 2)).await.unwrap();
            writer.write(&batch.slice(2, 2)).await.unwrap();
            let file = writer
                .close()
                .await
                .unwrap()
                .pop()
                .unwrap()
                .partition_spec_id(0)
                .build()
                .unwrap();
            let batches =
                scan_batches(io, test_scan_task(&path, target, file.file_size_in_bytes())).await;
            let mut actual = Vec::new();
            for batch in batches {
                let field = batch.schema().field(0).clone();
                assert_eq!(field.name(), "renamed_items");
                assert_eq!(
                    field
                        .metadata()
                        .get(PARQUET_FIELD_ID_META_KEY)
                        .map(String::as_str),
                    Some("1")
                );
                let lists = batch.column(0);
                for row in 0..batch.num_rows() {
                    if lists.is_null(row) {
                        actual.push(None);
                        continue;
                    }
                    let child = match lists.data_type() {
                        DataType::List(_) => lists
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap()
                            .value(row),
                        DataType::LargeList(_) => lists
                            .as_any()
                            .downcast_ref::<LargeListArray>()
                            .unwrap()
                            .value(row),
                        _ => panic!("list output"),
                    };
                    let child = child.as_any().downcast_ref::<StructArray>().unwrap();
                    assert_eq!(
                        child
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>(),
                        vec!["added", "renamed_label", "renamed_value"]
                    );
                    assert_eq!(
                        child
                            .fields()
                            .iter()
                            .map(|f| f
                                .metadata()
                                .get(PARQUET_FIELD_ID_META_KEY)
                                .unwrap()
                                .as_str())
                            .collect::<Vec<_>>(),
                        vec!["5", "4", "3"]
                    );
                    assert_eq!(child.column(0).data_type(), &DataType::Int64);
                    assert_eq!(child.column(2).data_type(), &DataType::Int64);
                    assert!(matches!(
                        child.column(1).data_type(),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ));
                    let labels = arrow_cast::cast(child.column(1), &DataType::Utf8).unwrap();
                    let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
                    let values = child
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let mut items = Vec::new();
                    for i in 0..child.len() {
                        if child.is_null(i) {
                            items.push(None);
                            continue;
                        }
                        assert!(child.column(0).is_null(i));
                        items.push(Some((
                            (!values.is_null(i)).then(|| values.value(i)),
                            (!labels.is_null(i)).then(|| labels.value(i).to_owned()),
                        )));
                    }
                    actual.push(Some(items));
                }
            }
            assert_eq!(
                actual,
                vec![
                    Some(vec![Some((Some(7), Some("seven".into()))), None]),
                    None,
                    Some(vec![]),
                    Some(vec![
                        Some((None, None)),
                        Some((Some(9), Some("nine".into())))
                    ])
                ]
            );
        }
    }
}

#[tokio::test]
async fn test_vortex_list_dictionary_and_null_child_normalization() {
    use arrow_array::types::Int8Type;
    use arrow_array::{ArrayRef, DictionaryArray, Int8Array, ListArray, NullArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    for dictionary in [false, true] {
        let tmp = TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("logical-null-list.vortex")
            .to_str()
            .unwrap()
            .to_string();
        let io = FileIO::new_with_fs();
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "items",
                        Type::List(iceberg::spec::ListType::new(Arc::new(
                            NestedField::optional(
                                2,
                                "element",
                                Type::Primitive(PrimitiveType::String),
                            ),
                        ))),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let child: ArrayRef = if dictionary {
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![0, 1, 0, 0]),
                    Arc::new(StringArray::from(vec![Some("x"), None])),
                )
                .unwrap(),
            )
        } else {
            Arc::new(NullArray::new(4))
        };
        let field = Arc::new(Field::new("element", child.data_type().clone(), true));
        let input = ListArray::new(
            field.clone(),
            OffsetBuffer::new(vec![0i32, 3, 4, 4].into()),
            child,
            Some(NullBuffer::from(vec![true, false, true])),
        );
        // Public safe ArrayData accepts nonnullable dictionary keys whose
        // values include logical nulls; typed ListArray::try_new rejects it.
        // Writer evolution must preserve the old normalization behavior.
        let input = if dictionary {
            ListArray::from(
                input
                    .to_data()
                    .into_builder()
                    .data_type(DataType::List(Arc::new(
                        field.as_ref().clone().with_nullable(false),
                    )))
                    .build()
                    .unwrap(),
            )
        } else {
            input
        };
        let source_arrow: ArrowSchema = schema.as_ref().try_into().unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                source_arrow
                    .field(0)
                    .clone()
                    .with_data_type(input.data_type().clone()),
            ])),
            vec![Arc::new(input)],
        )
        .unwrap();
        let file = write_batch_file(&path, &io, schema.clone(), &batch).await;
        let batches =
            scan_batches(io, test_scan_task(&path, schema, file.file_size_in_bytes())).await;
        let mut actual = Vec::new();
        for batch in batches {
            let lists = batch.column(0);
            for row in 0..batch.num_rows() {
                if lists.is_null(row) {
                    actual.push(None);
                    continue;
                }
                let child = match lists.data_type() {
                    DataType::List(_) => lists
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .value(row),
                    DataType::LargeList(_) => lists
                        .as_any()
                        .downcast_ref::<arrow_array::LargeListArray>()
                        .unwrap()
                        .value(row),
                    _ => panic!("list output"),
                };
                let child = arrow_cast::cast(&child, &DataType::Utf8).unwrap();
                let child = child.as_any().downcast_ref::<StringArray>().unwrap();
                actual.push(Some(
                    child
                        .iter()
                        .map(|v| v.map(str::to_owned))
                        .collect::<Vec<_>>(),
                ));
            }
        }
        let expected = if dictionary {
            vec![Some("x".to_owned()), None, Some("x".to_owned())]
        } else {
            vec![None, None, None]
        };
        assert_eq!(actual, vec![Some(expected), None, Some(vec![])]);
    }
}
