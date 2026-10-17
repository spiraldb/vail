use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_table::{ScalarIR, TableIR, TableNode};

fn column(name: &str) -> ScalarIR {
    ScalarIR::Column { name: name.into() }
}
fn int(value: i32) -> ScalarIR {
    ScalarIR::Literal {
        dtype: Type::Primitive(PrimitiveType::Int32),
        value: json!(value),
    }
}
fn greater(left: ScalarIR, right: ScalarIR) -> ScalarIR {
    ScalarIR::Binary {
        operator: BinaryOperator::Gt,
        left: Arc::new(left),
        right: Arc::new(right),
    }
}

#[tokio::test]
async fn branches_share_a_typed_child_and_execute_independently() {
    let source = TableIR::range(10, 3).unwrap();
    let left = source.head(2);
    let right = source.filter(greater(column("idx"), int(4)), true).unwrap();
    let TableNode::Head { child: a, .. } = left.node() else {
        panic!("expected Head")
    };
    let TableNode::Filter { child: b, .. } = right.node() else {
        panic!("expected Filter")
    };
    assert!(Arc::ptr_eq(a, b));
    assert!(Arc::ptr_eq(a, &source));
    assert_eq!(left.count().await.unwrap(), 2);
    assert_eq!(right.count().await.unwrap(), 5);
    assert_eq!(source.count().await.unwrap(), 10);
    assert!(right.known_count().is_none());
    assert_eq!(right.known_n_partitions(), Some(3));
}

#[tokio::test]
async fn range_is_lazy_and_head_stops_batch_generation() {
    let source = TableIR::range(i32::MAX as usize, 1).unwrap();
    let batches = source.head(3).collect_partitions().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0][0].num_rows(), 3);
    assert_eq!(
        batches[0][0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[0, 1, 2]
    );
}

#[tokio::test]
async fn parallelize_preserves_requested_partitions_and_values() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let source = TableIR::parallelize(
        RecordBatch::try_new(schema, vec![values.clone()]).unwrap(),
        5,
    )
    .unwrap();
    let partitions = source.collect_partitions().await.unwrap();
    assert_eq!(partitions.len(), 5);
    assert_eq!(
        partitions
            .iter()
            .flatten()
            .flat_map(|batch| batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        partitions
            .iter()
            .map(|p| p.iter().map(RecordBatch::num_rows).sum::<usize>())
            .collect::<Vec<_>>(),
        vec![0, 1, 0, 1, 1]
    );
}

#[test]
fn constructors_typecheck_scalar_ir_without_execution() {
    let source = TableIR::range(10, 3).unwrap();
    assert!(source.filter(column("idx"), true).is_err());
    assert!(source.map_rows(vec![("idx".into(), int(7))]).is_err());
    assert!(
        source
            .filter(greater(column("absent"), int(0)), true)
            .is_err()
    );
    assert!(
        source
            .map_rows(vec![("idx".into(), column("absent"))])
            .is_err()
    );
    assert!(source.key_by(vec!["idx".into(), "idx".into()]).is_err());
}

#[tokio::test]
async fn write_streams_transformed_rows_into_rolled_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table");
    let source = TableIR::range(20, 2).unwrap();
    let table = source.filter(greater(column("idx"), int(3)), true).unwrap();
    table
        .write_with_options(
            path.to_str().unwrap(),
            false,
            vail_storage::local_table::WriteOptions {
                max_buffer_bytes: usize::MAX,
                max_rows_per_file: 3,
            },
        )
        .await
        .unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();
    assert_eq!(read.known_count(), Some(16));
    assert_eq!(read.known_n_partitions(), Some(6));
    let collected = read.collect_partitions().await.unwrap();
    let values = collected
        .iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (4..20).collect::<Vec<_>>());
}

#[test]
fn zero_column_input_partition_cardinality_does_not_overflow() {
    use arrow_array::RecordBatchOptions;
    let schema = Arc::new(Schema::empty());
    let rows = RecordBatch::try_new_with_options(
        schema.clone(),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(usize::MAX)),
    )
    .unwrap();
    let table = TableIR::parallelize_batches(schema, vec![rows], 2).unwrap();
    assert_eq!(table.known_count(), Some(usize::MAX));
    assert_eq!(
        table
            .filter_partitions(vec![0], true)
            .unwrap()
            .known_count(),
        Some(usize::MAX / 2)
    );
    assert_eq!(
        table
            .filter_partitions(vec![1], true)
            .unwrap()
            .known_count(),
        Some(usize::MAX / 2 + 1)
    );
}

#[tokio::test]
async fn multibatch_inputs_keep_order_across_empty_batches_and_partitions() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
    let batches = vec![
        RecordBatch::new_empty(schema.clone()),
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        )
        .unwrap(),
        RecordBatch::new_empty(schema.clone()),
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(3), Some(4), Some(5)]))],
        )
        .unwrap(),
    ];
    for partitions in [1, 2, 7] {
        let table =
            TableIR::parallelize_batches(schema.clone(), batches.clone(), partitions).unwrap();
        let result = table.collect_partitions().await.unwrap();
        assert_eq!(result.len(), partitions);
        let values = result
            .iter()
            .flatten()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Some(1), None, Some(3), Some(4), Some(5)]);
    }
    assert!(TableIR::parallelize_batches(Arc::new(Schema::empty()), batches, 1).is_err());
}

#[tokio::test]
async fn partition_callbacks_preserve_membership_without_implicit_sorting() {
    let source = TableIR::range(9, 3).unwrap();
    let rows = column("rows");
    let identity = source
        .map_partitions("rows".into(), rows.clone(), Default::default())
        .unwrap();
    assert_eq!(identity.keys(), &["idx"]);
    let reversed = source
        .map_partitions(
            "rows".into(),
            ScalarIR::ArrayMap {
                value: Arc::new(rows),
                parameter: "r".into(),
                body: Arc::new(ScalarIR::Struct {
                    fields: vec![(
                        "idx".into(),
                        ScalarIR::Binary {
                            operator: BinaryOperator::Sub,
                            left: Arc::new(int(8)),
                            right: Arc::new(ScalarIR::GetField {
                                value: Arc::new(ScalarIR::LambdaVariable {
                                    name: "r".into(),
                                    dtype: None,
                                }),
                                name: "idx".into(),
                            }),
                        },
                    )],
                }),
            },
            Default::default(),
        )
        .unwrap();
    assert!(reversed.keys().is_empty());
    let partitions = reversed.collect_partitions().await.unwrap();
    let values = partitions
        .iter()
        .map(|batches| {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![vec![8, 7, 6], vec![5, 4, 3], vec![2, 1, 0]]);
    let ordered = reversed.key_by(vec!["idx".into()]).unwrap();
    let values = ordered
        .collect_partitions()
        .await
        .unwrap()
        .iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (0..9).collect::<Vec<_>>());
}

#[tokio::test]
async fn keyed_partition_identity_map_filter_remain_bounded_by_head() {
    let source = TableIR::range(i32::MAX as usize, 3).unwrap();
    let mapped = source
        .map_partitions(
            "rows".into(),
            ScalarIR::ArrayFilter {
                value: Arc::new(ScalarIR::ArrayMap {
                    value: Arc::new(column("rows")),
                    parameter: "r".into(),
                    body: Arc::new(ScalarIR::LambdaVariable {
                        name: "r".into(),
                        dtype: None,
                    }),
                }),
                parameter: "r".into(),
                predicate: Arc::new(greater(
                    int(3),
                    ScalarIR::GetField {
                        value: Arc::new(ScalarIR::LambdaVariable {
                            name: "r".into(),
                            dtype: None,
                        }),
                        name: "idx".into(),
                    },
                )),
            },
            Default::default(),
        )
        .unwrap();
    assert_eq!(mapped.keys(), &["idx"]);
    assert_eq!(mapped.head(1).count().await.unwrap(), 1);
}

#[tokio::test]
async fn query_prefix_retains_duplicate_prefixes_across_original_partitions() {
    // The arrays themselves prove (a,idx) is ordered; declare that known
    // ordering while retaining the public parallelize partition boundaries.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("idx", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![0; 9])),
            Arc::new(Int32Array::from_iter_values(0..9)),
        ],
    )
    .unwrap();
    let source = TableIR::parallelize(batch, 3)
        .unwrap()
        .with_keys(vec!["a".into(), "idx".into()])
        .unwrap();
    let original = source.collect_partitions().await.unwrap();
    let counts = original
        .iter()
        .map(|batches| batches.iter().map(RecordBatch::num_rows).sum::<usize>())
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![3, 3, 3]);
    let packed = source
        .query_prefix(1, "query".into(), "payload".into())
        .unwrap();
    assert_eq!(packed.keys(), &["query"]);
    let parts = packed.collect_partitions().await.unwrap();
    assert_eq!(
        parts
            .iter()
            .map(|batches| batches.iter().map(RecordBatch::num_rows).sum::<usize>())
            .collect::<Vec<_>>(),
        counts
    );
    let values = parts
        .iter()
        .flatten()
        .flat_map(|batch| {
            let payload = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap();
            payload
                .column_by_name("idx")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (0..9).collect::<Vec<_>>());
    assert!(source.query_prefix(0, "q".into(), "p".into()).is_err());
    assert!(source.query_prefix(3, "q".into(), "p".into()).is_err());
}

#[tokio::test]
async fn naive_coalesce_preserves_rows_order_and_partition_limits() {
    use vail_table::RepartitionStrategy;

    let source = TableIR::range(30, 1).unwrap();
    for requested in [1, 16] {
        let result = source
            .repartition(requested, RepartitionStrategy::NaiveCoalesce)
            .unwrap();
        let partitions = result.collect_partitions().await.unwrap();
        assert_eq!(partitions.len(), 1);
        let values: Vec<_> = partitions.iter().flatten().flat_map(|batch| {
            batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec()
        }).collect();
        assert_eq!(values, (0..30).collect::<Vec<_>>());
    }
    assert!(source.repartition(0, RepartitionStrategy::NaiveCoalesce).is_err());

    let source = TableIR::range(30, 4).unwrap();
    let reduced = source.repartition(2, RepartitionStrategy::NaiveCoalesce).unwrap();
    let partitions = reduced.collect_partitions().await.unwrap();
    assert_eq!(partitions.len(), 2);
    let values: Vec<_> = partitions.iter().flatten().flat_map(|batch| {
        batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec()
    }).collect();
    assert_eq!(values, (0..30).collect::<Vec<_>>());
}

#[tokio::test]
async fn annotated_range_joins_preserve_rows_and_key_order() {
    use vail_table::TableJoinKind;

    let source = TableIR::range(30, 1).unwrap();
    let left = source.map_rows(vec![
        ("idx".into(), column("idx")),
        ("left_value".into(), column("idx")),
    ]).unwrap();
    let right = source.map_rows(vec![
        ("idx".into(), column("idx")),
        ("right_value".into(), column("idx")),
    ]).unwrap();
    let joined = left.join(right, TableJoinKind::Inner, 1).unwrap();
    let batches = joined.collect_partitions().await.unwrap();
    let actual: Vec<_> = batches.iter().flatten().flat_map(|batch| {
        let fields: Vec<_> = (0..3).map(|index| {
            batch.column(index).as_any().downcast_ref::<Int32Array>().unwrap()
        }).collect();
        (0..batch.num_rows()).map(|row| {
            (fields[0].value(row), fields[1].value(row), fields[2].value(row))
        }).collect::<Vec<_>>()
    }).collect();
    assert_eq!(actual, (0..30).map(|i| (i, i, i)).collect::<Vec<_>>());
}
