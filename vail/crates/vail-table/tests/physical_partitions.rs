use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::ExecutionPlanProperties;
use futures::TryStreamExt;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{PrimitiveType, Type};
use vail_storage::local_table::{WriteOptions, open_local};
use vail_table::{RepartitionStrategy, ScalarIR, TableIR, TableRef, partitioned};

async fn values(plan: datafusion_physical_plan::SendableRecordBatchStream) -> Vec<i32> {
    let batches: Vec<RecordBatch> = plan.try_collect().await.unwrap();
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect()
}
fn keyed_source() -> TableRef {
    let schema = Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, true)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(0..12))]).unwrap();
    TableIR::parallelize(batch, 3)
        .unwrap()
        .key_by(vec!["idx".into()])
        .unwrap()
}
#[tokio::test]
async fn partitions_are_independent_datafusion_outputs_after_narrow_operations() {
    let table = TableIR::range(12, 3)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "flag".into(),
                ScalarIR::Literal {
                    dtype: Type::Primitive(PrimitiveType::Bool),
                    value: json!(true),
                },
            ),
        ])
        .unwrap();
    let prepared = partitioned::prepare(table).await.unwrap();
    let plan = prepared.physical_plan();
    assert_eq!(plan.output_partitioning().partition_count(), 3);
    assert_eq!(
        values(plan.execute(2, Arc::new(TaskContext::default())).unwrap()).await,
        vec![8, 9, 10, 11]
    );
    assert_eq!(
        values(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await,
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        values(plan.execute(1, Arc::new(TaskContext::default())).unwrap()).await,
        vec![4, 5, 6, 7]
    );
}
#[tokio::test]
async fn read_partition_runs_without_any_unrelated_data_files() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("table");
    let path = path.to_str().unwrap();
    TableIR::range(12, 1)
        .unwrap()
        .write_with_options(
            path,
            false,
            WriteOptions {
                max_buffer_bytes: 1024 * 1024,
                max_rows_per_file: 2,
            },
        )
        .await
        .unwrap();
    let read = open_local(path).await.unwrap();
    let files = &read.descriptor().files;
    assert_eq!(files.len(), 6);
    for i in [0, 1, 4, 5] {
        std::fs::remove_file(
            files[i]
                .path
                .strip_prefix("file://")
                .unwrap_or(&files[i].path),
        )
        .unwrap();
    }
    let table = TableIR::read(path).await.unwrap();
    let prepared = partitioned::prepare(table).await.unwrap();
    let plan = prepared.physical_plan();
    assert_eq!(plan.output_partitioning().partition_count(), 6);
    assert_eq!(
        values(plan.execute(2, Arc::new(TaskContext::default())).unwrap()).await,
        vec![4, 5]
    );
    assert!(
        plan.execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn pushed_key_filter_skips_unmatched_files_inside_a_source_partition() {
    use vail_execution::expressions::BinaryOperator;
    use vail_storage::local_table::WriteOptions;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("filtered");
    let path = path.to_str().unwrap();
    let keyed = TableIR::range(12, 1)
        .unwrap()
        .key_by(vec!["idx".into()])
        .unwrap();
    keyed
        .write_with_options(
            path,
            false,
            WriteOptions {
                max_buffer_bytes: 1024 * 1024,
                max_rows_per_file: 2,
            },
        )
        .await
        .unwrap();
    let read = open_local(path).await.unwrap();
    assert_eq!(read.descriptor().files.len(), 6);
    for file in read
        .descriptor()
        .files
        .iter()
        .take(4)
        .chain(read.descriptor().files.iter().skip(5))
    {
        std::fs::remove_file(file.path.strip_prefix("file://").unwrap_or(&file.path)).unwrap();
    }

    let read = TableIR::read(path).await.unwrap();
    let lower = ScalarIR::Binary {
        operator: BinaryOperator::Gt,
        left: Arc::new(ScalarIR::Column { name: "idx".into() }),
        right: Arc::new(ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: json!(7),
        }),
    };
    let upper = ScalarIR::Binary {
        operator: BinaryOperator::Lt,
        left: Arc::new(ScalarIR::Column { name: "idx".into() }),
        right: Arc::new(ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: json!(10),
        }),
    };
    let filter = ScalarIR::Binary {
        operator: BinaryOperator::And,
        left: Arc::new(lower),
        right: Arc::new(upper),
    };
    let selected = read
        .filter(filter, true)
        .unwrap()
        .key_by(vec![])
        .unwrap()
        .map_rows(vec![(
            "constant".into(),
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: json!(1),
            },
        )])
        .unwrap();
    let output = selected
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let values = output
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
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1, 1]);
}

#[tokio::test]
async fn pushed_key_or_filter_unions_disjoint_fragment_ranges() {
    use vail_execution::expressions::BinaryOperator;
    use vail_storage::local_table::WriteOptions;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("or-filtered");
    let path = path.to_str().unwrap();
    TableIR::range(12, 1)
        .unwrap()
        .key_by(vec!["idx".into()])
        .unwrap()
        .write_with_options(
            path,
            false,
            WriteOptions {
                max_buffer_bytes: 1024 * 1024,
                max_rows_per_file: 2,
            },
        )
        .await
        .unwrap();
    let source = open_local(path).await.unwrap();
    for file in &source.descriptor().files[2..4] {
        std::fs::remove_file(file.path.strip_prefix("file://").unwrap_or(&file.path)).unwrap();
    }

    let read = TableIR::read(path).await.unwrap();
    let comparison = |operator, value| ScalarIR::Binary {
        operator,
        left: Arc::new(ScalarIR::Column { name: "idx".into() }),
        right: Arc::new(ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: json!(value),
        }),
    };
    let predicate = ScalarIR::Binary {
        operator: BinaryOperator::Or,
        left: Arc::new(comparison(BinaryOperator::Lt, 2)),
        right: Arc::new(comparison(BinaryOperator::Gt, 9)),
    };
    let selected = read.filter(predicate, true).unwrap();
    let batches = selected
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let values = batches
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
        .collect::<Vec<_>>();
    assert_eq!(values, vec![0, 1, 10, 11]);
}

#[tokio::test]
async fn pushed_key_filter_prunes_unicode_utf8_string_fragments() {
    use vail_execution::expressions::BinaryOperator;
    use vail_storage::local_table::WriteOptions;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("unicode-filtered");
    let path = path.to_str().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let values = ["a", "é", "ø", "𝔘", "😀"];
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))]).unwrap();
    TableIR::parallelize(batch, 1)
        .unwrap()
        .key_by(vec!["s".into()])
        .unwrap()
        .write_with_options(
            path,
            false,
            WriteOptions {
                max_buffer_bytes: 1024 * 1024,
                max_rows_per_file: 1,
            },
        )
        .await
        .unwrap();
    let source = open_local(path).await.unwrap();
    assert_eq!(source.descriptor().files.len(), values.len());
    for file in &source.descriptor().files[..3] {
        std::fs::remove_file(file.path.strip_prefix("file://").unwrap_or(&file.path)).unwrap();
    }

    let read = TableIR::read(path).await.unwrap();
    let predicate = ScalarIR::Binary {
        operator: BinaryOperator::Gt,
        left: Arc::new(ScalarIR::Column { name: "s".into() }),
        right: Arc::new(ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Str),
            value: json!("ø"),
        }),
    };
    let batches = read
        .filter(predicate, true)
        .unwrap()
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let selected = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(selected, vec!["𝔘", "😀"]);
}
#[tokio::test]
async fn range_shuffle_and_coalesce_survive_planning_and_repeat_actions() {
    let shuffled = keyed_source()
        .repartition(3, RepartitionStrategy::Shuffle)
        .unwrap();
    for _ in 0..2 {
        assert_eq!(shuffled.n_partitions().await.unwrap(), 3);
        assert_eq!(shuffled.count().await.unwrap(), 12);
        let partitions = shuffled.collect_partitions().await.unwrap();
        assert_eq!(
            partitions
                .iter()
                .map(|p| p.iter().map(RecordBatch::num_rows).sum::<usize>())
                .collect::<Vec<_>>(),
            vec![4, 4, 4]
        );
    }
    let head = shuffled.head(5);
    let tail = shuffled.tail(5);
    assert_eq!(head.count().await.unwrap(), 5);
    assert_eq!(tail.count().await.unwrap(), 5);
    let coalesced = shuffled
        .repartition(2, RepartitionStrategy::Coalesce)
        .unwrap();
    assert_eq!(coalesced.n_partitions().await.unwrap(), 2);
    assert_eq!(coalesced.count().await.unwrap(), 12);
    assert_eq!(coalesced.collect_partitions().await.unwrap().len(), 2);
}
#[tokio::test]
async fn actual_shuffle_output_has_disjoint_ordered_key_ranges() {
    let shuffled = keyed_source()
        .repartition(3, RepartitionStrategy::Shuffle)
        .unwrap();
    let prepared = partitioned::prepare(shuffled).await.unwrap();
    let plan = prepared.physical_plan();
    let context = Arc::new(TaskContext::default());
    let mut all = Vec::new();
    let mut previous = None;
    for partition in 0..plan.output_partitioning().partition_count() {
        let part = values(plan.execute(partition, context.clone()).unwrap()).await;
        assert!(part.windows(2).all(|pair| pair[0] <= pair[1]));
        if let (Some(last), Some(first)) = (previous, part.first()) {
            assert!(last < *first);
        }
        if let Some(last) = part.last() {
            previous = Some(*last);
        }
        all.extend(part);
    }
    assert_eq!(all, (0..12).collect::<Vec<_>>());
}
