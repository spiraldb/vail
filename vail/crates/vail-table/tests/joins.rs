use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion_physical_plan::ExecutionPlanProperties;
use std::sync::Arc;
use vail_table::{TableIR, TableJoinKind};

fn table(key: &str, value: &str, keys: Vec<Option<i32>>, values: Vec<i32>) -> vail_table::TableRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new(key, DataType::Int32, true),
        Field::new(value, DataType::Int32, true),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap();
    TableIR::parallelize(rows, 2)
        .unwrap()
        .key_by(vec![key.into()])
        .unwrap()
}

#[tokio::test]
async fn native_join_has_lazy_datafusion_operator_and_cartesian_duplicates() {
    let left = table("k", "l", vec![Some(1), Some(1), None], vec![10, 11, 12]);
    let right = table("rk", "r", vec![Some(1), Some(1), None], vec![20, 21, 22]);
    for (kind, count) in [
        (TableJoinKind::Inner, 4),
        (TableJoinKind::Left, 5),
        (TableJoinKind::Right, 5),
        (TableJoinKind::Outer, 6),
    ] {
        let joined = left.join(right.clone(), kind, 1).unwrap();
        assert_eq!(joined.known_count(), None);
        assert_eq!(joined.explain()["node"], "TableJoin");
        let plan = joined.physical_plan().unwrap();
        assert!(plan.contains("SortMergeJoinExec"));
        assert!(plan.contains("SortExec"));
        assert_eq!(joined.count().await.unwrap(), count);
        assert_eq!(joined.count().await.unwrap(), count);
    }
}

#[tokio::test]
async fn membership_joins_preserve_independent_left_partitions() {
    let left = table(
        "k",
        "l",
        vec![Some(1), Some(2), None, Some(1)],
        vec![10, 11, 12, 13],
    );
    let right = table("rk", "r", vec![Some(1), Some(1), None], vec![20, 21, 22]);
    for kind in [TableJoinKind::Semi, TableJoinKind::Anti] {
        let joined = left.join(right.clone(), kind, 1).unwrap();
        assert_eq!(
            joined.n_partitions().await.unwrap(),
            left.n_partitions().await.unwrap()
        );
        let prepared = vail_table::partitioned::prepare(joined.clone())
            .await
            .unwrap();
        let physical = prepared.physical_plan();
        assert_eq!(
            physical.output_partitioning().partition_count(),
            prepared.parts.len()
        );
        assert_eq!(joined.count().await.unwrap(), 2);
        assert_eq!(joined.schema(), left.schema());
    }
}

#[tokio::test]
async fn interval_join_partitions_contain_merge_operators_and_metadata_needs_no_rows() {
    fn find<'a>(
        plan: &'a dyn datafusion_physical_plan::ExecutionPlan,
        name: &str,
    ) -> Option<&'a dyn datafusion_physical_plan::ExecutionPlan> {
        if plan.name() == name {
            return Some(plan);
        }
        plan.children()
            .into_iter()
            .find_map(|child| find(child.as_ref(), name))
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("left");
    TableIR::range(12, 3)
        .unwrap()
        .write_with_options(
            path.to_str().unwrap(),
            false,
            vail_storage::local_table::WriteOptions {
                max_buffer_bytes: 1024 * 1024,
                max_rows_per_file: 2,
            },
        )
        .await
        .unwrap();
    let source = vail_storage::local_table::open_local(path.to_str().unwrap())
        .await
        .unwrap();
    let bounds = source.partition_key_bounds().unwrap();
    assert_eq!(bounds.len(), 3);
    assert!(bounds.iter().all(Option::is_some));
    let left = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let right = TableIR::range(12, 4).unwrap();
    let joined = left.join(right, TableJoinKind::Outer, 1).unwrap();
    let prepared = vail_table::partitioned::prepare(joined.clone())
        .await
        .unwrap();
    assert!(prepared.parts.len() > 1);
    for partition in &prepared.parts {
        assert!(find(partition.as_ref(), "HashJoinExec").is_none());
        let merge = find(partition.as_ref(), "SortMergeJoinExec").unwrap();
        for child in merge.children() {
            assert_eq!(child.name(), "JoinKeyOrderingExec");
        }
    }
    assert_eq!(joined.count().await.unwrap(), 12);
    for file in &source.descriptor().files {
        std::fs::remove_file(file.path.strip_prefix("file://").unwrap_or(&file.path)).unwrap();
    }
    assert_eq!(source.partition_key_bounds().unwrap(), bounds);
}
