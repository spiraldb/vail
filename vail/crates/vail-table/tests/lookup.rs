use arrow_array::{Array, Int32Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;
use vail_table::{ScalarIR, TableIR, globals::GlobalBindings};

fn find<'a>(plan: &'a dyn ExecutionPlan, name: &str) -> Option<&'a dyn ExecutionPlan> {
    if plan.name() == name {
        return Some(plan);
    }
    plan.children()
        .into_iter()
        .find_map(|child| find(child.as_ref(), name))
}

#[tokio::test]
async fn lookup_is_a_streaming_merge_join_and_preserves_source_partitions() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Int32, true),
    ]));
    let rows = |ids: Vec<Option<i32>>, values: Vec<i32>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    };
    let left = TableIR::parallelize(
        rows(
            vec![Some(2), Some(1), Some(9), None, Some(1)],
            vec![0, 1, 2, 3, 4],
        ),
        3,
    )
    .unwrap();
    let right = TableIR::parallelize(
        rows(vec![Some(1), Some(1), Some(2), None], vec![30, 31, 40, 90]),
        2,
    )
    .unwrap()
    .key_by(vec!["id".into()])
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("target");
    right.write(path.to_str().unwrap(), false).await.unwrap();
    let right = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let table = left
        .lookup(
            right.clone(),
            vec![ScalarIR::Column { name: "id".into() }],
            "match".into(),
            GlobalBindings::new(),
            false,
            false,
        )
        .unwrap();
    assert_eq!(table.explain()["node"], "TableLookup");
    assert_eq!(table.known_count(), Some(5));
    assert_eq!(table.known_n_partitions(), Some(3));
    let prepared = vail_table::partitioned::prepare(table.clone())
        .await
        .unwrap();
    assert_eq!(
        prepared
            .physical_plan()
            .output_partitioning()
            .partition_count(),
        3
    );
    for part in &prepared.parts {
        assert!(find(part.as_ref(), "HashJoinExec").is_none());
        let join = find(part.as_ref(), "SortMergeJoinExec").unwrap();
        let target = join.children()[1];
        assert!(find(target.as_ref(), "SortExec").is_none());
        assert!(find(target.as_ref(), "BoundedWindowAggExec").is_some());
        assert!(find(target.as_ref(), "LookupSourceOrderingExec").is_some());
    }
    let partitions = table.collect_partitions().await.unwrap();
    assert_eq!(partitions.len(), 3);
    let mut values = Vec::new();
    let mut missing = Vec::new();
    for batch in partitions.iter().flatten() {
        let original = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let payload = batch
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        values.extend(original.values().iter().copied());
        missing.extend((0..batch.num_rows()).map(|i| payload.is_null(i)));
    }
    assert_eq!(values, vec![0, 1, 2, 3, 4]);
    assert_eq!(missing, vec![false, false, true, true, false]);
}

#[tokio::test]
async fn all_matches_lookup_keeps_duplicates_and_uses_empty_arrays_for_no_match() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("value", DataType::Int32, true),
    ]));
    let batch = |ids: Vec<Option<i32>>, values: Vec<i32>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    };
    let left = TableIR::parallelize(
        batch(vec![Some(1), Some(9), None, Some(1)], vec![0, 1, 2, 3]),
        2,
    )
    .unwrap();
    let right = TableIR::parallelize(batch(vec![Some(1), Some(1), None], vec![10, 11, 90]), 1)
        .unwrap()
        .key_by(vec!["id".into()])
        .unwrap();
    let table = left
        .lookup(
            right,
            vec![ScalarIR::Column { name: "id".into() }],
            "matches".into(),
            GlobalBindings::new(),
            true,
            false,
        )
        .unwrap();
    assert_eq!(table.known_n_partitions(), Some(2));
    let partitions = table.collect_partitions().await.unwrap();
    let mut lengths = Vec::new();
    for batch in partitions.iter().flatten() {
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        lengths.extend((0..values.len()).map(|i| values.value_length(i)));
    }
    assert_eq!(lengths, vec![2, 0, 0, 2]);
}
