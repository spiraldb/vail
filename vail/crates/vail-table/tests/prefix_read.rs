//! Prefix reads use the normal public table/storage APIs and real native files.
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{ExecutionPlan, execution_plan::collect, limit::GlobalLimitExec};
use futures::TryStreamExt;
use std::{collections::HashSet, sync::Arc};
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_storage::local_table::{PartitionEvent, open_local};
use vail_table::{ScalarIR, TableIR, plan};

fn ints(batches: &[RecordBatch], field: &str) -> Vec<i32> {
    batches.iter().flat_map(|batch| batch.column_by_name(field).unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec()).collect()
}
fn requested(plan: &Arc<dyn ExecutionPlan>) -> usize {
    fn visit(plan: &Arc<dyn ExecutionPlan>, seen: &mut HashSet<usize>) -> usize {
        if !seen.insert(Arc::as_ptr(plan) as *const () as usize) { return 0; }
        let own = plan.metrics().map_or(0, |metrics| metrics.iter()
            .filter(|m| m.value().name() == "bytes_requested").map(|m| m.value().as_usize()).sum::<usize>());
        own + plan.children().iter().map(|child| visit(child, seen)).sum::<usize>()
    }
    visit(plan, &mut HashSet::new())
}

#[tokio::test]
async fn native_prefix_skips_later_files_and_zero_skips_all_files() {
    let dir = tempfile::tempdir().unwrap(); let path = dir.path().join("rows");
    TableIR::range(400, 4).unwrap().write(path.to_str().unwrap(), false).await.unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let storage = open_local(path.to_str().unwrap()).await.unwrap();
    assert_eq!(storage.descriptor().files.len(), 4);
    for file in &storage.descriptor().files[1..] {
        std::fs::remove_file(file.path.strip_prefix("file://").unwrap_or(&file.path)).unwrap();
    }
    assert_eq!(ints(&read.sample(11).await.unwrap(), "idx"), (0..11).collect::<Vec<_>>());
    assert!(read.collect_partitions().await.is_err(), "full read must reach missing files");
    let first = &storage.descriptor().files[0].path;
    std::fs::remove_file(first.strip_prefix("file://").unwrap_or(first)).unwrap();
    assert!(read.sample(0).await.unwrap().is_empty());
}

#[tokio::test]
async fn native_prefix_preserves_partition_order_and_independent_full_consumer() {
    let dir = tempfile::tempdir().unwrap(); let path = dir.path().join("rows");
    TableIR::range(257, 3).unwrap().write(path.to_str().unwrap(), false).await.unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let small = read.head(11);
    let (prefix, full) = futures::join!(small.collect_partitions(), read.collect_partitions());
    assert_eq!(ints(&prefix.unwrap().into_iter().flatten().collect::<Vec<_>>(), "idx"), (0..11).collect::<Vec<_>>());
    assert_eq!(ints(&full.unwrap().into_iter().flatten().collect::<Vec<_>>(), "idx"), (0..257).collect::<Vec<_>>());
    assert_eq!(ints(&read.sample(95).await.unwrap(), "idx"), (0..95).collect::<Vec<_>>());
    let physical = plan::physical(&read.head(200)).await.unwrap();
    let offset = Arc::new(GlobalLimitExec::new(physical, 83, Some(11)));
    let output = collect(offset, Arc::new(TaskContext::default())).await.unwrap();
    assert_eq!(ints(&output, "idx"), (83..94).collect::<Vec<_>>());
}

#[tokio::test]
async fn dropping_empty_and_null_owners_keeps_scanning_until_enough_entries() {
    let mut values = Vec::new(); let mut offsets = vec![0]; let mut valid = Vec::new();
    for row in 0..24 {
        if row < 12 {
            if row % 2 == 1 { values.extend([9999, 9999]); }
            valid.push(row % 2 == 0);
        } else { values.extend([row * 2, row * 2 + 1]); valid.push(true); }
        offsets.push(values.len() as i32);
    }
    let items = Arc::new(ListArray::new(Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(offsets.into()), Arc::new(Int32Array::from(values)), Some(NullBuffer::from(valid)))) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("owner", DataType::Int32, false), Field::new("items", items.data_type().clone(), true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(0..24)), items]).unwrap();
    let dir = tempfile::tempdir().unwrap(); let path = dir.path().join("lists");
    TableIR::parallelize(batch, 4).unwrap().write(path.to_str().unwrap(), false).await.unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap().explode(vec!["items".into()]).unwrap();
    let predicate = ScalarIR::Binary { operator: BinaryOperator::Gt,
        left: Arc::new(ScalarIR::Column { name: "items".into() }),
        right: Arc::new(ScalarIR::Literal { dtype: Type::Primitive(PrimitiveType::Int32), value: serde_json::json!(25) }) };
    let filtered = read.filter(predicate, true).unwrap();
    let prefix = filtered.sample(3).await.unwrap();
    assert_eq!(ints(&prefix, "items"), vec![26, 27, 28]);
    assert_eq!(ints(&prefix, "owner"), vec![13, 13, 14]);
    let full = filtered.collect_partitions().await.unwrap().into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(ints(&full, "items"), (26..48).collect::<Vec<_>>());
    let action = plan::physical(&filtered.head(3)).await.unwrap();
    let context = Arc::new(TaskContext::default());
    assert_eq!(ints(&collect(action.clone(), context.clone()).await.unwrap(), "items"), vec![26, 27, 28]);
    let error = collect(action, context).await.unwrap_err();
    assert!(error.to_string().contains("explicit new action"));
    assert_eq!(ints(&filtered.sample(3).await.unwrap(), "items"), vec![26, 27, 28]);
}

#[tokio::test]
async fn reader_limits_and_requested_bytes_use_actual_native_path() {
    let dir = tempfile::tempdir().unwrap(); let path = dir.path().join("rows");
    TableIR::range(4000, 4).unwrap().write(path.to_str().unwrap(), false).await.unwrap();
    let storage = open_local(path.to_str().unwrap()).await.unwrap().with_scan_batch_size(3).with_partition_row_limit(Some(7));
    let events = storage.partition_batches(0).try_collect::<Vec<_>>().await.unwrap();
    let batches = events.into_iter().filter_map(|event| match event { PartitionEvent::Batch(batch) => Some(batch), _ => None }).collect::<Vec<_>>();
    assert_eq!(ints(&batches, "idx"), (0..7).collect::<Vec<_>>());
    assert!(batches.iter().all(|b| b.num_rows() <= 3));
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let prefix = plan::physical(&read.head(11)).await.unwrap();
    let small = collect(prefix.clone(), Arc::new(TaskContext::default())).await.unwrap();
    assert_eq!(ints(&small, "idx"), (0..11).collect::<Vec<_>>());
    let prefix_bytes = requested(&prefix);
    let full = plan::physical(&read.head(4000)).await.unwrap();
    let all = collect(full.clone(), Arc::new(TaskContext::default())).await.unwrap();
    assert_eq!(ints(&all, "idx"), (0..4000).collect::<Vec<_>>());
    let full_bytes = requested(&full);
    eprintln!("requested native bytes: prefix={prefix_bytes}, full={full_bytes}");
    assert!(prefix_bytes > 0 && prefix_bytes < full_bytes, "requested {prefix_bytes} prefix vs {full_bytes} full bytes");
}
