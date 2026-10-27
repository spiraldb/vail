//! Requested-byte counters on the normal public native source execution path.
use std::collections::HashSet;
use std::fs::remove_file;
use std::sync::Arc;
use tempfile::tempdir;

use arrow_array::Int32Array;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::metrics::{Metric, MetricsSet};
use datafusion_physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::{TryStreamExt, join};
use vail_storage::local_table::open_local;
use vail_table::partitioned::PreparedTable;
use vail_table::{TableIR, partitioned};

async fn fixture(path: &str) -> PreparedTable {
    TableIR::range(97, 1)
        .unwrap()
        .write(path, false)
        .await
        .unwrap();
    let table = TableIR::read(path).await.unwrap();
    partitioned::prepare(table).await.unwrap()
}

fn requested(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<Metric>> {
    fn visit(plan: &Arc<dyn ExecutionPlan>, seen: &mut HashSet<usize>, out: &mut Vec<Arc<Metric>>) {
        if !seen.insert(Arc::as_ptr(plan) as *const () as usize) {
            return;
        }
        if let Some(metrics) = plan.metrics() {
            out.extend(
                metrics
                    .iter()
                    .filter(|metric| metric.value().name() == "bytes_requested")
                    .cloned(),
            );
        }
        for child in plan.children() {
            visit(child, seen, out);
        }
    }
    let mut result = Vec::new();
    visit(plan, &mut HashSet::new(), &mut result);
    result
}

async fn values(stream: SendableRecordBatchStream) -> Vec<i32> {
    stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
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
        .collect()
}

#[tokio::test]
async fn normal_source_metrics_are_live_separate_and_summarize_without_mutation() {
    let dir = tempdir().unwrap();
    let prepared = fixture(dir.path().join("table").to_str().unwrap()).await;
    let plan = prepared.physical_plan();
    assert!(requested(&plan).is_empty());
    let first = values(
        prepared
            .start_action()
            .unwrap()
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap(),
    )
    .await;
    assert_eq!(first, (0..97).collect::<Vec<_>>());
    let domains = requested(&plan);
    assert_eq!(domains.len(), 1);
    let first_bytes = domains[0].value().as_usize();
    assert!(first_bytes > 0);
    let second = values(
        prepared
            .start_action()
            .unwrap()
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap(),
    )
    .await;
    assert_eq!(second, first);
    let domains = requested(&plan);
    assert_eq!(domains.len(), 2);
    assert_eq!(domains[0].value().as_usize(), first_bytes);
    assert!(domains[1].value().as_usize() > 0);
    let mut set = MetricsSet::new();
    for domain in &domains {
        set.push(domain.clone());
    }
    let before = domains
        .iter()
        .map(|d| d.value().as_usize())
        .collect::<Vec<_>>();
    let first_sum = set
        .sum(|metric| metric.value().name() == "bytes_requested")
        .unwrap()
        .as_usize();
    let second_sum = set
        .sum(|metric| metric.value().name() == "bytes_requested")
        .unwrap()
        .as_usize();
    assert_eq!(first_sum, before.iter().sum::<usize>());
    assert_eq!(second_sum, first_sum);
    assert_eq!(
        before,
        domains
            .iter()
            .map(|d| d.value().as_usize())
            .collect::<Vec<_>>()
    );

    let (left, right) = join!(
        values(
            prepared
                .start_action()
                .unwrap()
                .execute(0, Arc::new(TaskContext::default()))
                .unwrap()
        ),
        values(
            prepared
                .start_action()
                .unwrap()
                .execute(0, Arc::new(TaskContext::default()))
                .unwrap()
        ),
    );
    assert_eq!(left, first);
    assert_eq!(right, first);
    let domains = requested(&plan);
    assert_eq!(domains.len(), 4);
    assert!(domains.iter().all(|d| d.value().as_usize() > 0));
    assert_eq!(domains[0].value().as_usize(), first_bytes);
}

#[tokio::test]
async fn canceled_source_retains_its_observed_attempted_bytes() {
    let dir = tempdir().unwrap();
    let prepared = fixture(dir.path().join("table").to_str().unwrap()).await;
    let plan = prepared.physical_plan();
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert!(stream.try_next().await.unwrap().is_some());
    let domains = requested(&plan);
    assert_eq!(domains.len(), 1);
    let observed = domains[0].value().as_usize();
    assert!(observed > 0);
    drop(stream);
    // A canceled scan may still own background work. This is an observation,
    // never an assertion that every read-producing task has retired.
    assert!(domains[0].value().as_usize() >= observed);
}

#[tokio::test]
async fn missing_real_file_fails_without_fabricated_read_requests() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("table");
    let path = path.to_str().unwrap();
    let plan = fixture(path).await.physical_plan();
    let source = open_local(path).await.unwrap();
    let file = &source.descriptor().files[0].path;
    remove_file(file.strip_prefix("file://").unwrap_or(file)).unwrap();
    let result = plan
        .execute(0, Arc::new(TaskContext::default()))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await;
    assert!(result.is_err());
    let domains = requested(&plan);
    assert_eq!(domains.len(), 1);
    assert_eq!(domains[0].value().as_usize(), 0);
}

#[tokio::test]
async fn caller_owned_live_counter_combines_real_file_scans_only_when_shared() {
    use vail_storage::ScanMetrics;
    use vail_storage::local_table::{PartitionEvent, WriteOptions};
    let dir = tempdir().unwrap();
    let path = dir.path().join("table");
    let path = path.to_str().unwrap();
    TableIR::range(97, 1)
        .unwrap()
        .write_with_options(
            path,
            false,
            WriteOptions {
                max_buffer_bytes: 1024 * 1024,
                max_rows_per_file: 16,
            },
        )
        .await
        .unwrap();
    let source = open_local(path).await.unwrap();
    assert!(source.descriptor().files.len() > 1);
    let shared = ScanMetrics::new();
    let unrelated = ScanMetrics::new();
    let source = source.with_scan_metrics(shared.clone());
    assert_eq!(shared.bytes_read(), 0);
    let events = source.batches().try_collect::<Vec<_>>().await.unwrap();
    let rows = events
        .iter()
        .filter_map(|event| match event {
            PartitionEvent::Batch(batch) => Some(batch.num_rows()),
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(rows, 97);
    assert!(shared.bytes_read() > 0);
    assert_eq!(unrelated.bytes_read(), 0);
}
