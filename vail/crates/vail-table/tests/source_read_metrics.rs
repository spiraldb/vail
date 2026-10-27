//! Requested-byte counters on the normal public native source execution path.
use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow_array::Int64Array;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion_physical_plan::metrics::{Metric, MetricsSet};
use futures::TryStreamExt;
use vail_table::{TableIR, partitioned};
use vail_storage::local_table::open_local;

async fn fixture(path: &str) -> Arc<dyn ExecutionPlan> {
    TableIR::range(97, 1).unwrap().write(path, false).await.unwrap();
    let table = TableIR::read(path).await.unwrap();
    partitioned::prepare(table).await.unwrap().physical_plan()
}

fn requested(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<Metric>> {
    fn visit(plan: &Arc<dyn ExecutionPlan>, seen: &mut HashSet<usize>, out: &mut Vec<Arc<Metric>>) {
        if !seen.insert(Arc::as_ptr(plan) as *const () as usize) { return; }
        if let Some(metrics) = plan.metrics() {
            out.extend(metrics.iter().filter(|metric| metric.value().name() == "bytes_requested").cloned());
        }
        for child in plan.children() { visit(child, seen, out); }
    }
    let mut result = Vec::new();
    visit(plan, &mut HashSet::new(), &mut result);
    result
}

async fn values(stream: SendableRecordBatchStream) -> Vec<i64> {
    stream.try_collect::<Vec<_>>().await.unwrap().iter().flat_map(|batch|
        batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().iter().copied()
    ).collect()
}

#[tokio::test]
async fn normal_source_metrics_are_live_separate_and_summarize_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let plan = fixture(dir.path().join("table").to_str().unwrap()).await;
    assert!(requested(&plan).is_empty());
    let first = values(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    assert_eq!(first, (0..97).collect::<Vec<_>>());
    let domains = requested(&plan);
    assert_eq!(domains.len(), 1);
    let first_bytes = domains[0].value().as_usize();
    assert!(first_bytes > 0);
    let second = values(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    assert_eq!(second, first);
    let domains = requested(&plan);
    assert_eq!(domains.len(), 2);
    assert_eq!(domains[0].value().as_usize(), first_bytes);
    assert!(domains[1].value().as_usize() > 0);
    let mut set = MetricsSet::new();
    for domain in &domains { set.push(domain.clone()); }
    let before = domains.iter().map(|d| d.value().as_usize()).collect::<Vec<_>>();
    let first_sum = set.sum_by_name("bytes_requested").unwrap().as_usize();
    let second_sum = set.sum_by_name("bytes_requested").unwrap().as_usize();
    assert_eq!(first_sum, before.iter().sum());
    assert_eq!(second_sum, first_sum);
    assert_eq!(before, domains.iter().map(|d| d.value().as_usize()).collect::<Vec<_>>());

    let (left, right) = futures::join!(
        values(plan.execute(0, Arc::new(TaskContext::default())).unwrap()),
        values(plan.execute(0, Arc::new(TaskContext::default())).unwrap()),
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
    let dir = tempfile::tempdir().unwrap();
    let plan = fixture(dir.path().join("table").to_str().unwrap()).await;
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
async fn corrupt_real_file_keeps_attempts_when_execution_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table");
    let path = path.to_str().unwrap();
    let plan = fixture(path).await;
    let source = open_local(path).await.unwrap();
    let file = &source.descriptor().files[0].path;
    let mut file = std::fs::OpenOptions::new().read(true).write(true)
        .open(file.strip_prefix("file://").unwrap_or(file)).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    let mut last = [0];
    file.read_exact(&mut last).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[last[0] ^ 0xff]).unwrap();
    drop(file);
    let result = plan.execute(0, Arc::new(TaskContext::default())).unwrap()
        .try_collect::<Vec<_>>().await;
    assert!(result.is_err());
    let domains = requested(&plan);
    assert_eq!(domains.len(), 1);
    assert!(domains[0].value().as_usize() > 0);
}
