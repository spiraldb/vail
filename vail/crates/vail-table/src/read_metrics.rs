//! Bridge the storage reader's existing live counter to DataFusion metrics.
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion_physical_plan::metrics::{
    CustomMetricValue, ExecutionPlanMetricsSet, MetricBuilder, MetricValue,
};
use vail_storage::ScanMetrics;

#[derive(Debug)]
struct RequestedBytes {
    source: Option<ScanMetrics>,
    aggregated: AtomicU64,
}
impl RequestedBytes {
    fn value(&self) -> u64 {
        self.source.as_ref().map_or(0, ScanMetrics::bytes_read)
            .saturating_add(self.aggregated.load(Ordering::Relaxed))
    }
}
impl Display for RequestedBytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.value()) }
}
impl CustomMetricValue for RequestedBytes {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(Self { source: None, aggregated: AtomicU64::new(0) })
    }
    fn aggregate(&self, other: Arc<dyn CustomMetricValue>) {
        let value = other.as_any().downcast_ref::<Self>()
            .map_or_else(|| other.as_usize() as u64, Self::value);
        // Aggregation owns a separate accumulator; never increment a live
        // reader counter or make a repeated metrics summary change its source.
        self.aggregated.fetch_add(value, Ordering::Relaxed);
    }
    fn as_any(&self) -> &dyn Any { self }
    fn as_usize(&self) -> usize { usize::try_from(self.value()).unwrap_or(usize::MAX) }
    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|other| self.value() == other.value())
    }
}

/// Register one fresh domain per execute call. The plan's registry accumulates
/// executions; each metric remains live if a canceled read still owns work.
/// Values are observed attempted ranges, not completed or physical disk bytes.
pub(crate) fn register(metrics: &ExecutionPlanMetricsSet, partition: usize) -> ScanMetrics {
    let source = ScanMetrics::new();
    MetricBuilder::new(metrics).with_partition(partition).build(MetricValue::Custom {
        name: "bytes_requested".into(),
        value: Arc::new(RequestedBytes { source: Some(source.clone()), aggregated: AtomicU64::new(0) }),
    });
    source
}
