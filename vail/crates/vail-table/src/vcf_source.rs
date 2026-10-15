//! Bounded native VCF scan. Header planning is eager; record decoding is lazy.
//! A blocking decoder worker feeds at most two Arrow batches to the async scan.
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::{
    PartitionedFile, TableSchema,
    file::FileSource,
    file_scan_config::FileScanConfig,
    file_stream::{FileOpenFuture, FileOpener},
};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;
use std::{fmt, io::BufRead, sync::Arc};

#[derive(Clone, Debug)]
pub struct VcfSource {
    schema: TableSchema,
    header: vail_vcf::Header,
    options: vail_vcf::Options,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    projection: Option<datafusion_physical_expr::projection::ProjectionExprs>,
}
impl VcfSource {
    pub fn new(schema: SchemaRef, value: &serde_json::Value) -> Result<Self> {
        let header: vail_vcf::Header = serde_json::from_value(value["header"].clone())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let options: vail_vcf::Options = serde_json::from_value(value["options"].clone())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Self {
            schema: TableSchema::from(schema),
            header,
            options,
            batch_size: 8192,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: None,
        })
    }
}
impl FileSource for VcfSource {
    fn create_file_opener(
        &self,
        _store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(VcfOpener {
            source: self.clone(),
        }))
    }
    fn table_schema(&self) -> &TableSchema {
        &self.schema
    }
    fn projection(&self) -> Option<&datafusion_physical_expr::projection::ProjectionExprs> {
        self.projection.as_ref()
    }
    fn try_pushdown_projection(
        &self,
        projection: &datafusion_physical_expr::projection::ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        if !projection.expr_iter().all(|expr| {
            expr.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                .is_some()
        }) {
            return Ok(None);
        }
        let mut source = self.clone();
        source.projection = Some(projection.clone());
        Ok(Some(Arc::new(source)))
    }
    fn with_batch_size(&self, size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = size.max(1);
        Arc::new(source)
    }
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }
    fn file_type(&self) -> &str {
        "vcf"
    }
    fn supports_repartitioning(&self) -> bool {
        false
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_plan::PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        datafusion_physical_plan::apply_expression_roots(
            std::iter::empty::<Arc<dyn datafusion_physical_plan::PhysicalExpr>>(),
            f,
        )
    }
    fn fmt_extra(
        &self,
        _t: datafusion_physical_plan::display::DisplayFormatType,
        _f: &mut fmt::Formatter,
    ) -> fmt::Result {
        Ok(())
    }
}
struct VcfOpener {
    source: VcfSource,
}
impl FileOpener for VcfOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "VCF byte-range partitions are not implemented".into(),
            ));
        }
        let source = self.source.clone();
        let path = format!("/{}", file.object_meta.location);
        Ok(Box::pin(async move {
            let (sender, receiver) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(2);
            tokio::task::spawn_blocking(move || {
                let run = || -> vail_vcf::Result<()> {
                    let mut reader = vail_vcf::open(&path)?;
                    let samples = if source.options.drop_samples {
                        0
                    } else {
                        source.header.samples.len()
                    };
                    let batch_rows = source.batch_size.min((65536 / samples.max(1)).max(1));
                    let mut parser = vail_vcf::Parser::new(source.header, source.options)?;
                    let mut batch = parser.batch()?;
                    let indices = source.projection.as_ref().map(|p| {
                        p.expr_iter()
                            .map(|expr| {
                                expr.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                                    .unwrap()
                                    .index()
                            })
                            .collect::<Vec<_>>()
                    });
                    let count_only = indices.as_ref().is_some_and(Vec::is_empty);
                    let finish = |batch: &mut vail_vcf::Batch| -> vail_vcf::Result<RecordBatch> {
                        if count_only {
                            let rows = batch.rows;
                            batch.rows = 0;
                            RecordBatch::try_new_with_options(
                                Arc::new(arrow_schema::Schema::empty()),
                                vec![],
                                &RecordBatchOptions::new().with_row_count(Some(rows)),
                            )
                            .map_err(|e| e.to_string())
                        } else {
                            let batch = batch.finish()?;
                            if let Some(indices) = &indices {
                                batch.project(indices).map_err(|e| e.to_string())
                            } else {
                                Ok(batch)
                            }
                        }
                    };
                    let mut line = String::new();
                    let mut line_number = 0usize;
                    loop {
                        if sender.is_closed() {
                            return Ok(());
                        }
                        line.clear();
                        if reader
                            .read_line(&mut line)
                            .map_err(|e| format!("{path}: {e}"))?
                            == 0
                        {
                            break;
                        }
                        line_number += 1;
                        let record = line.trim_end_matches(['\r', '\n']);
                        if count_only {
                            if parser.count_record(record)? {
                                batch.rows += 1;
                            }
                        } else {
                            parser
                                .append(&mut batch, record)
                                .map_err(|e| format!("{path}:{line_number}: {e}"))?;
                        }
                        if batch.rows == batch_rows {
                            if sender.blocking_send(Ok(finish(&mut batch)?)).is_err() {
                                return Ok(());
                            }
                        }
                    }
                    if batch.rows > 0 {
                        let _ = sender.blocking_send(Ok(finish(&mut batch)?));
                    }
                    Ok(())
                };
                if let Err(error) = run() {
                    let _ = sender.blocking_send(Err(DataFusionError::Execution(error)));
                }
            });
            let stream = futures::stream::unfold(receiver, |mut receiver| async move {
                receiver.recv().await.map(|batch| (batch, receiver))
            });
            Ok(Box::pin(stream) as futures::stream::BoxStream<'static, Result<RecordBatch>>)
        }))
    }
}
