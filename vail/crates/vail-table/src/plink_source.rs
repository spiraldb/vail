//! Bounded native PLINK SNP-major BED scan. Genotype decoding is lazy.
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
use std::{
    fmt,
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

#[derive(Clone, Debug)]
struct PlinkOptions {
    bim: String,
    n_samples: usize,
    a2_reference: bool,
    contig_recoding: std::collections::HashMap<String, String>,
    lengths: Option<std::collections::HashMap<String, u64>>,
    skip_invalid_loci: bool,
    contig_ordinals: Option<std::collections::HashMap<String, usize>>,
}
impl PlinkOptions {
    fn parse(v: &serde_json::Value) -> std::result::Result<Self, String> {
        Ok(Self {
            bim: v["bim"]
                .as_str()
                .ok_or("PLINK bim must be a path")?
                .to_owned(),
            n_samples: usize::try_from(
                v["n_samples"]
                    .as_u64()
                    .ok_or("PLINK sample count must be nonnegative")?,
            )
            .map_err(|e| e.to_string())?,
            a2_reference: v["a2_reference"]
                .as_bool()
                .ok_or("PLINK a2_reference must be boolean")?,
            skip_invalid_loci: v["skip_invalid_loci"]
                .as_bool()
                .ok_or("PLINK skip_invalid_loci must be boolean")?,
            contig_recoding: serde_json::from_value(v["contig_recoding"].clone())
                .map_err(|e| e.to_string())?,
            lengths: serde_json::from_value(v["lengths"].clone()).map_err(|e| e.to_string())?,
            contig_ordinals: serde_json::from_value(v["contig_ordinals"].clone())
                .map_err(|e| e.to_string())?,
        })
    }
}
#[derive(Clone, Debug)]
pub struct PlinkSource {
    schema: TableSchema,
    options: PlinkOptions,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    projection: Option<datafusion_physical_expr::projection::ProjectionExprs>,
}
impl PlinkSource {
    pub fn new(schema: SchemaRef, value: &serde_json::Value) -> Result<Self> {
        Ok(Self {
            schema: TableSchema::from(schema),
            options: PlinkOptions::parse(value).map_err(DataFusionError::Plan)?,
            batch_size: 8192,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: None,
        })
    }
}
impl FileSource for PlinkSource {
    fn create_file_opener(
        &self,
        _store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(PlinkOpener {
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
        "plink"
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
struct PlinkOpener {
    source: PlinkSource,
}
impl FileOpener for PlinkOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "PLINK byte-range partitions are not implemented".into(),
            ));
        }
        let source = self.source.clone();
        let path = format!("/{}", file.object_meta.location);
        Ok(Box::pin(async move {
            let (sender, receiver) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(2);
            tokio::task::spawn_blocking(move || {
                let run = || -> std::result::Result<(), String> {
                    scan(&path, &source, &sender)?;
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

fn scan(
    path: &str,
    source: &PlinkSource,
    sender: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> std::result::Result<(), String> {
    use serde_json::{Value, json};
    use std::io::BufRead;
    let o = &source.options;
    let n = o.n_samples;
    let width = n.div_ceil(4);
    let mut bed = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mut magic = [0; 3];
    bed.read_exact(&mut magic).map_err(|e| e.to_string())?;
    if magic != [0x6c, 0x1b, 1] {
        return Err("PLINK BED requires SNP-major magic bytes 6c 1b 01".into());
    }
    let indices = source.projection.as_ref().map(|p| {
        p.expr_iter()
            .map(|e| {
                e.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                    .unwrap()
                    .index()
            })
            .collect::<Vec<_>>()
    });
    let decode = indices.as_ref().is_none_or(|p| p.contains(&1));
    let batch_size = source.batch_size.min((65536 / n.max(1)).max(1));
    let mut values = Vec::with_capacity(batch_size);
    let emit = |values: &mut Vec<Value>| -> std::result::Result<bool, String> {
        let batch = if indices.as_ref().is_some_and(Vec::is_empty) {
            RecordBatch::try_new_with_options(
                Arc::new(arrow_schema::Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(values.len())),
            )
            .map_err(|e| e.to_string())?
        } else {
            let mut decoder = arrow_json::ReaderBuilder::new(source.schema.table_schema().clone())
                .with_batch_size(values.len())
                .build_decoder()
                .map_err(|e| e.to_string())?;
            decoder.serialize(values).map_err(|e| e.to_string())?;
            let batch = decoder
                .flush()
                .map_err(|e| e.to_string())?
                .ok_or("empty PLINK batch")?;
            if let Some(p) = &indices {
                batch.project(p).map_err(|e| e.to_string())?
            } else {
                batch
            }
        };
        values.clear();
        Ok(sender.blocking_send(Ok(batch)).is_ok())
    };
    // Decode table depends only on the chosen reference orientation.
    let hom0 = if o.a2_reference { 1 } else { 0 };
    let hom1 = 1 - hom0;
    let call = |a, b| json!({"__vail_call_allele0":a,"__vail_call_allele1":b,"__vail_call_ploidy":2,"__vail_call_phased":false});
    let calls = [call(hom0, hom0), Value::Null, call(0, 1), call(hom1, hom1)];
    let mut bytes = vec![0; width];
    let bim = std::io::BufReader::new(std::fs::File::open(&o.bim).map_err(|e| e.to_string())?);
    let mut variants = 0u64;
    for line in bim.lines() {
        if sender.is_closed() {
            return Ok(());
        }
        let line = line.map_err(|e| e.to_string())?;
        let f = line.split_whitespace().collect::<Vec<_>>();
        if f.len() != 6 {
            return Err("PLINK BIM requires six fields per variant".into());
        }
        variants += 1;
        let pos = f[3].parse::<i32>().map_err(|_| "invalid BIM position")?;
        let cm = f[2]
            .parse::<f64>()
            .map_err(|_| "invalid BIM centimorgan position")?;
        if !cm.is_finite() {
            return Err("invalid BIM centimorgan position".into());
        }
        let contig = o
            .contig_recoding
            .get(f[0])
            .map(String::as_str)
            .unwrap_or(f[0]);
        let valid = o.lengths.as_ref().is_none_or(|l| {
            l.get(contig)
                .is_some_and(|len| pos > 0 && pos as u64 <= *len)
        });
        if !valid && !o.skip_invalid_loci {
            return Err(format!("Invalid locus {contig}:{pos}"));
        }
        if !valid || !decode {
            bed.seek(SeekFrom::Current(width as i64))
                .map_err(|e| e.to_string())?;
        } else {
            bed.read_exact(&mut bytes)
                .map_err(|e| format!("truncated PLINK BED: {e}"))?;
        }
        if !valid {
            continue;
        }
        let locus = if let Some(ord) = &o.contig_ordinals {
            json!({"__vail_locus_contig":contig,"__vail_locus_ordinal":ord.get(contig).ok_or("unknown reference contig")?,"__vail_locus_position":pos})
        } else {
            json!({"contig":contig,"position":pos})
        };
        let alleles = if o.a2_reference {
            [f[5], f[4]]
        } else {
            [f[4], f[5]]
        };
        let cells = if decode {
            (0..n).map(|i|json!({"__vail_matrix_col_id":i,"value":{"GT":calls[((bytes[i/4]>>(2*(i%4)))&3) as usize]}})).collect::<Vec<_>>()
        } else {
            vec![]
        };
        values.push(json!({"__vail_matrix_row":{"locus":locus,"alleles":alleles,"rsid":f[1],"cm_position":cm},"__vail_matrix_cells":cells}));
        if values.len() == batch_size && !emit(&mut values)? {
            return Ok(());
        }
    }
    let expected = 3 + variants
        .checked_mul(width as u64)
        .ok_or("PLINK size overflow")?;
    if bed.metadata().map_err(|e| e.to_string())?.len() != expected {
        return Err("PLINK BED size does not match BIM/FAM dimensions".into());
    }
    if !values.is_empty() {
        emit(&mut values)?;
    }
    Ok(())
}
