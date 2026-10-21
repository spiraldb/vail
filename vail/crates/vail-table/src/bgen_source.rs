//! Bounded native BGEN layout-2 scan. Variant and probability decoding is lazy.
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

/// Planning validates the selectable field set once. Record loops use fixed flags.
#[derive(Clone, Debug)]
struct BgenOptions {
    contig_recoding: std::collections::HashMap<String, String>,
    lengths: Option<std::collections::HashMap<String, u64>>,
    skip_invalid_loci: bool,
    contig_ordinals: Option<std::collections::HashMap<String, usize>>,
    gp: bool,
    gt: bool,
    dosage: bool,
    varid: bool,
    rsid: bool,
}
impl BgenOptions {
    fn parse(v: &serde_json::Value) -> std::result::Result<Self, String> {
        let strings = |key: &str| -> std::result::Result<Vec<String>, String> {
            v[key]
                .as_array()
                .ok_or_else(|| format!("BGEN {key} must be an array"))?
                .iter()
                .map(|x| {
                    x.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| format!("BGEN {key} must contain strings"))
                })
                .collect()
        };
        let entries = strings("entry_fields")?;
        let rows = strings("row_fields")?;
        if entries
            .iter()
            .any(|x| !["GT", "GP", "dosage"].contains(&x.as_str()))
            || rows
                .iter()
                .any(|x| !["rsid", "varid"].contains(&x.as_str()))
        {
            return Err("invalid BGEN selected field".into());
        }
        let contigs: Option<Vec<String>> =
            serde_json::from_value(v["contigs"].clone()).map_err(|e| e.to_string())?;
        Ok(Self {
            contig_ordinals: contigs
                .map(|v| v.into_iter().enumerate().map(|(i, c)| (c, i)).collect()),
            contig_recoding: serde_json::from_value(v["contig_recoding"].clone())
                .map_err(|e| e.to_string())?,
            lengths: serde_json::from_value(v["lengths"].clone()).map_err(|e| e.to_string())?,
            skip_invalid_loci: v["skip_invalid_loci"]
                .as_bool()
                .ok_or("BGEN skip_invalid_loci must be boolean")?,
            gp: entries.iter().any(|x| x == "GP"),
            gt: entries.iter().any(|x| x == "GT"),
            dosage: entries.iter().any(|x| x == "dosage"),
            varid: rows.iter().any(|x| x == "varid"),
            rsid: rows.iter().any(|x| x == "rsid"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct BgenSource {
    schema: TableSchema,
    options: BgenOptions,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    projection: Option<datafusion_physical_expr::projection::ProjectionExprs>,
}
impl BgenSource {
    pub fn new(schema: SchemaRef, value: &serde_json::Value) -> Result<Self> {
        Ok(Self {
            schema: TableSchema::from(schema),
            options: BgenOptions::parse(value).map_err(DataFusionError::Plan)?,
            batch_size: 8192,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: None,
        })
    }
}
impl FileSource for BgenSource {
    fn create_file_opener(
        &self,
        _store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(BgenOpener {
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
        "bgen"
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
struct BgenOpener {
    source: BgenSource,
}
impl FileOpener for BgenOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "BGEN byte-range partitions are not implemented".into(),
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

fn u16(r: &mut impl Read) -> std::result::Result<u16, String> {
    let mut b = [0; 2];
    r.read_exact(&mut b).map_err(|e| e.to_string())?;
    Ok(u16::from_le_bytes(b))
}
fn u32(r: &mut impl Read) -> std::result::Result<u32, String> {
    let mut b = [0; 4];
    r.read_exact(&mut b).map_err(|e| e.to_string())?;
    Ok(u32::from_le_bytes(b))
}
fn string(r: &mut impl Read, wide: bool) -> std::result::Result<String, String> {
    let n = if wide {
        u32(r)? as usize
    } else {
        u16(r)? as usize
    };
    // A malformed length must never trigger an unbounded allocation.
    if n > 16 * 1024 * 1024 {
        return Err("BGEN identifier exceeds 16 MB".into());
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b).map_err(|e| e.to_string())?;
    String::from_utf8(b).map_err(|e| e.to_string())
}
fn scan(
    path: &str,
    source: &BgenSource,
    sender: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> std::result::Result<(), String> {
    use serde_json::{Value, json};
    let mut r = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let file_len = r.metadata().map_err(|e| e.to_string())?.len();
    let offset = u32(&mut r)? as u64 + 4;
    let header_len = u32(&mut r)? as u64;
    let variants = u32(&mut r)?;
    let n = u32(&mut r)? as usize;
    let mut magic = [0; 4];
    r.read_exact(&mut magic).map_err(|e| e.to_string())?;
    if header_len < 20
        || header_len + 4 > offset
        || offset > file_len
        || (magic != *b"bgen" && magic != [0; 4])
    {
        return Err("invalid BGEN header".into());
    }
    r.seek(SeekFrom::Start(header_len))
        .map_err(|e| e.to_string())?;
    let flags = u32(&mut r)?;
    if (flags >> 2) & 15 != 2 || flags & 3 > 2 {
        return Err("BGEN requires layout 2 and zlib, zstd or uncompressed blocks".into());
    }
    r.seek(SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
    let indices = source.projection.as_ref().map(|p| {
        p.expr_iter()
            .map(|e| {
                e.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                    .unwrap()
                    .index()
            })
            .collect::<Vec<_>>()
    });
    let decode_entries = (source.options.gp || source.options.gt || source.options.dosage)
        && indices.as_ref().is_none_or(|v| v.contains(&1));
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
                .ok_or("empty BGEN batch")?;
            if let Some(indices) = &indices {
                batch.project(indices).map_err(|e| e.to_string())?
            } else {
                batch
            }
        };
        values.clear();
        Ok(sender.blocking_send(Ok(batch)).is_ok())
    };
    for _ in 0..variants {
        if sender.is_closed() {
            return Ok(());
        }
        let varid = string(&mut r, false)?;
        let rsid = string(&mut r, false)?;
        let raw_contig = string(&mut r, false)?;
        let contig = source
            .options
            .contig_recoding
            .get(&raw_contig)
            .map(String::as_str)
            .unwrap_or(&raw_contig);
        let position = u32(&mut r)?;
        let k = u16(&mut r)?;
        if k != 2 {
            return Err("BGEN requires biallelic variants".into());
        }
        let alleles = (0..k)
            .map(|_| string(&mut r, true))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let length = u32(&mut r)? as usize;
        let end = r
            .stream_position()
            .map_err(|e| e.to_string())?
            .checked_add(length as u64)
            .ok_or("BGEN block overflow")?;
        if end > file_len {
            return Err("truncated BGEN variant block".into());
        }
        let valid = source.options.lengths.as_ref().is_none_or(|lengths| {
            lengths
                .get(contig)
                .is_some_and(|len| position > 0 && position as u64 <= *len)
        });
        if !valid {
            if !source.options.skip_invalid_loci {
                return Err(format!("Invalid locus {contig}:{position}"));
            }
            r.seek(SeekFrom::Start(end)).map_err(|e| e.to_string())?;
            continue;
        }
        let mut row = json!({"locus":{"contig":contig,"position":position},"alleles":alleles});
        if let Some(ordinals) = &source.options.contig_ordinals {
            row["locus"] = json!({"__vail_locus_contig":contig,"__vail_locus_ordinal":ordinals.get(contig).ok_or("BGEN unknown reference contig")?,"__vail_locus_position":position});
        }
        if source.options.varid {
            row["varid"] = json!(varid);
        }
        if source.options.rsid {
            row["rsid"] = json!(rsid);
        }
        let mut cells = Vec::with_capacity(n);
        if decode_entries {
            let mut data = Vec::new();
            if flags & 3 != 0 {
                if length < 4 {
                    return Err("invalid BGEN compressed block length".into());
                }
                let uncompressed = u32(&mut r)? as usize;
                // Bound decompression by the maximum valid diploid 8-bit block.
                if uncompressed != 10 + 3 * n {
                    return Err("BGEN requires diploid 8-bit probability blocks".into());
                }
                let compressed = (&mut r).take((length - 4) as u64);
                let mut decoder: Box<dyn Read + '_> = if flags & 3 == 1 {
                    Box::new(flate2::read::ZlibDecoder::new(compressed))
                } else {
                    Box::new(
                        zstd::stream::read::Decoder::new(compressed).map_err(|e| e.to_string())?,
                    )
                };
                (&mut decoder)
                    .take((uncompressed + 1) as u64)
                    .read_to_end(&mut data)
                    .map_err(|e| e.to_string())?;
                if data.len() != uncompressed {
                    return Err("BGEN uncompressed block length mismatch".into());
                }
            } else {
                if length != 10 + 3 * n {
                    return Err("BGEN requires diploid 8-bit probability blocks".into());
                }
                data.resize(length, 0);
                r.read_exact(&mut data).map_err(|e| e.to_string())?;
            }
            let mut d = std::io::Cursor::new(&data);
            if u32(&mut d)? as usize != n || u16(&mut d)? != k {
                return Err("BGEN probability block sample/allele count mismatch".into());
            }
            if data[6] != 2 || data[7] != 2 || data[8 + n] != 0 || data[9 + n] != 8 {
                return Err("BGEN requires unphased diploid 8-bit probabilities".into());
            }
            for i in 0..n {
                let missing = data[8 + i] & 128 != 0;
                if data[8 + i] & 63 != 2 || data[8 + i] & 64 != 0 {
                    return Err("BGEN requires diploid samples".into());
                }
                let a = data[10 + n + 2 * i] as u16;
                let b = data[11 + n + 2 * i] as u16;
                if a + b > 255 {
                    return Err("BGEN probability sum exceeds one".into());
                }
                let probs = [a, b, 255 - a - b];
                let mut entry = json!({});
                if source.options.gp {
                    entry["GP"] = if missing {
                        Value::Null
                    } else {
                        json!(probs.map(|v| v as f64 / 255.0))
                    };
                }
                if source.options.dosage {
                    entry["dosage"] = if missing {
                        Value::Null
                    } else {
                        json!((b + 2 * probs[2]) as f64 / 255.0)
                    };
                }
                if source.options.gt {
                    // Ties have no hard call; all calls here are diploid and unphased.
                    let genotype = if a > b && a > probs[2] {
                        Some((0, 0))
                    } else if b > a && b > probs[2] {
                        Some((0, 1))
                    } else if probs[2] > a && probs[2] > b {
                        Some((1, 1))
                    } else {
                        None
                    };
                    entry["GT"] = match genotype.filter(|_| !missing) {
                        Some((a, b)) => {
                            json!({"__vail_call_allele0":a,"__vail_call_allele1":b,"__vail_call_ploidy":2,"__vail_call_phased":false})
                        }
                        None => Value::Null,
                    };
                }
                cells.push(json!({"__vail_matrix_col_id":i,"value":if missing { Value::Null } else { entry }}));
            }
        } else if indices.as_ref().is_none_or(|v| v.contains(&1)) {
            cells = (0..n)
                .map(|i| json!({"__vail_matrix_col_id":i,"value":{}}))
                .collect();
        }
        r.seek(SeekFrom::Start(end)).map_err(|e| e.to_string())?;
        values.push(json!({"__vail_matrix_row":row,"__vail_matrix_cells":cells}));
        if values.len() == batch_size && !emit(&mut values)? {
            return Ok(());
        }
    }
    if !values.is_empty() {
        emit(&mut values)?;
    }
    Ok(())
}
