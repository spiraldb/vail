//! Bounded native BGEN layout-2 scan. Variant and probability decoding is lazy.
//! A blocking decoder worker feeds at most two Arrow batches to the async scan.
use arrow_array::{
    ArrayRef, RecordBatch, RecordBatchOptions,
    builder::{
        ArrayBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, ListBuilder,
        StringBuilder, StructBuilder, UInt8Builder, UInt16Builder, make_builder,
    },
};
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
    variant_offsets: Option<Vec<u64>>,
    variant_keys: Option<Vec<(String, u32, Vec<String>)>>,
    decode_paths: Option<Vec<vail_vcf::DecodePath>>,
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
        let variant_offsets: Option<Vec<u64>> = v
            .get("variant_offsets")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| e.to_string())?;
        let variant_keys: Option<Vec<(String, u32, Vec<String>)>> = v
            .get("variant_keys")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| e.to_string())?;
        if variant_keys.as_ref().is_some_and(|keys| {
            variant_offsets
                .as_ref()
                .is_none_or(|offsets| keys.len() != offsets.len())
        }) {
            return Err("BGEN indexed key and offset counts must agree".into());
        }
        Ok(Self {
            variant_keys,
            variant_offsets,
            decode_paths: v
                .get("decode_paths")
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()
                .map_err(|e| e.to_string())?,
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
    selection: Option<Arc<crate::bgen_select::Selection>>,
    schema: TableSchema,
    options: BgenOptions,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    projection: Option<datafusion_physical_expr::projection::ProjectionExprs>,
}
impl BgenSource {
    pub fn new(schema: SchemaRef, value: &serde_json::Value) -> Result<Self> {
        Ok(Self {
            selection: None,
            schema: TableSchema::from(schema),
            options: BgenOptions::parse(value).map_err(DataFusionError::Plan)?,
            batch_size: 8192,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: None,
        })
    }
    pub(crate) fn with_selection(
        mut self,
        selection: Option<Arc<crate::bgen_select::Selection>>,
    ) -> Self {
        self.selection = selection;
        self
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
fn string(
    r: &mut (impl Read + Seek),
    wide: bool,
    file_len: u64,
) -> std::result::Result<String, String> {
    let n = if wide {
        u32(r)? as usize
    } else {
        u16(r)? as usize
    };
    // The specification permits uint32-sized allele strings. Check against
    // remaining physical bytes before allocating from an untrusted prefix.
    let position = r.stream_position().map_err(|e| e.to_string())?;
    if n as u64 > file_len.saturating_sub(position) {
        return Err("truncated BGEN identifier".into());
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b).map_err(|e| e.to_string())?;
    String::from_utf8(b).map_err(|e| e.to_string())
}
fn builder<T: ArrayBuilder + 'static>(
    value: &mut dyn ArrayBuilder,
) -> std::result::Result<&mut T, String> {
    value
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(|| "BGEN Arrow builder disagrees with planned schema".into())
}

/// A batch owns only the demanded top-level builders. Child capacities grow
/// from decoded records; header sample counts never allocate cell buffers.
struct Batch {
    schema: SchemaRef,
    indices: Vec<usize>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    rows: usize,
}
impl Batch {
    fn new(
        source: &BgenSource,
        indices: Vec<usize>,
        capacity: usize,
    ) -> std::result::Result<Self, String> {
        let schema = std::sync::Arc::new(
            source
                .schema
                .table_schema()
                .project(&indices)
                .map_err(|e| e.to_string())?,
        );
        let builders = schema
            .fields()
            .iter()
            .map(|f| make_builder(f.data_type(), capacity))
            .collect();
        Ok(Self {
            schema,
            indices,
            builders,
            rows: 0,
        })
    }
    fn finish(&mut self) -> std::result::Result<RecordBatch, String> {
        // Undemanded cells retain only a null top-level array slot. No sample
        // buffers are allocated for a row-only/count action.
        for (root, value) in self.indices.iter().zip(&mut self.builders) {
            if *root == 1 && value.len() < self.rows {
                let remaining = self.rows - value.len();
                builder::<ListBuilder<Box<dyn ArrayBuilder>>>(value.as_mut())?
                    .append_nulls(remaining);
            }
        }
        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(self.rows)),
        )
        .map_err(|e| e.to_string())?;
        self.rows = 0;
        Ok(batch)
    }
    fn row(
        &mut self,
        contig: &str,
        position: u32,
        alleles: &[String],
        varid: &str,
        rsid: &str,
        ordinal: Option<usize>,
    ) -> std::result::Result<(), String> {
        let Some(index) = self.indices.iter().position(|i| *i == 0) else {
            return Ok(());
        };
        let row = builder::<StructBuilder>(self.builders[index].as_mut())?;
        let names = row
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        for (index, name) in names.iter().enumerate() {
            let value = row.field_builders_mut()[index].as_mut();
            match name.as_str() {
                "locus" => {
                    let locus = builder::<StructBuilder>(value)?;
                    if let Some(ordinal) = ordinal {
                        locus
                            .field_builder::<Int32Builder>(0)
                            .ok_or("BGEN locus ordinal storage")?
                            .append_value(ordinal as i32);
                        locus
                            .field_builder::<StringBuilder>(1)
                            .ok_or("BGEN locus contig storage")?
                            .append_value(contig);
                        locus
                            .field_builder::<Int32Builder>(2)
                            .ok_or("BGEN locus position storage")?
                            .append_value(position as i32);
                    } else {
                        locus
                            .field_builder::<StringBuilder>(0)
                            .ok_or("BGEN locus contig storage")?
                            .append_value(contig);
                        locus
                            .field_builder::<Int32Builder>(1)
                            .ok_or("BGEN locus position storage")?
                            .append_value(position as i32);
                    }
                    locus.append(true);
                }
                "alleles" => {
                    let array = builder::<ListBuilder<Box<dyn ArrayBuilder>>>(value)?;
                    let strings = builder::<StringBuilder>(array.values().as_mut())?;
                    strings.append_value(&alleles[0]);
                    strings.append_value(&alleles[1]);
                    array.append(true);
                }
                "varid" => builder::<StringBuilder>(value)?.append_value(varid),
                "rsid" => builder::<StringBuilder>(value)?.append_value(rsid),
                _ => return Err("unexpected BGEN row field".into()),
            }
        }
        row.append(true);
        Ok(())
    }
    fn cells(
        &mut self,
        data: &[u8],
        n: usize,
        gp: bool,
        gt: bool,
        dosage: bool,
    ) -> std::result::Result<(), String> {
        let Some(index) = self.indices.iter().position(|i| *i == 1) else {
            return Ok(());
        };
        let cells = builder::<ListBuilder<Box<dyn ArrayBuilder>>>(self.builders[index].as_mut())?;
        let cell = builder::<StructBuilder>(cells.values().as_mut())?;
        // Field positions and typed builders are bound once per batch/variant,
        // outside the sample loop. All samples retain their physical cell ID.
        let fields = cell.field_builders_mut();
        let (ids, entries) = fields.split_at_mut(1);
        let ids = builder::<Int64Builder>(ids[0].as_mut())?;
        let entries = builder::<StructBuilder>(entries[0].as_mut())?;
        let entry_names = entries
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        for sample in 0..n {
            ids.append_value(sample as i64);
        }
        for (index, name) in entry_names.iter().enumerate() {
            let out = entries.field_builders_mut()[index].as_mut();
            match name.as_str() {
                "GP" => {
                    let out = builder::<ListBuilder<Box<dyn ArrayBuilder>>>(out)?;
                    if !gp {
                        out.append_nulls(n);
                        continue;
                    }
                    for sample in 0..n {
                        let present = gp && data[8 + sample] & 128 == 0;
                        if present {
                            let a = data[10 + n + 2 * sample] as u16;
                            let b = data[11 + n + 2 * sample] as u16;
                            let values = builder::<Float64Builder>(out.values().as_mut())?;
                            values.append_value(a as f64 / 255.0);
                            values.append_value(b as f64 / 255.0);
                            values.append_value((255 - a - b) as f64 / 255.0);
                        }
                        out.append(present);
                    }
                }
                "dosage" => {
                    let out = builder::<Float64Builder>(out)?;
                    if !dosage {
                        out.append_nulls(n);
                        continue;
                    }
                    for sample in 0..n {
                        if dosage && data[8 + sample] & 128 == 0 {
                            let a = data[10 + n + 2 * sample] as u16;
                            let b = data[11 + n + 2 * sample] as u16;
                            out.append_value((b + 2 * (255 - a - b)) as f64 / 255.0);
                        } else {
                            out.append_null();
                        }
                    }
                }
                "GT" => {
                    let out = builder::<StructBuilder>(out)?;
                    if !gt {
                        out.field_builder::<UInt16Builder>(0)
                            .ok_or("BGEN call first allele storage")?
                            .append_nulls(n);
                        out.field_builder::<UInt16Builder>(1)
                            .ok_or("BGEN call second allele storage")?
                            .append_nulls(n);
                        out.field_builder::<UInt8Builder>(2)
                            .ok_or("BGEN call ploidy storage")?
                            .append_nulls(n);
                        out.field_builder::<BooleanBuilder>(3)
                            .ok_or("BGEN call phase storage")?
                            .append_nulls(n);
                        out.append_nulls(n);
                        continue;
                    }
                    for sample in 0..n {
                        let a = data[10 + n + 2 * sample] as u16;
                        let b = data[11 + n + 2 * sample] as u16;
                        let c = 255 - a - b;
                        let genotype = if a > b && a > c {
                            Some((0, 0))
                        } else if b > a && b > c {
                            Some((0, 1))
                        } else if c > a && c > b {
                            Some((1, 1))
                        } else {
                            None
                        };
                        let genotype = genotype.filter(|_| gt && data[8 + sample] & 128 == 0);
                        let (first, second) = genotype.unwrap_or((0, 0));
                        out.field_builder::<UInt16Builder>(0)
                            .ok_or("BGEN call first allele storage")?
                            .append_value(first);
                        out.field_builder::<UInt16Builder>(1)
                            .ok_or("BGEN call second allele storage")?
                            .append_value(second);
                        out.field_builder::<UInt8Builder>(2)
                            .ok_or("BGEN call ploidy storage")?
                            .append_value(2);
                        out.field_builder::<BooleanBuilder>(3)
                            .ok_or("BGEN call phase storage")?
                            .append_value(false);
                        out.append(genotype.is_some());
                    }
                }
                _ => return Err("unexpected BGEN entry field".into()),
            }
        }
        for sample in 0..n {
            entries.append(data[8 + sample] & 128 == 0);
        }
        for _ in 0..n {
            cell.append(true);
        }
        cells.append(true);
        Ok(())
    }
}

fn scan(
    path: &str,
    source: &BgenSource,
    sender: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> std::result::Result<(), String> {
    let mut r = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let file_len = r.metadata().map_err(|e| e.to_string())?.len();
    let offset = u32(&mut r)? as u64 + 4;
    let header_len = u32(&mut r)? as u64;
    let variant_count = u32(&mut r)? as usize;
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
    let indices = source
        .projection
        .as_ref()
        .map(|p| {
            p.expr_iter()
                .map(|e| {
                    e.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                        .unwrap()
                        .index()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![0, 1]);
    let read_cells = indices.contains(&1)
        && source.options.decode_paths.as_ref().is_none_or(|paths| {
            paths.iter().any(|p| {
                p.fields
                    .first()
                    .is_some_and(|name| name == "__vail_matrix_cells")
            })
        });
    let needs = |name: &str| {
        source.options.decode_paths.as_ref().is_none_or(|paths| {
            paths.iter().any(|p| {
                p.fields.first().is_some_and(|s| s == "__vail_matrix_cells")
                    && !p.shape
                    && !p.validity
                    && (p.fields.len() < 3 || p.fields.iter().any(|s| s == name))
            })
        })
    };
    let gp = source.options.gp && needs("GP");
    let gt = source.options.gt && needs("GT");
    let dosage = source.options.dosage && needs("dosage");
    let batch_size = if read_cells {
        source.batch_size.min((65536 / n.max(1)).max(1))
    } else {
        source.batch_size
    };
    let mut batch = Batch::new(source, indices, batch_size)?;
    let mut data = Vec::new();
    let variants = source
        .options
        .variant_offsets
        .as_ref()
        .map_or(variant_count, Vec::len);
    for variant in 0..variants {
        if sender.is_closed() {
            return Ok(());
        }
        // Intersect the native selector with persisted index keys before any
        // identifying or probability bytes of an unselected variant are read.
        if let (Some(selection), Some(keys)) = (&source.selection, &source.options.variant_keys) {
            let (contig, position, alleles) = &keys[variant];
            if !selection.matches(contig, *position, alleles)? {
                continue;
            }
        }
        if let Some(offsets) = &source.options.variant_offsets {
            let current = offsets[variant];
            if current < offset || current >= file_len {
                return Err("BGEN indexed variant offset outside file".into());
            }
            r.seek(SeekFrom::Start(current))
                .map_err(|e| e.to_string())?;
        }
        let varid = string(&mut r, false, file_len)?;
        let rsid = string(&mut r, false, file_len)?;
        let raw_contig = string(&mut r, false, file_len)?;
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
        let alleles = [
            string(&mut r, true, file_len)?,
            string(&mut r, true, file_len)?,
        ];
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
        let ordinal = source
            .options
            .contig_ordinals
            .as_ref()
            .map(|v| {
                v.get(contig)
                    .copied()
                    .ok_or("BGEN unknown reference contig")
            })
            .transpose()?;
        if let Some(selection) = &source.selection {
            if !selection.matches(contig, position, &alleles)? {
                r.seek(SeekFrom::Start(end)).map_err(|e| e.to_string())?;
                continue;
            }
        }
        batch.row(contig, position, &alleles, &varid, &rsid, ordinal)?;
        if read_cells {
            let expected = n
                .checked_mul(3)
                .and_then(|v| v.checked_add(10))
                .ok_or("BGEN sample dimension overflow")?;
            data.clear();
            if flags & 3 != 0 {
                if length < 4 {
                    return Err("invalid BGEN compressed block length".into());
                }
                let uncompressed = u32(&mut r)? as usize;
                if uncompressed != expected {
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
                    .take((expected + 1) as u64)
                    .read_to_end(&mut data)
                    .map_err(|e| e.to_string())?;
                if data.len() != expected {
                    return Err("BGEN uncompressed block length mismatch".into());
                }
            } else {
                if length != expected {
                    return Err("BGEN requires diploid 8-bit probability blocks".into());
                }
                (&mut r)
                    .take(length as u64)
                    .read_to_end(&mut data)
                    .map_err(|e| e.to_string())?;
                if data.len() != expected {
                    return Err("truncated BGEN probability block".into());
                }
            }
            let mut d = std::io::Cursor::new(&data);
            if u32(&mut d)? as usize != n || u16(&mut d)? != k {
                return Err("BGEN probability block sample/allele count mismatch".into());
            }
            if data[6] != 2 || data[7] != 2 || data[8 + n] != 0 || data[9 + n] != 8 {
                return Err("BGEN requires unphased diploid 8-bit probabilities".into());
            }
            for i in 0..n {
                if data[8 + i] & 63 != 2 || data[8 + i] & 64 != 0 {
                    return Err("BGEN requires diploid samples".into());
                }
                if (gp || gt || dosage)
                    && data[10 + n + 2 * i] as u16 + data[11 + n + 2 * i] as u16 > 255
                {
                    return Err("BGEN probability sum exceeds one".into());
                }
            }
            batch.cells(&data, n, gp, gt, dosage)?;
        }
        r.seek(SeekFrom::Start(end)).map_err(|e| e.to_string())?;
        batch.rows += 1;
        if batch.rows == batch_size && sender.blocking_send(Ok(batch.finish()?)).is_err() {
            return Ok(());
        }
    }
    if batch.rows > 0 {
        let _ = sender.blocking_send(Ok(batch.finish()?));
    }
    Ok(())
}
