//! Bounded native BGEN layout-2 scan. Variant and probability decoding is lazy.
//! A blocking decoder worker feeds at most two Arrow batches to the async scan.
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, ListArray, RecordBatch, RecordBatchOptions, StructArray,
    UInt8Array, UInt16Array,
    builder::{
        ArrayBuilder, Float64Builder, Int32Builder, ListBuilder, StringBuilder, StructBuilder,
        make_builder,
    },
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef, Fields, SchemaRef};
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

fn bgen_builder(dtype: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match dtype {
        DataType::Struct(fields) => Box::new(StructBuilder::new(
            fields.clone(),
            fields
                .iter()
                .map(|f| bgen_builder(f.data_type(), capacity))
                .collect(),
        )),
        DataType::List(field) if field.data_type() == &DataType::Float64 => {
            Box::new(ListBuilder::new(Float64Builder::new()).with_field(field.clone()))
        }
        DataType::List(field) => Box::new(
            ListBuilder::new(bgen_builder(field.data_type(), capacity)).with_field(field.clone()),
        ),
        _ => make_builder(dtype, capacity),
    }
}

/// Row builders and reusable sample-layout buffers belong to the scan. Entry
/// batches own primitive Arrow buffers; offsets and Call metadata
/// are shared across variants. Undemanded cells allocate no sample buffers.
struct Batch {
    schema: SchemaRef,
    indices: Vec<usize>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    rows: usize,
    row_fields: Vec<String>,
    cell_field: FieldRef,
    cell_fields: Fields,
    entry_fields: Fields,
    ploidy: ArrayRef,
    phase: ArrayRef,
    gp_offsets: OffsetBuffer<i32>,
    missing_cells: ArrayRef,
    cell_arrays: Vec<ArrayRef>,
    cell_offsets: Vec<i32>,
}
impl Batch {
    fn new(
        source: &BgenSource,
        indices: Vec<usize>,
        capacity: usize,
        samples: usize,
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
            .map(|f| bgen_builder(f.data_type(), capacity))
            .collect();
        let field_names = |dtype: &DataType| match dtype {
            DataType::Struct(fields) => fields.iter().map(|f| f.name().clone()).collect(),
            _ => Vec::new(),
        };
        let row_fields = source
            .schema
            .table_schema()
            .fields()
            .first()
            .map(|f| field_names(f.data_type()))
            .unwrap_or_default();
        let DataType::List(cell_field) = source.schema.table_schema().field(1).data_type() else {
            return Err("BGEN cell schema must be a list".into());
        };
        let DataType::Struct(cell_fields) = cell_field.data_type() else {
            return Err("BGEN cell schema must contain structs".into());
        };
        let DataType::Struct(entry_fields) = cell_fields[0].data_type() else {
            return Err("BGEN entries must contain structs".into());
        };
        let gp_end = samples
            .checked_mul(3)
            .and_then(|n| i32::try_from(n).ok())
            .ok_or("BGEN sample dimension exceeds Arrow list offset range")?;
        let gp_offsets = OffsetBuffer::new(ScalarBuffer::from(
            (0..=samples).map(|i| (i * 3) as i32).collect::<Vec<_>>(),
        ));
        debug_assert_eq!(*gp_offsets.last().unwrap(), gp_end);
        let missing_entries =
            arrow_array::new_null_array(&DataType::Struct(entry_fields.clone()), samples);
        let missing_cells = Arc::new(
            StructArray::try_new_with_length(
                cell_fields.clone(),
                vec![missing_entries],
                None,
                samples,
            )
            .map_err(|e| e.to_string())?,
        ) as ArrayRef;
        Ok(Self {
            schema,
            indices,
            builders,
            rows: 0,
            row_fields,
            cell_field: cell_field.clone(),
            cell_fields: cell_fields.clone(),
            entry_fields: entry_fields.clone(),
            ploidy: Arc::new(UInt8Array::from(vec![2; samples])),
            phase: Arc::new(BooleanArray::new(BooleanBuffer::new_unset(samples), None)),
            gp_offsets,
            missing_cells,
            cell_arrays: Vec::new(),
            cell_offsets: vec![0],
        })
    }
    fn finish(&mut self) -> std::result::Result<RecordBatch, String> {
        let mut arrays = Vec::with_capacity(self.builders.len());
        for (root, value) in self.indices.iter().zip(&mut self.builders) {
            if *root == 1 {
                let cells: ArrayRef = if self.cell_arrays.is_empty() {
                    // Row-only/count action: no sample buffers were decoded.
                    Arc::new(ListArray::new_null(self.cell_field.clone(), self.rows))
                } else {
                    let child = if self.cell_arrays.len() == 1 {
                        self.cell_arrays.pop().unwrap()
                    } else {
                        let refs = self
                            .cell_arrays
                            .iter()
                            .map(|a| a.as_ref())
                            .collect::<Vec<_>>();
                        let result =
                            arrow_select::concat::concat(&refs).map_err(|e| e.to_string())?;
                        self.cell_arrays.clear();
                        result
                    };
                    let offsets = std::mem::replace(&mut self.cell_offsets, vec![0]);
                    Arc::new(
                        ListArray::try_new(
                            self.cell_field.clone(),
                            OffsetBuffer::new(offsets.into()),
                            child,
                            None,
                        )
                        .map_err(|e| e.to_string())?,
                    )
                };
                arrays.push(cells);
            } else {
                arrays.push(value.finish());
            }
        }
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
        for (index, name) in self.row_fields.iter().enumerate() {
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
        if !self.indices.contains(&1) {
            return Ok(());
        }
        // Build data buffers directly. Array builders' per-value validity
        // updates were dominating decoding; one bitmap represents each
        // nullable array, while constant child buffers are shared by variants.
        let present = NullBuffer::new(BooleanBuffer::collect_bool(n, |sample| {
            data[8 + sample] & 128 == 0
        }));
        if present.null_count() == n {
            return self.push_cells(self.missing_cells.clone(), n);
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.entry_fields.len());
        let probabilities = &data[10 + n..];
        for field in &self.entry_fields {
            let array: ArrayRef = match field.name().as_str() {
                "GP" if gp => {
                    let mut values = Vec::with_capacity(n * 3);
                    for pair in probabilities.chunks_exact(2) {
                        let a = pair[0] as f64;
                        let b = pair[1] as f64;
                        values.extend_from_slice(&[a / 255.0, b / 255.0, (255.0 - a - b) / 255.0]);
                    }
                    let DataType::List(child) = field.data_type() else {
                        return Err("BGEN GP must be a list".into());
                    };
                    Arc::new(
                        ListArray::try_new(
                            child.clone(),
                            self.gp_offsets.clone(),
                            Arc::new(Float64Array::from(values)),
                            Some(present.clone()),
                        )
                        .map_err(|e| e.to_string())?,
                    )
                }
                "dosage" if dosage => {
                    let values = probabilities
                        .chunks_exact(2)
                        .map(|p| (p[1] as f64 + 2.0 * (255.0 - p[0] as f64 - p[1] as f64)) / 255.0)
                        .collect::<Vec<_>>();
                    Arc::new(Float64Array::new(values.into(), Some(present.clone())))
                }
                "GT" if gt => {
                    let mut first = Vec::with_capacity(n);
                    let mut second = Vec::with_capacity(n);
                    // Pinned Arrow 59.2 collect_bool visits every index once,
                    // in ascending order (including the final partial word).
                    // Keep the allele columns in that same physical order.
                    let validity = BooleanBuffer::collect_bool(n, |sample| {
                        let a = probabilities[sample * 2] as i32;
                        let b = probabilities[sample * 2 + 1] as i32;
                        let c = 255 - a - b;
                        let hom_ref = (a > b) & (a > c);
                        let het = (b > a) & (b > c);
                        let hom_alt = (c > a) & (c > b);
                        first.push(hom_alt as u16);
                        second.push((het | hom_alt) as u16);
                        (hom_ref | het | hom_alt) & (data[8 + sample] & 128 == 0)
                    });
                    let DataType::Struct(fields) = field.data_type() else {
                        return Err("BGEN GT must be a struct".into());
                    };
                    Arc::new(
                        StructArray::try_new(
                            fields.clone(),
                            vec![
                                Arc::new(UInt16Array::from(first)),
                                Arc::new(UInt16Array::from(second)),
                                self.ploidy.clone(),
                                self.phase.clone(),
                            ],
                            Some(NullBuffer::new(validity)),
                        )
                        .map_err(|e| e.to_string())?,
                    )
                }
                "GP" | "dosage" | "GT" => arrow_array::new_null_array(field.data_type(), n),
                _ => return Err("unexpected BGEN entry field".into()),
            };
            arrays.push(array);
        }
        let entries = Arc::new(
            StructArray::try_new_with_length(self.entry_fields.clone(), arrays, Some(present), n)
                .map_err(|e| e.to_string())?,
        ) as ArrayRef;
        let cells = Arc::new(
            StructArray::try_new_with_length(self.cell_fields.clone(), vec![entries], None, n)
                .map_err(|e| e.to_string())?,
        ) as ArrayRef;
        self.push_cells(cells, n)
    }
    fn push_cells(&mut self, cells: ArrayRef, n: usize) -> std::result::Result<(), String> {
        let end = self
            .cell_offsets
            .last()
            .unwrap()
            .checked_add(n as i32)
            .ok_or("BGEN batch exceeds Arrow list offset range")?;
        self.cell_arrays.push(cells);
        self.cell_offsets.push(end);
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
    let mut batch = Batch::new(source, indices, batch_size, if read_cells { n } else { 0 })?;
    let mut data = Vec::new();
    let mut compressed = Vec::new();
    // Every probability block is an independent compression frame, but its
    // decompression workspace belongs to the scan, not to each variant.
    let mut zlib = (flags & 3 == 1).then(|| flate2::Decompress::new(true));
    let mut zstd = if flags & 3 == 2 {
        Some(zstd::bulk::Decompressor::new().map_err(|e| e.to_string())?)
    } else {
        None
    };
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
        i32::try_from(position).map_err(|_| "BGEN position exceeds signed 32-bit locus range")?;
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
            data.resize(expected, 0);
            if flags & 3 != 0 {
                if length < 4 {
                    return Err("invalid BGEN compressed block length".into());
                }
                let uncompressed = u32(&mut r)? as usize;
                if uncompressed != expected {
                    return Err("BGEN requires diploid 8-bit probability blocks".into());
                }
                compressed.resize(length - 4, 0);
                r.read_exact(&mut compressed).map_err(|e| e.to_string())?;
                if let Some(decoder) = zlib.as_mut() {
                    decoder.reset(true);
                    let status = decoder
                        .decompress(&compressed, &mut data, flate2::FlushDecompress::Finish)
                        .map_err(|e| e.to_string())?;
                    if status != flate2::Status::StreamEnd || decoder.total_out() != expected as u64
                    {
                        return Err("BGEN uncompressed block length mismatch".into());
                    }
                } else {
                    let written = zstd
                        .as_mut()
                        .unwrap()
                        .decompress_to_buffer(&compressed, &mut data[..])
                        .map_err(|e| e.to_string())?;
                    if written != expected {
                        return Err("BGEN uncompressed block length mismatch".into());
                    }
                }
            } else {
                if length != expected {
                    return Err("BGEN requires diploid 8-bit probability blocks".into());
                }
                r.read_exact(&mut data).map_err(|e| e.to_string())?;
            }
            let mut d = std::io::Cursor::new(&data);
            if u32(&mut d)? as usize != n || u16(&mut d)? != k {
                return Err("BGEN probability block sample/allele count mismatch".into());
            }
            if data[6] != 2 || data[7] != 2 || data[8 + n] != 0 || data[9 + n] != 8 {
                return Err("BGEN requires unphased diploid 8-bit probabilities".into());
            }
            // The global layout header establishes the supported diploid,
            // unphased 8-bit representation. Per-sample probability sums and
            // redundant ploidy flags are data semantics, not bounds checks.
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
