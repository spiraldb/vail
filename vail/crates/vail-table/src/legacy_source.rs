//! Native Hail file scans with independent partitions and bounded backpressure.
use arrow_array::RecordBatch;
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
use std::{fmt, sync::Arc};

#[derive(Clone, Debug)]
pub struct LegacySource {
    schema: TableSchema,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    projection: Option<datafusion_physical_expr::projection::ProjectionExprs>,
    component: Arc<vail_storage::legacy::Component>,
    counts: Arc<Option<Vec<u64>>>,
    output_schema: SchemaRef,
    matrix: Option<Arc<crate::legacy::Matrix>>,
    part: usize,
    pruned: bool,
    filter: Option<vortex::expr::Expression>,
    filter_columns: std::collections::BTreeSet<usize>,
    decode_columns: Option<Vec<usize>>,
    decode_paths: Option<Vec<Vec<String>>>,
    key_range: Option<vail_storage::legacy::IntKeyBounds>,
}
impl LegacySource {
    pub fn with_decode_paths(mut self, options: &serde_json::Value) -> Self {
        if let Some(paths) = options
            .get("decode_paths")
            .and_then(serde_json::Value::as_array)
        {
            self.decode_paths = Some(
                paths
                    .iter()
                    .filter_map(|p| {
                        p["fields"].as_array().map(|fields| {
                            let mut path = fields
                                .iter()
                                .filter_map(|f| f.as_str().map(str::to_owned))
                                .collect::<Vec<_>>();
                            if p["shape"].as_bool() == Some(true)
                                || p["validity"].as_bool() == Some(true)
                            {
                                // A nonexistent descendant asks the native decoder
                                // to retain container shape/validity and skip values.
                                path.push(shape_marker(&self.output_schema));
                            }
                            path
                        })
                    })
                    .collect(),
            );
            self.decode_columns = Some(
                self.output_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, f)| {
                        paths
                            .iter()
                            .any(|p| {
                                p["fields"].get(0).and_then(serde_json::Value::as_str)
                                    == Some(f.name().as_str())
                            })
                            .then_some(i)
                    })
                    .collect(),
            );
        }
        self
    }
    pub fn with_predicates(
        mut self,
        filters: &[Arc<dyn datafusion_physical_expr::PhysicalExpr>],
    ) -> Self {
        if self.matrix.is_none() && self.counts.as_ref().is_some() {
            for filter in filters {
                if let Some(Some(bounds)) = self.component.int_key_bounds.get(self.part) {
                    self.pruned |= !could_match(filter, bounds);
                    let base = self.key_range.as_ref().unwrap_or(bounds);
                    if let Some(narrowed) = narrow_integer_filter(filter, base) {
                        if narrowed.min != bounds.min || narrowed.max != bounds.max {
                            self.key_range = Some(narrowed);
                        }
                    } else {
                        self.pruned = true;
                    }
                }
                if let Some(expr) = vortex_filter(filter, &self.component.schema) {
                    filter_columns(filter, &self.component.schema, &mut self.filter_columns);
                    self.filter = Some(match self.filter.take() {
                        Some(previous) => vortex::expr::and(previous, expr),
                        None => expr,
                    });
                }
            }
        }
        self
    }
    pub fn new(schema: SchemaRef, read: Arc<crate::legacy::Read>, part: usize) -> Self {
        Self {
            schema: TableSchema::from(schema.clone()),
            output_schema: schema,
            matrix: read.matrix.clone(),
            part,
            pruned: false,
            batch_size: 8192,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: None,
            filter: None,
            filter_columns: Default::default(),
            decode_columns: None,
            decode_paths: None,
            key_range: None,
            component: read.component.clone(),
            counts: read.counts.clone(),
        }
    }
}
impl FileSource for LegacySource {
    fn create_file_opener(
        &self,
        _store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(LegacyOpener {
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
        let mut columns = std::collections::HashSet::new();
        if !projection.expr_iter().all(|expr| {
            expr.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                .is_some_and(|column| columns.insert(column.index()))
        }) {
            return Ok(None);
        }
        let mut source = self.clone();
        source.projection = Some(projection.clone());
        Ok(Some(Arc::new(source)))
    }
    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>>,
        _config: &datafusion_common::config::ConfigOptions,
    ) -> Result<
        datafusion_physical_plan::filter_pushdown::FilterPushdownPropagation<Arc<dyn FileSource>>,
    > {
        use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
        let mut source = self.clone();
        let mut accepted = vec![];
        for filter in filters {
            if self.matrix.is_none()
                && let Some(Some(bounds)) = self.component.int_key_bounds.get(self.part)
            {
                source.pruned |= !could_match(&filter, bounds);
                let base = source.key_range.as_ref().unwrap_or(bounds);
                if let Some(narrowed) = narrow_integer_filter(&filter, base) {
                    if narrowed.min != bounds.min || narrowed.max != bounds.max {
                        source.key_range = Some(narrowed);
                    }
                } else {
                    source.pruned = true;
                }
            }
            let expr = if self.matrix.is_none() && self.counts.as_ref().is_some() {
                vortex_filter(&filter, &self.component.schema)
            } else {
                None
            };
            if let Some(expr) = expr {
                filter_columns(&filter, &self.component.schema, &mut source.filter_columns);
                source.filter = Some(match source.filter.take() {
                    Some(previous) => vortex::expr::and(previous, expr),
                    None => expr,
                });
                accepted.push(PushedDown::Yes)
            } else {
                accepted.push(PushedDown::No)
            }
        }
        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(accepted)
                .with_updated_node(Arc::new(source) as Arc<dyn FileSource>),
        )
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
        "legacy"
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
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(
            f,
            ", legacy_part={}, pruned={}, filter_pushdown={}",
            self.part,
            self.pruned,
            self.filter.is_some()
        )
    }
}
struct LegacyOpener {
    source: LegacySource,
}
impl FileOpener for LegacyOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "Legacy byte-range partitions are not implemented".into(),
            ));
        }
        let source = self.source.clone();

        Ok(Box::pin(async move {
            use futures::TryStreamExt;
            if source.pruned {
                return Ok(Box::pin(futures::stream::empty())
                    as futures::stream::BoxStream<'static, Result<RecordBatch>>);
            }
            let part = source.part;
            let mut projection = match &source.projection {
                Some(p) => p
                    .expr_iter()
                    .map(|e| {
                        e.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                            .unwrap()
                            .index()
                    })
                    .collect(),
                None => (0..source.output_schema.fields().len()).collect::<Vec<_>>(),
            };
            if source.matrix.is_some() {
                if let Some(rows) = source
                    .counts
                    .as_ref()
                    .as_ref()
                    .and_then(|counts| counts.get(part))
                    .copied()
                    .filter(|rows| *rows > 0)
                {
                    return matrix_layout_stream(source, part, projection, rows);
                }
                return matrix_stream(source, part, projection);
            }
            let requested = projection.clone();
            if source.filter.is_some() {
                // Bound the filter by names against a complete physical schema.
                // Unneeded fields can be removed after the filter expression.
                for index in &source.filter_columns {
                    if !projection.contains(index) {
                        projection.push(*index)
                    }
                }
            }
            let decode_columns = source.decode_columns.clone().map(|mut columns| {
                for index in &source.filter_columns {
                    if !columns.contains(index) {
                        columns.push(*index)
                    }
                }
                columns
            });
            let decode_paths = source.decode_paths.clone().map(|mut paths| {
                for index in &source.filter_columns {
                    paths.push(vec![source.component.schema.field(*index).name().clone()]);
                }
                paths
            });
            let schema = Arc::new(source.component.schema.project(&requested)?);
            let stream: futures::stream::BoxStream<'static, Result<RecordBatch>> =
                if let Some(rows) = source
                    .counts
                    .as_ref()
                    .as_ref()
                    .and_then(|c| c.get(part))
                    .copied()
                    .filter(|n| *n > 0)
                {
                    use vortex::{
                        VortexSessionDefault,
                        layout::{LayoutReader, scan::scan_builder::ScanBuilder},
                        session::VortexSession,
                    };
                    let session = VortexSession::default();
                    let reader = vail_storage::legacy::layout::LegacyLayoutReader::new(
                        source.component.clone(),
                        part,
                        rows,
                        projection,
                        source.batch_size,
                        decode_columns,
                    )
                    .map_err(external)?
                    .with_decode_paths(decode_paths.clone());
                    // Sequential within a file; DataFusion schedules files independently.
                    // A single split in flight also bounds the layout's range cache.
                    let projection_expr = vortex::expr::select(
                        requested
                            .iter()
                            .map(|&i| source.component.schema.field(i).name().as_str())
                            .collect::<Vec<_>>(),
                        vortex::expr::root(),
                    )
                    .bind(reader.dtype())
                    .map_err(external)?;
                    let filter = source
                        .filter
                        .map(|e| e.bind(reader.dtype()))
                        .transpose()
                        .map_err(external)?;
                    let indexed_range = if let (Some(bounds), Some(spec)) =
                        (source.key_range, source.component.index.clone())
                    {
                        let path = source.component.paths[part].clone();
                        Some(
                            tokio::task::spawn_blocking(
                                move || -> vail_storage::legacy::Result<std::ops::Range<u64>> {
                                    let mut index = vail_storage::legacy::index::IndexReader::open(
                                        spec, &path,
                                    )?;
                                    if index.rows != rows {
                                        return Err(
                                            "Hail index row count differs from partition metadata"
                                                .into(),
                                        );
                                    }
                                    index.integer_rows(&bounds)
                                },
                            )
                            .await
                            .map_err(external)?
                            .map_err(external)?,
                        )
                    } else {
                        None
                    };
                    let mut builder = ScanBuilder::new(session.clone(), Arc::new(reader));
                    if let Some(range) = indexed_range {
                        builder = builder.with_row_range(range);
                    }
                    let scan = sequential_legacy_scan(
                        builder
                            .with_projection(projection_expr)
                            .with_some_filter(filter),
                    )
                    .map_err(external)?;
                    Box::pin(scan.map_err(external).and_then(move |array| {
                        let result = vail_storage::legacy::layout::arrow_batch(
                            array,
                            schema.clone(),
                            &session,
                        )
                        .map_err(external);
                        futures::future::ready(result)
                    }))
                } else {
                    // Old exports without partition counts are still streamed, without
                    // an eager discovery pass just to establish a Vortex row count.
                    let (sender, receiver) = tokio::sync::mpsc::channel(1);
                    let worker = tokio::task::spawn_blocking(move || {
                        let run = || -> vail_storage::legacy::Result<()> {
                            let mut reader = source
                                .component
                                .reader(part, &projection)?
                                .with_decode_columns(decode_columns)
                                .with_decode_paths(decode_paths);
                            while !sender.is_closed() {
                                let Some(batch) = reader.next_batch(source.batch_size)? else {
                                    break;
                                };
                                if sender.blocking_send(Ok(batch)).is_err() {
                                    return Ok(());
                                }
                            }
                            if let Some(expected) =
                                source.counts.as_ref().as_ref().and_then(|c| c.get(part))
                            {
                                if reader.rows != *expected {
                                    return Err("Hail partition counts differ".into());
                                }
                            }
                            Ok(())
                        };
                        if let Err(e) = run() {
                            let _ = sender
                                .blocking_send(Err(DataFusionError::Execution(e.to_string())));
                        }
                    });
                    Box::pin(async_stream::try_stream! {
                        let mut receiver=receiver;
                        while let Some(batch)=receiver.recv().await {yield batch?;}
                        worker.await.map_err(external)?;
                    })
                };
            Ok(stream)
        }))
    }
}
// Pick a name absent from the declared schema, so shape demand cannot
// accidentally select a real legacy field, even with arbitrary quoted names.
fn shape_marker(schema: &arrow_schema::Schema) -> String {
    fn contains(ty: &arrow_schema::DataType, name: &str) -> bool {
        match ty {
            arrow_schema::DataType::Struct(fields) => fields
                .iter()
                .any(|f| f.name() == name || contains(f.data_type(), name)),
            arrow_schema::DataType::List(field)
            | arrow_schema::DataType::LargeList(field)
            | arrow_schema::DataType::FixedSizeList(field, _) => contains(field.data_type(), name),
            _ => false,
        }
    }
    let mut marker = "\0legacy_shape".to_string();
    while schema
        .fields()
        .iter()
        .any(|f| f.name() == &marker || contains(f.data_type(), &marker))
    {
        marker.push('_');
    }
    marker
}
fn external(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

/// Vortex's stream concurrency is per runtime worker, so even a value of one
/// schedules several ranges of the same file concurrently. Native row codecs
/// share one sequential cursor: poll the public split tasks strictly in order.
/// DataFusion still executes independent file sources concurrently.
fn sequential_legacy_scan(
    builder: vortex::layout::scan::scan_builder::ScanBuilder<vortex::array::ArrayRef>,
) -> vortex::error::VortexResult<
    futures::stream::BoxStream<'static, vortex::error::VortexResult<vortex::array::ArrayRef>>,
> {
    use futures::StreamExt;
    let tasks = builder.build()?;
    Ok(futures::stream::iter(tasks)
        .then(|task| task)
        .filter_map(|result| futures::future::ready(result.transpose()))
        .boxed())
}

/// Vortex partitions row/entry demands through its struct and list readers.
/// Old exports without cardinality metadata retain the bounded streaming path.
fn matrix_layout_stream(
    source: LegacySource,
    part: usize,
    projection: Vec<usize>,
    rows: u64,
) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
    use futures::TryStreamExt;
    use vortex::{
        VortexSessionDefault, layout::scan::scan_builder::ScanBuilder, session::VortexSession,
    };
    let session = VortexSession::default();
    let matrix = source.matrix.as_ref().unwrap();
    let schema = Arc::new(source.output_schema.project(&projection)?);
    let reader = vail_storage::legacy::matrix_layout::reader(
        source.component.clone(),
        matrix.entries.clone(),
        part,
        rows,
        matrix.ncols,
        source.output_schema.clone(),
        source.batch_size,
        source.decode_paths,
        session.clone(),
    )
    .map_err(external)?;
    // DataFusion may retain both physical root columns while nested demand
    // proves one is unused. Make that placeholder explicit in the Vortex
    // expression so its struct layout never requests the unused file child.
    let fields = projection
        .iter()
        .map(|&index| {
            let field = source.output_schema.field(index);
            let expression = if source
                .decode_columns
                .as_ref()
                .is_some_and(|columns| !columns.contains(&index))
            {
                use vortex::arrow::ArrowSessionExt;
                let dtype = session.arrow().from_arrow_field(field)?;
                vortex::expr::lit(vortex::scalar::Scalar::null(dtype))
            } else {
                vortex::expr::get_item(field.name().as_str(), vortex::expr::root())
            };
            Ok((field.name().clone(), expression))
        })
        .collect::<vortex::error::VortexResult<Vec<_>>>()
        .map_err(external)?;
    let expression = vortex::expr::pack(fields, vortex::dtype::Nullability::NonNullable)
        .bind(reader.dtype())
        .map_err(external)?;
    let scan = sequential_legacy_scan(
        ScanBuilder::new(session.clone(), reader).with_projection(expression),
    )
    .map_err(external)?;
    Ok(Box::pin(scan.map_err(external).and_then(move |array| {
        let result = vail_storage::legacy::layout::arrow_batch(array, schema.clone(), &session)
            .map_err(external);
        futures::future::ready(result)
    })))
}

fn matrix_stream(
    source: LegacySource,
    part: usize,
    projection: Vec<usize>,
) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
    let matrix = source.matrix.as_ref().unwrap();
    let entries = matrix.entries.clone();
    let matrix = matrix.clone();
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let handle = tokio::task::spawn_blocking(move || {
        let run = || -> vail_storage::legacy::Result<()> {
            let needs_rows = projection.contains(&0)
                && source
                    .decode_columns
                    .as_ref()
                    .is_none_or(|columns| columns.contains(&0));
            let needs_entries = projection.contains(&1)
                && source
                    .decode_columns
                    .as_ref()
                    .is_none_or(|columns| columns.contains(&1));
            let row_projection = if needs_rows {
                (0..source.component.schema.fields().len()).collect::<Vec<_>>()
            } else {
                vec![]
            };
            let row_paths = source.decode_paths.as_ref().map(|paths| {
                paths
                    .iter()
                    .filter(|p| p.first().is_some_and(|s| s == "__vail_matrix_row"))
                    .flat_map(|p| {
                        if p.len() == 1 {
                            source
                                .component
                                .schema
                                .fields()
                                .iter()
                                .map(|f| vec![f.name().clone()])
                                .collect::<Vec<_>>()
                        } else {
                            vec![p[1..].to_vec()]
                        }
                    })
                    .collect::<Vec<_>>()
            });
            let entry_root = entries.schema.field(0).name().clone();
            let entry_paths = source.decode_paths.as_ref().map(|paths| {
                paths
                    .iter()
                    .filter(|p| p.first().is_some_and(|s| s == "__vail_matrix_cells"))
                    .filter_map(|p| {
                        if p.len() == 1 {
                            return Some(vec![entry_root.clone()]);
                        }
                        if p[1] == "value" {
                            let mut path = vec![entry_root.clone()];
                            path.extend_from_slice(&p[2..]);
                            Some(path)
                        } else if p[1] == shape_marker(&source.output_schema) {
                            Some(vec![entry_root.clone(), p[1].clone()])
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            });
            let known_rows = source
                .counts
                .as_ref()
                .as_ref()
                .and_then(|c| c.get(part))
                .copied();
            // Hail stores row and entry streams separately. When the query has no
            // row values, partition cardinality can drive alignment without opening
            // or decompressing the row file at all.
            let mut row_reader = if row_projection.is_empty() && known_rows.is_some() {
                None
            } else {
                Some(
                    source
                        .component
                        .reader(part, &row_projection)?
                        .with_decode_paths(row_paths),
                )
            };
            let mut entry_reader = if needs_entries {
                Some(entries.reader(part, &[0])?.with_decode_paths(entry_paths))
            } else {
                None
            };
            let mut pending: Option<RecordBatch> = None;
            let mut count = 0u64;
            while !sender.is_closed() {
                let batch = if let Some(reader) = &mut row_reader {
                    let Some(batch) = reader.next_batch(source.batch_size)? else {
                        break;
                    };
                    batch
                } else {
                    let remaining = known_rows.unwrap().saturating_sub(count);
                    if remaining == 0 {
                        break;
                    }
                    let n = remaining.min(source.batch_size as u64) as usize;
                    RecordBatch::try_new_with_options(
                        Arc::new(arrow_schema::Schema::empty()),
                        vec![],
                        &arrow_array::RecordBatchOptions::new().with_row_count(Some(n)),
                    )?
                };
                let mut offset = 0;
                while offset < batch.num_rows() {
                    if let Some(reader) = &mut entry_reader
                        && pending.is_none()
                    {
                        pending = Some(
                            reader
                                .next_batch(source.batch_size)?
                                .ok_or("Hail row and entry partition counts differ")?,
                        );
                    }
                    let n = pending.as_ref().map_or(batch.num_rows() - offset, |b| {
                        b.num_rows().min(batch.num_rows() - offset)
                    });
                    let rows = if row_projection.is_empty() {
                        let schema = source.component.schema.clone();
                        RecordBatch::try_new_with_options(
                            schema.clone(),
                            schema
                                .fields()
                                .iter()
                                .map(|f| arrow_array::new_null_array(f.data_type(), n))
                                .collect(),
                            &arrow_array::RecordBatchOptions::new().with_row_count(Some(n)),
                        )?
                    } else {
                        batch.slice(offset, n)
                    };
                    let e = pending.as_ref().map(|b| b.slice(0, n));
                    let core = crate::legacy::matrix_batch(
                        rows,
                        e,
                        source.output_schema.clone(),
                        &matrix,
                    )?
                    .project(&projection)?;
                    if sender.blocking_send(Ok(core)).is_err() {
                        return Ok(());
                    }
                    pending = pending.take().and_then(|b| {
                        if b.num_rows() > n {
                            Some(b.slice(n, b.num_rows() - n))
                        } else {
                            None
                        }
                    });
                    offset += n;
                    count += n as u64;
                }
            }
            if sender.is_closed() {
                return Ok(());
            }
            if pending.is_some()
                || if let Some(reader) = &mut entry_reader {
                    reader.next_batch(1)?.is_some()
                } else {
                    false
                }
            {
                return Err("Hail row and entry partition counts differ".into());
            }
            if let Some(expected) = source.counts.as_ref().as_ref().and_then(|c| c.get(part))
                && count != *expected
            {
                return Err("Hail partition counts differ".into());
            }
            Ok(())
        };
        if let Err(e) = run() {
            let _ = sender.blocking_send(Err(external(e)));
        }
    });
    Ok(Box::pin(async_stream::try_stream! {
        let mut receiver=receiver;
        while let Some(batch)=receiver.recv().await {yield batch?;}
        handle.await.map_err(external)?;
    }))
}

fn vortex_filter(
    expr: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    schema: &arrow_schema::Schema,
) -> Option<vortex::expr::Expression> {
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
    };
    use vortex::expr::*;
    if let Some(c) = expr.downcast_ref::<Column>() {
        let field = schema.field_with_name(c.name()).ok()?;
        if !matches!(
            field.data_type(),
            arrow_schema::DataType::Int32
                | arrow_schema::DataType::Int64
                | arrow_schema::DataType::Boolean
                | arrow_schema::DataType::Utf8
        ) {
            return None;
        }
        return Some(get_item(c.name(), root()));
    }
    if let Some(l) = expr.downcast_ref::<Literal>() {
        return Some(match l.value() {
            ScalarValue::Int32(v) => lit(*v),
            ScalarValue::Int64(v) => lit(*v),
            ScalarValue::Boolean(v) => lit(*v),
            ScalarValue::Utf8(Some(v)) => lit(v.as_str()),
            _ => return None,
        });
    }
    if let Some(b) = expr.downcast_ref::<BinaryExpr>() {
        let l = vortex_filter(b.left(), schema)?;
        let r = vortex_filter(b.right(), schema)?;
        return Some(match b.op() {
            Operator::Eq => eq(l, r),
            Operator::NotEq => not_eq(l, r),
            Operator::Lt => lt(l, r),
            Operator::LtEq => lt_eq(l, r),
            Operator::Gt => gt(l, r),
            Operator::GtEq => gt_eq(l, r),
            Operator::And => and(l, r),
            Operator::Or => or(l, r),
            _ => return None,
        });
    }
    if let Some(n) = expr.downcast_ref::<NotExpr>() {
        return Some(not(vortex_filter(n.arg(), schema)?));
    }
    if let Some(n) = expr.downcast_ref::<IsNullExpr>() {
        return Some(is_null(vortex_filter(n.arg(), schema)?));
    }
    if let Some(n) = expr.downcast_ref::<IsNotNullExpr>() {
        return Some(is_not_null(vortex_filter(n.arg(), schema)?));
    }
    None
}

fn filter_columns(
    expr: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    schema: &arrow_schema::Schema,
    out: &mut std::collections::BTreeSet<usize>,
) {
    if let Some(c) = expr.downcast_ref::<datafusion_physical_expr::expressions::Column>()
        && let Ok(index) = schema.index_of(c.name())
    {
        out.insert(index);
    }
    for child in expr.children() {
        filter_columns(child, schema, out)
    }
}

/// Conservative file pruning from Hail's single-integer-key partition bounds.
/// Unsupported predicates and missing/nullable bounds always retain the file.
fn could_match(
    expr: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    bounds: &vail_storage::legacy::IntKeyBounds,
) -> bool {
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    let Some(binary) = expr.downcast_ref::<BinaryExpr>() else {
        return true;
    };
    if *binary.op() == Operator::And {
        return could_match(binary.left(), bounds) && could_match(binary.right(), bounds);
    }
    if *binary.op() == Operator::Or {
        return could_match(binary.left(), bounds) || could_match(binary.right(), bounds);
    }
    let literal = |e: &Arc<dyn datafusion_physical_expr::PhysicalExpr>| {
        e.downcast_ref::<Literal>().and_then(|l| match l.value() {
            ScalarValue::Int32(v) => v.map(i64::from),
            ScalarValue::Int64(v) => *v,
            _ => None,
        })
    };
    let column = |e: &Arc<dyn datafusion_physical_expr::PhysicalExpr>| {
        e.downcast_ref::<Column>()
            .is_some_and(|c| c.name() == bounds.key)
    };
    let (op, value) = if column(binary.left()) {
        (*binary.op(), literal(binary.right()))
    } else if column(binary.right()) {
        let op = match binary.op() {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            other => *other,
        };
        (op, literal(binary.left()))
    } else {
        return true;
    };
    let Some(value) = value else { return true };
    match op {
        Operator::Eq => bounds.min <= value && value <= bounds.max,
        Operator::NotEq => bounds.min != value || bounds.max != value,
        Operator::Lt => bounds.min < value,
        Operator::LtEq => bounds.min <= value,
        Operator::Gt => bounds.max > value,
        Operator::GtEq => bounds.max >= value,
        _ => true,
    }
}

fn narrow_integer_filter(
    expr: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    bounds: &vail_storage::legacy::IntKeyBounds,
) -> Option<vail_storage::legacy::IntKeyBounds> {
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    let Some(binary) = expr.downcast_ref::<BinaryExpr>() else {
        return Some(bounds.clone());
    };
    if *binary.op() == Operator::And {
        return narrow_integer_filter(
            binary.right(),
            &narrow_integer_filter(binary.left(), bounds)?,
        );
    }
    if *binary.op() == Operator::Or {
        return match (
            narrow_integer_filter(binary.left(), bounds),
            narrow_integer_filter(binary.right(), bounds),
        ) {
            (Some(a), Some(b)) => Some(vail_storage::legacy::IntKeyBounds {
                key: bounds.key.clone(),
                min: a.min.min(b.min),
                max: a.max.max(b.max),
            }),
            (a, None) | (None, a) => a,
        };
    }
    let literal = |e: &Arc<dyn datafusion_physical_expr::PhysicalExpr>| {
        e.downcast_ref::<Literal>().and_then(|l| match l.value() {
            ScalarValue::Int32(v) => v.map(i64::from),
            ScalarValue::Int64(v) => *v,
            _ => None,
        })
    };
    let column = |e: &Arc<dyn datafusion_physical_expr::PhysicalExpr>| {
        e.downcast_ref::<Column>()
            .is_some_and(|c| c.name() == bounds.key)
    };
    let (op, value) = if column(binary.left()) {
        (*binary.op(), literal(binary.right()))
    } else if column(binary.right()) {
        let op = match binary.op() {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            other => *other,
        };
        (op, literal(binary.left()))
    } else {
        return Some(bounds.clone());
    };
    let Some(value) = value else {
        return Some(bounds.clone());
    };
    let mut min = bounds.min as i128;
    let mut max = bounds.max as i128;
    let value = value as i128;
    match op {
        Operator::Eq => {
            min = min.max(value);
            max = max.min(value)
        }
        Operator::Gt => min = min.max(value + 1),
        Operator::GtEq => min = min.max(value),
        Operator::Lt => max = max.min(value - 1),
        Operator::LtEq => max = max.min(value),
        _ => {}
    }
    (min <= max).then(|| vail_storage::legacy::IntKeyBounds {
        key: bounds.key.clone(),
        min: min as i64,
        max: max as i64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, TryStreamExt};
    use std::{
        any::Any,
        ops::Range,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use vortex::{
        VortexSessionDefault,
        array::{ArrayRef, IntoArray, MaskFuture, VortexSessionExecute, arrays::ConstantArray},
        dtype::{DType, FieldMask, Nullability, PType},
        error::VortexResult,
        expr::BoundExpression,
        layout::{LayoutReader, RowSplits, SplitRange, scan::scan_builder::ScanBuilder},
        mask::Mask,
        session::VortexSession,
    };

    struct SequentialReader {
        name: Arc<str>,
        dtype: DType,
        active: Arc<AtomicUsize>,
    }
    impl LayoutReader for SequentialReader {
        fn name(&self) -> &Arc<str> {
            &self.name
        }
        fn dtype(&self) -> &DType {
            &self.dtype
        }
        fn row_count(&self) -> u64 {
            4
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn register_splits(
            &self,
            _: &[FieldMask],
            range: &SplitRange,
            splits: &mut RowSplits,
        ) -> VortexResult<()> {
            for row in range.row_range().start + 1..=range.row_range().end {
                splits.push(row + range.row_offset());
            }
            Ok(())
        }
        fn pruning_evaluation(
            &self,
            _: &Range<u64>,
            _: &BoundExpression,
            mask: Mask,
        ) -> VortexResult<MaskFuture> {
            Ok(MaskFuture::ready(mask))
        }
        fn filter_evaluation(
            &self,
            _: &Range<u64>,
            _: &BoundExpression,
            mask: MaskFuture,
        ) -> VortexResult<MaskFuture> {
            Ok(mask)
        }
        fn projection_evaluation(
            &self,
            range: &Range<u64>,
            expr: &BoundExpression,
            mask: MaskFuture,
        ) -> VortexResult<futures::future::BoxFuture<'static, VortexResult<ArrayRef>>> {
            let active = self.active.clone();
            let range = range.clone();
            let expr = expr.clone();
            Ok(async move {
                let mask = mask.await?;
                assert_eq!(
                    active.fetch_add(1, Ordering::SeqCst),
                    0,
                    "only one range of a native file may execute at once"
                );
                tokio::task::yield_now().await;
                let result = ConstantArray::new(range.start as i64, mask.true_count())
                    .into_array()
                    .apply_bound(&expr);
                active.fetch_sub(1, Ordering::SeqCst);
                result
            }
            .boxed())
        }
    }

    #[tokio::test]
    async fn legacy_file_splits_execute_sequentially_in_row_order() -> VortexResult<()> {
        let session = VortexSession::default();
        let reader = Arc::new(SequentialReader {
            name: "sequential legacy file".into(),
            dtype: DType::Primitive(PType::I64, Nullability::NonNullable),
            active: Arc::new(AtomicUsize::new(0)),
        });
        let chunks = sequential_legacy_scan(ScanBuilder::new(session.clone(), reader))?
            .try_collect::<Vec<_>>()
            .await?;
        let mut ctx = session.create_execution_ctx();
        let values = chunks
            .iter()
            .map(|array| {
                Ok(array
                    .execute_scalar(0, &mut ctx)?
                    .as_primitive()
                    .as_::<i64>()
                    .unwrap())
            })
            .collect::<VortexResult<Vec<_>>>()?;
        assert_eq!(values, vec![0, 1, 2, 3]);
        Ok(())
    }
}
