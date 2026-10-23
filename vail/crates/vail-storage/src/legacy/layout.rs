//! File-backed Vortex layout reader. Each execution owns its cursor and cache;
//! independent files never share a lock or execution queue.
use super::*;
use arrow_array::{Array, RecordBatch, StructArray};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::{ops::Range, sync::Mutex};
use vortex::{
    VortexSessionDefault,
    array::{ArrayRef, Canonical, IntoArray, MaskFuture, VortexSessionExecute},
    arrow::ArrowSessionExt,
    dtype::{DType, FieldMask},
    error::{VortexResult, vortex_err},
    expr::BoundExpression,
    layout::{LayoutReader, RowSplits, SplitRange},
    mask::Mask,
    session::VortexSession,
};
type SharedArray =
    Shared<BoxFuture<'static, std::result::Result<ArrayRef, Arc<vortex::error::VortexError>>>>;
struct Cursor {
    reader: Option<super::decode::PartReader>,
    range: Range<u64>,
    cached: Option<RecordBatch>,
    index: Option<super::index::IndexReader>,
}
pub struct LegacyLayoutReader {
    name: Arc<str>,
    component: Arc<Component>,
    part: usize,
    rows: u64,
    dtype: DType,
    schema: SchemaRef,
    projection: Vec<usize>,
    cursor: Arc<Mutex<Cursor>>,
    session: VortexSession,
    batch_size: u64,
    decode_columns: Option<Vec<usize>>,
    decode_paths: Option<Vec<Vec<String>>>,
    pending: Mutex<HashMap<(u64, u64), std::sync::Weak<SharedArray>>>,
}
impl LegacyLayoutReader {
    pub fn new(
        component: Arc<Component>,
        part: usize,
        rows: u64,
        projection: Vec<usize>,
        batch_size: usize,
        decode_columns: Option<Vec<usize>>,
    ) -> VortexResult<Self> {
        let session = VortexSession::default();
        let schema = Arc::new(
            component
                .schema
                .project(&projection)
                .map_err(|e| vortex_err!("{e}"))?,
        );
        let dtype = session.arrow().from_arrow_schema(&schema)?;
        Ok(Self {
            name: component.paths[part].to_string_lossy().into_owned().into(),
            component,
            part,
            rows,
            dtype,
            schema,
            projection,
            cursor: Arc::new(Mutex::new(Cursor {
                reader: None,
                range: 0..0,
                cached: None,
                index: None,
            })),
            session,
            batch_size: batch_size.max(1) as u64,
            decode_columns,
            decode_paths: None,
            pending: Mutex::new(HashMap::new()),
        })
    }
    pub fn with_decode_paths(mut self, paths: Option<Vec<Vec<String>>>) -> Self {
        self.decode_paths = paths;
        self
    }
    fn array(&self, range: Range<u64>) -> BoxFuture<'static, VortexResult<ArrayRef>> {
        // A scan may schedule filter conjuncts and projection independently.
        // Share decoding for each live range instead of evicting/reopening it
        // when another phase advances the sequential file cursor.
        let mut pending = self.pending.lock().expect("legacy range cache poisoned");
        pending.retain(|_, future| future.strong_count() != 0);
        let key = (range.start, range.end);
        let future = if let Some(future) = pending.get(&key).and_then(std::sync::Weak::upgrade) {
            future
        } else {
            let future = Arc::new(
                self.load_array(range)
                    .map(|r| r.map_err(Arc::new))
                    .boxed()
                    .shared(),
            );
            pending.insert(key, Arc::downgrade(&future));
            future
        };
        async move {
            future
                .as_ref()
                .clone()
                .await
                .map_err(|e| vortex_err!("{e}"))
        }
        .boxed()
    }
    fn load_array(&self, range: Range<u64>) -> BoxFuture<'static, VortexResult<ArrayRef>> {
        let cursor = self.cursor.clone();
        let component = self.component.clone();
        let part = self.part;
        let projection = self.projection.clone();
        let schema = self.schema.clone();
        let session = self.session.clone();
        let rows = self.rows;
        let decode_columns = self.decode_columns.clone();
        let decode_paths = self.decode_paths.clone();
        async move {
            let batch = tokio::task::spawn_blocking(move || -> Result<RecordBatch> {
                let mut c = cursor.lock().map_err(|_| "Hail decoder mutex poisoned")?;
                if c.range == range
                    && let Some(batch) = &c.cached
                {
                    return Ok(batch.clone());
                }
                let reposition = c.reader.as_ref().is_none_or(|r| r.rows != range.start);
                if reposition
                    && range.start > 0
                    && let Some(spec) = &component.index
                {
                    if c.index.is_none() {
                        c.index = Some(super::index::IndexReader::open(
                            spec.clone(),
                            &component.paths[part],
                        )?);
                    }
                    let index = c.index.as_mut().unwrap();
                    if index.rows != rows {
                        return Err("Hail index row count differs from partition metadata".into());
                    }
                    let offset = index.row_offset(range.start)?;
                    let mut reader = component
                        .reader(part, &projection)?
                        .with_decode_columns(decode_columns)
                        .with_decode_paths(decode_paths);
                    reader.seek(&component, range.start, offset)?;
                    c.reader = Some(reader);
                } else if c.reader.is_none() || c.reader.as_ref().unwrap().rows > range.start {
                    c.reader = Some(
                        component
                            .reader(part, &projection)?
                            .with_decode_columns(decode_columns)
                            .with_decode_paths(decode_paths),
                    );
                }
                let reader = c.reader.as_mut().unwrap();
                while reader.rows < range.start {
                    let remaining = (range.start - reader.rows).min(8192) as usize;
                    if reader.next_batch(remaining)?.is_none() {
                        return Err("Hail partition count exceeds file rows".into());
                    }
                }
                let mut batches = vec![];
                while reader.rows < range.end {
                    let remaining = (range.end - reader.rows).min(8192) as usize;
                    batches.push(
                        reader
                            .next_batch(remaining)?
                            .ok_or("Hail partition count exceeds file rows")?,
                    );
                }
                if range.end == rows && reader.next_batch(1)?.is_some() {
                    return Err("Hail partition contains more rows than partition counts".into());
                }
                let batch = if batches.len() == 1 {
                    batches.pop().unwrap()
                } else {
                    arrow_select::concat::concat_batches(&schema, batches.iter())?
                };
                c.range = range;
                c.cached = Some(batch.clone());
                Ok(batch)
            })
            .await
            .map_err(|e| vortex_err!("Hail decoder task failed: {e}"))?
            .map_err(|e| vortex_err!("{e}"))?;
            session
                .arrow()
                .from_arrow_record_batch(batch.clone(), batch.schema_ref())
        }
        .boxed()
    }
}
impl LayoutReader for LegacyLayoutReader {
    fn name(&self) -> &Arc<str> {
        &self.name
    }
    fn dtype(&self) -> &DType {
        &self.dtype
    }
    fn row_count(&self) -> u64 {
        self.rows
    }
    fn register_splits(
        &self,
        _: &[FieldMask],
        range: &SplitRange,
        splits: &mut RowSplits,
    ) -> VortexResult<()> {
        range.check_bounds(self.rows)?;
        let r = range.row_range();
        let mut end = r.start.saturating_add(self.batch_size).min(r.end);
        while end < r.end {
            splits.push(end + range.row_offset());
            end = end.saturating_add(self.batch_size).min(r.end);
        }
        splits.push(r.end + range.row_offset());
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
        range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<MaskFuture> {
        let array = self.array(range.clone());
        let expr = expr.clone();
        let session = self.session.clone();
        Ok(MaskFuture::new(mask.len(), async move {
            let mask = mask.await?;
            if mask.all_false() {
                return Ok(mask);
            }
            let array = array.await?.apply_bound(&expr)?;
            let mut ctx = session.create_execution_ctx();
            // As in Vortex's flat layout, let expressions push the selection
            // down before materializing expensive results for sparse masks.
            if mask.density() < 0.2 {
                let selected = array
                    .filter(mask.clone())?
                    .null_as_false()
                    .execute(&mut ctx)?;
                Ok(mask.intersect_by_rank(&selected))
            } else {
                let selected = array.null_as_false().execute(&mut ctx)?;
                Ok(mask & &selected)
            }
        }))
    }
    fn projection_evaluation(
        &self,
        range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<BoxFuture<'static, VortexResult<ArrayRef>>> {
        let array = self.array(range.clone());
        let expr = expr.clone();
        let dtype = self.dtype.clone();
        Ok(async move {
            let mask = mask.await?;
            if mask.all_false() {
                // A rejected split must not open a data file just to produce
                // an empty projection. The expression still determines dtype.
                return Canonical::empty(&dtype).into_array().apply_bound(&expr);
            }
            let array = array.await?;
            let array = if mask.all_true() {
                array
            } else {
                array.filter(mask)?
            };
            array.apply_bound(&expr)
        }
        .boxed())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
pub fn arrow_batch(
    array: ArrayRef,
    schema: SchemaRef,
    session: &VortexSession,
) -> VortexResult<RecordBatch> {
    let mut ctx = session.create_execution_ctx();
    let field = arrow_schema::Field::new(
        "",
        arrow_schema::DataType::Struct(schema.fields().clone()),
        false,
    );
    let arrow = super::packed_call::export(array, Some(&field), &mut ctx)?;
    let s = arrow
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| vortex_err!("legacy projection must be a struct"))?;
    RecordBatch::try_new_with_options(
        schema,
        s.columns().to_vec(),
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(s.len())),
    )
    .map_err(|e| vortex_err!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejected_mask_does_not_open_file() {
        let component = Arc::new(Component {
            schema: Arc::new(Schema::empty()),
            dtype: Type::Struct { r#struct: vec![] },
            encoding: Encoding {
                kind: "EBaseStruct".into(),
                required: true,
                fields: vec![],
                rank: 0,
            },
            buffer: Value::Null,
            paths: vec![std::path::PathBuf::from("/missing/legacy-mask-test")],
            int_key_bounds: vec![],
            index: None,
        });
        let reader = LegacyLayoutReader::new(component, 0, 4, vec![], 4, None).unwrap();
        let rejected = MaskFuture::ready(Mask::new_false(4));
        let predicate = vortex::expr::lit(true).bind(reader.dtype()).unwrap();
        let mask = reader
            .filter_evaluation(&(0..4), &predicate, rejected.clone())
            .unwrap()
            .await
            .unwrap();
        assert!(mask.all_false());
        let projection = vortex::expr::root().bind(reader.dtype()).unwrap();
        let array = reader
            .projection_evaluation(&(0..4), &projection, rejected)
            .unwrap()
            .await
            .unwrap();
        assert_eq!(array.len(), 0);
        assert_eq!(array.dtype(), reader.dtype());
    }
}
