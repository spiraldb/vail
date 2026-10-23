//! Matrix files composed with Vortex's struct and list layouts. Rows and entry
//! values have independent native cursors; list offsets come from the shared
//! column dimension and never require opening an entry file.
use super::{Component, Result, decode, index};
use arrow_array::{
    Array, ArrayRef as ArrowArrayRef, ListArray, RecordBatch, StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, SchemaRef};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::{
    any::Any,
    collections::HashMap,
    fmt,
    ops::Range,
    sync::{Arc, Mutex, Weak},
};
use vortex::{
    array::{
        ArrayRef, Canonical, EmptyMetadata, IntoArray, MaskFuture, VortexSessionExecute,
        arrays::ConstantArray,
    },
    arrow::ArrowSessionExt,
    dtype::{DType, FieldMask, Nullability, PType},
    error::{VortexResult, vortex_err},
    expr::BoundExpression,
    layout::{
        Layout, LayoutChildType, LayoutDeserializeArgs, LayoutId, LayoutParts, LayoutReader,
        LayoutReaderContext, LayoutReaderRef, LayoutRef, RowSplits, SplitRange, VTable,
        layout_children,
        layouts::{list::ListLayout, struct_::StructLayout},
        segments::{SegmentFuture, SegmentId, SegmentSource},
    },
    mask::Mask,
    session::{VortexSession, registry::CachedId},
};

const ROW: &str = "__vail_matrix_row";
const CELLS: &str = "__vail_matrix_cells";
// Keep each matrix split bounded by entry work, including queries with wide rows.
const TARGET_SPLIT_ELEMENTS: u64 = 262_144;

type SharedArray =
    Shared<BoxFuture<'static, std::result::Result<ArrayRef, Arc<vortex::error::VortexError>>>>;

/// Construct one independently schedulable matrix-file layout. `decode_paths`
/// is the fixed union required by every filter and projection in this scan.
/// It must not change between expressions sharing this reader's range cache.
#[allow(clippy::too_many_arguments)]
pub fn reader(
    rows: Arc<Component>,
    entries: Arc<Component>,
    part: usize,
    row_count: u64,
    ncols: usize,
    schema: SchemaRef,
    batch_size: usize,
    decode_paths: Option<Vec<Vec<String>>>,
    session: VortexSession,
) -> VortexResult<LayoutReaderRef> {
    let ncols = u64::try_from(ncols).map_err(|e| vortex_err!("{e}"))?;
    let element_count = row_count
        .checked_mul(ncols)
        .ok_or_else(|| vortex_err!("Hail matrix entry dimension overflow"))?;
    let offset_count = row_count
        .checked_add(1)
        .ok_or_else(|| vortex_err!("Hail matrix row dimension overflow"))?;
    let row_field = schema
        .field_with_name(ROW)
        .map_err(|e| vortex_err!("{e}"))?
        .clone();
    let cells_field = schema
        .field_with_name(CELLS)
        .map_err(|e| vortex_err!("{e}"))?
        .clone();
    let DataType::List(slot_field) = cells_field.data_type() else {
        return Err(vortex_err!("Hail matrix cells must be a list"));
    };
    let DataType::Struct(slot_fields) = slot_field.data_type() else {
        return Err(vortex_err!("Hail matrix slots must be structs"));
    };
    if slot_fields.len() != 1 || slot_fields[0].name() != "value" {
        return Err(vortex_err!("Hail matrix slots require only a value field"));
    }
    if entries.schema.fields().len() != 1 {
        return Err(vortex_err!(
            "Hail matrix entry component must contain one array"
        ));
    }
    let row_paths = decode_paths.as_ref().map(|paths| {
        paths
            .iter()
            .filter(|p| p.first().is_some_and(|f| f == ROW))
            .map(|p| p[1..].to_vec())
            .collect::<Vec<_>>()
    });
    let entry_root = entries.schema.field(0).name();
    let entry_paths = decode_paths.as_ref().map(|paths| {
        paths
            .iter()
            .filter(|p| p.first().is_some_and(|f| f == CELLS))
            .filter_map(|p| {
                let mut path = vec![entry_root.clone()];
                if p.len() == 1 {
                    return Some(path);
                }
                if p[1] == "value" {
                    path.extend_from_slice(&p[2..]);
                } else {
                    // Shape/validity demand uses the caller's collision-free
                    // marker, which cannot select a real entry field.
                    path.extend_from_slice(&p[1..]);
                }
                Some(path)
            })
            .collect::<Vec<_>>()
    });
    let split_rows = (batch_size.max(1) as u64).min((TARGET_SPLIT_ELEMENTS / ncols.max(1)).max(1));
    let row_dtype = session.arrow().from_arrow_field(&row_field)?;
    let row_reader = NativeReader::file(
        rows,
        part,
        row_count,
        row_count,
        row_field,
        row_paths,
        FileShape::Rows,
        split_rows,
        session.clone(),
    )?;
    let element_reader = NativeReader::file(
        entries,
        part,
        row_count,
        element_count,
        slot_field.as_ref().clone(),
        entry_paths,
        FileShape::Elements { ncols },
        split_rows.saturating_mul(ncols).max(1),
        session.clone(),
    )?;
    let element_layout = leaf(Arc::new(element_reader));
    let offsets = leaf(Arc::new(NativeReader::synthetic(
        "Hail matrix offsets",
        offset_count,
        DType::Primitive(PType::U64, Nullability::NonNullable),
        Source::Offsets { ncols },
        split_rows,
        session.clone(),
    )));
    let cells_dtype = session.arrow().from_arrow_field(&cells_field)?;
    let validity = cells_dtype.is_nullable().then(|| {
        leaf(Arc::new(NativeReader::synthetic(
            "Hail matrix list validity",
            row_count,
            DType::Bool(Nullability::NonNullable),
            Source::Valid,
            split_rows,
            session.clone(),
        )))
    });
    let cells = ListLayout::new(cells_dtype, element_layout, offsets, validity).into_layout();
    debug_assert_eq!(row_dtype, row_reader.dtype);
    let root = StructLayout::new(
        row_count,
        session.arrow().from_arrow_schema(&schema)?,
        vec![leaf(Arc::new(row_reader)), cells],
    );
    root.new_reader(
        "Hail matrix".into(),
        Arc::new(NoSegments),
        &session,
        &LayoutReaderContext::new(),
    )
}

struct NoSegments;
impl SegmentSource for NoSegments {
    fn request(&self, _: SegmentId) -> SegmentFuture {
        async {
            Err(vortex_err!(
                "native Hail matrix layouts do not use Vortex segments"
            ))
        }
        .boxed()
    }
}

// Ephemeral leaf layouts let the unmodified Vortex struct/list readers route
// expressions and masks into native files. These nodes are never persisted.
#[derive(Clone, Debug)]
struct Native;
#[derive(Clone)]
struct NativeData(LayoutReaderRef);
impl fmt::Debug for NativeData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NativeData").field(self.0.name()).finish()
    }
}
impl VTable for Native {
    type LayoutData = NativeData;
    type Metadata = EmptyMetadata;
    fn id(&self) -> LayoutId {
        static ID: CachedId = CachedId::new("vail.hail-native-matrix");
        *ID
    }
    fn metadata(_: &Layout<Self>) -> EmptyMetadata {
        EmptyMetadata
    }
    fn deserialize(
        &self,
        _: &LayoutDeserializeArgs<'_>,
        _: &EmptyMetadata,
    ) -> VortexResult<NativeData> {
        Err(vortex_err!(
            "native Hail matrix layouts are execution-local"
        ))
    }
    fn child_dtype(_: &Layout<Self>, _: usize) -> VortexResult<DType> {
        Err(vortex_err!("native Hail matrix leaves have no children"))
    }
    fn child_type(_: &Layout<Self>, _: usize) -> LayoutChildType {
        unreachable!("native Hail matrix leaves have no children")
    }
    fn new_reader(
        layout: &Layout<Self>,
        _: Arc<str>,
        _: Arc<dyn SegmentSource>,
        _: &VortexSession,
        _: &LayoutReaderContext,
    ) -> VortexResult<LayoutReaderRef> {
        Ok(layout.data().0.clone())
    }
}
fn leaf(reader: LayoutReaderRef) -> LayoutRef {
    LayoutParts::new(
        Native,
        reader.dtype().clone(),
        reader.row_count(),
        vec![],
        layout_children(vec![]),
        NativeData(reader),
    )
    .into_layout()
}

#[derive(Clone, Copy)]
enum FileShape {
    Rows,
    Elements { ncols: u64 },
}
#[derive(Clone)]
enum Source {
    File {
        file: Arc<NativeFile>,
        shape: FileShape,
    },
    Offsets {
        ncols: u64,
    },
    Valid,
}
struct NativeFile {
    component: Arc<Component>,
    part: usize,
    row_count: u64,
    shape: FileShape,
    projection: Vec<usize>,
    decode_paths: Option<Vec<Vec<String>>>,
    output_field: Field,
    cursor: Mutex<Cursor>,
}
struct Cursor {
    reader: Option<decode::PartReader>,
    index: Option<index::IndexReader>,
    range: Range<u64>,
    cached: Option<RecordBatch>,
}
struct NativeReader {
    name: Arc<str>,
    dtype: DType,
    row_count: u64,
    split_rows: u64,
    source: Source,
    session: VortexSession,
    pending: Mutex<HashMap<(u64, u64), Weak<SharedArray>>>,
}
impl NativeReader {
    #[allow(clippy::too_many_arguments)]
    fn file(
        component: Arc<Component>,
        part: usize,
        file_rows: u64,
        row_count: u64,
        output_field: Field,
        decode_paths: Option<Vec<Vec<String>>>,
        shape: FileShape,
        split_rows: u64,
        session: VortexSession,
    ) -> VortexResult<Self> {
        let name = component
            .paths
            .get(part)
            .ok_or_else(|| vortex_err!("Hail part index out of bounds"))?
            .to_string_lossy()
            .into_owned()
            .into();
        let dtype = session.arrow().from_arrow_field(&output_field)?;
        let projection = (0..component.schema.fields().len()).collect();
        let file = Arc::new(NativeFile {
            component,
            part,
            row_count: file_rows,
            shape,
            projection,
            decode_paths,
            output_field,
            cursor: Mutex::new(Cursor {
                reader: None,
                index: None,
                range: 0..0,
                cached: None,
            }),
        });
        Ok(Self {
            name,
            dtype,
            row_count,
            split_rows,
            source: Source::File { file, shape },
            session,
            pending: Mutex::new(HashMap::new()),
        })
    }
    fn synthetic(
        name: &str,
        row_count: u64,
        dtype: DType,
        source: Source,
        split_rows: u64,
        session: VortexSession,
    ) -> Self {
        Self {
            name: name.into(),
            dtype,
            row_count,
            split_rows,
            source,
            session,
            pending: Mutex::new(HashMap::new()),
        }
    }
    fn array(&self, range: Range<u64>) -> BoxFuture<'static, VortexResult<ArrayRef>> {
        let mut pending = self
            .pending
            .lock()
            .expect("Hail matrix range cache poisoned");
        pending.retain(|_, f| f.strong_count() != 0);
        let key = (range.start, range.end);
        let future = if let Some(f) = pending.get(&key).and_then(Weak::upgrade) {
            f
        } else {
            let f = Arc::new(
                self.load_array(range)
                    .map(|r| r.map_err(Arc::new))
                    .boxed()
                    .shared(),
            );
            pending.insert(key, Arc::downgrade(&f));
            f
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
        let source = self.source.clone();
        let session = self.session.clone();
        let dtype = self.dtype.clone();
        let row_count = self.row_count;
        async move {
            if range.start > range.end || range.end > row_count {
                return Err(vortex_err!("Hail matrix child range out of bounds"));
            }
            let len = usize::try_from(range.end - range.start).map_err(|e| vortex_err!("{e}"))?;
            if len == 0 {
                return Ok(Canonical::empty(&dtype).into_array());
            }
            match source {
                Source::Valid => Ok(ConstantArray::new(true, len).into_array()),
                Source::Offsets { ncols } => {
                    let values = UInt64Array::from_iter_values(range.map(|i| i * ncols));
                    session.arrow().from_arrow_array(Arc::new(values), false)
                }
                Source::File { file, shape } => {
                    let output_field = file.output_field.clone();
                    let array = tokio::task::spawn_blocking(move || -> Result<ArrowArrayRef> {
                        let row_range = match shape {
                            FileShape::Rows => range.clone(),
                            FileShape::Elements { ncols } => {
                                if ncols == 0 {
                                    return Err("nonempty range in zero-column matrix".into());
                                }
                                range.start / ncols..range.end.div_ceil(ncols)
                            }
                        };
                        let batch = file.read_range(row_range.clone())?;
                        match shape {
                            FileShape::Rows => Ok(Arc::new(StructArray::from(batch))),
                            FileShape::Elements { ncols } => {
                                let entries = batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<ListArray>()
                                    .ok_or("invalid Hail matrix entry array")?;
                                for row in 0..batch.num_rows() {
                                    if entries.is_null(row)
                                        || entries.value_length(row) as u64 != ncols
                                    {
                                        return Err(
                                            "Hail entry array length differs from column count"
                                                .into(),
                                        );
                                    }
                                }
                                let first = entries.value_offsets()[0] as u64 + range.start
                                    - row_range.start * ncols;
                                let values = entries.values().slice(usize::try_from(first)?, len);
                                let DataType::Struct(fields) = file.output_field.data_type() else {
                                    return Err("invalid Hail slot type".into());
                                };
                                // The payload keeps the Hail entry struct's validity;
                                // a valid all-null payload remains a present entry.
                                Ok(Arc::new(StructArray::try_new_with_length(
                                    fields.clone(),
                                    vec![values],
                                    None,
                                    len,
                                )?))
                            }
                        }
                    })
                    .await
                    .map_err(|e| vortex_err!("Hail matrix decoder task failed: {e}"))?
                    .map_err(|e| vortex_err!("{e}"))?;
                    session.arrow().from_arrow_array(array, &output_field)
                }
            }
        }
        .boxed()
    }
}
impl NativeFile {
    fn read_range(&self, range: Range<u64>) -> Result<RecordBatch> {
        let mut cursor = self
            .cursor
            .lock()
            .map_err(|_| "Hail matrix cursor poisoned")?;
        if cursor.range == range
            && let Some(batch) = &cursor.cached
        {
            return Ok(batch.clone());
        }
        let reposition = cursor.reader.as_ref().is_none_or(|r| r.rows != range.start);
        if reposition
            && range.start > 0
            && let Some(spec) = &self.component.index
        {
            if cursor.index.is_none() {
                cursor.index = Some(index::IndexReader::open(
                    spec.clone(),
                    &self.component.paths[self.part],
                )?);
            }
            let index = cursor.index.as_mut().unwrap();
            if index.rows != self.row_count {
                return Err("Hail index row count differs from matrix metadata".into());
            }
            let offset = index.row_offset(range.start)?;
            let mut reader = self
                .component
                .reader(self.part, &self.projection)?
                .with_decode_paths(self.decode_paths.clone());
            reader.seek(&self.component, range.start, offset)?;
            cursor.reader = Some(reader);
        } else if cursor.reader.is_none() || cursor.reader.as_ref().unwrap().rows > range.start {
            cursor.reader = Some(
                self.component
                    .reader(self.part, &self.projection)?
                    .with_decode_paths(self.decode_paths.clone()),
            );
        }
        let short_count = match self.shape {
            FileShape::Rows => "Hail matrix row count exceeds file rows",
            FileShape::Elements { .. } => "Hail row and entry partition counts differ",
        };
        let reader = cursor.reader.as_mut().unwrap();
        while reader.rows < range.start {
            let n = usize::try_from((range.start - reader.rows).min(8192))?;
            if reader.next_batch(n)?.is_none() {
                return Err(short_count.into());
            }
        }
        let mut batches = vec![];
        while reader.rows < range.end {
            let n = usize::try_from((range.end - reader.rows).min(8192))?;
            batches.push(reader.next_batch(n)?.ok_or(short_count)?);
        }
        if range.end == self.row_count && reader.next_batch(1)?.is_some() {
            return Err(match self.shape {
                FileShape::Rows => "Hail matrix file contains more rows than partition metadata",
                FileShape::Elements { .. } => "Hail row and entry partition counts differ",
            }
            .into());
        }
        let batch = if batches.len() == 1 {
            batches.pop().unwrap()
        } else {
            arrow_select::concat::concat_batches(&self.component.schema, batches.iter())?
        };
        cursor.range = range;
        cursor.cached = Some(batch.clone());
        Ok(batch)
    }
}
impl LayoutReader for NativeReader {
    fn name(&self) -> &Arc<str> {
        &self.name
    }
    fn dtype(&self) -> &DType {
        &self.dtype
    }
    fn row_count(&self) -> u64 {
        self.row_count
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
        range.check_bounds(self.row_count)?;
        let r = range.row_range();
        let mut end = r.start.saturating_add(self.split_rows).min(r.end);
        while end < r.end {
            splits.push(end + range.row_offset());
            end = end.saturating_add(self.split_rows).min(r.end);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::Encoding;
    use arrow_array::Int32Array;
    use arrow_schema::Schema;
    use std::path::PathBuf;
    use vail_execution::expressions::{PrimitiveType, Type};
    use vortex::{
        VortexSessionDefault,
        expr::{get_item, list_length, root},
    };

    fn encoding(kind: &str, required: bool, fields: Vec<(String, Encoding)>) -> Encoding {
        Encoding {
            kind: kind.into(),
            required,
            fields,
            rank: 0,
        }
    }
    fn components(
        row_path: PathBuf,
        entry_path: PathBuf,
    ) -> (Arc<Component>, Arc<Component>, SchemaRef) {
        let int = Type::Primitive(PrimitiveType::Int32);
        let row_type = Type::Struct {
            r#struct: vec![("x".into(), int.clone())],
        };
        let entry_type = Type::Struct {
            r#struct: vec![("x".into(), int)],
        };
        let entries_type = Type::Struct {
            r#struct: vec![(
                "entries".into(),
                Type::Array {
                    array: Box::new(entry_type.clone()),
                },
            )],
        };
        let make = |path, dtype: Type, encoding| {
            let DataType::Struct(fields) = dtype.to_arrow().unwrap() else {
                unreachable!()
            };
            Arc::new(Component {
                schema: Arc::new(Schema::new(fields)),
                dtype,
                encoding,
                buffer: serde_json::json!({"name":"StreamBufferSpec"}),
                paths: vec![path],
                int_key_bounds: vec![],
                index: None,
            })
        };
        let rows = make(
            row_path,
            row_type.clone(),
            encoding(
                "EBaseStruct",
                true,
                vec![("x".into(), encoding("EInt32", true, vec![]))],
            ),
        );
        let entries = make(
            entry_path,
            entries_type,
            encoding(
                "EBaseStruct",
                true,
                vec![(
                    "entries".into(),
                    encoding(
                        "EArray",
                        true,
                        vec![(
                            "element".into(),
                            encoding(
                                "EBaseStruct",
                                false,
                                vec![("x".into(), encoding("EInt32", false, vec![]))],
                            ),
                        )],
                    ),
                )],
            ),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new(ROW, row_type.to_arrow().unwrap(), true),
            Field::new(
                CELLS,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![Field::new("value", entry_type.to_arrow().unwrap(), true)].into(),
                    ),
                    true,
                ))),
                true,
            ),
        ]));
        (rows, entries, schema)
    }

    #[tokio::test]
    async fn list_length_uses_metadata_without_opening_either_file() -> VortexResult<()> {
        let (rows, entries, schema) = components(
            "/missing/matrix-rows".into(),
            "/missing/matrix-entries".into(),
        );
        let session = VortexSession::default();
        let reader = reader(rows, entries, 0, 4, 3, schema, 2, None, session.clone())?;
        let expr = list_length(get_item(CELLS, root())).bind(reader.dtype())?;
        let result = reader
            .projection_evaluation(&(1..4), &expr, MaskFuture::new_true(3))?
            .await?;
        let mut ctx = session.create_execution_ctx();
        for i in 0..3 {
            assert_eq!(
                result
                    .execute_scalar(i, &mut ctx)?
                    .as_primitive()
                    .as_::<u64>(),
                Some(3)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn entry_projection_keeps_validity_and_never_opens_rows() -> VortexResult<()> {
        let dir = tempfile::tempdir().unwrap();
        let entry_path = dir.path().join("entries");
        // One required array with 3 nullable structs: valid all-null, missing,
        // and valid x=7. Optional bitmaps encode missing values as set bits.
        let mut data = vec![1];
        data.extend_from_slice(&3i32.to_le_bytes());
        data.extend_from_slice(&[0b010, 1, 0]);
        data.extend_from_slice(&7i32.to_le_bytes());
        data.push(0);
        std::fs::write(&entry_path, data).unwrap();
        let (rows, entries, schema) = components(dir.path().join("missing-rows"), entry_path);
        let session = VortexSession::default();
        let reader = reader(
            rows,
            entries,
            0,
            1,
            3,
            schema.clone(),
            4,
            None,
            session.clone(),
        )?;
        let expr = get_item(CELLS, root()).bind(reader.dtype())?;
        let result = reader
            .projection_evaluation(&(0..1), &expr, MaskFuture::new_true(1))?
            .await?;
        let mut ctx = session.create_execution_ctx();
        let arrow = session
            .arrow()
            .execute_arrow(result, Some(schema.field(1)), &mut ctx)?;
        let list = arrow.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_length(0), 3);
        let slots = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(slots.num_columns(), 1);
        let entries = slots
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(entries.is_valid(0));
        assert!(entries.column(0).is_null(0));
        assert!(entries.is_null(1));
        assert_eq!(
            entries
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(2),
            7
        );
        Ok(())
    }

    fn write_entries(path: &std::path::Path, rows: &[Vec<i32>]) {
        let mut data = vec![];
        for row in rows {
            data.push(1);
            data.extend_from_slice(&(row.len() as i32).to_le_bytes());
            data.extend(std::iter::repeat_n(0, row.len().div_ceil(8)));
            for value in row {
                data.push(0); // The nullable x field is present.
                data.extend_from_slice(&value.to_le_bytes());
            }
        }
        data.push(0);
        std::fs::write(path, data).unwrap();
    }

    #[tokio::test]
    async fn value_projection_rejects_wrong_entry_array_length() -> VortexResult<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("entries");
        write_entries(&path, &[vec![11, 12]]);
        let (rows, entries, schema) = components(dir.path().join("missing-rows"), path);
        let session = VortexSession::default();
        let reader = reader(rows, entries, 0, 1, 3, schema, 2, None, session)?;
        let expr = get_item(CELLS, root()).bind(reader.dtype())?;
        let error = reader
            .projection_evaluation(&(0..1), &expr, MaskFuture::new_true(1))?
            .await
            .err()
            .expect("malformed matrix entry length must fail");
        assert!(
            error
                .to_string()
                .contains("entry array length differs from column count")
        );
        Ok(())
    }

    #[tokio::test]
    async fn entry_partition_count_errors_keep_legacy_diagnostic() -> VortexResult<()> {
        for (expected, actual) in [(2, 1), (1, 2)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("entries");
            write_entries(&path, &vec![vec![11, 12, 13]; actual]);
            let (rows, entries, schema) = components(dir.path().join("missing-rows"), path);
            let session = VortexSession::default();
            let reader = reader(rows, entries, 0, expected, 3, schema, 2, None, session)?;
            let expr = get_item(CELLS, root()).bind(reader.dtype())?;
            let error = reader
                .projection_evaluation(
                    &(0..expected),
                    &expr,
                    MaskFuture::new_true(expected as usize),
                )?
                .await
                .err()
                .expect("mismatched matrix partition count must fail");
            assert!(
                error
                    .to_string()
                    .contains("row and entry partition counts differ")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn bounded_sparse_scan_rebases_entries_across_splits() -> VortexResult<()> {
        use futures::TryStreamExt;
        use vortex::{
            expr::{eq, lit, or},
            layout::scan::{scan_builder::ScanBuilder, split_by::SplitBy},
        };
        let dir = tempfile::tempdir().unwrap();
        let row_path = dir.path().join("rows");
        let entry_path = dir.path().join("entries");
        let mut row_data = vec![];
        for row in 0..10i32 {
            row_data.push(1);
            row_data.extend_from_slice(&row.to_le_bytes());
        }
        row_data.push(0);
        std::fs::write(&row_path, row_data).unwrap();
        let expected = (0..10i32)
            .map(|row| (0..3).map(|col| row * 10 + col).collect())
            .collect::<Vec<Vec<i32>>>();
        write_entries(&entry_path, &expected);
        let (rows, entries, schema) = components(row_path, entry_path);
        let session = VortexSession::default();
        let reader = reader(
            rows,
            entries,
            0,
            10,
            3,
            schema.clone(),
            2,
            None,
            session.clone(),
        )?;
        let row = get_item("x", get_item(ROW, root()));
        let predicate = or(
            eq(row.clone(), lit(2i32)),
            or(eq(row.clone(), lit(4i32)), eq(row, lit(7i32))),
        )
        .bind(reader.dtype())?;
        let projection = get_item(CELLS, root()).bind(reader.dtype())?;
        let mut scan = Box::pin(
            ScanBuilder::new(session.clone(), reader.clone())
                .with_row_range(1..9)
                .with_projection(projection.clone())
                .with_filter(predicate)
                .with_split_by(SplitBy::RowCount(3))
                .with_concurrency(1)
                .with_ordered(true)
                .into_array_stream()?,
        );
        let mut selected = vec![];
        let mut chunks = 0;
        while let Some(array) = scan.try_next().await? {
            let mut ctx = session.create_execution_ctx();
            let arrow = session
                .arrow()
                .execute_arrow(array, Some(schema.field(1)), &mut ctx)?;
            let list = arrow.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list.value_offsets()[0], 0);
            let slots = list
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let entries = slots
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let values = entries
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..list.len() {
                let start = list.value_offsets()[row] as usize;
                let end = list.value_offsets()[row + 1] as usize;
                selected.push(values.values()[start..end].to_vec());
            }
            chunks += 1;
        }
        assert!(chunks > 1);
        assert_eq!(
            selected,
            vec![
                expected[2].clone(),
                expected[4].clone(),
                expected[7].clone()
            ]
        );
        // A second, earlier sparse range forces the non-indexed native cursor
        // to restart while Vortex crops leading/trailing unselected lists.
        let array = reader
            .projection_evaluation(
                &(2..8),
                &projection,
                MaskFuture::ready(Mask::from_iter([false, true, false, false, true, false])),
            )?
            .await?;
        let mut ctx = session.create_execution_ctx();
        let arrow = session
            .arrow()
            .execute_arrow(array, Some(schema.field(1)), &mut ctx)?;
        let list = arrow.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_offsets(), &[0, 3, 6]);
        let slots = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let entries = slots
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let values = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), [30, 31, 32, 60, 61, 62]);
        Ok(())
    }
}
