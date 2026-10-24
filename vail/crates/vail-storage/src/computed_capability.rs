//! Conservative metadata-only proofs for closed computed scan operations.
//!
//! This is deliberately a closed proof, not a codec-name blacklist. Unsupported
//! valid metadata returns None and the caller retains its original DF expression.
use crate::{Result, SnapshotReadPlan, StorageError};
use futures::{FutureExt, future::BoxFuture};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use vortex::VortexSessionDefault;
use vortex::array::ArrayVTable;
use vortex::array::arrays::bool::BoolMetadata;
use vortex::array::arrays::{Bool, Constant, Primitive};
use vortex::array::buffer::BufferHandle;
use vortex::array::memory::MemorySessionExt;
use vortex::array::serde::SerializedArray;
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::{Alignment, ByteBuffer};
use vortex::dtype::{DType, Nullability, PType};
use vortex::encodings::fastlanes::{BitPacked, BitPackedMetadata};
use vortex::encodings::sequence::Sequence;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::{CoalesceConfig, VortexReadAt, session::RuntimeSessionExt, std_file::FileReadAt};
use vortex::layout::LayoutRef;
use vortex::layout::layouts::{
    chunked::Chunked, dict::Dict, flat::Flat, list::List, struct_::Struct, zoned::Zoned,
};
use vortex::layout::segments::SegmentSource;
use vortex::session::{VortexSession, registry::ReadContext};

// The selected small dictionary, its nullable bitmap, prepared lookup records,
// and decoder output/scratch must fit this explicit fixed preparation budget.
const DICTIONARY_PREPARATION_BYTES: usize = 64 * 1024;
const MAX_DICTIONARY_VALUES: usize = (DICTIONARY_PREPARATION_BYTES - 8 * 1024) / 32;
const MAX_TREE_BYTES: usize = 64 * 1024;
const MAX_QUERY_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_DEPTH: usize = 48;
const MAX_PREFLIGHT_FILES: usize = 8;
const MAX_CONSTANT_SEGMENT_BYTES: usize = 256;

/// Operand semantics selected once by the closed execution-planner translator.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComputedOperand {
    DenseI32Sum(Vec<String>),
    ListLength(Vec<String>),
}
impl ComputedOperand {
    fn path(&self) -> &[String] {
        match self {
            Self::DenseI32Sum(path) | Self::ListLength(path) => path,
        }
    }
}

/// Immutable proof tied to one snapshot, exact logical operands and source ranges.
#[derive(Debug)]
pub struct ComputedProjectionCertificate {
    pub(crate) plan: std::sync::Weak<SnapshotReadPlan>,
    pub(crate) partitions: Vec<Range<u64>>,
    pub(crate) operands: Vec<ComputedOperand>,
    // Retain explicit physical-domain accounting; this is not a claim about
    // arbitrary filtered selections or arbitrary native Vortex encodings.
    pub(crate) metadata_bytes: usize,
    pub(crate) domains: Vec<FileDomain>,
}

/// One immutable computed request bound to its physical-domain proof.
/// Fields are inaccessible across the storage/execution crate boundary, so an
/// execution caller cannot replace its expression or result schema later.
#[derive(Debug)]
pub struct PreparedComputedProjection {
    pub(crate) certificate: Arc<ComputedProjectionCertificate>,
    pub(crate) expression: vortex::expr::Expression,
    pub(crate) output_schema: Arc<arrow_schema::Schema>,
}
impl PreparedComputedProjection {
    pub fn expression(&self) -> &vortex::expr::Expression {
        &self.expression
    }
}

#[derive(Debug)]
pub(crate) struct FileDomain {
    path: String,
    bytes: u64,
    rows: u64,
    operands: Vec<(ComputedOperand, Vec<OperandDomain>)>,
}
#[derive(Debug)]
enum OperandDomain {
    ListShape {
        rows: u64,
        start: u64,
        width: u64,
    },
    DenseList {
        rows: u64,
        width: u64,
        elements: u64,
    },
    SmallDictionary {
        codes: u64,
        values: u64,
        preparation_bound: usize,
    },
}

pub(crate) async fn prepare(
    plan: Arc<SnapshotReadPlan>,
    partitions: Vec<Range<u64>>,
    operands: Vec<ComputedOperand>,
) -> Result<Option<Arc<ComputedProjectionCertificate>>> {
    // No later source-row slicing, pruning or scan filter may narrow a certified
    // operand. Whole-file coverage is necessary, but not the validity proof.
    for file in &plan.files {
        if file.task.predicate().is_some()
            || !file.task.deletes().is_empty()
            || file.task.start() != 0
            || (file.task.length() != 0 && file.task.length() != file.task.file_size_in_bytes())
        {
            return Ok(None);
        }
        let end = file
            .offset
            .checked_add(file.identity.row_count)
            .ok_or_else(|| StorageError::Invalid("file row range overflow".into()))?;
        if !partitions
            .iter()
            .any(|r| r.start <= file.offset && end <= r.end)
        {
            return Ok(None);
        }
    }
    let session = VortexSession::default();
    let budget = Arc::new(MetadataBudget::default());
    let field = arrow_schema::Field::new(
        "",
        arrow_schema::DataType::Struct(plan.schema.fields().clone()),
        false,
    );
    let expected = session.arrow().from_arrow_field(&field).map_err(invalid)?;
    // Bound the outstanding ordered window, not only active worker tasks:
    // a slow early file must not allow the rest of the query to be launched.
    // Footer and
    // layout validation must not serialize on the physical-planning thread.
    // Results are consumed in source-file order. Dropping JoinSet aborts active
    // tasks; already-dispatched blocking reads may still finish. One shared
    // cumulative budget charges reads before allocation and metadata estimates
    // after parsing; it is not a strict peak-heap cap.
    let domains = {
        let expected = Arc::new(expected);
        let prepared_operands = Arc::new(operands.clone());
        let mut tasks = tokio::task::JoinSet::new();
        let mut results = (0..plan.files.len()).map(|_| None).collect::<Vec<_>>();
        let mut domains = Vec::with_capacity(plan.files.len());
        let mut launched = 0;
        while domains.len() < plan.files.len() {
            while launched - domains.len() < MAX_PREFLIGHT_FILES && launched < plan.files.len() {
                let index = launched;
                launched += 1;
                let plan = plan.clone();
                let expected = expected.clone();
                let operands = prepared_operands.clone();
                let session = session.clone();
                let budget = budget.clone();
                tasks.spawn(async move {
                    let domain = prepare_file(&plan.files[index], &expected, &operands,
                        &session, budget).await;
                    (index, domain)
                });
            }
            let (index, domain) = tasks.join_next().await
                .ok_or_else(|| invalid("missing metadata preparation task"))?
                .map_err(invalid)?;
            results[index] = Some(domain);
            while let Some(domain) = results[domains.len()].take() {
                let Some(domain) = domain? else {
                    return Ok(None);
                };
                domains.push(domain);
                if domains.len() == results.len() {
                    break;
                }
            }
        }
        domains
    };
    Ok(Some(Arc::new(ComputedProjectionCertificate {
        plan: Arc::downgrade(&plan),
        partitions,
        operands,
        metadata_bytes: budget.used.load(Ordering::Relaxed),
        domains,
    })))
}

async fn prepare_file(
    file: &crate::PlannedFile,
    expected: &DType,
    operands: &[ComputedOperand],
    session: &VortexSession,
    budget: Arc<MetadataBudget>,
) -> Result<Option<FileDomain>> {
    let path = crate::local_path(&file.identity.path)?;
    if tokio::fs::metadata(&path).await?.len() != file.task.file_size_in_bytes() {
        return Err(StorageError::Invalid("Vortex file size mismatch".into()));
    }
    let inner = FileReadAt::open_with_allocator(&path, session.handle(), session.allocator())
        .map_err(invalid)?;
    let reader = Arc::new(MetadataRead {
        inner,
        budget: budget.clone(),
        denied: AtomicBool::new(false),
        initial_read: Arc::new(Mutex::new(None)),
    });
    let opened = match session
        .open_options()
        .with_file_size(file.task.file_size_in_bytes())
        .open(reader.clone())
        .await
    {
        Ok(file) => file,
        Err(_) if reader.denied.load(Ordering::Relaxed) => return Ok(None),
        Err(error) => return Err(invalid(error)),
    };
    if opened.row_count() != file.identity.row_count {
        return Err(invalid("Vortex file row count mismatch"));
    }
    let Some(bytes) = opened.footer().approx_byte_size() else {
        return Ok(None);
    };
    if !budget.reserve(bytes) {
        return Ok(None);
    }
    let mut probe = Probe {
        reader,
        segment_source: opened.segment_source(),
        segments: opened.footer().segment_map(),
        session,
        budget,
        trees: HashMap::new(),
        domains: Vec::new(),
    };
    let mut operand_domains = Vec::new();
    for operand in operands {
        let path = operand.path();
        let compatible = match operand {
            ComputedOperand::DenseI32Sum(_) => compatible_operand(expected, opened.dtype(), path),
            ComputedOperand::ListLength(_) => {
                compatible_shape_operand(expected, opened.dtype(), path)
            }
        };
        if !compatible {
            return Ok(None);
        }
        let first = probe.domains.len();
        let supported = match operand {
            ComputedOperand::DenseI32Sum(_) => {
                probe
                    .operand(opened.footer().layout().clone(), path, false, 0)
                    .await?
            }
            ComputedOperand::ListLength(_) => {
                probe
                    .list_shape(opened.footer().layout().clone(), path, 0)
                    .await?
            }
        };
        if !supported {
            return Ok(None);
        }
        operand_domains.push((operand.clone(), probe.domains.split_off(first)));
    }
    Ok(Some(FileDomain {
        path: file.identity.path.clone(),
        bytes: file.task.file_size_in_bytes(),
        rows: file.identity.row_count,
        operands: operand_domains,
    }))
}

/// Cumulative charged read bytes and metadata estimates, never reset per file.
/// This limits requested preparation work alongside the in-flight file limit;
/// it does not enforce peak heap usage or precharge every parsed allocation.
#[derive(Default)]
struct MetadataBudget {
    used: AtomicUsize,
}
impl MetadataBudget {
    fn reserve(&self, bytes: usize) -> bool {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|&n| n <= MAX_QUERY_METADATA_BYTES)
            })
            .is_ok()
    }
}

struct MetadataRead {
    inner: FileReadAt,
    budget: Arc<MetadataBudget>,
    denied: AtomicBool,
    // Share the opener's first real allocation only for this file's preparation.
    // Exact metadata subranges can then avoid duplicate tiny positional reads.
    initial_read: Arc<Mutex<Option<(u64, BufferHandle)>>>,
}
impl MetadataRead {
    fn cached_slice(&self, offset: u64, length: usize) -> Option<BufferHandle> {
        let initial = self.initial_read.lock().ok()?;
        let (start, buffer) = initial.as_ref()?;
        let relative = usize::try_from(offset.checked_sub(*start)?).ok()?;
        let end = relative.checked_add(length)?;
        (end <= buffer.len()).then(|| buffer.slice(relative..end))
    }
}
impl VortexReadAt for MetadataRead {
    fn uri(&self) -> Option<&Arc<str>> {
        self.inner.uri()
    }
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        self.inner.coalesce_config()
    }
    fn concurrency(&self) -> usize {
        self.inner.concurrency()
    }
    fn size(&self) -> BoxFuture<'static, vortex::error::VortexResult<u64>> {
        self.inner.size()
    }
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, vortex::error::VortexResult<BufferHandle>> {
        if !self.budget.reserve(length) {
            self.denied.store(true, Ordering::Relaxed);
            return futures::future::ready(Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "computed metadata budget exhausted",
            )
            .into()))
            .boxed();
        }
        let read = self.inner.read_at(offset, length, alignment);
        let initial = self.initial_read.clone();
        async move {
            let buffer = read.await?;
            if let Ok(mut initial) = initial.lock() {
                if initial.is_none() {
                    *initial = Some((offset, buffer.clone()));
                }
            }
            Ok(buffer)
        }.boxed()
    }
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::Invalid(error.to_string())
}

struct Probe<'a> {
    reader: Arc<MetadataRead>,
    segment_source: Arc<dyn SegmentSource>,
    segments: &'a Arc<[vortex::file::SegmentSpec]>,
    session: &'a VortexSession,
    budget: Arc<MetadataBudget>,
    trees: HashMap<usize, (SerializedArray, ReadContext)>,
    domains: Vec<OperandDomain>,
}

impl Probe<'_> {
    async fn read_metadata(&self, offset: u64, length: usize) -> Result<Option<ByteBuffer>> {
        let result = match self.reader.cached_slice(offset, length) {
            Some(buffer) => Ok(buffer),
            None => self.reader.read_at(offset, length, Alignment::none()).await,
        };
        let buffer = match result {
            Ok(buffer) => buffer,
            Err(_) if self.reader.denied.load(Ordering::Relaxed) => return Ok(None),
            Err(error) => return Err(invalid(error)),
        };
        Ok(Some(
            buffer
                .try_into_host()
                .map_err(invalid)?
                .await
                .map_err(invalid)?,
        ))
    }

    fn operand<'a>(
        &'a mut self,
        layout: LayoutRef,
        path: &'a [String],
        inside_list: bool,
        depth: usize,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Ok(false);
            }
            if layout.is::<Zoned>() {
                let Some(data) = layout.slot(0).map_err(invalid)? else {
                    return Ok(false);
                };
                return self.operand(data, path, inside_list, depth + 1).await;
            }
            if layout.is::<Chunked>() {
                let mut rows = 0u64;
                for child in layout.children().map_err(invalid)? {
                    rows = rows
                        .checked_add(child.row_count())
                        .ok_or_else(|| invalid("chunk row count overflow"))?;
                    if !self.operand(child, path, inside_list, depth + 1).await? {
                        return Ok(false);
                    }
                }
                return Ok(rows == layout.row_count());
            }
            if layout.is::<Struct>() {
                if let Some(validity) = layout.slot(0).map_err(invalid)? {
                    if !self.all_valid(validity, depth + 1).await? {
                        return Ok(false);
                    }
                }
                let Some((name, tail)) = path.split_first() else {
                    return Ok(false);
                };
                let Some(index) = layout
                    .dtype()
                    .as_struct_fields_opt()
                    .and_then(|f| f.find(name))
                else {
                    return Ok(false);
                };
                let Some(field) = layout.slot(index + 1).map_err(invalid)? else {
                    return Ok(false);
                };
                if field.row_count() != layout.row_count() {
                    return Err(invalid("struct row count mismatch"));
                }
                return self.operand(field, tail, inside_list, depth + 1).await;
            }
            if let Some(list) = layout.as_opt::<List>() {
                if inside_list {
                    return Ok(false);
                }
                if let Some(validity) = list.validity().map_err(invalid)? {
                    if !self.all_valid(validity, depth + 1).await? {
                        return Ok(false);
                    }
                }
                let elements = list.elements().map_err(invalid)?;
                let offsets = list.offsets().map_err(invalid)?;
                let Some((start, step, count)) = self.sequence(offsets, depth + 1).await? else {
                    return Ok(false);
                };
                let end = step
                    .checked_mul(layout.row_count())
                    .and_then(|n| start.checked_add(n));
                if start != 0
                    || count
                        != layout
                            .row_count()
                            .checked_add(1)
                            .ok_or_else(|| invalid("list row count overflow"))?
                    || end != Some(elements.row_count())
                    || step > i32::MAX as u64
                {
                    return Ok(false);
                }
                self.domains.push(OperandDomain::DenseList {
                    rows: layout.row_count(),
                    width: step,
                    elements: elements.row_count(),
                });
                return self.operand(elements, path, true, depth + 1).await;
            }
            if !inside_list || !path.is_empty() {
                return Ok(false);
            }
            self.integer_leaf(layout, false, depth + 1).await
        })
    }

    /// Offset/validity-only proof: element layout payloads are never inspected.
    fn list_shape<'a>(
        &'a mut self,
        layout: LayoutRef,
        path: &'a [String],
        depth: usize,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Ok(false);
            }
            if layout.is::<Zoned>() {
                let Some(data) = layout.slot(0).map_err(invalid)? else {
                    return Ok(false);
                };
                return self.list_shape(data, path, depth + 1).await;
            }
            if layout.is::<Chunked>() {
                let mut rows = 0u64;
                for child in layout.children().map_err(invalid)? {
                    rows = rows
                        .checked_add(child.row_count())
                        .ok_or_else(|| invalid("shape chunk rows overflow"))?;
                    if !self.list_shape(child, path, depth + 1).await? {
                        return Ok(false);
                    }
                }
                return Ok(rows == layout.row_count());
            }
            if layout.is::<Struct>() {
                if let Some(validity) = layout.slot(0).map_err(invalid)? {
                    if validity.row_count() != layout.row_count() {
                        return Err(invalid("struct validity row count mismatch"));
                    }
                    if !self.shape_validity(validity, depth + 1).await? {
                        return Ok(false);
                    }
                }
                let Some((name, tail)) = path.split_first() else {
                    return Ok(false);
                };
                let Some(index) = layout
                    .dtype()
                    .as_struct_fields_opt()
                    .and_then(|f| f.find(name))
                else {
                    return Ok(false);
                };
                let Some(field) = layout.slot(index + 1).map_err(invalid)? else {
                    return Ok(false);
                };
                if field.row_count() != layout.row_count() {
                    return Err(invalid("shape struct row count mismatch"));
                }
                return self.list_shape(field, tail, depth + 1).await;
            }
            let Some(list) = layout.as_opt::<List>() else {
                return Ok(false);
            };
            if !path.is_empty() {
                return Ok(false);
            }
            if let Some(validity) = list.validity().map_err(invalid)? {
                if validity.row_count() != layout.row_count() {
                    return Err(invalid("list validity row count mismatch"));
                }
                if !self.shape_validity(validity, depth + 1).await? {
                    return Ok(false);
                }
            }
            let Some((start, width, count)) = self
                .sequence(list.offsets().map_err(invalid)?, depth + 1)
                .await?
            else {
                return Ok(false);
            };
            let expected_count = layout
                .row_count()
                .checked_add(1)
                .ok_or_else(|| invalid("shape offset count overflow"))?;
            let end = width
                .checked_mul(layout.row_count())
                .and_then(|n| start.checked_add(n));
            // Unlike SUM, length never visits the elements. Orphan children,
            // nonzero starts and null elements do not enlarge this operation.
            let element_rows = list.elements().map_err(invalid)?.row_count();
            if count != expected_count
                || width > i32::MAX as u64
                || end.is_none_or(|end| end > element_rows)
            {
                return Ok(false);
            }
            self.domains.push(OperandDomain::ListShape {
                rows: layout.row_count(),
                start,
                width,
            });
            Ok(true)
        })
    }

    fn shape_validity<'a>(
        &'a mut self,
        layout: LayoutRef,
        depth: usize,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if !matches!(layout.dtype(), DType::Bool(Nullability::NonNullable)) {
                return Ok(false);
            }
            // Plain bitmaps are bounded by their outer row domain; Constant true
            // uses the existing bounded scalar inspection, not a bitmap read.
            if self.scalar(layout.clone(), false, true, depth).await? {
                return Ok(true);
            }
            self.all_valid(layout, depth).await
        })
    }

    fn integer_leaf<'a>(
        &'a mut self,
        layout: LayoutRef,
        small_dictionary: bool,
        depth: usize,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Ok(false);
            }
            if layout.is::<Zoned>() {
                let Some(data) = layout.slot(0).map_err(invalid)? else {
                    return Ok(false);
                };
                return self.integer_leaf(data, small_dictionary, depth + 1).await;
            }
            if layout.is::<Chunked>() {
                let mut rows = 0u64;
                for child in layout.children().map_err(invalid)? {
                    rows = rows
                        .checked_add(child.row_count())
                        .ok_or_else(|| invalid("chunk rows overflow"))?;
                    if !self
                        .integer_leaf(child, small_dictionary, depth + 1)
                        .await?
                    {
                        return Ok(false);
                    }
                }
                return Ok(rows == layout.row_count());
            }
            if layout.is::<Dict>() {
                if small_dictionary {
                    return Ok(false);
                }
                let Some(values) = layout.slot(0).map_err(invalid)? else {
                    return Ok(false);
                };
                let Some(codes) = layout.slot(1).map_err(invalid)? else {
                    return Ok(false);
                };
                if values.row_count() > MAX_DICTIONARY_VALUES as u64
                    || codes.row_count() != layout.row_count()
                {
                    return Ok(false);
                }
                self.domains.push(OperandDomain::SmallDictionary {
                    codes: codes.row_count(),
                    values: values.row_count(),
                    preparation_bound: DICTIONARY_PREPARATION_BYTES,
                });
                if !self.integer_leaf(values, true, depth + 1).await? {
                    return Ok(false);
                }
                return self.scalar(codes, false, false, depth + 1).await;
            }
            if !matches!(layout.dtype(), DType::Primitive(PType::I32, _)) {
                return Ok(false);
            }
            self.scalar(layout, small_dictionary, false, depth + 1)
                .await
        })
    }

    async fn tree(&mut self, layout: &LayoutRef) -> Result<Option<(SerializedArray, ReadContext)>> {
        let Some(flat) = layout.as_opt::<Flat>() else {
            return Ok(None);
        };
        let index = *flat.segment_id() as usize;
        if let Some(tree) = self.trees.get(&index) {
            return Ok(Some(tree.clone()));
        }
        let Some(segment) = self.segments.get(index) else {
            return Err(invalid("flat segment out of range"));
        };
        if segment.length < 4 {
            return Err(invalid("short array segment"));
        }
        let mut payload_bytes = None;
        let bytes = if let Some(tree) = flat.array_tree() {
            tree.clone()
        } else {
            if segment.length < 4 {
                return Err(invalid("short array segment"));
            }
            let end = segment
                .offset
                .checked_add(u64::from(segment.length))
                .ok_or_else(|| invalid("segment offset overflow"))?;
            let Some(suffix) = self.read_metadata(end - 4, 4).await? else {
                return Ok(None);
            };
            let length = u32::from_le_bytes(suffix.as_ref().try_into().map_err(invalid)?) as usize;
            if length > segment.length as usize - 4 {
                return Err(invalid("array tree outside segment"));
            }
            if length > MAX_TREE_BYTES {
                return Ok(None);
            }
            payload_bytes = Some(segment.length as usize - 4 - length);
            let Some(bytes) = self.read_metadata(end - 4 - length as u64, length).await? else {
                return Ok(None);
            };
            bytes
        };
        if bytes.len() > MAX_TREE_BYTES {
            return Ok(None);
        }
        if flat.array_tree().is_some() {
            if !self.budget.reserve(bytes.len()) {
                return Ok(None);
            }
            payload_bytes = Some(
                (segment.length as usize)
                    .checked_sub(bytes.len() + 4)
                    .ok_or_else(|| invalid("array tree outside segment"))?,
            );
        }
        let tree = SerializedArray::from_array_tree(bytes).map_err(invalid)?;
        let extent = tree.metadata_buffer_extent().map_err(invalid)?;
        let payload = payload_bytes.unwrap_or(0);
        if extent > payload {
            return Err(invalid("buffer descriptor outside serialized payload"));
        }
        if payload - extent > 7 {
            return Ok(None);
        }
        let mut visited = HashSet::new();
        if !visit_buffers(&tree, tree.buffer_lengths().len(), &mut visited, 0)?
            || visited.len() != tree.buffer_lengths().len()
        {
            return Ok(None);
        }
        let result = (tree, flat.array_ctx().clone());
        self.trees.insert(index, result.clone());
        Ok(Some(result))
    }

    fn all_valid<'a>(&'a mut self, layout: LayoutRef, depth: usize) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Ok(false);
            }
            if layout.is::<Zoned>() {
                let Some(data) = layout.slot(0).map_err(invalid)? else {
                    return Ok(false);
                };
                return self.all_valid(data, depth + 1).await;
            }
            if layout.is::<Chunked>() {
                let mut rows = 0u64;
                for child in layout.children().map_err(invalid)? {
                    rows = rows
                        .checked_add(child.row_count())
                        .ok_or_else(|| invalid("validity row count overflow"))?;
                    if !self.all_valid(child, depth + 1).await? {
                        return Ok(false);
                    }
                }
                return Ok(rows == layout.row_count());
            }
            let Some((tree, ctx)) = self.tree(&layout).await? else {
                return Ok(false);
            };
            if ctx.resolve(tree.encoding_id()) != Some(ArrayVTable::id(&Constant))
                || tree.nchildren() != 0
                || tree.metadata_buffer_indices() != [0]
                || !matches!(layout.dtype(), DType::Bool(_))
            {
                return Ok(false);
            }
            // Constant serializes one scalar, not a bitmap: inspect this fixed
            // bounded scalar with the pinned library validator. Never read a
            // physical-domain validity bitmap to establish dense eligibility.
            let lengths = tree.metadata_buffer_lengths().map_err(invalid)?;
            let Some(&length) = lengths.first() else {
                return Ok(false);
            };
            if length > 16 {
                return Ok(false);
            }
            let extent = tree.metadata_buffer_extent().map_err(invalid)?;
            let padding = extent
                .checked_sub(length)
                .ok_or_else(|| invalid("constant scalar descriptor underflow"))?;
            let flat = layout
                .as_opt::<Flat>()
                .ok_or_else(|| invalid("constant layout is not flat"))?;
            let segment = &self.segments[*flat.segment_id() as usize];
            let offset = segment
                .offset
                .checked_add(padding as u64)
                .ok_or_else(|| invalid("constant scalar offset overflow"))?;
            let bytes = if segment.length as usize <= MAX_CONSTANT_SEGMENT_BYTES {
                // A proven tiny Constant segment may already be in the opener's
                // footer-prefetch cache. Never request a whole unknown Flat.
                let result = self
                    .segment_source
                    .request((*flat.segment_id()).into())
                    .await;
                match result {
                    Ok(bytes) => bytes
                        .try_into_host()
                        .map_err(invalid)?
                        .await
                        .map_err(invalid)?
                        .slice(padding..padding + length),
                    Err(_) if self.reader.denied.load(Ordering::Relaxed) => return Ok(false),
                    Err(error) => return Err(invalid(error)),
                }
            } else {
                let Some(bytes) = self.read_metadata(offset, length).await? else {
                    return Ok(false);
                };
                bytes
            };
            let value = vortex::scalar::ScalarValue::from_proto_bytes(
                bytes.as_ref(),
                layout.dtype(),
                self.session,
            )
            .map_err(invalid)?;
            let scalar =
                vortex::scalar::Scalar::try_new(layout.dtype().clone(), value).map_err(invalid)?;
            Ok(scalar.as_bool().value() == Some(true))
        })
    }

    fn sequence<'a>(
        &'a mut self,
        layout: LayoutRef,
        depth: usize,
    ) -> BoxFuture<'a, Result<Option<(u64, u64, u64)>>> {
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Ok(None);
            }
            if layout.is::<Zoned>() {
                let Some(data) = layout.slot(0).map_err(invalid)? else {
                    return Ok(None);
                };
                return self.sequence(data, depth + 1).await;
            }
            let Some((tree, ctx)) = self.tree(&layout).await? else {
                return Ok(None);
            };
            if ctx.resolve(tree.encoding_id()) != Some(ArrayVTable::id(&Sequence))
                || tree.nchildren() != 0
                || tree.nbuffers() != 0
                || layout.dtype() != &DType::Primitive(PType::U64, Nullability::NonNullable)
            {
                return Ok(None);
            }
            let value = tree
                .decode(
                    layout.dtype(),
                    usize::try_from(layout.row_count()).map_err(invalid)?,
                    &ctx,
                    self.session,
                )
                .map_err(invalid)?;
            let Some(value) = value.as_opt::<Sequence>() else {
                return Ok(None);
            };
            let (start, step) = value.wrapping_bits().map_err(invalid)?;
            Ok(Some((start, step, layout.row_count())))
        })
    }

    fn scalar<'a>(
        &'a mut self,
        layout: LayoutRef,
        small: bool,
        boolean: bool,
        depth: usize,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Ok(false);
            }
            if layout.is::<Zoned>() {
                let Some(data) = layout.slot(0).map_err(invalid)? else {
                    return Ok(false);
                };
                return self.scalar(data, small, boolean, depth + 1).await;
            }
            if layout.is::<Chunked>() {
                let mut rows = 0u64;
                for child in layout.children().map_err(invalid)? {
                    rows = rows
                        .checked_add(child.row_count())
                        .ok_or_else(|| invalid("scalar chunk row count overflow"))?;
                    if !self.scalar(child, small, boolean, depth + 1).await? {
                        return Ok(false);
                    }
                }
                return Ok(rows == layout.row_count());
            }
            let Some((tree, ctx)) = self.tree(&layout).await? else {
                return Ok(false);
            };
            scalar_tree(
                &tree,
                &ctx,
                layout.dtype(),
                usize::try_from(layout.row_count()).map_err(invalid)?,
                small,
                boolean,
                self.session,
                0,
            )
        })
    }
}

fn scalar_tree(
    tree: &SerializedArray,
    ctx: &ReadContext,
    dtype: &DType,
    len: usize,
    small: bool,
    boolean: bool,
    session: &VortexSession,
    depth: usize,
) -> Result<bool> {
    if depth > MAX_DEPTH {
        return Ok(false);
    }
    let Some(id) = ctx.resolve(tree.encoding_id()) else {
        return Err(invalid("array encoding index out of range"));
    };
    if id == ArrayVTable::id(&Constant) {
        if tree.nchildren() != 0 {
            return Err(invalid("invalid constant metadata"));
        }
        // Numeric constants use a serialized scalar buffer in this version.
        // They are valid input but outside this initial metadata-only leaf proof.
        if tree.nbuffers() != 0 {
            return Ok(false);
        }
        let value = tree.decode(dtype, len, ctx, session).map_err(invalid)?;
        return Ok(small
            || !value
                .as_opt::<Constant>()
                .is_some_and(|value| value.scalar().is_null()));
    }
    let lengths = tree.metadata_buffer_lengths().map_err(invalid)?;
    if id == ArrayVTable::id(&Primitive) && !boolean {
        let DType::Primitive(ptype, _) = dtype else {
            return Ok(false);
        };
        if !tree.metadata().is_empty()
            || lengths.len() != 1
            || lengths[0]
                != len
                    .checked_mul(ptype.byte_width())
                    .ok_or_else(|| invalid("primitive byte count overflow"))?
        {
            return Err(invalid("invalid primitive buffer length"));
        }
        return validity_tree(tree, ctx, dtype, len, small, session, depth + 1);
    }
    if id == ArrayVTable::id(&Bool) && boolean {
        let metadata = BoolMetadata::decode(tree.metadata()).map_err(invalid)?;
        if metadata.offset >= 8 || lengths.len() != 1 {
            return Err(invalid("invalid Boolean metadata"));
        }
        let required = len
            .checked_add(metadata.offset as usize)
            .ok_or_else(|| invalid("boolean length overflow"))?
            .div_ceil(8);
        if lengths[0] < required {
            return Err(invalid("short Boolean buffer"));
        }
        if lengths[0] > required.saturating_add(7) {
            return Ok(false);
        }
        return validity_tree(tree, ctx, dtype, len, small, session, depth + 1);
    }
    if id == ArrayVTable::id(&BitPacked) && !boolean {
        let metadata = BitPackedMetadata::decode(tree.metadata()).map_err(invalid)?;
        let DType::Primitive(ptype, _) = dtype else {
            return Ok(false);
        };
        if !ptype.is_int()
            || metadata.offset() >= 1024
            || metadata.bit_width() > ptype.bit_width() as u32
        {
            return Err(invalid("invalid bitpacked metadata"));
        }
        if metadata.has_patches() {
            return Ok(false);
        }
        let expected = len
            .checked_add(metadata.offset() as usize)
            .and_then(|n| n.div_ceil(1024).checked_mul(128))
            .and_then(|n| n.checked_mul(metadata.bit_width() as usize))
            .ok_or_else(|| invalid("packed buffer length overflow"))?;
        if lengths.len() != 1 || lengths[0] != expected {
            return Err(invalid("invalid packed buffer length"));
        }
        return validity_tree(tree, ctx, dtype, len, small, session, depth + 1);
    }
    Ok(false)
}

fn validity_tree(
    tree: &SerializedArray,
    ctx: &ReadContext,
    dtype: &DType,
    len: usize,
    small: bool,
    session: &VortexSession,
    depth: usize,
) -> Result<bool> {
    if tree.nchildren() == 0 {
        return Ok(true);
    }
    if tree.nchildren() != 1 {
        return Ok(false);
    }
    let child = tree.child(0);
    if small {
        return scalar_tree(
            &child,
            ctx,
            &DType::Bool(Nullability::NonNullable),
            len,
            true,
            true,
            session,
            depth + 1,
        );
    }
    if ctx.resolve(child.encoding_id()) != Some(ArrayVTable::id(&Constant))
        || child.nchildren() != 0
        || child.nbuffers() != 0
    {
        return Ok(false);
    }
    let value = child
        .decode(&DType::Bool(Nullability::NonNullable), len, ctx, session)
        .map_err(invalid)?;
    Ok(value
        .as_opt::<Constant>()
        .is_some_and(|v| v.scalar().as_bool().value() == Some(true))
        && !matches!(dtype, DType::Null))
}

fn visit_buffers(
    tree: &SerializedArray,
    count: usize,
    visited: &mut HashSet<usize>,
    depth: usize,
) -> Result<bool> {
    if depth > MAX_DEPTH {
        return Ok(false);
    }
    for index in tree.metadata_buffer_indices() {
        if index >= count {
            return Err(invalid("buffer descriptor index out of range"));
        }
        visited.insert(index);
    }
    for index in 0..tree.nchildren() {
        if !visit_buffers(&tree.child(index), count, visited, depth + 1)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) type ProofCache = tokio::sync::Mutex<
    HashMap<
        (Vec<ComputedOperand>, Vec<(u64, u64)>),
        Arc<tokio::sync::OnceCell<Option<Arc<ComputedProjectionCertificate>>>>,
    >,
>;

fn compatible_operand(expected: &DType, actual: &DType, path: &[String]) -> bool {
    if expected.nullability() != actual.nullability() {
        return false;
    }
    match (expected, actual) {
        (DType::Struct(left, _), DType::Struct(right, _)) => {
            let Some((head, tail)) = path.split_first() else {
                return false;
            };
            let (Some(left), Some(right)) = (left.field(head), right.field(head)) else {
                return false;
            };
            compatible_operand(&left, &right, tail)
        }
        (DType::List(left, _), DType::List(right, _)) => compatible_operand(left, right, path),
        _ => path.is_empty() && expected == actual,
    }
}

fn compatible_shape_operand(expected: &DType, actual: &DType, path: &[String]) -> bool {
    if expected.nullability() != actual.nullability() {
        return false;
    }
    if path.is_empty() {
        return matches!(expected, DType::List(..)) && expected == actual;
    }
    let (DType::Struct(left, _), DType::Struct(right, _)) = (expected, actual) else {
        return false;
    };
    let (head, tail) = path.split_first().expect("nonempty path");
    let (Some(left), Some(right)) = (left.field(head), right.field(head)) else {
        return false;
    };
    compatible_shape_operand(&left, &right, tail)
}
