//! Conservative, metadata-only proof for dense fixed-width integer-list reduction.
//!
//! This is deliberately a closed proof, not a codec-name blacklist. Unsupported
//! valid metadata returns None and the caller retains its original DF expression.
use crate::{Result, SnapshotReadPlan, StorageError};
use futures::future::BoxFuture;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use vortex::VortexSessionDefault;
use vortex::array::ArrayVTable;
use vortex::array::arrays::bool::BoolMetadata;
use vortex::array::arrays::{Bool, Constant, Primitive};
use vortex::array::serde::SerializedArray;
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::{DType, Nullability, PType};
use vortex::encodings::fastlanes::{BitPacked, BitPackedMetadata};
use vortex::encodings::sequence::Sequence;
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::LayoutRef;
use vortex::layout::layouts::{
    chunked::Chunked, dict::Dict, flat::Flat, list::List, struct_::Struct, zoned::Zoned,
};
use vortex::session::{VortexSession, registry::ReadContext};

// The selected small dictionary, its nullable bitmap, prepared lookup records,
// and decoder output/scratch must fit this explicit fixed preparation budget.
const DICTIONARY_PREPARATION_BYTES: usize = 64 * 1024;
const MAX_DICTIONARY_VALUES: usize = (DICTIONARY_PREPARATION_BYTES - 8 * 1024) / 32;
const MAX_TREE_BYTES: usize = 64 * 1024;
const MAX_QUERY_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_DEPTH: usize = 48;

/// Immutable proof tied to one snapshot, exact logical operands and source ranges.
#[derive(Debug)]
pub struct DenseListProjectionCertificate {
    pub(crate) plan: std::sync::Weak<SnapshotReadPlan>,
    pub(crate) partitions: Vec<Range<u64>>,
    pub(crate) operands: Vec<Vec<String>>,
    // Retain explicit physical-domain accounting; this is not a claim about
    // arbitrary filtered selections or arbitrary native Vortex encodings.
    pub(crate) metadata_bytes: usize,
    pub(crate) domains: Vec<FileDomain>,
}

#[derive(Debug)]
pub(crate) struct FileDomain {
    path: String,
    bytes: u64,
    rows: u64,
    operands: Vec<(Vec<String>, Vec<OperandDomain>)>,
}
#[derive(Debug)]
enum OperandDomain {
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
    operands: Vec<Vec<String>>,
) -> Result<Option<Arc<DenseListProjectionCertificate>>> {
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
    let mut metadata_bytes = 0usize;
    let mut domains = Vec::new();
    let field = arrow_schema::Field::new(
        "",
        arrow_schema::DataType::Struct(plan.schema.fields().clone()),
        false,
    );
    let expected = session.arrow().from_arrow_field(&field).map_err(invalid)?;
    for file in &plan.files {
        let path = crate::local_path(&file.identity.path)?;
        if tokio::fs::metadata(&path).await?.len() != file.task.file_size_in_bytes() {
            return Err(StorageError::Invalid("Vortex file size mismatch".into()));
        }
        let opened = session
            .open_options()
            .with_file_size(file.task.file_size_in_bytes())
            .open_path(&path)
            .await
            .map_err(invalid)?;
        if opened.row_count() != file.identity.row_count {
            return Err(invalid("Vortex file row count mismatch"));
        }
        if let Some(bytes) = opened.footer().approx_byte_size() {
            metadata_bytes = metadata_bytes
                .checked_add(bytes)
                .ok_or_else(|| invalid("metadata budget overflow"))?;
            if metadata_bytes > MAX_QUERY_METADATA_BYTES {
                return Ok(None);
            }
        }
        let mut probe = Probe {
            file: tokio::fs::File::open(&path).await?,
            segments: opened.footer().segment_map(),
            session: &session,
            metadata_bytes: &mut metadata_bytes,
            trees: HashMap::new(),
            domains: Vec::new(),
        };
        let mut operand_domains = Vec::new();
        for operand in &operands {
            if !compatible_operand(&expected, opened.dtype(), operand) {
                return Ok(None);
            }
            let first = probe.domains.len();
            if !probe
                .operand(opened.footer().layout().clone(), operand, false, 0)
                .await?
            {
                return Ok(None);
            }
            operand_domains.push((operand.clone(), probe.domains.split_off(first)));
        }
        domains.push(FileDomain {
            path: file.identity.path.clone(),
            bytes: file.task.file_size_in_bytes(),
            rows: file.identity.row_count,
            operands: operand_domains,
        });
    }
    Ok(Some(Arc::new(DenseListProjectionCertificate {
        plan: Arc::downgrade(&plan),
        partitions,
        operands,
        metadata_bytes,
        domains,
    })))
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::Invalid(error.to_string())
}

struct Probe<'a> {
    file: tokio::fs::File,
    segments: &'a Arc<[vortex::file::SegmentSpec]>,
    session: &'a VortexSession,
    metadata_bytes: &'a mut usize,
    trees: HashMap<usize, (SerializedArray, ReadContext)>,
    domains: Vec<OperandDomain>,
}

impl Probe<'_> {
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
            self.file.seek(std::io::SeekFrom::Start(end - 4)).await?;
            let mut suffix = [0u8; 4];
            self.file.read_exact(&mut suffix).await?;
            let length = u32::from_le_bytes(suffix) as usize;
            if length > segment.length as usize - 4 {
                return Err(invalid("array tree outside segment"));
            }
            if length > MAX_TREE_BYTES {
                return Ok(None);
            }
            payload_bytes = Some(segment.length as usize - 4 - length);
            *self.metadata_bytes = self
                .metadata_bytes
                .checked_add(length + 4)
                .ok_or_else(|| invalid("metadata budget overflow"))?;
            if *self.metadata_bytes > MAX_QUERY_METADATA_BYTES {
                return Ok(None);
            }
            self.file
                .seek(std::io::SeekFrom::Start(end - 4 - length as u64))
                .await?;
            let mut bytes = vec![0; length];
            self.file.read_exact(&mut bytes).await?;
            bytes.into()
        };
        if bytes.len() > MAX_TREE_BYTES {
            return Ok(None);
        }
        if flat.array_tree().is_some() {
            *self.metadata_bytes = self
                .metadata_bytes
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("metadata budget overflow"))?;
            if *self.metadata_bytes > MAX_QUERY_METADATA_BYTES {
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
                || tree.nbuffers() != 0
            {
                return Ok(false);
            }
            let value = tree
                .decode(
                    layout.dtype(),
                    usize::try_from(layout.row_count()).map_err(invalid)?,
                    &ctx,
                    self.session,
                )
                .map_err(invalid)?;
            let Some(value) = value.as_opt::<Constant>() else {
                return Ok(false);
            };
            if !matches!(layout.dtype(), DType::Bool(_)) {
                return Ok(false);
            }
            Ok(value.scalar().as_bool().value() == Some(true))
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
        if tree.nchildren() != 0 || tree.nbuffers() != 0 {
            return Err(invalid("invalid constant metadata"));
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
        (Vec<Vec<String>>, Vec<(u64, u64)>),
        Arc<tokio::sync::OnceCell<Option<Arc<DenseListProjectionCertificate>>>>,
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
