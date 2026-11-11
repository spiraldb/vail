//! Typed block kernels and the final output sink for the prepared list program.
use super::*;
use std::{
    mem::take,
    ops::{Deref, DerefMut},
    sync::Mutex,
};

pub(super) const MAX_SCRATCH: usize = 4 * 1024 * 1024;
const MAX_BLOCK: usize = 8192;

enum Values {
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<u8>),
}
struct Buffer {
    // A view is authoritative when present and borrows the retained BoundRead;
    // otherwise the typed Vec owns the values. Scalar buffers expose one value,
    // array buffers expose the current lane count. all_valid makes validity
    // bytes irrelevant; otherwise active lanes have explicitly written bytes.
    // Only active leaves consume results, and mixed-leaf reads never borrow a
    // span or collapse differing domains into a scalar.
    values: Values,
    view: Option<(usize, usize)>,
    scalar: bool,
    valid: Vec<u8>,
    all_valid: bool,
}
macro_rules! getter {
    ($name:ident,$variant:ident,$array:ty,$native:ty) => {
        fn $name<'a>(&'a self, reads: &'a [BoundRead], count: usize) -> &'a [$native] {
            let len = if self.scalar { 1 } else { count };
            if let Some((read, start)) = self.view {
                &reads[read]
                    .values
                    .as_any()
                    .downcast_ref::<$array>()
                    .unwrap()
                    .values()[start..start + len]
            } else {
                let Values::$variant(values) = &self.values else {
                    unreachable!()
                };
                &values[..len]
            }
        }
    };
}
impl Buffer {
    fn new(kind: Kind, len: usize) -> Self {
        Self {
            values: match kind {
                Kind::I32 => Values::I32(vec![0; len]),
                Kind::I64 => Values::I64(vec![0; len]),
                Kind::F32 => Values::F32(vec![0.0; len]),
                Kind::F64 => Values::F64(vec![0.0; len]),
                Kind::Bool => Values::Bool(vec![0; len]),
            },
            view: None,
            scalar: false,
            valid: vec![0; len],
            all_valid: false,
        }
    }
    getter!(i32, I32, Int32Array, i32);
    getter!(i64, I64, Int64Array, i64);
    getter!(f32, F32, Float32Array, f32);
    getter!(f64, F64, Float64Array, f64);
    fn boolean(&self, count: usize) -> &[u8] {
        let Values::Bool(values) = &self.values else {
            unreachable!()
        };
        &values[..if self.scalar { 1 } else { count }]
    }
    fn valid(&self, index: usize) -> bool {
        self.all_valid || self.valid[if self.scalar { 0 } else { index }] != 0
    }
    fn true_at(&self, index: usize) -> bool {
        self.valid(index) && self.boolean(index + 1)[if self.scalar { 0 } else { index }] != 0
    }
}
// Retain at most one bounded workspace per prepared program. Concurrent batch
// evaluations take separate leases; the lock never covers kernel execution.
// The pool retains primitive scratch only, never input arrays or output values.
#[derive(Default)]
pub(super) struct WorkspacePool(Mutex<Option<(usize, Vec<Buffer>)>>);
impl fmt::Debug for WorkspacePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WorkspacePool")
    }
}
struct WorkspaceLease<'a> {
    pool: &'a WorkspacePool,
    capacity: usize,
    buffers: Vec<Buffer>,
}
impl WorkspacePool {
    fn lease(&self, registers: &[Register], capacity: usize) -> WorkspaceLease<'_> {
        let cached = self.0.lock().ok().and_then(|mut slot| slot.take());
        let buffers = match cached {
            Some((old, buffers)) if old == capacity => buffers,
            other => {
                drop(other);
                registers
                    .iter()
                    .map(|r| {
                        Buffer::new(
                            r.kind,
                            if matches!(r.op, Op::Constant(_)) {
                                1
                            } else {
                                capacity
                            },
                        )
                    })
                    .collect()
            }
        };
        WorkspaceLease {
            pool: self,
            capacity,
            buffers,
        }
    }
}
impl Deref for WorkspaceLease<'_> {
    type Target = [Buffer];
    fn deref(&self) -> &[Buffer] {
        &self.buffers
    }
}
impl DerefMut for WorkspaceLease<'_> {
    fn deref_mut(&mut self) -> &mut [Buffer] {
        &mut self.buffers
    }
}
impl Drop for WorkspaceLease<'_> {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.pool.0.lock() {
            if slot.is_none() {
                *slot = Some((self.capacity, take(&mut self.buffers)));
            }
        }
    }
}
include!("list_flatmap_output.rs");

enum FoldValues {
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
}
struct FoldOutput {
    values: FoldValues,
    valid: Vec<bool>,
    operator: Binary,
}
impl FoldOutput {
    fn new(dtype: &DataType, fold: Fold, rows: usize) -> Self {
        let values = match dtype {
            DataType::Int32 => FoldValues::I32(vec![fold.initial.bits as i32; rows]),
            DataType::Int64 => FoldValues::I64(vec![fold.initial.bits as i64; rows]),
            DataType::Float32 => {
                FoldValues::F32(vec![f32::from_bits(fold.initial.bits as u32); rows])
            }
            DataType::Float64 => FoldValues::F64(vec![f64::from_bits(fold.initial.bits); rows]),
            _ => unreachable!(),
        };
        Self {
            values,
            valid: vec![fold.initial.valid; rows],
            operator: fold.operator,
        }
    }
    fn append(
        &mut self,
        entries: &[(usize, usize, usize)],
        program: &ListFlatMap,
        workspace: &[Buffer],
        reads: &[BoundRead],
        count: usize,
    ) {
        let active = entries
            .iter()
            .fold(0u64, |mask, &(_, _, leaf)| mask | (1 << leaf));
        let mut sources = [&workspace[program.leaves[0].value]; MAX_LEAVES];
        for (leaf, descriptor) in program.leaves.iter().enumerate() {
            sources[leaf] = &workspace[descriptor.value];
        }
        macro_rules! reduce {
            ($values:expr, $getter:ident, $add:expr, $mul:expr) => {{
                let mut columns = [&[][..]; MAX_LEAVES];
                for (leaf, source) in sources.iter().enumerate().take(program.leaves.len()) {
                    if active & (1 << leaf) != 0 {
                        columns[leaf] = source.$getter(reads, count);
                    }
                }
                macro_rules! apply {
                    ($op:expr) => {
                        for &(row, lane, leaf) in entries {
                            let source = sources[leaf];
                            self.valid[row] &= source.valid(lane);
                            let value = columns[leaf][if source.scalar { 0 } else { lane }];
                            $values[row] = ($op)($values[row], value);
                        }
                    };
                }
                if self.operator == Binary::Add {
                    apply!($add);
                } else {
                    apply!($mul);
                }
            }};
        }
        match &mut self.values {
            FoldValues::I32(values) => reduce!(values, i32, i32::wrapping_add, i32::wrapping_mul),
            FoldValues::I64(values) => reduce!(values, i64, i64::wrapping_add, i64::wrapping_mul),
            // Fold in emission order, retaining the declared precision at every
            // step. This is a sequential recurrence, not a reassociated sum.
            FoldValues::F32(values) => {
                reduce!(values, f32, |a: f32, b: f32| a + b, |a: f32, b: f32| a * b)
            }
            FoldValues::F64(values) => {
                reduce!(values, f64, |a: f64, b: f64| a + b, |a: f64, b: f64| a * b)
            }
        }
    }
    fn finish(self) -> ArrayRef {
        let nulls = Some(NullBuffer::from(self.valid));
        match self.values {
            FoldValues::I32(values) => Arc::new(Int32Array::new(values.into(), nulls)),
            FoldValues::I64(values) => Arc::new(Int64Array::new(values.into(), nulls)),
            FoldValues::F32(values) => Arc::new(Float32Array::new(values.into(), nulls)),
            FoldValues::F64(values) => Arc::new(Float64Array::new(values.into(), nulls)),
        }
    }
}

enum Output {
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Bool(BooleanBuilder),
    Nested(Box<dyn ArrayBuilder>),
    Fold(FoldOutput),
}
impl Output {
    fn new(dtype: &DataType) -> Self {
        let Some(kind) = Kind::of(dtype) else {
            return Self::Nested(make_output_builder(dtype));
        };
        match kind {
            Kind::I32 => Self::I32(Int32Builder::with_capacity(0)),
            Kind::I64 => Self::I64(Int64Builder::with_capacity(0)),
            Kind::F32 => Self::F32(Float32Builder::with_capacity(0)),
            Kind::F64 => Self::F64(Float64Builder::with_capacity(0)),
            Kind::Bool => Self::Bool(BooleanBuilder::with_capacity(0)),
        }
    }
    fn finish(self) -> ArrayRef {
        match self {
            Self::I32(mut out) => Arc::new(out.finish()),
            Self::I64(mut out) => Arc::new(out.finish()),
            Self::F32(mut out) => Arc::new(out.finish()),
            Self::F64(mut out) => Arc::new(out.finish()),
            Self::Bool(mut out) => Arc::new(out.finish()),
            Self::Nested(mut out) => out.finish(),
            Self::Fold(out) => out.finish(),
        }
    }
}
// These lanes refer only to the root list's borrowed descendant. Construction below
// starts inside a run of valid parents; selection retains ascending, in-bounds
// indices from that run. pointwise_shape excludes other non-scalar domains.
#[derive(Clone, Copy)]
enum InputLanes {
    Contiguous(usize),
    Selected,
}
#[derive(Clone, Copy)]
struct SpanInput {
    coordinate: usize,
    leaves: u64,
    lanes: InputLanes,
}
impl SpanInput {
    fn index(self, coordinates: &[Vec<usize>], lane: usize) -> usize {
        match self.lanes {
            InputLanes::Contiguous(start) => start + lane,
            InputLanes::Selected => coordinates[self.coordinate][lane],
        }
    }
}
fn apply_predicate(mask: &mut [u8], predicate: &Buffer, expected: bool) {
    let values = predicate.boolean(mask.len());
    if predicate.scalar {
        if !predicate.valid(0) || (values[0] != 0) != expected {
            mask.fill(0);
        }
    } else if predicate.all_valid {
        for (mask, &value) in mask.iter_mut().zip(values) {
            *mask &= u8::from((value != 0) == expected);
        }
    } else {
        for (i, (mask, &value)) in mask.iter_mut().zip(values).enumerate() {
            *mask &= u8::from(predicate.valid(i) && (value != 0) == expected);
        }
    }
}
struct SpanRoute<'a> {
    loops: Vec<(usize, usize)>,
    leaves: Vec<usize>,
    guards: &'a [usize],
    guard_mask: u64,
}
struct Execution<'a> {
    program: &'a ListFlatMap,
    reads: Vec<BoundRead>,
    positions: Vec<usize>,
    coordinates: Vec<Vec<usize>>,
    leaves: Vec<usize>,
    workspace: WorkspaceLease<'a>,
    output: Output,
    output_len: usize,
    output_counts: Vec<usize>,
    guard_results: Vec<Vec<bool>>,
    capacity: usize,
    domains: Vec<(bool, bool, usize)>,
    span: Option<SpanInput>,
    emissions: Vec<(usize, usize)>,
    fold_entries: Vec<(usize, usize, usize)>,
    output_validity: Vec<u8>,
    reuse_guards: bool,
}
impl ListFlatMap {
    pub(super) fn execute(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let reads = bind_reads(self, batch)?;
        let route = self.pointwise_shape(&reads);
        if let Some((ordinary, schema)) = &self.ordinary {
            let prefer_ordinary = route.is_none();
            if prefer_ordinary && schema.as_ref() == batch.schema_ref().as_ref() {
                // Sources are certified plain Columns. Another Column getter is
                // an Arc lookup on this frame, not another source/UDF execution.
                return ordinary.evaluate(batch)?.into_array(batch.num_rows());
            }
        }
        let field = match &self.dtype {
            DataType::List(field) => Some(field),
            _ => None,
        };
        let element_type = field.map_or(&self.dtype, |field| field.data_type());
        // The bound includes every scalar register, coordinate and guard lane.
        // Constants need only one value; the estimate remains conservative.
        let bytes_per_lane = self
            .registers
            .iter()
            .map(|r| match r.kind {
                Kind::I32 | Kind::F32 => 5,
                Kind::I64 | Kind::F64 => 9,
                Kind::Bool => 2,
            })
            .sum::<usize>()
            + self.coordinates * 9
            + self.leaves.len()
                * (1 + if let Some(output) = &self.output {
                    16 + output.depth()
                } else if self.fold.is_some() {
                    24
                } else {
                    0
                })
            + 17;
        let capacity = reads
            .iter()
            .map(|read| read.values.len())
            .max()
            .unwrap_or(1)
            .saturating_mul(self.leaves.len())
            .clamp(1, MAX_BLOCK.min(MAX_SCRATCH / bytes_per_lane.max(1)).max(1));
        let mut execution = Execution {
            program: self,
            reads,
            positions: vec![0; self.coordinates],
            coordinates: (0..self.coordinates)
                .map(|_| Vec::with_capacity(capacity))
                .collect(),
            leaves: Vec::with_capacity(capacity),
            workspace: self.workspace.lease(&self.registers, capacity),
            output: if let Some(fold) = self.fold {
                Output::Fold(FoldOutput::new(element_type, fold, batch.num_rows()))
            } else {
                Output::new(element_type)
            },
            output_len: 0,
            output_counts: if self.fold.is_some() {
                vec![]
            } else {
                vec![0; batch.num_rows()]
            },
            guard_results: (0..self.coordinates)
                .map(|_| vec![false; capacity])
                .collect(),
            capacity,
            domains: vec![(false, false, 0); self.coordinates],
            span: None,
            emissions: if self.output.is_some() {
                Vec::with_capacity(capacity * self.leaves.len())
            } else {
                vec![]
            },
            fold_entries: if self.fold.is_some() {
                Vec::with_capacity(capacity * self.leaves.len())
            } else {
                vec![]
            },
            reuse_guards: false,
            output_validity: self.output.as_ref().map_or_else(Vec::new, |output| {
                vec![0; capacity * self.leaves.len() * output.depth()]
            }),
        };
        let mut validity = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            execution.positions[0] = row;
            let valid = execution.reads[self.root_read].valid(&execution.positions);
            validity.push(valid);
        }
        if let Some(route) = route {
            execution.execute_pointwise(&route, &validity)?;
        } else {
            for (row, &valid) in validity.iter().enumerate() {
                execution.positions[0] = row;
                if valid {
                    execution.visit(&self.root)?;
                }
            }
            execution.flush()?;
        }
        if let Output::Fold(output) = &mut execution.output {
            for (valid, &root) in output.valid.iter_mut().zip(&validity) {
                *valid &= root;
            }
            return Ok(execution.output.finish());
        }
        Ok(Arc::new(ListArray::try_new(
            field.unwrap().clone(),
            OffsetBuffer::<i32>::from_lengths(execution.output_counts),
            execution.output.finish(),
            validity
                .iter()
                .any(|valid| !valid)
                .then(|| NullBuffer::from(validity)),
        )?))
    }
    fn pointwise_shape(&self, reads: &[BoundRead]) -> Option<SpanRoute<'_>> {
        let mut node = &self.root;
        let mut loops: Vec<(usize, usize)> = vec![];
        let mut guard_list: &[usize] = &[];
        let mut guard_mask_value = 0;
        while let Node::Loop {
            read,
            coordinate,
            guards,
            guard_mask,
            body,
        } = node
        {
            if let Some(&(previous_read, previous_coordinate)) = loops.last() {
                if !guard_list.is_empty() {
                    return None;
                }
                let mut prefix = self.reads[previous_read].path.clone();
                prefix.push(Path::Values(
                    self.reads[previous_read].coordinate,
                    previous_coordinate,
                ));
                let next = &self.reads[*read];
                if next.coordinate != previous_coordinate
                    || next.child != self.reads[previous_read].child
                    || !next.path.starts_with(&prefix)
                    || !next.path[prefix.len()..]
                        .iter()
                        .all(|p| matches!(p, Path::Field(_)))
                {
                    return None;
                }
            }
            loops.push((*read, *coordinate));
            guard_list = guards;
            guard_mask_value = *guard_mask;
            node = body;
        }
        let &(read, coordinate) = loops.last()?;
        fn emitters(node: &Node, leaves: &mut Vec<usize>) -> Option<()> {
            match node {
                Node::Emit(leaf) => leaves.push(*leaf),
                Node::Concat(nodes) => {
                    for node in nodes {
                        emitters(node, leaves)?;
                    }
                }
                _ => return None,
            }
            Some(())
        }
        let mut leaves = vec![];
        emitters(node, &mut leaves)?;
        if loops[0].0 != self.root_read || reads[read].scalar || leaves.is_empty() {
            return None;
        }
        let mut prefix = self.reads[read].path.clone();
        prefix.push(Path::Values(self.reads[read].coordinate, coordinate));
        for register in &self.registers {
            let read = match register.op {
                Op::Read(read) | Op::Length(read) | Op::MissingRead(read) => read,
                _ => continue,
            };
            let desc = &self.reads[read];
            if desc.coordinate == coordinate
                && desc.child == self.reads[self.root_read].child
                && desc.path.starts_with(&prefix)
                && desc.path[prefix.len()..]
                    .iter()
                    .all(|p| matches!(p, Path::Field(_)))
            {
                continue;
            }
            if !(reads[read].scalar && desc.coordinate == 0) {
                return None;
            }
        }
        // Ordinary traversal skips whole null-owner spans. Keep that path
        // rather than scanning an arbitrarily large hidden child envelope.
        if loops[1..].iter().any(|&(read, _)| {
            let read = &reads[read];
            read.values.null_count() != 0
                || read
                    .ancestors
                    .iter()
                    .any(|(mask, slot)| *slot != 0 && mask.null_count() != 0)
        }) {
            return None;
        }
        Some(SpanRoute {
            loops,
            leaves,
            guards: guard_list,
            guard_mask: guard_mask_value,
        })
    }
}
impl Execution<'_> {
    // Evaluate each output column over original input lanes. Literal flatmap
    // fanout is interleaved in the final builder, without expanded coordinates.
    fn execute_pointwise(&mut self, route: &SpanRoute<'_>, validity: &[bool]) -> Result<()> {
        let coordinate = route.loops.last().unwrap().1;
        let leaves = &route.leaves;
        let guards = route.guards;
        let guard_mask = route.guard_mask;
        let lists = self.reads[self.program.root_read]
            .values
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone();
        // Compose only offsets; no inner ListArray or child payload is built.
        let mut offsets = lists.value_offsets().to_vec();
        for &(read, _) in &route.loops[1..] {
            let inner = self.reads[read]
                .values
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for offset in &mut offsets {
                *offset = inner.value_offsets()[*offset as usize];
            }
        }
        let leaf_mask = leaves.iter().fold(0u64, |mask, &leaf| mask | (1 << leaf));
        let unconditional = leaves
            .iter()
            .all(|&leaf| self.program.leaves[leaf].guards.is_empty());
        if unconditional && guards.is_empty() && self.program.fold.is_none() {
            let total = validity
                .iter()
                .enumerate()
                .filter(|(_, valid)| **valid)
                .try_fold(0usize, |total, (row, _)| {
                    total.checked_add((offsets[row + 1] - offsets[row]) as usize)
                })
                .and_then(|total| total.checked_mul(leaves.len()))
                .filter(|&total| total <= i32::MAX as usize)
                .ok_or_else(|| {
                    DataFusionError::Execution("flatmap output exceeds List32 capacity".into())
                })?;
            // This is the exact final output size, not a filter upper bound.
            match &mut self.output {
                Output::I32(out) => *out = Int32Builder::with_capacity(total),
                Output::I64(out) => *out = Int64Builder::with_capacity(total),
                Output::F32(out) => *out = Float32Builder::with_capacity(total),
                Output::F64(out) => *out = Float64Builder::with_capacity(total),
                Output::Bool(out) => *out = BooleanBuilder::with_capacity(total),
                Output::Nested(_) | Output::Fold(_) => {}
            }
        }
        let mut masks = if unconditional {
            vec![]
        } else {
            vec![vec![1u8; self.capacity]; leaves.len()]
        };
        let mut root_mask = if guards.is_empty() {
            vec![]
        } else {
            vec![1u8; self.capacity]
        };
        let mut row = 0;
        let mut output_row = 0;
        while row < validity.len() {
            if !validity[row] {
                row += 1;
                continue;
            }
            let first_row = row;
            while row < validity.len() && validity[row] {
                row += 1;
            }
            let start = offsets[first_row] as usize;
            let end = offsets[row] as usize;
            for first in (start..end).step_by(self.capacity) {
                let input_count = (end - first).min(self.capacity);
                self.span = Some(SpanInput {
                    coordinate,
                    leaves: 0,
                    lanes: InputLanes::Contiguous(first),
                });
                let count = if guards.is_empty() {
                    input_count
                } else {
                    self.compute(input_count, Some(guard_mask));
                    root_mask[..input_count].fill(1);
                    for &guard in guards {
                        apply_predicate(
                            &mut root_mask[..input_count],
                            &self.workspace[guard],
                            true,
                        );
                    }
                    let selected = root_mask[..input_count]
                        .iter()
                        .map(|&keep| keep as usize)
                        .sum::<usize>();
                    match selected {
                        0 => continue,
                        n if n == input_count => input_count,
                        n => {
                            let indices = &mut self.coordinates[coordinate];
                            indices.clear();
                            for (i, &keep) in root_mask[..input_count].iter().enumerate() {
                                if keep != 0 {
                                    indices.push(first + i);
                                }
                            }
                            self.span.as_mut().unwrap().lanes = InputLanes::Selected;
                            n
                        }
                    }
                };
                self.span.as_mut().unwrap().leaves = leaf_mask;
                self.compute(count, None);
                if !unconditional {
                    for (mask, &leaf_index) in masks.iter_mut().zip(leaves) {
                        mask[..count].fill(1);
                        let leaf = &self.program.leaves[leaf_index];
                        for (guard, expected) in leaf.guards.iter().copied() {
                            apply_predicate(&mut mask[..count], &self.workspace[guard], expected);
                        }
                    }
                }
                if let Output::Fold(out) = &mut self.output {
                    let entries = &mut self.fold_entries;
                    entries.clear();
                    for lane in 0..count {
                        let index = self.span.unwrap().index(&self.coordinates, lane);
                        while offsets[output_row + 1] as usize <= index {
                            output_row += 1;
                        }
                        for (column, &leaf) in leaves.iter().enumerate() {
                            if unconditional || masks[column][lane] != 0 {
                                entries.push((output_row, lane, leaf));
                            }
                        }
                    }
                    out.append(&entries, self.program, &self.workspace, &self.reads, count);
                    continue;
                }
                let written = if unconditional {
                    count * leaves.len()
                } else {
                    masks
                        .iter()
                        .map(|mask| mask[..count].iter().map(|&v| v as usize).sum::<usize>())
                        .sum()
                };
                let next = self
                    .output_len
                    .checked_add(written)
                    .filter(|&n| n <= i32::MAX as usize)
                    .ok_or_else(|| {
                        DataFusionError::Execution("flatmap output exceeds List32 capacity".into())
                    })?;
                let all_valid = leaves
                    .iter()
                    .all(|&leaf| self.workspace[self.program.leaves[leaf].value].all_valid);
                macro_rules! append {
                    ($out:expr, $getter:ident, $native:ty) => {{
                        let mut values: [&[$native]; MAX_LEAVES] = [&[]; MAX_LEAVES];
                        let mut scalar = [false; MAX_LEAVES];
                        for (column, &leaf) in leaves.iter().enumerate() {
                            let source = &self.workspace[self.program.leaves[leaf].value];
                            values[column] = source.$getter(&self.reads, count);
                            scalar[column] = source.scalar;
                        }
                        if all_valid && unconditional && leaves.len() == 1 {
                            if scalar[0] {
                                $out.append_value_n(values[0][0], count);
                            } else {
                                $out.append_slice(values[0]);
                            }
                        } else if all_valid {
                            let begin = self.output_len;
                            $out.append_value_n(<$native>::default(), written);
                            let target = &mut $out.values_slice_mut()[begin..next];
                            if unconditional {
                                for column in 0..leaves.len() {
                                    if scalar[column] {
                                        for entry in target.chunks_exact_mut(leaves.len()) {
                                            entry[column] = values[column][0];
                                        }
                                    } else {
                                        for (entry, &value) in target
                                            .chunks_exact_mut(leaves.len())
                                            .zip(values[column])
                                        {
                                            entry[column] = value;
                                        }
                                    }
                                }
                            } else if leaves.len() == 1 {
                                let mut at = 0;
                                if scalar[0] {
                                    target.fill(values[0][0]);
                                } else {
                                    for (&keep, &value) in masks[0][..count].iter().zip(values[0]) {
                                        if keep != 0 {
                                            target[at] = value;
                                            at += 1;
                                        }
                                    }
                                }
                            } else {
                                let mut at = 0;
                                for i in 0..count {
                                    for column in 0..leaves.len() {
                                        if masks[column][i] != 0 {
                                            target[at] =
                                                values[column][if scalar[column] { 0 } else { i }];
                                            at += 1;
                                        }
                                    }
                                }
                            }
                        } else {
                            for i in 0..count {
                                for (column, &leaf) in leaves.iter().enumerate() {
                                    if unconditional || masks[column][i] != 0 {
                                        let source =
                                            &self.workspace[self.program.leaves[leaf].value];
                                        $out.append_option(source.valid(i).then_some(
                                            values[column][if scalar[column] { 0 } else { i }],
                                        ));
                                    }
                                }
                            }
                        }
                    }};
                }
                match &mut self.output {
                    Output::I32(out) => append!(out, i32, i32),
                    Output::I64(out) => append!(out, i64, i64),
                    Output::F32(out) => append!(out, f32, f32),
                    Output::F64(out) => append!(out, f64, f64),
                    Output::Fold(_) => unreachable!(),
                    Output::Nested(out) => {
                        let entries = &mut self.emissions;
                        entries.clear();
                        for lane in 0..count {
                            for (column, &leaf) in leaves.iter().enumerate() {
                                if unconditional || masks[column][lane] != 0 {
                                    entries.push((lane, leaf));
                                }
                            }
                        }
                        append_nested(
                            out.as_mut(),
                            self.program,
                            &self.reads,
                            &self.workspace,
                            &self.coordinates,
                            self.span,
                            count,
                            entries,
                            &mut self.output_validity,
                        )?;
                    }
                    Output::Bool(out) => {
                        for i in 0..count {
                            for (column, &leaf) in leaves.iter().enumerate() {
                                if unconditional || masks[column][i] != 0 {
                                    let source = &self.workspace[self.program.leaves[leaf].value];
                                    out.append_option(source.valid(i).then(|| source.true_at(i)));
                                }
                            }
                        }
                    }
                }
                self.output_len = next;
                // Original input indices remain ordered after guard selection.
                if matches!(self.span.unwrap().lanes, InputLanes::Selected) {
                    for (i, &index) in self.coordinates[coordinate].iter().enumerate() {
                        while offsets[output_row + 1] as usize <= index {
                            output_row += 1;
                        }
                        self.output_counts[output_row] += if unconditional {
                            leaves.len()
                        } else {
                            masks.iter().map(|mask| mask[i] as usize).sum()
                        };
                    }
                } else {
                    let mut at = first;
                    while at < first + count {
                        while offsets[output_row + 1] as usize <= at {
                            output_row += 1;
                        }
                        let stop = (offsets[output_row + 1] as usize).min(first + count);
                        self.output_counts[output_row] += if unconditional {
                            (stop - at) * leaves.len()
                        } else {
                            masks
                                .iter()
                                .map(|mask| {
                                    mask[at - first..stop - first]
                                        .iter()
                                        .map(|&v| v as usize)
                                        .sum::<usize>()
                                })
                                .sum()
                        };
                        at = stop;
                    }
                }
            }
        }
        self.span = None;
        Ok(())
    }
    fn visit(&mut self, node: &Node) -> Result<()> {
        match node {
            Node::Concat(nodes) => {
                for node in nodes {
                    self.visit(node)?;
                }
            }
            Node::Emit(leaf) => {
                self.leaves.push(*leaf);
                for (slot, values) in self.coordinates.iter_mut().enumerate() {
                    values.push(self.positions[slot]);
                }
                if self.leaves.len() == self.capacity {
                    self.flush()?;
                }
            }
            Node::Loop {
                read,
                coordinate,
                guards,
                guard_mask,
                body,
            } => {
                let source = &self.reads[*read];
                if !source.valid(&self.positions) {
                    return Ok(());
                }
                let lists = source.values.as_any().downcast_ref::<ListArray>().unwrap();
                let index = source.index(&self.positions);
                let start = lists.value_offsets()[index] as usize;
                let end = lists.value_offsets()[index + 1] as usize;
                if guards.is_empty() {
                    if let Node::Emit(leaf) = body.as_ref() {
                        let mut index = start;
                        while index < end {
                            let count = (end - index).min(self.capacity - self.leaves.len());
                            self.leaves.extend(std::iter::repeat_n(*leaf, count));
                            for (slot, values) in self.coordinates.iter_mut().enumerate() {
                                if slot == *coordinate {
                                    values.extend(index..index + count);
                                } else {
                                    values.extend(std::iter::repeat_n(self.positions[slot], count));
                                }
                            }
                            index += count;
                            if self.leaves.len() == self.capacity {
                                self.flush()?;
                            }
                        }
                    } else {
                        for index in start..end {
                            self.positions[*coordinate] = index;
                            self.visit(body)?;
                        }
                    }
                } else {
                    for first in (start..end).step_by(self.capacity) {
                        self.flush()?;
                        let count = (end - first).min(self.capacity);
                        for (slot, values) in self.coordinates.iter_mut().enumerate() {
                            values.clear();
                            if slot == *coordinate {
                                values.extend(first..first + count);
                            } else {
                                values.resize(count, self.positions[slot]);
                            }
                        }
                        self.compute(count, Some(*guard_mask));
                        for i in 0..count {
                            self.guard_results[*coordinate][i] =
                                guards.iter().all(|&guard| self.workspace[guard].true_at(i));
                        }
                        for values in &mut self.coordinates {
                            values.clear();
                        }
                        for i in 0..count {
                            if self.guard_results[*coordinate][i] {
                                self.positions[*coordinate] = first + i;
                                self.visit(body)?;
                            }
                        }
                    }
                }
            }
            Node::Filter { .. } => unreachable!("filters are prepared in their input domain"),
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        let count = self.leaves.len();
        if count == 0 {
            return Ok(());
        }
        self.compute(count, None);
        let uniform = self.leaves.iter().all(|&leaf| leaf == self.leaves[0]);
        let active_leaves = self
            .leaves
            .iter()
            .fold(0u64, |mask, &leaf| mask | (1 << leaf));
        macro_rules! append {
            ($out:expr,$getter:ident,$native:ty) => {{
                let mut values: [&[$native]; MAX_LEAVES] = [&[]; MAX_LEAVES];
                for (index, leaf) in self.program.leaves.iter().enumerate() {
                    if active_leaves & (1 << index) != 0 {
                        values[index] = self.workspace[leaf.value].$getter(&self.reads, count);
                    }
                }
                let leaf = &self.program.leaves[self.leaves[0]];
                if uniform && leaf.guards.is_empty() && self.workspace[leaf.value].all_valid {
                    let next = self
                        .output_len
                        .checked_add(count)
                        .filter(|&len| len <= i32::MAX as usize)
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "flatmap output exceeds List32 capacity".into(),
                            )
                        })?;
                    if self.workspace[leaf.value].scalar {
                        $out.append_value_n(values[self.leaves[0]][0], count);
                    } else {
                        $out.append_slice(values[self.leaves[0]]);
                    }
                    self.output_len = next;
                    let mut rows = self.coordinates[0].as_slice();
                    while !rows.is_empty() {
                        let row = rows[0];
                        let n = rows.partition_point(|&value| value == row);
                        self.output_counts[row] += n;
                        rows = &rows[n..];
                    }
                } else {
                    let mut written = 0usize;
                    let mut row = self.coordinates[0][0];
                    let mut row_count = 0usize;
                    let dense_predicate = if uniform
                        && leaf.guards.len() == 1
                        && self.workspace[leaf.value].all_valid
                        && !self.workspace[leaf.value].scalar
                    {
                        let (guard, expected) = leaf.guards[0];
                        let predicate = &self.workspace[guard];
                        (predicate.all_valid && !predicate.scalar)
                            .then(|| (predicate.boolean(count), expected))
                    } else {
                        None
                    };
                    if let Some((predicate, expected)) = dense_predicate {
                        for ((&predicate, &value), &current_row) in predicate
                            .iter()
                            .zip(values[self.leaves[0]])
                            .zip(&self.coordinates[0])
                        {
                            if current_row != row {
                                self.output_counts[row] += row_count;
                                row = current_row;
                                row_count = 0;
                            }
                            if (predicate != 0) == expected {
                                if self.output_len + written == i32::MAX as usize {
                                    return Err(DataFusionError::Execution(
                                        "flatmap output exceeds List32 capacity".into(),
                                    ));
                                }
                                $out.append_value(value);
                                written += 1;
                                row_count += 1;
                            }
                        }
                    } else {
                        for i in 0..count {
                            let current_row = self.coordinates[0][i];
                            if current_row != row {
                                self.output_counts[row] += row_count;
                                row = current_row;
                                row_count = 0;
                            }
                            let leaf_index = self.leaves[i];
                            let leaf = &self.program.leaves[leaf_index];
                            if leaf.guards.iter().all(|&(guard, expected)| {
                                self.workspace[guard].valid(i)
                                    && self.workspace[guard].true_at(i) == expected
                            }) {
                                if self.output_len + written == i32::MAX as usize {
                                    return Err(DataFusionError::Execution(
                                        "flatmap output exceeds List32 capacity".into(),
                                    ));
                                }
                                let source = &self.workspace[leaf.value];
                                let value = values[leaf_index][if source.scalar { 0 } else { i }];
                                $out.append_option(source.valid(i).then_some(value));
                                written += 1;
                                row_count += 1;
                            }
                        }
                    }
                    self.output_len += written;
                    self.output_counts[row] += row_count;
                }
            }};
        }
        match &mut self.output {
            Output::I32(out) => append!(out, i32, i32),
            Output::I64(out) => append!(out, i64, i64),
            Output::F32(out) => append!(out, f32, f32),
            Output::F64(out) => append!(out, f64, f64),
            Output::Fold(out) => {
                let entries = &mut self.fold_entries;
                entries.clear();
                for lane in 0..count {
                    let leaf_index = self.leaves[lane];
                    let leaf = &self.program.leaves[leaf_index];
                    if leaf.guards.iter().all(|&(guard, expected)| {
                        self.workspace[guard].valid(lane)
                            && self.workspace[guard].true_at(lane) == expected
                    }) {
                        entries.push((self.coordinates[0][lane], lane, leaf_index));
                    }
                }
                out.append(entries, self.program, &self.workspace, &self.reads, count);
            }
            Output::Nested(out) => {
                let entries = &mut self.emissions;
                entries.clear();
                for lane in 0..count {
                    let leaf_index = self.leaves[lane];
                    if self.program.leaves[leaf_index]
                        .guards
                        .iter()
                        .all(|&(guard, expected)| {
                            self.workspace[guard].valid(lane)
                                && self.workspace[guard].true_at(lane) == expected
                        })
                    {
                        entries.push((lane, leaf_index));
                    }
                }
                let next = self
                    .output_len
                    .checked_add(entries.len())
                    .filter(|&n| n <= i32::MAX as usize)
                    .ok_or_else(|| {
                        DataFusionError::Execution("flatmap output exceeds List32 capacity".into())
                    })?;
                append_nested(
                    out.as_mut(),
                    self.program,
                    &self.reads,
                    &self.workspace,
                    &self.coordinates,
                    self.span,
                    count,
                    entries,
                    &mut self.output_validity,
                )?;
                self.output_len = next;
                for &(lane, _) in entries.iter() {
                    self.output_counts[self.coordinates[0][lane]] += 1;
                }
            }
            Output::Bool(out) => {
                for i in 0..count {
                    let leaf = &self.program.leaves[self.leaves[i]];
                    if leaf.guards.iter().all(|&(guard, expected)| {
                        self.workspace[guard].valid(i)
                            && self.workspace[guard].true_at(i) == expected
                    }) {
                        if self.output_len == i32::MAX as usize {
                            return Err(DataFusionError::Execution(
                                "flatmap output exceeds List32 capacity".into(),
                            ));
                        }
                        let source = &self.workspace[leaf.value];
                        out.append_option(source.valid(i).then(|| source.true_at(i)));
                        self.output_len += 1;
                        self.output_counts[self.coordinates[0][i]] += 1;
                    }
                }
            }
        }
        self.leaves.clear();
        for values in &mut self.coordinates {
            values.clear();
        }
        Ok(())
    }
    fn compute(&mut self, count: usize, guard: Option<u64>) {
        if let Some(span) = self.span {
            self.domains.fill((true, false, 0));
            self.domains[span.coordinate] = match span.lanes {
                InputLanes::Contiguous(start) => (false, true, start),
                InputLanes::Selected => (false, false, self.coordinates[span.coordinate][0]),
            };
        } else {
            for (values, domain) in self.coordinates.iter().zip(&mut self.domains) {
                let first = values[0];
                let constant = values.iter().all(|&v| v == first);
                *domain = (
                    constant,
                    !constant && values.iter().enumerate().all(|(i, &v)| v == first + i),
                    first,
                );
            }
        }
        let leaf = self.leaves.first().copied().unwrap_or(0);
        let uniform = self.span.is_some()
            || guard.is_some()
            || self.leaves.iter().all(|&leaf| leaf == self.leaves[0]);
        for (index, register) in self.program.registers.iter().enumerate() {
            let needed = if let Some(span) = self.span {
                register.leaves & span.leaves != 0
                    || guard.is_some_and(|mask| register.guards & mask != 0)
            } else {
                !uniform
                    || guard.map_or_else(
                        || register.leaves & (1 << leaf) != 0,
                        |mask| register.guards & mask != 0,
                    )
            };
            if !needed || (guard.is_none() && self.reuse_guards && register.guards != 0) {
                continue;
            }
            let (before, after) = self.workspace.split_at_mut(index);
            let out = &mut after[0];
            out.view = None;
            out.scalar = false;
            let active = |i: usize| uniform || register.leaves & (1 << self.leaves[i]) != 0;
            macro_rules! numeric {
                ($variant:ident,$getter:ident,$operation:expr,$a:expr,$b:expr) => {{
                    let a = &before[$a];
                    let b = &before[$b];
                    out.scalar = a.scalar && b.scalar;
                    let n = if out.scalar { 1 } else { count };
                    let av = a.$getter(&self.reads, count);
                    let bv = b.$getter(&self.reads, count);
                    let Values::$variant(values) = &mut out.values else {
                        unreachable!()
                    };
                    let values = &mut values[..n];
                    match (a.scalar, b.scalar) {
                        (true, true) => values[0] = ($operation)(av[0], bv[0]),
                        (true, false) => {
                            for (out, &b) in values.iter_mut().zip(bv) {
                                *out = ($operation)(av[0], b);
                            }
                        }
                        (false, true) => {
                            for (out, &a) in values.iter_mut().zip(av) {
                                *out = ($operation)(a, bv[0]);
                            }
                        }
                        (false, false) => {
                            for ((out, &a), &b) in values.iter_mut().zip(av).zip(bv) {
                                *out = ($operation)(a, b);
                            }
                        }
                    }
                    out.all_valid = a.all_valid && b.all_valid;
                    if !out.all_valid {
                        for i in 0..n {
                            out.valid[i] = u8::from(a.valid(i) && b.valid(i));
                        }
                    }
                }};
            }
            macro_rules! arithmetic {
                ($method:ident, $op:tt, $a:expr, $b:expr) => {
                    match register.kind {
                        Kind::I32 => numeric!(I32, i32, |a: i32, b: i32| a.$method(b), $a, $b),
                        Kind::I64 => numeric!(I64, i64, |a: i64, b: i64| a.$method(b), $a, $b),
                        Kind::F32 => numeric!(F32, f32, |a: f32, b: f32| a $op b, $a, $b),
                        Kind::F64 => numeric!(F64, f64, |a: f64, b: f64| a $op b, $a, $b),
                        _ => unreachable!(),
                    }
                };
            }
            macro_rules! compare {
                ($op:tt, $a:expr, $b:expr) => {{
                    let a = &before[$a];
                    let b = &before[$b];
                    out.scalar = a.scalar && b.scalar;
                    let n = if out.scalar { 1 } else { count };
                    let Values::Bool(values) = &mut out.values else { unreachable!() };
                    let values = &mut values[..n];
                    macro_rules! compare_values {
                        ($av:expr, $bv:expr) => {{
                            let av = $av;
                            let bv = $bv;
                            match (a.scalar, b.scalar) {
                                (true, true) => values[0] = u8::from(av[0] $op bv[0]),
                                (true, false) => {
                                    for (out, &value) in values.iter_mut().zip(bv) {
                                        *out = u8::from(av[0] $op value);
                                    }
                                }
                                (false, true) => {
                                    for (out, &value) in values.iter_mut().zip(av) {
                                        *out = u8::from(value $op bv[0]);
                                    }
                                }
                                (false, false) => {
                                    for ((out, &a), &b) in values.iter_mut().zip(av).zip(bv) {
                                        *out = u8::from(a $op b);
                                    }
                                }
                            }
                        }};
                    }
                    match self.program.registers[$a].kind {
                        Kind::I32 => compare_values!(a.i32(&self.reads, count), b.i32(&self.reads, count)),
                        Kind::I64 => compare_values!(a.i64(&self.reads, count), b.i64(&self.reads, count)),
                        Kind::Bool => compare_values!(a.boolean(count), b.boolean(count)),
                        _ => unreachable!(),
                    }
                    out.all_valid = a.all_valid && b.all_valid;
                    if !out.all_valid {
                        for i in 0..n { out.valid[i] = u8::from(a.valid(i) && b.valid(i)); }
                    }
                }};
            }
            match register.op {
                Op::Constant(cell) => {
                    out.scalar = true;
                    out.all_valid = cell.valid;
                    out.valid[0] = u8::from(cell.valid);
                    match &mut out.values {
                        Values::I32(v) => v[0] = cell.bits as i32,
                        Values::I64(v) => v[0] = cell.bits as i64,
                        Values::F32(v) => v[0] = f32::from_bits(cell.bits as u32),
                        Values::F64(v) => v[0] = f64::from_bits(cell.bits),
                        Values::Bool(v) => v[0] = cell.bits as u8,
                    }
                }
                Op::Read(read) => {
                    let source = &self.reads[read];
                    let (constant, contiguous, first) = self.domains[source.coordinate];
                    out.scalar = uniform && ((source.scalar && source.coordinate == 0) || constant);
                    let n = if out.scalar { 1 } else { count };
                    let first = if source.scalar && source.coordinate == 0 {
                        0
                    } else {
                        first
                    };
                    if (out.scalar || contiguous) && uniform && register.kind != Kind::Bool {
                        out.view = Some((read, first));
                    } else {
                        macro_rules! read {
                            ($array:ty,$variant:ident,$convert:expr) => {{
                                let values =
                                    source.values.as_any().downcast_ref::<$array>().unwrap();
                                let Values::$variant(output) = &mut out.values else {
                                    unreachable!()
                                };
                                for i in 0..n {
                                    if active(i) {
                                        let at = if let Some(span) = self
                                            .span
                                            .filter(|span| span.coordinate == source.coordinate)
                                        {
                                            span.index(&self.coordinates, i)
                                        } else if source.scalar && source.coordinate == 0 {
                                            0
                                        } else if out.scalar {
                                            first
                                        } else {
                                            self.coordinates[source.coordinate][i]
                                        };
                                        output[i] = ($convert)(values.value(at));
                                    }
                                }
                            }};
                        }
                        match register.kind {
                            Kind::I32 => read!(Int32Array, I32, |v| v),
                            Kind::I64 => read!(Int64Array, I64, |v| v),
                            Kind::F32 => read!(Float32Array, F32, |v| v),
                            Kind::F64 => read!(Float64Array, F64, |v| v),
                            Kind::Bool => read!(BooleanArray, Bool, |v| u8::from(v)),
                        }
                    }
                    let span = self
                        .span
                        .filter(|span| span.coordinate == source.coordinate);
                    out.all_valid = source.all_valid_in_span(span.map(|span| span.coordinate));
                    if !out.all_valid {
                        for i in 0..n {
                            out.valid[i] = if active(i) {
                                let at = if let Some(span) = span {
                                    span.index(&self.coordinates, i)
                                } else if source.scalar && source.coordinate == 0 {
                                    0
                                } else if out.scalar {
                                    first
                                } else {
                                    self.coordinates[source.coordinate][i]
                                };
                                u8::from(
                                    source.nulls.as_ref().is_none_or(|mask| mask.is_valid(at))
                                        && source.ancestors.iter().all(|(mask, slot)| {
                                            (span.is_some() && *slot != source.coordinate)
                                                || mask.is_valid(if let Some(span) = span {
                                                    span.index(&self.coordinates, i)
                                                } else if source.scalar && *slot == 0 {
                                                    0
                                                } else {
                                                    self.coordinates[*slot][i]
                                                })
                                        }),
                                )
                            } else {
                                0
                            };
                        }
                    }
                    if uniform && !out.all_valid {
                        out.all_valid = out.valid[..n].iter().all(|&valid| valid != 0);
                    }
                }
                Op::MissingRead(read) | Op::Length(read) => {
                    let source = &self.reads[read];
                    out.scalar = uniform
                        && ((source.scalar && source.coordinate == 0)
                            || self.domains[source.coordinate].0);
                    let n = if out.scalar { 1 } else { count };
                    out.all_valid = matches!(register.op, Op::MissingRead(_));
                    let span = self
                        .span
                        .filter(|span| span.coordinate == source.coordinate);
                    if matches!(register.op, Op::MissingRead(_))
                        && source.all_valid_in_span(span.map(|span| span.coordinate))
                    {
                        out.scalar = true;
                        let Values::Bool(values) = &mut out.values else { unreachable!() };
                        values[0] = 0;
                        continue;
                    }
                    let valid_at = |i: usize| {
                        let at = if let Some(span) = span {
                            span.index(&self.coordinates, i)
                        } else if source.scalar && source.coordinate == 0 {
                            0
                        } else {
                            self.coordinates[source.coordinate][i]
                        };
                        let valid = source.all_valid
                            || (source.nulls.as_ref().is_none_or(|mask| mask.is_valid(at))
                                && source.ancestors.iter().all(|(mask, slot)| {
                                    (span.is_some() && *slot != source.coordinate)
                                        || mask.is_valid(if let Some(span) = span {
                                            span.index(&self.coordinates, i)
                                        } else if source.scalar && *slot == 0 {
                                            0
                                        } else {
                                            self.coordinates[*slot][i]
                                        })
                                }));
                        (at, valid)
                    };
                    match &mut out.values {
                        Values::I32(values) => {
                            let lists = source.values.as_any().downcast_ref::<ListArray>().unwrap();
                            for i in 0..n {
                                if active(i) {
                                    let (at, valid) = valid_at(i);
                                    values[i] = lists.value_length(at);
                                    out.valid[i] = u8::from(valid);
                                }
                            }
                        }
                        Values::Bool(values) => {
                            for i in 0..n {
                                if active(i) {
                                    values[i] = u8::from(!valid_at(i).1);
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Op::Missing(a) => {
                    let a = &before[a];
                    out.scalar = a.scalar || a.all_valid;
                    out.all_valid = true;
                    let n = if out.scalar { 1 } else { count };
                    let Values::Bool(values) = &mut out.values else {
                        unreachable!()
                    };
                    for i in 0..n {
                        values[i] = u8::from(!a.valid(i));
                    }
                }
                Op::OrderKey(a) => {
                    let a = &before[a];
                    out.scalar = a.scalar;
                    out.all_valid = a.all_valid;
                    let n = if out.scalar { 1 } else { count };
                    if !out.all_valid { out.valid[..n].copy_from_slice(&a.valid[..n]); }
                    match register.kind {
                        Kind::I32 => {
                            let Values::I32(values) = &mut out.values else { unreachable!() };
                            for (out, &value) in values[..n].iter_mut().zip(a.f32(&self.reads, count)) {
                                *out = float32_order(value);
                            }
                        }
                        Kind::I64 => {
                            let Values::I64(values) = &mut out.values else { unreachable!() };
                            for (out, &value) in values[..n].iter_mut().zip(a.f64(&self.reads, count)) {
                                *out = float64_order(value);
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Op::Cast(a) => {
                    let source_kind = self.program.registers[a].kind;
                    let a = &before[a];
                    out.scalar = a.scalar;
                    out.all_valid = a.all_valid;
                    let n = if out.scalar { 1 } else { count };
                    if !out.all_valid {
                        out.valid[..n].copy_from_slice(&a.valid[..n]);
                    }
                    macro_rules! cast {
                        ($variant:ident,$getter:ident,$ty:ty) => {{
                            let Values::$variant(values) = &mut out.values else {
                                unreachable!()
                            };
                            for (out, &value) in
                                values[..n].iter_mut().zip(a.$getter(&self.reads, count))
                            {
                                *out = value as $ty;
                            }
                        }};
                    }
                    match (source_kind, register.kind) {
                        (Kind::I32, Kind::I64) => cast!(I64, i32, i64),
                        (Kind::I32, Kind::F32) => cast!(F32, i32, f32),
                        (Kind::I32, Kind::F64) => cast!(F64, i32, f64),
                        (Kind::I64, Kind::F32) => cast!(F32, i64, f32),
                        (Kind::I64, Kind::F64) => cast!(F64, i64, f64),
                        (Kind::F32, Kind::F64) => cast!(F64, f32, f64),
                        _ => unreachable!(),
                    }
                }
                Op::Unary(operator, a) => {
                    let a = &before[a];
                    out.scalar = a.scalar;
                    out.all_valid = a.all_valid;
                    let n = if out.scalar { 1 } else { count };
                    if !out.all_valid {
                        out.valid[..n].copy_from_slice(&a.valid[..n]);
                    }
                    macro_rules! unary {
                        ($variant:ident,$getter:ident,$op:expr) => {{
                            let Values::$variant(values) = &mut out.values else {
                                unreachable!()
                            };
                            for (out, &value) in
                                values[..n].iter_mut().zip(a.$getter(&self.reads, count))
                            {
                                *out = ($op)(value);
                            }
                        }};
                    }
                    match (operator, register.kind) {
                        (Unary::Pos, Kind::I32) => unary!(I32, i32, |v| v),
                        (Unary::Pos, Kind::I64) => unary!(I64, i64, |v| v),
                        (Unary::Pos, Kind::F32) => unary!(F32, f32, |v| v),
                        (Unary::Pos, Kind::F64) => unary!(F64, f64, |v| v),
                        (Unary::Neg, Kind::I32) => unary!(I32, i32, |v: i32| v.wrapping_neg()),
                        (Unary::Neg, Kind::I64) => unary!(I64, i64, |v: i64| v.wrapping_neg()),
                        (Unary::Neg, Kind::F32) => unary!(F32, f32, |v: f32| -v),
                        (Unary::Neg, Kind::F64) => unary!(F64, f64, |v: f64| -v),
                        (Unary::Not, Kind::Bool) => {
                            let Values::Bool(values) = &mut out.values else {
                                unreachable!()
                            };
                            for (out, &v) in values[..n].iter_mut().zip(a.boolean(count)) {
                                *out = u8::from(v == 0);
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Op::Binary(operator, a, b) => match operator {
                    Binary::Add => arithmetic!(wrapping_add,+,a,b),
                    Binary::Sub => arithmetic!(wrapping_sub,-,a,b),
                    Binary::Mul => arithmetic!(wrapping_mul,*,a,b),
                    Binary::Eq => compare!(==,a,b),
                    Binary::Ne => compare!(!=,a,b),
                    Binary::Lt => compare!(<,a,b),
                    Binary::Le => compare!(<=,a,b),
                    Binary::Gt => compare!(>,a,b),
                    Binary::Ge => compare!(>=,a,b),
                    Binary::Mod => match register.kind {
                        Kind::I32 => numeric!(
                            I32,
                            i32,
                            |a: i32, b: i32| {
                                let r = a.wrapping_rem(b);
                                r.wrapping_add(if r != 0 && (a < 0) != (b < 0) { b } else { 0 })
                            },
                            a,
                            b
                        ),
                        Kind::I64 => numeric!(
                            I64,
                            i64,
                            |a: i64, b: i64| {
                                let r = a.wrapping_rem(b);
                                r.wrapping_add(if r != 0 && (a < 0) != (b < 0) { b } else { 0 })
                            },
                            a,
                            b
                        ),
                        _ => unreachable!(),
                    },
                    Binary::Div => match register.kind {
                        Kind::F32 => numeric!(F32, f32, |a: f32, b: f32| a / b, a, b),
                        Kind::F64 => numeric!(F64, f64, |a: f64, b: f64| a / b, a, b),
                        _ => unreachable!(),
                    },
                    Binary::And | Binary::Or => {
                        let a = &before[a];
                        let b = &before[b];
                        out.scalar = a.scalar && b.scalar;
                        let n = if out.scalar { 1 } else { count };
                        out.all_valid = false;
                        let Values::Bool(values) = &mut out.values else {
                            unreachable!()
                        };
                        for i in 0..n {
                            let av = a.true_at(i);
                            let bv = b.true_at(i);
                            let decisive = if operator == Binary::And {
                                (a.valid(i) && !av) || (b.valid(i) && !bv)
                            } else {
                                av || bv
                            };
                            out.valid[i] = u8::from(decisive || (a.valid(i) && b.valid(i)));
                            values[i] = u8::from(if operator == Binary::And {
                                av && bv
                            } else {
                                av || bv
                            });
                        }
                    }
                    _ => unreachable!(),
                },
                Op::Select(c, a, b) => {
                    let c = &before[c];
                    let a = &before[a];
                    let b = &before[b];
                    out.scalar = c.scalar && a.scalar && b.scalar;
                    let n = if out.scalar { 1 } else { count };
                    out.all_valid = false;
                    macro_rules! select {
                        ($variant:ident,$getter:ident) => {{
                            let av = a.$getter(&self.reads, count);
                            let bv = b.$getter(&self.reads, count);
                            let Values::$variant(values) = &mut out.values else {
                                unreachable!()
                            };
                            for i in 0..n {
                                let yes = c.true_at(i);
                                values[i] = if yes {
                                    av[if a.scalar { 0 } else { i }]
                                } else {
                                    bv[if b.scalar { 0 } else { i }]
                                };
                                out.valid[i] = u8::from(
                                    c.valid(i) && if yes { a.valid(i) } else { b.valid(i) },
                                );
                            }
                        }};
                    }
                    match register.kind {
                        Kind::I32 => select!(I32, i32),
                        Kind::I64 => select!(I64, i64),
                        Kind::F32 => select!(F32, f32),
                        Kind::F64 => select!(F64, f64),
                        Kind::Bool => {
                            let Values::Bool(values) = &mut out.values else {
                                unreachable!()
                            };
                            for i in 0..n {
                                let selected = if c.true_at(i) { a } else { b };
                                values[i] = u8::from(selected.true_at(i));
                                out.valid[i] = u8::from(c.valid(i) && selected.valid(i));
                            }
                        }
                    }
                }
            }
        }
    }
}

fn bind_reads(program: &ListFlatMap, batch: &RecordBatch) -> Result<Vec<BoundRead>> {
    let children = program
        .children
        .iter()
        .map(|child| child.evaluate(batch))
        .collect::<Result<Vec<_>>>()?;
    let children = children
        .into_iter()
        .map(|value| match value {
            ColumnarValue::Array(array) => Ok((array, false)),
            ColumnarValue::Scalar(value) => Ok((value.to_array()?, true)),
        })
        .collect::<Result<Vec<_>>>()?;
    for ((child, scalar), expected) in children.iter().zip(&program.child_types) {
        if child.data_type() != expected || (!scalar && child.len() != batch.num_rows()) {
            return Err(DataFusionError::Execution(
                "flatmap source descriptor changed".into(),
            ));
        }
    }
    let mut reads = Vec::with_capacity(program.reads.len());
    for read in &program.reads {
        let (mut values, scalar) = children[read.child].clone();
        let mut ancestors = Vec::new();
        let mut coordinate = 0;
        for step in &read.path {
            if let Some(nulls) = values.nulls().filter(|mask| mask.null_count() != 0) {
                ancestors.push((nulls.clone(), coordinate));
            }
            values = match step {
                Path::Field(index) => values
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(*index)
                    .clone(),
                Path::Values(parent, child) => {
                    debug_assert_eq!(*parent, coordinate);
                    coordinate = *child;
                    values
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .values()
                        .clone()
                }
            };
        }
        let nulls = values.nulls().filter(|mask| mask.null_count() != 0).cloned();
        let all_valid = nulls.is_none() && ancestors.is_empty();
        reads.push(BoundRead {
            values,
            nulls,
            ancestors,
            coordinate: read.coordinate,
            scalar,
            all_valid,
        });
    }
    Ok(reads)
}

include!("list_flatmap_reductions_execute.rs");
