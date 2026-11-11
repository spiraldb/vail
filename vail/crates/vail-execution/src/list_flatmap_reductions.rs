// Shared block producers feeding compatible, same-owner list reductions.
use crate::expression_identity::Identity;
use arrow_schema::Fields;

fn numeric_kind(a: Kind, b: Kind) -> Kind {
    match (a, b) {
        (Kind::F64, _) | (_, Kind::F64) => Kind::F64,
        (Kind::F32, _) | (_, Kind::F32) => Kind::F32,
        (Kind::I64, _) | (_, Kind::I64) => Kind::I64,
        _ => Kind::I32,
    }
}
fn constant_nonzero(register: &Register) -> bool {
    let Op::Constant(value) = register.op else {
        return false;
    };
    value.valid
        && match register.kind {
            Kind::I32 => value.bits as i32 != 0,
            Kind::I64 => value.bits as i64 != 0,
            Kind::F32 => f32::from_bits(value.bits as u32) != 0.0,
            Kind::F64 => f64::from_bits(value.bits) != 0.0,
            Kind::Bool => false,
        }
}
// Match the ordinary comparison contract: zeros compare equally and NaNs use
// their normalized IEEE total-order keys. Shared producers normalize once.
fn float32_order(value: f32) -> i32 {
    let mut bits = (if value == 0.0 { 0.0 } else { value }).to_bits() as i32;
    bits ^= (((bits >> 31) as u32) >> 1) as i32;
    bits
}
fn float64_order(value: f64) -> i64 {
    let mut bits = (if value == 0.0 { 0.0 } else { value }).to_bits() as i64;
    bits ^= (((bits >> 63) as u64) >> 1) as i64;
    bits
}

impl Compiler<'_> {
    fn order_key(&mut self, source: usize) -> Option<usize> {
        let kind = match self.registers[source].kind {
            Kind::F32 => Kind::I32,
            Kind::F64 => Kind::I64,
            _ => return None,
        };
        let op = if let Op::Constant(mut value) = self.registers[source].op {
            if value.valid {
                value.bits = if kind == Kind::I32 {
                    float32_order(f32::from_bits(value.bits as u32)) as u64
                } else {
                    float64_order(f64::from_bits(value.bits)) as u64
                };
            }
            Op::Constant(value)
        } else { Op::OrderKey(source) };
        self.register(op, kind)
    }
    fn numeric_cast(&mut self, source: usize, kind: Kind) -> Option<usize> {
        let old = self.registers[source].kind;
        if old == kind {
            return Some(source);
        }
        if !matches!(
            (old, kind),
            (Kind::I32, Kind::I64 | Kind::F32 | Kind::F64)
                | (Kind::I64, Kind::F32 | Kind::F64)
                | (Kind::F32, Kind::F64)
        ) {
            return None;
        }
        if let Op::Constant(mut value) = self.registers[source].op {
            value.bits = match (old, kind) {
                (Kind::I32, Kind::I64) => value.bits as i32 as i64 as u64,
                (Kind::I32, Kind::F32) => (value.bits as i32 as f32).to_bits() as u64,
                (Kind::I32, Kind::F64) => (value.bits as i32 as f64).to_bits(),
                (Kind::I64, Kind::F32) => (value.bits as i64 as f32).to_bits() as u64,
                (Kind::I64, Kind::F64) => (value.bits as i64 as f64).to_bits(),
                (Kind::F32, Kind::F64) => (f32::from_bits(value.bits as u32) as f64).to_bits(),
                _ => unreachable!(),
            };
            self.register(Op::Constant(value), kind)
        } else {
            self.register(Op::Cast(source), kind)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ReductionSink {
    value: usize,
    fraction: bool,
}
struct Candidate<'a> {
    field: usize,
    owner: &'a Arc<Expr>,
    parameter: &'a str,
    body: &'a Arc<Expr>,
    fraction: bool,
}
fn candidate(field: usize, expr: &Expr) -> Option<Candidate<'_>> {
    let Expr::NestedFunction { function, args, .. } = expr else {
        return None;
    };
    let fraction = match function.as_str() {
        "__vail_list_reduce_count_where" => false,
        "__vail_list_reduce_fraction" => true,
        _ => return None,
    };
    let [value] = args.as_slice() else {
        return None;
    };
    let Expr::ArrayMap {
        value: owner,
        parameter,
        body,
    } = value
    else {
        return None;
    };
    fn supported(expr: &Expr, nodes: &mut usize) -> bool {
        *nodes += 1;
        if *nodes > 1024 {
            return false;
        }
        match expr {
            Expr::Column { .. }
            | Expr::LambdaVariable { .. }
            | Expr::Literal { .. }
            | Expr::RuntimeLiteral { .. } => true,
            Expr::GetField { value, .. }
            | Expr::ArrayLength { value }
            | Expr::IsMissing { value }
            | Expr::Cast { value, .. }
            | Expr::Unary { value, .. } => supported(value, nodes),
            Expr::Binary {
                left,
                right,
                operator,
            }
            | Expr::IntegerBinary {
                left,
                right,
                operator,
                ..
            } if !matches!(operator, Binary::Pow | Binary::Floordiv) => {
                supported(left, nodes) && supported(right, nodes)
            }
            Expr::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                supported(condition, nodes)
                    && supported(consequent, nodes)
                    && supported(alternate, nodes)
            }
            _ => false,
        }
    }
    supported(body, &mut 0).then_some(Candidate {
        field,
        owner,
        parameter,
        body,
        fraction,
    })
}
/// Cheap shape discovery runs before compiling a callback or walking its owner.
pub(crate) fn has_reduction_region(expression: &Expr) -> bool {
    let Expr::Struct { fields } = expression else {
        return false;
    };
    fields.len() <= MAX_LEAVES
        && fields
            .iter()
            .enumerate()
            .filter_map(|(i, (_, expr))| candidate(i, expr))
            .take(2)
            .count()
            == 2
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MultiReduce {
    program: ListFlatMap,
    sinks: Vec<ReductionSink>,
    guards: Vec<usize>,
    fields: Fields,
    // Each output is a fused sink or the next ordinary expression child.
    outputs: Vec<std::result::Result<usize, usize>>,
    ordinary: Vec<Arc<dyn PhysicalExpr>>,
    fallback: Option<Arc<dyn PhysicalExpr>>,
    workspace_bytes_per_lane: usize,
}
impl fmt::Display for MultiReduce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ListMultiReduce[sinks={}, registers={}]",
            self.sinks.len(),
            self.program.registers.len()
        )
    }
}
impl MultiReduce {
    pub(crate) fn normalize_ordinary<E>(
        &mut self,
        mut normalize: impl FnMut(
            Arc<dyn PhysicalExpr>,
            &DataType,
        ) -> std::result::Result<Arc<dyn PhysicalExpr>, E>,
    ) -> std::result::Result<(), E> {
        // Match ordinary Struct construction: prepare each field's physical
        // normalization once, including Utf8View and nested logical metadata.
        for (field, output) in self.fields.iter().zip(&self.outputs) {
            if let Err(index) = output {
                self.ordinary[*index] =
                    normalize(self.ordinary[*index].clone(), field.data_type())?;
            }
        }
        Ok(())
    }
    pub(crate) fn set_fallback(&mut self, fallback: Arc<dyn PhysicalExpr>) {
        self.fallback = Some(fallback);
    }
}
impl PhysicalExpr for MultiReduce {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Struct(self.fields.clone()))
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(false)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.fallback
            .iter()
            .chain(self.program.children.iter())
            .chain(self.ordinary.iter())
            .collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 + self.program.children.len() + self.ordinary.len() {
            return Err(DataFusionError::Internal(
                "multi reduction children changed".into(),
            ));
        }
        let n = self.program.children.len();
        if children[1..1 + n].iter().any(|child| !crate::expressions::is_frame_column(child)) {
            return Ok(children[0].clone());
        }
        let mut result = self.as_ref().clone();
        result.fallback = Some(children[0].clone());
        result.program.children = children[1..1 + n].to_vec();
        result.ordinary = children[1 + n..].to_vec();
        Ok(Arc::new(result))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Variable working payload (counters, masks, selected coordinates and
        // scalar buffers) shares the bound. Fixed Vec/Read descriptors, input
        // owners and final/ordinary outputs are separate. The
        // ordinary route is chosen before reading a source or running a body.
        let state_bytes = batch
            .num_rows()
            .checked_mul(self.sinks.len() + 1)
            .and_then(|n| n.checked_mul(8))
            .and_then(|n| n.checked_add(batch.num_rows()));
        if state_bytes
            .and_then(|n| n.checked_add(4096 + self.workspace_bytes_per_lane))
            .is_none_or(|n| n > execution::MAX_SCRATCH)
        {
            return self.fallback.as_ref().unwrap().evaluate(batch);
        }
        let mut reductions = None;
        let mut values = Vec::with_capacity(self.outputs.len());
        for output in &self.outputs {
            match output {
                Ok(index) => {
                    if reductions.is_none() {
                        reductions = Some(execution::reduce(self, batch, state_bytes.unwrap())?);
                    }
                    values.push(reductions.as_ref().unwrap()[*index].clone());
                }
                Err(index) => values.push(
                    self.ordinary[*index]
                        .evaluate(batch)?
                        .into_array(batch.num_rows())?,
                ),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(StructArray::try_new(
            self.fields.clone(),
            values,
            None,
        )?)))
    }
}

// Retain only sinks that benefit from shared work. Compact every descriptor
// once at preparation; dropped outputs use their ordinary expression path.
fn compact_reduction_region(
    compiler: &mut Compiler<'_>, sinks: &mut Vec<ReductionSink>, selected: &mut Vec<usize>,
    guards: &mut [usize], root_read: &mut usize, keep: u64,
) {
    let retained = (0..sinks.len()).filter(|&i| keep & (1 << i) != 0).collect::<Vec<_>>();
    let mut registers = Vec::new();
    let mut register_map = vec![usize::MAX; compiler.registers.len()];
    for (old, register) in compiler.registers.iter().enumerate() {
        if register.leaves & keep == 0 && register.guards == 0 { continue; }
        let mut register = register.clone();
        register.leaves = retained.iter().enumerate().fold(0, |mask, (new, &old)| {
            mask | (u64::from(register.leaves & (1 << old) != 0) << new)
        });
        register.op = match register.op {
            Op::Unary(op, a) => Op::Unary(op, register_map[a]),
            Op::Missing(a) => Op::Missing(register_map[a]),
            Op::Cast(a) => Op::Cast(register_map[a]),
            Op::OrderKey(a) => Op::OrderKey(register_map[a]),
            Op::Binary(op, a, b) => Op::Binary(op, register_map[a], register_map[b]),
            Op::Select(c, a, b) => Op::Select(register_map[c], register_map[a], register_map[b]),
            other => other,
        };
        register_map[old] = registers.len();
        registers.push(register);
    }
    let mut used_reads = vec![false; compiler.reads.len()];
    used_reads[*root_read] = true;
    for register in &registers {
        if let Op::Read(read) | Op::MissingRead(read) | Op::Length(read) = register.op {
            used_reads[read] = true;
        }
    }
    let mut used_children = vec![false; compiler.children.len()];
    for (read, used) in compiler.reads.iter().zip(&used_reads) {
        if *used { used_children[read.child] = true; }
    }
    let mut child_map = vec![usize::MAX; compiler.children.len()];
    let mut children = Vec::new();
    let mut child_types = Vec::new();
    for (old, &used) in used_children.iter().enumerate() {
        if used {
            child_map[old] = children.len();
            children.push(compiler.children[old].clone());
            child_types.push(compiler.child_types[old].clone());
        }
    }
    let mut read_map = vec![usize::MAX; compiler.reads.len()];
    let mut reads = Vec::new();
    for (old, &used) in used_reads.iter().enumerate() {
        if used {
            let mut read = compiler.reads[old].clone();
            read.child = child_map[read.child];
            read_map[old] = reads.len();
            reads.push(read);
        }
    }
    for register in &mut registers {
        register.op = match register.op {
            Op::Read(read) => Op::Read(read_map[read]),
            Op::MissingRead(read) => Op::MissingRead(read_map[read]),
            Op::Length(read) => Op::Length(read_map[read]),
            other => other,
        };
    }
    *sinks = retained.iter().map(|&old| ReductionSink {
        value: register_map[sinks[old].value], fraction: sinks[old].fraction,
    }).collect();
    *selected = retained.iter().map(|&old| selected[old]).collect();
    compiler.leaves = retained.iter().map(|&old| {
        let mut leaf = compiler.leaves[old].clone();
        leaf.value = register_map[leaf.value];
        leaf
    }).collect();
    for guard in guards { *guard = register_map[*guard]; }
    *root_read = read_map[*root_read];
    compiler.registers = registers;
    compiler.reads = reads;
    compiler.children = children;
    compiler.child_types = child_types;
}

pub(crate) fn prepare_reductions(
    expression: &Expr,
    dtype: &DataType,
    lower: &mut dyn FnMut(&Expr) -> Option<(Arc<dyn PhysicalExpr>, DataType)>,
) -> Option<MultiReduce> {
    if !has_reduction_region(expression) {
        return None;
    }
    let trace = std::env::var_os("VAIL_TRACE_SEMANTIC_PREP").is_some();
    macro_rules! decline {
        ($reason:expr) => {{
            if trace { eprintln!("MULTI_REDUCE_DECLINE {}", $reason); }
            return None;
        }};
    }
    let Expr::Struct { fields } = expression else { decline!("root_shape"); };
    let DataType::Struct(output_fields) = dtype else {
        return None;
    };
    let candidates = fields
        .iter()
        .enumerate()
        .filter_map(|(i, (_, expr))| candidate(i, expr))
        .collect::<Vec<_>>();
    // Only one same-owner region in this initial prototype. Different owner,
    // filter, or lexical contexts stay on their unchanged ordinary route.
    let owner = candidates.first()?.owner;
    if trace { eprintln!("MULTI_REDUCE_CANDIDATES count={} fields={}", candidates.len(), fields.len()); }
    if !owner.is_pure() { decline!("owner_effects"); }
    // Candidates retain every owner Arc while this query-local identity cache
    // is used. Addresses avoid rehashing the same DAG; equality still compares
    // the existing collision-safe structural identity for distinct owners.
    let mut identities = HashMap::new();
    let identity = Identity::with_bound_names(owner.as_ref().clone());
    identities.insert(Arc::as_ptr(owner) as usize, identity.clone());
    let candidates = candidates
        .into_iter()
        .filter(|candidate| {
            identities
                .entry(Arc::as_ptr(candidate.owner) as usize)
                .or_insert_with(|| Identity::with_bound_names(candidate.owner.as_ref().clone()))
                == &identity
        })
        .collect::<Vec<_>>();
    if candidates.len() < 2 { decline!("same_owner_count"); }
    let mut compiler = Compiler {
        lower,
        children: vec![],
        child_types: vec![],
        reads: vec![],
        registers: vec![],
        leaves: vec![],
        coordinates: 1,
        operations: 0,
        scalar_memo: HashMap::new(),
        environments: vec![],
        expressions: vec![],
        scalar_depth: 0,
        projections: HashMap::new(),
        output_nodes: 0,
        reduction_numeric: true,
    };
    let mut sinks = vec![];
    let mut selected = vec![];
    let mut emit = |c: &mut Compiler<'_>, value: Binding, guards: Vec<_>| {
        if !guards.is_empty() {
            return None;
        }
        for candidate in &candidates {
            let checkpoint = (
                c.children.len(),
                c.reads.len(),
                c.registers.len(),
                c.environments.len(),
                c.expressions.len(),
            );
            let env = c.environment(bind(&None, candidate.parameter, value.clone()));
            let compiled = c
                .scalar(candidate.body, &env)
                .filter(|&value| c.registers[value].kind == Kind::Bool);
            let Some(value) = compiled else {
                if trace { eprintln!("MULTI_REDUCE_FIELD_DECLINE field={}", candidate.field); }
                c.children.truncate(checkpoint.0);
                c.child_types.truncate(checkpoint.0);
                c.reads.truncate(checkpoint.1);
                c.registers.truncate(checkpoint.2);
                c.environments.truncate(checkpoint.3);
                c.expressions.truncate(checkpoint.4);
                c.scalar_memo.clear();
                c.projections.clear();
                continue;
            };
            sinks.push(ReductionSink {
                value,
                fraction: candidate.fraction,
            });
            selected.push(candidate.field);
            c.leaves.push(Leaf {
                value,
                guards: vec![],
                output: None,
            });
        }
        Some(Node::Concat((0..sinks.len()).map(Node::Emit).collect()))
    };
    let root = compiler.list(owner, &None, vec![], &mut emit);
    drop(emit);
    let Some(root) = root else { decline!("owner_list_compilation"); };
    if trace { eprintln!("MULTI_REDUCE_COMPILED sinks={} registers={} reads={} children={}", sinks.len(), compiler.registers.len(), compiler.reads.len(), compiler.children.len()); }
    if sinks.len() < 2 { decline!("compiled_sink_count"); }
    let Node::Loop {
        read: mut root_read,
        coordinate: 1,
        body,
        ..
    } = root
    else { decline!("owner_loop_shape"); };
    if compiler.coordinates != 2 { decline!("source_coordinates"); }
    let mut node = body.as_ref();
    let mut guards = vec![];
    while let Node::Filter { predicate, body } = node {
        guards.push(*predicate);
        node = body;
    }
    if !matches!(node, Node::Concat(nodes) if nodes.iter().all(|n| matches!(n, Node::Emit(_)))) {
        decline!("guard_body_shape");
    }
    fn mark(registers: &mut [Register], index: usize, bit: u64) {
        if registers[index].leaves & bit != 0 {
            return;
        }
        registers[index].leaves |= bit;
        match registers[index].op {
            Op::Unary(_, a) | Op::Missing(a) | Op::Cast(a) | Op::OrderKey(a) => mark(registers, a, bit),
            Op::Binary(_, a, b) => {
                mark(registers, a, bit);
                mark(registers, b, bit);
            }
            Op::Select(c, a, b) => {
                mark(registers, c, bit);
                mark(registers, a, bit);
                mark(registers, b, bit);
            }
            _ => {}
        }
    }
    for (index, sink) in sinks.iter().enumerate() {
        mark(&mut compiler.registers, sink.value, 1 << index);
    }
    fn mark_guard(registers: &mut [Register], index: usize) {
        if registers[index].guards != 0 {
            return;
        }
        registers[index].guards = 1;
        match registers[index].op {
            Op::Unary(_, a) | Op::Missing(a) | Op::Cast(a) | Op::OrderKey(a) => mark_guard(registers, a),
            Op::Binary(_, a, b) => {
                mark_guard(registers, a);
                mark_guard(registers, b);
            }
            Op::Select(c, a, b) => {
                mark_guard(registers, c);
                mark_guard(registers, a);
                mark_guard(registers, b);
            }
            _ => {}
        }
    }
    for &guard in &guards {
        mark_guard(&mut compiler.registers, guard);
    }
    // A second sink alone is not enough: ordinary cheap field reads/bitmap
    // reductions need not be turned into another block interpreter.
    let keep = compiler.registers.iter().filter(|r| {
        r.leaves.count_ones() > 1
            && matches!(r.op, Op::Binary(Binary::Div | Binary::Mod, ..) | Op::Cast(_))
    }).fold(0, |mask, register| mask | register.leaves);
    if keep.count_ones() < 2 { decline!("no_shared_costly_producer"); }
    compact_reduction_region(&mut compiler, &mut sinks, &mut selected, &mut guards, &mut root_read, keep);
    if trace { eprintln!("MULTI_REDUCE_REGION sinks={} registers={} reads={} children={}", sinks.len(), compiler.registers.len(), compiler.reads.len(), compiler.children.len()); }
    if compiler.children.iter().any(|child| !crate::expressions::is_frame_column(child)) {
        decline!("source_columns");
    }
    if compiler.registers.iter().any(|r| matches!(r.op, Op::Read(i) | Op::MissingRead(i) | Op::Length(i) if compiler.reads[i].coordinate == 0)) { decline!("frame_capture"); }
    let mut outputs = Vec::with_capacity(fields.len());
    let mut ordinary = vec![];
    for (field, (_, expr)) in fields.iter().enumerate() {
        if let Some(sink) = selected.iter().position(|&index| index == field) {
            outputs.push(Ok(sink));
        } else {
            let index = ordinary.len();
            ordinary.push((compiler.lower)(expr)?.0);
            outputs.push(Err(index));
        }
    }
    let program = ListFlatMap {
        children: compiler.children,
        child_types: compiler.child_types,
        reads: compiler.reads,
        registers: compiler.registers,
        leaves: compiler.leaves,
        root: Node::Concat(vec![]),
        root_read,
        fold: None,
        output: None,
        coordinates: 2,
        dtype: DataType::Boolean,
        workspace: Arc::new(execution::WorkspacePool::default()),
        ordinary: None,
    };
    let workspace_bytes_per_lane = program
        .registers
        .iter()
        .map(|r| match r.kind {
            Kind::I32 | Kind::F32 => 5,
            Kind::I64 | Kind::F64 => 9,
            Kind::Bool => 2,
        })
        .sum::<usize>()
        // Selected child coordinates (8), shared owner/end segments (16),
        // guard mask (1), and one reusable packed bitmap (at most 1).
        + 26;
    Some(MultiReduce {
        workspace_bytes_per_lane,
        program,
        sinks,
        guards,
        fields: output_fields.clone(),
        outputs,
        ordinary,
        fallback: None,
    })
}
