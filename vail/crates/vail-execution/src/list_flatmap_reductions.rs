//! Shared block producers feeding compatible, same-owner list reductions.
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
impl Compiler<'_> {
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
    } = value.as_ref()
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
        if children[1..1 + n].iter().any(|child| !child.is::<Column>()) {
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

pub(crate) fn prepare_reductions(
    expression: &Expr,
    dtype: &DataType,
    lower: &mut dyn FnMut(&Expr) -> Option<(Arc<dyn PhysicalExpr>, DataType)>,
) -> Option<MultiReduce> {
    if !has_reduction_region(expression) {
        return None;
    }
    let Expr::Struct { fields } = expression else {
        return None;
    };
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
    if !owner.is_pure() {
        return None;
    }
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
    if candidates.len() < 2 {
        return None;
    }
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
    let root = compiler.list(owner, &None, vec![], &mut emit)?;
    drop(emit);
    if sinks.len() < 2 {
        return None;
    }
    let Node::Loop {
        read: root_read,
        coordinate: 1,
        body,
        ..
    } = root
    else {
        return None;
    };
    if compiler.coordinates != 2 || compiler.children.iter().any(|child| !child.is::<Column>()) {
        return None;
    }
    let mut node = body.as_ref();
    let mut guards = vec![];
    while let Node::Filter { predicate, body } = node {
        guards.push(*predicate);
        node = body;
    }
    if !matches!(node, Node::Concat(nodes) if nodes.iter().all(|n| matches!(n, Node::Emit(_)))) {
        return None;
    }
    fn mark(registers: &mut [Register], index: usize, bit: u64) {
        if registers[index].leaves & bit != 0 {
            return;
        }
        registers[index].leaves |= bit;
        match registers[index].op {
            Op::Unary(_, a) | Op::Missing(a) | Op::Cast(a) => mark(registers, a, bit),
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
            Op::Unary(_, a) | Op::Missing(a) | Op::Cast(a) => mark_guard(registers, a),
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
    if compiler.registers.iter().any(|r| matches!(r.op, Op::Read(i) | Op::MissingRead(i) | Op::Length(i) if compiler.reads[i].coordinate == 0)) { return None; }
    let shared = compiler.registers.iter().any(|r| {
        r.leaves.count_ones() > 1
            && matches!(
                r.op,
                Op::Binary(Binary::Div | Binary::Mod, ..) | Op::Cast(_)
            )
    });
    if !shared {
        return None;
    }
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
        + 17;
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
