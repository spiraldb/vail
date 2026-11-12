//! Compose list maps, filters and flatmaps into one direct-output program.
//!
//! Collection nodes traverse borrowed Arrow spans. Scalar instructions operate
//! on blocks, with one dispatch per instruction rather than per element. Only
//! the final list is built; the fixed-size scalar workspace is reused.
use crate::expressions::{BinaryOperator as Binary, Expr, UnaryOperator as Unary};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StructArray,
    builder::{BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder},
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Schema};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{Column, Literal},
};
use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

const MAX_REGISTERS: usize = 256;
const MAX_LEAVES: usize = 64;
const MAX_DEPTH: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    I32,
    I64,
    F32,
    F64,
    Bool,
}
impl Kind {
    fn of(dtype: &DataType) -> Option<Self> {
        Some(match dtype {
            DataType::Int32 => Self::I32,
            DataType::Int64 => Self::I64,
            DataType::Float32 => Self::F32,
            DataType::Float64 => Self::F64,
            DataType::Boolean => Self::Bool,
            _ => return None,
        })
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Cell {
    bits: u64,
    valid: bool,
}
impl Cell {
    fn some(bits: u64) -> Self {
        Self { bits, valid: true }
    }
    fn boolean(value: bool) -> Self {
        Self::some(u64::from(value))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Path {
    Field(usize),
    Values(usize, usize),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Read {
    child: usize,
    path: Vec<Path>,
    coordinate: usize,
    dtype: DataType,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Op {
    Read(usize),
    MissingRead(usize),
    Length(usize),
    CharacterLength(usize),
    Constant(Cell),
    Unary(Unary, usize),
    Binary(Binary, usize, usize),
    Missing(usize),
    Cast(usize),
    NormalizeFloatZero(usize),
    Select(usize, usize, usize),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Register {
    op: Op,
    kind: Kind,
    leaves: u64,
    guards: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Leaf {
    value: usize,
    guards: Vec<(usize, bool)>,
    output: Option<OutputValue>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum OutputValue {
    Scalar(usize),
    Struct {
        valid: usize,
        fields: Vec<OutputValue>,
    },
    Borrowed {
        read: usize,
        valid: usize,
    },
}
impl OutputValue {
    fn registers(&self, out: &mut Vec<usize>) {
        match self {
            Self::Scalar(value) => out.push(*value),
            Self::Struct { valid, fields } => {
                out.push(*valid);
                for field in fields {
                    field.registers(out);
                }
            }
            Self::Borrowed { valid, .. } => out.push(*valid),
        }
    }
}
// Transpose fanout alternatives once at preparation, so execution walks one
// fixed writer tree rather than constructing a tree of field bindings per block.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum OutputColumn {
    Scalar {
        kind: Kind,
        registers: Vec<usize>,
    },
    Struct {
        valid: Vec<usize>,
        fields: Vec<OutputColumn>,
    },
    Borrowed {
        dtype: DataType,
        reads: Vec<usize>,
        valid: Vec<usize>,
    },
}
impl OutputColumn {
    fn new(values: &[&OutputValue], registers: &[Register], reads: &[Read]) -> Self {
        match values[0] {
            OutputValue::Scalar(value) => Self::Scalar {
                kind: registers[*value].kind,
                registers: values
                    .iter()
                    .map(|value| {
                        let OutputValue::Scalar(value) = value else {
                            unreachable!()
                        };
                        *value
                    })
                    .collect(),
            },
            OutputValue::Struct { fields, .. } => Self::Struct {
                valid: values
                    .iter()
                    .map(|value| {
                        let OutputValue::Struct { valid, .. } = value else {
                            unreachable!()
                        };
                        *valid
                    })
                    .collect(),
                fields: (0..fields.len())
                    .map(|i| {
                        let fields = values
                            .iter()
                            .map(|value| {
                                let OutputValue::Struct { fields, .. } = value else {
                                    unreachable!()
                                };
                                &fields[i]
                            })
                            .collect::<Vec<_>>();
                        Self::new(&fields, registers, reads)
                    })
                    .collect(),
            },
            OutputValue::Borrowed { read, .. } => Self::Borrowed {
                dtype: reads[*read].dtype.clone(),
                reads: values
                    .iter()
                    .map(|value| {
                        let OutputValue::Borrowed { read, .. } = value else {
                            unreachable!()
                        };
                        *read
                    })
                    .collect(),
                valid: values
                    .iter()
                    .map(|value| {
                        let OutputValue::Borrowed { valid, .. } = value else {
                            unreachable!()
                        };
                        *valid
                    })
                    .collect(),
            },
        }
    }
    fn depth(&self) -> usize {
        match self {
            Self::Struct { fields, .. } => 1 + fields.iter().map(Self::depth).max().unwrap_or(0),
            _ => 0,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Node {
    Loop {
        read: usize,
        coordinate: usize,
        guards: Vec<usize>,
        guard_mask: u64,
        body: Box<Node>,
    },
    Filter {
        predicate: usize,
        body: Box<Node>,
    },
    Concat(Vec<Node>),
    Emit(usize),
}

#[derive(Clone)]
enum Binding {
    Read(usize),
    Expression(Arc<Expr>, Option<Arc<Environment>>),
    Select(usize, Box<Binding>, Box<Binding>),
    Missing(DataType),
}
struct Environment {
    name: String,
    value: Binding,
    parent: Option<Arc<Environment>>,
}
type Env = Option<Arc<Environment>>;
fn lookup(env: &Env, name: &str) -> Option<Binding> {
    let mut current = env.as_ref();
    while let Some(scope) = current {
        if scope.name == name {
            return Some(scope.value.clone());
        }
        current = scope.parent.as_ref();
    }
    None
}
fn bind(env: &Env, name: &str, value: Binding) -> Env {
    Some(Arc::new(Environment {
        name: name.into(),
        value,
        parent: env.clone(),
    }))
}

type Consumer<'a> = dyn FnMut(&mut Compiler<'_>, Binding, Vec<(usize, bool)>) -> Option<Node> + 'a;
struct Compiler<'a> {
    lower: &'a mut dyn FnMut(&Expr) -> Option<(Arc<dyn PhysicalExpr>, DataType)>,
    children: Vec<Arc<dyn PhysicalExpr>>,
    child_types: Vec<DataType>,
    reads: Vec<Read>,
    registers: Vec<Register>,
    leaves: Vec<Leaf>,
    coordinates: usize,
    operations: usize,
    scalar_memo: HashMap<(usize, usize), usize>,
    // Environments are retained so their addresses remain valid memo keys.
    environments: Vec<Arc<Environment>>,
    expressions: Vec<Arc<Expr>>,
    scalar_depth: usize,
    projections: HashMap<(usize, usize, String), Binding>,
    output_nodes: usize,
    reduction_numeric: bool,
}
impl Compiler<'_> {
    fn environment(&mut self, env: Env) -> Env {
        if let Some(scope) = &env {
            self.environments.push(scope.clone());
        }
        env
    }
    fn read(&mut self, read: Read) -> usize {
        if let Some(index) = self.reads.iter().position(|old| old == &read) {
            return index;
        }
        let index = self.reads.len();
        self.reads.push(read);
        index
    }
    fn resolve(&mut self, expression: &Expr, env: &Env) -> Option<Binding> {
        if let Expr::LambdaVariable { name, .. } = expression {
            if let Some(value) = lookup(env, name) {
                return Some(value);
            }
        }
        if let Expr::GetField { value, name } = expression {
            let parent = self.binding(value, env);
            return self.project(parent, name);
        }
        if !matches!(
            expression,
            Expr::Column { .. }
                | Expr::Global { .. }
                | Expr::ExternalGlobal { .. }
                | Expr::LambdaVariable { .. }
                | Expr::AlignedMapInput { .. }
        ) {
            return None;
        }
        let (child, dtype) = (self.lower)(expression)?;
        let index = self
            .children
            .iter()
            .position(|old| old.eq(&child))
            .unwrap_or_else(|| {
                let index = self.children.len();
                self.children.push(child);
                self.child_types.push(dtype.clone());
                index
            });
        Some(Binding::Read(self.read(Read {
            child: index,
            path: vec![],
            coordinate: 0,
            dtype,
        })))
    }
    fn binding(&self, expression: &Arc<Expr>, env: &Env) -> Binding {
        if let Expr::LambdaVariable { name, .. } = expression.as_ref() {
            if let Some(value) = lookup(env, name) {
                return value;
            }
        }
        Binding::Expression(expression.clone(), env.clone())
    }
    fn project(&mut self, value: Binding, name: &str) -> Option<Binding> {
        match value {
            Binding::Read(parent) => {
                let mut read = self.reads[parent].clone();
                let DataType::Struct(fields) = &read.dtype else {
                    return None;
                };
                let index = fields.iter().position(|field| field.name() == name)?;
                read.dtype = fields[index].data_type().clone();
                read.path.push(Path::Field(index));
                Some(Binding::Read(self.read(read)))
            }
            Binding::Expression(expression, env) => {
                let key = (
                    Arc::as_ptr(&expression) as usize,
                    env.as_ref().map_or(0, |e| Arc::as_ptr(e) as usize),
                    name.to_owned(),
                );
                if let Some(value) = self.projections.get(&key) {
                    return Some(value.clone());
                }
                self.expressions.push(expression.clone());
                let value = match expression.as_ref() {
                    Expr::Struct { fields } => {
                        let (_, field) = fields.iter().find(|(field, _)| field == name)?;
                        Binding::Expression(Arc::new(field.clone()), env)
                    }
                    Expr::IfElse {
                        condition,
                        consequent,
                        alternate,
                    } => {
                        let condition = self.scalar(condition, &env)?;
                        let yes = self.project(self.binding(consequent, &env), name)?;
                        let no = self.project(self.binding(alternate, &env), name)?;
                        Binding::Select(condition, Box::new(yes), Box::new(no))
                    }
                    Expr::Literal { dtype, value } if value.is_null() => {
                        return self.project(Binding::Missing(dtype.to_arrow().ok()?), name);
                    }
                    _ => {
                        let resolved = self.resolve(&expression, &env)?;
                        self.project(resolved, name)?
                    }
                };
                self.projections.insert(key, value.clone());
                Some(value)
            }
            Binding::Select(condition, yes, no) => Some(Binding::Select(
                condition,
                Box::new(self.project(*yes, name)?),
                Box::new(self.project(*no, name)?),
            )),
            Binding::Missing(DataType::Struct(fields)) => Some(Binding::Missing(
                fields
                    .iter()
                    .find(|field| field.name() == name)?
                    .data_type()
                    .clone(),
            )),
            Binding::Missing(_) => None,
        }
    }
    fn present(&mut self, value: Binding) -> Option<usize> {
        match value {
            Binding::Read(read) => {
                let missing = self.register(Op::MissingRead(read), Kind::Bool)?;
                self.register(Op::Unary(Unary::Not, missing), Kind::Bool)
            }
            Binding::Missing(_) => self.register(Op::Constant(Cell::boolean(false)), Kind::Bool),
            Binding::Select(condition, yes, no) => {
                let yes = self.present(*yes)?;
                let no = self.present(*no)?;
                self.register(Op::Select(condition, yes, no), Kind::Bool)
            }
            Binding::Expression(expression, env) => match expression.as_ref() {
                Expr::Struct { .. } => self.register(Op::Constant(Cell::boolean(true)), Kind::Bool),
                Expr::IfElse {
                    condition,
                    consequent,
                    alternate,
                } => {
                    let condition = self.scalar(condition, &env)?;
                    let yes = self.present(self.binding(consequent, &env))?;
                    let no = self.present(self.binding(alternate, &env))?;
                    self.register(Op::Select(condition, yes, no), Kind::Bool)
                }
                Expr::Literal { value, .. } if value.is_null() => {
                    self.register(Op::Constant(Cell::boolean(false)), Kind::Bool)
                }
                _ => {
                    let value = self.resolve(&expression, &env)?;
                    self.present(value)
                }
            },
        }
    }
    fn output(&mut self, value: Binding, dtype: &DataType, depth: usize) -> Option<OutputValue> {
        if depth == MAX_DEPTH || self.output_nodes == MAX_REGISTERS {
            return None;
        }
        self.output_nodes += 1;
        if let Some(kind) = Kind::of(dtype) {
            let register = self.scalar_binding(value)?;
            return (self.registers[register].kind == kind)
                .then_some(OutputValue::Scalar(register));
        }
        match dtype {
            DataType::Struct(fields) => {
                let valid = self.present(value.clone())?;
                let fields = fields
                    .iter()
                    .map(|field| {
                        let child = self.project(value.clone(), field.name())?;
                        self.output(child, field.data_type(), depth + 1)
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(OutputValue::Struct { valid, fields })
            }
            DataType::List(_) | DataType::Utf8 => {
                let Binding::Read(read) = self.force(value)? else {
                    return None;
                };
                if self.reads[read].dtype != *dtype {
                    return None;
                }
                let valid = self.present(Binding::Read(read))?;
                Some(OutputValue::Borrowed { read, valid })
            }
            _ => None,
        }
    }
    fn force(&mut self, mut value: Binding) -> Option<Binding> {
        for _ in 0..MAX_REGISTERS {
            match value {
                Binding::Read(_) | Binding::Missing(_) | Binding::Select(..) => return Some(value),
                Binding::Expression(ref expression, ref env) => {
                    value = self.resolve(expression, env)?;
                }
            }
        }
        None
    }
    fn register(&mut self, op: Op, kind: Kind) -> Option<usize> {
        if self.registers.len() >= MAX_REGISTERS {
            return None;
        }
        if let Some(index) = self
            .registers
            .iter()
            .position(|old| old.op == op && old.kind == kind)
        {
            return Some(index);
        }
        let index = self.registers.len();
        self.registers.push(Register {
            op,
            kind,
            leaves: 0,
            guards: 0,
        });
        Some(index)
    }
    fn scalar_binding(&mut self, value: Binding) -> Option<usize> {
        match value {
            Binding::Read(read) => {
                self.register(Op::Read(read), Kind::of(&self.reads[read].dtype)?)
            }
            Binding::Missing(dtype) => {
                self.register(Op::Constant(Cell::default()), Kind::of(&dtype)?)
            }
            Binding::Select(condition, yes, no) => {
                let a = self.scalar_binding(*yes)?;
                let b = self.scalar_binding(*no)?;
                if self.registers[a].kind != self.registers[b].kind {
                    return None;
                }
                self.register(Op::Select(condition, a, b), self.registers[a].kind)
            }
            Binding::Expression(expression, env) => {
                self.expressions.push(expression.clone());
                self.scalar(&expression, &env)
            }
        }
    }
    fn scalar(&mut self, expression: &Expr, env: &Env) -> Option<usize> {
        if self.scalar_depth == 64 {
            return None;
        }
        self.scalar_depth += 1;
        let result = self.scalar_inner(expression, env);
        self.scalar_depth -= 1;
        result
    }
    fn scalar_inner(&mut self, expression: &Expr, env: &Env) -> Option<usize> {
        let key = (
            expression as *const Expr as usize,
            env.as_ref().map_or(0, |e| Arc::as_ptr(e) as usize),
        );
        if let Some(&index) = self.scalar_memo.get(&key) {
            return Some(index);
        }
        let index = match expression {
            Expr::Literal { dtype, value } => {
                let kind = Kind::of(&dtype.to_arrow().ok()?)?;
                let cell = if value.is_null() {
                    Cell::default()
                } else {
                    Cell::some(match kind {
                        Kind::I32 | Kind::I64 => value.as_i64()? as u64,
                        Kind::F32 => (value.as_f64()? as f32).to_bits() as u64,
                        Kind::F64 => value.as_f64()?.to_bits(),
                        Kind::Bool => u64::from(value.as_bool()?),
                    })
                };
                self.register(Op::Constant(cell), kind)?
            }
            Expr::RuntimeLiteral { value } => {
                let kind = Kind::of(&value.data_type())?;
                let cell = match value {
                    ScalarValue::Int32(v) => v.map(|v| Cell::some(v as u64)),
                    ScalarValue::Int64(v) => v.map(|v| Cell::some(v as u64)),
                    ScalarValue::Float32(v) => v.map(|v| Cell::some(v.to_bits() as u64)),
                    ScalarValue::Float64(v) => v.map(|v| Cell::some(v.to_bits())),
                    ScalarValue::Boolean(v) => v.map(Cell::boolean),
                    _ => return None,
                }
                .unwrap_or_default();
                self.register(Op::Constant(cell), kind)?
            }
            Expr::Binary {
                operator,
                left,
                right,
            }
            | Expr::IntegerBinary {
                operator,
                left,
                right,
            } => {
                let mut a = self.scalar(left, env)?;
                let mut b = self.scalar(right, env)?;
                let mut kind = self.registers[a].kind;
                if self.reduction_numeric
                    && kind != Kind::Bool
                    && self.registers[b].kind != Kind::Bool
                {
                    kind = numeric_kind(kind, self.registers[b].kind);
                    if *operator == Binary::Div && !matches!(kind, Kind::F32 | Kind::F64) {
                        kind = Kind::F64;
                    }
                    a = self.numeric_cast(a, kind)?;
                    b = self.numeric_cast(b, kind)?;
                } else if kind != self.registers[b].kind {
                    return None;
                }
                if self.reduction_numeric
                    && *operator == Binary::Div
                    && !matches!(kind, Kind::F32 | Kind::F64)
                {
                    kind = Kind::F64;
                    a = self.numeric_cast(a, kind)?;
                    b = self.numeric_cast(b, kind)?;
                }
                if self.reduction_numeric
                    && matches!(kind, Kind::F32 | Kind::F64)
                    && matches!(operator, Binary::Eq | Binary::Ne | Binary::Lt | Binary::Le | Binary::Gt | Binary::Ge)
                {
                    a = self.normalized_float(a)?;
                    b = self.normalized_float(b)?;
                    kind = self.registers[a].kind;
                }
                let output = match operator {
                    Binary::Add | Binary::Sub | Binary::Mul if kind != Kind::Bool => kind,
                    Binary::Eq | Binary::Ne | Binary::Lt | Binary::Le | Binary::Gt | Binary::Ge
                        if matches!(kind, Kind::I32 | Kind::I64 | Kind::Bool) =>
                    {
                        Kind::Bool
                    }
                    Binary::And | Binary::Or if kind == Kind::Bool => Kind::Bool,
                    Binary::Eq | Binary::Ne | Binary::Lt | Binary::Le | Binary::Gt | Binary::Ge
                        if self.reduction_numeric && matches!(kind, Kind::F32 | Kind::F64) => Kind::Bool,
                    Binary::Mod
                        if self.reduction_numeric
                            && matches!(kind, Kind::I32 | Kind::I64)
                            && constant_nonzero(&self.registers[b]) =>
                    {
                        kind
                    }
                    Binary::Div
                        if self.reduction_numeric
                            && matches!(kind, Kind::F32 | Kind::F64)
                            && constant_nonzero(&self.registers[b]) =>
                    {
                        kind
                    }
                    _ => return None,
                };
                self.register(Op::Binary(*operator, a, b), output)?
            }
            Expr::Cast { value, dtype } if self.reduction_numeric => {
                let value = self.scalar(value, env)?;
                self.numeric_cast(value, Kind::of(&dtype.to_arrow().ok()?)?)?
            }
            Expr::Unary { operator, value } => {
                let value = self.scalar(value, env)?;
                let kind = self.registers[value].kind;
                if (*operator == Unary::Not) != (kind == Kind::Bool) {
                    return None;
                }
                self.register(Op::Unary(*operator, value), kind)?
            }
            Expr::IsMissing { value } => {
                if let Some(Binding::Read(read)) =
                    self.resolve(value, env).and_then(|value| self.force(value))
                {
                    self.register(Op::MissingRead(read), Kind::Bool)?
                } else if matches!(value.as_ref(), Expr::Struct { .. }) {
                    self.register(Op::Constant(Cell::boolean(false)), Kind::Bool)?
                } else {
                    let value = self.scalar(value, env)?;
                    self.register(Op::Missing(value), Kind::Bool)?
                }
            }
            Expr::StringFunction { function, args }
                if self.reduction_numeric && function == "character_length" && args.len() == 1 =>
            {
                let Binding::Read(read) = self.resolve(&args[0], env).and_then(|v| self.force(v))? else {
                    return None;
                };
                if self.reads[read].dtype != DataType::Utf8 { return None; }
                self.register(Op::CharacterLength(read), Kind::I32)?
            }
            Expr::ArrayLength { value } => {
                let Binding::Read(read) = self
                    .resolve(value, env)
                    .and_then(|value| self.force(value))?
                else {
                    return None;
                };
                if !matches!(self.reads[read].dtype, DataType::List(_)) {
                    return None;
                }
                self.register(Op::Length(read), Kind::I32)?
            }
            Expr::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                let c = self.scalar(condition, env)?;
                let a = self.scalar(consequent, env)?;
                let b = self.scalar(alternate, env)?;
                if self.registers[c].kind != Kind::Bool
                    || self.registers[a].kind != self.registers[b].kind
                {
                    return None;
                }
                self.register(Op::Select(c, a, b), self.registers[a].kind)?
            }
            _ => {
                let value = self.resolve(expression, env)?;
                self.scalar_binding(value)?
            }
        };
        self.scalar_memo.insert(key, index);
        Some(index)
    }
    fn collection(
        &mut self,
        value: Binding,
        guards: Vec<(usize, bool)>,
        emit: &mut Consumer<'_>,
    ) -> Option<Node> {
        if self.coordinates >= MAX_DEPTH {
            return None;
        }
        if let Binding::Expression(expression, env) = value {
            self.expressions.push(expression.clone());
            return self.list(&expression, &env, guards, emit);
        }
        let Binding::Read(read) = value else {
            unreachable!()
        };
        // Conditional collection branches must not enumerate an unselected
        // borrowed child list. A future batched branch selector can admit it.
        if !guards.is_empty() {
            return None;
        }
        let mut element = self.reads[read].clone();
        let DataType::List(field) = &element.dtype else {
            return None;
        };
        element.dtype = field.data_type().clone();
        let coordinate = self.coordinates;
        self.coordinates += 1;
        element
            .path
            .push(Path::Values(element.coordinate, coordinate));
        element.coordinate = coordinate;
        let element = self.read(element);
        let body = emit(self, Binding::Read(element), guards)?;
        Some(Node::Loop {
            read,
            coordinate,
            guards: vec![],
            guard_mask: 0,
            body: Box::new(body),
        })
    }
    fn list(
        &mut self,
        expression: &Expr,
        env: &Env,
        guards: Vec<(usize, bool)>,
        emit: &mut Consumer<'_>,
    ) -> Option<Node> {
        self.operations += 1;
        if self.operations > MAX_REGISTERS {
            return None;
        }
        match expression {
            Expr::ArrayMap {
                value,
                parameter,
                body,
            } => {
                let mut next = |c: &mut Compiler<'_>, value, guards| {
                    let nested = c.environment(bind(env, parameter, value));
                    emit(c, Binding::Expression(body.clone(), nested), guards)
                };
                self.list(value, env, guards, &mut next)
            }
            Expr::ArrayFilter {
                value,
                parameter,
                predicate,
            } => {
                let mut next = |c: &mut Compiler<'_>, value: Binding, guards: Vec<_>| {
                    let nested = c.environment(bind(env, parameter, value.clone()));
                    let predicate = c.scalar(predicate, &nested)?;
                    if c.registers[predicate].kind != Kind::Bool {
                        return None;
                    }
                    Some(Node::Filter {
                        predicate,
                        body: Box::new(emit(c, value, guards)?),
                    })
                };
                self.list(value, env, guards, &mut next)
            }
            Expr::ArrayFlatten { value } => {
                let mut next =
                    |c: &mut Compiler<'_>, value, guards| c.collection(value, guards, emit);
                self.list(value, env, guards, &mut next)
            }
            Expr::Array { elements, .. } => Some(Node::Concat(
                elements
                    .iter()
                    .map(|element| {
                        emit(
                            self,
                            Binding::Expression(Arc::new(element.clone()), env.clone()),
                            guards.clone(),
                        )
                    })
                    .collect::<Option<Vec<_>>>()?,
            )),
            Expr::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                let condition = self.scalar(condition, env)?;
                if self.registers[condition].kind != Kind::Bool {
                    return None;
                }
                let mut yes = guards.clone();
                yes.push((condition, true));
                let mut no = guards;
                no.push((condition, false));
                Some(Node::Concat(vec![
                    self.list(consequent, env, yes, emit)?,
                    self.list(alternate, env, no, emit)?,
                ]))
            }
            Expr::Literal { value, .. } if value.is_null() => Some(Node::Concat(vec![])),
            Expr::RuntimeLiteral { value } if value.is_null() => Some(Node::Concat(vec![])),
            _ => {
                let value = self.resolve(expression, env)?;
                self.collection(value, guards, emit)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Fold {
    initial: Cell,
    operator: Binary,
}

#[derive(Clone, Debug)]
pub(crate) struct ListFlatMap {
    children: Vec<Arc<dyn PhysicalExpr>>,
    child_types: Vec<DataType>,
    reads: Vec<Read>,
    registers: Vec<Register>,
    leaves: Vec<Leaf>,
    root: Node,
    root_read: usize,
    fold: Option<Fold>,
    output: Option<OutputColumn>,
    coordinates: usize,
    dtype: DataType,
    workspace: Arc<execution::WorkspacePool>,
    ordinary: Option<(Arc<dyn PhysicalExpr>, Arc<Schema>)>,
}
impl ListFlatMap {
    // Only plain frame-column reads can use an ordinary backend on that SAME
    // frame. No UDF, bound callback input, global getter or source is rerun.
    fn frame_captures(&self) -> bool {
        self.registers.iter().any(|register| match register.op {
            Op::Read(read) | Op::MissingRead(read) | Op::Length(read) => {
                self.reads[read].coordinate == 0
            }
            _ => false,
        })
    }
    pub(crate) fn can_use_ordinary(&self) -> bool {
        self.children.iter().all(|child| child.is::<Column>())
            && (self.coordinates > 2 || self.children.len() > 1 || self.frame_captures())
    }
    pub(crate) fn prefer_ordinary(&self) -> bool {
        // These shapes already map one borrowed child column efficiently, or
        // need row-array captures outside the original-lane emitter's domain.
        self.children.len() > 1
            || self.frame_captures()
            || (self.coordinates > 2 && self.leaves.len() == 1)
    }
    pub(crate) fn set_ordinary(&mut self, expression: Arc<dyn PhysicalExpr>, schema: &Schema) {
        self.ordinary = Some((expression, Arc::new(schema.clone())));
    }
}
impl PartialEq for ListFlatMap {
    fn eq(&self, other: &Self) -> bool {
        self.children == other.children
            && self.child_types == other.child_types
            && self.reads == other.reads
            && self.registers == other.registers
            && self.leaves == other.leaves
            && self.root == other.root
            && self.root_read == other.root_read
            && self.fold == other.fold
            && self.output == other.output
            && self.coordinates == other.coordinates
            && self.dtype == other.dtype
            && self.ordinary == other.ordinary
    }
}
impl Eq for ListFlatMap {}
impl Hash for ListFlatMap {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.children.hash(h);
        self.child_types.hash(h);
        self.reads.hash(h);
        self.registers.hash(h);
        self.leaves.hash(h);
        self.root.hash(h);
        self.root_read.hash(h);
        self.fold.hash(h);
        self.output.hash(h);
        self.coordinates.hash(h);
        self.dtype.hash(h);
        self.ordinary.hash(h);
    }
}
impl fmt::Display for ListFlatMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            if self.fold.is_some() {
                "ListFold"
            } else {
                "ListFlatMap"
            }
        )
    }
}
impl PhysicalExpr for ListFlatMap {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.children.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.children.len() {
            return Err(DataFusionError::Internal("flatmap inputs changed".into()));
        }
        let ordinary = (children == self.children
            && children.iter().all(|child| child.is::<Column>()))
        .then(|| self.ordinary.clone())
        .flatten();
        Ok(Arc::new(Self {
            children,
            ordinary,
            ..self.as_ref().clone()
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.execute(batch).map(ColumnarValue::Array)
    }
}

/// Unsupported bodies decline during preparation, before any input evaluation.
pub(crate) fn prepare(
    expression: &Expr,
    dtype: DataType,
    lower: &mut dyn FnMut(&Expr) -> Option<(Arc<dyn PhysicalExpr>, DataType)>,
) -> Option<ListFlatMap> {
    let original = expression;
    let (expression, output_type, fold) = if let Expr::ArrayFold {
        value,
        zero,
        accumulator,
        parameter,
        body,
        scan: false,
    } = expression
    {
        if accumulator == parameter
            || !matches!(
                dtype,
                DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
            )
        {
            return None;
        }
        let (Expr::Binary {
            operator,
            left,
            right,
        }
        | Expr::IntegerBinary {
            operator,
            left,
            right,
        }) = body.as_ref()
        else {
            return None;
        };
        if !matches!(operator, Binary::Add | Binary::Mul)
            || !matches!(left.as_ref(), Expr::LambdaVariable { name, .. } if name == accumulator)
            || !matches!(right.as_ref(), Expr::LambdaVariable { name, .. } if name == parameter)
        {
            return None;
        }
        let initial = match zero.as_ref() {
            Expr::Literal { dtype: ty, value } if ty.to_arrow().ok()? == dtype => {
                if value.is_null() {
                    Cell::default()
                } else {
                    Cell::some(match dtype {
                        DataType::Float32 => (value.as_f64()? as f32).to_bits() as u64,
                        DataType::Float64 => value.as_f64()?.to_bits(),
                        _ => value.as_i64()? as u64,
                    })
                }
            }
            Expr::RuntimeLiteral {
                value: ScalarValue::Int32(value),
            } if dtype == DataType::Int32 => {
                value.map(|v| Cell::some(v as u64)).unwrap_or_default()
            }
            Expr::RuntimeLiteral {
                value: ScalarValue::Int64(value),
            } if dtype == DataType::Int64 => {
                value.map(|v| Cell::some(v as u64)).unwrap_or_default()
            }
            Expr::RuntimeLiteral {
                value: ScalarValue::Float32(value),
            } if dtype == DataType::Float32 => value
                .map(|v| Cell::some(v.to_bits() as u64))
                .unwrap_or_default(),
            Expr::RuntimeLiteral {
                value: ScalarValue::Float64(value),
            } if dtype == DataType::Float64 => {
                value.map(|v| Cell::some(v.to_bits())).unwrap_or_default()
            }
            _ => return None,
        };
        (
            value.as_ref(),
            dtype.clone(),
            Some(Fold {
                initial,
                operator: *operator,
            }),
        )
    } else {
        let DataType::List(field) = &dtype else {
            return None;
        };
        (expression, field.data_type().clone(), None)
    };
    if !execution::supported(&output_type) {
        return None;
    }
    let mut count = 0;
    let mut flatmap = false;
    let mut base = expression;
    loop {
        match base {
            Expr::ArrayMap { value, .. } | Expr::ArrayFilter { value, .. } => {
                count += 1;
                base = value;
            }
            Expr::ArrayFlatten { value } => {
                flatmap = true;
                count += 2;
                base = value;
            }
            _ => break,
        }
    }
    // Existing whole-column kernels are cheaper for map/filter-only chains.
    if count < if fold.is_some() { 1 } else { 2 } || (!flatmap && fold.is_none()) {
        return None;
    }
    // Certify the original DAG, including undemanded constructor fields, before
    // projection/filtering can remove work. Ineligible shapes avoid this walk.
    if !original.is_pure() {
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
        reduction_numeric: false,
    };
    let Binding::Read(root_read) = compiler.resolve(base, &None)? else {
        return None;
    };
    let mut emit = |c: &mut Compiler<'_>, value, guards| {
        if c.leaves.len() == MAX_LEAVES {
            return None;
        }
        let output = c.output(value, &output_type, 0)?;
        let (value, output) = match output {
            OutputValue::Scalar(value) => (value, None),
            output => (
                c.register(Op::Constant(Cell::boolean(true)), Kind::Bool)?,
                Some(output),
            ),
        };
        let leaf = c.leaves.len();
        c.leaves.push(Leaf {
            value,
            guards,
            output,
        });
        Some(Node::Emit(leaf))
    };
    let root = compiler.list(expression, &None, vec![], &mut emit)?;
    if compiler.leaves.is_empty() {
        return None;
    }
    fn mark(registers: &mut [Register], index: usize, bit: u64, guard: bool) {
        let uses = if guard {
            &mut registers[index].guards
        } else {
            &mut registers[index].leaves
        };
        if *uses & bit != 0 {
            return;
        }
        *uses |= bit;
        match registers[index].op {
            Op::Unary(_, a) | Op::Missing(a) | Op::Cast(a) | Op::NormalizeFloatZero(a) => mark(registers, a, bit, guard),
            Op::Binary(_, a, b) => {
                mark(registers, a, bit, guard);
                mark(registers, b, bit, guard);
            }
            Op::Select(c, a, b) => {
                mark(registers, c, bit, guard);
                mark(registers, a, bit, guard);
                mark(registers, b, bit, guard);
            }
            _ => {}
        }
    }
    // Guards on a loop's item run in that item domain, before any inner
    // expansion. A guard below a concat can be folded into a single emit, but
    // more complicated fanout retains the ordinary route.
    fn normalize(node: Node, compiler: &mut Compiler<'_>, next_guard: &mut usize) -> Option<Node> {
        Some(match node {
            Node::Loop {
                read,
                coordinate,
                body,
                ..
            } => {
                let mut body = *body;
                let mut guards = vec![];
                while let Node::Filter {
                    predicate,
                    body: inner,
                } = body
                {
                    guards.push(predicate);
                    body = *inner;
                }
                if let Node::Emit(leaf) = body
                    && compiler.leaves[leaf].output.is_none()
                {
                    compiler.leaves[leaf]
                        .guards
                        .extend(guards.into_iter().map(|guard| (guard, true)));
                    return Some(Node::Loop {
                        read,
                        coordinate,
                        guards: vec![],
                        guard_mask: 0,
                        body: Box::new(Node::Emit(leaf)),
                    });
                }
                let mask = if guards.is_empty() {
                    0
                } else {
                    if *next_guard == MAX_LEAVES {
                        return None;
                    }
                    let bit = 1u64 << *next_guard;
                    *next_guard += 1;
                    for &guard in &guards {
                        mark(&mut compiler.registers, guard, bit, true);
                    }
                    bit
                };
                Node::Loop {
                    read,
                    coordinate,
                    guards,
                    guard_mask: mask,
                    body: Box::new(normalize(body, compiler, next_guard)?),
                }
            }
            Node::Filter { predicate, body } => {
                let body = normalize(*body, compiler, next_guard)?;
                let Node::Emit(leaf) = body else {
                    return None;
                };
                compiler.leaves[leaf].guards.push((predicate, true));
                Node::Emit(leaf)
            }
            Node::Concat(nodes) => Node::Concat(
                nodes
                    .into_iter()
                    .map(|node| normalize(node, compiler, next_guard))
                    .collect::<Option<Vec<_>>>()?,
            ),
            Node::Emit(leaf) => Node::Emit(leaf),
        })
    }
    let root = normalize(root, &mut compiler, &mut 0)?;
    for (index, leaf) in compiler.leaves.iter().enumerate() {
        mark(&mut compiler.registers, leaf.value, 1 << index, false);
        if let Some(output) = &leaf.output {
            let mut registers = vec![];
            output.registers(&mut registers);
            for value in registers {
                mark(&mut compiler.registers, value, 1 << index, false);
            }
        }
        for &(guard, _) in &leaf.guards {
            mark(&mut compiler.registers, guard, 1 << index, false);
        }
    }
    let output = compiler.leaves[0].output.as_ref().map(|_| {
        let values = compiler
            .leaves
            .iter()
            .map(|leaf| leaf.output.as_ref().unwrap())
            .collect::<Vec<_>>();
        OutputColumn::new(&values, &compiler.registers, &compiler.reads)
    });
    let program = ListFlatMap {
        children: compiler.children,
        child_types: compiler.child_types,
        reads: compiler.reads,
        registers: compiler.registers,
        leaves: compiler.leaves,
        root,
        root_read,
        fold,
        output,
        coordinates: compiler.coordinates,
        dtype,
        workspace: Arc::new(execution::WorkspacePool::default()),
        ordinary: None,
    };
    // Scalar literals have a declared broadcast domain. Unknown/free array
    // captures need existing kernels unless every source is an original Column.
    let all_columns = program.children.iter().all(|source| source.is::<Column>());
    if !all_columns
        && program.registers.iter().any(|register| match register.op {
            Op::Read(read) | Op::MissingRead(read) | Op::Length(read) => {
                let read = &program.reads[read];
                read.coordinate == 0 && !program.children[read.child].is::<Literal>()
            }
            _ => false,
        })
    {
        return None;
    }
    Some(program)
}

struct BoundRead {
    values: ArrayRef,
    // Cache the same physical bitmap used by Array::is_valid, once per batch.
    nulls: Option<NullBuffer>,
    ancestors: Vec<(NullBuffer, usize)>,
    coordinate: usize,
    scalar: bool,
    all_valid: bool,
}
impl BoundRead {
    fn index(&self, positions: &[usize]) -> usize {
        if self.scalar && self.coordinate == 0 {
            0
        } else {
            positions[self.coordinate]
        }
    }
    fn valid(&self, positions: &[usize]) -> bool {
        self.ancestors.iter().all(|(mask, slot)| {
            mask.is_valid(if self.scalar && *slot == 0 {
                0
            } else {
                positions[*slot]
            })
        }) && self.nulls.as_ref().is_none_or(|mask| mask.is_valid(self.index(positions)))
    }
    fn all_valid_in_span(&self, coordinate: Option<usize>) -> bool {
        self.all_valid || (coordinate == Some(self.coordinate)
            && self.nulls.is_none()
            && self.ancestors.iter().all(|(_, slot)| *slot != self.coordinate))
    }
}

#[path = "list_flatmap_execute.rs"]
mod execution;

include!("list_flatmap_reductions.rs");
