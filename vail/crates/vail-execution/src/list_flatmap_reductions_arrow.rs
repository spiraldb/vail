// Prepared array kernels for the shared reduction DAG. Inputs stay borrowed;
// only bounded primitive/bitmap outputs are allocated by the ordinary kernels.
use crate::expressions::{PrimitiveType, Type};
use arrow_array::{Datum, UInt32Array};
use arrow_buffer::BooleanBuffer;

type ArrowResult<T> = std::result::Result<T, arrow_schema::ArrowError>;
type NumericKernel = fn(&dyn Datum, &dyn Datum) -> ArrowResult<ArrayRef>;
type CompareKernel = fn(&dyn Datum, &dyn Datum) -> ArrowResult<BooleanArray>;
type BooleanKernel = fn(&BooleanArray, &BooleanArray) -> ArrowResult<BooleanArray>;

#[derive(Clone, Debug)]
struct DatumSlot {
    array: ArrayRef,
    scalar: bool,
}
impl Datum for DatumSlot {
    fn get(&self) -> (&dyn Array, bool) {
        (self.array.as_ref(), self.scalar)
    }
}
#[derive(Clone, Debug)]
enum ArrayStep {
    Constant(DatumSlot),
    Read(usize),
    MissingRead(usize),
    Length(usize),
    Missing(usize),
    Numeric(NumericKernel, usize, usize),
    Compare(CompareKernel, usize, usize),
    Boolean(BooleanKernel, usize, usize),
    Mod(usize, usize, Type),
    Normalize(usize),
    Cast(usize, DataType),
    Neg(usize),
    Not(usize),
    Alias(usize),
}
impl ArrayStep {
    fn constant_inputs(&self, constants: &[Option<DatumSlot>]) -> bool {
        match self {
            Self::Constant(_) => true,
            Self::Read(_) | Self::MissingRead(_) | Self::Length(_) => false,
            Self::Numeric(_, a, b)
            | Self::Compare(_, a, b)
            | Self::Boolean(_, a, b)
            | Self::Mod(a, b, _) => constants[*a].is_some() && constants[*b].is_some(),
            Self::Missing(a)
            | Self::Normalize(a)
            | Self::Cast(a, _)
            | Self::Neg(a)
            | Self::Not(a)
            | Self::Alias(a) => constants[*a].is_some(),
        }
    }
}

#[derive(Debug)]
pub(super) struct ArrayKernels {
    signature: Vec<(Op, Kind)>,
    steps: Vec<ArrayStep>,
    constants: Vec<Option<DatumSlot>>,
    mod_constants: Vec<(usize, ScalarValue)>,
    false_value: DatumSlot,
    pub(super) fixed_bytes: usize,
    pub(super) bytes_per_lane: usize,
}
impl PartialEq for ArrayKernels {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}
impl Eq for ArrayKernels {}
impl Hash for ArrayKernels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

fn kind_type(kind: Kind) -> DataType {
    match kind {
        Kind::I32 => DataType::Int32,
        Kind::I64 => DataType::Int64,
        Kind::F32 => DataType::Float32,
        Kind::F64 => DataType::Float64,
        Kind::Bool => DataType::Boolean,
    }
}
fn constant_slot(kind: Kind, cell: Cell) -> Result<DatumSlot> {
    let value = match kind {
        Kind::I32 => ScalarValue::Int32(cell.valid.then_some(cell.bits as i32)),
        Kind::I64 => ScalarValue::Int64(cell.valid.then_some(cell.bits as i64)),
        Kind::F32 => ScalarValue::Float32(cell.valid.then_some(f32::from_bits(cell.bits as u32))),
        Kind::F64 => ScalarValue::Float64(cell.valid.then_some(f64::from_bits(cell.bits))),
        Kind::Bool => ScalarValue::Boolean(cell.valid.then_some(cell.bits != 0)),
    };
    Ok(DatumSlot {
        array: value.to_array()?,
        scalar: true,
    })
}
impl ArrayKernels {
    pub(super) fn prepare(program: &ListFlatMap) -> Option<Self> {
        use arrow_arith::{boolean, numeric};
        use arrow_ord::cmp;
        let mut steps = Vec::with_capacity(program.registers.len());
        let mut constants = Vec::<Option<DatumSlot>>::with_capacity(program.registers.len());
        let empty = Broadcasts::new(&[], 1).ok()?;
        let false_value = constant_slot(Kind::Bool, Cell::boolean(false)).ok()?;
        for register in &program.registers {
            let mut step = match register.op {
                Op::Constant(cell) => ArrayStep::Constant(constant_slot(register.kind, cell).ok()?),
                Op::Read(r) => ArrayStep::Read(r),
                Op::MissingRead(r) => ArrayStep::MissingRead(r),
                Op::Length(r) => ArrayStep::Length(r),
                Op::Missing(a) => ArrayStep::Missing(a),
                Op::NormalizeFloatZero(a) => ArrayStep::Normalize(a),
                Op::Cast(a) => ArrayStep::Cast(a, kind_type(register.kind)),
                Op::Unary(Unary::Pos, a) => ArrayStep::Alias(a),
                Op::Unary(Unary::Neg, a) => ArrayStep::Neg(a),
                Op::Unary(Unary::Not, a) => ArrayStep::Not(a),
                Op::Binary(op, a, b) => match op {
                    Binary::Add => ArrayStep::Numeric(numeric::add_wrapping, a, b),
                    Binary::Sub => ArrayStep::Numeric(numeric::sub_wrapping, a, b),
                    Binary::Mul => ArrayStep::Numeric(numeric::mul_wrapping, a, b),
                    Binary::Div => ArrayStep::Numeric(numeric::div, a, b),
                    Binary::Eq => ArrayStep::Compare(cmp::eq, a, b),
                    Binary::Ne => ArrayStep::Compare(cmp::neq, a, b),
                    Binary::Lt => ArrayStep::Compare(cmp::lt, a, b),
                    Binary::Le => ArrayStep::Compare(cmp::lt_eq, a, b),
                    Binary::Gt => ArrayStep::Compare(cmp::gt, a, b),
                    Binary::Ge => ArrayStep::Compare(cmp::gt_eq, a, b),
                    Binary::And => ArrayStep::Boolean(boolean::and_kleene, a, b),
                    Binary::Or => ArrayStep::Boolean(boolean::or_kleene, a, b),
                    Binary::Mod => ArrayStep::Mod(
                        a,
                        b,
                        Type::Primitive(match register.kind {
                            Kind::I32 => PrimitiveType::Int32,
                            Kind::I64 => PrimitiveType::Int64,
                            _ => return None,
                        }),
                    ),
                    _ => return None,
                },
                // Keep unimplemented conditional kernels on their original route.
                Op::Select(..) => return None,
            };
            let constant = if step.constant_inputs(&constants) {
                let value = execute_array_step(&step, &constants, &empty, &false_value, 1).ok()?;
                step = ArrayStep::Constant(value.clone());
                Some(value)
            } else {
                None
            };
            steps.push(step);
            constants.push(constant);
        }
        let mut mod_constants = Vec::new();
        for step in &steps {
            if let ArrayStep::Mod(a, b, _) = step {
                for &input in [a, b] {
                    if let Some(value) = &constants[input]
                        && !mod_constants.iter().any(|(old, _)| *old == input)
                    {
                        mod_constants.push((
                            input,
                            ScalarValue::try_from_array(value.array.as_ref(), 0).ok()?,
                        ));
                    }
                }
            }
        }
        // Conservative sum, not a claimed optimal live-range estimate. Reserve
        // complete slot payloads, shared indices/segments/masks, cached scalar
        // broadcasts and two widest native-kernel temporaries plus bitmap slack.
        let slot_bytes = program
            .registers
            .iter()
            .map(|r| match r.kind {
                Kind::I32 | Kind::F32 => 5,
                Kind::I64 | Kind::F64 => 9,
                Kind::Bool => 1,
            })
            .sum::<usize>();
        let broadcast_bytes = mod_constants
            .iter()
            .map(|(i, _)| match program.registers[*i].kind {
                Kind::I32 | Kind::F32 => 5,
                Kind::I64 | Kind::F64 => 9,
                Kind::Bool => 1,
            })
            .sum::<usize>();
        let constant_bytes = false_value.array.get_buffer_memory_size()
            + constants
                .iter()
                .flatten()
                .map(|s| s.array.get_buffer_memory_size())
                .sum::<usize>();
        let read_headers = program
            .reads
            .iter()
            .map(|read| {
                std::mem::size_of::<BoundRead>()
                    + read.path.len().max(4).next_power_of_two()
                        * std::mem::size_of::<(NullBuffer, usize)>()
            })
            .sum::<usize>();
        let fixed_bytes = 4096
            + constant_bytes
            + read_headers
            + program.registers.len() * (std::mem::size_of::<Option<DatumSlot>>() + 512)
            + program.children.len() * 128;
        Some(Self {
            signature: program
                .registers
                .iter()
                .map(|r| (r.op.clone(), r.kind))
                .collect(),
            steps,
            constants,
            mod_constants,
            false_value,
            fixed_bytes,
            bytes_per_lane: slot_bytes + broadcast_bytes + 42,
        })
    }
}

struct Broadcasts {
    numeric: Vec<(usize, ArrayRef)>,
    booleans: [ArrayRef; 3],
    scalar_booleans: [ArrayRef; 3],
}
impl Broadcasts {
    fn new(constants: &[(usize, ScalarValue)], capacity: usize) -> Result<Self> {
        let booleans = [Some(false), Some(true), None]
            .map(|value| Arc::new(BooleanArray::from(vec![value; capacity])) as ArrayRef);
        let scalar_booleans = booleans.each_ref().map(|array| array.slice(0, 1));
        Ok(Self {
            scalar_booleans,
            numeric: constants
                .iter()
                .map(|(i, value)| Ok((*i, value.to_array_of_size(capacity)?)))
                .collect::<Result<_>>()?,
            booleans,
        })
    }
    fn array(&self, id: usize, value: &DatumSlot, len: usize) -> Result<ArrayRef> {
        if !value.scalar || len == 1 {
            return Ok(value.array.clone());
        }
        if let Some((_, array)) = self.numeric.iter().find(|(i, _)| *i == id) {
            return Ok(array.slice(0, len));
        }
        if let Some(array) = value.array.as_any().downcast_ref::<BooleanArray>() {
            let at = if array.is_null(0) {
                2
            } else {
                usize::from(array.value(0))
            };
            return Ok(self.booleans[at].slice(0, len));
        }
        Err(DataFusionError::Internal(
            "unprepared scalar broadcast".into(),
        ))
    }
}
fn execute_array_step(
    step: &ArrayStep,
    slots: &[Option<DatumSlot>],
    broadcasts: &Broadcasts,
    false_value: &DatumSlot,
    len: usize,
) -> Result<DatumSlot> {
    let get = |i: usize| slots[i].as_ref().expect("prepared dependency is evaluated");
    let (array, scalar) = match step {
        ArrayStep::Constant(value) => return Ok(value.clone()),
        ArrayStep::Numeric(kernel, a, b) => {
            (kernel(get(*a), get(*b))?, get(*a).scalar && get(*b).scalar)
        }
        ArrayStep::Compare(kernel, a, b) => (
            Arc::new(kernel(get(*a), get(*b))?) as ArrayRef,
            get(*a).scalar && get(*b).scalar,
        ),
        ArrayStep::Boolean(kernel, a, b) => {
            let scalar = get(*a).scalar && get(*b).scalar;
            let len = if scalar { 1 } else { len };
            let a = broadcasts.array(*a, get(*a), len)?;
            let b = broadcasts.array(*b, get(*b), len)?;
            (
                Arc::new(kernel(
                    a.as_any().downcast_ref().unwrap(),
                    b.as_any().downcast_ref().unwrap(),
                )?) as ArrayRef,
                scalar,
            )
        }
        ArrayStep::Mod(a, b, dtype) => {
            let scalar = get(*a).scalar && get(*b).scalar;
            let len = if scalar { 1 } else { len };
            (
                crate::expressions::columnar_numeric(
                    Binary::Mod,
                    &broadcasts.array(*a, get(*a), len)?,
                    &broadcasts.array(*b, get(*b), len)?,
                    dtype,
                )
                .map_err(|e| DataFusionError::Execution(e.to_string()))?,
                scalar,
            )
        }
        ArrayStep::Normalize(a) => (
            datafusion_common::utils::normalize_float_zero(&get(*a).array),
            get(*a).scalar,
        ),
        ArrayStep::Cast(a, dtype) => (
            arrow_cast::cast(get(*a).array.as_ref(), dtype)?,
            get(*a).scalar,
        ),
        ArrayStep::Neg(a) => (
            arrow_arith::numeric::neg_wrapping(get(*a).array.as_ref())?,
            get(*a).scalar,
        ),
        ArrayStep::Not(a) => {
            let value = get(*a);
            let values = value.array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if value.scalar {
                let at = if values.is_null(0) {
                    2
                } else {
                    usize::from(!values.value(0))
                };
                return Ok(DatumSlot {
                    array: broadcasts.scalar_booleans[at].clone(),
                    scalar: true,
                });
            }
            (
                Arc::new(arrow_arith::boolean::not(values)?) as ArrayRef,
                false,
            )
        }
        ArrayStep::Missing(a) => {
            if get(*a).array.null_count() == 0 {
                return Ok(false_value.clone());
            }
            (
                Arc::new(arrow_arith::boolean::is_null(get(*a).array.as_ref())?) as ArrayRef,
                get(*a).scalar,
            )
        }
        ArrayStep::Alias(a) => return Ok(get(*a).clone()),
        ArrayStep::Read(_) | ArrayStep::MissingRead(_) | ArrayStep::Length(_) => unreachable!(),
    };
    Ok(DatumSlot { array, scalar })
}

#[derive(Clone, Copy)]
struct ArrayDomain<'a> {
    start: usize,
    len: usize,
    selected: Option<&'a UInt32Array>,
}
impl ArrayDomain<'_> {
    fn output_len(self) -> usize {
        self.selected.map_or(self.len, |ix| ix.len())
    }
}
fn read_nulls(source: &BoundRead, domain: ArrayDomain<'_>) -> Result<Option<NullBuffer>> {
    let mut nulls = source
        .nulls
        .as_ref()
        .map(|mask| mask.slice(domain.start, domain.len));
    for (mask, coordinate) in &source.ancestors {
        if *coordinate == 1 {
            nulls = NullBuffer::union(nulls.as_ref(), Some(&mask.slice(domain.start, domain.len)));
        }
    }
    if let (Some(nulls), Some(indices)) = (&nulls, domain.selected) {
        let values = BooleanArray::new(nulls.inner().clone(), None);
        let values = arrow_select::take::take(&values, indices, None)?;
        return Ok(Some(NullBuffer::new(
            values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .values()
                .clone(),
        )));
    }
    Ok(nulls)
}
fn attach_nulls(array: ArrayRef, nulls: Option<NullBuffer>) -> Result<ArrayRef> {
    if nulls.is_none() && array.nulls().is_none() {
        return Ok(array);
    }
    Ok(arrow_array::make_array(
        array.to_data().into_builder().nulls(nulls).build()?,
    ))
}
fn read_array(
    step: &ArrayStep,
    reads: &[BoundRead],
    false_value: &DatumSlot,
    domain: ArrayDomain<'_>,
) -> Result<DatumSlot> {
    let (read, missing, length) = match *step {
        ArrayStep::Read(i) => (i, false, false),
        ArrayStep::MissingRead(i) => (i, true, false),
        ArrayStep::Length(i) => (i, false, true),
        _ => unreachable!(),
    };
    let source = &reads[read];
    let nulls = read_nulls(source, domain)?;
    if missing {
        let Some(nulls) = nulls else {
            return Ok(false_value.clone());
        };
        return Ok(DatumSlot {
            array: Arc::new(BooleanArray::new(!nulls.inner(), None)),
            scalar: false,
        });
    }
    // A length kernel inspects only offsets. Never gather a nested list's
    // children just to select its outer lengths.
    let mut array = source.values.slice(domain.start, domain.len);
    if length {
        array = crate::array_length::lengths(&array)?;
    }
    if let Some(indices) = domain.selected {
        array = arrow_select::take::take(array.as_ref(), indices, None)?;
    }
    Ok(DatumSlot {
        array: attach_nulls(array, nulls)?,
        scalar: false,
    })
}
fn evaluate_arrays(
    bundle: &MultiReduce,
    reads: &[BoundRead],
    slots: &mut [Option<DatumSlot>],
    broadcasts: &Broadcasts,
    domain: ArrayDomain<'_>,
    guard: bool,
    reuse_guards: bool,
) -> Result<()> {
    for (index, (register, step)) in bundle
        .program
        .registers
        .iter()
        .zip(&bundle.kernels.steps)
        .enumerate()
    {
        if (if guard {
            register.guards == 0
        } else {
            register.leaves == 0
        }) || (!guard && reuse_guards && register.guards != 0)
        {
            continue;
        }
        let value = match step {
            ArrayStep::Read(_) | ArrayStep::MissingRead(_) | ArrayStep::Length(_) => {
                read_array(step, reads, &bundle.kernels.false_value, domain)?
            }
            _ => execute_array_step(
                step,
                slots,
                broadcasts,
                &bundle.kernels.false_value,
                domain.output_len(),
            )?,
        };
        slots[index] = Some(value);
    }
    Ok(())
}
