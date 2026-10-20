//! Hail numeric/Boolean identities with typed Arrow batch reductions.
use crate::expressions::ExprError;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
};
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
    function::{AccumulatorArgs, StateFieldsArgs},
};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Hail's SUM contract has a stable widened type and a zero identity, while
/// DataFusion's native SUM owns the actual arithmetic and merge algorithm.
#[derive(Debug, Clone)]
struct SumAdapter {
    input: DataType,
    normalized: DataType,
    output: DataType,
    signature: Signature,
    native: Arc<AggregateUDF>,
}
impl PartialEq for SumAdapter {
    fn eq(&self, other: &Self) -> bool {
        (&self.input, &self.normalized, &self.output)
            == (&other.input, &other.normalized, &other.output)
    }
}
impl Eq for SumAdapter {}
impl Hash for SumAdapter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.normalized.hash(state);
        self.output.hash(state);
    }
}

fn sum_type(input: &DataType) -> Option<DataType> {
    match input {
        DataType::Boolean | DataType::Int32 | DataType::Int64 => Some(DataType::Int64),
        DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
        _ => None,
    }
}

fn sum_zero(dtype: &DataType) -> Result<ScalarValue> {
    match dtype {
        DataType::Int64 => Ok(ScalarValue::Int64(Some(0))),
        DataType::Float64 => Ok(ScalarValue::Float64(Some(0.0))),
        _ => Err(DataFusionError::Plan(format!(
            "unsupported SUM result {dtype}"
        ))),
    }
}

fn normalize_sum_array(values: &ArrayRef, dtype: &DataType) -> Result<ArrayRef> {
    Ok(arrow_cast::cast(values, dtype)?)
}

#[derive(Debug)]
struct SumAccumulator {
    inner: Box<dyn Accumulator>,
    input: DataType,
    normalized: DataType,
    output: DataType,
}
impl Accumulator for SumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value = values
            .first()
            .ok_or_else(|| DataFusionError::Execution("SUM requires one input".into()))?;
        if value.data_type() != &self.input {
            return Err(DataFusionError::Execution("SUM input type mismatch".into()));
        }
        self.inner
            .update_batch(&[normalize_sum_array(value, &self.normalized)?])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.inner.evaluate()?;
        if value.is_null() {
            sum_zero(&self.output)
        } else {
            Ok(value)
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.inner.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value = values
            .first()
            .ok_or_else(|| DataFusionError::Execution("SUM requires one input".into()))?;
        if value.data_type() != &self.input {
            return Err(DataFusionError::Execution("SUM input type mismatch".into()));
        }
        self.inner
            .retract_batch(&[normalize_sum_array(value, &self.normalized)?])
    }
    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

struct SumGroups {
    inner: Box<dyn GroupsAccumulator>,
    input: DataType,
    normalized: DataType,
    output: DataType,
}
impl std::fmt::Debug for SumGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeSumGroups")
            .field("input", &self.input)
            .field("output", &self.output)
            .finish()
    }
}
impl SumGroups {
    fn normalize(&self, values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        let [value] = values else {
            return Err(DataFusionError::Execution("SUM requires one input".into()));
        };
        if value.data_type() != &self.input {
            return Err(DataFusionError::Execution("SUM input type mismatch".into()));
        }
        Ok(vec![normalize_sum_array(value, &self.normalized)?])
    }
    fn fill_empty(&self, values: ArrayRef) -> Result<ArrayRef> {
        if values.null_count() == 0 {
            return Ok(values);
        }
        let missing = arrow_arith::boolean::is_null(values.as_ref())?;
        match &self.output {
            DataType::Int64 => Ok(arrow_select::zip::zip(
                &missing,
                &Int64Array::new_scalar(0),
                &values,
            )?),
            DataType::Float64 => Ok(arrow_select::zip::zip(
                &missing,
                &Float64Array::new_scalar(0.0),
                &values,
            )?),
            _ => Err(DataFusionError::Plan("unsupported SUM output".into())),
        }
    }
}
impl GroupsAccumulator for SumGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        indices: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(&self.normalize(values)?, indices, filter, total)
    }
    fn evaluate(&mut self, to: EmitTo) -> Result<ArrayRef> {
        let values = self.inner.evaluate(to)?;
        self.fill_empty(values)
    }
    fn state(&mut self, to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(to)
    }
    fn merge_batch(&mut self, states: &[ArrayRef], indices: &[usize], total: usize) -> Result<()> {
        self.inner.merge_batch(states, indices, total)
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner
            .convert_to_state(&self.normalize(values)?, filter)
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.inner.size()
    }
}

impl AggregateUDFImpl for SumAdapter {
    fn name(&self) -> &str {
        "sum"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args != [self.input.clone()] {
            return Err(DataFusionError::Plan("SUM input type mismatch".into()));
        }
        Ok(self.output.clone())
    }
    fn is_nullable(&self) -> bool {
        false
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.expr_fields.len() != 1
            || args.expr_fields[0].data_type() != &self.input
            || args.return_field.data_type() != &self.output
        {
            return Err(DataFusionError::Plan(
                "SUM accumulator arguments mismatch".into(),
            ));
        }
        Ok(Box::new(SumAccumulator {
            inner: self.native.inner().accumulator(args)?,
            input: self.input.clone(),
            normalized: self.normalized.clone(),
            output: self.output.clone(),
        }))
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumAccumulator {
            inner: self.native.inner().create_sliding_accumulator(args)?,
            input: self.input.clone(),
            normalized: self.normalized.clone(),
            output: self.output.clone(),
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.native.inner().state_fields(args)
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
            && args.order_bys.is_empty()
            && !args.is_reversed
            && args.expr_fields.len() == 1
            && args.expr_fields[0].data_type() == &self.input
            && args.return_field.data_type() == &self.output
            && self.native.groups_accumulator_supported(args)
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if !self.groups_accumulator_supported(args.clone()) {
            return Err(DataFusionError::NotImplemented(
                "native SUM groups accumulator does not support these arguments".into(),
            ));
        }
        Ok(Box::new(SumGroups {
            inner: self.native.inner().create_groups_accumulator(args)?,
            input: self.input.clone(),
            normalized: self.normalized.clone(),
            output: self.output.clone(),
        }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Count,
    Sum,
    Product,
    Max,
    /// Compatibility path for direct Rust callers of `aggregate_udf`.
    /// Vail's planners lower count_where to native COUNT instead.
    CountWhere,
}
#[derive(Debug)]
struct Reducer {
    kind: Kind,
    input: DataType,
    count: i64,
    integer: i64,
    float: f64,
    seen: bool,
}
impl Reducer {
    fn new(kind: Kind, input: DataType) -> Self {
        Self {
            kind,
            input,
            count: 0,
            integer: if kind == Kind::Product { 1 } else { 0 },
            float: if kind == Kind::Product { 1. } else { 0. },
            seen: false,
        }
    }
    fn integral(&self) -> bool {
        matches!(self.input, DataType::Int32 | DataType::Int64)
    }
    fn extreme(&mut self, input: &ArrayRef) -> Result<()> {
        macro_rules! integer {
            ($array:ty,$convert:expr) => {{
                let values = input.as_any().downcast_ref::<$array>().unwrap();
                let value = arrow_arith::aggregate::max(values);
                if let Some(value) = value {
                    let value = ($convert)(value);
                    if !self.seen || value > self.integer {
                        self.integer = value;
                    }
                    self.seen = true;
                }
            }};
        }
        macro_rules! floating {
            ($array:ty,$native:ty) => {{
                let values = input.as_any().downcast_ref::<$array>().unwrap();
                let value = arrow_arith::aggregate::max(values);
                if let Some(value) = value {
                    // Arrow's batch extrema use IEEE total order; merges use
                    // this same comparator.
                    let order = value.total_cmp(&(self.float as $native));
                    if !self.seen || order.is_gt() {
                        self.float = value as f64;
                    }
                    self.seen = true;
                }
            }};
        }
        match self.input {
            DataType::Int32 => integer!(Int32Array, |x: i32| i64::from(x)),
            DataType::Int64 => integer!(Int64Array, |x: i64| x),
            DataType::Float32 => floating!(Float32Array, f32),
            DataType::Float64 => floating!(Float64Array, f64),
            _ => unreachable!(),
        };
        Ok(())
    }
    fn value(&self) -> ScalarValue {
        if self.integral() {
            if self.input == DataType::Int32 && self.kind == Kind::Max {
                ScalarValue::Int32(Some(self.integer as i32))
            } else {
                ScalarValue::Int64(Some(self.integer))
            }
        } else if self.input == DataType::Float32 && self.kind == Kind::Max {
            ScalarValue::Float32(Some(self.float as f32))
        } else {
            ScalarValue::Float64(Some(self.float))
        }
    }
    fn boolean(input: &ArrayRef) -> Result<&BooleanArray> {
        input
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("Boolean aggregate requires Boolean input".into())
            })
    }
    fn counts(input: &ArrayRef) -> Result<&Int64Array> {
        input.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            DataFusionError::Execution("aggregate count state requires int64".into())
        })
    }
}
impl Accumulator for Reducer {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // DataFusion may append ordering arguments; only the value is consumed.
        let input = values
            .first()
            .ok_or_else(|| DataFusionError::Execution("aggregate requires input".into()))?;
        if input.data_type() != &self.input {
            return Err(DataFusionError::Execution(
                "aggregate input type differs from declared type".into(),
            ));
        }
        match self.kind {
            Kind::Count => {
                self.count = self
                    .count
                    .wrapping_add((input.len() - input.null_count()) as i64)
            }
            Kind::CountWhere => {
                self.count = self
                    .count
                    .wrapping_add(Self::boolean(input)?.true_count() as i64)
            }
            Kind::Max => self.extreme(input)?,
            Kind::Sum | Kind::Product if self.integral() => {
                let cast = arrow_cast::cast(input, &DataType::Int64)?;
                let values = cast.as_any().downcast_ref::<Int64Array>().unwrap();
                if self.kind == Kind::Sum {
                    self.integer = self
                        .integer
                        .wrapping_add(arrow_arith::aggregate::sum(values).unwrap_or(0));
                } else {
                    self.integer = self
                        .integer
                        .wrapping_mul(arrow_arith::aggregate::product(values).unwrap_or(1));
                }
                self.count = self
                    .count
                    .wrapping_add((values.len() - values.null_count()) as i64);
            }
            Kind::Sum | Kind::Product => {
                let cast = arrow_cast::cast(input, &DataType::Float64)?;
                let values = cast.as_any().downcast_ref::<Float64Array>().unwrap();
                if self.kind == Kind::Sum {
                    self.float = values
                        .iter()
                        .flatten()
                        .fold(self.float, |sum, value| sum + value);
                } else {
                    self.float *= arrow_arith::aggregate::product(values).unwrap_or(1.0);
                }
                self.count = self
                    .count
                    .wrapping_add((values.len() - values.null_count()) as i64);
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(match self.kind {
            Kind::Count | Kind::CountWhere => ScalarValue::Int64(Some(self.count)),
            Kind::Max if !self.seen => ScalarValue::try_from(&self.input)?,
            _ => self.value(),
        })
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(match self.kind {
            Kind::Count | Kind::CountWhere => vec![ScalarValue::Int64(Some(self.count))],
            Kind::Max => vec![self.evaluate()?],
            Kind::Sum | Kind::Product => vec![ScalarValue::Int64(Some(self.count)), self.value()],
        })
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let expected = if matches!(self.kind, Kind::Sum | Kind::Product) {
            2
        } else {
            1
        };
        if states.len() != expected || states.iter().any(|array| array.len() != states[0].len()) {
            return Err(DataFusionError::Execution(
                "aggregate state shape mismatch".into(),
            ));
        }
        match self.kind {
            Kind::Count | Kind::CountWhere => {
                self.count = self.count.wrapping_add(
                    arrow_arith::aggregate::sum(Self::counts(&states[0])?).unwrap_or(0),
                )
            }
            Kind::Max => self.update_batch(&states[..1])?,
            Kind::Sum | Kind::Product => {
                self.count = self.count.wrapping_add(
                    arrow_arith::aggregate::sum(Self::counts(&states[0])?).unwrap_or(0),
                );
                if self.integral() {
                    let values = Self::counts(&states[1])?;
                    if self.kind == Kind::Sum {
                        self.integer = self
                            .integer
                            .wrapping_add(arrow_arith::aggregate::sum(values).unwrap_or(0));
                    } else {
                        self.integer = values
                            .iter()
                            .flatten()
                            .fold(self.integer, i64::wrapping_mul);
                    }
                } else {
                    let values = states[1]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "aggregate value state requires float64".into(),
                            )
                        })?;
                    if self.kind == Kind::Sum {
                        self.float = values
                            .iter()
                            .flatten()
                            .fold(self.float, |sum, value| sum + value);
                    } else {
                        self.float = values
                            .iter()
                            .flatten()
                            .fold(self.float, |product, value| product * value);
                    }
                }
            }
        }
        Ok(())
    }
}
/// Construct one of the low-level scalar aggregate UDAFs.
///
/// Vail's planners lower `count_where` to native COUNT over a false-to-null
/// Boolean predicate. The legacy direct-factory route remains available for
/// Rust callers that explicitly construct this low-level UDAF.
pub fn aggregate_udf(
    function: &str,
    input: Option<&DataType>,
) -> std::result::Result<Arc<AggregateUDF>, ExprError> {
    if function == "fraction" {
        return crate::fraction::udf(input);
    }
    if function == "sum" {
        return sum_udf(input);
    }
    if function == "min" {
        let input = input.ok_or_else(|| ExprError::Invalid("min requires input".into()))?;
        if !matches!(
            input,
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
        ) {
            return Err(ExprError::Invalid("min requires numeric input".into()));
        }
        return Ok(datafusion_functions_aggregate::min_max::min_udaf());
    }
    if function == "product" {
        return product_udf(input);
    }
    if function == "mean" {
        return crate::native_mean::udf(input);
    }
    if function == "stddev_pop" {
        return crate::moments::udf(function, input);
    }
    if function == "all" {
        if input.is_some_and(|ty| ty != &DataType::Boolean) {
            return Err(ExprError::Invalid("all requires Boolean input".into()));
        }
        return Ok(crate::native_bool::total_bool_udaf("all"));
    }
    if function == "any" {
        if input.is_some_and(|ty| ty != &DataType::Boolean) {
            return Err(ExprError::Invalid("any requires Boolean input".into()));
        }
        return Ok(crate::native_bool::total_bool_udaf("any"));
    }
    let (kind, input, output, state) = match function {
        "count" => (
            Kind::Count,
            DataType::Int64,
            DataType::Int64,
            vec![DataType::Int64],
        ),
        "count_where" => {
            if input.is_some_and(|ty| ty != &DataType::Boolean) {
                return Err(ExprError::Invalid(format!(
                    "{function} requires Boolean input"
                )));
            }
            match function {
                "count_where" => (
                    Kind::CountWhere,
                    DataType::Boolean,
                    DataType::Int64,
                    vec![DataType::Int64],
                ),
                _ => unreachable!("Boolean aggregate name is handled above"),
            }
        }
        "sum" | "product" | "max" => {
            let input =
                input.ok_or_else(|| ExprError::Invalid(format!("{function} requires input")))?;
            if !matches!(
                input,
                DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
            ) {
                return Err(ExprError::Invalid(format!(
                    "{function} requires numeric input"
                )));
            }
            let kind = match function {
                "sum" => Kind::Sum,
                "product" => Kind::Product,
                _ => Kind::Max,
            };
            let output = if kind == Kind::Max {
                input.clone()
            } else if matches!(input, DataType::Int32 | DataType::Int64) {
                DataType::Int64
            } else {
                DataType::Float64
            };
            let state = if kind == Kind::Max {
                vec![output.clone()]
            } else {
                vec![DataType::Int64, output.clone()]
            };
            (kind, input.clone(), output, state)
        }
        _ => {
            return Err(ExprError::Invalid(format!(
                "unknown aggregate {function:?}"
            )));
        }
    };
    Ok(crate::kernel_identity::aggregate_named(
        function,
        (function.to_owned(), input.clone()),
        vec![input.clone()],
        output,
        state,
        Arc::new(move |_| Ok(Box::new(Reducer::new(kind, input.clone())))),
    ))
}

fn sum_udf(input: Option<&DataType>) -> std::result::Result<Arc<AggregateUDF>, ExprError> {
    let input = input.ok_or_else(|| ExprError::Invalid("sum requires input".into()))?;
    let normalized = sum_type(input)
        .ok_or_else(|| ExprError::Invalid("sum requires Boolean or numeric input".into()))?;
    let native = datafusion_functions_aggregate::sum::sum_udaf();
    let output = native.return_type(std::slice::from_ref(&normalized))?;
    Ok(Arc::new(AggregateUDF::from(SumAdapter {
        input: input.clone(),
        normalized,
        output,
        signature: Signature::exact(vec![input.clone()], Volatility::Immutable),
        native,
    })))
}

fn product_udf(input: Option<&DataType>) -> std::result::Result<Arc<AggregateUDF>, ExprError> {
    let input = input
        .ok_or_else(|| ExprError::Invalid("product requires input".into()))?
        .clone();
    if !matches!(
        input,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    ) {
        return Err(ExprError::Invalid("product requires numeric input".into()));
    }
    let output = if matches!(input, DataType::Int32 | DataType::Int64) {
        DataType::Int64
    } else {
        DataType::Float64
    };
    let group_input = input.clone();
    let support_input = input.clone();
    let group_output = output.clone();
    Ok(crate::kernel_identity::aggregate_named_with_groups(
        "product",
        ("product".to_owned(), input.clone()),
        vec![input.clone()],
        output.clone(),
        vec![DataType::Int64, output.clone()],
        Arc::new(move |_| Ok(Box::new(Reducer::new(Kind::Product, input.clone())))),
        Some(Arc::new(move || {
            Ok(Box::new(ProductGroups::new(
                group_input.clone(),
                group_output.clone(),
            )))
        })),
        Some(Arc::new(
            move |args: &datafusion_expr::function::AccumulatorArgs<'_>| {
                !args.is_distinct
                    && args.order_bys.is_empty()
                    && !args.is_reversed
                    && args.expr_fields.len() == 1
                    && args.expr_fields[0].data_type() == &support_input
            },
        )),
    ))
}

/// Contiguous per-group product state. The count column remains in the wire
/// state for compatibility with existing partial aggregate plans.
struct ProductGroups {
    input: DataType,
    output: DataType,
    integer: Option<Vec<i64>>,
    float: Option<Vec<f64>>,
    count: Vec<i64>,
}
impl ProductGroups {
    fn new(input: DataType, output: DataType) -> Self {
        let integral = output == DataType::Int64;
        Self {
            input,
            output,
            integer: integral.then(Vec::new),
            float: (!integral).then(Vec::new),
            count: Vec::new(),
        }
    }
    fn ensure(&mut self, total: usize) {
        if let Some(values) = &mut self.integer {
            values.resize(total, 1);
        }
        if let Some(values) = &mut self.float {
            values.resize(total, 1.);
        }
        self.count.resize(total, 0);
    }
    fn emit<T>(values: &mut Vec<T>, to: EmitTo) -> Vec<T> {
        to.take_needed(values)
    }
    fn update_values(
        &mut self,
        array: &ArrayRef,
        indices: &[usize],
        filter: Option<&BooleanArray>,
    ) -> Result<()> {
        if self.output == DataType::Int64 {
            let integer = self
                .integer
                .as_mut()
                .expect("integer output has integer state");
            macro_rules! multiply_rows {
                ($values:expr) => {
                    for (row, value) in $values.iter().enumerate() {
                        if filter.is_none_or(|f| f.is_valid(row) && f.value(row)) {
                            if let Some(v) = value {
                                let i = indices[row];
                                integer[i] = integer[i].wrapping_mul(i64::from(v));
                                self.count[i] = self.count[i].wrapping_add(1);
                            }
                        }
                    }
                };
            }
            match &self.input {
                DataType::Int32 => {
                    multiply_rows!(array.as_any().downcast_ref::<Int32Array>().unwrap())
                }
                DataType::Int64 => {
                    multiply_rows!(array.as_any().downcast_ref::<Int64Array>().unwrap())
                }
                _ => unreachable!(),
            }
        } else {
            let float = self.float.as_mut().expect("float output has float state");
            let cast = arrow_cast::cast(array, &DataType::Float64)?;
            let values = cast.as_any().downcast_ref::<Float64Array>().unwrap();
            for (row, value) in values.iter().enumerate() {
                if filter.is_none_or(|f| f.is_valid(row) && f.value(row)) {
                    if let Some(v) = value {
                        let i = indices[row];
                        float[i] *= v;
                        self.count[i] = self.count[i].wrapping_add(1);
                    }
                }
            }
        }
        Ok(())
    }
    fn emit_state(&mut self, to: EmitTo) -> Result<Vec<ArrayRef>> {
        let count = Self::emit(&mut self.count, to);
        let values = if self.output == DataType::Int64 {
            Arc::new(Int64Array::from(Self::emit(
                self.integer.as_mut().unwrap(),
                to,
            ))) as ArrayRef
        } else {
            Arc::new(Float64Array::from(Self::emit(
                self.float.as_mut().unwrap(),
                to,
            ))) as ArrayRef
        };
        Ok(vec![Arc::new(Int64Array::from(count)), values])
    }
}
impl GroupsAccumulator for ProductGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        indices: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "product requires one input".into(),
            ));
        };
        if input.len() != indices.len()
            || filter.is_some_and(|f| f.len() != indices.len())
            || input.data_type() != &self.input
            || indices.iter().any(|i| *i >= total)
        {
            return Err(DataFusionError::Execution(
                "product grouping shape/type mismatch".into(),
            ));
        }
        self.ensure(total);
        self.update_values(input, indices, filter)
    }
    fn evaluate(&mut self, to: EmitTo) -> Result<ArrayRef> {
        let vals = if self.output == DataType::Int64 {
            Arc::new(Int64Array::from(Self::emit(
                self.integer.as_mut().unwrap(),
                to,
            ))) as ArrayRef
        } else {
            Arc::new(Float64Array::from(Self::emit(
                self.float.as_mut().unwrap(),
                to,
            ))) as ArrayRef
        };
        let _ = Self::emit(&mut self.count, to);
        Ok(vals)
    }
    fn state(&mut self, to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.emit_state(to)
    }
    fn merge_batch(&mut self, states: &[ArrayRef], indices: &[usize], total: usize) -> Result<()> {
        if states.len() != 2
            || states[0].len() != indices.len()
            || states[1].len() != indices.len()
            || indices.iter().any(|i| *i >= total)
        {
            return Err(DataFusionError::Execution(
                "product state shape mismatch".into(),
            ));
        }
        self.ensure(total);
        let counts = states[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("product count state must be int64".into())
            })?;
        if self.output == DataType::Int64 {
            let vals = states[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("product value state must be int64".into())
                })?;
            for (r, (i, value)) in indices.iter().zip(vals.iter()).enumerate() {
                if let Some(v) = value {
                    let integer = self.integer.as_mut().unwrap();
                    integer[*i] = integer[*i].wrapping_mul(v);
                }
                if counts.is_valid(r) {
                    self.count[*i] = self.count[*i].wrapping_add(counts.value(r));
                }
            }
        } else {
            let vals = states[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("product value state must be float64".into())
                })?;
            for (r, (i, value)) in indices.iter().zip(vals.iter()).enumerate() {
                if let Some(value) = value {
                    self.float.as_mut().unwrap()[*i] *= value;
                }
                if counts.is_valid(r) {
                    self.count[*i] = self.count[*i].wrapping_add(counts.value(r));
                }
            }
        }
        Ok(())
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "product requires one input".into(),
            ));
        };
        let rows = input.len();
        if input.data_type() != &self.input || filter.is_some_and(|f| f.len() != rows) {
            return Err(DataFusionError::Execution(
                "product grouping shape/type mismatch".into(),
            ));
        }
        let selected = |row: usize| filter.is_none_or(|f| f.is_valid(row) && f.value(row));
        let counts = Int64Array::from_iter_values((0..rows).map(|row| {
            if selected(row) && input.is_valid(row) {
                1
            } else {
                0
            }
        }));
        let products = if self.output == DataType::Int64 {
            let values = match &self.input {
                DataType::Int32 => input
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(row, value)| {
                        if selected(row) && value.is_some() {
                            i64::from(value.unwrap_or(1))
                        } else {
                            1
                        }
                    })
                    .collect::<Vec<_>>(),
                DataType::Int64 => input
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(row, value)| {
                        if selected(row) && value.is_some() {
                            value.unwrap_or(1)
                        } else {
                            1
                        }
                    })
                    .collect::<Vec<_>>(),
                _ => unreachable!(),
            };
            Arc::new(Int64Array::from(values)) as ArrayRef
        } else {
            let cast = arrow_cast::cast(input, &DataType::Float64)?;
            let values = cast.as_any().downcast_ref::<Float64Array>().unwrap();
            Arc::new(Float64Array::from_iter_values(
                values.iter().enumerate().map(|(row, value)| {
                    if selected(row) {
                        value.unwrap_or(1.)
                    } else {
                        1.
                    }
                }),
            )) as ArrayRef
        };
        Ok(vec![Arc::new(counts), products])
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.integer.as_ref().map_or(0, |v| v.capacity() * 8)
            + self.float.as_ref().map_or(0, |v| v.capacity() * 8)
            + self.count.capacity() * 8
    }
}
