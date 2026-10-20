//! Hail numeric/Boolean identities with typed Arrow batch reductions.
use crate::expressions::ExprError;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, EmitTo, GroupsAccumulator};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Count,
    Sum,
    Product,
    Min,
    Max,
    CountWhere,
    Fraction,
    All,
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
        let minimum = self.kind == Kind::Min;
        macro_rules! integer {
            ($array:ty,$convert:expr) => {{
                let values = input.as_any().downcast_ref::<$array>().unwrap();
                let value = if minimum {
                    arrow_arith::aggregate::min(values)
                } else {
                    arrow_arith::aggregate::max(values)
                };
                if let Some(value) = value {
                    let value = ($convert)(value);
                    if !self.seen
                        || (if minimum {
                            value < self.integer
                        } else {
                            value > self.integer
                        })
                    {
                        self.integer = value;
                    }
                    self.seen = true;
                }
            }};
        }
        macro_rules! floating {
            ($array:ty,$native:ty) => {{
                let values = input.as_any().downcast_ref::<$array>().unwrap();
                let value = if minimum {
                    arrow_arith::aggregate::min(values)
                } else {
                    arrow_arith::aggregate::max(values)
                };
                if let Some(value) = value {
                    // Arrow's batch extrema use IEEE total order, including
                    // NaN sign and -0 < +0; merges use this same comparator.
                    let order = value.total_cmp(&(self.float as $native));
                    if !self.seen
                        || (if minimum {
                            order.is_lt()
                        } else {
                            order.is_gt()
                        })
                    {
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
            if self.input == DataType::Int32 && matches!(self.kind, Kind::Min | Kind::Max) {
                ScalarValue::Int32(Some(self.integer as i32))
            } else {
                ScalarValue::Int64(Some(self.integer))
            }
        } else if self.input == DataType::Float32 && matches!(self.kind, Kind::Min | Kind::Max) {
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
            Kind::Fraction => {
                self.count = self.count.wrapping_add(input.len() as i64);
                self.integer = self
                    .integer
                    .wrapping_add(Self::boolean(input)?.true_count() as i64);
            }
            Kind::All => {
                let values = Self::boolean(input)?;
                self.seen |= values.len() - values.null_count() - values.true_count() != 0;
            }
            Kind::Min | Kind::Max => self.extreme(input)?,
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
            Kind::Fraction => ScalarValue::Float64(
                (self.count != 0).then(|| self.integer as f64 / self.count as f64),
            ),
            Kind::All => ScalarValue::Boolean(Some(!self.seen)),
            Kind::Min | Kind::Max if !self.seen => ScalarValue::try_from(&self.input)?,
            _ => self.value(),
        })
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(match self.kind {
            Kind::Count | Kind::CountWhere => vec![ScalarValue::Int64(Some(self.count))],
            Kind::Fraction => vec![
                ScalarValue::Int64(Some(self.count)),
                ScalarValue::Int64(Some(self.integer)),
            ],
            Kind::All => vec![ScalarValue::Boolean(Some(self.seen))],
            Kind::Min | Kind::Max => vec![self.evaluate()?],
            Kind::Sum | Kind::Product => vec![ScalarValue::Int64(Some(self.count)), self.value()],
        })
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let expected = if matches!(self.kind, Kind::Fraction | Kind::Sum | Kind::Product) {
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
            Kind::Fraction => {
                self.count = self.count.wrapping_add(
                    arrow_arith::aggregate::sum(Self::counts(&states[0])?).unwrap_or(0),
                );
                self.integer = self.integer.wrapping_add(
                    arrow_arith::aggregate::sum(Self::counts(&states[1])?).unwrap_or(0),
                );
            }
            Kind::All => self.seen |= Self::boolean(&states[0])?.true_count() != 0,
            Kind::Min | Kind::Max => self.update_batch(&states[..1])?,
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
pub fn aggregate_udf(
    function: &str,
    input: Option<&DataType>,
) -> std::result::Result<Arc<AggregateUDF>, ExprError> {
    if function == "product" {
        return product_udf(input);
    }
    if matches!(function, "mean" | "stddev_pop") {
        return crate::moments::udf(function, input);
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
        "count_where" | "fraction" | "all" => {
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
                "fraction" => (
                    Kind::Fraction,
                    DataType::Boolean,
                    DataType::Float64,
                    vec![DataType::Int64, DataType::Int64],
                ),
                _ => (
                    Kind::All,
                    DataType::Boolean,
                    DataType::Boolean,
                    vec![DataType::Boolean],
                ),
            }
        }
        "sum" | "product" | "min" | "max" => {
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
                "min" => Kind::Min,
                _ => Kind::Max,
            };
            let output = if matches!(kind, Kind::Min | Kind::Max) {
                input.clone()
            } else if matches!(input, DataType::Int32 | DataType::Int64) {
                DataType::Int64
            } else {
                DataType::Float64
            };
            let state = if matches!(kind, Kind::Min | Kind::Max) {
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
