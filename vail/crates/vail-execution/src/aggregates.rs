//! Hail numeric/Boolean identities with typed Arrow batch reductions.
use crate::expressions::ExprError;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Count,
    Sum,
    Product,
    Min,
    Max,
    Fraction,
    Any,
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
            Kind::Fraction => {
                self.count = self.count.wrapping_add(input.len() as i64);
                self.integer = self
                    .integer
                    .wrapping_add(Self::boolean(input)?.true_count() as i64);
            }
            Kind::Any => self.seen |= Self::boolean(input)?.true_count() != 0,
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
                    self.integer = values
                        .iter()
                        .flatten()
                        .fold(self.integer, i64::wrapping_mul);
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
                    self.float = values
                        .iter()
                        .flatten()
                        .fold(self.float, |product, value| product * value);
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
            Kind::Count => ScalarValue::Int64(Some(self.count)),
            Kind::Fraction => ScalarValue::Float64(
                (self.count != 0).then(|| self.integer as f64 / self.count as f64),
            ),
            Kind::Any => ScalarValue::Boolean(Some(self.seen)),
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
            Kind::Count => vec![ScalarValue::Int64(Some(self.count))],
            Kind::Fraction => vec![
                ScalarValue::Int64(Some(self.count)),
                ScalarValue::Int64(Some(self.integer)),
            ],
            Kind::Any | Kind::All => vec![ScalarValue::Boolean(Some(self.seen))],
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
            Kind::Count => {
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
            Kind::Any | Kind::All => self.seen |= Self::boolean(&states[0])?.true_count() != 0,
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
    if matches!(function, "mean" | "stddev_pop") {
        return crate::moments::udf(function, input);
    }
    let (kind, input, output, state) = match function {
        "count" => (
            Kind::Count,
            DataType::Int64,
            DataType::Int64,
            vec![DataType::Int64],
        ),
        "fraction" | "any" | "all" => {
            if input.is_some_and(|ty| ty != &DataType::Boolean) {
                return Err(ExprError::Invalid(format!(
                    "{function} requires Boolean input"
                )));
            }
            match function {
                "fraction" => (
                    Kind::Fraction,
                    DataType::Boolean,
                    DataType::Float64,
                    vec![DataType::Int64, DataType::Int64],
                ),
                "any" => (
                    Kind::Any,
                    DataType::Boolean,
                    DataType::Boolean,
                    vec![DataType::Boolean],
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
