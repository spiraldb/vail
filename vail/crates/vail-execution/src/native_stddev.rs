//! DataFusion's native population standard deviation with Hail's empty-result
//! convention and Vail's numeric-input signature.
use arrow_array::{Array, ArrayRef, Float64Array, UInt64Array};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, Expr, GroupsAccumulator, Signature,
    function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NativeStddevPop {
    input: DataType,
    finite_input: bool,
    signature: Signature,
}

pub(crate) fn coordinate_stddev(udf: &AggregateUDF) -> bool {
    udf.inner().downcast_ref::<NativeStddevPop>().is_some()
}

fn native() -> Arc<AggregateUDF> {
    datafusion_functions_aggregate::stddev::stddev_pop_udaf()
}

fn cast_values(values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    if values.len() != 1 {
        return Err(DataFusionError::Execution(
            "stddev_pop requires one input".into(),
        ));
    }
    Ok(vec![arrow_cast::cast(&values[0], &DataType::Float64)?])
}

fn nan_for_empty(value: ScalarValue) -> ScalarValue {
    match value {
        ScalarValue::Float64(None) => ScalarValue::Float64(Some(f64::NAN)),
        value => value,
    }
}

fn nonfinite_count(values: &Float64Array) -> Result<(u64, bool)> {
    let mut count = 0u64;
    for _ in values.iter().flatten().filter(|value| !value.is_finite()) {
        count = count.checked_add(1).ok_or_else(|| {
            DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
        })?;
    }
    Ok((count, count != 0))
}

fn mask_nonfinite(values: &[ArrayRef], has_nonfinite: bool) -> Vec<ArrayRef> {
    let input = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
    if !has_nonfinite {
        return values.to_vec();
    }
    let finite = BooleanBuffer::collect_bool(input.len(), |row| {
        !input.is_valid(row) || input.value(row).is_finite()
    });
    let nonfinite_nulls = NullBuffer::new(finite);
    let nulls = NullBuffer::union(input.nulls(), Some(&nonfinite_nulls));
    vec![Arc::new(Float64Array::new(input.values().clone(), nulls))]
}

fn state_flag(states: &[ArrayRef]) -> Result<&UInt64Array> {
    states
        .get(3)
        .and_then(|state| state.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| DataFusionError::Execution("stddev_pop nonfinite state is missing".into()))
}

fn filter_passes(filter: Option<&arrow_array::BooleanArray>, row: usize) -> bool {
    filter.is_none_or(|filter| filter.is_valid(row) && filter.value(row))
}

impl AggregateUDFImpl for NativeStddevPop {
    fn name(&self) -> &str {
        "stddev_pop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args != [self.input.clone()] {
            return Err(DataFusionError::Plan(
                "stddev_pop input signature mismatch".into(),
            ));
        }
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let name = args.name.to_owned();
        let mut fields = native().inner().state_fields(args)?;
        fields.push(Arc::new(Field::new(
            format!("{name}[nonfinite]"),
            DataType::UInt64,
            false,
        )));
        Ok(fields)
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        if self.finite_input {
            return None;
        }
        let mut finite = self.clone();
        finite.finite_input = true;
        Some(Box::new(move |mut aggregate, info| {
            // Keep the cast visible and shared with sibling aggregates. Only
            // its integer source proves finite values; Float32/64 do not.
            if let [Expr::Cast(cast)] = aggregate.params.args.as_slice()
                && cast.field.data_type() == &DataType::Float64
                && matches!(
                    info.get_data_type(&cast.expr)?,
                    DataType::Int32 | DataType::Int64
                )
            {
                aggregate.func = Arc::new(AggregateUDF::from(finite.clone()));
            }
            Ok(Expr::AggregateFunction(aggregate))
        }))
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let inner = native().inner().accumulator(args)?;
        Ok(if self.finite_input {
            Box::new(NativeStddevAccumulator::<true> {
                inner,
                nonfinite_count: 0,
            })
        } else {
            Box::new(NativeStddevAccumulator::<false> {
                inner,
                nonfinite_count: 0,
            })
        })
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        native().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let inner = native().create_groups_accumulator(args)?;
        Ok(if self.finite_input {
            Box::new(NativeStddevGroups::<true> {
                inner,
                nonfinite: Vec::new(),
            })
        } else {
            Box::new(NativeStddevGroups::<false> {
                inner,
                nonfinite: Vec::new(),
            })
        })
    }
}

#[derive(Debug)]
struct NativeStddevAccumulator<const FINITE: bool> {
    inner: Box<dyn Accumulator>,
    nonfinite_count: u64,
}

impl<const FINITE: bool> Accumulator for NativeStddevAccumulator<FINITE> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = cast_values(values)?;
        if FINITE {
            return self.inner.update_batch(&values);
        }
        let input = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let (nonfinite_count, has_nonfinite) = nonfinite_count(input)?;
        self.nonfinite_count = self
            .nonfinite_count
            .checked_add(nonfinite_count)
            .ok_or_else(|| {
                DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
            })?;
        self.inner
            .update_batch(&mask_nonfinite(&values, has_nonfinite))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.nonfinite_count != 0 {
            return Ok(ScalarValue::Float64(Some(f64::NAN)));
        }
        Ok(nan_for_empty(self.inner.evaluate()?))
    }

    fn size(&self) -> usize {
        self.inner.size() + std::mem::size_of::<u64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut state = self.inner.state()?;
        state.push(ScalarValue::UInt64(Some(self.nonfinite_count)));
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let nonfinite = state_flag(states)?;
        let merged_nonfinite = nonfinite
            .iter()
            .flatten()
            .try_fold(0u64, |total, value| total.checked_add(value))
            .ok_or_else(|| {
                DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
            })?;
        self.nonfinite_count = self
            .nonfinite_count
            .checked_add(merged_nonfinite)
            .ok_or_else(|| {
                DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
            })?;
        self.inner.merge_batch(&states[..3])
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = cast_values(values)?;
        if FINITE {
            return self.inner.retract_batch(&values);
        }
        let input = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let (nonfinite, has_nonfinite) = nonfinite_count(input)?;
        self.inner
            .retract_batch(&mask_nonfinite(&values, has_nonfinite))?;
        self.nonfinite_count = self.nonfinite_count.saturating_sub(nonfinite);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

struct NativeStddevGroups<const FINITE: bool> {
    inner: Box<dyn GroupsAccumulator>,
    nonfinite: Vec<u64>,
}

impl<const FINITE: bool> GroupsAccumulator for NativeStddevGroups<FINITE> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        filter: Option<&arrow_array::BooleanArray>,
        total_groups: usize,
    ) -> Result<()> {
        let casted = cast_values(values)?;
        let input = casted[0].as_any().downcast_ref::<Float64Array>().unwrap();
        if groups.len() != input.len() || filter.is_some_and(|filter| filter.len() != input.len()) {
            return Err(DataFusionError::Execution(
                "stddev_pop grouped input lengths differ".into(),
            ));
        }
        if groups.iter().any(|&group| group >= total_groups) {
            return Err(DataFusionError::Execution(
                "stddev_pop group index exceeds state".into(),
            ));
        }
        self.nonfinite.resize(total_groups, 0);
        if FINITE {
            return self
                .inner
                .update_batch(&casted, groups, filter, total_groups);
        }
        let mut has_nonfinite = false;
        for (row, &group) in groups.iter().enumerate() {
            if input.is_valid(row) && !input.value(row).is_finite() {
                has_nonfinite = true;
                if !filter_passes(filter, row) {
                    continue;
                }
                let count = self.nonfinite.get_mut(group).ok_or_else(|| {
                    DataFusionError::Execution("stddev_pop group index exceeds state".into())
                })?;
                *count = count.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
                })?;
            }
        }
        self.inner.update_batch(
            &mask_nonfinite(&casted, has_nonfinite),
            groups,
            filter,
            total_groups,
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = self.inner.evaluate(emit_to)?;
        let native_values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        let nonfinite = self.emit_nonfinite(native_values.len(), emit_to)?;
        if native_values.null_count() == 0 && nonfinite.iter().all(|&count| count == 0) {
            return Ok(values);
        }
        Ok(Arc::new(Float64Array::from_iter_values(
            native_values.iter().zip(nonfinite).map(|(value, count)| {
                if count != 0 {
                    f64::NAN
                } else {
                    value.unwrap_or(f64::NAN)
                }
            }),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut states = self.inner.state(emit_to)?;
        let count = states.first().map_or(0, |array| array.len());
        states.push(Arc::new(UInt64Array::from(
            self.emit_nonfinite(count, emit_to)?,
        )));
        Ok(states)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        total_groups: usize,
    ) -> Result<()> {
        let nonfinite = state_flag(values)?;
        if nonfinite.len() != groups.len() {
            return Err(DataFusionError::Execution(
                "stddev_pop grouped state lengths differ".into(),
            ));
        }
        if groups.iter().any(|&group| group >= total_groups) {
            return Err(DataFusionError::Execution(
                "stddev_pop group index exceeds state".into(),
            ));
        }
        self.nonfinite.resize(total_groups, 0);
        for (row, &group) in groups.iter().enumerate() {
            let count = self.nonfinite.get_mut(group).ok_or_else(|| {
                DataFusionError::Execution("stddev_pop group index exceeds state".into())
            })?;
            *count = count.checked_add(nonfinite.value(row)).ok_or_else(|| {
                DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
            })?;
        }
        self.inner.merge_batch(&values[..3], groups, total_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&arrow_array::BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let casted = cast_values(values)?;
        let input = casted[0].as_any().downcast_ref::<Float64Array>().unwrap();
        if FINITE {
            let mut states = self.inner.convert_to_state(&casted, filter)?;
            // The public partial-state protocol retains its zero flag column.
            states.push(Arc::new(UInt64Array::from(vec![0; input.len()])));
            return Ok(states);
        }
        let mut has_nonfinite = false;
        let nonfinite = input
            .iter()
            .enumerate()
            .map(|(row, value)| {
                let is_nonfinite = value.is_some_and(|value| !value.is_finite());
                has_nonfinite |= is_nonfinite;
                u64::from(filter_passes(filter, row) && is_nonfinite)
            })
            .collect::<Vec<_>>();
        let mut states = self
            .inner
            .convert_to_state(&mask_nonfinite(&casted, has_nonfinite), filter)?;
        states.push(Arc::new(UInt64Array::from(nonfinite)));
        Ok(states)
    }

    fn size(&self) -> usize {
        self.inner.size() + self.nonfinite.capacity() * std::mem::size_of::<u64>()
    }
}

impl<const FINITE: bool> NativeStddevGroups<FINITE> {
    fn emit_nonfinite(&mut self, count: usize, emit_to: EmitTo) -> Result<Vec<u64>> {
        if count > self.nonfinite.len() {
            return Err(DataFusionError::Execution(
                "stddev_pop group state is shorter than emitted output".into(),
            ));
        }
        let values = emit_to.take_needed(&mut self.nonfinite);
        if values.len() != count {
            return Err(DataFusionError::Execution(
                "stddev_pop emitted group count does not match native output".into(),
            ));
        }
        Ok(values)
    }
}

pub(crate) fn udf(input: &DataType) -> Result<Arc<AggregateUDF>> {
    if !matches!(
        input,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    ) {
        return Err(DataFusionError::Plan(
            "stddev_pop requires numeric input".into(),
        ));
    }
    Ok(Arc::new(AggregateUDF::from(NativeStddevPop {
        input: input.clone(),
        finite_input: matches!(input, DataType::Int32 | DataType::Int64),
        signature: Signature::exact(vec![input.clone()], datafusion_expr::Volatility::Immutable),
    })))
}
