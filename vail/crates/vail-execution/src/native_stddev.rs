//! DataFusion's native population standard deviation with Hail's empty-result
//! convention and Vail's numeric-input signature.
use arrow_array::{Array, ArrayRef, Float64Array, UInt64Array};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
    function::{AccumulatorArgs, StateFieldsArgs},
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
struct NativeStddevPop {
    input: DataType,
    signature: Signature,
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

fn nonfinite_count(values: &Float64Array) -> Result<u64> {
    u64::try_from(
        values
            .iter()
            .flatten()
            .filter(|value| !value.is_finite())
            .count(),
    )
    .map_err(|_| DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into()))
}

fn mask_nonfinite(values: &[ArrayRef]) -> Vec<ArrayRef> {
    let input = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
    if input.iter().flatten().all(|value| value.is_finite()) {
        return values.to_vec();
    }
    vec![Arc::new(Float64Array::from_iter(
        input
            .iter()
            .map(|value| value.filter(|value| value.is_finite())),
    ))]
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

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(NativeStddevAccumulator {
            inner: native().inner().accumulator(args)?,
            nonfinite_count: 0,
        }))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        native().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(NativeStddevGroups {
            inner: native().create_groups_accumulator(args)?,
            nonfinite: Vec::new(),
        }))
    }
}

#[derive(Debug)]
struct NativeStddevAccumulator {
    inner: Box<dyn Accumulator>,
    nonfinite_count: u64,
}

impl Accumulator for NativeStddevAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = cast_values(values)?;
        let input = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        self.nonfinite_count = self
            .nonfinite_count
            .checked_add(nonfinite_count(input)?)
            .ok_or_else(|| {
                DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
            })?;
        self.inner.update_batch(&mask_nonfinite(&values))
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
        let input = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let nonfinite = nonfinite_count(input)?;
        self.inner.retract_batch(&mask_nonfinite(&values))?;
        self.nonfinite_count = self.nonfinite_count.saturating_sub(nonfinite);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

struct NativeStddevGroups {
    inner: Box<dyn GroupsAccumulator>,
    nonfinite: Vec<u64>,
}

impl GroupsAccumulator for NativeStddevGroups {
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
        for (row, &group) in groups.iter().enumerate() {
            if filter_passes(filter, row) && input.is_valid(row) && !input.value(row).is_finite() {
                let count = self.nonfinite.get_mut(group).ok_or_else(|| {
                    DataFusionError::Execution("stddev_pop group index exceeds state".into())
                })?;
                *count = count.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
                })?;
            }
        }
        self.inner
            .update_batch(&mask_nonfinite(&casted), groups, filter, total_groups)
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
        let mut states = self
            .inner
            .convert_to_state(&mask_nonfinite(&casted), filter)?;
        let input = casted[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let nonfinite = input.iter().enumerate().map(|(row, value)| {
            u64::from(filter_passes(filter, row) && value.is_some_and(|value| !value.is_finite()))
        });
        states.push(Arc::new(UInt64Array::from_iter_values(nonfinite)));
        Ok(states)
    }

    fn size(&self) -> usize {
        self.inner.size() + self.nonfinite.capacity() * std::mem::size_of::<u64>()
    }
}

impl NativeStddevGroups {
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
        signature: Signature::exact(vec![input.clone()], datafusion_expr::Volatility::Immutable),
    })))
}
