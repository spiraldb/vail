//! Centered, power-of-two-scaled mergeable mean and population variance.
use crate::expressions::ExprError;
use arrow_array::{Array, ArrayRef, Float64Array, Int32Array, UInt64Array};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
    function::{AccumulatorArgs, StateFieldsArgs},
};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Mean,
    Stddev,
}
#[derive(Debug)]
struct Moments {
    kind: Kind,
    finite: u64,
    defined: u64,
    origin: f64,
    scale: f64,
    mean: f64,
    m2: f64,
    nonfinite: i32,
}
impl Moments {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            finite: 0,
            defined: 0,
            origin: 0.,
            scale: 0.,
            mean: 0.,
            m2: 0.,
            nonfinite: 0,
        }
    }
    fn power_scale(value: f64) -> f64 {
        if value == 0. {
            return 0.;
        }
        let bits = value.abs().to_bits();
        let exponent = (bits >> 52) & 0x7ff;
        if exponent != 0 {
            f64::from_bits(exponent << 52)
        } else {
            f64::from_bits(1u64 << (63 - bits.leading_zeros()))
        }
    }
    fn transform(
        origin: f64,
        scale: f64,
        mean: f64,
        m2: f64,
        target_origin: f64,
        target_scale: f64,
    ) -> (f64, f64) {
        if target_scale == 0. {
            return (0., 0.);
        }
        let ratio = scale / target_scale;
        // Scale before subtraction, including opposite finite extrema.
        let shift = origin / target_scale - target_origin / target_scale;
        (ratio.mul_add(mean, shift), m2 * ratio * ratio)
    }
    fn frame(&mut self, other_origin: f64, other_scale: f64) {
        let difference = other_origin - self.origin;
        let (origin, scale) = if difference.is_finite() {
            (
                self.origin,
                self.scale
                    .max(other_scale)
                    .max(Self::power_scale(difference)),
            )
        } else {
            (
                0.,
                self.scale
                    .max(other_scale)
                    .max(Self::power_scale(self.origin.abs().max(other_origin.abs()))),
            )
        };
        if origin != self.origin || scale != self.scale {
            (self.mean, self.m2) =
                Self::transform(self.origin, self.scale, self.mean, self.m2, origin, scale);
            self.origin = origin;
            self.scale = scale;
        }
    }
    fn observe<const VARIANCE: bool>(&mut self, value: f64) -> Result<()> {
        self.defined = self
            .defined
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("moment count exceeds uint64".into()))?;
        if !value.is_finite() {
            self.nonfinite |= if value.is_nan() {
                1
            } else if value.is_sign_positive() {
                2
            } else {
                4
            };
            return Ok(());
        }
        if self.finite == 0 {
            self.origin = value;
            self.finite = 1;
            return Ok(());
        }
        self.frame(value, 0.);
        let normalized = if self.scale == 0. {
            0.
        } else {
            // For a finite difference, subtracting first preserves ULP-sized
            // deviations around a large common origin.
            let difference = value - self.origin;
            if difference.is_finite() {
                difference / self.scale
            } else {
                value / self.scale - self.origin / self.scale
            }
        };
        let next = self
            .finite
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("moment count exceeds uint64".into()))?;
        let delta = normalized - self.mean;
        self.mean += delta / next as f64;
        if VARIANCE {
            self.m2 += delta * (normalized - self.mean);
        }
        self.finite = next;
        Ok(())
    }
    fn add_partial(
        &mut self,
        finite: u64,
        defined: u64,
        origin: f64,
        scale: f64,
        mean: f64,
        m2: f64,
        nonfinite: i32,
    ) -> Result<()> {
        self.defined = self
            .defined
            .checked_add(defined)
            .ok_or_else(|| DataFusionError::Execution("moment count exceeds uint64".into()))?;
        self.nonfinite |= nonfinite;
        if finite == 0 {
            return Ok(());
        }
        if self.finite == 0 {
            self.finite = finite;
            self.origin = origin;
            self.scale = scale;
            self.mean = mean;
            self.m2 = m2;
            return Ok(());
        }
        self.frame(origin, scale);
        let (mean, m2) = Self::transform(origin, scale, mean, m2, self.origin, self.scale);
        let next = self
            .finite
            .checked_add(finite)
            .ok_or_else(|| DataFusionError::Execution("moment count exceeds uint64".into()))?;
        let delta = mean - self.mean;
        let ratio = finite as f64 / next as f64;
        self.mean += delta * ratio;
        self.m2 += m2 + delta * delta * (self.finite as f64 * ratio);
        self.finite = next;
        Ok(())
    }
}
impl Accumulator for Moments {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "moments require one input".into(),
            ));
        };
        let input = arrow_cast::cast(input, &DataType::Float64)?;
        let values = input.as_any().downcast_ref::<Float64Array>().unwrap();
        match self.kind {
            Kind::Mean => {
                for value in values.iter().flatten() {
                    self.observe::<false>(value)?;
                }
            }
            Kind::Stddev => {
                for value in values.iter().flatten() {
                    self.observe::<true>(value)?;
                }
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = match self.kind {
            Kind::Mean if self.nonfinite & 1 != 0 || self.nonfinite & 6 == 6 => f64::NAN,
            Kind::Mean if self.nonfinite & 2 != 0 => f64::INFINITY,
            Kind::Mean if self.nonfinite & 4 != 0 => f64::NEG_INFINITY,
            _ if self.defined == 0 => f64::NAN,
            Kind::Mean => self.scale.mul_add(self.mean, self.origin),
            Kind::Stddev if self.nonfinite != 0 => f64::NAN,
            Kind::Stddev => self.scale * (self.m2 / self.finite as f64).sqrt(),
        };
        Ok(ScalarValue::Float64(Some(value)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.finite)),
            ScalarValue::UInt64(Some(self.defined)),
            ScalarValue::Float64(Some(self.origin)),
            ScalarValue::Float64(Some(self.scale)),
            ScalarValue::Float64(Some(self.mean)),
            ScalarValue::Float64(Some(self.m2)),
            ScalarValue::Int32(Some(self.nonfinite)),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let batch = MomentBatch::new(states)?;
        for row in 0..batch.len() {
            let state = batch.get(row)?;
            self.add_partial(
                state.finite,
                state.defined,
                state.origin,
                state.scale,
                state.mean,
                state.m2,
                state.flags,
            )?;
        }
        Ok(())
    }
}
#[derive(Clone, Copy)]
struct Partial {
    finite: u64,
    defined: u64,
    origin: f64,
    scale: f64,
    mean: f64,
    m2: f64,
    flags: i32,
}
struct MomentBatch<'a> {
    finite: &'a UInt64Array,
    defined: &'a UInt64Array,
    origin: &'a Float64Array,
    scale: &'a Float64Array,
    mean: &'a Float64Array,
    m2: &'a Float64Array,
    flags: &'a Int32Array,
}
impl<'a> MomentBatch<'a> {
    fn new(states: &'a [ArrayRef]) -> Result<Self> {
        let [finite, defined, origin, scale, mean, m2, flags] = states else {
            return Err(DataFusionError::Execution(
                "moment state arity mismatch".into(),
            ));
        };
        fn counts(array: &ArrayRef) -> Result<&UInt64Array> {
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| DataFusionError::Execution("moment counts must be uint64".into()))
        }
        fn floating(array: &ArrayRef) -> Result<&Float64Array> {
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("moment values must be float64".into()))
        }
        let result = Self {
            finite: counts(finite)?,
            defined: counts(defined)?,
            origin: floating(origin)?,
            scale: floating(scale)?,
            mean: floating(mean)?,
            m2: floating(m2)?,
            flags: flags
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Execution("moment flags must be int32".into()))?,
        };
        if states
            .iter()
            .any(|array| array.len() != result.len() || array.null_count() != 0)
        {
            return Err(DataFusionError::Execution(
                "moment states must be defined with matching rows".into(),
            ));
        }
        Ok(result)
    }
    fn len(&self) -> usize {
        self.finite.len()
    }
    fn get(&self, row: usize) -> Result<Partial> {
        let result = Partial {
            finite: self.finite.value(row),
            defined: self.defined.value(row),
            origin: self.origin.value(row),
            scale: self.scale.value(row),
            mean: self.mean.value(row),
            m2: self.m2.value(row),
            flags: self.flags.value(row),
        };
        if result.finite > result.defined
            || !result.origin.is_finite()
            || !result.scale.is_finite()
            || result.scale < 0.
            || !result.mean.is_finite()
            || !result.m2.is_finite()
            || result.m2 < 0.
            || result.flags & !7 != 0
        {
            return Err(DataFusionError::Execution(
                "invalid normalized moment state".into(),
            ));
        }
        Ok(result)
    }
}
#[derive(Debug)]
struct MomentGroups {
    kind: Kind,
    groups: Vec<Moments>,
}
impl MomentGroups {
    fn ensure(&mut self, total: usize) {
        if total > self.groups.len() {
            self.groups.resize_with(total, || Moments::new(self.kind));
        }
    }
    fn emit(&mut self, to: EmitTo) -> Result<Vec<Moments>> {
        match to {
            EmitTo::All => Ok(std::mem::take(&mut self.groups)),
            EmitTo::First(count) => {
                if count > self.groups.len() {
                    return Err(DataFusionError::Execution(
                        "moment emitted groups exceed state".into(),
                    ));
                }
                Ok(self.groups.drain(..count).collect())
            }
        }
    }
    fn update<const VARIANCE: bool>(
        &mut self,
        values: &Float64Array,
        indices: &[usize],
        filter: Option<&arrow_array::BooleanArray>,
    ) -> Result<()> {
        match filter {
            None => {
                for (row, value) in values.iter().enumerate() {
                    if let Some(value) = value {
                        self.groups
                            .get_mut(indices[row])
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "moment group index exceeds state".into(),
                                )
                            })?
                            .observe::<VARIANCE>(value)?;
                    }
                }
            }
            Some(filter) => {
                for (row, value) in values.iter().enumerate() {
                    if filter.is_valid(row) && filter.value(row) {
                        if let Some(value) = value {
                            self.groups
                                .get_mut(indices[row])
                                .ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "moment group index exceeds state".into(),
                                    )
                                })?
                                .observe::<VARIANCE>(value)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
impl GroupsAccumulator for MomentGroups {
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&arrow_array::BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let rows = values.first().map_or(0, |array| array.len());
        let indices: Vec<usize> = (0..rows).collect();
        let mut groups = Self {
            kind: self.kind,
            groups: Vec::new(),
        };
        groups.update_batch(values, &indices, filter, rows)?;
        groups.state(EmitTo::All)
    }

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        indices: &[usize],
        filter: Option<&arrow_array::BooleanArray>,
        total: usize,
    ) -> Result<()> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "moments require one input".into(),
            ));
        };
        if input.len() != indices.len()
            || filter.is_some_and(|filter| filter.len() != indices.len())
        {
            return Err(DataFusionError::Execution(
                "moment grouping row counts differ".into(),
            ));
        }
        self.ensure(total);
        let input = arrow_cast::cast(input, &DataType::Float64)?;
        let values = input.as_any().downcast_ref::<Float64Array>().unwrap();
        match self.kind {
            Kind::Mean => self.update::<false>(values, indices, filter),
            Kind::Stddev => self.update::<true>(values, indices, filter),
        }
    }
    fn evaluate(&mut self, to: EmitTo) -> Result<ArrayRef> {
        let groups = self.emit(to)?;
        let values = groups
            .into_iter()
            .map(|mut group| group.evaluate())
            .collect::<Result<Vec<_>>>()?;
        if values.is_empty() {
            Ok(arrow_array::new_empty_array(&DataType::Float64))
        } else {
            ScalarValue::iter_to_array(values)
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.groups.capacity() * std::mem::size_of::<Moments>()
    }
    fn state(&mut self, to: EmitTo) -> Result<Vec<ArrayRef>> {
        let groups = self.emit(to)?;
        Ok(vec![
            Arc::new(UInt64Array::from_iter_values(
                groups.iter().map(|group| group.finite),
            )),
            Arc::new(UInt64Array::from_iter_values(
                groups.iter().map(|group| group.defined),
            )),
            Arc::new(Float64Array::from_iter_values(
                groups.iter().map(|group| group.origin),
            )),
            Arc::new(Float64Array::from_iter_values(
                groups.iter().map(|group| group.scale),
            )),
            Arc::new(Float64Array::from_iter_values(
                groups.iter().map(|group| group.mean),
            )),
            Arc::new(Float64Array::from_iter_values(
                groups.iter().map(|group| group.m2),
            )),
            Arc::new(Int32Array::from_iter_values(
                groups.iter().map(|group| group.nonfinite),
            )),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef], indices: &[usize], total: usize) -> Result<()> {
        let batch = MomentBatch::new(states)?;
        if batch.len() != indices.len() {
            return Err(DataFusionError::Execution(
                "moment grouping state rows differ".into(),
            ));
        }
        self.ensure(total);
        for (row, index) in indices.iter().enumerate() {
            let state = batch.get(row)?;
            self.groups
                .get_mut(*index)
                .ok_or_else(|| {
                    DataFusionError::Execution("moment group index exceeds state".into())
                })?
                .add_partial(
                    state.finite,
                    state.defined,
                    state.origin,
                    state.scale,
                    state.mean,
                    state.m2,
                    state.flags,
                )?;
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct MomentUdf {
    kind: Kind,
    input: DataType,
    signature: Signature,
}
impl AggregateUDFImpl for MomentUdf {
    fn name(&self) -> &str {
        match self.kind {
            Kind::Mean => "mean",
            Kind::Stddev => "stddev_pop",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args != [self.input.clone()] {
            return Err(DataFusionError::Plan(
                "moment input signature mismatch".into(),
            ));
        }
        Ok(DataType::Float64)
    }
    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(Moments::new(self.kind)))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok([
            ("finite", DataType::UInt64),
            ("defined", DataType::UInt64),
            ("origin", DataType::Float64),
            ("scale", DataType::Float64),
            ("mean", DataType::Float64),
            ("m2", DataType::Float64),
            ("flags", DataType::Int32),
        ]
        .into_iter()
        .map(|(name, ty)| Arc::new(Field::new(format!("{}[{name}]", args.name), ty, false)))
        .collect())
    }
    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        self.kind == Kind::Mean
    }
    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(MomentGroups {
            kind: self.kind,
            groups: Vec::new(),
        }))
    }
}

pub(crate) fn udf(
    name: &str,
    input: Option<&DataType>,
) -> std::result::Result<Arc<AggregateUDF>, ExprError> {
    let input = input.ok_or_else(|| ExprError::Invalid(format!("{name} requires input")))?;
    if !matches!(
        input,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    ) {
        return Err(ExprError::Invalid(format!("{name} requires numeric input")));
    }
    let kind = if name == "mean" {
        Kind::Mean
    } else {
        Kind::Stddev
    };
    Ok(Arc::new(AggregateUDF::from(MomentUdf {
        kind,
        input: input.clone(),
        signature: Signature::exact(vec![input.clone()], Volatility::Immutable),
    })))
}
