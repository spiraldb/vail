//! Hail-compatible numeric mean built on DataFusion's native AVG state.
use crate::expressions::ExprError;
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
    function::{AccumulatorArgs, StateFieldsArgs},
};
use std::sync::Arc;

#[derive(Debug)]
struct MeanAccumulator {
    inner: Box<dyn Accumulator>,
}

fn cast_values(values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    let Some((first, rest)) = values.split_first() else {
        return Err(DataFusionError::Execution("mean requires one input".into()));
    };
    let first = if first.data_type() == &DataType::Float64 {
        Arc::clone(first)
    } else {
        arrow_cast::cast(first, &DataType::Float64)?
    };
    Ok(std::iter::once(first).chain(rest.iter().cloned()).collect())
}

impl Accumulator for MeanAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(&cast_values(values)?)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(match self.inner.evaluate()? {
            ScalarValue::Float64(Some(value)) => ScalarValue::Float64(Some(value)),
            ScalarValue::Float64(None) => ScalarValue::Float64(Some(f64::NAN)),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "native AVG returned unexpected mean value {other:?}"
                )));
            }
        })
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(&cast_values(values)?)
    }
    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

#[derive(Debug)]
struct MeanGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
}

impl GroupsAccumulator for MeanGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner.update_batch(
            &cast_values(values)?,
            group_indices,
            filter,
            total_num_groups,
        )
    }
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let result = self.inner.evaluate(emit_to)?;
        let values = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("native AVG groups output must be float64".into())
            })?;
        if values.null_count() == 0 {
            return Ok(result);
        }
        let valid = values
            .nulls()
            .expect("null count is nonzero when building a missing mask")
            .inner()
            .clone();
        let missing = BooleanArray::new(!valid, None);
        let nan = Float64Array::new_scalar(f64::NAN);
        Ok(arrow_select::zip::zip(&missing, &nan, values)?)
    }
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }
    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(states, group_indices, total_num_groups)
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&arrow_array::BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(&cast_values(values)?, filter)
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MeanUdf {
    input: DataType,
    signature: Signature,
}

impl MeanUdf {
    fn native() -> Arc<AggregateUDF> {
        datafusion_functions_aggregate::average::avg_udaf()
    }

    fn native_fields(fields: &[FieldRef]) -> Vec<FieldRef> {
        fields
            .iter()
            .map(|field| {
                Arc::new(Field::new(
                    field.name(),
                    DataType::Float64,
                    field.is_nullable(),
                ))
            })
            .collect()
    }

    fn native_args<'a>(args: &AccumulatorArgs<'a>, fields: &'a [FieldRef]) -> AccumulatorArgs<'a> {
        AccumulatorArgs {
            return_field: Arc::new(Field::new(
                args.return_field.name(),
                DataType::Float64,
                args.return_field.is_nullable(),
            )),
            schema: args.schema,
            ignore_nulls: args.ignore_nulls,
            order_bys: args.order_bys,
            is_reversed: args.is_reversed,
            name: args.name,
            is_distinct: args.is_distinct,
            exprs: args.exprs,
            expr_fields: fields,
        }
    }
}

impl AggregateUDFImpl for MeanUdf {
    fn name(&self) -> &str {
        "mean"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args != [self.input.clone()] {
            return Err(DataFusionError::Plan(
                "mean input signature mismatch".into(),
            ));
        }
        Ok(DataType::Float64)
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let fields = Self::native_fields(args.expr_fields);
        let native_args = Self::native_args(&args, &fields);
        Ok(Box::new(MeanAccumulator {
            inner: Self::native().accumulator(native_args)?,
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let fields = Self::native_fields(args.input_fields);
        Self::native().state_fields(StateFieldsArgs {
            name: args.name,
            input_fields: &fields,
            return_field: Arc::new(Field::new(
                args.return_field.name(),
                DataType::Float64,
                args.return_field.is_nullable(),
            )),
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        })
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        let fields = Self::native_fields(args.expr_fields);
        Self::native().groups_accumulator_supported(Self::native_args(&args, &fields))
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let fields = Self::native_fields(args.expr_fields);
        let native_args = Self::native_args(&args, &fields);
        Ok(Box::new(MeanGroupsAccumulator {
            inner: Self::native().create_groups_accumulator(native_args)?,
        }))
    }
}

pub(crate) fn udf(input: Option<&DataType>) -> std::result::Result<Arc<AggregateUDF>, ExprError> {
    let input = input.ok_or_else(|| ExprError::Invalid("mean requires input".into()))?;
    if !matches!(
        input,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    ) {
        return Err(ExprError::Invalid("mean requires numeric input".into()));
    }
    Ok(Arc::new(AggregateUDF::from(MeanUdf {
        input: input.clone(),
        signature: Signature::exact(vec![input.clone()], Volatility::Immutable),
    })))
}
