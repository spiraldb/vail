//! Parallel scans expose and combine existing DataFusion accumulator states.
//! These adapters do not implement aggregate algorithms.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Datum, Float64Array, Int64Array, Scalar, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDF, ColumnarValue, ScalarUDF, Volatility};
use datafusion_physical_expr::expressions::Literal;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ScanState {
    aggregate: Arc<AggregateUDF>,
    inputs: Vec<DataType>,
    output: DataType,
    fields: Fields,
    factory: Arc<FactoryDescriptor>,
}

#[derive(Debug)]
struct FactoryDescriptor {
    schema: Schema,
    output: FieldRef,
    inputs: Vec<FieldRef>,
    expressions: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>>,
}

// Factory descriptors are immutable derivations of the semantic fields. Keep
// identities exactly as before, without hashing the cached physical literals.
impl PartialEq for ScanState {
    fn eq(&self, other: &Self) -> bool {
        self.aggregate == other.aggregate
            && self.inputs == other.inputs
            && self.output == other.output
            && self.fields == other.fields
    }
}
impl Eq for ScanState {}
impl Hash for ScanState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.aggregate.hash(state);
        self.inputs.hash(state);
        self.output.hash(state);
        self.fields.hash(state);
    }
}
impl ScanState {
    pub fn new(aggregate: Arc<AggregateUDF>, inputs: Vec<DataType>) -> Result<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "scan states require exactly one packed input".into(),
            ));
        }
        let output = aggregate.return_type(&inputs)?;
        let input_fields: Vec<FieldRef> = inputs
            .iter()
            .enumerate()
            .map(|(i, t)| Arc::new(Field::new(format!("input{i}"), t.clone(), true)))
            .collect();
        let coerced = datafusion_expr::type_coercion::functions::fields_with_udf(
            &input_fields,
            aggregate.as_ref(),
        )?;
        if coerced
            .iter()
            .map(|field| field.data_type())
            .ne(inputs.iter())
        {
            return Err(DataFusionError::Plan(
                "scan input must already match the normalized aggregate signature".into(),
            ));
        }
        let fields = aggregate.state_fields(StateFieldsArgs {
            name: "scan",
            input_fields: &input_fields,
            return_field: Arc::new(Field::new("value", output.clone(), true)),
            ordering_fields: &[],
            is_distinct: false,
        })?;
        let factory = Arc::new(FactoryDescriptor {
            schema: Schema::empty(),
            output: Arc::new(Field::new("value", output.clone(), true)),
            inputs: input_fields,
            expressions: inputs
                .iter()
                .map(|t| {
                    Ok(Arc::new(Literal::new(ScalarValue::try_from(t)?))
                        as Arc<dyn datafusion_physical_expr::PhysicalExpr>)
                })
                .collect::<Result<Vec<_>>>()?,
        });
        Ok(Self {
            aggregate,
            inputs,
            output,
            fields: fields.into(),
            factory,
        })
    }

    pub fn output_type(&self) -> DataType {
        self.output.clone()
    }
    pub fn input_type(&self) -> DataType {
        self.inputs[0].clone()
    }
    pub fn fields(&self) -> Fields {
        self.fields.clone()
    }
    pub fn snapshot_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StateAccumulator {
            inner: self.accumulator()?,
            fields: self.fields.clone(),
            merge_input: false,
            input_count: self.inputs.len(),
        }))
    }
    pub fn finish_states(&self, states: &StructArray) -> Result<ArrayRef> {
        if states.fields() != &self.fields {
            return Err(DataFusionError::Execution(
                "scan state schema mismatch".into(),
            ));
        }
        if let Some(initial) = crate::fold_aggregate::state_initializer(&self.aggregate) {
            if self.fields.len() != 1 || self.fields[0].data_type() != &self.output {
                return Err(DataFusionError::Internal(
                    "fold finalization requires one result-typed state field".into(),
                ));
            }
            let values = states.column(0);
            if states.null_count() == 0 {
                // FoldAccumulator::state is exactly [evaluate()]. No replay,
                // accumulator construction or expression evaluation is needed.
                return Ok(values.clone());
            }
            // Null outer states denote an empty frame. Its result is the fold
            // initializer, which may itself be a defined, non-identity value.
            let initial =
                crate::logical_expr::restore_arrow_array_type(&initial.to_array()?, &self.output)?;
            let present = BooleanArray::new(states.nulls().unwrap().inner().clone(), None);
            return arrow_select::zip::zip(&present, values, &Scalar::new(initial));
        }
        let mut values = Vec::with_capacity(states.len());
        for row in 0..states.len() {
            let mut accumulator = self.accumulator()?;
            if states.is_valid(row) {
                accumulator.merge_batch(
                    &states
                        .columns()
                        .iter()
                        .map(|c| c.slice(row, 1))
                        .collect::<Vec<_>>(),
                )?;
            }
            values.push(normalize_empty_collect(self, accumulator.evaluate()?));
        }
        if values.is_empty() {
            Ok(arrow_array::new_empty_array(&self.output))
        } else {
            ScalarValue::iter_to_array(values)
        }
    }
    pub fn state_type(&self) -> DataType {
        DataType::Struct(self.fields.clone())
    }
    pub fn accumulator(&self) -> Result<Box<dyn Accumulator>> {
        self.aggregate.accumulator(AccumulatorArgs {
            return_field: self.factory.output.clone(),
            schema: &self.factory.schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "scan",
            is_distinct: false,
            exprs: &self.factory.expressions,
            expr_fields: &self.factory.inputs,
        })
    }
    pub fn state_udaf(&self) -> Arc<AggregateUDF> {
        self.adapter(false)
    }
    pub fn merge_udaf(&self) -> Arc<AggregateUDF> {
        self.adapter(true)
    }
    fn adapter(&self, merge_input: bool) -> Arc<AggregateUDF> {
        let descriptor = self.clone();
        let input = if merge_input {
            vec![self.state_type()]
        } else {
            self.inputs.clone()
        };
        crate::kernel_identity::aggregate(
            if merge_input {
                "vail_scan_merge"
            } else {
                "vail_scan_state"
            },
            (self.clone(), merge_input),
            input,
            self.state_type(),
            self.fields.iter().map(|f| f.data_type().clone()).collect(),
            Arc::new(move |_| {
                Ok(Box::new(StateAccumulator {
                    inner: descriptor.accumulator()?,
                    fields: descriptor.fields.clone(),
                    merge_input,
                    input_count: descriptor.inputs.len(),
                }))
            }),
        )
    }
    pub fn finish_udf(&self) -> Arc<ScalarUDF> {
        self.finisher(false)
    }
    /// Keep inclusive summary validation in the plan while returning the exclusive prefix.
    pub fn validated_finish_udf(&self) -> Arc<ScalarUDF> {
        self.finisher(true)
    }
    fn finisher(&self, validation: bool) -> Arc<ScalarUDF> {
        let descriptor = self.clone();
        let sum_fast_path =
            !validation && self.aggregate.name() == "sum" && is_hail_sum_state(self);
        Arc::new(crate::kernel_identity::scalar(
            "vail_scan_finish",
            (self.clone(), validation),
            datafusion_expr::Signature::exact(
                vec![self.state_type(); if validation { 3 } else { 2 }],
                Volatility::Immutable,
            ),
            self.output.clone(),
            Arc::new(move |args| {
                if args.len() != if validation { 3 } else { 2 } {
                    return Err(DataFusionError::Execution(
                        "scan finalize argument count mismatch".into(),
                    ));
                }
                let rows = args
                    .iter()
                    .filter_map(|value| match value {
                        ColumnarValue::Array(array) => Some(array.len()),
                        _ => None,
                    })
                    .next()
                    .unwrap_or(1);
                let carry = StateRows::new(&args[0], rows, &descriptor.fields)?;
                let local = StateRows::new(&args[1], rows, &descriptor.fields)?;
                if sum_fast_path {
                    let left = sum_state_values(&carry.array, &descriptor.output)?;
                    let right = sum_state_values(&local.array, &descriptor.output)?;
                    let left_scalar = carry.broadcast.then(|| Scalar::new(left.clone()));
                    let right_scalar = local.broadcast.then(|| Scalar::new(right.clone()));
                    let left: &dyn Datum = if carry.broadcast {
                        left_scalar.as_ref().unwrap()
                    } else {
                        &left
                    };
                    let right: &dyn Datum = if local.broadcast {
                        right_scalar.as_ref().unwrap()
                    } else {
                        &right
                    };
                    let result = if descriptor.output == DataType::Int64 {
                        arrow_arith::numeric::add_wrapping(left, right)?
                    } else {
                        arrow_arith::numeric::add(left, right)?
                    };
                    return Ok(ColumnarValue::Array(result));
                }
                Ok(ColumnarValue::Array(finish_generic_states(
                    &descriptor,
                    &carry,
                    &local,
                    rows,
                )?))
            }),
        ))
    }
}
fn is_hail_sum_state(scan: &ScanState) -> bool {
    matches!(
        scan.inputs.as_slice(),
        [DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64]
    ) && ((scan.fields.len() == 2
        && scan.fields[0].data_type() == &DataType::Int64
        && scan.fields[1].data_type() == &scan.output)
        || (scan.fields.len() == 1 && scan.fields[0].data_type() == &scan.output))
        && matches!(scan.output, DataType::Int64 | DataType::Float64)
}

fn sum_state_values(state: &StructArray, output: &DataType) -> Result<ArrayRef> {
    // Older Hail SUM states store [count, sum]; native DataFusion SUM stores
    // only its mergeable sum value. The fast carry path supports both shapes.
    let values = state
        .columns()
        .last()
        .ok_or_else(|| DataFusionError::Execution("sum scan state has no value column".into()))?;
    if state.null_count() == 0 && values.null_count() == 0 {
        return Ok(values.clone());
    }

    let Some(validity) = NullBuffer::union(state.nulls(), values.nulls()) else {
        return Ok(values.clone());
    };
    let valid = BooleanArray::new(validity.inner().clone(), None);
    match output {
        DataType::Int64 => Ok(arrow_select::zip::zip(
            &valid,
            values,
            &Int64Array::new_scalar(0),
        )?),
        DataType::Float64 => Ok(arrow_select::zip::zip(
            &valid,
            values,
            &Float64Array::new_scalar(0.0),
        )?),
        other => Err(DataFusionError::Execution(format!(
            "unsupported sum scan output type {other:?}"
        ))),
    }
}
fn as_struct(array: &ArrayRef) -> Result<&StructArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| DataFusionError::Execution("scan state must be an Arrow struct".into()))
}

// Scalar carry snapshots remain one row; each output borrows that same snapshot.
struct StateRows {
    array: Arc<StructArray>,
    broadcast: bool,
}
impl StateRows {
    fn new(value: &ColumnarValue, rows: usize, fields: &Fields) -> Result<Self> {
        let (array, broadcast) = match value {
            ColumnarValue::Scalar(ScalarValue::Struct(array)) => (array.clone(), true),
            ColumnarValue::Scalar(value) => {
                let array = value.to_array_of_size(1)?;
                (Arc::new(as_struct(&array)?.clone()), true)
            }
            ColumnarValue::Array(array) => (Arc::new(as_struct(array)?.clone()), false),
        };
        if array.fields() != fields || array.len() != if broadcast { 1 } else { rows } {
            return Err(DataFusionError::Execution(
                "scan state schema or row count mismatch".into(),
            ));
        }
        Ok(Self { array, broadcast })
    }
    fn merge_row(&self, accumulator: &mut dyn Accumulator, row: usize) -> Result<()> {
        let row = if self.broadcast { 0 } else { row };
        if self.array.is_valid(row) {
            let columns = self
                .array
                .columns()
                .iter()
                .map(|column| column.slice(row, 1))
                .collect::<Vec<_>>();
            accumulator.merge_batch(&columns)?;
        }
        Ok(())
    }
}
fn finish_generic_states(
    descriptor: &ScanState,
    carry: &StateRows,
    local: &StateRows,
    rows: usize,
) -> Result<ArrayRef> {
    let mut values = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut accumulator = descriptor.accumulator()?;
        carry.merge_row(accumulator.as_mut(), row)?;
        local.merge_row(accumulator.as_mut(), row)?;
        values.push(normalize_empty_collect(descriptor, accumulator.evaluate()?));
    }
    if values.is_empty() {
        Ok(arrow_array::new_empty_array(&descriptor.output))
    } else {
        ScalarValue::iter_to_array(values)
    }
}

pub(crate) fn normalize_empty_collect(descriptor: &ScanState, value: ScalarValue) -> ScalarValue {
    if descriptor.aggregate.name() == "array_agg" && value.is_null() {
        if let DataType::List(field) = &descriptor.output {
            return ScalarValue::List(ScalarValue::new_list(&[], field.data_type(), true));
        }
    }
    value
}

#[derive(Debug)]
struct StateAccumulator {
    inner: Box<dyn Accumulator>,
    fields: Fields,
    merge_input: bool,
    input_count: usize,
}
impl Accumulator for StateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.merge_input {
            let arguments = values.get(..self.input_count).ok_or_else(|| {
                DataFusionError::Execution("scan state is missing aggregate arguments".into())
            })?;

            return self.inner.update_batch(arguments);
        }
        let [state] = values else {
            return Err(DataFusionError::Execution(
                "scan merge expects one state struct".into(),
            ));
        };
        let state = as_struct(state)?;
        // Null outer states represent empty window frames, not null state fields.
        if state.null_count() == 0 {
            return self.inner.merge_batch(state.columns());
        }
        for row in 0..state.len() {
            if state.is_valid(row) {
                self.inner.merge_batch(
                    &state
                        .columns()
                        .iter()
                        .map(|c| c.slice(row, 1))
                        .collect::<Vec<_>>(),
                )?;
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let columns = self
            .inner
            .state()?
            .into_iter()
            .map(|value| value.to_array_of_size(1))
            .collect::<Result<Vec<_>>>()?;
        Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
            self.fields.clone(),
            columns,
            None,
        )?)))
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.inner.size()
    }
}
