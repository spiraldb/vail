//! Parallel scans expose and combine existing DataFusion accumulator states.
//! These adapters do not implement aggregate algorithms.
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    Accumulator, AggregateUDF, ColumnarValue, ScalarUDF, Volatility,
    expr_fn::{create_udaf, create_udf},
};
use datafusion_physical_expr::expressions::Literal;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
static NEXT_SCAN_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct ScanState {
    id: u64,
    aggregate: Arc<AggregateUDF>,
    inputs: Vec<DataType>,
    output: DataType,
    fields: Fields,
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
        Ok(Self {
            id: NEXT_SCAN_ID.fetch_add(1, Ordering::Relaxed),
            aggregate,
            inputs,
            output,
            fields: fields.into(),
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
            values.push(accumulator.evaluate()?);
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
        let schema = Schema::empty();
        let expr_fields: Vec<FieldRef> = self
            .inputs
            .iter()
            .enumerate()
            .map(|(i, t)| Arc::new(Field::new(format!("input{i}"), t.clone(), true)))
            .collect();
        let exprs = self
            .inputs
            .iter()
            .map(|t| {
                Ok(Arc::new(Literal::new(ScalarValue::try_from(t)?))
                    as Arc<dyn datafusion_physical_expr::PhysicalExpr>)
            })
            .collect::<Result<Vec<_>>>()?;
        self.aggregate.accumulator(AccumulatorArgs {
            return_field: Arc::new(Field::new("value", self.output.clone(), true)),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "scan",
            is_distinct: false,
            exprs: &exprs,
            expr_fields: &expr_fields,
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
        let name = format!(
            "vail_scan_{}_{}_{}",
            if merge_input { "merge" } else { "state" },
            self.aggregate.name(),
            self.id
        );
        Arc::new(create_udaf(
            &name,
            input,
            Arc::new(self.state_type()),
            Volatility::Immutable,
            Arc::new(move |_| {
                Ok(Box::new(StateAccumulator {
                    inner: descriptor.accumulator()?,
                    fields: descriptor.fields.clone(),
                    merge_input,
                    input_count: descriptor.inputs.len(),
                }))
            }),
            Arc::new(self.fields.iter().map(|f| f.data_type().clone()).collect()),
        ))
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
        let name = format!(
            "vail_scan_finish_{}_{}_{}",
            self.aggregate.name(),
            self.id,
            validation
        );
        Arc::new(create_udf(
            &name,
            vec![self.state_type(); if validation { 3 } else { 2 }],
            self.output.clone(),
            Volatility::Immutable,
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
        values.push(accumulator.evaluate()?);
    }
    if values.is_empty() {
        Ok(arrow_array::new_empty_array(&descriptor.output))
    } else {
        ScalarValue::iter_to_array(values)
    }
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
