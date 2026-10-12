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
        let output = aggregate.return_type(&inputs)?;
        let input_fields: Vec<FieldRef> = inputs
            .iter()
            .enumerate()
            .map(|(i, t)| Arc::new(Field::new(format!("input{i}"), t.clone(), true)))
            .collect();
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






    pub fn state_type(&self) -> DataType {
        DataType::Struct(self.fields.clone())
    }
    fn accumulator(&self) -> Result<Box<dyn Accumulator>> {
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
        let descriptor = self.clone();
        let name = format!("vail_scan_finish_{}_{}", self.aggregate.name(), self.id);
        Arc::new(create_udf(
            &name,
            vec![self.state_type(), self.state_type()],
            self.output.clone(),
            Volatility::Immutable,
            Arc::new(move |args| {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                let [carry, local] = arrays.as_slice() else {
                    return Err(DataFusionError::Execution(
                        "scan finalize requires carry and local states".into(),
                    ));
                };
                let carry = as_struct(carry)?;
                let local = as_struct(local)?;
                let mut values = Vec::with_capacity(carry.len());
                for row in 0..carry.len() {
                    let mut accumulator = descriptor.accumulator()?;
                    for state in [carry, local] {
                        if state.is_valid(row) {
                            accumulator.merge_batch(
                                &state
                                    .columns()
                                    .iter()
                                    .map(|column| column.slice(row, 1))
                                    .collect::<Vec<_>>(),
                            )?;
                        }
                    }
                    values.push(accumulator.evaluate()?);
                }
                let result = if values.is_empty() {
                    arrow_array::new_empty_array(&descriptor.output)
                } else {
                    ScalarValue::iter_to_array(values)?
                };
                Ok(ColumnarValue::Array(result))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    fn state(descriptor: &ScanState, values: Vec<Option<i64>>) -> Result<Vec<ScalarValue>> {
        let mut accumulator = descriptor.accumulator()?;
        accumulator.update_batch(&[Arc::new(Int64Array::from(values))])?;
        accumulator.state()
    }
    #[test]
    fn combines_unequal_missing_partitions_using_original_mean_state() -> Result<()> {
        let descriptor = ScanState::new(
            crate::aggregates::aggregate_udf("mean", Some(&DataType::Int64)).unwrap(),
            vec![DataType::Int64],
        )?;
        let mut accumulator = descriptor.accumulator()?;
        for partition in [vec![Some(2), None, Some(4)], vec![Some(12)]] {
            let arrays = state(&descriptor, partition)?
                .into_iter()
                .map(|v| v.to_array_of_size(1))
                .collect::<Result<Vec<_>>>()?;
            accumulator.merge_batch(&arrays)?;
        }
        assert_eq!(accumulator.evaluate()?, ScalarValue::Float64(Some(6.0)));
        Ok(())
    }
    #[test]
    fn collect_state_preserves_partition_order_and_missing_elements() -> Result<()> {
        let descriptor = ScanState::new(
            crate::collect_aggregate::collect_udf(&DataType::Int64),
            vec![DataType::Int64],
        )?;
        let mut accumulator = descriptor.accumulator()?;
        for partition in [vec![Some(5), None], vec![], vec![Some(2)]] {
            let arrays = state(&descriptor, partition)?
                .into_iter()
                .map(|v| v.to_array_of_size(1))
                .collect::<Result<Vec<_>>>()?;
            accumulator.merge_batch(&arrays)?;
        }
        let result = accumulator.evaluate()?;
        let ScalarValue::List(list) = result else {
            panic!("collect must return list");
        };
        assert_eq!(
            list.value(0).as_ref(),
            &Int64Array::from(vec![Some(5), None, Some(2)])
        );
        Ok(())
    }
}
