//! Hail array_agg requires one common length within each DataFusion group.
use arrow_array::{Array, ArrayRef, Int64Array, ListArray};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF};
use std::sync::Arc;

pub fn length_udaf(dtype: &DataType) -> Arc<AggregateUDF> {
    crate::kernel_identity::aggregate_named(
        "vail_array_agg_length",
        dtype.clone(),
        vec![dtype.clone()],
        DataType::Int64,
        vec![DataType::Int64],
        Arc::new(|_| Ok(Box::new(Length(None)))),
    )
}
#[derive(Debug)]
struct Length(Option<i64>);
impl Length {
    fn add(&mut self, n: i64) -> Result<()> {
        if let Some(old) = self.0 {
            if old != n {
                return Err(DataFusionError::Execution(format!(
                    "array_agg requires equal array lengths, found {old} and {n}"
                )));
            }
        }
        self.0 = Some(n);
        Ok(())
    }
}
impl Accumulator for Length {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("array_agg requires list input".into()))?;
        for row in 0..array.len() {
            if array.is_valid(row) {
                self.add(i64::from(array.value_length(row)))?;
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(self.0))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn merge_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let a = values[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("invalid array length state".into()))?;
        for i in 0..a.len() {
            if a.is_valid(i) {
                self.add(a.value(i))?;
            }
        }
        Ok(())
    }
}
