//! Hail-compatible collect aggregate. Values stay in Arrow arrays throughout
//! accumulation so nested Arrow field metadata is retained and missing values
//! remain ordinary list elements.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, new_empty_array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, Volatility, expr_fn::create_udaf};

#[derive(Debug)]
struct CollectAccumulator {
    item_type: DataType,
    chunks: Vec<ArrayRef>,
    len: usize,
}

impl CollectAccumulator {
    fn new(item_type: DataType) -> Self {
        Self {
            item_type,
            chunks: Vec::new(),
            len: 0,
        }
    }

    fn append(&mut self, values: ArrayRef) -> DfResult<()> {
        self.len = self.len.checked_add(values.len()).ok_or_else(|| {
            DataFusionError::Execution("collect result exceeds Arrow list capacity".into())
        })?;
        if self.len > i32::MAX as usize {
            return Err(DataFusionError::Execution(
                "collect result exceeds Arrow list capacity".into(),
            ));
        }
        if values.len() > 0 {
            self.chunks.push(values);
        }
        Ok(())
    }

    fn values(&self) -> DfResult<ArrayRef> {
        if self.chunks.is_empty() {
            return Ok(new_empty_array(&self.item_type));
        }
        if let [chunk] = self.chunks.as_slice() {
            return Ok(chunk.clone());
        }
        let chunks = self
            .chunks
            .iter()
            .map(|chunk| chunk.as_ref())
            .collect::<Vec<_>>();
        concat(&chunks).map_err(Into::into)
    }

    fn list_scalar(&self) -> DfResult<ScalarValue> {
        let values = self.values()?;
        let field = Arc::new(Field::new("item", self.item_type.clone(), true));
        let list = ListArray::new(field, OffsetBuffer::from_lengths([self.len]), values, None);
        Ok(ScalarValue::List(Arc::new(list)))
    }
}

impl Accumulator for CollectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let Some(values) = values.first() else {
            return Err(DataFusionError::Execution(
                "collect expects exactly one input".into(),
            ));
        };
        if values.data_type() != &self.item_type {
            // Physical expression results may refine nested field nullability
            // (for example, mapping an array to a non-null constant). Preserve
            // the declared logical type and normalize with Arrow's cast kernel.
            if !matches!(
                (crate::expressions::Type::from_arrow(values.data_type()),
                 crate::expressions::Type::from_arrow(&self.item_type)),
                (Ok(actual), Ok(expected)) if actual == expected
            ) {
                return Err(DataFusionError::Execution(format!(
                    "collect expected {:?}, got {:?}",
                    self.item_type,
                    values.data_type()
                )));
            }
            return self.append(arrow_cast::cast(values.as_ref(), &self.item_type)?);
        }
        self.append(values.clone())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.list_scalar()
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.chunks.capacity() * std::mem::size_of::<ArrayRef>()
            + self
                .chunks
                .iter()
                .map(|chunk| chunk.get_array_memory_size())
                .sum::<usize>()
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.list_scalar()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [states] = states else {
            return Err(DataFusionError::Execution(
                "collect state must contain exactly one list".into(),
            ));
        };
        let list = states
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("collect state must be a list".into()))?;
        if list.value_type() != self.item_type {
            return Err(DataFusionError::Execution(format!(
                "collect state expected {:?}, got {:?}",
                self.item_type,
                list.value_type()
            )));
        }
        for row in 0..list.len() {
            if list.is_valid(row) {
                self.append(list.value(row))?;
            }
        }
        Ok(())
    }
}

/// Construct the Hail `collect` UDAF for an Arrow input type.
pub fn collect_udf(input_type: &DataType) -> Arc<AggregateUDF> {
    let item_type = input_type.clone();
    let return_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
    let state_type = return_type.clone();
    let input_types = vec![item_type.clone()];
    let factory: datafusion_expr::AccumulatorFactoryFunction =
        Arc::new(move |_| Ok(Box::new(CollectAccumulator::new(item_type.clone()))));
    Arc::new(create_udaf(
        "collect",
        input_types,
        Arc::new(return_type),
        Volatility::Immutable,
        factory,
        Arc::new(vec![state_type]),
    ))
}
