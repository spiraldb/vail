//! Hail-compatible collect aggregate. Values stay in Arrow arrays throughout
//! accumulation so nested Arrow field metadata is retained and missing values
//! remain ordinary list elements.

use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_array::{Array, ArrayRef, ListArray, new_empty_array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion_functions_aggregate::array_agg::array_agg_udaf;

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
    Arc::new(AggregateUDF::from(CollectUdf {
        item_type: input_type.clone(),
        signature: Signature::exact(vec![input_type.clone()], Volatility::Immutable),
    }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CollectUdf {
    item_type: DataType,
    signature: Signature,
}

impl AggregateUDFImpl for CollectUdf {
    fn name(&self) -> &str {
        "collect"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> DfResult<DataType> {
        if args != [self.item_type.clone()] {
            return Err(DataFusionError::Plan(
                "collect input signature mismatch".into(),
            ));
        }
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            self.item_type.clone(),
            true,
        ))))
    }
    fn accumulator(&self, _args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        Ok(Box::new(CollectAccumulator::new(self.item_type.clone())))
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        args.expr_fields.len() == 1
            && args.expr_fields[0].data_type() == &self.item_type
            && !args.is_distinct
            && args.order_bys.is_empty()
            && !args.ignore_nulls
            && array_agg_udaf().groups_accumulator_supported(args)
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DfResult<Box<dyn GroupsAccumulator>> {
        if !self.groups_accumulator_supported(args.clone()) {
            return Err(DataFusionError::Plan(
                "collect groups accumulator is not supported for these arguments".into(),
            ));
        }
        Ok(Box::new(CollectGroupsAccumulator {
            inner: array_agg_udaf().create_groups_accumulator(args)?,
            counts: Vec::new(),
            total_count: 0,
        }))
    }
}

/// Reuses DataFusion's batch-oriented array_agg storage and interleave kernel.
/// Counts are tracked separately to keep collect's explicit List capacity error.
struct CollectGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    counts: Vec<usize>,
    total_count: usize,
}

impl CollectGroupsAccumulator {
    fn ensure_groups(&mut self, n: usize) {
        self.counts.resize(self.counts.len().max(n), 0);
    }
    fn add_count(&mut self, group: usize, n: usize) -> DfResult<()> {
        let count = self.counts.get_mut(group).ok_or_else(|| {
            DataFusionError::Execution("collect group index exceeds state".into())
        })?;
        let next = count.checked_add(n).ok_or_else(|| {
            DataFusionError::Execution("collect result exceeds Arrow list capacity".into())
        })?;
        if next > i32::MAX as usize {
            return Err(DataFusionError::Execution(
                "collect result exceeds Arrow list capacity".into(),
            ));
        }
        let total = self.total_count.checked_add(n).ok_or_else(|| {
            DataFusionError::Execution("collect result exceeds Arrow list capacity".into())
        })?;
        if total > i32::MAX as usize {
            return Err(DataFusionError::Execution(
                "collect result exceeds Arrow list capacity".into(),
            ));
        }
        *count = next;
        self.total_count = total;
        Ok(())
    }
    fn emitted(&mut self, to: EmitTo) {
        match to {
            EmitTo::All => {
                self.counts = Vec::new();
                self.total_count = 0;
            }
            EmitTo::First(n) => {
                let emitted: usize = self.counts.iter().take(n).sum();
                self.counts.drain(..n);
                self.total_count -= emitted;
            }
        }
    }
    fn normalize_empty_lists(array: ArrayRef) -> DfResult<ArrayRef> {
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Execution("collect groups state must be a list".into())
        })?;
        if list.null_count() == 0 {
            return Ok(array);
        }
        Ok(Arc::new(ListArray::new(
            Arc::new(Field::new("item", list.value_type(), true)),
            list.offsets().clone(),
            Arc::clone(list.values()),
            None,
        )))
    }
}

impl GroupsAccumulator for CollectGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> DfResult<()> {
        self.ensure_groups(total);
        for (row, &group) in group_indices.iter().enumerate() {
            if filter.is_none_or(|f| f.is_valid(row) && f.value(row)) {
                self.add_count(group, 1)?;
            }
        }
        self.inner
            .update_batch(values, group_indices, filter, total)
    }
    fn evaluate(&mut self, to: EmitTo) -> DfResult<ArrayRef> {
        let result = self.inner.evaluate(to)?;
        self.emitted(to);
        Self::normalize_empty_lists(result)
    }
    fn state(&mut self, to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        let mut state = self.inner.state(to)?;
        self.emitted(to);
        if state.len() == 1 {
            let list = state.pop().unwrap();
            state.push(Self::normalize_empty_lists(list)?);
        }
        Ok(state)
    }
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total: usize,
    ) -> DfResult<()> {
        self.ensure_groups(total);
        let [states] = values else {
            return Err(DataFusionError::Execution(
                "collect state must contain exactly one list".into(),
            ));
        };
        let list = states
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("collect state must be a list".into()))?;
        for (row, &group) in group_indices.iter().enumerate() {
            if list.is_valid(row) {
                self.add_count(
                    group,
                    (list.value_offsets()[row + 1] - list.value_offsets()[row]) as usize,
                )?;
            }
        }
        self.inner.merge_batch(values, group_indices, total)
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> DfResult<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, filter)
    }
    fn size(&self) -> usize {
        self.inner.size() + self.counts.capacity() * std::mem::size_of::<usize>()
    }
}
