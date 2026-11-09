//! Stream a single list's children without gathering an already-flat buffer.
use crate::plan::PhysicalPlan;
use arrow_array::{Array, ArrayRef, Int32Array, LargeListArray, ListArray, RecordBatch, RunArray, UInt64Array, new_null_array, types::Int32Type};
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_select::take::take;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, stream::RecordBatchStreamAdapter, unnest::UnnestExec};
use futures::TryStreamExt;
use async_stream::try_stream;
use std::{fmt, sync::Arc};

#[derive(Debug)]
pub(crate) struct ListChildUnnestExec {
    original: PhysicalPlan,
}

pub(crate) fn original(plan: &PhysicalPlan) -> Option<&UnnestExec> {
    if let Some(unnest) = plan.downcast_ref::<ListChildUnnestExec>() {
        unnest.original.downcast_ref::<UnnestExec>()
    } else {
        plan.downcast_ref::<UnnestExec>()
    }
}

pub(crate) fn replace(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    let Some(unnest) = plan.downcast_ref::<UnnestExec>() else { return Ok(plan); };
    let lists = unnest.list_column_indices();
    if lists.len() != 1 || lists[0].depth != 1 || !unnest.struct_column_indices().is_empty() {
        return Ok(plan);
    }
    if !matches!(unnest.input().schema().field(lists[0].index_in_input_schema).data_type(), DataType::List(_) | DataType::LargeList(_)) {
        return Ok(plan);
    }
    Ok(Arc::new(ListChildUnnestExec { original: plan }))
}

impl DisplayAs for ListChildUnnestExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ListChildUnnestExec")
    }
}

impl ExecutionPlan for ListChildUnnestExec {
    fn name(&self) -> &'static str { "ListChildUnnestExec" }
    fn properties(&self) -> &Arc<PlanProperties> { self.original.properties() }
    fn children(&self) -> Vec<&PhysicalPlan> { self.original.children() }
    fn required_input_distribution(&self) -> Vec<Distribution> { self.original.required_input_distribution() }
    fn maintains_input_order(&self) -> Vec<bool> { self.original.maintains_input_order() }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        Ok(Arc::new(Self { original: self.original.clone().replace_children(children, ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute))? }))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let unnest = original(&self.original).expect("list unnest template");
        let column = unnest.list_column_indices()[0].index_in_input_schema;
        let preserve_nulls = unnest.options().preserve_nulls();
        let expand_empty = unnest.options().expand_empty_as_null();
        let batch_size = context.session_config().batch_size().max(1).min(i32::MAX as usize);
        let schema = self.schema();
        let mut input = unnest.input().execute(partition, context)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema.clone(), try_stream! {
            while let Some(batch) = input.try_next().await? {
                let list = batch.column(column);
                let (values, offsets): (&ArrayRef, Vec<usize>) = if let Some(list) = list.as_any().downcast_ref::<ListArray>() {
                    (list.values(), list.value_offsets().iter().map(|&v| v as usize).collect())
                } else {
                    let list = list.as_any().downcast_ref::<LargeListArray>().expect("admitted list type");
                    (list.values(), list.value_offsets().iter().map(|&v| v as usize).collect())
                };
                let mut row = 0;
                let mut within = 0;
                while row < batch.num_rows() {
                    let mut child_start = None;
                    let mut output_len = 0;
                    let mut parents = Vec::new();
                    let mut ends = Vec::new();
                    while row < batch.num_rows() && output_len < batch_size {
                        let null = list.is_null(row);
                        let length = if null { 0 } else { offsets[row + 1] - offsets[row] };
                        let padding = (null && preserve_nulls) || (length == 0 && expand_empty);
                        if length == 0 {
                            if padding {
                                if output_len > 0 { break; }
                                parents.push(row as u64);
                                ends.push(1);
                                output_len = 1;
                                row += 1;
                                break;
                            }
                            row += 1;
                            continue;
                        }
                        let start = offsets[row] + within;
                        if let Some(first) = child_start {
                            if first + output_len != start { break; }
                        } else { child_start = Some(start); }
                        let count = (length - within).min(batch_size - output_len);
                        output_len += count;
                        parents.push(row as u64);
                        ends.push(output_len as i32);
                        within += count;
                        if within == length { row += 1; within = 0; }
                    }
                    if output_len == 0 { continue; }
                    let children = match child_start {
                        Some(start) => values.slice(start, output_len),
                        None => new_null_array(values.data_type(), output_len),
                    };
                    let contiguous = parents.windows(2).all(|pair| pair[1] == pair[0] + 1);
                    let selected = (!contiguous).then(|| UInt64Array::from(parents.clone()));
                    let run_ends = Int32Array::from(ends);
                    let mut output = Vec::with_capacity(batch.num_columns());
                    for (index, array) in batch.columns().iter().enumerate() {
                        if index == column { output.push(children.clone()); continue; }
                        let run_values = match &selected {
                            Some(indices) => take(array, indices, None)?,
                            None => array.slice(parents[0] as usize, parents.len()),
                        };
                        if output_len == parents.len() { output.push(run_values); }
                        else {
                            let encoded = RunArray::<Int32Type>::try_new(&run_ends, run_values.as_ref())?;
                            output.push(cast(&encoded, array.data_type())?);
                        }
                    }
                    yield RecordBatch::try_new(schema.clone(), output)?;
                }
            }
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{Field, Schema};
    use datafusion_common::{NullHandling, UnnestOptions};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_plan::{execution_plan::collect, unnest::ListUnnest};

    #[tokio::test]
    async fn nullable_sliced_children_and_parent_runs_keep_their_values() {
        for large in [false, true] {
            for handling in [NullHandling::Drop, NullHandling::Preserve, NullHandling::PreserveAndExpandEmpty] {
                let values = Arc::new(Int32Array::from(vec![Some(0), None, Some(12), Some(13), Some(14), Some(15), Some(16), Some(17)])) as ArrayRef;
                let item = Arc::new(Field::new("item", DataType::Int32, true));
                let nulls = Some(NullBuffer::from(vec![true, false, true, true]));
                let items: ArrayRef = if large {
                    Arc::new(LargeListArray::new(item, OffsetBuffer::new(vec![1i64, 3, 5, 5, 8].into()), values, nulls))
                } else {
                    Arc::new(ListArray::new(item, OffsetBuffer::new(vec![1i32, 3, 5, 5, 8].into()), values, nulls))
                };
                let schema = Arc::new(Schema::new(vec![Field::new("parent", DataType::Int32, true), Field::new("items", items.data_type().clone(), true)]));
                let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![Some(100), Some(200), Some(300), None])), items]).unwrap();
                let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
                let output = Arc::new(Schema::new(vec![Field::new("parent", DataType::Int32, true), Field::new("items", DataType::Int32, true)]));
                let original: PhysicalPlan = Arc::new(UnnestExec::new(input, vec![ListUnnest { index_in_input_schema: 1, depth: 1 }], vec![], output, UnnestOptions::new().with_null_handling(handling)).unwrap());
                let plan = replace(original).unwrap();
                let context = Arc::new(TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(2)));
                let batches = collect(plan, context).await.unwrap();
                let mut actual = Vec::new();
                for batch in batches {
                    let parents = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                    let values = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
                    actual.extend(parents.iter().zip(values.iter()));
                }
                let mut expected = vec![(Some(100), None), (Some(100), Some(12))];
                if handling != NullHandling::Drop { expected.push((Some(200), None)); }
                if handling == NullHandling::PreserveAndExpandEmpty { expected.push((Some(300), None)); }
                expected.extend([(None, Some(15)), (None, Some(16)), (None, Some(17))]);
                assert_eq!(actual, expected);
            }
        }
    }
}
