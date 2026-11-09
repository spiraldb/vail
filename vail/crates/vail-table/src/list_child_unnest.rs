//! Stream a single list's children without gathering an already-flat buffer.
use crate::plan::PhysicalPlan;
use arrow_array::{Array, ArrayRef, Int32Array, LargeListArray, ListArray, RecordBatch, RunArray, StructArray, UInt64Array, types::Int32Type};
use arrow_cast::cast;
use arrow_schema::{DataType, Fields};
use arrow_select::take::take;
use async_stream::try_stream;
use datafusion_common::{Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, stream::RecordBatchStreamAdapter, unnest::UnnestExec};
use futures::TryStreamExt;
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
    if lists.len() != 1 || lists[0].depth != 1 || !unnest.struct_column_indices().is_empty()
        || unnest.options().preserve_nulls() || unnest.options().expand_empty_as_null()
    {
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
    fn apply_expressions(&self, f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        self.original.apply_expressions(f)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        replace(self.original.clone().replace_children(children, ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute))?)
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let unnest = original(&self.original).expect("list unnest template");
        let column = unnest.list_column_indices()[0].index_in_input_schema;
        let batch_size = context.session_config().batch_size().max(1).min(i32::MAX as usize);
        let schema = self.schema();
        let input_schema = unnest.input().schema();
        let parent_columns: Vec<_> = (0..input_schema.fields().len()).filter(|&index| index != column).collect();
        let parent_fields: Fields = parent_columns.iter().map(|&index| input_schema.field(index).clone()).collect::<Vec<_>>().into();
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
                        let length = if list.is_null(row) { 0 } else { offsets[row + 1] - offsets[row] };
                        if length == 0 { row += 1; continue; }
                        let start = offsets[row] + within;
                        if let Some(first) = child_start {
                            // Null lists may hide child values. Never expose those values.
                            if first + output_len != start { break; }
                        } else { child_start = Some(start); }
                        let count = (length - within).min(batch_size - output_len);
                        output_len += count;
                        parents.push(row as u64);
                        ends.push(output_len as i32);
                        within += count;
                        if within == length { row += 1; within = 0; }
                    }
                    let Some(start) = child_start else { continue; };
                    let children = values.slice(start, output_len);
                    let contiguous = parents.windows(2).all(|pair| pair[1] == pair[0] + 1);
                    let mut output = if parent_columns.is_empty() { vec![] }
                    else if contiguous {
                        let run_values = StructArray::try_new(parent_fields.clone(), parent_columns.iter()
                            .map(|&index| batch.column(index).slice(parents[0] as usize, parents.len())).collect(), None)?;
                        if output_len == parents.len() { run_values.columns().to_vec() }
                        else {
                            // Decode one struct run array, sharing the expansion indices across fields.
                            let encoded = RunArray::<Int32Type>::try_new(&Int32Array::from(ends), &run_values)?;
                            let expanded = cast(&encoded, run_values.data_type())?;
                            expanded.as_any().downcast_ref::<StructArray>().expect("expanded parent struct").columns().to_vec()
                        }
                    } else {
                        let mut indices = Vec::with_capacity(output_len);
                        for (parent, end) in parents.into_iter().zip(ends) { indices.resize(end as usize, parent); }
                        let indices = UInt64Array::from(indices);
                        parent_columns.iter().map(|&index| take(batch.column(index), &indices, None)).collect::<std::result::Result<Vec<_>, _>>()?
                    };
                    output.insert(column, children);
                    yield RecordBatch::try_new(schema.clone(), output)?;
                }
            }
        })))
    }
}
