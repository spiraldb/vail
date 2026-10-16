//! Adjacent present-key deduplication over a certified canonical-key stream.
use crate::plan::{PhysicalPlan, column};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::SortOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use vail_storage::fragment_index::make_typed_key_comparators;

#[derive(Debug)]
struct AdjacentKeys {
    input: PhysicalPlan,
    key: usize,
    ordering: LexOrdering,
    cache: Arc<PlanProperties>,
}
/// The caller establishes canonical key order (nulls last), including stable
/// source ordinals within ties. Missing key parents are never deduplicated.
pub(crate) fn distinct(input: PhysicalPlan, key: usize) -> Result<PhysicalPlan> {
    if key >= input.schema().fields().len() {
        return Err(DataFusionError::Plan(
            "zip key column exceeds schema".into(),
        ));
    }
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        column(&input, key),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )])
    .unwrap();
    let mut cache = (**input.properties()).clone();
    cache.eq_properties.add_ordering(ordering.clone());
    cache.set_eq_properties(cache.eq_properties.clone());
    Ok(Arc::new(AdjacentKeys {
        input,
        key,
        ordering,
        cache: Arc::new(cache),
    }))
}
impl DisplayAs for AdjacentKeys {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZipAdjacentKeysExec")
    }
}
impl ExecutionPlan for AdjacentKeys {
    fn name(&self) -> &str {
        "ZipAdjacentKeysExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![datafusion_physical_plan::Distribution::SinglePartition]
    }
    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion_physical_expr::OrderingRequirements>> {
        vec![Some(self.ordering.clone().into())]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, mut children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan("zip dedup requires one input".into()));
        }
        let input = children.remove(0);
        if input.schema() != self.input.schema() {
            return Err(DataFusionError::Plan(
                "zip dedup child schema changed".into(),
            ));
        }
        distinct(input, self.key)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let key = self.key;
        let schema = self.schema();
        let output = schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                let mut previous:Option<ArrayRef>=None;
                let options=[SortOptions{descending:false,nulls_first:false}];
                while let Some(batch)=input.try_next().await?{
                    if batch.num_rows()==0{continue;}
                    let keys=batch.column(key);
                    let compare=make_typed_key_comparators(std::slice::from_ref(keys),std::slice::from_ref(keys),&options).map_err(|e|DataFusionError::Execution(e.to_string()))?;
                    let boundary=previous.as_ref().map(|previous|make_typed_key_comparators(std::slice::from_ref(previous),std::slice::from_ref(keys),&options)).transpose().map_err(|e|DataFusionError::Execution(e.to_string()))?;
                    let mut keep=Vec::with_capacity(batch.num_rows());
                    for row in 0..batch.num_rows(){
                        let duplicate=if row==0{boundary.as_ref().is_some_and(|compare|compare[0](0,0).is_eq())}else{compare[0](row-1,row).is_eq()};
                        if keys.is_null(row)||!duplicate{keep.push(row as u64);}
                    }
                    let last=UInt64Array::from(vec![(batch.num_rows()-1) as u64]);
                    previous=Some(arrow_select::take::take(keys.as_ref(),&last,None)?);
                    if keep.len()==batch.num_rows(){yield batch;}
                    else if !keep.is_empty(){
                        let indices=UInt64Array::from(keep);
                        let columns=batch.columns().iter().map(|column|arrow_select::take::take(column.as_ref(),&indices,None)).collect::<std::result::Result<Vec<_>,_>>()?;
                        yield RecordBatch::try_new(output.clone(),columns)?;
                    }
                }
            },
        )))
    }
}
pub(crate) fn repeatable_node(plan: &PhysicalPlan) -> bool {
    plan.as_ref().is::<AdjacentKeys>()
}
