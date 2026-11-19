//! Ordered split rows stay partitioned upstream; only the final merge has one stream.
use crate::{
    Result as TableResult, order_requirement,
    plan::{PhysicalPlan, column, ordinal},
};
use arrow_array::ArrayRef;
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::{DataFusionError, Result, ScalarValue, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr, expressions::Literal};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    empty::EmptyExec,
    projection::ProjectionExec,
    sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    stream::RecordBatchStreamAdapter,
    union::UnionExec,
};
use futures::TryStreamExt;
use std::{cmp::Ordering, fmt, sync::Arc};
use vail_storage::fragment_index::make_typed_key_comparators;

pub(crate) fn merge(
    inputs: Vec<Vec<PhysicalPlan>>,
    keys: &[String],
    schema: SchemaRef,
) -> TableResult<PhysicalPlan> {
    let ordering = order_requirement(&keys.iter().map(|k| (k.clone(), false)).collect::<Vec<_>>())?
        .physical_ordering(&schema)?;
    let mut streams = Vec::new();
    for (branch, parts) in inputs.into_iter().enumerate() {
        for input in parts {
            // Logical partition tags no longer describe the merged output.
            let mut fields = (0..schema.fields().len() - 1)
                .map(|i| (column(&input, i), schema.field(i).name().clone()))
                .collect::<Vec<_>>();
            fields.push((
                Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
                schema.fields().last().unwrap().name().clone(),
            ));
            let projected: PhysicalPlan = Arc::new(ProjectionExec::try_new(fields, input)?);
            streams.push(if branch == 0 {
                CheckOrder::create(projected, ordering.clone())
            } else {
                let numbered = ordinal(projected);
                let mut local_order = ordering.iter().cloned().collect::<Vec<_>>();
                local_order.push(PhysicalSortExpr::new(
                    column(&numbered, schema.fields().len()),
                    SortOptions::default(),
                ));
                let sorted: PhysicalPlan = Arc::new(
                    SortExec::new(LexOrdering::new(local_order).unwrap(), numbered)
                        .with_preserve_partitioning(true),
                );
                let fields = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, field)| (column(&sorted, i), field.name().clone()))
                    .collect::<Vec<_>>();
                Arc::new(ProjectionExec::try_new(fields, sorted)?)
            });
        }
    }
    if streams.is_empty() {
        return Ok(Arc::new(EmptyExec::new(schema)));
    }
    let union = UnionExec::try_new(streams)?;
    // DataFusion spawns a buffered task per input partition; upstream work and
    // right-side local sorts run concurrently rather than inside the merge task.
    Ok(Arc::new(SortPreservingMergeExec::new(ordering, union)))
}

#[derive(Debug)]
struct CheckOrder {
    input: PhysicalPlan,
    ordering: LexOrdering,
    cache: Arc<PlanProperties>,
}
impl CheckOrder {
    fn create(input: PhysicalPlan, ordering: LexOrdering) -> PhysicalPlan {
        let mut cache = (**input.properties()).clone();
        cache.eq_properties.add_ordering(ordering.clone());
        cache.set_eq_properties(cache.eq_properties.clone());
        Arc::new(Self {
            input,
            ordering,
            cache: Arc::new(cache),
        })
    }
}
impl DisplayAs for CheckOrder {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CheckKeyOrderExec")
    }
}
impl ExecutionPlan for CheckOrder {
    fn name(&self) -> &str {
        "CheckKeyOrderExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn apply_expressions(
        &self,
        visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for sort in &self.ordering {
            if visitor(&sort.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "key-order check requires one child".into(),
            ));
        }
        Ok(Self::create(children[0].clone(), self.ordering.clone()))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let ordering = self.ordering.clone();
        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                let mut previous: Option<Vec<ArrayRef>> = None;
                let options = ordering.iter().map(|sort| sort.options).collect::<Vec<_>>();
                while let Some(batch) = input.try_next().await? {
                    let n = batch.num_rows();
                    if n == 0 { continue; }
                    let keys = ordering.iter().map(|sort| sort.expr.evaluate(&batch)?.into_array(n)).collect::<Result<Vec<_>>>()?;
                    if let Some(previous) = &previous {
                        check(previous, &keys, &options, 0, 0)?;
                    }
                    let comparators = make_typed_key_comparators(&keys, &keys, &options)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    for row in 1..n {
                        if comparators.iter().map(|cmp| cmp(row - 1, row)).find(|c| *c != Ordering::Equal) == Some(Ordering::Greater) {
                            Err(DataFusionError::Execution("out-of-order split results; use permit_shuffle=True".into()))?;
                        }
                    }
                    previous = Some(keys.iter().map(|key| key.slice(n - 1, 1)).collect());
                    yield batch;
                }
            },
        )))
    }
}
fn check(
    left: &[ArrayRef],
    right: &[ArrayRef],
    options: &[SortOptions],
    i: usize,
    j: usize,
) -> Result<()> {
    let comparators = make_typed_key_comparators(left, right, options)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    if comparators
        .iter()
        .map(|cmp| cmp(i, j))
        .find(|c| *c != Ordering::Equal)
        == Some(Ordering::Greater)
    {
        return Err(DataFusionError::Execution(
            "out-of-order split results; use permit_shuffle=True".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TableIR;
    use arrow_array::{Int32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_physical_plan::{collect, displayable};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("partition", DataType::UInt64, false),
        ]))
    }
    fn input(batches: &[&[Option<i32>]]) -> PhysicalPlan {
        let batches = batches
            .iter()
            .map(|values| {
                RecordBatch::try_new(
                    schema(),
                    vec![
                        Arc::new(Int32Array::from(values.to_vec())),
                        Arc::new(UInt64Array::from(vec![0; values.len()])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        MemorySourceConfig::try_new_exec(&[batches], schema(), None).unwrap()
    }

    #[tokio::test]
    async fn partition_local_sort_then_merge_overlapping_ranges() {
        let plan = merge(
            vec![
                vec![input(&[&[Some(1), Some(4)]]), input(&[&[Some(2), Some(5)]])],
                vec![input(&[&[Some(6), Some(3)]]), input(&[&[None, Some(0)]])],
            ],
            &["key".into()],
            schema(),
        )
        .unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert_eq!(text.matches("CheckKeyOrderExec").count(), 2);
        assert_eq!(text.matches("SortExec:").count(), 2);
        assert!(!text.contains("CoalescePartitionsExec"));
        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let keys = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                None
            ]
        );
    }

    #[tokio::test]
    async fn logical_planning_preserves_upstream_partition_lanes() {
        let source = TableIR::range(6, 3).unwrap();
        let table = TableIR::ordered_union(vec![source], vec!["idx".into()]).unwrap();
        let plan = table.physical_plan().unwrap();
        assert_eq!(plan.matches("CheckKeyOrderExec").count(), 3, "{plan}");
        assert!(!plan.contains("SortExec:"), "{plan}");
        assert_eq!(table.force_count().await.unwrap(), 6);
    }

    #[tokio::test]
    async fn checks_unchanged_order_within_and_between_batches() {
        for batches in [
            vec![&[Some(2), Some(1)][..]],
            vec![&[Some(2)][..], &[Some(1)][..]],
        ] {
            let plan = merge(vec![vec![input(&batches)]], &["key".into()], schema()).unwrap();
            let error = collect(plan, Arc::new(TaskContext::default()))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("permit_shuffle=True"));
        }
    }
}
