//! Freeze source row order before window sorting or hash redistribution.
use crate::plan::PhysicalPlan;
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::{
    AcrossPartitions, LexOrdering, PhysicalSortExpr, expressions::Column,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT_ORDINAL: AtomicU64 = AtomicU64::new(0);
fn same_fields(left: &Arc<Schema>, right: &Arc<Schema>) -> bool {
    left.fields().len() == right.fields().len()
        && left.fields().iter().zip(right.fields()).all(|(a, b)| {
            a.name() == b.name() && a.data_type() == b.data_type() && a.metadata() == b.metadata()
        })
}
fn ordinal_schema(input: &Arc<Schema>, ordinal: Arc<Field>) -> Arc<Schema> {
    let mut fields = input.fields().to_vec();
    fields.push(ordinal);
    Arc::new(Schema::new_with_metadata(fields, input.metadata().clone()))
}
#[derive(Debug, Clone)]
pub(crate) struct SourceOrdinal {
    id: u64,
    input: LogicalPlan,
    schema: DFSchemaRef,
    partition_name: String,
}
impl PartialEq for SourceOrdinal {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for SourceOrdinal {}
impl PartialOrd for SourceOrdinal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl Hash for SourceOrdinal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
impl UserDefinedLogicalNodeCore for SourceOrdinal {
    fn name(&self) -> &str {
        "VailSourceOrdinal"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailSourceOrdinal")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "source ordinal requires one input".into(),
            ));
        }
        let input = inputs.remove(0);
        input
            .schema()
            .index_of_column_by_name(None, &self.partition_name)
            .ok_or_else(|| {
                DataFusionError::Plan("source ordinal partition field is missing".into())
            })?;
        let ordinal = DFSchema::try_from(Schema::new(vec![
            self.schema
                .as_arrow()
                .fields()
                .last()
                .expect("ordinal field")
                .clone(),
        ]))?;
        let schema = Arc::new(input.schema().join(&ordinal)?);
        let id = if schema == self.schema {
            self.id
        } else {
            NEXT_ORDINAL.fetch_add(1, Ordering::Relaxed)
        };
        Ok(Self {
            id,
            input,
            schema,
            partition_name: self.partition_name.clone(),
        })
    }
}
pub(crate) fn numbered(
    input: LogicalPlan,
    name: &str,
    partition_name: &str,
) -> Result<LogicalPlan> {
    // The ordinal counter is local to each input execution stream. Callers must
    // preserve a single-stream domain for each logical partition tag (or
    // retag partitions after splitting them) so repeated rows with the same tag
    // cannot begin in independent streams. The provider's PartitionSetExec
    // stamps each source partition with its own tag; prepared union/repartition
    // paths retag their output partitions in partitioned.rs. lower_sort and
    // lower_uncached's grouped-aggregate branch use an ordered global result
    // with a uniform tag. SourceOrdinalExec also forbids optimizer-inserted
    // input repartitioning and maintains input order, preserving these caller
    // domains after physical planning.
    if input
        .schema()
        .fields()
        .iter()
        .any(|field| field.name() == name)
    {
        return Err(DataFusionError::Plan("source ordinal name collides".into()));
    }
    let ordinal = DFSchema::try_from(Schema::new(vec![Field::new(name, DataType::UInt64, false)]))?;
    let schema = Arc::new(input.schema().join(&ordinal)?);
    input
        .schema()
        .index_of_column_by_name(None, partition_name)
        .ok_or_else(|| DataFusionError::Plan("source ordinal partition field is missing".into()))?;
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(SourceOrdinal {
            id: NEXT_ORDINAL.fetch_add(1, Ordering::Relaxed),
            input,
            schema,
            partition_name: partition_name.to_owned(),
        }),
    }))
}
impl SourceOrdinal {
    pub(crate) fn physical(&self, input: PhysicalPlan) -> Result<PhysicalPlan> {
        if !same_fields(
            &input.schema(),
            &Arc::new(self.input.schema().as_arrow().clone()),
        ) {
            return Err(DataFusionError::Plan(
                "source ordinal physical schema mismatch".into(),
            ));
        }
        Ok(Arc::new(SourceOrdinalExec::new(
            input.clone(),
            ordinal_schema(
                &input.schema(),
                self.schema
                    .as_arrow()
                    .fields()
                    .last()
                    .expect("ordinal field")
                    .clone(),
            ),
            &self.partition_name,
        )?))
    }
}

fn source_partition_constant(
    input: &PhysicalPlan,
    partition_index: usize,
) -> Option<AcrossPartitions> {
    let column_constant = |plan: &PhysicalPlan, index: usize| {
        let column: Arc<dyn datafusion_physical_expr::PhysicalExpr> =
            Arc::new(Column::new(plan.schema().field(index).name(), index));
        plan.properties()
            .equivalence_properties()
            .is_expr_constant(&column)
    };

    if let Some(constant) = column_constant(input, partition_index) {
        return Some(constant);
    }

    // DF55 CrossJoinExec's Full-join equivalence calculation drops both
    // sides' constants and orderings. Its output partitioning and execute()
    // still come directly from the right input, so a right-side partition tag
    // remains constant within each output stream. Remap only right columns;
    // never infer a tag constant from the buffered left input.
    let cross = input
        .as_ref()
        .downcast_ref::<datafusion_physical_plan::joins::CrossJoinExec>()?;
    let left_width = cross.left().schema().fields().len();
    if partition_index < left_width {
        return None;
    }
    column_constant(cross.right(), partition_index - left_width)
}

#[derive(Debug)]
struct SourceOrdinalExec {
    input: PhysicalPlan,
    cache: Arc<PlanProperties>,
    partition_name: String,
    partition_index: usize,
    partition_tag_constant_per_stream: bool,
}
impl SourceOrdinalExec {
    fn new(input: PhysicalPlan, schema: Arc<Schema>, partition_name: &str) -> Result<Self> {
        let partition_index = input.schema().index_of(partition_name).map_err(|_| {
            DataFusionError::Plan("source ordinal partition field is missing".into())
        })?;
        let mut equivalence = datafusion_physical_expr::EquivalenceProperties::new(schema.clone());
        let partition = Arc::new(Column::new(
            schema.field(partition_index).name(),
            partition_index,
        ));
        let ordinal = Arc::new(Column::new(
            schema.field(schema.fields().len() - 1).name(),
            schema.fields().len() - 1,
        ));
        // Only certify the tag when the input properties prove it, including
        // the narrowly handled CrossJoin right-side case above. The ordinal is
        // generated here, so the resulting (tag, ordinal) order is ours.
        let across_partitions = source_partition_constant(&input, partition_index);
        let partition_tag_constant_per_stream = across_partitions.is_some();
        if let Some(across_partitions) = across_partitions {
            equivalence
                .add_constants([datafusion_physical_expr::ConstExpr::new(
                    partition.clone(),
                    across_partitions,
                )])?;
            equivalence.add_ordering(
                LexOrdering::new(vec![
                    PhysicalSortExpr::new(partition, SortOptions::default()),
                    PhysicalSortExpr::new(ordinal, SortOptions::default()),
                ])
                .expect("non-empty source ordinal ordering"),
            );
        }
        let cache = Arc::new(PlanProperties::new(
            equivalence,
            input.properties().output_partitioning().clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            cache,
            partition_name: partition_name.to_owned(),
            partition_index,
            partition_tag_constant_per_stream,
        })
    }
}
impl DisplayAs for SourceOrdinalExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailSourceOrdinalExec")
    }
}
impl ExecutionPlan for SourceOrdinalExec {
    fn name(&self) -> &str {
        "VailSourceOrdinalExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn required_input_ordering(&self) -> Vec<Option<datafusion_physical_expr::OrderingRequirements>> {
        // Sequence determines computed values (prefix state / source ordinal),
        // even when an outer aggregate does not observe output ordering.
        // Preserve an established input ordering; do not manufacture one when
        // the source has no explicit certificate.
        vec![self.input.properties().output_ordering().cloned().map(Into::into)]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, mut children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "source ordinal requires one physical input".into(),
            ));
        }
        let input = children.remove(0);
        if !same_fields(&input.schema(), &self.input.schema()) {
            return Err(DataFusionError::Plan(
                "source ordinal physical child schema changed".into(),
            ));
        }
        let schema = ordinal_schema(
            &input.schema(),
            self.schema()
                .fields()
                .last()
                .expect("ordinal field")
                .clone(),
        );
        Ok(Arc::new(Self::new(input, schema, &self.partition_name)?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let output = schema.clone();
        let partition_index = self.partition_index;
        let partition_tag_constant_per_stream = self.partition_tag_constant_per_stream;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                // Vail's source boundary exposes each logical partition as one
                // physical stream. The counter belongs to this execution only.
                let mut offset=0u64;
                let mut offsets_by_partition = HashMap::<u64, u64>::new();
                while let Some(batch)=input.try_next().await?{
                    let ordinals = if partition_tag_constant_per_stream {
                        let end=offset.checked_add(batch.num_rows() as u64).ok_or_else(||DataFusionError::Execution("source ordinal overflow".into()))?;
                        let ordinals = UInt64Array::from_iter_values(offset..end);
                        offset=end;
                        ordinals
                    } else {
                        let tags = batch
                            .column(partition_index)
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .ok_or_else(|| DataFusionError::Execution("source partition tag is not UInt64".into()))?;
                        let mut ordinals = Vec::with_capacity(batch.num_rows());
                        let mut start = 0;
                        while start < batch.num_rows() {
                            if tags.is_null(start) {
                                Err(DataFusionError::Execution("source partition tag is null".into()))?;
                            }
                            let tag = tags.value(start);
                            let mut end = start + 1;
                            while end < batch.num_rows() && !tags.is_null(end) && tags.value(end) == tag {
                                end += 1;
                            }
                            if end < batch.num_rows() && tags.is_null(end) {
                                Err(DataFusionError::Execution("source partition tag is null".into()))?;
                            }
                            let next = offsets_by_partition.entry(tag).or_default();
                            let run_end = next.checked_add((end - start) as u64).ok_or_else(|| DataFusionError::Execution("source ordinal overflow".into()))?;
                            ordinals.extend(*next..run_end);
                            *next = run_end;
                            start = end;
                        }
                        UInt64Array::from(ordinals)
                    };
                    let mut columns=batch.columns().to_vec();columns.push(Arc::new(ordinals));
                    yield RecordBatch::try_new(output.clone(),columns)?;
                }
            },
        )))
    }
}
pub(crate) fn repeatable_node(plan: &PhysicalPlan) -> bool {
    plan.as_ref().is::<SourceOrdinalExec>()
}
