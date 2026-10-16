//! Shared, immutable point index for interval queries over sorted table keys.
use crate::plan::PhysicalPlan;
use arrow_array::{
    Array, ArrayRef, BooleanArray, ListArray, RecordBatch, StructArray, new_empty_array,
};
use arrow_buffer::OffsetBuffer;
use arrow_ord::ord::make_comparator;
use arrow_schema::{DataType, SchemaRef, SortOptions};
use datafusion_common::{DataFusionError, Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{cmp::Ordering, fmt, sync::Arc};
use tokio::sync::OnceCell;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
fn options() -> SortOptions {
    SortOptions::default()
}

/// Positions are nondecreasing in interval comparison order; equal keys retain source
/// order. Payload storage owns the same row positions. Construction validates
/// schemas and prepares this invariant once for every left partition/batch.
pub(crate) struct PointIndex {
    keys: ArrayRef,
    raw_schema: SchemaRef,
    key_transform: Arc<dyn PhysicalExpr>,
    payload: Vec<StructArray>,
    positions: Vec<(usize, usize, usize)>,
    source_ordered: bool,
}
impl PointIndex {
    async fn build(
        input: PhysicalPlan,
        key: usize,
        payload: &[usize],
        item: &DataType,
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        let batches = datafusion_physical_plan::collect(input.clone(), context).await?;
        let point_type =
            vail_execution::expressions::Type::from_arrow(input.schema().field(key).data_type())
                .map_err(|error| invalid(&error.to_string()))?;
        let dtype = point_type
            .to_arrow()
            .map_err(|error| invalid(&error.to_string()))?;
        let raw_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "key",
            dtype.clone(),
            true,
        )]));
        // Reuse the exact interval predicate's comparison representation:
        // top-level missing key components sort last via explicit flags;
        // nested values retain native interval comparison's null ordering.
        let key_transform = vail_execution::expressions::lower_with_bindings(
            &crate::interval_read::ordered_key_point(
                vail_execution::expressions::Expr::Column { name: "key".into() },
                &point_type,
            ),
            &raw_schema,
            &arrow_schema::Schema::empty(),
            &Default::default(),
        )
        .map_err(|error| invalid(&error.to_string()))?;
        let key_parts = batches
            .iter()
            .map(|batch| batch.column(key).as_ref())
            .collect::<Vec<_>>();
        let raw_keys = if key_parts.is_empty() {
            new_empty_array(&dtype)
        } else {
            arrow_cast::cast(&arrow_select::concat::concat(&key_parts)?, &dtype)?
        };
        let raw_batch = RecordBatch::try_new(raw_schema.clone(), vec![raw_keys])?;
        let keys = key_transform
            .evaluate(&raw_batch)?
            .into_array(raw_batch.num_rows())?;
        let DataType::Struct(fields) = item else {
            return Err(invalid("range lookup payload must be a struct"));
        };
        let mut views = Vec::with_capacity(batches.len());
        let mut positions = Vec::with_capacity(keys.len());
        let mut offset = 0usize;
        for (batch_index, batch) in batches.iter().enumerate() {
            views.push(StructArray::try_new_with_length(
                fields.clone(),
                payload.iter().map(|&i| batch.column(i).clone()).collect(),
                None,
                batch.num_rows(),
            )?);
            for row in 0..batch.num_rows() {
                if keys.is_valid(offset + row) {
                    positions.push((offset + row, batch_index, row));
                }
            }
            offset = offset
                .checked_add(batch.num_rows())
                .ok_or_else(|| invalid("range lookup row count overflow"))?;
        }
        let payload = views;
        let compare = make_comparator(keys.as_ref(), keys.as_ref(), options())?;
        let source_ordered = positions
            .windows(2)
            .all(|pair| compare(pair[0].0, pair[1].0) != Ordering::Greater);
        if !source_ordered {
            positions.sort_by(|a, b| compare(a.0, b.0));
        }
        Ok(Self {
            keys,
            raw_schema,
            key_transform,
            payload,
            positions,
            source_ordered,
        })
    }
    fn lookup(
        &self,
        intervals: &StructArray,
        output_item: Arc<arrow_schema::Field>,
    ) -> Result<ArrayRef> {
        let raw_start =
            arrow_cast::cast(intervals.column(0), self.raw_schema.field(0).data_type())?;
        let raw_end = arrow_cast::cast(intervals.column(2), self.raw_schema.field(0).data_type())?;
        let convert = |value: &ArrayRef| -> Result<ArrayRef> {
            let batch = RecordBatch::try_new(self.raw_schema.clone(), vec![value.clone()])?;
            self.key_transform
                .evaluate(&batch)?
                .into_array(batch.num_rows())
        };
        let start = convert(&raw_start)?;
        let end = convert(&raw_end)?;
        let exclusive = intervals
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("range lower bound flag is not Boolean"))?;
        let inclusive = intervals
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("range upper bound flag is not Boolean"))?;
        let lower = make_comparator(self.keys.as_ref(), start.as_ref(), options())?;
        let upper = make_comparator(self.keys.as_ref(), end.as_ref(), options())?;
        let mut offsets = vec![0_i32];
        let mut indices = Vec::<(usize, usize)>::new();
        for row in 0..intervals.len() {
            if intervals.is_valid(row)
                && raw_start.is_valid(row)
                && raw_end.is_valid(row)
                && exclusive.is_valid(row)
                && inclusive.is_valid(row)
            {
                let lo = self.positions.partition_point(|&index| {
                    let order = lower(index.0, row);
                    order == Ordering::Less || exclusive.value(row) && order == Ordering::Equal
                });
                let hi = self.positions.partition_point(|&index| {
                    let order = upper(index.0, row);
                    order == Ordering::Less || inclusive.value(row) && order == Ordering::Equal
                });
                if lo < hi {
                    let begin = indices.len();
                    let count = hi - lo;
                    if count > i32::MAX as usize - indices.len() {
                        return Err(invalid("range lookup matches exceed Arrow list capacity"));
                    }
                    indices.extend(
                        self.positions[lo..hi]
                            .iter()
                            .map(|&(_, batch, row)| (batch, row)),
                    );
                    if !self.source_ordered {
                        indices[begin..].sort_unstable();
                    }
                }
            }
            offsets.push(
                i32::try_from(indices.len())
                    .map_err(|_| invalid("range lookup matches exceed Arrow list capacity"))?,
            );
        }
        let values = if indices.is_empty() {
            new_empty_array(output_item.data_type())
        } else {
            let payloads = self
                .payload
                .iter()
                .map(|batch| batch as &dyn Array)
                .collect::<Vec<_>>();
            arrow_select::interleave::interleave(&payloads, &indices)?
        };
        Ok(Arc::new(ListArray::try_new(
            output_item,
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        )?))
    }
}

/// One opaque right-side owner is shared in the physical DAG. Its explicit
/// reset refreshes the hidden source and cell once per action, so resetting
/// individual left consumers cannot split or retain the previous index.
struct PointIndexOwner {
    input: PhysicalPlan,
    key: usize,
    payload: Vec<usize>,
    item: DataType,
    index: OnceCell<Arc<PointIndex>>,
}
impl fmt::Debug for PointIndexOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PointIndexOwner")
            .field("item", &self.item)
            .finish()
    }
}
impl PointIndexOwner {
    async fn get(&self, context: Arc<TaskContext>) -> Result<&Arc<PointIndex>> {
        self.index
            .get_or_try_init(|| async {
                PointIndex::build(
                    self.input.clone(),
                    self.key,
                    &self.payload,
                    &self.item,
                    context,
                )
                .await
                .map(Arc::new)
            })
            .await
    }
}
impl DisplayAs for PointIndexOwner {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailPointIndexOwner")
    }
}
impl ExecutionPlan for PointIndexOwner {
    fn name(&self) -> &'static str {
        "VailPointIndexOwner"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if !children.is_empty() {
            return Err(invalid("point index owner has no physical children"));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> Result<PhysicalPlan> {
        Ok(Arc::new(Self {
            input: crate::partitioned::reset_plan_states(self.input.clone())?,
            key: self.key,
            payload: self.payload.clone(),
            item: self.item.clone(),
            index: OnceCell::new(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

fn index_owner(plan: &PhysicalPlan) -> Result<&PointIndexOwner> {
    let node = plan.as_ref() as &dyn std::any::Any;
    if let Some(owner) = node.downcast_ref::<PointIndexOwner>() {
        return Ok(owner);
    }
    // DataFusion may add a scheduling-only wrapper around a physical source.
    // It preserves this dependency's data/schema and shared owner identity.
    if node.is::<datafusion_physical_plan::coop::CooperativeExec>() {
        let children = plan.children();
        if children.len() == 1 {
            return index_owner(children[0]);
        }
    }
    Err(invalid("range lookup requires a shared point index owner"))
}

#[derive(Debug)]
pub(crate) struct RangeLookupExec {
    left: PhysicalPlan,
    right: PhysicalPlan,
    left_key: usize,
    left_values: Vec<usize>,
    output: SchemaRef,
    item: Arc<arrow_schema::Field>,
    cache: Arc<PlanProperties>,
}
impl RangeLookupExec {
    pub(crate) fn indexed_right(
        input: PhysicalPlan,
        key: &str,
        payload: &[(String, String)],
        output: &SchemaRef,
        field: &str,
    ) -> Result<PhysicalPlan> {
        let key = input.schema().index_of(key)?;
        let payload = payload
            .iter()
            .map(|(_, name)| input.schema().index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let DataType::List(item) = output.field_with_name(field)?.data_type() else {
            return Err(invalid("range lookup result must be a list"));
        };
        Ok(Arc::new(PointIndexOwner {
            input,
            key,
            payload,
            item: item.data_type().clone(),
            index: OnceCell::new(),
        }))
    }
    pub(crate) fn new(
        left: PhysicalPlan,
        right: PhysicalPlan,
        left_key: &str,
        left_values: &[String],
        output: SchemaRef,
        field: &str,
    ) -> Result<PhysicalPlan> {
        let owner = index_owner(&right)?;
        let left_key = left.schema().index_of(left_key)?;
        let left_values = left_values
            .iter()
            .map(|name| left.schema().index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let DataType::List(item) = output.field_with_name(field)?.data_type() else {
            return Err(invalid("range lookup result must be a list"));
        };
        let item = item.clone();
        if left_values.is_empty()
            || left_values.len() + 1 != output.fields().len()
            || *item.data_type() != owner.item
        {
            return Err(invalid("range lookup output schema mismatch"));
        }
        let vail_execution::expressions::Type::Interval {
            interval: point_type,
        } = vail_execution::expressions::Type::from_arrow(
            left.schema().field(left_key).data_type(),
        )
        .map_err(|error| invalid(&error.to_string()))?
        else {
            return Err(invalid("range lookup requires an interval key"));
        };
        if *point_type
            != vail_execution::expressions::Type::from_arrow(
                owner.input.schema().field(owner.key).data_type(),
            )
            .map_err(|error| invalid(&error.to_string()))?
        {
            return Err(invalid("range lookup point types differ"));
        }
        let cache = crate::plan::properties(
            output.clone(),
            datafusion_physical_plan::execution_plan::EmissionType::Incremental,
        );
        Ok(Arc::new(Self {
            left,
            right,
            left_key,
            left_values,
            output,
            item,
            cache,
        }))
    }
}
impl DisplayAs for RangeLookupExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailRangeLookupExec")
    }
}
impl ExecutionPlan for RangeLookupExec {
    fn name(&self) -> &'static str {
        "VailRangeLookupExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.left, &self.right]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true, false]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
    }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![datafusion_physical_plan::Distribution::SinglePartition; 2]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 2
            || children[0].schema() != self.left.schema()
            || children[1].schema() != self.right.schema()
            || index_owner(&children[1]).is_err()
        {
            return Err(invalid("range lookup replacement inputs differ"));
        }
        Ok(Arc::new(Self {
            left: children[0].clone(),
            right: children[1].clone(),
            left_key: self.left_key,
            left_values: self.left_values.clone(),
            output: self.output.clone(),
            item: self.item.clone(),
            cache: self.cache.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(invalid("range lookup requires partition zero"));
        }
        let mut left = self.left.execute(0, context.clone())?;
        let right = self.right.clone();
        let item = self.item.clone();
        let output = self.output.clone();
        let values = self.left_values.clone();
        let left_key = self.left_key;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output.clone(),
            async_stream::try_stream! {
                while let Some(batch)=left.try_next().await?{
                    if batch.num_rows()==0{continue;}
                    let intervals=batch.column(left_key).as_any().downcast_ref::<StructArray>().ok_or_else(||invalid("range query is not an interval struct"))?;
                    let matches=if intervals.null_count()==intervals.len(){
                        Arc::new(ListArray::try_new(item.clone(),OffsetBuffer::new(vec![0_i32;intervals.len()+1].into()),new_empty_array(item.data_type()),None)?) as ArrayRef
                    }else{
                        let owner=index_owner(&right)?;
                        owner.get(context.clone()).await?.lookup(intervals,item.clone())?
                    };
                    let mut columns=values[..values.len()-1].iter().map(|&i|batch.column(i).clone()).collect::<Vec<_>>();
                    columns.push(matches);columns.push(batch.column(*values.last().ok_or_else(||invalid("range lookup lacks partition tag"))?).clone());
                    yield RecordBatch::try_new(output.clone(),columns)?;
                }
            },
        )))
    }
}
