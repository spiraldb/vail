//! Metadata-only proof for ordinary DataFusion grouped aggregation.
//! The proof covers the complete input relation; execution remains a transparent
//! child stream and all grouping is performed by DataFusion's AggregateExec.
use crate::{Result, ScalarIR, TableNode, TableRef, plan::PhysicalPlan};
use arrow_schema::{DataType, SortOptions};
use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue, SplitPoint};
use datafusion_execution::TaskContext;
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::{
    LexOrdering, PhysicalExpr, PhysicalSortExpr, RangePartitioning, expressions::Column,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use std::{
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub(crate) struct GroupRanges {
    id: u64,
    input: LogicalPlan,
    source: TableRef,
    names: Vec<String>,
    splits: Vec<SplitPoint>,
}
impl PartialEq for GroupRanges {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.input == other.input
    }
}
impl Eq for GroupRanges {}
impl PartialOrd for GroupRanges {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.id.cmp(&other.id) {
            std::cmp::Ordering::Equal => self.input.partial_cmp(&other.input),
            order => Some(order),
        }
    }
}
impl Hash for GroupRanges {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.input.hash(state);
    }
}
impl UserDefinedLogicalNodeCore for GroupRanges {
    fn name(&self) -> &str {
        "VailCertifiedGroupRanges"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailCertifiedGroupRanges")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan("invalid group range rewrite".into()));
        }
        Ok(Self {
            input: inputs.remove(0),
            ..self.clone()
        })
    }
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.input
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }
}

// Float grouping equality can differ from total sort equality (signed zeros and
// NaNs). Until that equivalence is certified too, retain the ordinary hash plan.
fn compatible_type(dtype: &DataType) -> bool {
    match dtype {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => false,
        DataType::Struct(fields) => {
            !fields.is_empty() && fields.iter().all(|f| compatible_type(f.data_type()))
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            compatible_type(field.data_type())
        }
        DataType::Boolean
        | DataType::UInt8
        | DataType::UInt16
        | DataType::Int32
        | DataType::Int64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary => true,
        _ => false,
    }
}
fn source(table: &TableRef, arity: usize) -> Option<TableRef> {
    match table.node() {
        TableNode::Read { .. } | TableNode::Range { .. } => Some(table.clone()),
        TableNode::MapGlobals { child } | TableNode::Filter { child, .. }
            if table.keys() == child.keys() =>
        {
            source(child, arity)
        }
        TableNode::MapRows { child, fields, .. }
            if crate::join_interval::forwards_keys(table, child, fields, arity) =>
        {
            source(child, arity)
        }
        _ => None,
    }
}
fn compare(
    left: &[arrow_array::ArrayRef],
    right: &[arrow_array::ArrayRef],
) -> Result<std::cmp::Ordering> {
    let options = vec![
        SortOptions {
            descending: false,
            nulls_first: false
        };
        left.len()
    ];
    let comparators = crate::join_key::comparators(left, right, &options)?;
    Ok(comparators
        .iter()
        .map(|cmp| cmp(0, 0))
        .find(|cmp| !cmp.is_eq())
        .unwrap_or(std::cmp::Ordering::Equal))
}

pub(crate) fn certify(
    table: &TableRef,
    keys: &[(String, ScalarIR)],
    names: &[String],
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    let table = match table.node() {
        TableNode::Aggregate { child, .. } => child,
        _ => table,
    };
    if keys.is_empty()
        || keys.len() > table.keys().len()
        || keys
            .iter()
            .zip(table.keys())
            .any(|((_, expr), key)| !matches!(expr, ScalarIR::Column { name } if name == key))
    {
        return Ok(input);
    }
    if keys.iter().zip(table.keys()).any(|(_, key)| {
        table
            .schema()
            .field_with_name(key)
            .map_or(true, |field| !compatible_type(field.data_type()))
    }) {
        return Ok(input);
    }
    let Some(source) = source(table, keys.len()) else {
        return Ok(input);
    };
    let Some(bounds) = crate::join_interval::inherited_bounds(table, keys.len())? else {
        return Ok(input);
    };
    // Empty/unknown partitions and touching/overlapping bounds conservatively
    // keep DataFusion's ordinary exchange. No data scan fills metadata gaps.
    if bounds.is_empty() || bounds.iter().any(Option::is_none) {
        return Ok(input);
    }
    let bounds = bounds.into_iter().map(Option::unwrap).collect::<Vec<_>>();
    for bound in &bounds {
        if compare(&bound.first, &bound.last)?.is_gt() {
            return Ok(input);
        }
    }
    for pair in bounds.windows(2) {
        if !compare(&pair[0].last, &pair[1].first)?.is_lt() {
            return Ok(input);
        }
    }
    let splits = bounds
        .iter()
        .skip(1)
        .map(|bound| {
            Ok(SplitPoint::new(
                bound
                    .first
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, 0))
                    .collect::<datafusion_common::Result<Vec<_>>>()?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(GroupRanges {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            input,
            source,
            names: names.to_vec(),
            splits,
        }),
    }))
}

fn singleton(input: &PhysicalPlan) -> bool {
    use datafusion_common::stats::Precision;
    use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    if input.output_partitioning().partition_count() != 1 {
        return false;
    }
    if crate::plan::singleton_globals_source(input) {
        return true;
    }
    if input
        .as_ref()
        .is::<datafusion_physical_plan::projection::ProjectionExec>()
        || input.as_ref().is::<crate::rng_context::SourceLineageExec>()
        || input
            .as_ref()
            .is::<datafusion_physical_plan::coop::CooperativeExec>()
    {
        return input.children().len() == 1 && singleton(input.children()[0]);
    }
    StatisticsContext::new()
        .compute(input.as_ref(), &StatisticsArgs::new())
        .is_ok_and(|stats| stats.num_rows == Precision::Exact(1))
}
fn preserves(input: &PhysicalPlan, source: &TableRef, count: usize) -> bool {
    use datafusion_physical_plan::{
        coalesce_batches::CoalesceBatchesExec,
        filter::FilterExec,
        joins::{CrossJoinExec, NestedLoopJoinExec},
        projection::ProjectionExec,
    };
    if input.output_partitioning().partition_count() != count {
        return false;
    }
    if crate::provider::matches_source_partitions(input, source, count) {
        return true;
    }
    if let Some(join) = input.as_ref().downcast_ref::<NestedLoopJoinExec>() {
        return *join.join_type() == datafusion_expr::JoinType::Inner
            && singleton(join.left())
            && preserves(join.right(), source, count);
    }
    if let Some(join) = input.as_ref().downcast_ref::<CrossJoinExec>() {
        return singleton(join.left()) && preserves(join.right(), source, count);
    }
    // These wrappers preserve partition assignment and relative row sequence.
    // Unknown operators, including every exchange, invalidate the certificate.
    let transparent = input
        .as_ref()
        .is::<datafusion_physical_plan::coop::CooperativeExec>()
        || input.as_ref().is::<ProjectionExec>()
        || input.as_ref().is::<FilterExec>()
        || input.as_ref().is::<CoalesceBatchesExec>()
        || input.as_ref().is::<crate::rng_context::SourceLineageExec>()
        || input.as_ref().is::<crate::rng_context::ConsumerScopeExec>();
    transparent && input.children().len() == 1 && preserves(input.children()[0], source, count)
}
impl GroupRanges {
    pub(crate) fn physical(&self, input: PhysicalPlan) -> datafusion_common::Result<PhysicalPlan> {
        CertifiedExec::wrap(
            input,
            self.source.clone(),
            self.names.clone(),
            self.splits.clone(),
        )
    }
}
#[derive(Debug)]
struct CertifiedExec {
    input: PhysicalPlan,
    source: TableRef,
    names: Vec<String>,
    splits: Vec<SplitPoint>,
    cache: Arc<PlanProperties>,
}
impl CertifiedExec {
    fn wrap(
        input: PhysicalPlan,
        source: TableRef,
        names: Vec<String>,
        splits: Vec<SplitPoint>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if !preserves(&input, &source, splits.len() + 1)
            || names
                .iter()
                .any(|name| input.schema().index_of(name).is_err())
        {
            return Ok(input);
        }
        let ordering = LexOrdering::new(
            names
                .iter()
                .map(|name| {
                    Ok(PhysicalSortExpr::new(
                        Arc::new(Column::new(name, input.schema().index_of(name)?)),
                        SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    ))
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?,
        )
        .ok_or_else(|| DataFusionError::Plan("empty group range ordering".into()))?;
        let range = RangePartitioning::try_new(ordering.clone(), splits.clone())?;
        let mut cache = (**input.properties()).clone();
        // Advertise the certificate in the actual grouping-column namespace.
        // Source aliases can otherwise normalize these keys back to columns
        // removed by the aggregate's ordinary input projection.
        let mut equivalence = datafusion_physical_expr::EquivalenceProperties::new(input.schema());
        equivalence.add_ordering(ordering);
        cache.set_eq_properties(equivalence);
        cache.partitioning = Partitioning::Range(range);
        Ok(Arc::new(Self {
            input,
            source,
            names,
            splits,
            cache: Arc::new(cache),
        }))
    }
}
impl DisplayAs for CertifiedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailCertifiedGroupRangesExec")
    }
}
impl ExecutionPlan for CertifiedExec {
    fn name(&self) -> &'static str {
        "VailCertifiedGroupRangesExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
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
        ) -> datafusion_common::Result<
            datafusion_common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "group range certificate requires one child".into(),
            ));
        }
        Self::wrap(
            children.remove(0),
            self.source.clone(),
            self.names.clone(),
            self.splits.clone(),
        )
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
