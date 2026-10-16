//! Explicit physical consumer scopes for stateful exchange inputs.
//!
//! A logical consumer is planned once for an action. Optimizer tree rewrites may
//! copy that consumer under several partition views; its stable identity lets
//! final action preparation retain one exchange DAG for those views. Independent
//! logical consumers and explicit action forks receive independent identities.
//! No execution-time partition request creates or selects a replay graph.
use crate::plan::PhysicalPlan;
use datafusion_common::{DataFusionError, Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT_CONSUMER: AtomicU64 = AtomicU64::new(1);

/// Conservative native execution capability. Exchanges and unknown operators
/// keep one-shot consumers; stateless wrappers propagate proven repeatability.
pub(crate) fn repeatable(plan: &PhysicalPlan) -> bool {
    repeatable_with_memo(plan, &mut HashMap::new())
}
fn repeatable_with_memo(plan: &PhysicalPlan, memo: &mut HashMap<usize, bool>) -> bool {
    let pointer = Arc::as_ptr(plan) as *const () as usize;
    if let Some(value) = memo.get(&pointer) {
        return *value;
    }
    use datafusion_physical_plan::{
        aggregates::AggregateExec,
        coalesce_batches::CoalesceBatchesExec,
        coalesce_partitions::CoalescePartitionsExec,
        empty::EmptyExec,
        filter::FilterExec,
        joins::{CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec},
        limit::{GlobalLimitExec, LocalLimitExec},
        placeholder_row::PlaceholderRowExec,
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    };
    let own_replay = crate::zip_snapshot::repeatable_owner(plan);
    let known = own_replay
        || plan.as_ref().is::<ConsumerScopeExec>()
        || plan.as_ref().is::<SourceLineageExec>()
        || crate::plan::repeatable_node(plan)
        || crate::partitioned::repeatable_node(plan)
        || crate::action_parameters::repeatable_node(plan)
        || crate::scan_ordinal::repeatable_node(plan)
        || crate::zip_dedup::repeatable_node(plan)
        || crate::joins::repeatable_node(plan)
        || plan.as_ref().is::<ProjectionExec>()
        || plan.as_ref().is::<FilterExec>()
        || plan.as_ref().is::<SortExec>()
        || plan.as_ref().is::<SortPreservingMergeExec>()
        || plan.as_ref().is::<GlobalLimitExec>()
        || plan.as_ref().is::<LocalLimitExec>()
        || plan.as_ref().is::<UnionExec>()
        || plan.as_ref().is::<CoalesceBatchesExec>()
        || plan.as_ref().is::<CoalescePartitionsExec>()
        || plan.as_ref().is::<AggregateExec>()
        || plan.as_ref().is::<HashJoinExec>()
        || plan.as_ref().is::<NestedLoopJoinExec>()
        || plan.as_ref().is::<CrossJoinExec>()
        || plan.as_ref().is::<SortMergeJoinExec>()
        || plan.as_ref().is::<EmptyExec>()
        || plan.as_ref().is::<PlaceholderRowExec>();
    let value = own_replay
        || (known
            && plan
                .children()
                .into_iter()
                .all(|child| repeatable_with_memo(child, memo)));
    memo.insert(pointer, value);
    value
}
pub(crate) fn consumer(input: PhysicalPlan) -> PhysicalPlan {
    if repeatable(&input) {
        input
    } else {
        Arc::new(ConsumerScopeExec::new(input))
    }
}

pub(crate) fn new_owner_identity() -> (u64, Arc<()>) {
    (NEXT_CONSUMER.fetch_add(1, Ordering::Relaxed), Arc::new(()))
}

fn declared_scope(plan: &PhysicalPlan) -> Option<(u64, Arc<()>, PhysicalPlan)> {
    if let Some(scope) = plan.as_ref().downcast_ref::<ConsumerScopeExec>() {
        Some((scope.id, scope.origin.clone(), scope.input.clone()))
    } else {
        crate::zip_snapshot::owner_identity(plan)
    }
}

#[derive(Debug)]
pub(crate) struct ConsumerScopeExec {
    id: u64,
    input: PhysicalPlan,
    origin: Arc<()>,
    claimed: Mutex<HashSet<usize>>,
}
impl ConsumerScopeExec {
    pub(crate) fn new(input: PhysicalPlan) -> Self {
        Self::rewritten(
            NEXT_CONSUMER.fetch_add(1, Ordering::Relaxed),
            Arc::new(()),
            input,
        )
    }
    fn rewritten(id: u64, origin: Arc<()>, input: PhysicalPlan) -> Self {
        Self {
            id,
            origin,
            input,
            claimed: Mutex::new(HashSet::new()),
        }
    }
}
impl DisplayAs for ConsumerScopeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailConsumerScopeExec: consumer={}", self.id)
    }
}
impl ExecutionPlan for ConsumerScopeExec {
    fn name(&self) -> &'static str {
        "VailConsumerScopeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "consumer scope requires one data child".into(),
            ));
        }
        Ok(Arc::new(Self::rewritten(
            self.id,
            self.origin.clone(),
            children[0].clone(),
        )))
    }
    fn reset_state(self: Arc<Self>) -> Result<PhysicalPlan> {
        // An explicit action/consumer fork receives a fresh identity. The
        // DAG-aware caller has already reset the exposed input's state.
        Ok(Arc::new(Self::new(self.input.clone())))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.input.properties().partitioning.partition_count() {
            return Err(DataFusionError::Execution(
                "consumer partition is out of range".into(),
            ));
        }
        let mut claimed = self
            .claimed
            .lock()
            .map_err(|_| DataFusionError::Execution("consumer state was poisoned".into()))?;
        if !claimed.insert(partition) {
            return Err(DataFusionError::Execution(format!(
                "consumer {} partition {partition} already executed; create an explicit new action or consumer scope for replay",
                self.id
            )));
        }
        drop(claimed);
        self.input.execute(partition, context)
    }
}

/// Stable native provenance for a provider-created source. Physical optimizer
/// copies retain this identity while rebuilding the source's implementation.
#[derive(Debug)]
pub(crate) struct SourceLineageExec {
    input: PhysicalPlan,
    origin: Arc<()>,
}
impl SourceLineageExec {
    pub(crate) fn new(input: PhysicalPlan) -> Self {
        Self {
            input,
            origin: Arc::new(()),
        }
    }
}
impl DisplayAs for SourceLineageExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VailSourceLineageExec")
    }
}
impl ExecutionPlan for SourceLineageExec {
    fn name(&self) -> &'static str {
        "VailSourceLineageExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "source lineage needs one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children[0].clone(),
            origin: self.origin.clone(),
        }))
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

/// Physical optimizers use tree traversal even when partition views share a
/// producer DAG. Reunify only copies of the SAME declared consumer, after all
/// rewrites have completed. Distinct consumer identities never share exchange
/// execution state merely because their logical expressions happen to match.
// DataFusion's Partitioning PartialEq models known distribution equivalence:
// UnknownPartitioning deliberately never equals even itself. Our consumer
// boundary instead validates the output stream count for an already-proven
// common producer lineage; unknown distribution adds no further guarantee.
fn same_consumer_distribution(
    left: &datafusion_physical_plan::Partitioning,
    right: &datafusion_physical_plan::Partitioning,
) -> bool {
    match (left, right) {
        (
            datafusion_physical_plan::Partitioning::UnknownPartitioning(a),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(b),
        ) => a == b,
        _ => left == right,
    }
}

pub(crate) fn bind_consumers(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    #[derive(Debug, PartialEq, Eq, Hash)]
    enum SourceContract {
        Leaf(usize, String),
        Empty(arrow_schema::SchemaRef),
        Singleton(arrow_schema::SchemaRef, usize),
        Inputs(Vec<usize>),
    }
    #[derive(Default)]
    struct Binding {
        sources: HashMap<usize, usize>,
        contracts: HashMap<SourceContract, usize>,
        consumers: HashMap<u64, (PhysicalPlan, usize)>,
        rewritten: HashMap<usize, PhysicalPlan>,
        repeatable: HashMap<usize, bool>,
    }
    impl Binding {
        // Intern a compact DAG, preserving the order and multiplicity of
        // branch inputs. Unary rewrites leave source provenance unchanged.
        // IDs are backed by exact structural equality, never digest equality.
        fn source_contract(&mut self, plan: &PhysicalPlan) -> usize {
            let pointer = Arc::as_ptr(plan) as *const () as usize;
            if let Some(contract) = self.sources.get(&pointer) {
                return *contract;
            }
            let children = plan.children();
            let contract = if let Some(source) = plan.as_ref().downcast_ref::<SourceLineageExec>() {
                let signature = SourceContract::Leaf(
                    Arc::as_ptr(&source.origin) as usize,
                    "native provider source".into(),
                );
                let next = self.contracts.len();
                *self.contracts.entry(signature).or_insert(next)
            } else if children.len() == 1 {
                self.source_contract(children[0])
            } else {
                let signature = if children.is_empty() {
                    if plan.name() == "OrderedConcatExec"
                        || plan
                            .as_ref()
                            .is::<datafusion_physical_plan::empty::EmptyExec>()
                    {
                        SourceContract::Empty(plan.schema())
                    } else if plan
                        .as_ref()
                        .is::<datafusion_physical_plan::placeholder_row::PlaceholderRowExec>()
                    {
                        SourceContract::Singleton(
                            plan.schema(),
                            plan.properties().partitioning.partition_count(),
                        )
                    } else {
                        SourceContract::Leaf(pointer, plan.name().to_owned())
                    }
                } else {
                    SourceContract::Inputs(
                        children
                            .into_iter()
                            .map(|child| self.source_contract(child))
                            .collect(),
                    )
                };
                let next = self.contracts.len();
                *self.contracts.entry(signature).or_insert(next)
            };
            self.sources.insert(pointer, contract);
            contract
        }
        fn visit(&mut self, plan: PhysicalPlan) -> Result<PhysicalPlan> {
            let pointer = Arc::as_ptr(&plan) as *const () as usize;
            if let Some(bound) = self.rewritten.get(&pointer) {
                return Ok(bound.clone());
            }
            let bound = if let Some((id, origin, input)) = declared_scope(&plan) {
                let contract = self.source_contract(&input);
                if let Some((bound, prior_contract)) = self.consumers.get(&id) {
                    let (_, prior_origin, _) = declared_scope(bound).expect("declared owner entry");
                    let mismatch = if !Arc::ptr_eq(&origin, &prior_origin) {
                        Some("producer lineage differs".to_owned())
                    } else if plan.schema() != bound.schema() {
                        Some(format!(
                            "output schema differs: {:?} versus {:?}",
                            plan.schema(),
                            bound.schema()
                        ))
                    } else if !same_consumer_distribution(
                        &plan.properties().partitioning,
                        &bound.properties().partitioning,
                    ) {
                        Some(format!(
                            "output distribution differs: {:?} versus {:?}",
                            plan.properties().partitioning,
                            bound.properties().partitioning
                        ))
                    } else if plan.properties().output_ordering()
                        != bound.properties().output_ordering()
                    {
                        Some(format!(
                            "output ordering differs: {:?} versus {:?}",
                            plan.properties().output_ordering(),
                            bound.properties().output_ordering()
                        ))
                    } else if contract != *prior_contract {
                        let details = self
                            .contracts
                            .iter()
                            .take(16)
                            .map(|(signature, id)| format!("{id}={signature:?}"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        Some(format!(
                            "source provenance differs: {contract} versus {prior_contract}; compact contracts [{details}]"
                        ))
                    } else {
                        None
                    };
                    if let Some(mismatch) = mismatch {
                        return Err(DataFusionError::Plan(format!(
                            "optimizer copies of consumer {} have incompatible source/output contracts: {mismatch}",
                            id
                        )));
                    }
                    bound.clone()
                } else {
                    let input = self.visit(input)?;
                    let bound = plan.clone().replace_children(
                        vec![input],
                        datafusion_physical_plan::ReplaceChildrenOptions::new(
                            datafusion_physical_plan::ChildrenPropertiesMode::Keep,
                        ),
                    )?;
                    self.consumers.insert(id, (bound.clone(), contract));
                    bound
                }
            } else {
                let old = plan.children();
                let children = old
                    .iter()
                    .map(|child| self.visit((*child).clone()))
                    .collect::<Result<Vec<_>>>()?;
                if old.iter().zip(&children).all(|(a, b)| Arc::ptr_eq(a, b)) {
                    plan
                } else {
                    plan.replace_children(
                        children,
                        datafusion_physical_plan::ReplaceChildrenOptions::new(
                            datafusion_physical_plan::ChildrenPropertiesMode::Keep,
                        ),
                    )?
                }
            };
            // Validate declared identity/output/source contracts before
            // omitting a redundant one-shot guard on a repeatable producer.
            let bound = if let Some(scope) = bound.as_ref().downcast_ref::<ConsumerScopeExec>()
                && repeatable_with_memo(&scope.input, &mut self.repeatable)
            {
                scope.input.clone()
            } else {
                bound
            };
            self.rewritten.insert(pointer, bound.clone());
            Ok(bound)
        }
    }
    // The original root stays alive throughout the traversal, so pointer keys
    // cannot be recycled for newly reconstructed nodes during this action.
    Binding::default().visit(plan.clone())
}
