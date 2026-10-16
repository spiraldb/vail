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
            let bound = if let Some(scope) = plan.as_ref().downcast_ref::<ConsumerScopeExec>() {
                let contract = self.source_contract(&scope.input);
                if let Some((bound, prior_contract)) = self.consumers.get(&scope.id) {
                    let prior = bound
                        .as_ref()
                        .downcast_ref::<ConsumerScopeExec>()
                        .expect("consumer entry");
                    let mismatch = if !Arc::ptr_eq(&scope.origin, &prior.origin) {
                        Some("producer lineage differs".to_owned())
                    } else if scope.schema() != prior.schema() {
                        Some(format!(
                            "output schema differs: {:?} versus {:?}",
                            scope.schema(),
                            prior.schema()
                        ))
                    } else if !same_consumer_distribution(
                        &scope.properties().partitioning,
                        &prior.properties().partitioning,
                    ) {
                        Some(format!(
                            "output distribution differs: {:?} versus {:?}",
                            scope.properties().partitioning,
                            prior.properties().partitioning
                        ))
                    } else if scope.properties().output_ordering()
                        != prior.properties().output_ordering()
                    {
                        Some(format!(
                            "output ordering differs: {:?} versus {:?}",
                            scope.properties().output_ordering(),
                            prior.properties().output_ordering()
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
                            scope.id
                        )));
                    }
                    bound.clone()
                } else {
                    let input = self.visit(scope.input.clone())?;
                    let bound: PhysicalPlan = Arc::new(ConsumerScopeExec::rewritten(
                        scope.id,
                        scope.origin.clone(),
                        input,
                    ));
                    self.consumers.insert(scope.id, (bound.clone(), contract));
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
            self.rewritten.insert(pointer, bound.clone());
            Ok(bound)
        }
    }
    // The original root stays alive throughout the traversal, so pointer keys
    // cannot be recycled for newly reconstructed nodes during this action.
    Binding::default().visit(plan.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_physical_plan::{Partitioning, repartition::RepartitionExec};
    use futures::TryStreamExt;

    fn source() -> PhysicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batches = (0..3)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![i * 2, i * 2 + 1])) as ArrayRef],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let source = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        Arc::new(RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(3)).unwrap())
    }
    async fn values(scope: &PhysicalPlan, partition: usize) -> Vec<i64> {
        scope
            .execute(partition, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }
    #[tokio::test]
    async fn independent_consumer_scopes_support_interleaved_partition_order() {
        let first: PhysicalPlan = Arc::new(ConsumerScopeExec::new(source()));
        let second = crate::partitioned::reset_plan_states(first.clone()).unwrap();
        for partition in [2, 0, 1] {
            assert_eq!(
                values(&first, partition).await,
                vec![partition as i64 * 2, partition as i64 * 2 + 1]
            );
            assert_eq!(
                values(&second, partition).await,
                vec![partition as i64 * 2, partition as i64 * 2 + 1]
            );
        }
        let error = match first.execute(0, Arc::new(TaskContext::default())) {
            Err(error) => error,
            Ok(_) => panic!("duplicate consumer partition must fail"),
        };
        assert!(error.to_string().contains("explicit new action"));
    }
    #[tokio::test]
    async fn optimizer_forks_share_one_exchange_across_partition_views() {
        let scope: PhysicalPlan = Arc::new(ConsumerScopeExec::new(source()));
        // Physical optimizer tree traversals reconstruct a producer separately
        // underneath each partition view. All views still name one consumer.
        let mut views = Vec::new();
        for partition in [2, 0, 1] {
            let producer = scope.children()[0].clone();
            let rebuilt = producer
                .clone()
                .with_new_children(producer.children().into_iter().cloned().collect())
                .unwrap();
            let fork = scope.clone().with_new_children(vec![rebuilt]).unwrap();
            views.push(
                Arc::new(crate::partitioned::PartitionViewExec::new(fork, partition))
                    as PhysicalPlan,
            );
        }
        let output = Arc::new(crate::partitioned::OrderedConcatExec::new(
            views,
            scope.schema(),
        )) as PhysicalPlan;
        let bound = bind_consumers(output).unwrap();
        assert_eq!(values(&bound, 0).await, vec![4, 5, 0, 1, 2, 3]);
    }
    #[tokio::test]
    async fn independent_forks_preserve_optimizer_copied_fanout() {
        let producer: PhysicalPlan = Arc::new(ConsumerScopeExec::new(source()));
        let mut views = Vec::new();
        for partition in [2, 0, 1] {
            let copy = producer
                .clone()
                .with_new_children(producer.children().into_iter().cloned().collect())
                .unwrap();
            views.push(
                Arc::new(crate::partitioned::PartitionViewExec::new(copy, partition))
                    as PhysicalPlan,
            );
        }
        let output: PhysicalPlan = Arc::new(crate::partitioned::OrderedConcatExec::new(
            views,
            producer.schema(),
        ));
        let first = crate::partitioned::reset_plan_states(output.clone()).unwrap();
        let second = crate::partitioned::reset_plan_states(output).unwrap();
        assert_eq!(values(&first, 0).await, vec![4, 5, 0, 1, 2, 3]);
        assert_eq!(values(&second, 0).await, vec![4, 5, 0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn nested_exchanges_above_inner_scopes_share_the_complete_producer() {
        fn optimizer_copy(plan: PhysicalPlan) -> PhysicalPlan {
            if plan.children().is_empty() {
                return plan;
            }
            let children = plan
                .children()
                .into_iter()
                .cloned()
                .map(optimizer_copy)
                .collect();
            plan.with_new_children(children).unwrap()
        }
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let partitions = (0..3)
            .map(|i| {
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![i * 2, i * 2 + 1])) as ArrayRef],
                    )
                    .unwrap(),
                ]
            })
            .collect::<Vec<_>>();
        let sources = MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap();
        let first_exchange =
            Arc::new(RepartitionExec::try_new(sources, Partitioning::RoundRobinBatch(3)).unwrap())
                as PhysicalPlan;
        let inner: PhysicalPlan = Arc::new(ConsumerScopeExec::new(first_exchange));
        let outer =
            Arc::new(RepartitionExec::try_new(inner, Partitioning::RoundRobinBatch(3)).unwrap())
                as PhysicalPlan;
        let schema = outer.schema();
        let views = crate::partitioned::partition_views(outer, 3)
            .into_iter()
            .map(optimizer_copy)
            .collect();
        let output =
            Arc::new(crate::partitioned::OrderedConcatExec::new(views, schema)) as PhysicalPlan;
        let bound = bind_consumers(output).unwrap();
        let mut actual = values(&bound, 0).await;
        actual.sort_unstable();
        assert_eq!(actual, vec![0, 1, 2, 3, 4, 5]);
    }
    #[tokio::test]
    async fn unknown_distribution_fanout_keeps_all_real_source_streams() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let partitions = (0..3)
            .map(|i| {
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![i * 2, i * 2 + 1])) as ArrayRef],
                    )
                    .unwrap(),
                ]
            })
            .collect::<Vec<_>>();
        let source = MemorySourceConfig::try_new_exec(&partitions, schema.clone(), None).unwrap();
        let views = crate::partitioned::partition_views(source, 3)
            .into_iter()
            .map(|view| {
                let scope = view.children()[0].clone();
                let rebuilt = scope
                    .clone()
                    .with_new_children(scope.children().into_iter().cloned().collect())
                    .unwrap();
                view.with_new_children(vec![rebuilt]).unwrap()
            })
            .collect();
        let output =
            Arc::new(crate::partitioned::OrderedConcatExec::new(views, schema)) as PhysicalPlan;
        let bound = bind_consumers(output).unwrap();
        assert_eq!(values(&bound, 0).await, vec![0, 1, 2, 3, 4, 5]);
    }
    #[test]
    fn optimizer_forks_with_incompatible_distribution_are_rejected() {
        let scope: PhysicalPlan = Arc::new(ConsumerScopeExec::new(source()));
        let changed =
            Arc::new(RepartitionExec::try_new(source(), Partitioning::RoundRobinBatch(2)).unwrap())
                as PhysicalPlan;
        let fork = scope.clone().with_new_children(vec![changed]).unwrap();
        let output = Arc::new(crate::partitioned::OrderedConcatExec::new(
            vec![scope, fork],
            source().schema(),
        )) as PhysicalPlan;
        let error = bind_consumers(output).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("incompatible source/output contracts")
        );
    }
    #[test]
    fn optimizer_forks_with_unrelated_sources_are_rejected() {
        let scope: PhysicalPlan = Arc::new(ConsumerScopeExec::new(source()));
        let fork = scope.clone().with_new_children(vec![source()]).unwrap();
        let output = Arc::new(crate::partitioned::OrderedConcatExec::new(
            vec![scope, fork],
            source().schema(),
        )) as PhysicalPlan;
        assert!(
            bind_consumers(output)
                .unwrap_err()
                .to_string()
                .contains("incompatible source/output contracts")
        );
    }
    #[tokio::test]
    async fn canceled_action_does_not_retain_a_hidden_run_graph() {
        let input = source();
        let weak = Arc::downgrade(&input);
        let scope: PhysicalPlan = Arc::new(ConsumerScopeExec::new(input));
        let mut stream = scope.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert!(stream.try_next().await.unwrap().is_some());
        drop(scope);
        drop(stream);
        // The source/exchange graph belongs to the action and its streams;
        // there is no retained global run list after both are dropped.
        assert!(weak.upgrade().is_none());
    }
}
