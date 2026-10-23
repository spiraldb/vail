//! Exclusive previous-defined-value scans over Arrow batches. DataFusion plans
//! the input projections; this extension supplies the streaming state that its
//! scalar window evaluator cannot represent without per-row scalar conversion.
use crate::plan::PhysicalPlan;
use arrow_array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow_schema::{Field, Schema};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use futures::future::BoxFuture;
use std::sync::atomic::AtomicBool;
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{OnceCell, Semaphore};
use vail_execution::expressions::{Expr as VExpr, UnaryOperator};

/// Recognize only the canonical last-defined fold with a null identity. A
/// different initializer/sequence/combine must keep ordinary fold semantics.
pub(crate) fn argument(node: &VExpr) -> Option<&VExpr> {
    let VExpr::Aggregate {
        function,
        value: Some(fold),
        options,
        init,
        ..
    } = node
    else {
        return None;
    };
    if function != "__scan_fold"
        || !init.is_empty()
        || options.as_object().is_some_and(|o| {
            o.keys()
                .any(|k| !matches!(k.as_str(), "__vail_scan" | "__vail_result_type"))
        })
    {
        return None;
    }
    let VExpr::AggregateFold {
        initial,
        seq,
        combine,
        state_parameter,
        left_parameter,
        right_parameter,
    } = fold.as_ref()
    else {
        return None;
    };
    let null = match initial.as_ref() {
        VExpr::Literal { value, .. } => value.is_null(),
        VExpr::RuntimeLiteral { value } => value.is_null(),
        _ => false,
    };
    if !null {
        return None;
    }
    fn defined(test: &VExpr, value: &VExpr) -> bool {
        matches!(test, VExpr::Unary { operator: UnaryOperator::Not, value: test }
            if matches!(test.as_ref(), VExpr::IsMissing {value: child} if child.as_ref() == value))
    }
    fn variable(expr: &VExpr, parameter: &str) -> bool {
        matches!(expr, VExpr::LambdaVariable {name, ..} if name == parameter)
    }
    let VExpr::IfElse {
        condition,
        consequent,
        alternate,
    } = seq.as_ref()
    else {
        return None;
    };
    if !defined(condition, consequent) || !variable(alternate, state_parameter) {
        return None;
    }
    // The value must be a row expression, not a recurrence over fold state.
    // Syntactically similar folds may still depend on the previous state.
    let mut captures_state = false;
    consequent.visit_unique(&mut |expr| {
        captures_state |= matches!(expr, VExpr::LambdaVariable {name, ..}
            if name == state_parameter || name == left_parameter || name == right_parameter);
        captures_state |=
            matches!(expr, VExpr::NestedFunction {function, ..} if function.starts_with("random_"));
    });
    if captures_state
        || state_parameter == left_parameter
        || state_parameter == right_parameter
        || left_parameter == right_parameter
    {
        return None;
    }
    let VExpr::IfElse {
        condition,
        consequent: right,
        alternate: left,
    } = combine.as_ref()
    else {
        return None;
    };
    (defined(condition, right)
        && variable(right, right_parameter)
        && variable(left, left_parameter))
    .then_some(consequent.as_ref())
}

// A replayable source partition is independent of other output streams.
// Exchanges/sorts/joins/upstream scans retain the generic lowering until their
// producer exposes an explicit demand-safe replay domain. Deterministic
// pointwise expressions preserve the native source's partition membership.
pub(crate) fn independent_source(table: &crate::TableRef) -> bool {
    use crate::TableNode;
    fn stable(expr: &VExpr) -> bool {
        let mut stable = true;
        expr.visit_unique(&mut |node| {
            stable &= !matches!(node, VExpr::Aggregate { .. } | VExpr::AggregateFold { .. });
            stable &= !matches!(node, VExpr::NestedFunction {function, ..} if function.starts_with("random_"));
        });
        stable
    }
    match table.node() {
        TableNode::Read { .. } | TableNode::Parallelize { .. } | TableNode::Range { .. } => true,
        TableNode::MapGlobals { child } => independent_source(child),
        TableNode::MapRows { child, fields, .. } => {
            independent_source(child) && fields.iter().all(|(_, expr)| stable(expr))
        }
        TableNode::Filter {
            child, predicate, ..
        } => independent_source(child) && stable(predicate),
        TableNode::KeyBy { child, keys } if keys.is_empty() => independent_source(child),
        _ => false,
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
#[derive(Debug, Clone)]
pub(crate) struct Previous {
    id: u64,
    input: LogicalPlan,
    schema: DFSchemaRef,
    arguments: Vec<String>,
    outputs: Vec<String>,
}
impl PartialEq for Previous {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Previous {}
impl PartialOrd for Previous {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}
impl Hash for Previous {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
fn schema(input: &LogicalPlan, arguments: &[String], outputs: &[String]) -> Result<DFSchemaRef> {
    let fields = arguments
        .iter()
        .zip(outputs)
        .map(|(argument, output)| {
            let field = input.schema().field_with_unqualified_name(argument)?;
            Ok(Field::new(output, field.data_type().clone(), true))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(
        input
            .schema()
            .join(&DFSchema::try_from(Schema::new(fields))?)?,
    ))
}
impl UserDefinedLogicalNodeCore for Previous {
    fn name(&self) -> &str {
        "VailPreviousNonNull"
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
        write!(f, "VailPreviousNonNull")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "previous scan requires one input".into(),
            ));
        }
        let input = inputs.pop().unwrap();
        let schema = schema(&input, &self.arguments, &self.outputs)?;
        Ok(Self {
            input,
            schema,
            ..self.clone()
        })
    }
}
pub(crate) fn logical(
    input: LogicalPlan,
    arguments: Vec<(Expr, String, String)>,
) -> Result<LogicalPlan> {
    let mut columns: Vec<_> = input
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    columns.extend(
        arguments
            .iter()
            .map(|(expr, name, _)| expr.clone().alias(name)),
    );
    let input = LogicalPlanBuilder::from(input).project(columns)?.build()?;
    let names: Vec<_> = arguments.iter().map(|(_, name, _)| name.clone()).collect();
    let outputs: Vec<_> = arguments.into_iter().map(|(_, _, name)| name).collect();
    let schema = schema(&input, &names, &outputs)?;
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(Previous {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            input: crate::logical::independent_consumer(input),
            schema,
            arguments: names,
            outputs,
        }),
    }))
}
impl Previous {
    pub(crate) fn physical(&self, input: PhysicalPlan) -> Result<PhysicalPlan> {
        let indices = self
            .arguments
            .iter()
            .map(|name| input.schema().index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Arc::new(PreviousExec::new(
            input,
            Arc::new(self.schema.as_arrow().clone()),
            indices,
        )?))
    }
}

type Values = Vec<ArrayRef>;
type Saved = OnceCell<std::result::Result<Values, Arc<DataFusionError>>>;
#[derive(Debug)]
struct Summary {
    value: Saved,
    attempted: AtomicBool,
}
#[derive(Debug)]
struct Prefixes {
    input: PhysicalPlan,
    indices: Vec<usize>,
    producer: OnceCell<PhysicalPlan>,
    summaries: Vec<Summary>,
    carries: Vec<Saved>,
    permits: Arc<Semaphore>,
}
impl Prefixes {
    fn empty(&self) -> Values {
        self.indices
            .iter()
            .map(|i| new_null_array(self.input.schema().field(*i).data_type(), 1))
            .collect()
    }
    fn carry(
        self: Arc<Self>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> BoxFuture<'static, Result<Values>> {
        Box::pin(async move {
            if partition == 0 {
                return Ok(self.empty());
            }
            let saved = self.carries.get(partition).ok_or_else(|| {
                DataFusionError::Execution("previous scan partition out of range".into())
            })?;
            let value = saved
                .get_or_init(|| async {
                    // Each carry and summary is initialized once. This dependency
                    // chain visits only preceding partitions, with O(P) shared work.
                    // Spawn each dependency stage so thousands of partitions
                    // do not create a recursively polled future stack. Tasks
                    // and retained state scale with partitions, never rows.
                    let state = self.clone();
                    let task = datafusion_common_runtime::SpawnedTask::spawn(async move {
                        let (mut carry, total) = tokio::try_join!(
                            state.clone().carry(partition - 1, context.clone()),
                            state.clone().summary(partition - 1, context)
                        )?;
                        for (carry, value) in carry.iter_mut().zip(total) {
                            if value.is_valid(0) {
                                *carry = value;
                            }
                        }
                        Ok::<_, DataFusionError>(carry)
                    });
                    match task.await {
                        Ok(result) => result.map_err(Arc::new),
                        Err(error) => Err(Arc::new(DataFusionError::Execution(error.to_string()))),
                    }
                })
                .await;
            value
                .clone()
                .map_err(|error| DataFusionError::Execution(error.to_string()))
        })
    }
    async fn summary(
        self: Arc<Self>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<Values> {
        let saved = &self.summaries[partition];
        let result = saved
            .value
            .get_or_init(|| async {
                let state = self.clone();
                let task = datafusion_common_runtime::SpawnedTask::spawn(async move {
                    let _permit = state
                        .permits
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                    // Fork the *same optimized physical producer*, not a second
                    // independently optimized projection with a potentially
                    // different partition domain. No logical replanning occurs.
                    let producer = state
                        .producer
                        .get_or_try_init(|| async {
                            crate::partitioned::reset_plan_states(state.input.clone())
                        })
                        .await?;
                    let retry = state.summaries[partition]
                        .attempted
                        .swap(true, Ordering::Relaxed);
                    // Cancellation can leave an exchange/consumer partly claimed.
                    // A retry receives fresh producer state; other readers keep
                    // their original action-owned producer and completed summaries.
                    let input = if retry {
                        crate::partitioned::reset_plan_states(producer.clone())?
                    } else {
                        producer.clone()
                    };
                    let mut stream = input.execute(partition, context)?;
                    let mut last = state.empty();
                    while let Some(batch) = stream.try_next().await? {
                        tokio::task::consume_budget().await;
                        for (i, index) in state.indices.iter().enumerate() {
                            let values = batch.column(*index);
                            if let Some(row) = last_defined(values) {
                                last[i] = compact_row(values, row)?;
                            }
                        }
                    }
                    Ok::<_, DataFusionError>(last)
                });
                match task.await {
                    Ok(result) => result.map_err(Arc::new),
                    Err(error) => Err(Arc::new(DataFusionError::Execution(error.to_string()))),
                }
            })
            .await;
        result
            .clone()
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }
}
#[derive(Debug)]
struct PreviousExec {
    input: PhysicalPlan,
    indices: Vec<usize>,
    cache: Arc<PlanProperties>,
    prefixes: Arc<Prefixes>,
}
impl PreviousExec {
    fn new(input: PhysicalPlan, schema: Arc<Schema>, indices: Vec<usize>) -> Result<Self> {
        let partitions = input.properties().output_partitioning().partition_count();
        let mut eq = datafusion_physical_expr::EquivalenceProperties::new(schema);
        if let Some(ordering) = input.properties().output_ordering() {
            eq.add_ordering(ordering.clone());
        }
        let cache = Arc::new(PlanProperties::new(
            eq,
            input.properties().output_partitioning().clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        let prefixes = Arc::new(Prefixes {
            input: input.clone(),
            indices: indices.clone(),
            producer: OnceCell::new(),
            summaries: (0..partitions)
                .map(|_| Summary {
                    value: OnceCell::new(),
                    attempted: AtomicBool::new(false),
                })
                .collect(),
            carries: (0..partitions).map(|_| OnceCell::new()).collect(),
            permits: Arc::new(Semaphore::new(32)),
        });
        Ok(Self {
            input,
            indices,
            cache,
            prefixes,
        })
    }
}
impl DisplayAs for PreviousExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailPreviousNonNullExec")
    }
}
impl ExecutionPlan for PreviousExec {
    fn name(&self) -> &str {
        "VailPreviousNonNullExec"
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
                "previous scan requires one physical input".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.schema(),
            self.indices.clone(),
        )?))
    }
    fn reset_state(self: Arc<Self>) -> Result<PhysicalPlan> {
        Ok(Arc::new(Self::new(
            self.input.clone(),
            self.schema(),
            self.indices.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let output = schema.clone();
        let input = self.input.clone();
        let indices = self.indices.clone();
        let prefixes = self.prefixes.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                let mut previous=prefixes.carry(partition, context.clone()).await?;
                let mut stream=input.execute(partition,context)?;
                while let Some(batch)=stream.try_next().await? {
                    if batch.num_rows()==0 {continue;}
                    let mut columns=batch.columns().to_vec();
                    for (i,index) in indices.iter().enumerate() {
                        let values=batch.column(*index);
                        columns.push(shift(values, &previous[i])?);
                        if let Some(last)=last_defined(values) {previous[i]=compact_row(values,last)?;}
                    }
                    yield RecordBatch::try_new(output.clone(),columns)?;
                }
            },
        )))
    }
}
fn last_defined(values: &ArrayRef) -> Option<usize> {
    if values.null_count() == 0 {
        values.len().checked_sub(1)
    } else {
        (0..values.len()).rfind(|i| values.is_valid(*i))
    }
}
fn compact_row(values: &ArrayRef, row: usize) -> Result<ArrayRef> {
    // take copies only one retained row; a slice would pin a whole batch's
    // nested buffers for arbitrarily long runs of missing values.
    Ok(arrow_select::take::take(
        values.as_ref(),
        &arrow_array::UInt64Array::from(vec![row as u64]),
        None,
    )?)
}
fn shift(values: &ArrayRef, previous: &ArrayRef) -> Result<ArrayRef> {
    if values.null_count() == 0 {
        return Ok(arrow_select::concat::concat(&[
            previous.as_ref(),
            values.slice(0, values.len() - 1).as_ref(),
        ])?);
    }
    let mut selected = (0, 0);
    let mut indices = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        indices.push(selected);
        if values.is_valid(i) {
            selected = (1, i);
        }
    }
    Ok(arrow_select::interleave::interleave(
        &[previous.as_ref(), values.as_ref()],
        &indices,
    )?)
}
