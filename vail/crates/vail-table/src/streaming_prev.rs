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
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::OnceCell;
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

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
#[derive(Debug, Clone)]
pub(crate) struct Previous {
    id: u64,
    input: LogicalPlan,
    summary: LogicalPlan,
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
        vec![&self.input, &self.summary]
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
        if !exprs.is_empty() || inputs.len() != 2 {
            return Err(DataFusionError::Plan(
                "previous scan requires two inputs".into(),
            ));
        }
        let summary = inputs.pop().unwrap();
        let input = inputs.pop().unwrap();
        let schema = schema(&input, &self.arguments, &self.outputs)?;
        Ok(Self {
            input,
            summary,
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
    // Two explicit logical consumers: the summary projection needs only scan
    // arguments, while the output stream retains all current-row columns.
    let summary = LogicalPlanBuilder::from(input.clone())
        .project(names.iter().map(datafusion_expr::col).collect::<Vec<_>>())?
        .build()?;
    let schema = schema(&input, &names, &outputs)?;
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(Previous {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            input: crate::logical::ordered(crate::logical::independent_consumer(input)),
            summary: crate::logical::ordered(crate::logical::independent_consumer(summary)),
            schema,
            arguments: names,
            outputs,
        }),
    }))
}
impl Previous {
    pub(crate) fn physical(
        &self,
        input: PhysicalPlan,
        summary: PhysicalPlan,
    ) -> Result<PhysicalPlan> {
        let indices = self
            .arguments
            .iter()
            .map(|name| input.schema().index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if input.properties().output_partitioning().partition_count()
            != summary.properties().output_partitioning().partition_count()
        {
            return Err(DataFusionError::Plan(
                "previous scan input partitions differ".into(),
            ));
        }
        Ok(Arc::new(PreviousExec::new(
            input,
            summary,
            Arc::new(self.schema.as_arrow().clone()),
            indices,
        )?))
    }
}

type Carries = Vec<Vec<ArrayRef>>;
type SharedCarries = Arc<OnceCell<std::result::Result<Carries, Arc<DataFusionError>>>>;
#[derive(Debug)]
struct PreviousExec {
    input: PhysicalPlan,
    summary: PhysicalPlan,
    indices: Vec<usize>,
    cache: Arc<PlanProperties>,
    carries: SharedCarries,
}
impl PreviousExec {
    fn new(
        input: PhysicalPlan,
        summary: PhysicalPlan,
        schema: Arc<Schema>,
        indices: Vec<usize>,
    ) -> Result<Self> {
        let eq = input
            .properties()
            .eq_properties
            .clone()
            .with_new_schema(schema)?;
        let cache = Arc::new(PlanProperties::new(
            eq,
            input.properties().output_partitioning().clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            summary,
            indices,
            cache,
            carries: Arc::new(OnceCell::new()),
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
        vec![&self.input, &self.summary]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true, false]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
    }
    fn with_new_children(self: Arc<Self>, mut children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 2 {
            return Err(DataFusionError::Plan(
                "previous scan requires two physical inputs".into(),
            ));
        }
        let summary = children.pop().unwrap();
        let input = children.pop().unwrap();
        Ok(Arc::new(Self::new(
            input,
            summary,
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
        let summary = self.summary.clone();
        let indices = self.indices.clone();
        let shared = self.carries.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                let mut previous=indices.iter().map(|i| new_null_array(input.schema().field(*i).data_type(),1)).collect::<Vec<_>>();
                if partition>0 {
                    let carries=shared.get_or_init(|| async {
                        summarize(summary, context.clone()).await.map_err(Arc::new)
                    }).await.as_ref().map_err(|error| DataFusionError::Execution(error.to_string()))?;
                    previous=carries.get(partition).ok_or_else(||DataFusionError::Execution("previous scan partition out of range".into()))?.clone();
                }
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
async fn summarize(input: PhysicalPlan, context: Arc<TaskContext>) -> Result<Carries> {
    let partitions = input.properties().output_partitioning().partition_count();
    let empty = input
        .schema()
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
        .collect::<Vec<_>>();
    // JoinSet runs CPU-ready streams on independent workers and aborts them
    // when the action is cancelled. A buffered collection of inline futures
    // alone would serialize these CPU-bound source scans.
    let count = partitions.saturating_sub(1);
    let mut totals = vec![None; count];
    let mut tasks = tokio::task::JoinSet::new();
    let mut next = 0;
    while next < count || !tasks.is_empty() {
        while next < count && tasks.len() < 32 {
            let partition = next;
            next += 1;
            let input = input.clone();
            let context = context.clone();
            let mut last = empty.clone();
            tasks.spawn(async move {
                let mut stream = input.execute(partition, context)?;
                while let Some(batch) = stream.try_next().await? {
                    for (i, values) in batch.columns().iter().enumerate() {
                        if let Some(row) = last_defined(values) {
                            last[i] = compact_row(values, row)?;
                        }
                    }
                }
                Ok::<_, DataFusionError>((partition, last))
            });
        }
        let (partition, last) = tasks
            .join_next()
            .await
            .expect("summary task")
            .map_err(|error| DataFusionError::Execution(error.to_string()))??;
        totals[partition] = Some(last);
    }
    let mut carry = empty;
    let mut result = Vec::with_capacity(partitions);
    result.push(carry.clone());
    for total in totals {
        for (carry, value) in carry.iter_mut().zip(total.expect("completed summary")) {
            if value.is_valid(0) {
                *carry = value;
            }
        }
        result.push(carry.clone());
    }
    Ok(result)
}
