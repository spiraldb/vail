//! Freeze source row order before window sorting or hash redistribution.
use crate::plan::PhysicalPlan;
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
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
        Ok(Self { id, input, schema })
    }
}
pub(crate) fn numbered(input: LogicalPlan, name: &str) -> Result<LogicalPlan> {
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
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(SourceOrdinal {
            id: NEXT_ORDINAL.fetch_add(1, Ordering::Relaxed),
            input,
            schema,
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
        )))
    }
}
#[derive(Debug)]
struct SourceOrdinalExec {
    input: PhysicalPlan,
    cache: Arc<PlanProperties>,
}
impl SourceOrdinalExec {
    fn new(input: PhysicalPlan, schema: Arc<Schema>) -> Self {
        let cache = Arc::new(PlanProperties::new(
            datafusion_physical_expr::EquivalenceProperties::new(schema),
            input.properties().output_partitioning().clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Self { input, cache }
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
        Ok(Arc::new(Self::new(input, schema)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let output = schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                // Vail's source boundary exposes each logical partition as one
                // physical stream. The counter belongs to this execution only.
                let mut offset=0u64;
                while let Some(batch)=input.try_next().await?{
                    let end=offset.checked_add(batch.num_rows() as u64).ok_or_else(||DataFusionError::Execution("source ordinal overflow".into()))?;
                    let mut columns=batch.columns().to_vec();columns.push(Arc::new(UInt64Array::from_iter_values(offset..end)));
                    offset=end;yield RecordBatch::try_new(output.clone(),columns)?;
                }
            },
        )))
    }
}
pub(crate) fn repeatable_node(plan: &PhysicalPlan) -> bool {
    plan.as_ref().is::<SourceOrdinalExec>()
}
