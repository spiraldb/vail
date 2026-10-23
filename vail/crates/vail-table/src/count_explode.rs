//! Action-local cardinality of a terminal, validity-filtered list expansion.
//!
//! The fully planned input still executes every source column and upstream
//! expression. Only infallible field access and physical row replication are
//! replaced; arbitrary expressions, sorts, limits and predicates block this rule.
use crate::plan::PhysicalPlan;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, RecordBatch,
    RecordBatchOptions, StructArray,
};
use arrow_schema::Schema;
use datafusion_common::{DataFusionError, NullHandling, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{
    EquivalenceProperties, PhysicalExpr, ScalarFunctionExpr,
    expressions::{Column, IsNotNullExpr, Literal},
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    filter::FilterExec,
    projection::ProjectionExec,
    scalar_subquery::ScalarSubqueryExec,
    stream::RecordBatchStreamAdapter,
    unnest::UnnestExec,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use vail_execution::expressions::Expr;

fn path(expr: &Arc<dyn PhysicalExpr>) -> Option<(usize, Vec<String>)> {
    let expr = crate::projection_broadcast::count_inner(expr);
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some((column.index(), vec![]));
    }
    let function = expr.downcast_ref::<ScalarFunctionExpr>()?;
    let (expression, schema) = vail_execution::logical_expr::semantic_kernel(function.fun())?;
    fn fields<'a>(expr: &'a Expr, names: &mut Vec<String>) -> Option<&'a str> {
        match expr {
            Expr::Column { name } => Some(name),
            Expr::GetField { value, name } => {
                let root = fields(value, names)?;
                names.push(name.clone());
                Some(root)
            }
            _ => None,
        }
    }
    let mut suffix = vec![];
    let root = fields(expression, &mut suffix)?;
    let argument = function.args().get(schema.index_of(root).ok()?)?;
    let (index, mut names) = path(argument)?;
    names.extend(suffix);
    Some((index, names))
}
fn field_only(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let expr = crate::projection_broadcast::count_inner(expr);
    expr.is::<Literal>() || path(&expr).is_some()
}

pub(crate) fn optimize(plan: PhysicalPlan) -> Result<Option<PhysicalPlan>> {
    if let Some(entries) = plan.downcast_ref::<crate::matrix_entries::MatrixEntriesExec>() {
        return Ok(Some(entries.count_plan()));
    }
    if let Some(merge) = plan.downcast_ref::<datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec>() {
        if merge.fetch().is_none() {
            if let Some(input) = optimize(merge.input().clone())? {
                return Ok(Some(Arc::new(datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input))));
            }
        }
        return Ok(None);
    }
    if plan.is::<crate::partitioned::PartitionSetExec>() {
        let schema = plan.schema();
        if plan.children().iter().any(|child| {
            let child = child.schema();
            child.fields().len() != schema.fields().len()
                || child
                    .fields()
                    .iter()
                    .zip(schema.fields())
                    .any(|(left, right)| left.data_type() != right.data_type())
        }) {
            return Ok(None);
        }
        let mut parts = Vec::new();
        for child in plan.children() {
            let Some(part) = optimize(child.clone())? else {
                return Ok(None);
            };
            parts.push(part);
        }
        return Ok(Some(Arc::new(crate::partitioned::PartitionSetExec::new(
            parts,
            Arc::new(Schema::empty()),
        ))));
    }
    if plan.is::<datafusion_physical_plan::union::UnionExec>() {
        let mut parts = Vec::new();
        for child in plan.children() {
            let Some(part) = optimize(child.clone())? else {
                return Ok(None);
            };
            parts.push(part);
        }
        return Ok(Some(datafusion_physical_plan::union::UnionExec::try_new(
            parts,
        )?));
    }
    if plan.is::<crate::partitioned::OrderedConcatExec>() {
        // Concat may normally cast to its declared schema. Only erase row
        // assembly when every child already has exactly those value types.
        let schema = plan.schema();
        if plan.children().iter().any(|child| {
            let child = child.schema();
            child.fields().len() != schema.fields().len()
                || child
                    .fields()
                    .iter()
                    .zip(schema.fields())
                    .any(|(left, right)| left.data_type() != right.data_type())
        }) {
            return Ok(None);
        }
        let mut parts = Vec::new();
        for child in plan.children() {
            let Some(part) = optimize(child.clone())? else {
                return Ok(None);
            };
            parts.push(part);
        }
        return Ok(Some(Arc::new(crate::partitioned::OrderedConcatExec::new(
            parts,
            Arc::new(Schema::empty()),
        ))));
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        if projection
            .expr()
            .iter()
            .all(|entry| field_only(&entry.expr))
        {
            return optimize(projection.input().clone());
        }
        return Ok(None);
    }
    if plan.is::<datafusion_physical_plan::coop::CooperativeExec>()
        || plan.is::<crate::partitioned::PartitionViewExec>()
        || plan.is::<ScalarSubqueryExec>()
        || crate::action_parameters::repeatable_node(&plan)
        || plan.is::<crate::rng_context::ConsumerScopeExec>()
    {
        let mut children = plan.children().into_iter().cloned().collect::<Vec<_>>();
        let Some(input) = children.first() else {
            return Ok(None);
        };
        if let Some(replacement) = optimize(input.clone())? {
            children[0] = replacement;
            return Ok(Some(plan.replace_children(
                children,
                datafusion_physical_plan::ReplaceChildrenOptions::new(
                    datafusion_physical_plan::ChildrenPropertiesMode::Recompute,
                ),
            )?));
        }
        return Ok(None);
    }
    let Some(filter) = plan.downcast_ref::<FilterExec>() else {
        return Ok(None);
    };
    if filter.fetch().is_some() {
        return Ok(None);
    }
    let Some(valid) = filter.predicate().downcast_ref::<IsNotNullExpr>() else {
        return Ok(None);
    };
    let Some((index, fields)) = path(valid.arg()) else {
        return Ok(None);
    };
    let Some(unnest) = filter.input().downcast_ref::<UnnestExec>() else {
        return Ok(None);
    };
    let lists = unnest.list_column_indices();
    if lists.len() != 1
        || lists[0].depth != 1
        || lists[0].index_in_input_schema != index
        || !unnest.struct_column_indices().is_empty()
        || unnest.options().null_handling != NullHandling::Drop
    {
        return Ok(None);
    }
    if let Some((input, program, selected_fields)) = zip_program(unnest.input(), index, &fields) {
        return Ok(Some(Arc::new(
            CountListExec::new(input, 0, selected_fields).with_program(program),
        )));
    }
    Ok(Some(Arc::new(CountListExec::new(
        unnest.input().clone(),
        index,
        fields,
    ))))
}

#[derive(Debug, Clone)]
enum ZipArguments {
    Builtin(Vec<Arc<dyn PhysicalExpr>>),
    Semantic {
        bindings: Vec<Arc<dyn PhysicalExpr>>,
        programs: Vec<vail_execution::logical_expr::BoundSemanticProgram>,
    },
}
impl ZipArguments {
    fn len(&self) -> usize {
        match self {
            Self::Builtin(arguments) => arguments.len(),
            Self::Semantic { programs, .. } => programs.len(),
        }
    }
    fn expressions(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        match self {
            Self::Builtin(arguments) => arguments.iter().collect(),
            Self::Semantic { bindings, .. } => bindings.iter().collect(),
        }
    }
    fn count(&self, batch: &RecordBatch, selected: usize, fields: &[String]) -> Result<usize> {
        let Self::Semantic { bindings, programs } = self else {
            return count_value(self.evaluate(batch, selected)?, batch.num_rows(), fields);
        };
        let values = bindings
            .iter()
            .map(|expression| expression.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        let mut count = None;
        for (index, program) in programs.iter().enumerate() {
            if index == selected {
                count = Some(
                    if let Some(result) = program.count_take(&values, batch.num_rows(), fields) {
                        result?
                    } else {
                        count_value(
                            program.evaluate(&values, batch.num_rows())?,
                            batch.num_rows(),
                            fields,
                        )?
                    },
                );
            } else {
                program.evaluate(&values, batch.num_rows())?;
            }
        }
        count.ok_or_else(|| {
            DataFusionError::Internal("semantic zip count selected argument missing".into())
        })
    }
    fn evaluate(&self, batch: &RecordBatch, selected: usize) -> Result<ColumnarValue> {
        match self {
            Self::Builtin(arguments) => {
                let mut selected_value = None;
                for (index, expression) in arguments.iter().enumerate() {
                    let value = expression.evaluate(batch)?;
                    if index == selected {
                        selected_value = Some(value);
                    }
                }
                selected_value.ok_or_else(|| {
                    DataFusionError::Internal("zip count selected argument missing".into())
                })
            }
            Self::Semantic { bindings, programs } => {
                // Match ScalarFunctionExpr: capture every physical ref once,
                // before evaluating any part of the native semantic program.
                let values = bindings
                    .iter()
                    .map(|expression| expression.evaluate(batch))
                    .collect::<Result<Vec<_>>>()?;
                let mut selected_value = None;
                for (index, program) in programs.iter().enumerate() {
                    let value = program.evaluate(&values, batch.num_rows())?;
                    if index == selected {
                        selected_value = Some(value);
                    }
                }
                selected_value.ok_or_else(|| {
                    DataFusionError::Internal("semantic zip count selected argument missing".into())
                })
            }
        }
    }
}
#[derive(Debug, Clone)]
enum Evaluation {
    Ordinary(Arc<dyn PhysicalExpr>),
    Zip {
        arguments: ZipArguments,
        selected: usize,
    },
}
#[derive(Debug, Clone)]
struct ZipProgram(Vec<Evaluation>);
impl ZipProgram {
    fn count(&self, batch: &RecordBatch, fields: &[String]) -> Result<usize> {
        let mut count = None;
        for step in &self.0 {
            match step {
                Evaluation::Ordinary(expression) => {
                    expression.evaluate(batch)?;
                }
                Evaluation::Zip {
                    arguments,
                    selected,
                } => {
                    count = Some(arguments.count(batch, *selected, fields)?);
                }
            }
        }
        count.ok_or_else(|| DataFusionError::Internal("zip count has no selected argument".into()))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let mut selected_value = None;
        for step in &self.0 {
            match step {
                Evaluation::Ordinary(expression) => {
                    expression.evaluate(batch)?;
                }
                Evaluation::Zip {
                    arguments,
                    selected,
                } => {
                    selected_value = Some(arguments.evaluate(batch, *selected)?);
                }
            }
        }
        selected_value
            .ok_or_else(|| DataFusionError::Internal("zip count has no selected argument".into()))
    }
}
fn zip_arguments(expr: &Arc<dyn PhysicalExpr>) -> Option<ZipArguments> {
    let expr = crate::projection_broadcast::count_inner(expr);
    let function = expr.downcast_ref::<ScalarFunctionExpr>()?;
    if function
        .fun()
        .inner()
        .is::<datafusion_functions_nested::arrays_zip::ArraysZip>()
    {
        return Some(ZipArguments::Builtin(function.args().to_vec()));
    }
    Some(ZipArguments::Semantic {
        bindings: function.args().to_vec(),
        programs: vail_execution::logical_expr::semantic_zip_programs(function.fun())?,
    })
}
fn zip_program(
    plan: &PhysicalPlan,
    index: usize,
    fields: &[String],
) -> Option<(PhysicalPlan, ZipProgram, Vec<String>)> {
    let projection = plan.downcast_ref::<ProjectionExec>()?;
    let selected = fields.first()?.parse::<usize>().ok()?.checked_sub(1)?;
    let mut steps = Vec::with_capacity(projection.expr().len());
    for (position, entry) in projection.expr().iter().enumerate() {
        if position == index {
            let arguments = zip_arguments(&entry.expr)?;
            if selected >= arguments.len() {
                return None;
            }
            steps.push(Evaluation::Zip {
                arguments,
                selected,
            });
        } else {
            // Evaluate all other outputs exactly as the owning projection did,
            // including computations whose values a count does not consume.
            steps.push(Evaluation::Ordinary(entry.expr.clone()));
        }
    }
    if index >= projection.expr().len() {
        return None;
    }
    Some((
        projection.input().clone(),
        ZipProgram(steps),
        fields[1..].to_vec(),
    ))
}

#[derive(Debug)]
struct CountListExec {
    input: PhysicalPlan,
    column: usize,
    fields: Vec<String>,
    program: Option<ZipProgram>,
    cache: Arc<PlanProperties>,
}
impl CountListExec {
    fn new(input: PhysicalPlan, column: usize, fields: Vec<String>) -> Self {
        let schema = Arc::new(Schema::empty());
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            column,
            fields,
            program: None,
            cache,
        }
    }
}
impl CountListExec {
    fn with_program(mut self, program: ZipProgram) -> Self {
        self.program = Some(program);
        self
    }
}
impl DisplayAs for CountListExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CountListExpansionExec: column={}, fields={:?}",
            self.column, self.fields
        )
    }
}
impl ExecutionPlan for CountListExec {
    fn name(&self) -> &'static str {
        "CountListExpansionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "list count needs one child".into(),
            ));
        }
        let mut replacement = Self::new(children[0].clone(), self.column, self.fields.clone());
        replacement.program = self.program.clone();
        Ok(Arc::new(replacement))
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        let expressions = self
            .program
            .iter()
            .flat_map(|program| program.0.iter())
            .flat_map(|step| match step {
                Evaluation::Ordinary(expression) => vec![expression],
                Evaluation::Zip { arguments, .. } => arguments.expressions(),
            })
            .collect::<Vec<_>>();
        datafusion_physical_plan::apply_expression_roots(expressions, f)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = self.schema();
        let output = schema.clone();
        let column = self.column;
        let fields = self.fields.clone();
        let program = self.program.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream.and_then(move |batch| {
                let count = if let Some(program) = &program {
                    program.count(&batch, &fields)
                } else {
                    count_list(batch.column(column), &fields)
                };
                let result = count.and_then(|n| {
                    Ok(RecordBatch::try_new_with_options(
                        output.clone(),
                        vec![],
                        &RecordBatchOptions::new().with_row_count(Some(n)),
                    )?)
                });
                futures::future::ready(result)
            }),
        )))
    }
}
fn count_value(value: ColumnarValue, rows: usize, fields: &[String]) -> Result<usize> {
    match value {
        ColumnarValue::Array(array) => count_list(&array, fields),
        ColumnarValue::Scalar(value) => count_list(&value.to_array_of_size(1)?, fields)?
            .checked_mul(rows)
            .ok_or_else(|| DataFusionError::Execution("list count overflow".into())),
    }
}
pub(crate) fn count_list(array: &ArrayRef, fields: &[String]) -> Result<usize> {
    if array.data_type() == &arrow_schema::DataType::Null {
        return Ok(0);
    }
    if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let start = list.value_offset(0) as usize;
        let width = list.value_length() as usize;
        return count_offsets(
            list,
            list.values(),
            (0..=list.len()).map(|i| start + i * width).collect(),
            fields,
        );
    }

    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return count_offsets(
            list,
            list.values(),
            list.offsets().iter().map(|v| *v as usize).collect(),
            fields,
        );
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        return count_offsets(
            list,
            list.values(),
            list.offsets().iter().map(|v| *v as usize).collect(),
            fields,
        );
    }
    Err(DataFusionError::Internal(
        "list expansion count requires list input".into(),
    ))
}
fn count_offsets(
    list: &dyn Array,
    values: &ArrayRef,
    offsets: Vec<usize>,
    fields: &[String],
) -> Result<usize> {
    let mut levels = vec![values.clone()];
    for name in fields {
        let parent = levels
            .last()
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Internal("count field path requires struct".into()))?;
        let child = parent
            .column_by_name(name)
            .ok_or_else(|| DataFusionError::Internal("count field absent".into()))?
            .clone();
        levels.push(child);
    }
    // Combine ancestor/entry logical validity once per batch, then count bits
    // over each list range. Never inspect nullable payload fields.
    let validity = levels
        .iter()
        .filter_map(|array| array.logical_nulls())
        .filter(|nulls| nulls.null_count() != 0)
        .reduce(|left, right| arrow_buffer::NullBuffer::union(Some(&left), Some(&right)).unwrap());
    let list_nulls = list.logical_nulls();
    if list_nulls
        .as_ref()
        .is_none_or(|nulls| nulls.null_count() == 0)
    {
        let start = offsets[0];
        let len = offsets[list.len()] - start;
        return Ok(validity
            .as_ref()
            .map_or(len, |nulls| len - nulls.slice(start, len).null_count()));
    }
    let mut count = 0;
    for row in 0..list.len() {
        if !list_nulls.as_ref().unwrap().is_valid(row) {
            continue;
        }
        let start = offsets[row];
        let len = offsets[row + 1] - start;
        count += validity
            .as_ref()
            .map_or(len, |nulls| len - nulls.slice(start, len).null_count());
    }
    Ok(count)
}
