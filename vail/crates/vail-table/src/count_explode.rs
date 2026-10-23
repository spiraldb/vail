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
    if plan.is::<ScalarSubqueryExec>()
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
fn count_list(array: &ArrayRef, fields: &[String]) -> Result<usize> {
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
    let nullable = levels
        .iter()
        .filter(|array| array.null_count() != 0)
        .collect::<Vec<_>>();
    let mut count = 0;
    for row in 0..list.len() {
        if list.is_null(row) {
            continue;
        }
        let range = offsets[row]..offsets[row + 1];
        count += if nullable.is_empty() {
            range.len()
        } else {
            range
                .filter(|index| nullable.iter().all(|array| array.is_valid(*index)))
                .count()
        };
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field};
    use datafusion_common::UnnestOptions;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_physical_expr::expressions::CastExpr;
    use datafusion_physical_plan::{execution_plan::collect, unnest::ListUnnest};

    fn input() -> PhysicalPlan {
        let values = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![
                None,
                Some(2),
                Some(3),
                Some(4),
            ]))],
            Some(NullBuffer::from(vec![true, false, true, true])),
        )) as ArrayRef;
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3, 4])),
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )) as ArrayRef;
        let batch = RecordBatch::try_from_iter(vec![
            ("items", list),
            (
                "error",
                Arc::new(StringArray::from(vec!["invalid"; 3])) as ArrayRef,
            ),
        ])
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap()
    }
    fn expanded(input: PhysicalPlan) -> PhysicalPlan {
        let mut fields = input.schema().fields().to_vec();
        let DataType::List(item) = fields[0].data_type() else {
            unreachable!()
        };
        fields[0] = Arc::new(Field::new("items", item.data_type().clone(), true));
        let unnest = Arc::new(
            UnnestExec::new(
                input,
                vec![ListUnnest {
                    index_in_input_schema: 0,
                    depth: 1,
                }],
                vec![],
                Arc::new(Schema::new(fields)),
                UnnestOptions::new().with_null_handling(NullHandling::Drop),
            )
            .unwrap(),
        );
        Arc::new(
            FilterExec::try_new(
                Arc::new(IsNotNullExpr::new(Arc::new(Column::new("items", 0)))),
                unnest,
            )
            .unwrap(),
        )
    }
    async fn rows(plan: PhysicalPlan) -> Result<usize> {
        Ok(collect(plan, Arc::new(TaskContext::default()))
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum())
    }
    #[tokio::test]
    async fn counts_match_expansion_with_null_lists_and_defined_allnull_structs() {
        let original = expanded(input());
        let optimized = optimize(original.clone())
            .unwrap()
            .expect("eligible expansion");
        assert_eq!(rows(original).await.unwrap(), 2);
        assert_eq!(rows(optimized).await.unwrap(), 2);
    }
    #[tokio::test]
    async fn owning_projection_errors_still_execute() {
        let input = input();
        let projected = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (
                        Arc::new(Column::new("items", 0)) as Arc<dyn PhysicalExpr>,
                        "items".to_owned(),
                    ),
                    (
                        Arc::new(CastExpr::new(
                            Arc::new(Column::new("error", 1)),
                            DataType::Int64,
                            Some(arrow_cast::CastOptions {
                                safe: false,
                                ..Default::default()
                            }),
                        )) as Arc<dyn PhysicalExpr>,
                        "error".to_owned(),
                    ),
                ],
                input,
            )
            .unwrap(),
        );
        let optimized = optimize(expanded(projected)).unwrap().unwrap();
        assert!(rows(optimized).await.is_err());
    }
    #[test]
    fn post_expansion_fallible_projection_blocks_rule() {
        let plan = expanded(input());
        let projected = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(CastExpr::new(
                        Arc::new(Column::new("error", 1)),
                        DataType::Int64,
                        None,
                    )) as Arc<dyn PhysicalExpr>,
                    "error".to_owned(),
                )],
                plan,
            )
            .unwrap(),
        );
        assert!(optimize(projected).unwrap().is_none());
    }
    #[tokio::test]
    async fn typed_field_validity_matches_normal_expansion() {
        let expanded = expanded(input());
        let unnest = expanded
            .downcast_ref::<FilterExec>()
            .unwrap()
            .input()
            .clone();
        let schema =
            datafusion_common::DFSchema::try_from(unnest.schema().as_ref().clone()).unwrap();
        let expression = Expr::GetField {
            value: Arc::new(Expr::Column {
                name: "items".into(),
            }),
            name: "x".into(),
        };
        let logical = vail_execution::logical_expr::lower_logical_with_bindings(
            &expression,
            &schema,
            &Schema::empty(),
            &Default::default(),
        )
        .unwrap();
        let field = datafusion_physical_expr::create_physical_expr(
            &logical,
            &schema,
            &datafusion_expr::execution_props::ExecutionProps::new(),
            &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
        )
        .unwrap();
        assert_eq!(path(&field), Some((0, vec!["x".into()])));
        let original =
            Arc::new(FilterExec::try_new(Arc::new(IsNotNullExpr::new(field)), unnest).unwrap())
                as PhysicalPlan;
        let optimized = optimize(original.clone()).unwrap().unwrap();
        assert_eq!(rows(original).await.unwrap(), 1);
        assert_eq!(rows(optimized).await.unwrap(), 1);
    }
    fn field_expression(input: &PhysicalPlan, names: &[&str]) -> Arc<dyn PhysicalExpr> {
        let schema =
            datafusion_common::DFSchema::try_from(input.schema().as_ref().clone()).unwrap();
        let mut expression = Expr::Column {
            name: "items".into(),
        };
        for name in names {
            expression = Expr::GetField {
                value: Arc::new(expression),
                name: (*name).into(),
            };
        }
        let logical = vail_execution::logical_expr::lower_logical_with_bindings(
            &expression,
            &schema,
            &Schema::empty(),
            &Default::default(),
        )
        .unwrap();
        datafusion_physical_expr::create_physical_expr(
            &logical,
            &schema,
            &datafusion_expr::execution_props::ExecutionProps::new(),
            &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
        )
        .unwrap()
    }
    fn filtered_zip(
        input: PhysicalPlan,
        other: Arc<dyn PhysicalExpr>,
        extra: Option<Arc<dyn PhysicalExpr>>,
    ) -> PhysicalPlan {
        let zip = Arc::new(
            ScalarFunctionExpr::try_new(
                datafusion_functions_nested::arrays_zip::arrays_zip_udf(),
                vec![Arc::new(Column::new("items", 0)), other],
                input.schema().as_ref(),
                Arc::new(datafusion_common::config::ConfigOptions::default()),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let mut expressions = vec![(zip, "items".to_string())];
        if let Some(extra) = extra {
            expressions.push((extra, "extra".to_string()));
        }
        let projected = Arc::new(ProjectionExec::try_new(expressions, input).unwrap());
        let expansion = expanded(projected);
        let unnest = expansion
            .downcast_ref::<FilterExec>()
            .unwrap()
            .input()
            .clone();
        Arc::new(
            FilterExec::try_new(
                Arc::new(IsNotNullExpr::new(field_expression(&unnest, &["1"]))),
                unnest,
            )
            .unwrap(),
        )
    }
    #[tokio::test]
    async fn zip_bypass_preserves_padding_and_null_lists() {
        use arrow_array::types::Int32Type;
        let batches = collect(input(), Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let other = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10)]),
            None,
            Some(vec![Some(1), Some(2), Some(3)]),
        ])) as ArrayRef;
        let batch = RecordBatch::try_from_iter(vec![
            ("items", batches[0].column(0).clone()),
            ("other", other),
        ])
        .unwrap();
        let source =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        let original = filtered_zip(source, Arc::new(Column::new("other", 1)), None);
        let optimized = optimize(original.clone()).unwrap().unwrap();
        assert!(
            optimized
                .downcast_ref::<CountListExec>()
                .unwrap()
                .program
                .is_some()
        );
        assert_eq!(rows(original).await.unwrap(), 2);
        assert_eq!(rows(optimized).await.unwrap(), 2);
    }
    #[tokio::test]
    async fn zip_bypass_still_executes_other_arguments_and_projection_outputs() {
        let failure = Arc::new(CastExpr::new(
            Arc::new(Column::new("error", 1)),
            DataType::Int64,
            Some(arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            }),
        )) as Arc<dyn PhysicalExpr>;
        let null = Arc::new(Literal::new(
            datafusion_common::ScalarValue::try_from(&DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true,
            ))))
            .unwrap(),
        )) as Arc<dyn PhysicalExpr>;
        let source = input();
        let failing_argument = Arc::new(
            ScalarFunctionExpr::try_new(
                datafusion_functions_nested::make_array::make_array_udf(),
                vec![failure.clone()],
                source.schema().as_ref(),
                Arc::new(datafusion_common::config::ConfigOptions::default()),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;
        let original_argument_error = filtered_zip(source, failing_argument, None);
        assert!(
            rows(optimize(original_argument_error.clone()).unwrap().unwrap())
                .await
                .is_err()
        );
        assert!(rows(original_argument_error).await.is_err());
        let original = filtered_zip(input(), null.clone(), Some(failure));
        let optimized = optimize(original).unwrap().unwrap();
        assert!(rows(optimized).await.is_err());
        // Null scalar arguments are still evaluated, while null-only padding
        // cannot create a present element in the selected zip input.
        let original = filtered_zip(input(), null, None);
        assert_eq!(
            rows(optimize(original.clone()).unwrap().unwrap())
                .await
                .unwrap(),
            rows(original).await.unwrap()
        );
    }
    #[tokio::test]
    async fn typed_kernel_zip_bypass_preserves_selection_and_errors() {
        use vail_execution::expressions::Type;
        // __vail_list_take requires defined owning arrays. Null-list behavior
        // belongs to the separate arrays_zip regression above.
        let batches = collect(input(), Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let original = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let DataType::List(item) = original.data_type() else {
            unreachable!()
        };
        let defined = Arc::new(ListArray::new(
            item.clone(),
            original.offsets().clone(),
            original.values().clone(),
            None,
        )) as ArrayRef;
        let batch = RecordBatch::try_from_iter(vec![
            ("items", defined),
            ("error", batches[0].column(1).clone()),
        ])
        .unwrap();
        let source =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        let schema =
            datafusion_common::DFSchema::try_from(source.schema().as_ref().clone()).unwrap();
        let list_type = Type::from_arrow(source.schema().field(0).data_type()).unwrap();
        let Type::Array { array: element } = &list_type else {
            unreachable!()
        };
        for position in [0, 1] {
            let positions = datafusion_common::ScalarValue::new_list(
                &[
                    datafusion_common::ScalarValue::Int32(Some(position)),
                    datafusion_common::ScalarValue::Int32(Some(position)),
                ],
                &DataType::Int32,
                true,
            );
            let selected = Expr::NestedFunction {
                function: "__vail_list_take".into(),
                args: vec![
                    Expr::Column {
                        name: "items".into(),
                    },
                    Expr::RuntimeLiteral {
                        value: datafusion_common::ScalarValue::List(positions),
                    },
                ],
                dtype: list_type.clone(),
            };
            let expression = Expr::NestedFunction {
                function: "arrays_zip".into(),
                args: vec![
                    selected,
                    Expr::Column {
                        name: "items".into(),
                    },
                ],
                dtype: Type::Array {
                    array: Box::new(Type::Struct {
                        r#struct: vec![
                            ("1".into(), element.as_ref().clone()),
                            ("2".into(), element.as_ref().clone()),
                        ],
                    }),
                },
            };
            let logical = vail_execution::logical_expr::lower_logical_with_bindings(
                &expression,
                &schema,
                &Schema::empty(),
                &Default::default(),
            )
            .unwrap();
            let zip = datafusion_physical_expr::create_physical_expr(
                &logical,
                &schema,
                &datafusion_expr::execution_props::ExecutionProps::new(),
                &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
            )
            .unwrap();
            let function = zip.downcast_ref::<ScalarFunctionExpr>().unwrap();
            let mut guarded_arguments = function.args().to_vec();
            guarded_arguments[0] = Arc::new(CastExpr::new(
                guarded_arguments[0].clone(),
                guarded_arguments[0]
                    .data_type(source.schema().as_ref())
                    .unwrap(),
                None,
            ));
            let guarded = zip.clone().with_new_children(guarded_arguments).unwrap();
            assert!(
                zip_arguments(&guarded).is_some(),
                "explicit binding capture supports nontrivial physical refs"
            );
            if position == 0 {
                use std::sync::atomic::{AtomicUsize, Ordering};
                assert_eq!(function.args().len(), 1);
                let scalar_list = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .slice(0, 1);
                for fail in [false, true] {
                    let calls = Arc::new(AtomicUsize::new(0));
                    let observed = calls.clone();
                    let scalar =
                        datafusion_common::ScalarValue::List(Arc::new(scalar_list.clone()));
                    let probe = datafusion_expr::create_udf(
                        "semantic_zip_binding_probe",
                        vec![],
                        batch.schema().field(0).data_type().clone(),
                        datafusion_expr::Volatility::Volatile,
                        Arc::new(move |_| {
                            observed.fetch_add(1, Ordering::SeqCst);
                            if fail {
                                return Err(DataFusionError::Execution(
                                    "binding failed before semantic program".into(),
                                ));
                            }
                            Ok(ColumnarValue::Scalar(scalar.clone()))
                        }),
                    );
                    let probe = Arc::new(
                        ScalarFunctionExpr::try_new(
                            Arc::new(probe),
                            vec![],
                            source.schema().as_ref(),
                            Arc::new(datafusion_common::config::ConfigOptions::default()),
                        )
                        .unwrap(),
                    ) as Arc<dyn PhysicalExpr>;
                    let bound = zip.clone().with_new_children(vec![probe]).unwrap();
                    let program = zip_arguments(&bound)
                        .expect("nontrivial scalar ref must use explicit capture");
                    let ordinary = bound.evaluate(&batch);
                    assert_eq!(calls.swap(0, Ordering::SeqCst), 1);
                    let selected = program.evaluate(&batch, 0);
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    if fail {
                        assert!(ordinary.unwrap_err().to_string().contains("binding failed"));
                        assert!(selected.unwrap_err().to_string().contains("binding failed"));
                    } else {
                        assert!(matches!(
                            ordinary.as_ref().unwrap(),
                            ColumnarValue::Scalar(_)
                        ));
                        assert!(matches!(
                            selected.as_ref().unwrap(),
                            ColumnarValue::Scalar(_)
                        ));
                        assert_eq!(
                            count_list(&ordinary.unwrap().into_array(1).unwrap(), &["1".into()])
                                .unwrap(),
                            count_list(&selected.unwrap().into_array(1).unwrap(), &[]).unwrap()
                        );
                    }
                }
            }
            let projected = Arc::new(
                ProjectionExec::try_new(vec![(zip, "items".into())], source.clone()).unwrap(),
            );
            let expansion = expanded(projected);
            let unnest = expansion
                .downcast_ref::<FilterExec>()
                .unwrap()
                .input()
                .clone();
            let original = Arc::new(
                FilterExec::try_new(
                    Arc::new(IsNotNullExpr::new(field_expression(&unnest, &["1"]))),
                    unnest,
                )
                .unwrap(),
            ) as PhysicalPlan;
            let optimized = optimize(original.clone()).unwrap().unwrap();
            assert!(
                optimized
                    .downcast_ref::<CountListExec>()
                    .unwrap()
                    .program
                    .is_some()
            );
            if position == 0 {
                assert_eq!(rows(original).await.unwrap(), 6);
                assert_eq!(rows(optimized).await.unwrap(), 6);
            } else {
                assert!(rows(original).await.is_err());
                assert!(rows(optimized).await.is_err());
            }
        }
    }
}
