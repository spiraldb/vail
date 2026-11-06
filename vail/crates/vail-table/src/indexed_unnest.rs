//! Fuse an entry update with a narrow, single-list row expansion.
//! Only parent indices cross expansion; the original entry owner stays local
//! to this input batch until the final aligned update has produced its output.
use crate::plan::PhysicalPlan;
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::{filter::filter_record_batch, take::take};
use datafusion_common::{DataFusionError, NullHandling, Result, ScalarValue, config::ConfigOptions};
use datafusion_execution::TaskContext;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use vail_execution::selected_aligned_producer::{PreparedArguments, SelectedProducerProgram};
use datafusion_physical_expr::{
    EquivalenceProperties, PhysicalExpr, ScalarFunctionExpr,
    expressions::{BinaryExpr, Column, IsNotNullExpr, IsNullExpr, Literal},
    scalar_subquery::ScalarSubqueryExpr,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
    filter::FilterExec, projection::ProjectionExec, stream::RecordBatchStreamAdapter,
    unnest::UnnestExec,
};
use futures::future::poll_fn;
use std::{collections::{HashMap, HashSet}, fmt, sync::{Arc,OnceLock}, task::Poll, thread::ThreadId, time::Instant};
use vail_execution::logical_expr::{AlignedSelection, SelectedAlignedProgram, recognize_selected_entry_projection_expr};
use vail_execution::expressions::PreparationOptions;
use crate::early_aligned_product::{BoundRegion, Proof as EarlyProof, Step as EarlyStep};

// Batch-boundary diagnostics only. These spans are elapsed time, not CPU.
fn phase_start(enabled:bool)->Option<(u128,ThreadId)> {
    enabled.then(||(phase_clock(),std::thread::current().id()))
}
fn phase_clock()->u128 {
    static EPOCH:OnceLock<Instant>=OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_micros()
}
fn phase_end(plan:usize,partition:usize,batch:usize,stage:&str,start:Option<(u128,ThreadId)>,rows:usize,event:&mut usize) {
    if let Some((start,thread))=start {
        let end=phase_clock();
        eprintln!("VAIL_FUSED_PHASE plan={plan} partition={partition} batch={batch} event={} phase={stage} start_us={start} end_us={end} rows={rows} thread_start={thread:?} thread_end={:?}",*event,std::thread::current().id());
        *event+=1;
    }
}

#[derive(Debug, Clone)]
enum Step {
    Project {
        expressions: Vec<Arc<dyn PhysicalExpr>>,
        marker_in: usize,
        marker_out: usize,
        schema: SchemaRef,
    },
    Filter {
        predicate: Arc<dyn PhysicalExpr>,
        projection: Option<Vec<usize>>,
        schema: SchemaRef,
    },
}
#[derive(Debug)]
struct OriginalProducer {
    function: ScalarUDF,
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    expression: Arc<dyn PhysicalExpr>,
}
#[derive(Debug)]
struct SelectedProducer {
    source_arguments: Vec<Arc<dyn PhysicalExpr>>,
    fallback: ProducerFallback,
    program: SelectedProducerProgram,
    scratch_bytes: usize,
    trace: bool,
}

/// The ordinary producer, bound to the same already evaluated arguments used
/// by selected execution. Only its remaining projection runs after fallback.
#[derive(Debug)]
struct ProducerFallback {
    function: ScalarUDF,
    argument_fields: Vec<Arc<Field>>,
    return_field: Arc<Field>,
    config: Arc<ConfigOptions>,
    schema: SchemaRef,
    output: Arc<dyn PhysicalExpr>,
}
impl ProducerFallback {
    fn prepare(
        original: &OriginalProducer,
        producer: &Arc<dyn PhysicalExpr>,
        source_schema: &SchemaRef,
    ) -> Result<Option<Self>> {
        let function = original.expression.downcast_ref::<ScalarFunctionExpr>().unwrap();
        let return_field = function.return_field(source_schema)?;
        let names = source_schema.fields().iter().map(|field| field.name().as_str())
            .collect::<HashSet<_>>();
        let name = (0..=names.len()).map(|suffix| format!("__vail_producer_result_{suffix}"))
            .find(|candidate| !names.contains(candidate.as_str())).unwrap();
        let replacement: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new(&name, source_schema.fields().len()));
        let output = replace(producer, &original.expression, &replacement, &mut HashMap::new())?;
        if Arc::ptr_eq(&output, producer) {
            return Ok(None);
        }
        let mut fields = source_schema.fields().to_vec();
        fields.push(Arc::new(return_field.as_ref().clone().with_name(name)));
        Ok(Some(Self {
            function: original.function.clone(),
            argument_fields: original.arguments.iter()
                .map(|argument| argument.return_field(source_schema))
                .collect::<Result<_>>()?,
            return_field,
            config: Arc::new(function.config_options().clone()),
            schema: Arc::new(Schema::new_with_metadata(fields, source_schema.metadata().clone())),
            output,
        }))
    }

    fn evaluate(&self, raw: &RecordBatch, arguments: Vec<ColumnarValue>) -> Result<ArrayRef> {
        let value = self.function.invoke_with_args(ScalarFunctionArgs {
            args: arguments,
            arg_fields: self.argument_fields.clone(),
            number_rows: raw.num_rows(),
            return_field: self.return_field.clone(),
            config_options: self.config.clone(),
        })?.into_array(raw.num_rows())?;
        if value.len() != raw.num_rows() || value.data_type() != self.return_field.data_type() {
            return Err(DataFusionError::Execution("original producer result descriptor differs".into()));
        }
        let mut columns = raw.columns().to_vec();
        columns.push(value);
        let bound = RecordBatch::try_new(self.schema.clone(), columns)?;
        self.output.evaluate(&bound)?.into_array(raw.num_rows())
    }
}
#[derive(Debug)]
struct Program {
    source_schema: SchemaRef,
    output_schema: SchemaRef,
    restored_schema: SchemaRef,
    unnest_schema: SchemaRef,
    source_entry: usize,
    source_list: usize,
    marker: usize,
    steps: Vec<Step>,
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    selected: SelectedAlignedProgram,
    output: Vec<Arc<dyn PhysicalExpr>>,
    input_marker_schema: SchemaRef,
    raw_entry: usize,
    before: Vec<Step>,
    producer: Option<Arc<dyn PhysicalExpr>>,
    selected_producer: Option<SelectedProducer>,
}

fn marker_schema(schema: &SchemaRef, marker: usize, dtype: DataType, nullable: bool) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields[marker] = Arc::new(Field::new(fields[marker].name(), dtype, nullable));
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

// A physical projection may prove a logical nullable field non-null. Keep all
// other field/type/metadata bindings exact, including nested nullability.
fn refines_frame(actual: &Schema, expected: &Schema) -> bool {
    actual == expected || (actual.metadata() == expected.metadata()
        && actual.fields().len() == expected.fields().len()
        && actual.fields().iter().zip(expected.fields()).all(|(actual, expected)| {
            actual == expected || (!actual.is_nullable() && expected.is_nullable()
                && &actual.as_ref().clone().with_nullable(true) == expected.as_ref())
        }))
}

fn trace_refined_frame(stage: &str, actual: &Schema, expected: &Schema) {
    if actual != expected && std::env::var_os("VAIL_TRACE_EARLY_ALIGNED_PRODUCT").is_some() {
        for (actual, expected) in actual.fields().iter().zip(expected.fields()) {
            if actual != expected {
                eprintln!("VAIL_EARLY_NULLABILITY_REFINEMENT stage={stage} field={} actual_nullable={} expected_nullable={}",
                    expected.name(), actual.is_nullable(), expected.is_nullable());
            }
        }
    }
}
type Dependencies = HashMap<(usize, usize, usize), bool>;
fn depends(
    expression: &Arc<dyn PhysicalExpr>,
    index: usize,
    target: Option<&Arc<dyn PhysicalExpr>>,
    memo: &mut Dependencies,
) -> bool {
    let address = Arc::as_ptr(expression) as *const () as usize;
    let ignored = target.map_or(0, |target| Arc::as_ptr(target) as *const () as usize);
    let key = (address, index, ignored);
    if let Some(answer) = memo.get(&key) {
        return *answer;
    }
    let answer = if target.is_some_and(|target| Arc::ptr_eq(expression, target)) {
        false
    } else if let Some(column) = expression.downcast_ref::<Column>() {
        column.index() == index
    } else {
        expression
            .children()
            .iter()
            .any(|child| depends(child, index, target, memo))
    };
    memo.insert(key, answer);
    answer
}
type MetadataMemo = HashMap<(usize, usize, usize), bool>;
fn total_metadata(
    expression: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    target: Option<&Arc<dyn PhysicalExpr>>,
    memo: &mut MetadataMemo,
) -> bool {
    let key = (
        Arc::as_ptr(expression) as *const () as usize,
        schema as *const Schema as usize,
        target.map_or(0, |target| Arc::as_ptr(target) as *const () as usize),
    );
    if let Some(answer) = memo.get(&key) {
        return *answer;
    }
    let result = total_metadata_node(expression, schema, target, memo);
    memo.insert(key, result);
    result
}
fn total_metadata_node(
    expression: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    target: Option<&Arc<dyn PhysicalExpr>>,
    memo: &mut MetadataMemo,
) -> bool {
    if target.is_some_and(|target| Arc::ptr_eq(expression, target)) {
        return true;
    }
    let expression = crate::projection_broadcast::count_inner(expression);
    if target.is_some_and(|target| Arc::ptr_eq(&expression, target)) {
        return true;
    }
    // ScalarSubqueryExec produces these query-scoped values before its child
    // executes. The deferred expression remains visible to dependency visitors.
    if expression.is::<ScalarSubqueryExpr>() {
        return true;
    }
    if let Some(function) = expression.downcast_ref::<ScalarFunctionExpr>() {
        let typed = vail_execution::logical_expr::semantic_kernel(function.fun())
            .is_some_and(|(expr, _)| expr.is_pure());
        let named = function
            .fun()
            .inner()
            .is::<datafusion_functions::core::named_struct::NamedStructFunc>()
            && function.args().len() % 2 == 0
            && function.args().iter().step_by(2).all(|name| {
                name.downcast_ref::<Literal>()
                    .is_some_and(|value| matches!(value.value(), ScalarValue::Utf8(Some(_))))
            });
        let length = vail_execution::array_length::is_outer_length(function.fun())
            && function
                .args()
                .first()
                .is_some_and(|arg| matches!(arg.data_type(schema), Ok(DataType::List(_))));
        if !typed && !named && !length {
            return false;
        }
    } else if let Some(binary) = expression.downcast_ref::<BinaryExpr>() {
        use datafusion_expr::Operator;
        if !matches!(
            binary.op(),
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::And
                | Operator::Or
        ) {
            return false;
        }
    } else if !expression.is::<Column>()
        && !expression.is::<Literal>()
        && !expression.is::<IsNullExpr>()
        && !expression.is::<IsNotNullExpr>()
    {
        return false;
    }
    expression
        .children()
        .iter()
        .all(|child| total_metadata(child, schema, target, memo))
}
fn replace(
    expression: &Arc<dyn PhysicalExpr>,
    target: &Arc<dyn PhysicalExpr>,
    value: &Arc<dyn PhysicalExpr>,
    memo: &mut HashMap<usize, Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let address = Arc::as_ptr(expression) as *const () as usize;
    if let Some(answer) = memo.get(&address) {
        return Ok(answer.clone());
    }
    let answer = if Arc::ptr_eq(expression, target) {
        value.clone()
    } else {
        let children = expression
            .children()
            .into_iter()
            .map(|child| replace(child, target, value, memo))
            .collect::<Result<Vec<_>>>()?;
        if children
            .iter()
            .zip(expression.children())
            .all(|(a, b)| Arc::ptr_eq(a, b))
        {
            expression.clone()
        } else {
            expression.clone().with_new_children(children)?
        }
    };
    memo.insert(address, answer.clone());
    Ok(answer)
}

// Projections preserve the original row domain. The marker therefore remains
// an ordinal into the same raw input batch, even after metadata filters.
fn deferred_input(
    mut input: PhysicalPlan,
    mut marker: usize,
    projected: bool,
) -> Result<Option<(PhysicalPlan, usize, Vec<Step>, Arc<dyn PhysicalExpr>, OriginalProducer)>> {
    let mut before = Vec::new();
    let mut dependencies = Dependencies::new();
    let mut metadata = MetadataMemo::new();
    while let Some(project) = input.downcast_ref::<ProjectionExec>() {
        let Some(entry) = project.expr().get(marker) else {
            return Ok(None);
        };
        let expression = crate::projection_broadcast::count_inner(&entry.expr);
        let mut producer = false;
        let mut original = None;
        let incoming = if let Some(column) = expression.downcast_ref::<Column>() {
            column.index()
        } else {
            let Some(function) = expression.downcast_ref::<ScalarFunctionExpr>() else {
                return Ok(None);
            };
            let aligned = if vail_execution::logical_expr::deferrable_aligned_entry_argument(
                function.fun(),
            )
            .is_some()
            {
                expression.clone()
            } else {
                let Some((native, schema)) =
                    vail_execution::logical_expr::semantic_kernel(function.fun())
                else {
                    return Ok(None);
                };
                if !native.is_pure() || function.args().len() != 1 || schema.fields().len() != 1 {
                    return Ok(None);
                }
                let vail_execution::expressions::Expr::ArrayMap { value, .. } = native else {
                    return Ok(None);
                };
                if !matches!(value.as_ref(), vail_execution::expressions::Expr::Column { name } if schema.index_of(name).ok()==Some(0))
                {
                    return Ok(None);
                }
                crate::projection_broadcast::count_inner(&function.args()[0])
            };
            let Some(function) = aligned.downcast_ref::<ScalarFunctionExpr>() else {
                return Ok(None);
            };
            let Some(argument) =
                vail_execution::logical_expr::deferrable_aligned_entry_argument(function.fun())
            else {
                return Ok(None);
            };
            let Some(argument_expr) = function.args().get(argument) else {
                return Ok(None);
            };
            let argument_expr = crate::projection_broadcast::count_inner(argument_expr);
            let Some(column) = argument_expr.downcast_ref::<Column>() else {
                return Ok(None);
            };
            let index = column.index();
            if function.args().iter().enumerate().any(|(i, e)| {
                i != argument
                    && (depends(e, index, None, &mut dependencies)
                        || !total_metadata(
                            e,
                            project.input().schema().as_ref(),
                            None,
                            &mut metadata,
                        ))
            }) {
                return Ok(None);
            }
            producer = true;
            original = Some(OriginalProducer { function: function.fun().clone(), arguments: function.args().to_vec(), expression: aligned.clone() });
            index
        };
        if project.expr().iter().enumerate().any(|(i, e)| {
            i != marker
                && (depends(&e.expr, incoming, None, &mut dependencies)
                    || !total_metadata(
                        &e.expr,
                        project.input().schema().as_ref(),
                        None,
                        &mut metadata,
                    ))
        }) {
            return Ok(None);
        }
        before.push(Step::Project {
            expressions: project.expr().iter().map(|e| e.expr.clone()).collect(),
            marker_in: incoming,
            marker_out: marker,
            schema: marker_schema(&project.schema(), marker, DataType::Int32, false),
        });
        if producer {
            let entry = entry.expr.clone();
            let entry=if projected {
                crate::projected_aligned::prepare(entry.clone()).unwrap_or(entry)
            }else{entry};
            let raw = project.input().clone();
            if !matches!(raw.schema().field(incoming).data_type(), DataType::List(_)) {
                return Ok(None);
            }
            before.reverse();
            return Ok(Some((raw, incoming, before, entry, original.unwrap())));
        }
        marker = incoming;
        input = project.input().clone();
    }
    Ok(None)
}

type UnnestTails = HashMap<usize, (PhysicalPlan, bool)>;
fn has_unnest_tail(plan: &PhysicalPlan, memo: &mut UnnestTails) -> bool {
    let key = Arc::as_ptr(plan) as *const () as usize;
    if let Some((_, answer)) = memo.get(&key) {
        return *answer;
    }
    let answer = if plan.is::<UnnestExec>() {
        true
    } else if let Some(project) = plan.downcast_ref::<ProjectionExec>() {
        has_unnest_tail(project.input(), memo)
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        has_unnest_tail(filter.input(), memo)
    } else {
        false
    };
    memo.insert(key, (plan.clone(), answer));
    answer
}
fn candidate(
    projection: &ProjectionExec,
    tails: &mut UnnestTails,
    defer: bool,
    projected: bool,
) -> Result<Option<(PhysicalPlan, Program)>> {
    // Reject unrelated maps before preparing a selected entry evaluator.
    if !has_unnest_tail(projection.input(), tails) {
        return Ok(None);
    }
    let mut found = None;
    let mut dependencies = Dependencies::new();
    let mut metadata = MetadataMemo::new();
    for entry in projection.expr() {
        let target = crate::projection_broadcast::count_inner(&entry.expr);
        let Some(outer) = target.downcast_ref::<ScalarFunctionExpr>() else {
            continue;
        };
        if outer.args().len() != 1 {
            continue;
        }
        let inner = crate::projection_broadcast::count_inner(&outer.args()[0]);
        let Some(inner) = inner.downcast_ref::<ScalarFunctionExpr>() else {
            continue;
        };
        let Some(selected) =
            vail_execution::logical_expr::recognize_selected_entry_projection(inner.fun(), outer.fun())
        else {
            continue;
        };
        let Some(argument) = inner.args().get(selected.entry_argument()) else {
            continue;
        };
        let argument = crate::projection_broadcast::count_inner(argument);
        let Some(column) = argument.downcast_ref::<Column>() else {
            continue;
        };
        let marker = column.index();
        if inner.args().iter().enumerate().any(|(i, arg)| {
            i != selected.entry_argument() && depends(arg, marker, None, &mut dependencies)
        }) {
            continue;
        }
        if found.is_some() {
            return Ok(None);
        }
        found = Some((target.clone(), selected, inner.args().to_vec(), marker));
    }
    let Some((target, selected, arguments, final_marker)) = found else {
        return Ok(None);
    };
    if projection.expr().iter().any(|entry| {
        depends(&entry.expr, final_marker, Some(&target), &mut dependencies)
            || !total_metadata(
                &entry.expr,
                projection.input().schema().as_ref(),
                Some(&target),
                &mut metadata,
            )
    }) {
        return Ok(None);
    }
    let mut current = projection.input().clone();
    let mut marker = final_marker;
    let mut steps = vec![];
    loop {
        if let Some(project) = current.downcast_ref::<ProjectionExec>() {
            let Some(forward) = project.expr().get(marker) else {
                return Ok(None);
            };
            let forward = crate::projection_broadcast::count_inner(&forward.expr);
            let Some(column) = forward.downcast_ref::<Column>() else {
                return Ok(None);
            };
            let marker_in = column.index();
            if project.expr().iter().enumerate().any(|(i, e)| {
                i != marker
                    && (depends(&e.expr, marker_in, None, &mut dependencies)
                        || !total_metadata(
                            &e.expr,
                            project.input().schema().as_ref(),
                            None,
                            &mut metadata,
                        ))
            }) {
                return Ok(None);
            }
            steps.push(Step::Project {
                expressions: project.expr().iter().map(|e| e.expr.clone()).collect(),
                marker_in,
                marker_out: marker,
                schema: marker_schema(&project.schema(), marker, DataType::Int32, false),
            });
            marker = marker_in;
            current = project.input().clone();
        } else if let Some(filter) = current.downcast_ref::<FilterExec>() {
            if filter.fetch().is_some() {
                return Ok(None);
            }
            let projection = filter
                .projection()
                .as_ref()
                .map(|indices| indices.as_ref().to_vec());
            let marker_in = projection
                .as_ref()
                .map_or(Some(marker), |indices: &Vec<usize>| {
                    indices.get(marker).copied()
                });
            let Some(marker_in) = marker_in else {
                return Ok(None);
            };
            if depends(filter.predicate(), marker_in, None, &mut dependencies)
                || !total_metadata(
                    filter.predicate(),
                    filter.input().schema().as_ref(),
                    None,
                    &mut metadata,
                )
            {
                return Ok(None);
            }
            if projection
                .as_ref()
                .is_some_and(|p| p.iter().filter(|&&index| index == marker_in).count() != 1)
            {
                return Ok(None);
            }
            steps.push(Step::Filter {
                predicate: filter.predicate().clone(),
                projection,
                schema: marker_schema(&filter.schema(), marker, DataType::Int32, false),
            });
            marker = marker_in;
            current = filter.input().clone();
        } else {
            break;
        }
    }
    let Some(unnest) = current.downcast_ref::<UnnestExec>() else {
        return Ok(None);
    };
    let [list] = unnest.list_column_indices() else {
        return Ok(None);
    };
    if list.depth != 1
        || !unnest.struct_column_indices().is_empty()
        || unnest.options().null_handling != NullHandling::Drop
        || unnest.options().recursions.iter().any(|r| r.depth != 1)
        || marker == list.index_in_input_schema
        || unnest.schema().fields().len() != unnest.input().schema().fields().len()
        || unnest.input().schema().field(marker).data_type() != selected.entry_type()
        || !matches!(
            unnest
                .input()
                .schema()
                .field(list.index_in_input_schema)
                .data_type(),
            DataType::List(_)
        )
    {
        return Ok(None);
    }
    let Some(selected) = selected.prepare() else { return Ok(None); };
    steps.reverse();
    let replacement: Arc<dyn PhysicalExpr> = Arc::new(Column::new(
        projection.input().schema().field(final_marker).name(),
        final_marker,
    ));
    let mut replacements = HashMap::new();
    let output = projection
        .expr()
        .iter()
        .map(|entry| replace(&entry.expr, &target, &replacement, &mut replacements))
        .collect::<Result<Vec<_>>>()?;
    let (input, raw_entry, before, producer) = if defer {
        match deferred_input(unnest.input().clone(), marker, projected)? {
            Some((input, entry, before, producer, _)) => (input, entry, before, Some(producer)),
            None => (unnest.input().clone(), marker, vec![], None),
        }
    } else {
        (unnest.input().clone(), marker, vec![], None)
    };
    let input_marker_schema = marker_schema(&input.schema(), raw_entry, DataType::Int32, false);
    Ok(Some((
        input.clone(),
        Program {
            source_schema: input.schema(),
            input_marker_schema,
            raw_entry,
            before,
            producer,
            selected_producer: None,
            output_schema: projection.schema(),
            restored_schema: marker_schema(
                &projection.input().schema(),
                final_marker,
                selected.output_type().clone(),
                true,
            ),
            unnest_schema: marker_schema(&unnest.schema(), marker, DataType::Int32, false),
            source_entry: marker,
            source_list: list.index_in_input_schema,
            marker: final_marker,
            steps,
            arguments,
            selected,
            output,
        },
    )))
}

// Refuse the endpoint when the current bounded scheduler would split one
// original parent's choices. The original producer remains the old fallback.
fn single_chunk_parent_groups(lists:&ListArray,parents:&Int32Array,lengths:&[usize],row_cap:usize,lane_cap:usize)->bool {
    let mut rows=0usize;let mut lanes=0usize;
    for row in 0..lists.len() {
        if lists.is_null(row){continue;}
        let count=lists.value_length(row) as usize;
        if count==0{continue;}
        let Some(parent)=parents.value(row).try_into().ok().and_then(|parent:usize|lengths.get(parent)) else{return false;};
        let Some(group_lanes)=count.checked_mul(*parent) else{return false;};
        if count>row_cap || group_lanes>lane_cap{return false;}
        let available=(row_cap-rows).min(if *parent==0{usize::MAX}else{(lane_cap-lanes)/ *parent});
        if count>available {
            if available>0{return false;}
            rows=0;lanes=0;
        }
        rows+=count;lanes+=group_lanes;
    }
    true
}

impl Program {
    fn chunk(
        &self,
        input: &RecordBatch,
        parents: Vec<i32>,
        items: Vec<i32>,
    ) -> Result<Option<RecordBatch>> {
        let parents = Int32Array::from(parents);
        let items = Int32Array::from(items);
        let list = input
            .column(self.source_list)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let columns = input
            .columns()
            .iter()
            .enumerate()
            .map(|(index, array)| {
                if index == self.source_list {
                    Ok(take(list.values().as_ref(), &items, None)?)
                } else {
                    Ok(take(array.as_ref(), &parents, None)?)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let mut batch = RecordBatch::try_new(self.unnest_schema.clone(), columns)?;
        batch = self.apply_steps(batch, &self.steps)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(batch))
    }
    fn apply_steps(&self, mut batch: RecordBatch, steps: &[Step]) -> Result<RecordBatch> {
        for step in steps {
            if batch.num_rows() == 0 {
                return Ok(batch);
            }
            batch = match step {
                Step::Project {
                    expressions,
                    marker_in,
                    marker_out,
                    schema,
                } => {
                    let columns = expressions
                        .iter()
                        .enumerate()
                        .map(|(i, expression)| {
                            if i == *marker_out {
                                Ok(batch.column(*marker_in).clone())
                            } else {
                                expression.evaluate(&batch)?.into_array(batch.num_rows())
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    RecordBatch::try_new(schema.clone(), columns)?
                }
                Step::Filter {
                    predicate,
                    projection,
                    schema,
                } => {
                    let predicate = predicate.evaluate(&batch)?.into_array(batch.num_rows())?;
                    let predicate = predicate
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("fused filter is not boolean".into())
                        })?;
                    let filtered = filter_record_batch(&batch, predicate)?;
                    let filtered = match projection {
                        Some(indices) => filtered.project(indices)?,
                        None => filtered,
                    };
                    RecordBatch::try_new(schema.clone(), filtered.columns().to_vec())?
                }
            };
        }
        Ok(batch)
    }
    fn metadata_input(&self, input: &RecordBatch) -> Result<RecordBatch> {
        let count = i32::try_from(input.num_rows())
            .map_err(|_| DataFusionError::Execution("raw row count exceeds int32".into()))?;
        let mut columns = input.columns().to_vec();
        columns[self.raw_entry] = Arc::new(Int32Array::from_iter_values(0..count));
        self.apply_steps(
            RecordBatch::try_new(self.input_marker_schema.clone(), columns)?,
            &self.before,
        )
    }
    fn finish(&self, batch: RecordBatch, owner: ArrayRef, trace: bool) -> Result<RecordBatch> {
        let parents = batch
            .column(self.marker)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| DataFusionError::Internal("fused entry parent marker changed".into()))?
            .clone();
        let arguments = self
            .arguments
            .iter()
            .enumerate()
            .map(|(i, argument)| {
                if i == self.selected.entry_argument {
                    Ok(ColumnarValue::Scalar(ScalarValue::Null))
                } else {
                    argument.evaluate(&batch)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let mapped = self
            .selected
            .invoke(
                &arguments,
                AlignedSelection {
                    entries: owner,
                    parents,
                },
            )?
            .into_array(batch.num_rows())?;
        if trace {
            let output = mapped.as_any().downcast_ref::<ListArray>().unwrap();
            eprintln!(
                "VAIL_FUSED_UNNEST_BODY rows={} lanes={}",
                batch.num_rows(),
                output.values().len()
            );
        }
        self.finish_mapped(batch, mapped)
    }
    fn finish_selected(&self, batch: RecordBatch, owner: ArrayRef, original: &PreparedArguments, scratch_bytes: usize) -> Result<Option<RecordBatch>> {
        let selected = self.selected_producer.as_ref().unwrap();
        let parents = batch.column(self.marker).as_any().downcast_ref::<Int32Array>()
            .ok_or_else(|| DataFusionError::Internal("selected parent marker changed".into()))?.clone();
        let arguments = self.arguments.iter().enumerate().map(|(i, argument)| if i == self.selected.entry_argument {
            Ok(ColumnarValue::Scalar(ScalarValue::Null))
        } else { argument.evaluate(&batch) }).collect::<Result<Vec<_>>>()?;
        let Some(mapped) = selected.program.invoke(&arguments, original, AlignedSelection { entries: owner, parents }, scratch_bytes)? else { return Ok(None); };
        let mapped = mapped.into_array(batch.num_rows())?;
        self.finish_mapped(batch, mapped).map(Some)
    }
    fn finish_mapped(&self, batch: RecordBatch, mapped: ArrayRef) -> Result<RecordBatch> {
        let mut columns = batch.columns().to_vec();
        columns[self.marker] = mapped;
        let batch = RecordBatch::try_new(self.restored_schema.clone(), columns)?;
        let columns = self
            .output
            .iter()
            .map(|expression| expression.evaluate(&batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }
}
#[derive(Debug, Clone)]
struct EarlyRebuild {
    proof: Arc<EarlyProof>,
    options: PreparationOptions,
    regions: Vec<BoundRegion>,
    trace: bool,
}

#[derive(Debug)]
struct IndexedUnnestExec {
    input: PhysicalPlan,
    template: PhysicalPlan,
    program: Arc<Program>,
    cache: Arc<PlanProperties>,
    early: Option<EarlyRebuild>,
}
impl DisplayAs for IndexedUnnestExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        if self.early.is_some() {
            write!(f, "EarlyAlignedProductExec")
        } else {
            write!(f, "IndexedUnnestEntriesExec: projected_producer={}",self.program.producer.as_ref().is_some_and(crate::projected_aligned::is_projected))
        }
    }
}
impl ExecutionPlan for IndexedUnnestExec {
    fn name(&self) -> &'static str {
        "IndexedUnnestEntriesExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
        if children.len() != 1 || children[0].schema() != self.program.source_schema {
            return Err(DataFusionError::Plan(
                "indexed expansion source schema changed".into(),
            ));
        }
        if let Some(early) = &self.early {
            let unchanged = Arc::ptr_eq(&self.input, &children[0]);
            if early.trace {
                eprintln!("VAIL_EARLY_ALIGNED_PRODUCT_REBUILD reused_program=true unchanged_child={unchanged}");
            }
            if unchanged { return Ok(self); }
            // Every input field, including metadata and marker positions, was
            // checked above. This immutable program binds fresh owners during
            // each invocation; it has no dependency on child identity/order.
            let cache = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.program.output_schema.clone()),
                Partitioning::UnknownPartitioning(children[0].properties().partitioning.partition_count()),
                children[0].properties().emission_type,
                children[0].properties().boundedness,
            ));
            return Ok(Arc::new(Self {
                input: children[0].clone(), template: children[0].clone(),
                program: self.program.clone(), cache, early: Some(early.clone()),
            }));
        }
        fn rebuild(
            plan: &PhysicalPlan,
            source: &PhysicalPlan,
            replacement: &PhysicalPlan,
            memo: &mut HashMap<usize, PhysicalPlan>,
        ) -> Result<PhysicalPlan> {
            if Arc::ptr_eq(plan, source) {
                return Ok(replacement.clone());
            }
            let key = Arc::as_ptr(plan) as *const () as usize;
            if let Some(answer) = memo.get(&key) {
                return Ok(answer.clone());
            }
            let children = plan
                .children()
                .iter()
                .map(|child| rebuild(child, source, replacement, memo))
                .collect::<Result<Vec<_>>>()?;
            let result = plan.clone().with_new_children(children)?;
            memo.insert(key, result.clone());
            Ok(result)
        }
        let template = rebuild(
            &self.template,
            &self.input,
            &children[0],
            &mut HashMap::new(),
        )?;
        let cache = template.properties().clone();
        Ok(Arc::new(Self {
            input: children[0].clone(),
            template,
            program: self.program.clone(),
            cache,
            early: None,
        }))
    }
    fn apply_expressions(
        &self,
        visitor: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        let mut expressions = self
            .program
            .arguments
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.program.selected.entry_argument)
            .map(|(_, expr)| expr)
            .collect::<Vec<_>>();
        if let Some(producer) = &self.program.producer {
            expressions.push(producer);
        }
        for step in self.program.before.iter().chain(&self.program.steps) {
            match step {
                Step::Project {
                    expressions: values,
                    marker_out,
                    ..
                } => expressions.extend(
                    values
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| i != marker_out)
                        .map(|(_, value)| value),
                ),
                Step::Filter { predicate, .. } => expressions.push(predicate),
            }
        }
        expressions.extend(self.program.output.iter());
        datafusion_physical_plan::apply_expression_roots(expressions, visitor)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context.clone())?;
        let program = self.program.clone();
        let schema = program.output_schema.clone();
        let batch_size = context.session_config().batch_size().max(1);
        // Flat entry lanes are a different domain from outer DataFusion rows.
        let entry_batch_size = std::env::var("VAIL_EXPERIMENT_ENTRY_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(262_144);
        let trace = std::env::var_os("VAIL_TRACE_FUSED_UNNEST").is_some();
        let phase_trace=std::env::var_os("VAIL_TRACE_FUSED_PHASES").is_some();
        let plan_id=Arc::as_ptr(&program) as usize;
        if trace {
            eprintln!("VAIL_FUSED_UNNEST_EXEC partition={partition}");
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            async_stream::try_stream! {
                let mut event=0usize;
                let mut batch_id=0usize;
                let stream_started=phase_start(phase_trace);
                loop {
                    let upstream_started=phase_start(phase_trace);
                    let next=poll_fn(|cx| {
                        let started=phase_start(phase_trace);
                        let result=input.as_mut().poll_next(cx);
                        let (stage,rows)=match &result {
                            Poll::Pending=>("upstream_poll_pending",0),
                            Poll::Ready(None)=>("upstream_poll_eof",0),
                            Poll::Ready(Some(Err(_)))=>("upstream_poll_error",0),
                            Poll::Ready(Some(Ok(batch)))=>("upstream_poll_ready",batch.num_rows()),
                        };
                        phase_end(plan_id,partition,batch_id,stage,started,rows,&mut event);
                        result
                    }).await;
                    let rows=next.as_ref().and_then(|result|result.as_ref().ok()).map_or(0,RecordBatch::num_rows);
                    phase_end(plan_id,partition,batch_id,"upstream_interval",upstream_started,rows,&mut event);
                    let Some(batch)=next else {
                        phase_end(plan_id,partition,batch_id,"stream_complete",stream_started,0,&mut event);
                        break;
                    };
                    let batch=batch?;
                    let batch_started=phase_start(phase_trace);
                    if batch.schema() != program.source_schema { Err(DataFusionError::Execution("indexed expansion input schema changed".into()))?; }
                    let entries = batch.column(program.raw_entry).as_any().downcast_ref::<ListArray>().unwrap();
                    let lengths = (0..entries.len()).map(|row| if entries.is_null(row) { 0 } else { entries.value_length(row) as usize }).collect::<Vec<_>>();
                    if trace { eprintln!("VAIL_FUSED_UNNEST_OWNER rows={} entries={} bytes={} deferred={}",batch.num_rows(),entries.values().len(),entries.get_buffer_memory_size(),program.producer.is_some()); }
                    let metadata_started=phase_start(phase_trace);
                    let metadata = program.metadata_input(&batch)?;
                    phase_end(plan_id,partition,batch_id,"metadata",metadata_started,metadata.num_rows(),&mut event);
                    let lists = metadata.column(program.source_list).as_any().downcast_ref::<ListArray>().unwrap();
                    let raw_parents = metadata.column(program.source_entry).as_any().downcast_ref::<Int32Array>().unwrap();
                    let selected_budget=program.selected_producer.as_ref().and_then(|endpoint| {
                        if !single_chunk_parent_groups(lists,raw_parents,&lengths,batch_size,entry_batch_size){return None;}
                        endpoint.program.argument_budget(entries,endpoint.scratch_bytes)
                    });
                    if let (Some(endpoint),Some(remaining))=(&program.selected_producer,selected_budget) {
                        if endpoint.trace {eprintln!("VAIL_SELECTED_PRODUCER_BUDGET rows={} limit={} argument_reservation={} native_remaining={}",batch.num_rows(),endpoint.scratch_bytes,endpoint.scratch_bytes-remaining,remaining);}
                    }
                    let mut selected_fallback=program.selected_producer.is_some()&&selected_budget.is_none();
                    if selected_fallback && program.selected_producer.as_ref().unwrap().trace {eprintln!("VAIL_SELECTED_PRODUCER_RESOURCE_FALLBACK rows={}",batch.num_rows());}
                    let mut owner = if program.producer.is_none() { Some(batch.column(program.raw_entry).clone()) } else { None };
                    let mut pending = Some(batch);
                    let mut selected_arguments: Option<PreparedArguments> = None;
                    let mut original_arguments: Option<Vec<ColumnarValue>> = None;
                    let mut parents = Vec::new(); let mut items = Vec::new(); let mut lanes = 0usize;
                    for row in 0..=metadata.num_rows() {
                        let range = if row==metadata.num_rows() { 0..1 } else if lists.is_null(row) { 0..0 } else { lists.value_offsets()[row]..lists.value_offsets()[row+1] };
                        for item in range {
                            let end = row==metadata.num_rows();
                            let entry_count = if end { 0 } else { lengths[raw_parents.value(row) as usize] };
                            if !parents.is_empty() && (end || parents.len() >= batch_size || lanes.saturating_add(entry_count) > entry_batch_size) {
                                let chunk_started=phase_start(phase_trace);
                                let selected=program.chunk(&metadata,std::mem::take(&mut parents),std::mem::take(&mut items))?;
                                phase_end(plan_id,partition,batch_id,"chunk_filter",chunk_started,selected.as_ref().map_or(0,RecordBatch::num_rows),&mut event);
                                if let Some(selected) = selected {
                                    let finish_started=phase_start(phase_trace);
                                    let mut selected_output = None;
                                    if let Some(endpoint) = &program.selected_producer {
                                        if !selected_fallback {
                                            let raw = pending.as_ref().unwrap();
                                            if selected_arguments.is_none() {
                                                let arguments = endpoint.source_arguments.iter().map(|argument| argument.evaluate(raw)).collect::<Result<Vec<_>>>()?;
                                                let entries = raw.column(program.raw_entry).as_any().downcast_ref::<ListArray>().unwrap();
                                                selected_arguments = Some(endpoint.program.arguments(&arguments, entries)?);
                                                original_arguments = Some(arguments);
                                                if endpoint.trace { eprintln!("VAIL_SELECTED_PRODUCER_ARG_EVAL rows={}", raw.num_rows()); }
                                            }
                                            selected_output = program.finish_selected(selected.clone(), raw.column(program.raw_entry).clone(), selected_arguments.as_ref().unwrap(), selected_budget.unwrap())?;
                                            if selected_output.is_none() {
                                                selected_fallback = true;
                                                if endpoint.trace { eprintln!("VAIL_SELECTED_PRODUCER_FALLBACK rows={}", raw.num_rows()); }
                                            }
                                        }
                                    }
                                    let output = if let Some(output) = selected_output { output } else {
                                    if owner.is_none() {
                                        let raw = pending.take().unwrap();
                                        let producer_started=phase_start(phase_trace);
                                        let prepared = if let Some(arguments) = original_arguments.take() {
                                            if program.selected_producer.as_ref().unwrap().trace {
                                                eprintln!("VAIL_SELECTED_PRODUCER_REUSE_ARGUMENTS rows={}", raw.num_rows());
                                            }
                                            program.selected_producer.as_ref().unwrap().fallback.evaluate(&raw, arguments)?
                                        } else {
                                            program.producer.as_ref().unwrap().evaluate(&raw)?.into_array(raw.num_rows())?
                                        };
                                        selected_arguments = None;
                                        phase_end(plan_id,partition,batch_id,"producer",producer_started,raw.num_rows(),&mut event);
                                        if prepared.len()!=raw.num_rows() || prepared.data_type()!=program.selected.entry_type() { Err(DataFusionError::Execution("deferred entry shape or descriptor differs".into()))?; }
                                        if trace { eprintln!("VAIL_FUSED_PRODUCER_EVAL rows={}",raw.num_rows()); }
                                        owner = Some(prepared);
                                    }
                                    program.finish(selected,owner.as_ref().unwrap().clone(),trace)?
                                    };
                                    let rows=output.num_rows();
                                    phase_end(plan_id,partition,batch_id,"finish",finish_started,rows,&mut event);
                                    let suspended=phase_start(phase_trace);
                                    yield output;
                                    phase_end(plan_id,partition,batch_id,"yield_suspend",suspended,rows,&mut event);
                                }
                                lanes = 0;
                            }
                            if !end {
                                parents.push(i32::try_from(row).map_err(|_| DataFusionError::Execution("expanded parent exceeds int32".into()))?); items.push(item);
                                lanes = lanes.checked_add(entry_count).ok_or_else(|| DataFusionError::Execution("expanded lane count overflow".into()))?;
                            }
                        }
                    }
                    phase_end(plan_id,partition,batch_id,"batch_total",batch_started,metadata.num_rows(),&mut event);
                    batch_id+=1;
                }
            },
        )))
    }
}

pub(crate) fn optimize(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    if std::env::var_os("VAIL_EXPERIMENT_FUSED_UNNEST").is_none() {
        return Ok(plan);
    }
    let options = PreparationOptions::capture();
    let defer = options.defer_aligned_producer();
    let projected = options.projected_producer();
    fn visit(
        plan: PhysicalPlan,
        memo: &mut HashMap<usize, (PhysicalPlan, PhysicalPlan)>,
        tails: &mut UnnestTails,
        defer: bool,
        projected: bool,
    ) -> Result<PhysicalPlan> {
        let key = Arc::as_ptr(&plan) as *const () as usize;
        if let Some((_, result)) = memo.get(&key) {
            return Ok(result.clone());
        }
        let mut changed = false;
        let children = plan
            .children()
            .into_iter()
            .map(|child| {
                let replacement = visit(child.clone(), memo, tails, defer, projected)?;
                changed |= !Arc::ptr_eq(child, &replacement);
                Ok(replacement)
            })
            .collect::<Result<Vec<_>>>()?;
        let current = if changed {
            plan.clone().with_new_children(children)?
        } else {
            plan.clone()
        };
        let result: PhysicalPlan =
            if let Some(projection) = current.downcast_ref::<ProjectionExec>() {
                if let Some((input, program)) = candidate(projection, tails, defer, projected)? {
                    Arc::new(IndexedUnnestExec {
                        input,
                        template: current.clone(),
                        program: Arc::new(program),
                        cache: current.properties().clone(),
                        early: None,
                    })
                } else {
                    current
                }
            } else {
                current
            };
        memo.insert(key, (plan, result.clone()));
        Ok(result)
    }
    visit(plan, &mut HashMap::new(), &mut HashMap::new(), defer, projected)
}

/// Prepare an already-certified native region without planning its exploded
/// intermediate table projections. The original source still owns its entries.
pub(crate) fn from_early(
    input: PhysicalPlan,
    proof: &EarlyProof,
    options: &PreparationOptions,
    regions: &[BoundRegion],
) -> Result<Option<PhysicalPlan>> {
    build_early(input, proof, options, regions, true)
}

fn build_early(
    input: PhysicalPlan,
    proof: &EarlyProof,
    options: &PreparationOptions,
    regions: &[BoundRegion],
    wrap_output: bool,
) -> Result<Option<PhysicalPlan>> {
    let expected_source = crate::plan::physical_schema(&proof.source);
    if !refines_frame(&input.schema(), &expected_source) {
        return Err(DataFusionError::Plan(format!("early source frame changed: actual={:?}; expected={expected_source:?}", input.schema())));
    }
    trace_refined_frame("source", &input.schema(), &expected_source);
    let template = input.clone();
    let (input, raw_entry, before, mut producer, original_producer) = if options.defer_aligned_producer() {
        match deferred_input(input.clone(), proof.source_entry, options.projected_producer() && !options.selected_producer())? {
            Some((raw, entry, before, producer, original)) => (raw, entry, before, Some(producer), Some(original)),
            None => (input, proof.source_entry, vec![], None, None),
        }
    } else { (input, proof.source_entry, vec![], None, None) };
    if std::env::var_os("VAIL_TRACE_EARLY_ALIGNED_PRODUCT").is_some() {
        eprintln!("VAIL_EARLY_ALIGNED_PRODUCT_PRODUCER deferred={} before_steps={}", producer.is_some(), before.len());
    }
    let region = |owner: &crate::TableRef| {
        BoundRegion::for_owner(regions, owner)
            .ok_or_else(|| DataFusionError::Plan("early expression binding owner absent".into()))
    };
    let consumer_region = region(&proof.consumer)?;
    let output_region = region(&proof.output)?;
    let input_marker_schema =
        marker_schema(&input.schema(), raw_entry, DataType::Int32, false);
    let unnest_schema = marker_schema(
        &crate::plan::physical_schema(&proof.expanded),
        proof.source_entry,
        DataType::Int32,
        false,
    );
    let mut current = unnest_schema.clone();
    let mut steps = Vec::new();
    for step in &proof.steps {
        match step {
            EarlyStep::Project {
                table,
                marker_in,
                marker_out,
            } => {
                let crate::TableNode::MapRows { fields, .. } = table.node() else {
                    unreachable!()
                };
                let marker_in = current.index_of(marker_in)?;
                let marker_out = table.schema().index_of(marker_out)?;
                let columns = crate::early_native::Columns::new(&current);
                let mut expressions = fields
                    .iter()
                    .enumerate()
                    .map(|(i, (_, expression))| {
                        if i == marker_out {
                            Ok(
                                Arc::new(Column::new(current.field(marker_in).name(), marker_in))
                                    as Arc<dyn PhysicalExpr>,
                            )
                        } else {
                            crate::early_native::prepare(
                                expression,
                                current.clone(),
                                region(table)?,
                                options,
                                &columns,
                            )
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                expressions.push(Arc::new(Column::new(
                    current.fields().last().unwrap().name(),
                    current.fields().len() - 1,
                )));
                let schema = marker_schema(
                    &crate::plan::physical_schema(table),
                    marker_out,
                    DataType::Int32,
                    false,
                );
                steps.push(Step::Project {
                    expressions,
                    marker_in,
                    marker_out,
                    schema: schema.clone(),
                });
                current = schema;
            }
            EarlyStep::Filter { table, marker } => {
                let crate::TableNode::Filter {
                    predicate, keep, ..
                } = table.node()
                else {
                    unreachable!()
                };
                let columns = crate::early_native::Columns::new(&current);
                let mut predicate = crate::early_native::prepare(
                    predicate,
                    current.clone(),
                    region(table)?,
                    options,
                    &columns,
                )?;
                if !keep {
                    predicate = Arc::new(datafusion_physical_expr::expressions::NotExpr::new(
                        predicate,
                    ));
                }
                let schema = marker_schema(
                    &crate::plan::physical_schema(table),
                    current.index_of(marker)?,
                    DataType::Int32,
                    false,
                );
                steps.push(Step::Filter {
                    predicate,
                    projection: None,
                    schema: schema.clone(),
                });
                current = schema;
            }
        }
    }
    let expected = marker_schema(
        &crate::plan::physical_schema(proof.consumer_input()),
        proof.final_marker,
        DataType::Int32,
        false,
    );
    if current != expected {
        return Err(DataFusionError::Plan("early metadata frame changed".into()));
    }
    let native = recognize_selected_entry_projection_expr(
        proof.consumer_expression(),
        proof.consumer_schema(),
        proof.selection_expression(),
        &proof.selection_schema,
        options,
    )
    .ok_or_else(|| DataFusionError::Plan("early native private frame no longer matches".into()))?;
    let current_columns = crate::early_native::Columns::new(&current);
    let arguments = crate::early_native::arguments(&proof.consumer_frame, &current, consumer_region, &current_columns)?;
    let selected_producer = if options.selected_producer() {
        if let Some(source) = original_producer {
            if let Some(program) = SelectedProducerProgram::prepare(&source.function, &native, options) {
                ProducerFallback::prepare(&source, producer.as_ref().unwrap(), &input.schema())?
                    .map(|fallback| SelectedProducer {
                        source_arguments: source.arguments, fallback, program,
                        scratch_bytes: options.selected_producer_scratch_bytes(),
                        trace: std::env::var_os("VAIL_TRACE_SELECTED_LIST_PRODUCER").is_some(),
                    })
            } else { None }
        } else { None }
    } else { None };
    if options.selected_producer() && selected_producer.is_none() && options.projected_producer() {
        producer = producer.map(|entry| crate::projected_aligned::prepare(entry.clone()).unwrap_or(entry));
    }
    if std::env::var_os("VAIL_TRACE_EARLY_ALIGNED_PRODUCT").is_some() {
        eprintln!("VAIL_EARLY_SELECTED_PRODUCER admitted={}", selected_producer.is_some());
    }
    let Some(selected) = native.prepare() else {
        return Ok(None);
    };
    let restored_schema = marker_schema(
        &current,
        proof.final_marker,
        selected.output_type().clone(),
        true,
    );
    let crate::TableNode::MapRows {
        fields: consumer_fields,
        ..
    } = proof.consumer.node()
    else {
        unreachable!()
    };
    let restored_columns = crate::early_native::Columns::new(&restored_schema);
    let mut output = consumer_fields
        .iter()
        .enumerate()
        .map(|(i, (_, expression))| {
            if i == proof.consumer_field {
                Ok(Arc::new(Column::new(
                    restored_schema.field(proof.final_marker).name(),
                    proof.final_marker,
                )) as Arc<dyn PhysicalExpr>)
            } else {
                crate::early_native::prepare(
                    expression,
                    restored_schema.clone(),
                    consumer_region,
                    options,
                    &restored_columns,
                )
            }
        })
        .collect::<Result<Vec<_>>>()?;
    output.push(Arc::new(Column::new(
        restored_schema.fields().last().unwrap().name(),
        restored_schema.fields().len() - 1,
    )));
    let output_schema = marker_schema(
        &crate::plan::physical_schema(&proof.consumer),
        proof.consumer_field,
        selected.output_type().clone(),
        true,
    );
    let cache = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(output_schema.clone()),
        Partitioning::UnknownPartitioning(input.properties().partitioning.partition_count()),
        input.properties().emission_type,
        input.properties().boundedness,
    ));
    let program = Program {
        source_schema: input.schema(),
        output_schema: output_schema.clone(),
        restored_schema,
        unnest_schema,
        source_entry: proof.source_entry,
        source_list: proof.source_list,
        marker: proof.final_marker,
        steps,
        arguments,
        selected,
        output,
        input_marker_schema,
        raw_entry,
        before,
        producer,
        selected_producer,
    };
    let core: PhysicalPlan = Arc::new(IndexedUnnestExec {
        input,
        template,
        program: Arc::new(program),
        cache,
        early: Some(EarlyRebuild {
            proof: Arc::new(proof.clone()),
            options: options.clone(),
            regions: regions.to_vec(),
            trace: std::env::var_os("VAIL_TRACE_EARLY_ALIGNED_PRODUCT").is_some(),
        }),
    });
    if !wrap_output {
        return Ok(Some(core));
    }
    let crate::TableNode::MapRows {
        fields: outer_fields,
        ..
    } = proof.output.node()
    else {
        unreachable!()
    };
    let output_columns = crate::early_native::Columns::new(&output_schema);
    let mut outer = outer_fields
        .iter()
        .enumerate()
        .map(|(i, (name, expression))| {
            let value = if i == proof.output_field {
                Arc::new(Column::new(
                    output_schema.field(proof.consumer_field).name(),
                    proof.consumer_field,
                )) as Arc<dyn PhysicalExpr>
            } else {
                crate::early_native::prepare(
                    expression,
                    output_schema.clone(),
                    output_region,
                    options,
                    &output_columns,
                )?
            };
            Ok((value, name.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    outer.push((
        Arc::new(Column::new(
            output_schema.fields().last().unwrap().name(),
            output_schema.fields().len() - 1,
        )),
        output_schema.fields().last().unwrap().name().clone(),
    ));
    let expected = crate::plan::physical_schema(&proof.output);
    let projected: PhysicalPlan = Arc::new(ProjectionExec::try_new_with_schema_metadata(
        outer, core, &expected,
    )?);
    if !refines_frame(&projected.schema(), &expected) {
        return Err(DataFusionError::Plan(format!("early output descriptor changed: actual={:?}; expected={expected:?}", projected.schema())));
    }
    trace_refined_frame("output", &projected.schema(), &expected);
    Ok(Some(projected))
}
