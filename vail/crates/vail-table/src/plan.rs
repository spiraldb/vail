//! TableIR lowering to DataFusion physical plans. Logical partition identity is
//! carried in a private Arrow column, never exposed to expressions or storage.
//! The embedded scan is one ordered physical stream; logical partitions remain
//! independently observable without allowing a parallel coalescer to reorder them.
use crate::globals::GlobalBindings;
use crate::{PartitionEvent, Result, TableError, TableIR, TableNode, order_requirement};
use arrow_array::{Array, Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion_common::{DataFusionError, ScalarValue, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
    expressions::{Column, Literal, NotExpr},
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    empty::EmptyExec,
    execution_plan::{Boundedness, EmissionType},
    filter::FilterExec,
    joins::CrossJoinExec,
    limit::GlobalLimitExec,
    projection::ProjectionExec,
    sorts::sort::SortExec,
    stream::RecordBatchStreamAdapter,
};
use futures::{StreamExt, TryStreamExt};
use std::{collections::VecDeque, fmt, sync::Arc};
use vail_execution::expressions::{Expr, lower_with_bindings, lower_with_global_columns};
use vail_storage::local_table::{LocalRead, normalize};

pub type PhysicalPlan = Arc<dyn ExecutionPlan>;

fn external(error: impl Into<TableError>) -> DataFusionError {
    DataFusionError::External(Box::new(error.into()))
}
pub(crate) fn properties(schema: SchemaRef, emission: EmissionType) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        emission,
        Boundedness::Bounded,
    ))
}
pub(crate) fn column(plan: &PhysicalPlan, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(plan.schema().field(index).name(), index))
}
fn private_name(schema: &Schema, globals: &Schema, base: &str) -> String {
    let mut name = base.to_owned();
    while schema.index_of(&name).is_ok() || globals.index_of(&name).is_ok() {
        name.push('_');
    }
    name
}
fn source_properties(schema: SchemaRef, keys: &[String]) -> Result<Arc<PlanProperties>> {
    let mut cache = (*properties(schema.clone(), EmissionType::Incremental)).clone();
    if !keys.is_empty() {
        let requirement = order_requirement(
            &keys
                .iter()
                .map(|key| (key.clone(), false))
                .collect::<Vec<_>>(),
        )?;
        cache
            .eq_properties
            .add_ordering(requirement.physical_ordering(&schema)?);
    }
    cache.set_eq_properties(cache.eq_properties.clone());
    Ok(Arc::new(cache))
}
fn with_partition(schema: &Schema, name: &str) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(name, DataType::UInt64, false)));
    Arc::new(Schema::new(fields))
}
fn tag(
    batch: RecordBatch,
    partition: usize,
    schema: SchemaRef,
) -> datafusion_common::Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from_value(
        partition as u64,
        batch.num_rows(),
    )));
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[derive(Debug, Clone)]
enum Source {
    Read(Arc<LocalRead>),
    Range {
        n: usize,
        partitions: usize,
    },
    Parallelize {
        rows: RecordBatch,
        partitions: usize,
    },
    Globals(Arc<LocalRead>),
}
#[derive(Debug)]
struct SourceExec {
    source: Source,
    cache: Arc<PlanProperties>,
    row_schema: SchemaRef,
}
impl DisplayAs for SourceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
impl ExecutionPlan for SourceExec {
    fn name(&self) -> &'static str {
        match self.source {
            Source::Read(_) => "VicebergScanExec",
            Source::Range { .. } => "RangeExec",
            Source::Globals(_) => "GlobalsScanExec",
            Source::Parallelize { .. } => "ParallelizeExec",
        }
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("source has no children".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "invalid source partition".into(),
            ));
        }
        let source = self.source.clone();
        let schema = self.schema();
        let output_schema = schema.clone();
        let row_schema = self.row_schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            async_stream::try_stream! {
                match source {
                    Source::Read(source) => {
                        let mut input = source.batches();
                        let mut current = 0usize;
                        let mut first = true;
                        while let Some(event) = input.try_next().await.map_err(external)? {
                            match event {
                                PartitionEvent::StartPartition => { if !first { current += 1; } first = false; }
                                PartitionEvent::Batch(batch) => {
                                    let batch = normalize(&batch, row_schema.clone()).map_err(external)?;
                                    yield tag(batch, current, schema.clone())?;
                                }
                            }
                        }
                    }
                    Source::Range { n, partitions } => {
                        let mut start = 0;
                        for p in 0..partitions {
                            let end = start + n / partitions + usize::from(p < n % partitions);
                            for offset in (start..end).step_by(65536) {
                                let array = Int32Array::from_iter_values((offset..end.min(offset + 65536)).map(|i| i as i32));
                                let batch = RecordBatch::try_new(row_schema.clone(), vec![Arc::new(array)])?;
                                yield tag(batch, p, schema.clone())?;
                            }
                            start = end;
                        }
                    }
                    Source::Parallelize { rows, partitions } => {
                        for p in 0..partitions {
                            let start = rows.num_rows()*p/partitions;
                            let end = rows.num_rows()*(p+1)/partitions;
                            for offset in (start..end).step_by(65536) {
                                yield tag(rows.slice(offset, (end-offset).min(65536)), p, schema.clone())?;
                            }
                        }
                    }
                    Source::Globals(source) => {
                        let batch = vail_storage::globals::read(&source.semantics.globals).map_err(external)?;
                        yield normalize(&batch, row_schema).map_err(external)?;
                    }
                }
            },
        )))
    }
}
pub(crate) fn globals_scan(source: Arc<LocalRead>, schema: SchemaRef) -> PhysicalPlan {
    Arc::new(SourceExec {
        source: Source::Globals(source),
        cache: properties(schema.clone(), EmissionType::Incremental),
        row_schema: schema,
    })
}

/// Streaming extensions for operations without an equivalent stock DF operator.
/// These consume child ExecutionPlans, never TableIR or a second interpreter.
#[derive(Clone, Debug)]
enum Unary {
    Tail(usize),
    Ordinal,
    CheckAscii(Vec<usize>),
}
#[derive(Debug)]
struct UnaryExec {
    input: PhysicalPlan,
    kind: Unary,
    cache: Arc<PlanProperties>,
}
impl UnaryExec {
    fn create(input: PhysicalPlan, kind: Unary) -> PhysicalPlan {
        let mut schema = input.schema();
        let emission = if matches!(kind, Unary::Tail(_)) {
            EmissionType::Final
        } else {
            EmissionType::Incremental
        };
        if matches!(kind, Unary::Ordinal) {
            let name = private_name(&schema, &Schema::empty(), "__vail_ordinal");
            schema = with_partition(&schema, &name);
        }
        let mut cache = (*properties(schema.clone(), emission)).clone();
        if matches!(kind, Unary::Ordinal) {
            cache.eq_properties.add_ordering(
                LexOrdering::new(vec![PhysicalSortExpr::new(
                    Arc::new(Column::new(
                        schema.field(schema.fields().len() - 1).name(),
                        schema.fields().len() - 1,
                    )),
                    SortOptions::default(),
                )])
                .unwrap(),
            );
        }
        cache.set_eq_properties(cache.eq_properties.clone());
        Arc::new(Self {
            input,
            kind,
            cache: Arc::new(cache),
        })
    }
}
impl DisplayAs for UnaryExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self.kind)
    }
}
impl ExecutionPlan for UnaryExec {
    fn name(&self) -> &'static str {
        match self.kind {
            Unary::Tail(_) => "OrderedTailExec",
            Unary::Ordinal => "OrdinalExec",
            Unary::CheckAscii(_) => "CheckKeyDomainExec",
        }
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![datafusion_physical_plan::Distribution::SinglePartition]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "unary operator requires one child".into(),
            ));
        }
        Ok(Self::create(children[0].clone(), self.kind.clone()))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "ordered operator requires one physical partition".into(),
            ));
        }
        let input = self.input.clone();
        let kind = self.kind.clone();
        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                // A zero suffix must not open its source or evaluate its expressions.
                if !matches!(kind, Unary::Tail(0)) {
                    let mut input = input.execute(0, context)?;
                    match kind {
                        Unary::Tail(n) => {
                            let mut retained = VecDeque::new();
                            let mut rows = 0;
                            while let Some(batch) = input.try_next().await? {
                                if batch.num_rows() == 0 { continue; }
                                rows += batch.num_rows();
                                retained.push_back(batch);
                                while rows > n {
                                    let first = retained.pop_front().unwrap();
                                    let remove = (rows - n).min(first.num_rows());
                                    rows -= remove;
                                    if remove < first.num_rows() { retained.push_front(first.slice(remove, first.num_rows()-remove)); }
                                }
                            }
                            for batch in retained { yield batch; }
                        }
                        Unary::Ordinal => {
                            let mut offset = 0u64;
                            while let Some(batch) = input.try_next().await? {
                                let end = offset.checked_add(batch.num_rows() as u64).ok_or_else(|| DataFusionError::Execution("row ordinal overflow".into()))?;
                                let mut columns = batch.columns().to_vec();
                                columns.push(Arc::new(UInt64Array::from_iter_values(offset..end)));
                                offset = end;
                                yield RecordBatch::try_new(schema.clone(), columns)?;
                            }
                        }
                        Unary::CheckAscii(indices) => {
                            while let Some(batch) = input.try_next().await? {
                                for index in &indices {
                                    if let Some(values) = batch.column(*index).as_any().downcast_ref::<StringArray>() {
                                        if values.iter().flatten().any(|s| !s.is_ascii()) {
                                            Err(DataFusionError::Execution("non-ASCII key data requires Hail string collation".into()))?;
                                        }
                                    }
                                }
                                yield batch;
                            }
                        }
                    }
                }
            },
        )))
    }
}
pub(crate) fn ordinal(input: PhysicalPlan) -> PhysicalPlan {
    UnaryExec::create(input, Unary::Ordinal)
}

#[derive(Clone, Copy, Debug)]
pub enum PartitionLayout {
    Fixed(usize),
    Observed,
}
pub struct TablePlan {
    pub rows: PhysicalPlan,
    pub layout: PartitionLayout,
}

pub fn lower(table: &Arc<TableIR>) -> Result<TablePlan> {
    let row_schema = table.schema().clone();
    let partition_name = private_name(&row_schema, table.globals().schema(), "__vail_partition");
    let schema = with_partition(&row_schema, &partition_name);
    let fixed = |n| PartitionLayout::Fixed(n);
    let result = match table.node() {
        TableNode::Read { source } => TablePlan {
            rows: Arc::new(SourceExec {
                source: Source::Read(source.clone()),
                cache: source_properties(schema, &source.keys)?,
                row_schema,
            }),
            layout: fixed(source.partition_rows().len()),
        },
        TableNode::Range { n, n_partitions } => TablePlan {
            rows: Arc::new(SourceExec {
                source: Source::Range {
                    n: *n,
                    partitions: *n_partitions,
                },
                cache: source_properties(schema, &["idx".into()])?,
                row_schema,
            }),
            layout: fixed(*n_partitions),
        },
        TableNode::Parallelize { rows, n_partitions } => TablePlan {
            rows: Arc::new(SourceExec {
                source: Source::Parallelize {
                    rows: rows.clone(),
                    partitions: *n_partitions,
                },
                cache: properties(schema, EmissionType::Incremental),
                row_schema,
            }),
            layout: fixed(*n_partitions),
        },
        TableNode::MapGlobals { child } => lower(child)?,
        TableNode::MapRows {
            child,
            fields,
            bindings,
        } => {
            let input = lower(child)?;
            let (bound, indices) = bind_external(bind_globals(input.rows, child)?, bindings)?;
            let physical_schema = bound.schema();
            let mut expressions = fields
                .iter()
                .map(|(name, expr)| {
                    Ok((
                        lower_with_bindings(
                            expr,
                            &physical_schema,
                            child.globals().schema(),
                            &indices,
                        )?,
                        name.clone(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            expressions.push((
                column(&bound, physical_schema.fields().len() - 1),
                partition_name,
            ));
            TablePlan {
                rows: Arc::new(ProjectionExec::try_new(expressions, bound)?),
                layout: input.layout,
            }
        }
        TableNode::Filter {
            child,
            predicate,
            keep,
            bindings,
        } => {
            let input = lower(child)?;
            let (bound, indices) = bind_external(bind_globals(input.rows, child)?, bindings)?;
            let mut predicate = lower_with_bindings(
                predicate,
                &bound.schema(),
                child.globals().schema(),
                &indices,
            )?;
            if !keep {
                predicate = Arc::new(NotExpr::new(predicate));
            }
            let filtered: PhysicalPlan = Arc::new(FilterExec::try_new(predicate, bound)?);
            let offset = filtered.schema().fields().len() - child.schema().fields().len() - 1;
            let expressions: Vec<_> = (offset..filtered.schema().fields().len())
                .map(|i| {
                    (
                        column(&filtered, i),
                        filtered.schema().field(i).name().clone(),
                    )
                })
                .collect();
            TablePlan {
                rows: Arc::new(ProjectionExec::try_new(expressions, filtered)?),
                layout: input.layout,
            }
        }
        TableNode::Head { n: 0, .. } | TableNode::Tail { n: 0, .. } => TablePlan {
            rows: Arc::new(EmptyExec::new(schema)),
            layout: PartitionLayout::Observed,
        },
        TableNode::Head { child, n } => {
            let input = lower(child)?;
            TablePlan {
                rows: Arc::new(GlobalLimitExec::new(input.rows, 0, Some(*n))),
                layout: PartitionLayout::Observed,
            }
        }
        TableNode::Tail { child, n } => {
            let input = lower(child)?;
            TablePlan {
                rows: UnaryExec::create(input.rows, Unary::Tail(*n)),
                layout: PartitionLayout::Observed,
            }
        }
        TableNode::KeyBy { child, keys } => sort(
            lower(child)?,
            &keys.iter().map(|k| (k.clone(), false)).collect::<Vec<_>>(),
        )?,
        TableNode::OrderBy { child, keys } => sort(lower(child)?, keys)?,
    };
    Ok(result)
}
/// Join captured singleton plans without exposing their field names in the row
/// or local-global namespaces. Physical indices remain explicit after each join.
pub(crate) fn bind_external(
    mut input: PhysicalPlan,
    bindings: &GlobalBindings,
) -> Result<(PhysicalPlan, std::collections::BTreeMap<String, usize>)> {
    let mut indices = std::collections::BTreeMap::<String, usize>::new();
    for (reference, globals) in bindings {
        let plan = globals.lower()?;
        let value = Expr::Struct {
            fields: globals
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.name().clone(),
                        Expr::Global {
                            name: field.name().clone(),
                        },
                    )
                })
                .collect(),
        };
        let expression = lower_with_global_columns(&value, &plan.schema(), globals.schema())?;
        let name = private_name(&input.schema(), &Schema::empty(), "__vail_external_globals");
        let singleton = Arc::new(ProjectionExec::try_new(vec![(expression, name)], plan)?);
        input = Arc::new(CrossJoinExec::new(singleton, input));
        for index in indices.values_mut() {
            *index += 1;
        }
        indices.insert(reference.clone(), 0);
    }
    Ok((input, indices))
}

fn bind_globals(rows: PhysicalPlan, table: &Arc<TableIR>) -> Result<PhysicalPlan> {
    if table.globals().schema().fields().is_empty() {
        return Ok(rows);
    }
    // CrossJoin builds its left input: exactly one row, including for empty tables.
    Ok(Arc::new(CrossJoinExec::new(table.globals().lower()?, rows)))
}
fn sort(input: TablePlan, keys: &[(String, bool)]) -> Result<TablePlan> {
    if keys.is_empty() {
        return Ok(input);
    }
    let schema = input.rows.schema();
    let requirement = order_requirement(keys)?;
    let mut ordering = requirement
        .physical_ordering(&schema)?
        .into_iter()
        .collect::<Vec<_>>();
    let indices = keys
        .iter()
        .map(|(name, _)| schema.index_of(name))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let checked = UnaryExec::create(input.rows, Unary::CheckAscii(indices));
    let numbered = ordinal(checked);
    ordering.push(PhysicalSortExpr::new(
        column(&numbered, numbered.schema().fields().len() - 1),
        SortOptions::default(),
    ));
    let sorted: PhysicalPlan =
        Arc::new(SortExec::new(LexOrdering::new(ordering).unwrap(), numbered));
    let mut expressions = (0..schema.fields().len() - 1)
        .map(|i| (column(&sorted, i), schema.field(i).name().clone()))
        .collect::<Vec<_>>();
    expressions.push((
        Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
        schema.field(schema.fields().len() - 1).name().clone(),
    ));
    Ok(TablePlan {
        rows: Arc::new(ProjectionExec::try_new(expressions, sorted)?),
        layout: PartitionLayout::Fixed(1),
    })
}

/// Adapt completed DataFusion batches to the storage boundary's partition events.
/// No TableIR operations are interpreted here.
pub fn execute(table: &Arc<TableIR>) -> crate::BatchStream {
    let table = table.clone();
    async_stream::try_stream! {
        let plan = lower(&table)?;
        let mut input = plan.rows.execute(0, Arc::new(TaskContext::default()))?;
        let mut next_partition = 0usize;
        let mut previous = None;
        while let Some(batch) = input.try_next().await? {
            let ids = batch.column(batch.num_columns()-1).as_any().downcast_ref::<UInt64Array>()
                .ok_or_else(|| TableError::Invalid("missing physical partition identity".into()))?;
            let mut offset = 0;
            while offset < batch.num_rows() {
                let id = ids.value(offset) as usize;
                if previous != Some(id) {
                    if previous.is_some_and(|previous| id < previous) || matches!(plan.layout, PartitionLayout::Fixed(n) if id >= n) {
                        Err(TableError::Invalid("DataFusion plan reordered logical partitions".into()))?;
                    }
                    match plan.layout {
                        PartitionLayout::Fixed(_) => {
                            while next_partition <= id { yield PartitionEvent::StartPartition; next_partition += 1; }
                        }
                        PartitionLayout::Observed => yield PartitionEvent::StartPartition,
                    }
                    previous = Some(id);
                }
                let mut end = offset+1;
                while end < batch.num_rows() && ids.value(end) == id as u64 { end += 1; }
                let piece = batch.slice(offset, end-offset).project(&(0..table.schema().fields().len()).collect::<Vec<_>>())?;
                yield PartitionEvent::Batch(normalize(&piece, table.schema().clone())?);
                offset = end;
            }
        }
        if let PartitionLayout::Fixed(total) = plan.layout {
            while next_partition < total { yield PartitionEvent::StartPartition; next_partition += 1; }
        }
    }.boxed()
}

/// Cardinality is a DataFusion aggregate, including forced counts of empty inputs.
pub fn count_plan(table: &Arc<TableIR>) -> Result<PhysicalPlan> {
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    let input = lower(table)?.rows;
    let schema = input.schema();
    let aggregate = AggregateExprBuilder::new(
        datafusion_functions_aggregate::count::count_udaf(),
        vec![Arc::new(Literal::new(ScalarValue::Int32(Some(1))))],
    )
    .schema(schema.clone())
    .alias("count")
    .build()?;
    Ok(Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![]),
        vec![Arc::new(aggregate)],
        vec![None],
        input,
        schema,
    )?))
}
pub async fn count(table: &Arc<TableIR>) -> Result<usize> {
    let batches = datafusion_physical_plan::execution_plan::collect(
        count_plan(table)?,
        Arc::new(TaskContext::default()),
    )
    .await?;
    if batches.len() != 1 || batches[0].num_rows() != 1 {
        return Err(TableError::Invalid(
            "count aggregate must produce one row".into(),
        ));
    }
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .ok_or_else(|| TableError::Invalid("count aggregate must return int64".into()))?;
    usize::try_from(array.value(0)).map_err(|_| TableError::Invalid("count exceeds usize".into()))
}
