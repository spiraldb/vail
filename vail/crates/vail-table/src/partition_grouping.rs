//! Physical lowering for Hail's contiguous per-partition grouping operation.
use crate::plan::{PhysicalPlan, TablePlan, column};
use crate::{Result, TableError};
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion_common::{
    DataFusionError, ScalarValue, config::ConfigOptions, tree_node::TreeNodeRecursion,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::{
    LexOrdering, PhysicalExpr, PhysicalSortExpr,
    aggregate::AggregateExprBuilder,
    expressions::{Column, Literal},
};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    projection::ProjectionExec,
    sorts::sort::SortExec,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};

/// Add the group number for each row, resetting its counter when the logical
/// partition tag changes. The input is the table's single ordered stream.
#[derive(Debug)]
struct GroupIndexExec {
    input: PhysicalPlan,
    n: usize,
    ordinal_name: String,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl GroupIndexExec {
    fn new(input: PhysicalPlan, n: usize, name: String, ordinal_name: String) -> Self {
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(name, DataType::UInt64, false)));
        fields.push(Arc::new(Field::new(
            ordinal_name.clone(),
            DataType::UInt64,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        let mut equivalences = datafusion_physical_expr::EquivalenceProperties::new(schema.clone());
        let tag_index = input.schema().fields().len() - 1;
        equivalences.add_ordering(
            LexOrdering::new(vec![
                PhysicalSortExpr::new(
                    Arc::new(Column::new(
                        input.schema().field(tag_index).name(),
                        tag_index,
                    )),
                    SortOptions::default(),
                ),
                PhysicalSortExpr::new(
                    Arc::new(Column::new(
                        schema.field(input.schema().fields().len()).name(),
                        input.schema().fields().len(),
                    )),
                    SortOptions::default(),
                ),
                PhysicalSortExpr::new(
                    Arc::new(Column::new(
                        &ordinal_name,
                        input.schema().fields().len() + 1,
                    )),
                    SortOptions::default(),
                ),
            ])
            .unwrap(),
        );
        let properties = Arc::new(PlanProperties::new(
            equivalences,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            n,
            ordinal_name,
            schema,
            properties,
        }
    }
}

impl DisplayAs for GroupIndexExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GroupIndexExec")
    }
}
impl ExecutionPlan for GroupIndexExec {
    fn name(&self) -> &'static str {
        "GroupIndexExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        vec![datafusion_physical_plan::Distribution::SinglePartition]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "GroupIndexExec expects one child".into(),
            ));
        }
        let index_name = self
            .schema
            .field(self.input.schema().fields().len())
            .name()
            .clone();
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.n,
            index_name,
            self.ordinal_name.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "GroupIndexExec requires one physical partition".into(),
            ));
        }
        let mut input = self.input.execute(0, context)?;
        let schema = self.schema.clone();
        let n = self.n as u64;
        let partition_column = self.input.schema().fields().len() - 1;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
                let mut current_partition = None;
                let mut offset = 0u64;
                while let Some(batch) = input.try_next().await? {
                    let tags = batch.column(partition_column).as_any().downcast_ref::<UInt64Array>()
                        .ok_or_else(|| DataFusionError::Execution("logical partition tag must be UInt64".into()))?;
                    let mut values = Vec::with_capacity(batch.num_rows());
                    let mut ordinals = Vec::with_capacity(batch.num_rows());
                    for i in 0..batch.num_rows() {
                        let tag = tags.value(i);
                        if current_partition != Some(tag) { current_partition = Some(tag); offset = 0; }
                        values.push(offset / n);
                        ordinals.push(offset);
                        offset = offset.checked_add(1).ok_or_else(|| DataFusionError::Execution("partition row count overflow".into()))?;
                    }
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(UInt64Array::from(values)));
                    columns.push(Arc::new(UInt64Array::from(ordinals)));
                    yield RecordBatch::try_new(schema.clone(), columns)?;
                }
            },
        )))
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

pub(crate) fn lower_group_within(
    input: TablePlan,
    row_schema: SchemaRef,
    keys: &[String],
    name: &str,
    n: usize,
) -> Result<TablePlan> {
    if n == 0 {
        return Err(TableError::Invalid(
            "group_within_partitions requires n > 0".into(),
        ));
    }
    let source_schema = input.rows.schema();
    let tag_index = source_schema.fields().len().checked_sub(1).ok_or_else(|| {
        TableError::Invalid("group_within_partitions input has no partition tag".into())
    })?;
    let tag_name = source_schema.field(tag_index).name().clone();
    let index_name = fresh_name(&source_schema, "__vail_group_index");
    let ordinal_name = fresh_name(&source_schema, "__vail_group_row_ordinal");
    let numbered: PhysicalPlan = Arc::new(GroupIndexExec::new(
        input.rows,
        n,
        index_name.clone(),
        ordinal_name.clone(),
    ));
    let numbered_schema = numbered.schema();

    let mut row_args: Vec<Arc<dyn PhysicalExpr>> =
        Vec::with_capacity(row_schema.fields().len() * 2);
    for field in row_schema.fields() {
        row_args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(
            field.name().clone(),
        )))));
        row_args.push(column(&numbered, numbered_schema.index_of(field.name())?));
    }
    let row_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::core::named_struct(),
        row_args,
        &numbered_schema,
        Arc::new(ConfigOptions::default()),
    )?);
    let row_aggregate = AggregateExprBuilder::new(
        datafusion_functions_aggregate::array_agg::array_agg_udaf(),
        vec![row_expr.clone()],
    )
    .order_by(vec![PhysicalSortExpr::new(
        column(&numbered, numbered_schema.index_of(&ordinal_name)?),
        SortOptions::default(),
    )])
    .schema(numbered_schema.clone())
    .alias(name)
    .build()?;

    let group_exprs = vec![
        (column(&numbered, tag_index), tag_name.clone()),
        (
            column(&numbered, numbered_schema.index_of(&index_name)?),
            index_name.clone(),
        ),
    ];
    // Group only by the partition and group ordinal. Key fields are represented
    // by the first row, as required when a chunk crosses distinct table keys.
    let keys = keys
        .iter()
        .map(|key| {
            let index = numbered_schema.index_of(key)?;
            let expr = Arc::new(datafusion_physical_expr::expressions::Column::new(
                key, index,
            )) as Arc<dyn PhysicalExpr>;
            Ok((expr, key.clone()))
        })
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    let row_ordinal = PhysicalSortExpr::new(
        column(&numbered, numbered_schema.index_of(&ordinal_name)?),
        SortOptions::default(),
    );
    let key_aggs = keys
        .iter()
        .map(|(expr, key)| {
            AggregateExprBuilder::new(
                datafusion_functions_aggregate::first_last::first_value_udaf(),
                vec![expr.clone()],
            )
            .order_by(vec![row_ordinal.clone()])
            .schema(numbered_schema.clone())
            .alias(key)
            .build()
            .map(Arc::new)
        })
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    let mut aggregates = key_aggs;
    aggregates.push(Arc::new(row_aggregate));
    let filters = vec![None; aggregates.len()];
    let grouped: PhysicalPlan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(group_exprs),
        aggregates,
        filters,
        numbered,
        numbered_schema,
    )?);

    let sorted: PhysicalPlan = Arc::new(SortExec::new(
        LexOrdering::new(vec![
            PhysicalSortExpr::new(column(&grouped, 0), SortOptions::default()),
            PhysicalSortExpr::new(column(&grouped, 1), SortOptions::default()),
        ])
        .unwrap(),
        grouped,
    ));
    let mut projection: Vec<(Arc<dyn PhysicalExpr>, String)> = keys
        .iter()
        .enumerate()
        .map(|(i, (_, key))| (column(&sorted, i + 2), key.clone()))
        .collect();
    projection.push((column(&sorted, keys.len() + 2), name.to_owned()));
    projection.push((column(&sorted, 0), tag_name));
    let rows: PhysicalPlan = Arc::new(ProjectionExec::try_new(projection, sorted)?);
    Ok(TablePlan {
        rows,
        layout: input.layout,
    })
}

fn fresh_name(schema: &Schema, base: &str) -> String {
    let mut candidate = base.to_owned();
    while schema.index_of(&candidate).is_ok() {
        candidate.push('_');
    }
    candidate
}
