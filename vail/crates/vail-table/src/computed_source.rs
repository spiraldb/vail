//! Row-preserving encoded projections; DataFusion retains relational/global work.
use crate::{Result, TableNode, TableRef};
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType, provider_as_source};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_execution::TaskContext;
use datafusion_expr::{Expr as DFExpr, LogicalPlan, LogicalPlanBuilder};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    metrics::{ExecutionPlanMetricsSet, SplitMetrics},
    projection::ProjectionExec,
    stream::{BatchSplitStream, RecordBatchStreamAdapter},
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use vail_execution::expressions::{ArrayReduction, Expr, PrimitiveType, Type, UnaryOperator};
use vail_storage::local_table::{LocalRead, PartitionEvent};
use vortex::dtype::Nullability;
use vortex::expr::{Expression, fill_null, get_item, is_not_null, list_sum, lit, mask, pack, root};
use vortex::{VortexSessionDefault, arrow::ArrowSessionExt, session::VortexSession};

fn path(expr: &Expr, parameter: &str) -> Option<Vec<String>> {
    match expr {
        Expr::LambdaVariable { name, .. } if name == parameter => Some(vec![]),
        Expr::GetField { value, name } => {
            let mut p = path(value, parameter)?;
            p.push(name.clone());
            Some(p)
        }
        Expr::Cast {
            dtype: Type::Primitive(PrimitiveType::Int64),
            value,
        } => path(value, parameter),
        _ => None,
    }
}

fn translate(
    expr: &Expr,
    schema: &Schema,
    session: &VortexSession,
) -> Option<(Expression, bool, Option<Vec<String>>)> {
    if let Expr::Column { name } = expr {
        return Some((get_item(name.as_str(), root()), false, None));
    }
    let Expr::ArrayReduce {
        operation: ArrayReduction::Sum,
        filter_missing: true,
        value,
    } = expr
    else {
        return None;
    };
    let Expr::ArrayMap {
        value,
        parameter,
        body,
    } = value.as_ref()
    else {
        return None;
    };
    let Expr::Column { name } = value.as_ref() else {
        return None;
    };
    if expr.dtype(schema).ok()?.to_arrow().ok()? != DataType::Int64 {
        return None;
    }
    let (body, guard) = if let Expr::IfElse {
        condition,
        consequent,
        alternate,
    } = body.as_ref()
    {
        let missing = match alternate.as_ref() {
            Expr::Literal { value, .. } => value.is_null(),
            Expr::RuntimeLiteral { value } => value.is_null(),
            _ => false,
        };
        if !missing {
            return None;
        }
        let Expr::Unary {
            operator: UnaryOperator::Not,
            value,
        } = condition.as_ref()
        else {
            return None;
        };
        let Expr::IsMissing { value } = value.as_ref() else {
            return None;
        };
        (consequent.as_ref(), Some(path(value, parameter)?))
    } else {
        (body.as_ref(), None)
    };
    let names = path(body, parameter)?;
    if guard.is_some_and(|g| !names.starts_with(&g)) {
        return None;
    }
    // Arrow List bounds the maximum valid per-list child count, proving i32
    // accumulation in i64 cannot overflow. LargeList and other widths fall back.
    let field = schema.field_with_name(name).ok()?;
    let DataType::List(element) = field.data_type() else {
        return None;
    };
    let mut leaf = element.data_type();
    for name in &names {
        let DataType::Struct(fields) = leaf else {
            return None;
        };
        leaf = fields.iter().find(|f| f.name() == name)?.data_type();
    }
    if leaf != &DataType::Int32 {
        return None;
    }
    let dtype = session.arrow().from_arrow_field(field).ok()?;
    let input = get_item(name.as_str(), root());
    let mut operand = vec![name.clone()];
    operand.extend(names.iter().cloned());
    let elements = vortex::array::scalar_fn::fns::list_field::list_field(
        input.clone(),
        &dtype,
        &names.into_iter().map(Into::into).collect::<Vec<_>>(),
    )
    .ok()?;
    // This proven nonoverflowing subset permits zero identity for empty/all-null
    // lists while separately retaining a missing outer list.
    Some((
        mask(
            fill_null(list_sum(elements), lit(0_i64)),
            is_not_null(input),
        ),
        true,
        Some(operand),
    ))
}

pub(crate) fn try_lower(
    table: &TableRef,
    child: &TableRef,
    fields: &[(String, Expr)],
) -> Result<Option<LogicalPlan>> {
    let TableNode::Read { source } = child.node() else {
        return Ok(None);
    };
    let session = VortexSession::default();
    let Some(translated) = fields
        .iter()
        .map(|(name, expr)| {
            translate(expr, child.schema(), &session)
                .map(|(e, reduction, operand)| (name.clone(), e, reduction, operand))
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    if !translated.iter().any(|(_, _, r, _)| *r) {
        return Ok(None);
    }
    let mut output = table
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    let tag =
        crate::plan::private_name(table.schema(), table.globals().schema(), "__vail_partition");
    output.push(Field::new(tag, DataType::UInt64, false));
    let provider = ComputedProvider {
        original_source: child.clone(),
        original_fields: fields.to_vec(),
        source: source.clone(),
        schema: Arc::new(Schema::new(output)),
        operands: translated
            .iter()
            .map(|(_, _, _, operand)| operand.clone())
            .collect(),
        expressions: translated.into_iter().map(|(n, e, _, _)| (n, e)).collect(),
        keys: child
            .keys()
            .iter()
            .map_while(|key| {
                fields.iter().find_map(|(name, expr)| {
                    matches!(expr, Expr::Column {name: source} if source == key)
                        .then(|| name.clone())
                })
            })
            .collect(),
    };
    Ok(Some(
        LogicalPlanBuilder::scan(
            "vail_encoded_projection",
            provider_as_source(Arc::new(provider)),
            None,
        )?
        .build()?,
    ))
}

/// Push a supported row-local aggregate argument into its source without
/// changing the global aggregate or its row multiplicities.
pub(crate) fn try_total(
    table: &TableRef,
    expression: &Expr,
) -> Result<Option<(LogicalPlan, Expr)>> {
    let TableNode::Read { source } = table.node() else {
        return Ok(None);
    };
    let Expr::Aggregate {
        function,
        value: Some(value),
        ..
    } = expression
    else {
        return Ok(None);
    };
    // The initial total route is order-insensitive SUM. Order-sensitive
    // consumers retain the ordinary source ordering contract.
    if function != "sum" {
        return Ok(None);
    }
    let session = VortexSession::default();
    let Some((projection, true, operand)) = translate(value, table.schema(), &session) else {
        return Ok(None);
    };
    let name = crate::plan::private_name(
        table.schema(),
        table.globals().schema(),
        "__vail_encoded_value",
    );
    // Aggregate scopes retain their input schema at the independent-consumer
    // boundary. Supply only dependencies actually consumed by this total,
    // rather than decoding original source columns that cannot be pruned later.
    let tag =
        crate::plan::private_name(table.schema(), table.globals().schema(), "__vail_partition");
    let provider = ComputedProvider {
        original_source: table.clone(),
        original_fields: vec![(name.clone(), value.as_ref().clone())],
        source: source.clone(),
        schema: Arc::new(Schema::new(vec![
            Field::new(&name, DataType::Int64, true),
            Field::new(tag, DataType::UInt64, false),
        ])),
        expressions: vec![(name.clone(), projection)],
        operands: vec![operand],
        keys: vec![],
    };
    let child = LogicalPlanBuilder::scan(
        "vail_encoded_projection",
        provider_as_source(Arc::new(provider)),
        None,
    )?
    .build()?;
    let mut rewritten = expression.clone();
    if let Expr::Aggregate { value, .. } = &mut rewritten {
        *value = Some(Arc::new(Expr::Column { name }));
    }
    Ok(Some((child, rewritten)))
}

#[derive(Debug)]
struct ComputedProvider {
    // Keep the original typed expressions until physical source capabilities
    // have proved encoded evaluation safe. Unsupported capabilities must use
    // the ordinary DataFusion source/projection before evaluating the payload.
    original_source: TableRef,
    original_fields: Vec<(String, Expr)>,
    operands: Vec<Option<Vec<String>>>,
    source: Arc<LocalRead>,
    schema: SchemaRef,
    expressions: Vec<(String, Expression)>,
    keys: Vec<String>,
}
impl ComputedProvider {
    /// The fallback keeps row expressions visible to DataFusion and begins at
    /// the original source; no encoded evaluation or partial output precedes it.
    async fn original_scan(
        &self,
        state: &dyn Session,
        indices: &[usize],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let tag = self.original_fields.len();
        let fields = indices
            .iter()
            .filter(|&&index| index != tag)
            .map(|&index| self.original_fields[index].clone())
            .collect::<Vec<_>>();
        let paths = crate::nested_projection::map_rows_storage_paths(
            &fields,
            self.original_source.schema(),
        );
        let provider =
            crate::provider::provider_with_nested_projection_paths(&self.original_source, paths)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let source_schema = provider.schema();
        let source_tag = source_schema.fields().len() - 1;
        let mut columns = std::collections::HashSet::new();
        for (_, expression) in &fields {
            expression.visit_unique(&mut |node| {
                if let Expr::Column { name } = node {
                    columns.insert(name.clone());
                }
            });
        }
        let projection = source_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                (columns.contains(field.name()) || (index == source_tag && indices.contains(&tag)))
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        let source_tag_name = source_schema.field(source_tag).name().clone();
        let input = provider.scan(state, Some(&projection), &[], None).await?;
        let df_schema = datafusion_common::DFSchema::try_from(input.schema().as_ref().clone())?;
        let expressions = indices
            .iter()
            .map(|&index| {
                let expression = if index == tag {
                    datafusion_expr::col(&source_tag_name)
                } else {
                    vail_execution::logical_expr::lower_logical_with_bindings(
                        &self.original_fields[index].1,
                        &df_schema,
                        self.original_source.globals().schema(),
                        &Default::default(),
                    )
                    .map_err(|error| DataFusionError::External(Box::new(error)))?
                };
                Ok((
                    state.create_physical_expr(expression, &df_schema)?,
                    self.schema.field(index).name().clone(),
                ))
            })
            .collect::<DFResult<Vec<_>>>()?;
        let schema = self.schema.project(indices)?;
        Ok(Arc::new(ProjectionExec::try_new_with_schema_metadata(
            expressions,
            input,
            &schema,
        )?))
    }
}

#[async_trait::async_trait]
impl TableProvider for ComputedProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[DFExpr],
        _: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let indices = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let tag = self.expressions.len();
        let operands = indices
            .iter()
            .filter_map(|&index| self.operands.get(index).and_then(Clone::clone))
            .collect::<Vec<_>>();
        if operands.is_empty() {
            return self.original_scan(state, &indices).await;
        }
        let Some(certificate) = self
            .source
            .prepare_dense_list_projection(operands.clone())
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
        else {
            return self.original_scan(state, &indices).await;
        };
        let fields = indices
            .iter()
            .filter(|&&i| i != tag)
            .map(|&i| self.expressions[i].clone())
            .collect::<Vec<_>>();
        let expression = pack(fields, Nullability::NonNullable);
        let row_schema = Arc::new(
            self.schema.project(
                &indices
                    .iter()
                    .copied()
                    .filter(|&i| i != tag)
                    .collect::<Vec<_>>(),
            )?,
        );
        let schema = Arc::new(self.schema.project(&indices)?);
        let mut equivalence = EquivalenceProperties::new(schema.clone());
        let keys = self
            .keys
            .iter()
            .take_while(|name| schema.index_of(name).is_ok())
            .map(|name| (name.clone(), false))
            .collect::<Vec<_>>();
        if !keys.is_empty() {
            let requirement = crate::order_requirement(&keys)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            equivalence.add_ordering(
                requirement
                    .physical_ordering(&schema)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            );
        }
        let partitioning = if let Some(position) = indices.iter().position(|&i| i == tag) {
            crate::provider::partition_id_properties(
                &schema,
                &mut equivalence,
                self.source.partition_rows().len(),
                position,
            )
        } else {
            Partitioning::UnknownPartitioning(self.source.partition_rows().len())
        };
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(ComputedExec {
            certificate,
            source: self.source.clone(),
            expression,
            row_schema,
            schema,
            indices,
            tag,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }
}
#[derive(Debug)]
struct ComputedExec {
    certificate: Arc<vail_storage::computed_capability::DenseListProjectionCertificate>,
    source: Arc<LocalRead>,
    expression: Expression,
    row_schema: SchemaRef,
    schema: SchemaRef,
    indices: Vec<usize>,
    tag: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}
impl DisplayAs for ComputedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VortexComputedProjectionExec: {}", self.expression)
    }
}
impl ExecutionPlan for ComputedExec {
    fn name(&self) -> &'static str {
        "VortexComputedProjectionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn metrics(&self) -> Option<datafusion_physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        ) -> DFResult<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "computed source has no children".into(),
            ))
        }
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.source.partition_rows().len() {
            return Err(DataFusionError::Execution(
                "source partition out of range".into(),
            ));
        }
        let mut input = self.source.computed_partition_batches(
            partition,
            &self.certificate,
            self.expression.clone(),
            self.row_schema.clone(),
        );
        let schema = self.schema.clone();
        let output_schema = schema.clone();
        let indices = self.indices.clone();
        let tag = self.tag;
        let stream = async_stream::try_stream! {
            while let Some(event)=input.try_next().await.map_err(|e|DataFusionError::External(Box::new(e)))?{
                if let PartitionEvent::Batch(batch)=event {
                    let mut row=0;let mut columns=Vec::with_capacity(indices.len());
                    for &index in &indices {if index==tag {columns.push(Arc::new(UInt64Array::from_value(partition as u64,batch.num_rows())) as arrow_array::ArrayRef);}else{columns.push(batch.column(row).clone());row+=1;}}
                    yield RecordBatch::try_new_with_options(schema.clone(),columns,&arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())))?;
                }
            }
        };
        let stream = Box::pin(RecordBatchStreamAdapter::new(output_schema, stream));
        Ok(Box::pin(BatchSplitStream::new(
            stream,
            context.session_config().batch_size(),
            SplitMetrics::new(&self.metrics, partition),
        )))
    }
}
