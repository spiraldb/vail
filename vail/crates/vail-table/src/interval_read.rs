//! Independent physical sources for table interval reads.
use crate::partitioned::PreparedTable;
use crate::plan::{self, PhysicalPlan, source_partition};
use crate::{Result, ScalarIR, TableError, TableRef};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::{DataFusionError, ScalarValue, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    filter::FilterExec,
    stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use vail_execution::expressions::{Expr, Type, lower_with_global_columns};
use vail_storage::{
    fragment_index::{Endpoint, IndexBinding, KeyType, KeyValue, QueryInterval},
    local_table::LocalRead,
};

#[derive(Debug)]
struct IntervalSourceExec {
    source: Arc<LocalRead>,
    query: QueryInterval,
    row_schema: SchemaRef,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}
impl IntervalSourceExec {
    fn new(
        source: Arc<LocalRead>,
        query: QueryInterval,
        row_schema: SchemaRef,
        schema: SchemaRef,
    ) -> Self {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            source,
            query,
            row_schema,
            schema,
            cache,
        }
    }
}
impl DisplayAs for IntervalSourceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntervalSourceExec")
    }
}
impl ExecutionPlan for IntervalSourceExec {
    fn name(&self) -> &'static str {
        "IntervalSourceExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&PhysicalPlan> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion_common::Result<TreeNodeRecursion>,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "IntervalSourceExec has no children".into(),
            ));
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
                "interval source requires partition zero".into(),
            ));
        }
        let mut batches = self
            .source
            .interval_batches(std::slice::from_ref(&self.query));
        let row_schema = self.row_schema.clone();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            async_stream::try_stream! {
            while let Some((_, batch)) = batches.try_next().await.map_err(storage_error)? {
                let batch = vail_storage::local_table::normalize(&batch, row_schema.clone()).map_err(storage_error)?;
                let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(UInt64Array::from_value(0, batch.num_rows())));
                    yield RecordBatch::try_new(schema.clone(), columns)?;
                }
            },
        )))
    }
}

pub(crate) async fn prepare(
    child: &TableRef,
    intervals: &[ScalarIR],
    point_type: &Type,
    filter: bool,
) -> Result<PreparedTable> {
    let crate::TableNode::Read { source } = child.node() else {
        return Err(TableError::Invalid(
            "interval reads require a table read source".into(),
        ));
    };
    let schema = plan::physical_schema(child);
    if intervals.is_empty() {
        return Ok(PreparedTable {
            parts: vec![],
            counts: vec![],
            schema,
        });
    }
    let binding = source.index_binding().map_err(TableError::Storage)?;
    let keys = child.keys();
    let mut bounds = Vec::with_capacity(intervals.len());
    for interval in intervals {
        let literal = eval_scalar(interval)?;
        let ScalarValue::Struct(fields) = literal else {
            return Err(TableError::Invalid(
                "interval literal cannot be missing".into(),
            ));
        };
        if fields.is_null(0) {
            return Err(TableError::Invalid(
                "interval literal cannot be missing".into(),
            ));
        }
        if fields.num_columns() != 4 {
            return Err(TableError::Invalid(
                "interval literal has invalid storage shape".into(),
            ));
        }
        let field = |index| {
            ScalarValue::try_from_array(fields.column(index).as_ref(), 0).map_err(TableError::from)
        };
        bounds.push(IntervalBounds {
            lower: field(0)?,
            upper: field(2)?,
            lower_inclusive: !scalar_boolean(&field(1)?)?,
            upper_inclusive: scalar_boolean(&field(3)?)?,
            sources: vec![interval.clone()],
        });
    }
    let full_key =
        keys.len() == binding.key_fields.len() && point_component_count(point_type) == keys.len();
    let bounds = normalize_bounds(bounds, point_type, &binding)?;
    let queries = if full_key {
        bounds
            .iter()
            .map(|b| make_query(b, point_type, &binding))
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![QueryInterval::default(); bounds.len()]
    };
    let all_predicate = union_predicate(intervals, keys, point_type)?;
    if filter {
        // Prefix intervals deliberately over-select files. Partition identity is
        // retained, then the exact DataFusion predicate removes false positives.
        let candidates = source
            .candidate_partitions(&queries)
            .await
            .map_err(TableError::Storage)?;
        let mut parts = Vec::with_capacity(candidates.len());
        let mut counts = Vec::with_capacity(candidates.len());
        for part in candidates {
            let rows = source_partition(child, part)?;
            parts.push(apply_filter(rows, &all_predicate, child)?);
            counts.push(None);
        }
        return Ok(PreparedTable {
            parts,
            counts,
            schema,
        });
    }

    let mut parts = Vec::with_capacity(queries.len());
    let mut counts = Vec::with_capacity(queries.len());
    for (query, bound) in queries.into_iter().zip(bounds) {
        let candidate_ids = source
            .candidate_partitions(std::slice::from_ref(&query))
            .await
            .map_err(TableError::Storage)?;
        let rows = if full_key {
            Arc::new(IntervalSourceExec::new(
                source.clone(),
                query,
                child.schema().clone(),
                schema.clone(),
            )) as PhysicalPlan
        } else {
            // A prefix cannot be expanded to full composite bounds from index
            // metadata alone. Read ordered source partitions and filter rows.
            let physical = (0..source.partition_rows().len())
                .map(|p| source_partition(child, p))
                .collect::<Result<Vec<_>>>()?;
            PreparedTable {
                parts: physical,
                counts: source.partition_rows().iter().copied().map(Some).collect(),
                schema: schema.clone(),
            }
            .ordered()
        };
        let predicate = union_predicate(&bound.sources, keys, point_type)?;
        parts.push(apply_filter(rows, &predicate, child)?);
        counts.push(if candidate_ids.is_empty() {
            Some(0)
        } else {
            None
        });
    }
    Ok(PreparedTable {
        parts,
        counts,
        schema,
    })
}

struct IntervalBounds {
    lower: ScalarValue,
    upper: ScalarValue,
    lower_inclusive: bool,
    upper_inclusive: bool,
    sources: Vec<ScalarIR>,
}

fn eval_scalar(expr: &Expr) -> Result<ScalarValue> {
    let schema = Schema::empty();
    let expression = lower_with_global_columns(expr, &schema, &schema)?;
    let batch = RecordBatch::try_new_with_options(
        Arc::new(schema),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    let value = expression.evaluate(&batch)?;
    match value {
        ColumnarValue::Scalar(value) => Ok(value),
        ColumnarValue::Array(array) => Ok(ScalarValue::try_from_array(array.as_ref(), 0)?),
    }
}
fn scalar_boolean(value: &ScalarValue) -> Result<bool> {
    match value {
        ScalarValue::Boolean(Some(value)) => Ok(*value),
        other => Err(TableError::Invalid(format!(
            "interval inclusivity must be a nonmissing bool, got {other:?}"
        ))),
    }
}
fn point_component_count(point_type: &Type) -> usize {
    match point_type {
        Type::Primitive(_) | Type::Locus { .. } => 1,
        Type::Struct { r#struct } => r#struct.len(),
        Type::Tuple { tuple } => tuple.len(),
        _ => 0,
    }
}

fn make_query(
    bounds: &IntervalBounds,
    point_type: &Type,
    binding: &IndexBinding,
) -> Result<QueryInterval> {
    let lower = endpoint(&bounds.lower, point_type, binding, bounds.lower_inclusive)?;
    let upper = endpoint(&bounds.upper, point_type, binding, bounds.upper_inclusive)?;
    Ok(QueryInterval { lower, upper })
}
fn endpoint(
    value: &ScalarValue,
    point_type: &Type,
    binding: &IndexBinding,
    inclusive: bool,
) -> Result<Option<Endpoint>> {
    let array = value.to_array_of_size(1)?;
    if array.is_null(0) {
        return Ok(Some(Endpoint {
            key: vec![KeyValue::Missing; point_component_count(point_type)],
            inclusive,
        }));
    }
    let children: Vec<(ArrayRef, Type)> = match point_type {
        Type::Primitive(_) | Type::Locus { .. } => vec![(array, point_type.clone())],
        Type::Struct { r#struct } => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or_else(|| TableError::Invalid("interval endpoint must be a struct".into()))?;
            structure
                .columns()
                .iter()
                .cloned()
                .zip(r#struct.iter().map(|(_, t)| t.clone()))
                .collect()
        }
        Type::Tuple { tuple } => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or_else(|| {
                    TableError::Invalid("tuple interval endpoint must be a struct".into())
                })?;
            structure
                .columns()
                .iter()
                .cloned()
                .zip(tuple.iter().cloned())
                .collect()
        }
        _ => return Ok(None),
    };
    if children.len() != binding.key_fields.len() {
        return Ok(None);
    }
    let key = children
        .into_iter()
        .zip(&binding.key_fields)
        .map(|((array, _logical), field)| key_value(&array, &field.data_type))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(Endpoint { key, inclusive }))
}
fn key_value(array: &ArrayRef, key_type: &KeyType) -> Result<KeyValue> {
    if array.is_null(0) {
        return Ok(KeyValue::Missing);
    }
    Ok(match key_type {
        KeyType::Int32 => KeyValue::Int32(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .ok_or_else(|| TableError::Invalid("int32 interval key expected".into()))?
                .value(0),
        ),
        KeyType::Int64 => KeyValue::Int64(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| TableError::Invalid("int64 interval key expected".into()))?
                .value(0),
        ),
        KeyType::Bool => KeyValue::Bool(
            array
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or_else(|| TableError::Invalid("bool interval key expected".into()))?
                .value(0),
        ),
        KeyType::AsciiString => KeyValue::AsciiString(
            array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| TableError::Invalid("string interval key expected".into()))?
                .value(0)
                .to_owned(),
        ),
        KeyType::Canonical(logical_type) => KeyValue::Canonical(
            vail_ordering::encode_typed_value(array.as_ref(), 0, Some(logical_type))
                .map_err(|e| TableError::Invalid(e.to_string()))?,
        ),
    })
}

fn normalize_bounds(
    mut bounds: Vec<IntervalBounds>,
    point_type: &Type,
    binding: &IndexBinding,
) -> Result<Vec<IntervalBounds>> {
    let mut keyed = bounds
        .drain(..)
        .map(|bound| {
            let lower = endpoint_key(&bound.lower, point_type, binding)?;
            let upper = endpoint_key(&bound.upper, point_type, binding)?;
            Ok((lower, upper, bound))
        })
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|a, b| {
        compare_key(&a.0, &b.0, binding).then_with(|| b.2.lower_inclusive.cmp(&a.2.lower_inclusive))
    });
    let mut merged: Vec<(Vec<KeyValue>, Vec<KeyValue>, IntervalBounds)> = Vec::new();
    for (lower, upper, mut current) in keyed {
        if let Some((_, prior_upper, prior)) = merged.last_mut() {
            let relation = compare_key(&lower, prior_upper, binding);
            if relation == std::cmp::Ordering::Less
                || (relation == std::cmp::Ordering::Equal
                    // Keep adjacent intervals as distinct output partitions.
                    // Only merge a shared endpoint when both intervals include
                    // it, since otherwise the key is not duplicated.
                    && (prior.upper_inclusive && current.lower_inclusive))
            {
                let end_relation = compare_key(&upper, prior_upper, binding);
                if end_relation == std::cmp::Ordering::Greater {
                    prior.upper = current.upper.clone();
                    prior.upper_inclusive = current.upper_inclusive;
                    *prior_upper = upper;
                } else if end_relation == std::cmp::Ordering::Equal {
                    prior.upper_inclusive |= current.upper_inclusive;
                }
                prior.sources.append(&mut current.sources);
                continue;
            }
        }
        merged.push((lower, upper, current));
    }
    Ok(merged.into_iter().map(|(_, _, bound)| bound).collect())
}

fn endpoint_key(
    value: &ScalarValue,
    point_type: &Type,
    binding: &IndexBinding,
) -> Result<Vec<KeyValue>> {
    let array = value.to_array_of_size(1)?;
    if array.is_null(0) {
        return Ok(vec![KeyValue::Missing; point_component_count(point_type)]);
    }
    let components: Vec<(ArrayRef, Type)> = match point_type {
        Type::Primitive(_) | Type::Locus { .. } => vec![(array, point_type.clone())],
        Type::Struct { r#struct } => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or_else(|| TableError::Invalid("interval endpoint must be a struct".into()))?;
            structure
                .columns()
                .iter()
                .cloned()
                .zip(r#struct.iter().map(|(_, ty)| ty.clone()))
                .collect()
        }
        Type::Tuple { tuple } => {
            let structure = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or_else(|| {
                    TableError::Invalid("tuple interval endpoint must be a struct".into())
                })?;
            structure
                .columns()
                .iter()
                .cloned()
                .zip(tuple.iter().cloned())
                .collect()
        }
        _ => {
            return Err(TableError::Invalid(
                "unsupported interval point type".into(),
            ));
        }
    };
    if components.len() > binding.key_fields.len() {
        return Err(TableError::Invalid(
            "interval point has more fields than the table key".into(),
        ));
    }
    components
        .into_iter()
        .zip(&binding.key_fields)
        .map(|((array, _logical), field)| key_value(&array, &field.data_type))
        .collect()
}
fn compare_key(a: &[KeyValue], b: &[KeyValue], binding: &IndexBinding) -> std::cmp::Ordering {
    for ((left, right), field) in a.iter().zip(b).zip(&binding.key_fields) {
        let mut order = match (left, right) {
            (KeyValue::Missing, KeyValue::Missing) => std::cmp::Ordering::Equal,
            (KeyValue::Missing, _) => {
                if field.nulls_last {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (_, KeyValue::Missing) => {
                if field.nulls_last {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (KeyValue::Int32(a), KeyValue::Int32(b)) => a.cmp(b),
            (KeyValue::Int64(a), KeyValue::Int64(b)) => a.cmp(b),
            (KeyValue::Bool(a), KeyValue::Bool(b)) => a.cmp(b),
            (KeyValue::AsciiString(a), KeyValue::AsciiString(b)) => a.cmp(b),
            (KeyValue::Canonical(a), KeyValue::Canonical(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        };
        if field.descending {
            order = order.reverse();
        }
        if order != std::cmp::Ordering::Equal {
            return order;
        }
    }
    a.len().cmp(&b.len())
}

/// Composite storage bounds sort missing key fields last. Represent presence
/// explicitly to retain that order with native DataFusion struct comparisons.
pub(crate) fn ordered_key_point(value: Expr, point_type: &Type) -> Expr {
    if let Type::Struct { r#struct } = point_type {
        Expr::Struct {
            fields: r#struct
                .iter()
                .map(|(name, _)| {
                    let field = Expr::GetField {
                        value: Box::new(value.clone()),
                        name: name.clone(),
                    };
                    (
                        name.clone(),
                        Expr::Struct {
                            fields: vec![
                                (
                                    "missing".into(),
                                    Expr::IsMissing {
                                        value: Box::new(field.clone()),
                                    },
                                ),
                                ("value".into(), field),
                            ],
                        },
                    )
                })
                .collect(),
        }
    } else {
        value
    }
}
pub(crate) fn ordered_key_interval(value: Expr, point_type: &Type) -> Expr {
    let field = |name: &str| Expr::GetField {
        value: Box::new(value.clone()),
        name: name.into(),
    };
    Expr::Interval {
        start: Box::new(ordered_key_point(field("start"), point_type)),
        end: Box::new(ordered_key_point(field("end"), point_type)),
        includes_start: Box::new(field("includes_start")),
        includes_end: Box::new(field("includes_end")),
    }
}

fn union_predicate(intervals: &[ScalarIR], keys: &[String], point_type: &Type) -> Result<Expr> {
    let point = match point_type {
        Type::Primitive(_) | Type::Locus { .. } => Expr::Column {
            name: keys
                .first()
                .ok_or_else(|| TableError::Invalid("interval read has no key".into()))?
                .clone(),
        },
        Type::Struct { r#struct } => Expr::Struct {
            fields: r#struct
                .iter()
                .zip(keys)
                .map(|((name, _), key)| (name.clone(), Expr::Column { name: key.clone() }))
                .collect(),
        },
        Type::Tuple { .. } => {
            return Err(TableError::Invalid(
                "tuple interval residual filtering is unsupported".into(),
            ));
        }
        _ => {
            return Err(TableError::Invalid(
                "unsupported interval point type".into(),
            ));
        }
    };
    let mut predicates = intervals.iter().map(|interval| Expr::IntervalContains {
        interval: Box::new(ordered_key_interval(interval.clone(), point_type)),
        point: Box::new(ordered_key_point(point.clone(), point_type)),
    });
    let mut result = predicates
        .next()
        .ok_or_else(|| TableError::Invalid("interval query list is empty".into()))?;
    for predicate in predicates {
        result = Expr::Binary {
            operator: vail_execution::expressions::BinaryOperator::Or,
            left: Box::new(result),
            right: Box::new(predicate),
        };
    }
    Ok(result)
}
fn apply_filter(rows: PhysicalPlan, predicate: &Expr, child: &TableRef) -> Result<PhysicalPlan> {
    let expression =
        lower_with_global_columns(predicate, &rows.schema(), child.globals().schema())?;
    Ok(Arc::new(FilterExec::try_new(expression, rows)?))
}
fn storage_error(error: vail_storage::StorageError) -> DataFusionError {
    DataFusionError::External(Box::new(TableError::Storage(error)))
}
