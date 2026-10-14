//! Whole-table aggregation lowers to DataFusion relations. Each distinct
//! filter/explode scope has one AggregateExec, with all its reducers combined.
//! Their singleton results are projected with scalar expressions and globals.
use crate::globals::GlobalBindings;
use crate::plan::{PhysicalPlan, bind_external, column};
use crate::{Result, TableError, TableRef};
use arrow_array::{Array, ArrayRef, StructArray, make_array};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{NullHandling, ScalarValue, UnnestOptions};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{
    PhysicalExpr, aggregate::AggregateExprBuilder, expressions::Literal,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    filter::FilterExec,
    projection::ProjectionExec,
    unnest::{ListUnnest, UnnestExec},
};
use std::{
    collections::{BTreeMap, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};
use vail_execution::expressions::{Expr, PrimitiveType, Type, lower_with_bindings};

fn empty_struct_scalar(missing: bool) -> ScalarValue {
    let nulls = missing.then(|| NullBuffer::new_null(1));
    ScalarValue::Struct(Arc::new(StructArray::new_empty_fields(1, nulls)))
}

/// Restore a logical Arrow field type when the physical buffers already have
/// the same layout. Arrow's cast kernel cannot cast every zero-field nested
/// struct (notably the List<Dict> shape produced by grouped collection), while
/// changing only field metadata/names does not require touching those buffers.
fn retag_array(array: &ArrayRef, dtype: &DataType) -> datafusion_common::Result<ArrayRef> {
    if vail_ordering::same_layout(array.data_type(), dtype) {
        fn retag_data(
            data: arrow_data::ArrayData,
            dtype: &DataType,
        ) -> datafusion_common::Result<arrow_data::ArrayData> {
            let child_types = match dtype {
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => vec![field.data_type()],
                DataType::Struct(fields) => fields.iter().map(|field| field.data_type()).collect(),
                _ => Vec::new(),
            };
            let children = data
                .child_data()
                .iter()
                .zip(child_types)
                .map(|(child, child_type)| retag_data(child.clone(), child_type))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            Ok(data
                .into_builder()
                .data_type(dtype.clone())
                .child_data(children)
                .build()?)
        }
        return Ok(make_array(retag_data(array.to_data(), dtype)?));
    }
    Ok(arrow_cast::cast(array.as_ref(), dtype)?)
}

#[derive(Debug)]
struct RetagExpr {
    input: Arc<dyn PhysicalExpr>,
    dtype: DataType,
}

impl RetagExpr {
    fn new(input: Arc<dyn PhysicalExpr>, dtype: DataType) -> Self {
        Self { input, dtype }
    }
}

impl PartialEq for RetagExpr {
    fn eq(&self, other: &Self) -> bool {
        self.input.as_ref().dyn_eq(other.input.as_ref()) && self.dtype == other.dtype
    }
}
impl Eq for RetagExpr {}
impl Hash for RetagExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.dtype.hash(state);
    }
}
impl std::fmt::Display for RetagExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Retag({})", self.input)
    }
}
impl PhysicalExpr for RetagExpr {
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
    fn data_type(&self, _: &Schema) -> datafusion_common::Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, schema: &Schema) -> datafusion_common::Result<bool> {
        self.input.nullable(schema)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "retag expression child count mismatch".into(),
            ));
        }
        Ok(Arc::new(Self::new(children.remove(0), self.dtype.clone())))
    }
    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion_common::Result<ColumnarValue> {
        let array = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(retag_array(&array, &self.dtype)?))
    }
}

pub(crate) fn logical_group_key(
    value: datafusion_expr::Expr,
    dtype: &DataType,
) -> Result<datafusion_expr::Expr> {
    if matches!(dtype, DataType::Struct(fields) if fields.is_empty()) {
        return Ok(datafusion_expr::expr_fn::when(
            value.is_null(),
            datafusion_expr::lit(ScalarValue::Boolean(None)),
        )
        .otherwise(datafusion_expr::lit(ScalarValue::Boolean(Some(true))))?);
    }
    Ok(value)
}

pub(crate) fn logical_restore_group_key(
    value: datafusion_expr::Expr,
    dtype: &DataType,
) -> Result<datafusion_expr::Expr> {
    if matches!(dtype, DataType::Struct(fields) if fields.is_empty()) {
        return Ok(datafusion_expr::expr_fn::when(
            value.is_null(),
            datafusion_expr::lit(empty_struct_scalar(true)),
        )
        .otherwise(datafusion_expr::lit(empty_struct_scalar(false)))?);
    }
    Ok(vail_execution::logical_expr::restore_type(
        value,
        dtype.clone(),
    ))
}

fn physical_group_key(input: &PhysicalPlan, name: &str) -> Result<Arc<dyn PhysicalExpr>> {
    let schema = input.schema();
    let key: Arc<dyn PhysicalExpr> = column(input, schema.index_of(name)?);
    let DataType::Struct(fields) = schema.field_with_name(name)?.data_type() else {
        return Ok(key);
    };
    if !fields.is_empty() {
        return Ok(key);
    }
    use datafusion_physical_expr::expressions::{CaseExpr, IsNullExpr};
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(
            Arc::new(IsNullExpr::new(key)),
            Arc::new(Literal::new(ScalarValue::Boolean(None))),
        )],
        Some(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))),
    )?))
}

fn restore_physical_group_key(
    value: Arc<dyn PhysicalExpr>,
    dtype: &DataType,
) -> Result<Arc<dyn PhysicalExpr>> {
    if matches!(dtype, DataType::Struct(fields) if fields.is_empty()) {
        use datafusion_physical_expr::expressions::{CaseExpr, IsNullExpr};
        return Ok(Arc::new(CaseExpr::try_new(
            None,
            vec![(
                Arc::new(IsNullExpr::new(value)),
                Arc::new(Literal::new(empty_struct_scalar(true))),
            )],
            Some(Arc::new(Literal::new(empty_struct_scalar(false)))),
        )?));
    }
    Ok(Arc::new(
        datafusion_physical_expr::expressions::CastExpr::new(value, dtype.clone(), None),
    ))
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Step {
    Filter(Expr),
    Explode { value: Expr, parameter: String },
}
pub(crate) struct Reducer {
    pub(crate) name: String,
    pub(crate) function: String,
    pub(crate) value: Option<Expr>,
    pub(crate) options: serde_json::Value,
}
pub(crate) struct Group {
    pub(crate) steps: Vec<Step>,
    pub(crate) reducers: Vec<Reducer>,
}

fn fresh(used: &mut HashSet<String>, base: &str) -> String {
    let mut name = base.to_string();
    while !used.insert(name.clone()) {
        name.push('_');
    }
    name
}

/// Remove aggregate nodes from the final scalar expression, assigning a private
/// result column to each reducer. Literal payloads are never walked as IR.
pub(crate) fn extract(
    expr: &Expr,
    steps: &[Step],
    groups: &mut Vec<Group>,
    used: &mut HashSet<String>,
) -> Result<Expr> {
    match expr {
        Expr::AggregateFold { .. } | Expr::AggGroupBy { .. } | Expr::AggArray { .. } => {
            let index = match groups.iter().position(|g| g.steps == steps) {
                Some(i) => i,
                None => {
                    groups.push(Group {
                        steps: steps.to_vec(),
                        reducers: vec![],
                    });
                    groups.len() - 1
                }
            };
            let name = fresh(used, "__vail_nested_aggregate");
            groups[index].reducers.push(Reducer {
                name: name.clone(),
                function: if matches!(expr, Expr::AggregateFold { .. }) {
                    "__fold"
                } else {
                    "__nested"
                }
                .into(),
                value: Some(expr.clone()),
                options: serde_json::Value::Null,
            });
            Ok(Expr::Column { name })
        }
        Expr::Aggregate {
            function,
            value,
            options,
            init,
        } => {
            if !init.is_empty() {
                return Err(TableError::Invalid(
                    "aggregate initializer was not bound".into(),
                ));
            }
            let index = match groups.iter().position(|g| g.steps == steps) {
                Some(index) => index,
                None => {
                    groups.push(Group {
                        steps: steps.to_vec(),
                        reducers: vec![],
                    });
                    groups.len() - 1
                }
            };
            if let Some(reducer) = groups[index].reducers.iter().find(|r| {
                r.function == *function
                    && r.value.as_ref() == value.as_deref()
                    && r.options == *options
            }) {
                return Ok(Expr::Column {
                    name: reducer.name.clone(),
                });
            }
            let name = fresh(used, "__vail_aggregate");
            groups[index].reducers.push(Reducer {
                name: name.clone(),
                function: function.clone(),
                value: value.as_deref().cloned(),
                options: options.clone(),
            });
            Ok(Expr::Column { name })
        }
        Expr::AggFilter { predicate, body } => {
            let mut nested = steps.to_vec();
            nested.push(Step::Filter((**predicate).clone()));
            let mut has_aggregate = false;
            body.visit(&mut |node| {
                has_aggregate |= matches!(
                    node,
                    Expr::Aggregate { .. }
                        | Expr::AggregateFold { .. }
                        | Expr::AggGroupBy { .. }
                        | Expr::AggArray { .. }
                )
            });
            if !has_aggregate {
                return Err(TableError::Invalid(
                    "agg.filter body must contain an aggregator".into(),
                ));
            }
            extract(body, &nested, groups, used)
        }
        Expr::AggExplode {
            value,
            parameter,
            body,
        } => {
            let mut nested = steps.to_vec();
            nested.push(Step::Explode {
                value: (**value).clone(),
                parameter: parameter.clone(),
            });
            let mut has_aggregate = false;
            body.visit(&mut |node| {
                has_aggregate |= matches!(
                    node,
                    Expr::Aggregate { .. }
                        | Expr::AggregateFold { .. }
                        | Expr::AggGroupBy { .. }
                        | Expr::AggArray { .. }
                )
            });
            if !has_aggregate {
                return Err(TableError::Invalid(
                    "agg.explode body must contain an aggregator".into(),
                ));
            }
            extract(body, &nested, groups, used)
        }
        Expr::Column { .. } => Err(TableError::Invalid(
            "row fields must be inside an aggregator in Table.aggregate".into(),
        )),
        _ => expr.try_map_children(|child| extract(child, steps, groups, used)),
    }
}

/// Exploded variables become private relational columns. Array lambda variables
/// stay lexical and continue through the existing scalar-expression lowering.
pub(crate) fn resolve(expr: &Expr, scope: &BTreeMap<String, String>) -> Result<Expr> {
    match expr {
        Expr::LambdaVariable { name } if scope.contains_key(name) => Ok(Expr::Column {
            name: scope[name].clone(),
        }),
        Expr::ArrayMap { parameter, .. }
        | Expr::ArrayFilter { parameter, .. }
        | Expr::ArrayPredicate { parameter, .. }
            if scope.contains_key(parameter) =>
        {
            Err(TableError::Invalid(
                "duplicate lambda parameter identity".into(),
            ))
        }
        _ => expr.try_map_children(|child| resolve(child, scope)),
    }
}

fn lower_group(table: &TableRef, group: &Group, bindings: &GlobalBindings) -> Result<PhysicalPlan> {
    Ok(lower_scope(table, group, bindings, &[])?.0)
}

fn lower_scope(
    table: &TableRef,
    group: &Group,
    bindings: &GlobalBindings,
    keys: &[(String, Expr)],
) -> Result<(PhysicalPlan, Vec<ScalarValue>)> {
    let mut input = crate::plan::lower(table)?.rows;
    if !table.globals().schema().fields().is_empty() {
        input = crate::plan::cross_join(table.globals().lower()?, input)?;
    }
    let (mut input, indices) = bind_external(input, bindings)?;
    let logical_fields = table.schema().fields().to_vec();
    let globals = table.globals().schema();
    // Evaluate grouping expressions in the original row scope, before filters
    // and explode alter the aggregate input. Private aliases avoid shadowing.
    if !keys.is_empty() {
        let mut projections = (0..input.schema().fields().len())
            .map(|i| (column(&input, i), input.schema().field(i).name().clone()))
            .collect::<Vec<_>>();
        for (name, expr) in keys {
            expr.dtype_with_globals(table.schema(), globals)?;
            projections.push((
                lower_with_bindings(expr, &input.schema(), globals, &indices)?,
                name.clone(),
            ));
        }
        input = Arc::new(ProjectionExec::try_new(projections, input)?);
    }
    lower_bound_scope(
        table,
        input,
        logical_fields,
        BTreeMap::new(),
        &indices,
        group,
        keys,
        bindings,
    )
}

fn lower_bound_scope(
    table: &TableRef,
    mut input: PhysicalPlan,
    mut logical_fields: Vec<Arc<Field>>,
    mut scope: BTreeMap<String, String>,
    indices: &BTreeMap<String, usize>,
    group: &Group,
    keys: &[(String, Expr)],
    bindings: &GlobalBindings,
) -> Result<(PhysicalPlan, Vec<ScalarValue>)> {
    let globals = table.globals().schema();
    let mut used = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for step in &group.steps {
        let logical = Schema::new(logical_fields.clone());
        match step {
            Step::Filter(predicate) => {
                let predicate = resolve(predicate, &scope)?;
                if predicate.dtype_with_globals(&logical, globals)?
                    != Type::Primitive(PrimitiveType::Bool)
                {
                    return Err(TableError::Invalid(
                        "agg.filter predicate requires bool".into(),
                    ));
                }
                let predicate =
                    lower_with_bindings(&predicate, &input.schema(), globals, &indices)?;
                input = Arc::new(FilterExec::try_new(predicate, input)?);
            }
            Step::Explode { value, parameter } => {
                if scope.contains_key(parameter) {
                    return Err(TableError::Invalid(
                        "duplicate aggregation lambda parameter identity".into(),
                    ));
                }
                let value = resolve(value, &scope)?;
                let element = match value.dtype_with_globals(&logical, globals)? {
                    Type::Array { array } | Type::Set { set: array } => *array,
                    _ => {
                        return Err(TableError::Invalid(
                            "agg.explode requires an array or set".into(),
                        ));
                    }
                };
                let name = fresh(&mut used, "__vail_exploded");
                let expression = lower_with_bindings(&value, &input.schema(), globals, &indices)?;
                let index = input.schema().fields().len();
                let mut projections = (0..index)
                    .map(|i| (column(&input, i), input.schema().field(i).name().clone()))
                    .collect::<Vec<_>>();
                projections.push((expression, name.clone()));
                input = Arc::new(ProjectionExec::try_new(projections, input)?);
                let mut fields = input.schema().fields().to_vec();
                fields[index] = Arc::new(Field::new(&name, element.to_arrow()?, true));
                input = Arc::new(UnnestExec::new(
                    input,
                    vec![ListUnnest {
                        index_in_input_schema: index,
                        depth: 1,
                    }],
                    vec![],
                    Arc::new(Schema::new(fields)),
                    UnnestOptions::new().with_null_handling(NullHandling::Drop),
                )?);
                logical_fields.push(Arc::new(Field::new(&name, element.to_arrow()?, true)));
                scope.insert(parameter.clone(), name);
            }
        }
    }
    let logical = Schema::new(logical_fields.clone());
    let nested_input = input.clone();
    let schema = input.schema();
    let mut aggregates = Vec::new();
    let mut defaults = Vec::new();
    for reducer in &group.reducers {
        if reducer.function == "__nested" {
            continue;
        }
        if reducer.function == "__fold" {
            let (udf, value) = lower_fold(
                reducer.value.as_ref().unwrap(),
                &input,
                &logical,
                globals,
                indices,
                &scope,
            )?;
            let aggregate = AggregateExprBuilder::new(udf, vec![value])
                .schema(schema.clone())
                .alias(&reducer.name)
                .build()?;
            defaults.push(aggregate.create_accumulator()?.evaluate()?);
            aggregates.push(Arc::new(aggregate));
            continue;
        }
        let (value, input_type): (Arc<dyn PhysicalExpr>, Option<DataType>) = match &reducer.value {
            Some(value) if reducer.function != "count" => {
                let value = resolve(value, &scope)?;
                let dtype = value.dtype_with_globals(&logical, globals)?.to_arrow()?;
                (
                    lower_with_bindings(&value, &schema, globals, &indices)?,
                    Some(dtype),
                )
            }
            None if reducer.function == "count" => {
                (Arc::new(Literal::new(ScalarValue::Int64(Some(1)))), None)
            }
            _ => {
                return Err(TableError::Invalid(
                    "aggregate argument count mismatch".into(),
                ));
            }
        };
        let function = if reducer.function == "collect" {
            vail_execution::collect_aggregate::collect_udf(
                input_type
                    .as_ref()
                    .ok_or_else(|| TableError::Invalid("collect requires an argument".into()))?,
            )
        } else if let Some(udf) = input_type
            .as_ref()
            .map(|ty| {
                vail_execution::advanced_collection_aggregates::make_udaf(
                    &reducer.function,
                    ty,
                    &reducer.options,
                )
            })
            .transpose()?
            .flatten()
        {
            udf
        } else if let Some(udf) = input_type
            .as_ref()
            .map(|ty| {
                vail_execution::advanced_numeric_aggregates::make_udaf(
                    &reducer.function,
                    ty,
                    &reducer.options,
                )
            })
            .transpose()?
            .flatten()
        {
            udf
        } else if let Some(udf) = input_type
            .as_ref()
            .map(|ty| {
                vail_execution::advanced_genomic_aggregates::make_udaf(
                    &reducer.function,
                    ty,
                    &reducer.options,
                )
            })
            .transpose()?
            .flatten()
        {
            udf
        } else {
            vail_execution::aggregates::aggregate_udf(&reducer.function, input_type.as_ref())?
        };
        let aggregate = AggregateExprBuilder::new(function, vec![value])
            .schema(schema.clone())
            .alias(&reducer.name)
            .build()?;
        defaults.push(aggregate.create_accumulator()?.evaluate()?);
        aggregates.push(Arc::new(aggregate));
    }
    let grouping = keys
        .iter()
        .map(|(name, _)| Ok((physical_group_key(&input, name)?, name.clone())))
        .collect::<Result<Vec<_>>>()?;
    if aggregates.is_empty() && keys.is_empty() {
        // DataFusion's zero-column aggregate batch cannot represent its one
        // output row. Carry a private count until the result projection.
        let udf = vail_execution::aggregates::aggregate_udf("count", None)?;
        let aggregate = AggregateExprBuilder::new(
            udf,
            vec![Arc::new(Literal::new(ScalarValue::Int64(Some(1))))],
        )
        .schema(schema.clone())
        .alias("__vail_domain_count")
        .build()?;
        aggregates.push(Arc::new(aggregate));
    }
    let representative_types = keys
        .iter()
        .map(|(name, _)| Ok(schema.field_with_name(name)?.data_type().clone()))
        .collect::<Result<Vec<_>>>()?;
    let filters = vec![None; aggregates.len()];
    let mut result: PhysicalPlan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(grouping),
        aggregates,
        filters,
        input,
        schema,
    )?);
    // Native grouping preserves the typed keys directly.
    let key_count = keys.len();
    let mut restored = keys
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            Ok((
                restore_physical_group_key(column(&result, i), &representative_types[i])?,
                name.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    for i in key_count..result.schema().fields().len() {
        restored.push((column(&result, i), result.schema().field(i).name().clone()));
    }
    result = Arc::new(ProjectionExec::try_new(restored, result)?);
    let mut named_defaults = group
        .reducers
        .iter()
        .filter(|r| r.function != "__nested")
        .map(|r| r.name.clone())
        .zip(defaults)
        .collect::<BTreeMap<_, _>>();
    for reducer in group.reducers.iter().filter(|r| r.function == "__nested") {
        let (right, default) = lower_nested(
            table,
            nested_input.clone(),
            logical_fields.clone(),
            scope.clone(),
            indices,
            keys,
            reducer.value.as_ref().unwrap(),
            &reducer.name,
            bindings,
        )?;
        result = join_results(
            result,
            right,
            keys.len(),
            &[(reducer.name.clone(), default.clone())],
        )?;
        named_defaults.insert(reducer.name.clone(), default);
    }
    let mut projection = keys
        .iter()
        .enumerate()
        .map(|(i, (name, _))| (column(&result, i), name.clone()))
        .collect::<Vec<_>>();
    for reducer in &group.reducers {
        projection.push((
            column(&result, result.schema().index_of(&reducer.name)?),
            reducer.name.clone(),
        ));
    }
    let defaults = group
        .reducers
        .iter()
        .map(|r| named_defaults[&r.name].clone())
        .collect();
    Ok((
        Arc::new(ProjectionExec::try_new(projection, result)?),
        defaults,
    ))
}

/// Bind scalar aggregate initialization independently of the row relation.
/// During validation we substitute typed placeholders solely to obtain the
/// output schema. At execution a DataFusion projection evaluates the actual
/// expressions on the singleton globals relation before any row scan starts.
pub(crate) fn bind_initializers(
    expr: &Expr,
    bind: &mut impl FnMut(&str, &Expr) -> Result<ScalarValue>,
) -> Result<Expr> {
    let mut result = expr.clone();
    match &mut result {
        Expr::Aggregate { init, options, .. } => {
            if !init.is_empty() && options.is_null() {
                *options = serde_json::json!({});
            }
            let mut names = HashSet::new();
            for (name, expression) in std::mem::take(init) {
                if !names.insert(name.clone()) {
                    return Err(TableError::Invalid(format!(
                        "duplicate aggregate initializer {name:?}"
                    )));
                }
                let value = bind(&name, &expression)?;
                if name == "n_alleles" && value.is_null() {
                    return Err(TableError::Invalid(
                        "hl.agg.call_stats: n_alleles may not be missing".into(),
                    ));
                }
                options
                    .as_object_mut()
                    .ok_or_else(|| {
                        TableError::Invalid("aggregate options must be an object".into())
                    })?
                    .insert(name, initializer_option(&value)?);
            }
        }
        Expr::AggregateFold { initial, .. }
            if !matches!(initial.as_ref(), Expr::RuntimeLiteral { .. }) =>
        {
            **initial = Expr::RuntimeLiteral {
                value: bind("fold", initial)?,
            };
        }
        _ => (),
    }
    result.try_map_children(|child| bind_initializers(child, bind))
}

// Runtime fold states stay as Arrow ScalarValues. The existing library UDAF
// factories take numeric configuration; only those small options use JSON.
fn initializer_option(value: &ScalarValue) -> Result<serde_json::Value> {
    use serde_json::{Value, json};
    if value.is_null() {
        return Ok(Value::Null);
    }
    Ok(match value {
        ScalarValue::Int32(Some(v)) => json!(v),
        ScalarValue::Int64(Some(v)) => json!(v),
        ScalarValue::Float32(Some(v)) if v.is_finite() => json!(v),
        ScalarValue::Float64(Some(v)) if v.is_finite() => json!(v),
        ScalarValue::List(a) => {
            let values = a.value(0);
            Value::Array(
                (0..values.len())
                    .map(|i| {
                        initializer_option(&vail_execution::expressions::scalar_at(&values, i)?)
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        _ => {
            return Err(TableError::Invalid(format!(
                "unsupported aggregate initializer type {:?}",
                value.data_type()
            )));
        }
    })
}

pub(crate) fn initializer_placeholder(name: &str, dtype: &DataType) -> Result<ScalarValue> {
    Ok(match name {
        "start" => ScalarValue::Float64(Some(0.0)),
        "end" => ScalarValue::Float64(Some(1.0)),
        "bins" => ScalarValue::Int32(Some(1)),
        "n_alleles" => ScalarValue::Int32(Some(0)),
        "quantiles" => ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Float64(Some(0.5))],
            &DataType::Float64,
            true,
        )),
        "fold" => ScalarValue::try_from(dtype)?,
        _ => {
            return Err(TableError::Invalid(format!(
                "unknown aggregate initializer {name:?}"
            )));
        }
    })
}

fn initialize(
    table: &TableRef,
    expressions: &[(String, Expr)],
    bindings: &GlobalBindings,
    keys: Option<&[(String, Expr)]>,
) -> Result<Option<PhysicalPlan>> {
    let mut parameters = Vec::new();
    let placeholders = expressions
        .iter()
        .map(|(name, expr)| {
            let expr = bind_initializers(expr, &mut |parameter, value| {
                // Empty row schema rejects row references but permits lexical
                // array lambdas and references to this table's or captured globals.
                let dtype = value
                    .dtype_with_globals(&Schema::empty(), table.globals().schema())?
                    .to_arrow()?;
                parameters.push(value.clone());
                initializer_placeholder(parameter, &dtype)
            })?;
            Ok((name.clone(), expr))
        })
        .collect::<Result<Vec<_>>>()?;
    if parameters.is_empty() {
        return Ok(None);
    }
    let (globals, indices) = bind_external(table.globals().lower()?, bindings)?;
    let projection = parameters
        .iter()
        .enumerate()
        .map(|(i, value)| {
            Ok((
                lower_with_bindings(value, &globals.schema(), table.globals().schema(), &indices)?,
                format!("init_{i}"),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let initializers: PhysicalPlan = Arc::new(ProjectionExec::try_new(projection, globals)?);
    let template = match keys {
        Some(keys) => lower_grouped(table, keys, &placeholders, bindings)?,
        None => lower(table, &placeholders[0].1, bindings)?,
    };
    let table = table.clone();
    let expressions = expressions.to_vec();
    let keys = keys.map(<[_]>::to_vec);
    let bindings = bindings.clone();
    let build = Arc::new(move |batch: arrow_array::RecordBatch| {
        let build = || -> Result<PhysicalPlan> {
            let mut index = 0;
            let expressions = expressions
                .iter()
                .map(|(name, expr)| {
                    let expr = bind_initializers(expr, &mut |_, _| {
                        let value = vail_execution::expressions::scalar_at(batch.column(index), 0)?;
                        index += 1;
                        Ok(value)
                    })?;
                    Ok((name.clone(), expr))
                })
                .collect::<Result<Vec<_>>>()?;
            match &keys {
                Some(keys) => lower_grouped(&table, keys, &expressions, &bindings),
                None => lower(&table, &expressions[0].1, &bindings),
            }
        };
        build().map_err(|err| datafusion_common::DataFusionError::External(Box::new(err)))
    });
    Ok(Some(crate::initialized_aggregate::initialized_aggregate(
        initializers,
        template,
        build,
    )))
}

/// Compile without reading any rows. The result always has one column `value`
/// and one row, including for empty input and expressions with no aggregators.
pub fn lower(
    table: &TableRef,
    expression: &Expr,
    bindings: &GlobalBindings,
) -> Result<PhysicalPlan> {
    if let Some(plan) = initialize(
        table,
        &[("value".into(), expression.clone())],
        bindings,
        None,
    )? {
        return Ok(plan);
    }
    let mut used = table
        .schema()
        .fields()
        .iter()
        .chain(table.globals().schema().fields())
        .map(|f| f.name().clone())
        .collect();
    let mut groups = Vec::new();
    let output = extract(expression, &[], &mut groups, &mut used)?;
    let mut input = table.globals().lower()?;
    for group in groups {
        input = crate::plan::cross_join(input, lower_group(table, &group, bindings)?)?;
    }
    let (input, indices) = bind_external(input, bindings)?;
    let expression =
        lower_with_bindings(&output, &input.schema(), table.globals().schema(), &indices)?;
    Ok(Arc::new(ProjectionExec::try_new(
        vec![(expression, "value".into())],
        input,
    )?))
}

/// Group rows using DataFusion's hash aggregation. Separate aggregate scopes
/// are null-safe left-joined onto the complete key domain so filters/explode
/// cannot delete a group. Empty-scope identities come from the same UDAFs.
pub fn lower_grouped(
    table: &TableRef,
    keys: &[(String, Expr)],
    fields: &[(String, Expr)],
    bindings: &GlobalBindings,
) -> Result<PhysicalPlan> {
    if let Some(plan) = initialize(table, fields, bindings, Some(keys))? {
        return Ok(plan);
    }
    use datafusion_common::NullEquality;
    use datafusion_expr::JoinType;
    use datafusion_physical_expr::expressions::{CaseExpr, IsNullExpr};
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    let mut used: HashSet<String> = table
        .schema()
        .fields()
        .iter()
        .chain(table.globals().schema().fields())
        .map(|f| f.name().clone())
        .chain(keys.iter().chain(fields).map(|(name, _)| name.clone()))
        .collect();
    let mut private_keys = Vec::new();
    for (_, expr) in keys {
        private_keys.push((fresh(&mut used, "__vail_group_key"), expr.clone()));
    }
    // A constant grouping key gives zero groups for empty input, unlike a total
    // aggregation, which always emits one row.
    if private_keys.is_empty() {
        private_keys.push((
            fresh(&mut used, "__vail_group_key"),
            Expr::Literal {
                value: serde_json::json!(0),
                dtype: Type::Primitive(PrimitiveType::Int32),
            },
        ));
    }
    let mut groups = Vec::new();
    let outputs = fields
        .iter()
        .map(|(name, expr)| Ok((name.clone(), extract(expr, &[], &mut groups, &mut used)?)))
        .collect::<Result<Vec<_>>>()?;
    let anchor = groups
        .iter()
        .position(|g| g.steps.is_empty())
        .map(|i| groups.remove(i))
        .unwrap_or(Group {
            steps: vec![],
            reducers: vec![],
        });
    let (mut input, _) = lower_scope(table, &anchor, bindings, &private_keys)?;
    for group in groups {
        let (right, defaults) = lower_scope(table, &group, bindings, &private_keys)?;
        let offset = input.schema().fields().len();
        let left_names = input
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let on = (0..private_keys.len())
            .map(|i| (column(&input, i), column(&right, i)))
            .collect();
        let joined: PhysicalPlan = Arc::new(HashJoinExec::try_new(
            input,
            right,
            on,
            None,
            &JoinType::Left,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNull,
            false,
        )?);
        let mut projection = left_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| (column(&joined, i), name))
            .collect::<Vec<_>>();
        for (i, (reducer, default)) in group.reducers.iter().zip(defaults).enumerate() {
            let value = column(&joined, offset + private_keys.len() + i);
            let value: Arc<dyn PhysicalExpr> = if default.is_null() {
                value
            } else {
                Arc::new(CaseExpr::try_new(
                    None,
                    vec![(
                        Arc::new(IsNullExpr::new(value.clone())),
                        Arc::new(Literal::new(default)),
                    )],
                    Some(value),
                )?)
            };
            projection.push((value, reducer.name.clone()));
        }
        input = Arc::new(ProjectionExec::try_new(projection, joined)?);
    }
    // Globals are a singleton relation; attaching them after aggregation keeps
    // them available even when a scope has no qualifying rows.
    input = crate::plan::cross_join(input, table.globals().lower()?)?;
    let (input, indices) = bind_external(input, bindings)?;
    let mut projection = keys
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            Ok((
                column(&input, input.schema().index_of(&private_keys[i].0)?),
                name.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    for (name, expr) in outputs {
        projection.push((
            lower_with_bindings(&expr, &input.schema(), table.globals().schema(), &indices)?,
            name,
        ));
    }
    Ok(Arc::new(ProjectionExec::try_new(projection, input)?))
}

fn join_results(
    left: PhysicalPlan,
    right: PhysicalPlan,
    key_count: usize,
    fields: &[(String, ScalarValue)],
) -> Result<PhysicalPlan> {
    use datafusion_common::NullEquality;
    use datafusion_expr::JoinType;
    use datafusion_physical_expr::expressions::{CaseExpr, IsNullExpr};
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
    if key_count == 0 {
        return Ok(crate::plan::cross_join(left, right)?);
    }
    let names = left
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    let offset = names.len();
    // A reducer's NULL value is not a reliable indication that the right
    // scope did not match: a real group may produce NULL. Carry an explicit
    // non-null marker through the left join and use that to select empty-scope
    // identities.
    let mut marker = "__vail_scope_present".to_owned();
    while right.schema().index_of(&marker).is_ok() {
        marker.push('_');
    }
    let mut right_projection = (0..right.schema().fields().len())
        .map(|i| (column(&right, i), right.schema().field(i).name().clone()))
        .collect::<Vec<_>>();
    right_projection.push((
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        marker.clone(),
    ));
    let right: PhysicalPlan = Arc::new(ProjectionExec::try_new(right_projection, right)?);
    let marker_index = offset + right.schema().fields().len() - 1;
    let left_schema = left.schema();
    let right_schema = right.schema();
    let on = (0..key_count)
        .map(|i| {
            let left_name = left_schema.field(i).name();
            let right_name = right_schema.field(i).name();
            Ok((
                physical_group_key(&left, left_name)?,
                physical_group_key(&right, right_name)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let joined: PhysicalPlan = Arc::new(HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &JoinType::Left,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNull,
        false,
    )?);
    let mut projection = names
        .into_iter()
        .enumerate()
        .map(|(i, name)| (column(&joined, i), name))
        .collect::<Vec<_>>();
    for (i, (name, default)) in fields.iter().enumerate() {
        let value = column(&joined, offset + key_count + i);
        let value: Arc<dyn PhysicalExpr> = if default.is_null() {
            value
        } else {
            Arc::new(CaseExpr::try_new(
                None,
                vec![(
                    Arc::new(IsNullExpr::new(column(&joined, marker_index))),
                    Arc::new(Literal::new(default.clone())),
                )],
                Some(value),
            )?)
        };
        let dtype = value.data_type(&joined.schema())?;
        projection.push((
            vail_execution::expressions::restore_physical_type(value, dtype),
            name.clone(),
        ));
    }
    Ok(Arc::new(ProjectionExec::try_new(projection, joined)?))
}

#[cfg(test)]
mod scope_join_tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use datafusion_execution::TaskContext;
    use datafusion_physical_plan::test::TestMemoryExec;
    use futures::TryStreamExt;

    fn memory_plan(batch: RecordBatch) -> Result<PhysicalPlan> {
        let schema = batch.schema();
        Ok(TestMemoryExec::try_new_exec(&[vec![batch]], schema, None)?)
    }

    async fn joined_value(left_key: i32, right_key: i32) -> Result<ScalarValue> {
        let left_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let left = memory_plan(RecordBatch::try_new(
            left_schema,
            vec![Arc::new(Int32Array::from(vec![left_key]))],
        )?)?;
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]));
        let right = memory_plan(RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int32Array::from(vec![right_key])),
                Arc::new(Int32Array::from(vec![None::<i32>])),
            ],
        )?)?;
        let plan = join_results(
            left,
            right,
            1,
            &[("value".into(), ScalarValue::Int32(Some(99)))],
        )?;
        let mut stream = plan.execute(0, Arc::new(TaskContext::default()))?;
        let batch = stream
            .try_next()
            .await?
            .ok_or_else(|| TableError::Invalid("scope join returned no batch".into()))?;
        Ok(vail_execution::expressions::scalar_at(batch.column(1), 0)?)
    }

    #[tokio::test]
    async fn scoped_default_only_applies_when_the_scope_is_absent() {
        // The matching scope has a real row, but its reducer result is NULL.
        assert_eq!(joined_value(1, 1).await.unwrap(), ScalarValue::Int32(None));
        // A different key has no right-side group, so it receives the identity.
        assert_eq!(
            joined_value(2, 1).await.unwrap(),
            ScalarValue::Int32(Some(99))
        );
    }
}

/// Recursively compile an aggregation body over an already-bound relation. The
/// group keys are private projected columns; scalar result expressions can only
/// reference reducers and singleton globals, never an arbitrary input row.
fn lower_bound_body(
    table: &TableRef,
    input: PhysicalPlan,
    logical: Vec<Arc<Field>>,
    scope: BTreeMap<String, String>,
    indices: &BTreeMap<String, usize>,
    keys: &[(String, Expr)],
    body: &Expr,
    bindings: &GlobalBindings,
    output_name: &str,
) -> Result<PhysicalPlan> {
    let mut used = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut groups = vec![];
    let output = extract(body, &[], &mut groups, &mut used)?;
    let anchor = groups
        .iter()
        .position(|g| g.steps.is_empty())
        .map(|i| groups.remove(i))
        .unwrap_or(Group {
            steps: vec![],
            reducers: vec![],
        });
    let (mut result, _) = lower_bound_scope(
        table,
        input.clone(),
        logical.clone(),
        scope.clone(),
        indices,
        &anchor,
        keys,
        bindings,
    )?;
    for group in groups {
        let (right, defaults) = lower_bound_scope(
            table,
            input.clone(),
            logical.clone(),
            scope.clone(),
            indices,
            &group,
            keys,
            bindings,
        )?;
        result = join_results(
            result,
            right,
            keys.len(),
            &group
                .reducers
                .iter()
                .map(|r| r.name.clone())
                .zip(defaults)
                .collect::<Vec<_>>(),
        )?;
    }
    result = crate::plan::cross_join(table.globals().lower()?, result)?;
    let (result, bound) = bind_external(result, bindings)?;
    let mut projection = keys
        .iter()
        .map(|(name, _)| {
            Ok((
                column(&result, result.schema().index_of(name)?),
                name.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    projection.push((
        lower_with_bindings(&output, &result.schema(), table.globals().schema(), &bound)?,
        output_name.into(),
    ));
    Ok(Arc::new(ProjectionExec::try_new(projection, result)?))
}

fn lower_nested(
    table: &TableRef,
    mut input: PhysicalPlan,
    mut logical: Vec<Arc<Field>>,
    scope: BTreeMap<String, String>,
    indices: &BTreeMap<String, usize>,
    keys: &[(String, Expr)],
    expression: &Expr,
    name: &str,
    bindings: &GlobalBindings,
) -> Result<(PhysicalPlan, ScalarValue)> {
    let Expr::AggGroupBy { key, body } = expression else {
        return lower_array_aggregation(
            table, input, logical, scope, indices, keys, expression, name, bindings,
        );
    };
    let key = resolve(key, &scope)?;
    let dtype = key.dtype_with_globals(&Schema::new(logical.clone()), table.globals().schema())?;
    let mut used = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let key_name = fresh(&mut used, "__vail_subgroup_key");
    let value_name = fresh(&mut used, "__vail_subgroup_value");
    let mut projection = (0..input.schema().fields().len())
        .map(|i| (column(&input, i), input.schema().field(i).name().clone()))
        .collect::<Vec<_>>();
    projection.push((
        lower_with_bindings(&key, &input.schema(), table.globals().schema(), indices)?,
        key_name.clone(),
    ));
    input = Arc::new(ProjectionExec::try_new(projection, input)?);
    logical.push(Arc::new(Field::new(&key_name, dtype.to_arrow()?, true)));
    let mut nested_keys = keys.to_vec();
    nested_keys.push((
        key_name.clone(),
        Expr::Column {
            name: key_name.clone(),
        },
    ));
    let nested = lower_bound_body(
        table,
        input,
        logical,
        scope,
        indices,
        &nested_keys,
        body,
        bindings,
        &value_name,
    )?;
    let value_type = Type::from_arrow(
        nested
            .schema()
            .field(nested.schema().index_of(&value_name)?)
            .data_type(),
    )?;
    let entry = Expr::Struct {
        fields: vec![
            ("key".into(), Expr::Column { name: key_name }),
            ("value".into(), Expr::Column { name: value_name }),
        ],
    };
    let entry = lower_with_bindings(&entry, &nested.schema(), &Schema::empty(), &BTreeMap::new())?;
    let output_type = Type::Dict {
        dict: Box::new((dtype, value_type)),
    }
    .to_arrow()?;
    collect_nested(nested, keys, entry, output_type, name, vec![])
}

fn collect_nested(
    input: PhysicalPlan,
    keys: &[(String, Expr)],
    value: Arc<dyn PhysicalExpr>,
    output_type: DataType,
    name: &str,
    order_by: Vec<datafusion_physical_expr::PhysicalSortExpr>,
) -> Result<(PhysicalPlan, ScalarValue)> {
    let schema = input.schema();
    let function = vail_execution::collect_aggregate::collect_udf(&value.data_type(&schema)?);
    let aggregate = AggregateExprBuilder::new(function, vec![value])
        .order_by(order_by)
        .schema(schema.clone())
        .alias(name)
        .build()?;
    let default = aggregate.create_accumulator()?.evaluate()?;
    let default = vail_execution::expressions::scalar_at(
        &retag_array(&default.to_array_of_size(1)?, &output_type)?,
        0,
    )?;
    let groups = keys
        .iter()
        .map(|(name, _)| Ok((physical_group_key(&input, name)?, name.clone())))
        .collect::<Result<Vec<_>>>()?;
    let representative_types = keys
        .iter()
        .map(|(name, _)| Ok(schema.field_with_name(name)?.data_type().clone()))
        .collect::<Result<Vec<_>>>()?;
    let aggregates = vec![Arc::new(aggregate)];
    let result: PhysicalPlan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(groups),
        aggregates,
        vec![None],
        input,
        schema,
    )?);
    let mut projection = keys
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            Ok((
                restore_physical_group_key(column(&result, i), &representative_types[i])?,
                name.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    projection.push((
        Arc::new(RetagExpr::new(column(&result, keys.len()), output_type)),
        name.into(),
    ));
    Ok((
        Arc::new(ProjectionExec::try_new(projection, result)?),
        default,
    ))
}

fn lower_array_aggregation(
    table: &TableRef,
    input: PhysicalPlan,
    mut logical: Vec<Arc<Field>>,
    mut scope: BTreeMap<String, String>,
    indices: &BTreeMap<String, usize>,
    keys: &[(String, Expr)],
    expression: &Expr,
    name: &str,
    bindings: &GlobalBindings,
) -> Result<(PhysicalPlan, ScalarValue)> {
    use datafusion_common::config::ConfigOptions;
    use datafusion_physical_expr::expressions::{CaseExpr, IsNullExpr};
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, ScalarFunctionExpr};
    use datafusion_physical_plan::sorts::sort::SortExec;
    let Expr::AggArray {
        value,
        parameter,
        body,
    } = expression
    else {
        return Err(TableError::Invalid("invalid nested aggregate".into()));
    };
    let value = resolve(value, &scope)?;
    let Type::Array { array: element } =
        value.dtype_with_globals(&Schema::new(logical.clone()), table.globals().schema())?
    else {
        return Err(TableError::Invalid("array_agg requires array input".into()));
    };
    let value = lower_with_bindings(&value, &input.schema(), table.globals().schema(), indices)?;
    let mut used = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let length_name = fresh(&mut used, "__vail_array_length");
    let index_name = fresh(&mut used, "__vail_array_index");
    let element_name = fresh(&mut used, "__vail_array_element");
    let body_name = fresh(&mut used, "__vail_array_result");
    let aggregate = AggregateExprBuilder::new(
        vail_execution::array_length_aggregate::length_udaf(&value.data_type(&input.schema())?),
        vec![value.clone()],
    )
    .schema(input.schema())
    .alias(&length_name)
    .build()?;
    let group_exprs = keys
        .iter()
        .map(|(name, _)| Ok((physical_group_key(&input, name)?, name.clone())))
        .collect::<Result<Vec<_>>>()?;
    let length_aggregates = vec![Arc::new(aggregate)];
    let lengths: PhysicalPlan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(group_exprs),
        length_aggregates,
        vec![None],
        input.clone(),
        input.schema(),
    )?);
    let mut length_projection = keys
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            Ok((
                restore_physical_group_key(
                    column(&lengths, i),
                    input.schema().field_with_name(name)?.data_type(),
                )?,
                name.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    length_projection.push((column(&lengths, keys.len()), length_name.clone()));
    let lengths: PhysicalPlan = Arc::new(ProjectionExec::try_new(length_projection, lengths)?);
    let length = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::length::array_length_udf(),
        vec![value.clone()],
        &input.schema(),
        Arc::new(ConfigOptions::default()),
    )?);
    let positions = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions_nested::range::range_udf(),
        vec![
            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
            datafusion_physical_expr::expressions::cast(length, &input.schema(), DataType::Int64)?,
        ],
        &input.schema(),
        Arc::new(ConfigOptions::default()),
    )?);
    let index = input.schema().fields().len();
    let mut projections = (0..index)
        .map(|i| (column(&input, i), input.schema().field(i).name().clone()))
        .collect::<Vec<_>>();
    projections.push((value, element_name.clone()));
    projections.push((positions, index_name.clone()));
    let projected: PhysicalPlan = Arc::new(ProjectionExec::try_new(projections, input)?);
    let mut fields = projected.schema().fields().to_vec();
    fields[index] = Arc::new(Field::new(&element_name, element.to_arrow()?, true));
    fields[index + 1] = Arc::new(Field::new(&index_name, DataType::Int64, true));
    let unnested: PhysicalPlan = Arc::new(UnnestExec::new(
        projected,
        vec![
            ListUnnest {
                index_in_input_schema: index,
                depth: 1,
            },
            ListUnnest {
                index_in_input_schema: index + 1,
                depth: 1,
            },
        ],
        vec![],
        Arc::new(Schema::new(fields)),
        UnnestOptions::new().with_null_handling(NullHandling::Drop),
    )?);
    logical.push(Arc::new(Field::new(
        &element_name,
        element.to_arrow()?,
        true,
    )));
    logical.push(Arc::new(Field::new(&index_name, DataType::Int64, true)));
    scope.insert(parameter.clone(), element_name);
    let mut nested_keys = keys.to_vec();
    nested_keys.push((
        index_name.clone(),
        Expr::Column {
            name: index_name.clone(),
        },
    ));
    let body = lower_bound_body(
        table,
        unnested,
        logical,
        scope,
        indices,
        &nested_keys,
        body,
        bindings,
        &body_name,
    )?;
    let dtype = body.schema().field(keys.len() + 1).data_type().clone();
    // Collect array coordinates in ascending position order. The outer group
    // keys need not be sortable: sorting by position alone orders every group.
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        column(&body, keys.len()),
        arrow_schema::SortOptions::default(),
    )])
    .unwrap();
    let sorted: PhysicalPlan = Arc::new(SortExec::new(ordering, body));
    let output_type = DataType::List(Arc::new(Field::new("item", dtype, true)));
    let (values, empty) = collect_nested(
        sorted.clone(),
        keys,
        column(&sorted, keys.len() + 1),
        output_type.clone(),
        name,
        vec![PhysicalSortExpr::new(
            column(&sorted, keys.len()),
            arrow_schema::SortOptions::default(),
        )],
    )?;
    let result = join_results(lengths, values, keys.len(), &[(name.into(), empty)])?;
    let null = ScalarValue::try_from(&output_type)?;
    let output = Arc::new(CaseExpr::try_new(
        None,
        vec![(
            Arc::new(IsNullExpr::new(column(&result, keys.len()))),
            Arc::new(Literal::new(null.clone())),
        )],
        Some(column(&result, keys.len() + 1)),
    )?);
    let mut projection = keys
        .iter()
        .enumerate()
        .map(|(i, (name, _))| (column(&result, i), name.clone()))
        .collect::<Vec<_>>();
    projection.push((output, name.into()));
    Ok((Arc::new(ProjectionExec::try_new(projection, result)?), null))
}

pub(crate) fn lower_fold(
    expr: &Expr,
    input: &PhysicalPlan,
    logical: &Schema,
    globals: &Schema,
    indices: &BTreeMap<String, usize>,
    scope: &BTreeMap<String, String>,
) -> Result<(Arc<datafusion_expr::AggregateUDF>, Arc<dyn PhysicalExpr>)> {
    let Expr::AggregateFold {
        initial,
        seq,
        combine,
        state_parameter,
        left_parameter,
        right_parameter,
    } = expr
    else {
        unreachable!()
    };
    let Expr::RuntimeLiteral { value: initial } = initial.as_ref() else {
        return Err(TableError::Invalid("fold initializer was not bound".into()));
    };
    let initial = initial.clone();
    let state_type = initial.data_type();
    let mut used = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let state = fresh(&mut used, "__vail_fold_state");
    let left = fresh(&mut used, "__vail_fold_left");
    let right = fresh(&mut used, "__vail_fold_right");
    let mut seq_scope = scope.clone();
    seq_scope.insert(state_parameter.clone(), state.clone());
    let seq = resolve(seq, &seq_scope)?;
    let mut logical_fields = logical.fields().to_vec();
    logical_fields.push(Arc::new(Field::new(&state, state_type.clone(), true)));
    if seq
        .dtype_with_globals(&Schema::new(logical_fields), globals)?
        .to_arrow()?
        != state_type
    {
        return Err(TableError::Invalid("fold state type mismatch".into()));
    }
    let payload_schema = Arc::new(Schema::new(
        input
            .schema()
            .fields()
            .iter()
            .filter(|f| *f.data_type() != DataType::UInt64)
            .cloned()
            .collect::<Vec<_>>(),
    ));
    let mut fields = vec![Arc::new(Field::new(&state, state_type.clone(), true))];
    fields.extend(payload_schema.fields().iter().cloned());
    let seq_schema = Arc::new(Schema::new(fields));
    let seq_indices = indices
        .iter()
        .map(|(name, i)| {
            Ok((
                name.clone(),
                payload_schema.index_of(input.schema().field(*i).name())? + 1,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let seq = lower_with_bindings(&seq, &seq_schema, globals, &seq_indices)?;
    let combine = resolve(
        combine,
        &BTreeMap::from([
            (left_parameter.clone(), left.clone()),
            (right_parameter.clone(), right.clone()),
        ]),
    )?;
    let combine_schema = Arc::new(Schema::new(vec![
        Field::new(&left, state_type.clone(), true),
        Field::new(&right, state_type.clone(), true),
    ]));
    let combine = lower_with_bindings(
        &combine,
        &combine_schema,
        &Schema::empty(),
        &BTreeMap::new(),
    )?;
    if combine.data_type(&combine_schema)? != state_type {
        return Err(TableError::Invalid("fold combine type mismatch".into()));
    }
    let packed = Expr::Struct {
        fields: payload_schema
            .fields()
            .iter()
            .map(|f| {
                (
                    f.name().clone(),
                    Expr::Column {
                        name: f.name().clone(),
                    },
                )
            })
            .collect(),
    };
    let packed = lower_with_bindings(&packed, &input.schema(), &Schema::empty(), &BTreeMap::new())?;
    Ok((
        Arc::new(vail_execution::fold_aggregate::fold_udaf(
            payload_schema,
            initial,
            state_type,
            seq,
            combine,
        )),
        packed,
    ))
}
