//! Hail aggregate scopes expressed as optimizable DataFusion relations.
use crate::aggregation::{Group, Reducer, Step, extract, resolve};
use crate::{Result, TableError, TableNode, TableRef};
use arrow_schema::{DataType, Schema};
use datafusion_common::{NullHandling, ScalarValue, UnnestOptions};
use datafusion_expr::{
    AggregateUDF, Expr as DFExpr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, lit,
};
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Literal};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::Expr;
use vail_execution::logical_expr::{canonical_key_logical, lower_logical_with_bindings};

// API field names are literal identifiers, not SQL text to parse/normalize.
fn col(name: impl AsRef<str>) -> DFExpr {
    DFExpr::Column(datafusion_common::Column::new_unqualified(name.as_ref()))
}

pub(crate) fn factory(reducer: &Reducer, dtype: Option<&DataType>) -> Result<Arc<AggregateUDF>> {
    let f = &reducer.function;
    if reducer
        .options
        .get("__vail_scan")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return Err(TableError::Invalid(
            "scan expressions cannot be used in table aggregation".into(),
        ));
    }
    if (f == "count") != reducer.value.is_none() {
        return Err(TableError::Invalid(
            "aggregate argument count mismatch".into(),
        ));
    }
    if f == "collect" {
        return Ok(vail_execution::collect_aggregate::collect_udf(
            dtype.ok_or_else(|| TableError::Invalid("collect requires input".into()))?,
        ));
    }
    if let Some(dtype) = dtype {
        if let Some(udf) =
            vail_execution::advanced_collection_aggregates::make_udaf(f, dtype, &reducer.options)?
        {
            return Ok(udf);
        }
        if let Some(udf) =
            vail_execution::advanced_numeric_aggregates::make_udaf(f, dtype, &reducer.options)?
        {
            return Ok(udf);
        }
        if let Some(udf) =
            vail_execution::advanced_genomic_aggregates::make_udaf(f, dtype, &reducer.options)?
        {
            return Ok(udf);
        }
    }
    Ok(vail_execution::aggregates::aggregate_udf(f, dtype)?)
}

fn columns(plan: &LogicalPlan) -> Vec<DFExpr> {
    plan.schema()
        .columns()
        .into_iter()
        .map(DFExpr::Column)
        .collect()
}

fn unique(used: &mut HashSet<String>, base: &str) -> String {
    let mut result = base.to_owned();
    while !used.insert(result.clone()) {
        result.push('_');
    }
    result
}

fn scope_plan(
    mut input: LogicalPlan,
    table: &TableRef,
    group: &Group,
    key_names: &[String],
    encoded_names: &[String],
    used: &mut HashSet<String>,
    bindings: &BTreeMap<String, usize>,
) -> Result<(LogicalPlan, BTreeMap<String, ScalarValue>, String)> {
    let mut scope = BTreeMap::new();
    for step in &group.steps {
        match step {
            Step::Filter(predicate) => {
                let predicate = resolve(predicate, &scope)?;
                if predicate
                    .dtype_with_globals(input.schema().as_arrow(), table.globals().schema())?
                    != vail_execution::expressions::Type::Primitive(
                        vail_execution::expressions::PrimitiveType::Bool,
                    )
                {
                    return Err(TableError::Invalid(
                        "aggregate filter predicate must be Boolean".into(),
                    ));
                }
                let value = lower_logical_with_bindings(
                    &predicate,
                    input.schema(),
                    table.globals().schema(),
                    bindings,
                )?;
                input = LogicalPlanBuilder::from(input).filter(value)?.build()?;
            }
            Step::Explode { value, parameter } => {
                let name = unique(used, "__vail_agg_explode");
                let expr = resolve(value, &scope)?;
                if !matches!(
                    expr.dtype_with_globals(input.schema().as_arrow(), table.globals().schema())?,
                    vail_execution::expressions::Type::Array { .. }
                        | vail_execution::expressions::Type::Set { .. }
                ) {
                    return Err(TableError::Invalid(
                        "aggregate explode requires an array or set".into(),
                    ));
                }
                let value = lower_logical_with_bindings(
                    &expr,
                    input.schema(),
                    table.globals().schema(),
                    bindings,
                )?;
                let mut fields = columns(&input);
                fields.push(value.alias(&name));
                input = LogicalPlanBuilder::from(input)
                    .project(fields)?
                    .unnest_column_with_options(
                        name.clone(),
                        UnnestOptions::new().with_null_handling(NullHandling::Drop),
                    )?
                    .build()?;
                scope.insert(parameter.clone(), name);
            }
        }
    }
    let mut aggregates = Vec::new();
    let mut defaults = BTreeMap::new();
    for reducer in &group.reducers {
        let value = match &reducer.value {
            Some(value) => resolve(value, &scope)?,
            None if reducer.function == "count" => Expr::RuntimeLiteral {
                value: ScalarValue::Int64(Some(1)),
            },
            _ => return Err(TableError::Invalid("aggregate argument missing".into())),
        };
        let logical_value = lower_logical_with_bindings(
            &value,
            input.schema(),
            table.globals().schema(),
            bindings,
        )?;
        let dtype = if reducer.function == "count" {
            None
        } else {
            Some(
                value
                    .dtype_with_globals(input.schema().as_arrow(), table.globals().schema())?
                    .to_arrow()?,
            )
        };
        let udf = factory(reducer, dtype.as_ref())?;
        // Query the existing UDAF for its empty-input identity. This does not
        // evaluate input rows or construct an executable aggregate pipeline.
        let argument = ScalarValue::try_from(dtype.as_ref().unwrap_or(&DataType::Int64))?;
        let dummy_schema = Arc::new(Schema::empty());
        let physical =
            AggregateExprBuilder::new(udf.clone(), vec![Arc::new(Literal::new(argument))])
                .schema(dummy_schema)
                .alias(&reducer.name)
                .build()?;
        defaults.insert(
            reducer.name.clone(),
            physical.create_accumulator()?.evaluate()?,
        );
        aggregates.push(udf.call(vec![logical_value]).alias(&reducer.name));
    }
    let mut representative_types = BTreeMap::new();
    for name in key_names {
        let dtype = input
            .schema()
            .field_with_unqualified_name(name)?
            .data_type()
            .clone();
        representative_types.insert(name.clone(), dtype.clone());
        aggregates.push(
            datafusion_functions_aggregate::any_value::any_value_udaf()
                .call(vec![col(name).cast_to(
                    &vail_ordering::without_type_metadata(&dtype),
                    input.schema(),
                )?])
                .alias(name),
        );
    }
    let presence = unique(used, "__vail_scope_presence");
    aggregates.push(
        datafusion_functions_aggregate::count::count_udaf()
            .call(vec![lit(1i64)])
            .alias(&presence),
    );
    let groups = encoded_names.iter().map(col).collect::<Vec<_>>();
    let result = LogicalPlanBuilder::from(input)
        .aggregate(groups, aggregates)?
        .build()?;
    let projection = result
        .schema()
        .columns()
        .into_iter()
        .map(|column| {
            let name = column.name.clone();
            let value = DFExpr::Column(column);
            match representative_types.get(&name) {
                Some(dtype) => {
                    vail_execution::logical_expr::restore_type(value, dtype.clone()).alias(&name)
                }
                None => value,
            }
        })
        .collect::<Vec<_>>();
    let result = LogicalPlanBuilder::from(result)
        .project(projection)?
        .build()?;
    Ok((result, defaults, presence))
}

/// Dynamic aggregate initialization and nested fold/group reducers need their
/// explicit Vail extension planner. Ordinary scopes use DF Aggregate/Join nodes.
pub(crate) fn lower_logical_grouped(
    table: &TableRef,
    child: LogicalPlan,
) -> Result<Option<LogicalPlan>> {
    let TableNode::Aggregate {
        keys,
        fields,
        bindings,
        ..
    } = table.node()
    else {
        return Ok(None);
    };
    let mut specialized = false;
    for (_, expr) in fields {
        expr.visit(&mut |node| match node {
            Expr::Aggregate { init, .. } if !init.is_empty() => specialized = true,
            Expr::AggregateFold { .. } | Expr::AggGroupBy { .. } | Expr::AggArray { .. } => {
                specialized = true
            }
            _ => (),
        });
    }
    if specialized {
        return Ok(None);
    }
    let child = crate::logical_globals::bind_globals(crate::logical::ordered(child), table)?;
    let (child, indices) = crate::logical_globals::bind_external(child, bindings)?;
    let mut used: HashSet<String> = child
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .chain(keys.iter().chain(fields).map(|(n, _)| n.clone()))
        .collect();
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
    let grouping = if keys.is_empty() {
        vec![(
            unique(&mut used, "__vail_group_constant"),
            Expr::RuntimeLiteral {
                value: ScalarValue::Int32(Some(0)),
            },
        )]
    } else {
        keys.clone()
    };
    let mut key_names = Vec::new();
    let mut encoded_names = Vec::new();
    let mut projections = columns(&child);
    for (_, expr) in &grouping {
        let name = unique(&mut used, "__vail_logical_group_value");
        projections.push(
            lower_logical_with_bindings(expr, child.schema(), table.globals().schema(), &indices)?
                .alias(&name),
        );
        key_names.push(name);
    }
    let mut projected = LogicalPlanBuilder::from(child)
        .project(projections)?
        .build()?;
    let mut projections = columns(&projected);
    for ((_, expr), name) in grouping.iter().zip(&key_names) {
        let encoded = unique(&mut used, "__vail_logical_group_key");
        let dtype =
            expr.dtype_with_globals(projected.schema().as_arrow(), table.globals().schema())?;
        projections.push(canonical_key_logical(col(name), &dtype, false)?.alias(&encoded));
        encoded_names.push(encoded);
    }
    projected = LogicalPlanBuilder::from(projected)
        .project(projections)?
        .build()?;
    let (mut result, _, _) = scope_plan(
        projected.clone(),
        table,
        &anchor,
        &key_names,
        &encoded_names,
        &mut used,
        &indices,
    )?;
    for group in groups {
        let (right, defaults, presence) = scope_plan(
            projected.clone(),
            table,
            &group,
            &key_names,
            &encoded_names,
            &mut used,
            &indices,
        )?;
        let right_keys = encoded_names
            .iter()
            .map(|_| unique(&mut used, "__vail_scope_join_key"))
            .collect::<Vec<_>>();
        let mut projection = encoded_names
            .iter()
            .zip(&right_keys)
            .map(|(l, r)| col(l).alias(r))
            .collect::<Vec<_>>();
        projection.extend(group.reducers.iter().map(|r| col(&r.name)));
        projection.push(col(&presence));
        let right = LogicalPlanBuilder::from(right)
            .project(projection)?
            .build()?;
        let left_columns = columns(&result);
        result = LogicalPlanBuilder::from(result)
            .join(
                right,
                JoinType::Left,
                (encoded_names.clone(), right_keys),
                None,
            )?
            .build()?;
        let mut projection = left_columns;
        for reducer in &group.reducers {
            let default = &defaults[&reducer.name];
            let value = col(&reducer.name);
            let value = if default.is_null() {
                value
            } else {
                datafusion_expr::expr_fn::when(col(&presence).is_null(), lit(default.clone()))
                    .otherwise(value)?
            };
            projection.push(value.alias(&reducer.name));
        }
        result = LogicalPlanBuilder::from(result)
            .project(projection)?
            .build()?;
    }
    result = crate::logical_globals::bind_globals(result, table)?;
    let (result, output_indices) = crate::logical_globals::bind_external(result, bindings)?;
    let mut projection = keys
        .iter()
        .zip(&key_names)
        .map(|((name, _), private)| col(private).alias(name))
        .collect::<Vec<_>>();
    for (name, expr) in outputs {
        projection.push(
            lower_logical_with_bindings(
                &expr,
                result.schema(),
                table.globals().schema(),
                &output_indices,
            )?
            .alias(name),
        );
    }
    Ok(Some(
        LogicalPlanBuilder::from(result)
            .project(projection)?
            .build()?,
    ))
}

/// Total aggregation emits one row even for empty input. Aggregate scopes are
/// singleton relations combined with ordinary logical cross joins.
pub(crate) fn lower_logical_total(
    table: &TableRef,
    expression: &Expr,
    bindings: &crate::globals::GlobalBindings,
    child: LogicalPlan,
) -> Result<Option<LogicalPlan>> {
    let mut specialized = false;
    expression.visit(&mut |node| match node {
        Expr::Aggregate { init, .. } if !init.is_empty() => specialized = true,
        Expr::AggregateFold { .. } | Expr::AggGroupBy { .. } | Expr::AggArray { .. } => {
            specialized = true
        }
        _ => (),
    });
    if specialized {
        return Ok(None);
    }
    let mut used: HashSet<String> = child
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut groups = Vec::new();
    let output = extract(expression, &[], &mut groups, &mut used)?;
    let mut result = LogicalPlanBuilder::empty(true).build()?;
    if !groups.is_empty() {
        let child = crate::logical_globals::bind_globals(crate::logical::ordered(child), table)?;
        let (child, indices) = crate::logical_globals::bind_external(child, bindings)?;
        for group in groups {
            let (right, _, _) =
                scope_plan(child.clone(), table, &group, &[], &[], &mut used, &indices)?;
            result = LogicalPlanBuilder::from(result)
                .cross_join(right)?
                .build()?;
        }
    }
    result = crate::logical_globals::bind_globals(result, table)?;
    let (result, output_indices) = crate::logical_globals::bind_external(result, bindings)?;
    let value = lower_logical_with_bindings(
        &output,
        result.schema(),
        table.globals().schema(),
        &output_indices,
    )?;
    Ok(Some(
        LogicalPlanBuilder::from(result)
            .project(vec![value.alias("value")])?
            .build()?,
    ))
}
