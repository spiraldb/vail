//! Hail aggregate scopes expressed as optimizable DataFusion relations.
use crate::aggregation::{Group, Reducer, Step, extract, resolve};
use crate::{Result, TableError, TableNode, TableRef};
use arrow_schema::{DataType, Schema};
use datafusion_common::{NullHandling, ScalarValue, UnnestOptions};
use datafusion_expr::{
    AggregateUDF, Expr as DFExpr, JoinType, LogicalPlan, LogicalPlanBuilder, lit,
};
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Literal};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::Expr;
use vail_execution::logical_expr::lower_logical_with_bindings;

// API field names are literal identifiers, not SQL text to parse/normalize.
pub(crate) fn col(name: impl AsRef<str>) -> DFExpr {
    DFExpr::Column(datafusion_common::Column::new_unqualified(name.as_ref()))
}

/// Lift ordinary coordinate reductions to one mergeable DataFusion UDAF.
/// Complex nested scopes retain the general relational implementation.
fn coordinate_reductions(expression: &Expr) -> Result<Expr> {
    if let Expr::AggArray {
        value,
        parameter,
        body,
    } = expression
    {
        if let Expr::Aggregate {
            function,
            value: argument,
            options,
            result_type,
            init,
        } = body.as_ref()
        {
            if init.is_empty()
                && options
                    .as_object()
                    .is_none_or(|options| options.keys().all(|key| key == "__vail_result_type"))
                && matches!(
                    function.as_str(),
                    "sum" | "min" | "max" | "mean" | "count" | "stddev_pop"
                )
            {
                use vail_execution::expressions::{PrimitiveType, Type};
                let mut element = argument
                    .as_deref()
                    .cloned()
                    .unwrap_or(Expr::RuntimeLiteral {
                        value: ScalarValue::Int64(Some(1)),
                    });
                if matches!(function.as_str(), "mean" | "stddev_pop") {
                    element = Expr::Cast {
                        value: Arc::new(element),
                        dtype: Type::Primitive(PrimitiveType::Float64),
                    };
                }
                // Sum infers its widened output from the mapped argument's
                // native Arrow type in factory(). Optional frontend result
                // annotations must not select integer versus floating math.
                return Ok(Expr::Aggregate {
                    function: format!("__coordinate_{function}"),
                    value: Some(Arc::new(Expr::ArrayMap {
                        value: value.clone(),
                        parameter: parameter.clone(),
                        body: Arc::new(element),
                    })),
                    options: options.clone(),
                    result_type: result_type.clone(),
                    init: vec![],
                });
            }
        }
    }
    expression.try_map_children(coordinate_reductions)
}

pub(crate) fn factory(reducer: &Reducer, dtype: Option<&DataType>) -> Result<Arc<AggregateUDF>> {
    if reducer.function.starts_with("__coordinate_") {
        return factory_inner(reducer, dtype);
    }
    // Initializer values have been bound by this point. The descriptor and
    // executable UDAF must promise precisely the same logical output.
    let input_type = dtype
        .map(vail_execution::expressions::Type::from_arrow)
        .transpose()?;
    let expected = vail_execution::aggregate_contract::result_type(
        &reducer.function,
        if reducer.function == "count" {
            None
        } else {
            input_type.as_ref()
        },
        &reducer.options,
        &[],
    )?;
    let udf = factory_inner(reducer, dtype)?;
    let arguments = dtype.into_iter().cloned().collect::<Vec<_>>();
    let actual = vail_execution::expressions::Type::from_arrow(&udf.return_type(&arguments)?)?;
    if actual != expected {
        return Err(TableError::Invalid(
            "aggregate descriptor disagrees with executable result".into(),
        ));
    }
    Ok(udf)
}
fn factory_inner(reducer: &Reducer, dtype: Option<&DataType>) -> Result<Arc<AggregateUDF>> {
    let f = &reducer.function;
    if let Some(function) = f.strip_prefix("__coordinate_") {
        let Some(DataType::List(element)) = dtype else {
            return Err(TableError::Invalid(
                "coordinate aggregate requires list input".into(),
            ));
        };
        let input = element.data_type();
        let inner = match function {
            "sum" => {
                vail_execution::reducer::configured_udaf("sum", Some(input), &reducer.options)?
            }
            "min" => datafusion_functions_aggregate::min_max::min_udaf(),
            "max" => datafusion_functions_aggregate::min_max::max_udaf(),
            "mean" => vail_execution::aggregates::aggregate_udf("mean", Some(input))?,
            "count" => datafusion_functions_aggregate::count::count_udaf(),
            "stddev_pop" => vail_execution::aggregates::aggregate_udf(function, Some(input))?,
            _ => {
                return Err(TableError::Invalid(
                    "unsupported coordinate aggregate".into(),
                ));
            }
        };
        let empty = match function {
            "sum" if matches!(input, DataType::Int32 | DataType::Int64) => {
                ScalarValue::Int64(Some(0))
            }
            "sum" => ScalarValue::Float64(Some(0.0)),
            "count" => ScalarValue::Int64(Some(0)),
            "mean" | "stddev_pop" => ScalarValue::Float64(Some(f64::NAN)),
            _ => ScalarValue::try_from(input)?,
        };
        return Ok(vail_execution::array_aggregate::coordinate_udaf(
            inner,
            input.clone(),
            empty,
        )?);
    }
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
    Ok(vail_execution::reducer::configured_udaf(
        f,
        dtype,
        &reducer.options,
    )?)
}

pub(crate) fn columns(plan: &LogicalPlan) -> Vec<DFExpr> {
    plan.schema()
        .columns()
        .into_iter()
        .map(DFExpr::Column)
        .collect()
}

pub(crate) fn unique(used: &mut HashSet<String>, base: &str) -> String {
    let mut result = base.to_owned();
    while !used.insert(result.clone()) {
        result.push('_');
    }
    result
}

pub(crate) fn needs_order(expression: &Expr) -> bool {
    let mut ordered = false;
    expression.visit_unique(&mut |node| {
        // A fold's initial state is applied once, including for empty input.
        // The native accumulator also preserves sequence order.
        if matches!(node, Expr::AggregateFold { .. }) {
            ordered = true;
        }
        if let Expr::Aggregate { function, .. } = node {
            ordered |= !function.starts_with("__coordinate_")
                && !matches!(
                    function.as_str(),
                    "count"
                    | "sum"
                    | "product"
                    | "min"
                    | "max"
                    | "mean"
                    | "stats"
                    | "count_where"
                    | "fraction"
                    // stats expands to this internal aggregate, whose native
                    // DataFusion accumulator can be merged across partitions.
                    | "stddev_pop"
                    | "any"
                    | "all"
                    | "hist"
                    | "corr"
                    | "linreg"
                    | "hardy_weinberg_test"
                    | "inbreeding"
                    | "call_stats"
                );
        }
    });
    ordered
}

/// Whether a total expression contains an aggregate and only uses reducers
/// whose results can be combined without preserving row order.
pub(crate) fn can_parallelize_total(expression: &Expr) -> bool {
    let mut contains_aggregate = false;
    expression.visit_unique(&mut |node| {
        contains_aggregate |= matches!(node, Expr::Aggregate { .. } | Expr::AggregateFold { .. });
    });
    contains_aggregate && !needs_order(expression)
}

pub(crate) fn scope_plan(
    mut input: LogicalPlan,
    table: &TableRef,
    group: &Group,
    key_names: &[String],
    group_names: &[String],
    used: &mut HashSet<String>,
    bindings: &BTreeMap<String, usize>,
    outer_scope: &BTreeMap<String, String>,
    external: &crate::globals::GlobalBindings,
    context: &mut crate::logical::LoweringContext,
) -> Result<(LogicalPlan, BTreeMap<String, ScalarValue>, String)> {
    // Each aggregate scope is an independent consumer of the same relation.
    // Its logical boundary prevents sibling scopes competing for exchange output.
    input = crate::logical::independent_consumer(input);
    let key_types = key_names
        .iter()
        .map(|name| {
            Ok(input
                .schema()
                .field_with_unqualified_name(name)?
                .data_type()
                .clone())
        })
        .collect::<Result<Vec<_>>>()?;
    let mut scope = outer_scope.clone();
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
    let nested_input = input.clone();
    let mut aggregates = Vec::new();
    let mut defaults = BTreeMap::new();
    let mut native_sums = BTreeMap::new();
    for reducer in &group.reducers {
        if reducer.function == "__nested" {
            continue;
        }
        let value = match &reducer.value {
            Some(value) => resolve(value, &scope)?,
            None if reducer.function == "count" => Expr::RuntimeLiteral {
                value: ScalarValue::Int64(Some(1)),
            },
            _ => return Err(TableError::Invalid("aggregate argument missing".into())),
        };
        let (fold_udf, fold_value, fold_storage) = if reducer.function == "__fold" {
            let (udf, packed_input) = crate::aggregation::lower_fold(
                &value,
                input.schema().as_arrow(),
                input.schema().as_arrow(),
                table.globals().schema(),
                bindings,
                &scope,
            )?;
            let DataType::Struct(payload_fields) =
                packed_input.data_type(input.schema().as_arrow())?
            else {
                return Err(TableError::Invalid("fold input must be a struct".into()));
            };
            let packed_names = payload_fields
                .iter()
                .map(|field| field.name())
                .collect::<std::collections::HashSet<_>>();
            let packed = Expr::Struct {
                fields: input
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| packed_names.contains(f.name()))
                    .map(|(i, f)| {
                        (
                            f.name().clone(),
                            if i < table.globals().schema().fields().len() {
                                Expr::Global {
                                    name: table.globals().schema().field(i).name().clone(),
                                }
                            } else {
                                Expr::Column {
                                    name: f.name().clone(),
                                }
                            },
                        )
                    })
                    .collect(),
            };
            (
                Some(udf),
                Some(packed),
                Some(DataType::Struct(payload_fields)),
            )
        } else {
            (None, None, None)
        };
        let value = fold_value.as_ref().unwrap_or(&value);
        let mut logical_value =
            lower_logical_with_bindings(value, input.schema(), table.globals().schema(), bindings)?;
        let mut dtype = if reducer.function == "count" {
            None
        } else {
            Some(
                value
                    .dtype_with_globals(input.schema().as_arrow(), table.globals().schema())?
                    .to_arrow()?,
            )
        };
        let mut dtype = if let Some(payload) = fold_storage {
            logical_value =
                vail_execution::logical_expr::restore_type(logical_value, payload.clone());
            Some(payload)
        } else {
            dtype
        };
        // Use DataFusion's grouped kernels rather than constructing one scalar
        // accumulator per group. Widen before summing as required by Hail.
        let native_sum_type = if reducer.function == "sum" {
            match dtype.as_ref() {
                Some(DataType::Int32 | DataType::Int64) => Some(DataType::Int64),
                Some(DataType::Float32 | DataType::Float64) => Some(DataType::Float64),
                _ => None,
            }
        } else {
            None
        };
        if let Some(output_type) = native_sum_type {
            logical_value = datafusion_expr::expr_fn::cast(logical_value, output_type.clone());
            let zero = if output_type == DataType::Int64 {
                ScalarValue::Int64(Some(0))
            } else {
                ScalarValue::Float64(Some(0.0))
            };
            native_sums.insert(reducer.name.clone(), zero);
            dtype = Some(output_type);
        }
        let udf = match fold_udf {
            Some(udf) => udf,
            None if native_sums.contains_key(&reducer.name) => {
                datafusion_functions_aggregate::sum::sum_udaf()
            }
            None if reducer.function == "count" => {
                datafusion_functions_aggregate::count::count_udaf()
            }
            None => factory(reducer, dtype.as_ref())?,
        };
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
            match native_sums.get(&reducer.name) {
                Some(zero) => zero.clone(),
                None => physical.create_accumulator()?.evaluate()?,
            },
        );
        aggregates.push(udf.call(vec![logical_value]).alias(&reducer.name));
    }
    let presence = unique(used, "__vail_scope_presence");
    aggregates.push(
        datafusion_functions_aggregate::count::count_udaf()
            .call(vec![lit(1i64)])
            .alias(&presence),
    );
    let groups = group_names.iter().map(col).collect::<Vec<_>>();
    let result = LogicalPlanBuilder::from(input)
        .aggregate(groups, aggregates)?
        .build()?;
    // Native grouping retains the typed key itself; no representative
    // accumulator is necessary, including for zero-field structs.
    let mut projection = result
        .schema()
        .columns()
        .into_iter()
        .map(|column| match native_sums.get(&column.name) {
            Some(zero) => datafusion_functions::core::coalesce()
                .call(vec![DFExpr::Column(column.clone()), lit(zero.clone())])
                .alias(&column.name),
            None => DFExpr::Column(column),
        })
        .collect::<Vec<_>>();
    let restored_keys = group_names
        .iter()
        .zip(key_names)
        .zip(&key_types)
        .map(|((group, value), dtype)| {
            Ok(crate::aggregation::logical_restore_group_key(col(group), dtype)?.alias(value))
        })
        .collect::<Result<Vec<_>>>()?;
    projection.extend(restored_keys);
    let result = LogicalPlanBuilder::from(result)
        .project(projection)?
        .build()?;
    let mut result = result;
    for reducer in group.reducers.iter().filter(|r| r.function == "__nested") {
        let (right, default, right_presence) = crate::logical_nested_aggregation::lower(
            nested_input.clone(),
            table,
            key_names,
            group_names,
            &scope,
            used,
            bindings,
            reducer.value.as_ref().unwrap(),
            &reducer.name,
            external,
            context,
        )?;
        result = join_scope(
            result,
            right,
            group_names,
            &[(reducer.name.clone(), default.clone())],
            &right_presence,
            used,
        )?;
        defaults.insert(reducer.name.clone(), default);
    }
    Ok((result, defaults, presence))
}

/// All grouped aggregate scopes use the logical relational lowering. Runtime
/// initializer values are evaluated at an explicit logical extension boundary.
pub(crate) fn lower_logical_grouped(
    table: &TableRef,
    child: LogicalPlan,
    context: &mut crate::logical::LoweringContext,
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
    Ok(Some(lower_grouped_fields(
        table, child, keys, fields, bindings, context,
    )?))
}

pub(crate) fn lower_grouped_fields(
    table: &TableRef,
    child: LogicalPlan,
    keys: &[(String, Expr)],
    fields: &[(String, Expr)],
    bindings: &crate::globals::GlobalBindings,
    context: &mut crate::logical::LoweringContext,
) -> Result<LogicalPlan> {
    let fields = fields
        .iter()
        .map(|(name, expression)| Ok((name.clone(), coordinate_reductions(expression)?)))
        .collect::<Result<Vec<_>>>()?;
    let fields = &fields;
    if let Some(plan) = crate::logical_aggregate_init::lower(
        table,
        child.clone(),
        Some(keys),
        fields,
        bindings,
        context,
    )? {
        return Ok(plan);
    }
    // Keep partitions independent for associative reductions. Ordered reducers
    // retain the source boundary, before scope/key columns are appended.
    let child = if fields.iter().any(|(_, expr)| needs_order(expr)) {
        crate::logical::ordered(child)
    } else {
        child
    };
    let child = crate::logical_globals::bind_globals(child, table, context)?;
    let (child, indices) = crate::logical_globals::bind_external(child, bindings, context)?;
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
        keys.to_vec()
    };
    let mut key_names = Vec::new();
    let mut group_names = Vec::new();
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
        let group_key = unique(&mut used, "__vail_logical_group_key");
        let dtype =
            expr.dtype_with_globals(projected.schema().as_arrow(), table.globals().schema())?;
        dtype.ensure_keyable()?;
        if !dtype.native_key() {
            return Err(TableError::Invalid(
                "grouping requires a native DataFusion key type".into(),
            ));
        }
        projections.push(
            crate::aggregation::logical_group_key(col(name), &dtype.to_arrow()?)?.alias(&group_key),
        );
        group_names.push(group_key);
    }
    projected = LogicalPlanBuilder::from(projected)
        .project(projections)?
        .build()?;
    let (mut result, _, _) = scope_plan(
        projected.clone(),
        table,
        &anchor,
        &key_names,
        &group_names,
        &mut used,
        &indices,
        &BTreeMap::new(),
        bindings,
        context,
    )?;
    for group in groups {
        let (right, defaults, presence) = scope_plan(
            projected.clone(),
            table,
            &group,
            &key_names,
            &group_names,
            &mut used,
            &indices,
            &BTreeMap::new(),
            bindings,
            context,
        )?;
        let right_keys = group_names
            .iter()
            .map(|_| unique(&mut used, "__vail_scope_join_key"))
            .collect::<Vec<_>>();
        let mut projection = group_names
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
            .join_detailed(
                right,
                JoinType::Left,
                (group_names.clone(), right_keys),
                None,
                datafusion_common::NullEquality::NullEqualsNull,
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
    result = crate::logical_globals::bind_globals(result, table, context)?;
    let (result, output_indices) =
        crate::logical_globals::bind_external(result, bindings, context)?;
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
    Ok(LogicalPlanBuilder::from(result)
        .project(projection)?
        .build()?)
}

/// Total aggregation emits one row even for empty input. Aggregate scopes are
/// singleton relations combined with ordinary logical cross joins.
pub(crate) fn lower_logical_total(
    table: &TableRef,
    expression: &Expr,
    bindings: &crate::globals::GlobalBindings,
    child: LogicalPlan,
    context: &mut crate::logical::LoweringContext,
) -> Result<Option<LogicalPlan>> {
    let expression = coordinate_reductions(expression)?;
    let expression = &expression;
    if let Some(plan) = crate::logical_aggregate_init::lower(
        table,
        child.clone(),
        None,
        &[("value".into(), expression.clone())],
        bindings,
        context,
    )? {
        return Ok(Some(plan));
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
        let order_sensitive = needs_order(expression);
        let child = if order_sensitive {
            crate::logical::ordered(child)
        } else {
            child
        };
        let mut child = crate::logical_globals::bind_globals(child, table, context)?;
        let (bound_child, indices) = crate::logical_globals::bind_external(child, bindings, context)?;
        child = bound_child;
        // Fan out only this total aggregate's input. Preserve visible table
        // partitioning and avoid changing order-sensitive reducers.
        if crate::logical::should_fan_out_total(table, expression)
            && !table
                .known_n_partitions()
                .is_some_and(|partitions| partitions >= crate::EXECUTION_CONCURRENCY)
        {
            child = crate::logical::protect_round_robin(child, crate::EXECUTION_CONCURRENCY);
        }
        for group in groups {
            let (right, _, _) = scope_plan(
                child.clone(),
                table,
                &group,
                &[],
                &[],
                &mut used,
                &indices,
                &BTreeMap::new(),
                bindings,
                context,
            )?;
            result = LogicalPlanBuilder::from(result)
                .cross_join(right)?
                .build()?;
        }
    }
    result = crate::logical_globals::bind_globals(result, table, context)?;
    let (result, output_indices) =
        crate::logical_globals::bind_external(result, bindings, context)?;
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

/// Preserve the enclosing group domain and distinguish absent scopes from
/// present scopes whose reducer legitimately returns missing.
pub(crate) fn join_scope(
    left: LogicalPlan,
    right: LogicalPlan,
    keys: &[String],
    values: &[(String, ScalarValue)],
    presence: &str,
    used: &mut HashSet<String>,
) -> Result<LogicalPlan> {
    let aliases = keys
        .iter()
        .map(|_| unique(used, "__vail_nested_join_key"))
        .collect::<Vec<_>>();
    let mut fields = keys
        .iter()
        .zip(&aliases)
        .map(|(l, r)| col(l).alias(r))
        .collect::<Vec<_>>();
    fields.extend(values.iter().map(|(name, _)| col(name)));
    fields.push(col(presence));
    let right = LogicalPlanBuilder::from(right).project(fields)?.build()?;
    let mut fields = columns(&left);
    let joined = if keys.is_empty() {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()?
    } else {
        LogicalPlanBuilder::from(left)
            .join_detailed(
                right,
                JoinType::Left,
                (keys.to_vec(), aliases),
                None,
                datafusion_common::NullEquality::NullEqualsNull,
            )?
            .build()?
    };
    for (name, default) in values {
        let value = if default.is_null() {
            col(name)
        } else {
            datafusion_expr::expr_fn::when(col(presence).is_null(), lit(default.clone()))
                .otherwise(col(name))?
        };
        fields.push(value.alias(name));
    }
    Ok(LogicalPlanBuilder::from(joined).project(fields)?.build()?)
}

/// Recursively reduce a nested body, preserving its enclosing group keys.
pub(crate) fn body_plan(
    input: LogicalPlan,
    table: &TableRef,
    key_names: &[String],
    group_names: &[String],
    scope: &BTreeMap<String, String>,
    used: &mut HashSet<String>,
    bindings: &BTreeMap<String, usize>,
    body: &Expr,
    name: &str,
    external: &crate::globals::GlobalBindings,
    context: &mut crate::logical::LoweringContext,
) -> Result<LogicalPlan> {
    let mut groups = Vec::new();
    let output = extract(body, &[], &mut groups, used)?;
    let anchor = groups
        .iter()
        .position(|g| g.steps.is_empty())
        .map(|i| groups.remove(i))
        .unwrap_or(Group {
            steps: vec![],
            reducers: vec![],
        });
    let (mut result, _, _) = scope_plan(
        input.clone(),
        table,
        &anchor,
        key_names,
        group_names,
        used,
        bindings,
        scope,
        external,
        context,
    )?;
    for group in groups {
        let (right, defaults, presence) = scope_plan(
            input.clone(),
            table,
            &group,
            key_names,
            group_names,
            used,
            bindings,
            scope,
            external,
            context,
        )?;
        let values = group
            .reducers
            .iter()
            .map(|r| (r.name.clone(), defaults[&r.name].clone()))
            .collect::<Vec<_>>();
        result = join_scope(result, right, group_names, &values, &presence, used)?;
    }
    result = crate::logical_globals::bind_globals(result, table, context)?;
    // The external bindings in the input were removed by aggregation.
    let (result, indices) = crate::logical_globals::bind_external(result, external, context)?;
    let output = resolve(&output, scope)?;
    let mut fields = group_names
        .iter()
        .chain(key_names)
        .map(col)
        .collect::<Vec<_>>();
    fields.push(
        lower_logical_with_bindings(&output, result.schema(), table.globals().schema(), &indices)?
            .alias(name),
    );
    Ok(LogicalPlanBuilder::from(result).project(fields)?.build()?)
}
