//! Hail aggregate scopes expressed as optimizable DataFusion relations.
use crate::aggregation::{Group, Reducer, Step, extract, resolve};
use crate::{Result, TableError, TableNode, TableRef};
use arrow_schema::{DataType, Schema};
use datafusion_common::{NullHandling, ScalarValue, UnnestOptions};
use datafusion_expr::expr::NullTreatment;
use datafusion_expr::{
    AggregateUDF, Expr as DFExpr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder, lit,
};
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Literal};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::Expr;
use vail_execution::logical_expr::lower_logical_with_bindings;

// API field names are literal identifiers, not SQL text to parse/normalize.
pub(crate) fn col(name: impl AsRef<str>) -> DFExpr {
    DFExpr::Column(datafusion_common::Column::new_unqualified(name.as_ref()))
}

fn is_independent_of_callback_parameter(expression: &Expr, parameter: &str) -> bool {
    let mut independent = true;
    expression.visit_unique(&mut |node| {
        if matches!(node, Expr::LambdaVariable { name, .. } if name == parameter) {
            independent = false;
        }
    });
    independent
}

/// Lift ordinary coordinate reductions to one mergeable DataFusion UDAF.
/// Complex nested scopes retain the general relational implementation.
fn coordinate_reductions(expression: &Expr) -> Result<Expr> {
    // Most scalar aggregates do not contain coordinate-array reductions.
    // Preserve the original Arc DAG in that common case instead of rebuilding
    // each shared edge as an expanded occurrence tree.
    let mut has_array_aggregate = false;
    expression.visit_unique(&mut |node| {
        has_array_aggregate |= matches!(node, Expr::AggArray { .. });
    });
    if !has_array_aggregate {
        return Ok(expression.clone());
    }
    if let Expr::AggArray {
        value,
        parameter,
        body,
    } = expression
    {
        if !matches!(body.as_ref(), Expr::Aggregate { .. }) {
            if let Some(lifted) = coordinate_bundle(value, parameter, body)? {
                return Ok(lifted);
            }
        }
        if let Expr::Aggregate {
            function,
            value: argument,
            options,
            result_type,
            init,
        } = body.as_ref()
        {
            let coordinate_options = options.as_object().is_none_or(|options| {
                options.keys().all(|key| {
                    key == "__vail_result_type"
                        || key == "__vail_share_linreg_predictors"
                        || (function == "linreg"
                            && matches!(key.as_str(), "nested_dim" | "n_predictors"))
                })
            });
            if init.is_empty()
                && coordinate_options
                && matches!(
                    function.as_str(),
                    "sum" | "min" | "max" | "mean" | "count" | "count_where" | "product" | "all" | "any" | "fraction" | "stddev_pop" | "linreg"
                )
            {
                use vail_execution::expressions::{PrimitiveType, Type};
                let mut element = argument
                    .as_deref()
                    .cloned()
                    .unwrap_or(Expr::RuntimeLiteral {
                        value: ScalarValue::Int64(Some(1)),
                    });
                if function == "stddev_pop" {
                    element = Expr::Cast {
                        value: Arc::new(element),
                        dtype: Type::Primitive(PrimitiveType::Float64),
                    };
                }
                // Sum infers its widened output from the mapped argument's
                // native Arrow type in factory(). Optional frontend result
                // annotations must not select integer versus floating math.
                let mut options = options.clone();
                if function == "linreg" {
                    if let Some(Expr::Struct { fields }) = argument.as_deref() {
                        let predictors = fields
                            .iter()
                            .find(|(name, _)| name == "x")
                            .map(|(_, value)| value);
                        let weight = fields
                            .iter()
                            .find(|(name, _)| name == "weight")
                            .map(|(_, value)| value);
                        if predictors.is_some_and(|value| {
                            is_independent_of_callback_parameter(value, parameter)
                        }) && weight.is_none_or(|value| {
                            is_independent_of_callback_parameter(value, parameter)
                        }) {
                            if let Some(options) = options.as_object_mut() {
                                options.insert(
                                    "__vail_share_linreg_predictors".into(),
                                    serde_json::Value::Bool(true),
                                );
                            }
                        }
                    }
                }
                return Ok(Expr::Aggregate {
                    function: format!("__coordinate_{function}"),
                    value: Some(Arc::new(Expr::ArrayMap {
                        value: value.clone(),
                        parameter: parameter.clone(),
                        body: Arc::new(element),
                    })),
                    options,
                    result_type: result_type.clone(),
                    init: vec![],
                });
            }
        }
        // A configured or structured reducer has the same coordinate semantics
        // whether returned directly or wrapped in a result struct. Preserve the
        // simple specialized paths above, then apply the existing bundle's
        // scope/initializer checks to bare reducers too.
        if matches!(body.as_ref(), Expr::Aggregate { .. }) {
            if let Some(lifted) = coordinate_bundle(value, parameter, body)? {
                return Ok(lifted);
            }
        }
    }
    expression.try_map_children(coordinate_reductions)
}

#[derive(Clone)]
struct CoordinateLeaf {
    function: String,
    value: Option<Expr>,
    options: serde_json::Value,
    filters: Vec<Expr>,
    init: Vec<(String, Expr)>,
}

#[derive(Hash, PartialEq, Eq)]
struct CoordinateLeafKey {
    function: String,
    value: Option<Expr>,
    options: String,
    filters: Vec<Expr>,
    init: Vec<(String, Expr)>,
}

/// Lift direct reducer leaves from a callback into one packed coordinate
/// aggregate. Nested aggregate scopes retain the relational fallback.
fn coordinate_bundle(value: &Arc<Expr>, parameter: &str, body: &Arc<Expr>) -> Result<Option<Expr>> {
    fn visit(
        expr: &Expr,
        scope: usize,
        scopes: &mut Vec<Vec<Expr>>,
        scope_ids: &mut HashMap<(usize, usize), usize>,
        memo: &mut HashMap<(usize, usize), Option<Expr>>,
        leaves: &mut Vec<CoordinateLeaf>,
        leaf_ids: &mut HashMap<CoordinateLeafKey, usize>,
        prefix: &str,
    ) -> Result<Option<Expr>> {
        let memo_key = (expr as *const Expr as usize, scope);
        if let Some(mapped) = memo.get(&memo_key) {
            return Ok(mapped.clone());
        }
        let mapped = match expr {
            Expr::Aggregate {
                function,
                value,
                options,
                init,
                ..
            } => {
                if !init.is_empty() || options.as_object().is_none() {
                    None
                } else {
                    let leaf = CoordinateLeaf {
                        function: function.clone(),
                        value: value.as_deref().cloned(),
                        options: options.clone(),
                        filters: scopes[scope].clone(),
                        init: init.clone(),
                    };
                    let key = CoordinateLeafKey {
                        function: leaf.function.clone(),
                        value: leaf.value.clone(),
                        options: serde_json::to_string(&leaf.options)
                            .map_err(|error| TableError::Invalid(error.to_string()))?,
                        filters: leaf.filters.clone(),
                        init: leaf.init.clone(),
                    };
                    let index = if let Some(index) = leaf_ids.get(&key) {
                        *index
                    } else {
                        let index = leaves.len();
                        leaves.push(leaf);
                        leaf_ids.insert(key, index);
                        index
                    };
                    Some(Expr::LambdaVariable {
                        name: format!("{prefix}{index}"),
                        dtype: None,
                    })
                }
            }
            Expr::AggFilter { predicate, body } => {
                let key = (scope, predicate.as_ref() as *const Expr as usize);
                let next_scope = if let Some(scope) = scope_ids.get(&key) {
                    *scope
                } else {
                    let mut nested = scopes[scope].clone();
                    nested.push((**predicate).clone());
                    let next = scopes.len();
                    scopes.push(nested);
                    scope_ids.insert(key, next);
                    next
                };
                visit(
                    body, next_scope, scopes, scope_ids, memo, leaves, leaf_ids, prefix,
                )?
            }
            Expr::AggArray { .. }
            | Expr::AggGroupBy { .. }
            | Expr::AggExplode { .. }
            | Expr::AggregateFold { .. } => None,
            Expr::Column { .. } | Expr::MatrixField { .. } => None,
            Expr::Global { .. } | Expr::LambdaVariable { .. } => Some(expr.clone()),
            _ => {
                let mut unsupported = false;
                let mut failure = None;
                let mapped = expr.try_map_children(|child| {
                    match visit(
                        child, scope, scopes, scope_ids, memo, leaves, leaf_ids, prefix,
                    ) {
                        Ok(Some(value)) => {
                            Ok::<Expr, vail_execution::expressions::ExprError>(value)
                        }
                        Ok(None) => {
                            unsupported = true;
                            Ok(child.clone())
                        }
                        Err(error) => {
                            failure = Some(error);
                            Ok(child.clone())
                        }
                    }
                })?;
                if let Some(error) = failure {
                    return Err(error);
                }
                if unsupported { None } else { Some(mapped) }
            }
        };
        memo.insert(memo_key, mapped.clone());
        Ok(mapped)
    }
    let mut used_names = HashSet::new();
    body.visit_unique(&mut |node| match node {
        Expr::LambdaVariable { name, .. } => {
            used_names.insert(name.clone());
        }
        Expr::ArrayMap { parameter, .. }
        | Expr::ArrayFilter { parameter, .. }
        | Expr::ArrayPredicate { parameter, .. }
        | Expr::AggArray { parameter, .. }
        | Expr::AggExplode { parameter, .. } => {
            used_names.insert(parameter.clone());
        }
        Expr::ArrayFold {
            accumulator,
            parameter,
            ..
        } => {
            used_names.insert(accumulator.clone());
            used_names.insert(parameter.clone());
        }
        _ => {}
    });
    let mut prefix = "__vail_coordinate_slot_".to_owned();
    while used_names.iter().any(|name| name.starts_with(&prefix)) {
        prefix.push('_');
    }
    let mut leaves = Vec::new();
    let mut scopes = vec![vec![]];
    let mut scope_ids = HashMap::new();
    let mut memo = HashMap::new();
    let mut leaf_ids: HashMap<CoordinateLeafKey, usize> = HashMap::new();
    let Some(mut replaced) = visit(
        body,
        0,
        &mut scopes,
        &mut scope_ids,
        &mut memo,
        &mut leaves,
        &mut leaf_ids,
        &prefix,
    )?
    else {
        return Ok(None);
    };
    // Indexed callbacks bind the source position separately from the source
    // element. The bundle output loop only binds its reducer struct, so keep
    // those callbacks on the relational lowering that binds both values.
    let mut references_source_parameter = false;
    replaced.visit_unique(&mut |node| {
        if matches!(node, Expr::LambdaVariable { name, .. } if name == parameter) {
            references_source_parameter = true;
        }
    });
    if references_source_parameter {
        return Ok(None);
    }
    if leaves.is_empty() {
        // Constant or global-only callbacks still need an implicit position
        // group to validate widths and preserve the callback's output shape.
        leaves.push(CoordinateLeaf {
            function: "count".into(),
            value: Some(Expr::RuntimeLiteral {
                value: ScalarValue::Int64(Some(1)),
            }),
            options: serde_json::json!({}),
            filters: vec![],
            init: vec![],
        });
    }
    let slot_remap = leaves
        .iter()
        .enumerate()
        .map(|(index, _)| (index, None))
        .collect::<Vec<_>>();
    // The first traversal leaves stable slot names. Build one mapped struct
    // containing every reducer value and its optional row filter.
    use vail_execution::expressions::BinaryOperator;
    let mut fields = Vec::new();
    let mut descriptors = Vec::new();
    let mut ordered = false;
    for (index, leaf) in leaves.iter().enumerate() {
        let argument = leaf.value.clone().unwrap_or(Expr::RuntimeLiteral {
            value: ScalarValue::Int64(Some(1)),
        });
        let value_index = fields.len();
        fields.push((format!("v{index}"), argument));
        let mut predicate = leaf.filters.first().cloned();
        for extra in leaf.filters.iter().skip(1) {
            predicate = Some(Expr::Binary {
                operator: BinaryOperator::And,
                left: Arc::new(predicate.unwrap()),
                right: Arc::new(extra.clone()),
            });
        }
        let mut function = leaf.function.clone();
        let options = leaf.options.clone();
        let mut filter_index = None;
        if function == "count_where" {
            // COUNT_WHERE counts defined true values, so its value slot is the
            // filter; a constant one is counted after mask application.
            fields.pop();
            let mut mask = leaf.value.clone().unwrap();
            if let Some(predicate) = predicate.take() {
                mask = Expr::Binary {
                    operator: BinaryOperator::And,
                    left: Arc::new(mask),
                    right: Arc::new(predicate),
                };
            }
            filter_index = Some(fields.len());
            fields.push((format!("p{index}"), mask));
            let value_index = fields.len();
            fields.push((
                format!("v{index}"),
                Expr::RuntimeLiteral {
                    value: ScalarValue::Int64(Some(1)),
                },
            ));
            function = "count".into();
            descriptors.push(serde_json::json!({"function": function, "options": options, "value_index": value_index, "filter_index": filter_index, "name": format!("r{index}")}));
            continue;
        } else if let Some(predicate) = predicate {
            let index = fields.len();
            fields.push((format!("f{index}"), predicate));
            filter_index = Some(index);
        }
        ordered |= (function == "collect"
            && options.get("ordered").and_then(serde_json::Value::as_bool) == Some(true))
            || function == "take";
        descriptors.push(serde_json::json!({"function": function, "options": options, "value_index": value_index, "filter_index": filter_index, "name": format!("r{index}")}));
    }
    let packed = Expr::ArrayMap {
        value: value.clone(),
        parameter: parameter.to_owned(),
        body: Arc::new(Expr::Struct { fields }),
    };
    let bundle = Expr::Aggregate {
        function: "__coordinate_bundle".into(),
        value: Some(Arc::new(packed)),
        options: serde_json::json!({"reducers": descriptors, "ordered": ordered}),
        result_type: None,
        init: vec![],
    };
    let mut result_param = "__vail_coordinate_bundle_result".to_owned();
    while used_names.contains(&result_param) || result_param.starts_with(&prefix) {
        result_param.push('_');
    }
    let slot_indices = slot_remap
        .iter()
        .enumerate()
        .map(|(source_index, (index, fit))| (format!("{prefix}{source_index}"), (*index, *fit)))
        .collect::<HashMap<_, _>>();
    fn replace(
        expr: &Expr,
        param: &str,
        slots: &HashMap<String, (usize, Option<usize>)>,
        memo: &mut HashMap<usize, Expr>,
    ) -> Result<Expr> {
        let key = expr as *const Expr as usize;
        if let Some(mapped) = memo.get(&key) {
            return Ok(mapped.clone());
        }
        let mapped = match expr {
            Expr::LambdaVariable { name, .. } if slots.contains_key(name) => {
                let (index, fit) = slots[name];
                let aggregate_result = Expr::GetField {
                    value: Arc::new(Expr::LambdaVariable {
                        name: param.to_owned(),
                        dtype: None,
                    }),
                    name: format!("r{index}"),
                };
                if let Some(fit) = fit {
                    Expr::GetField {
                        value: Arc::new(aggregate_result),
                        name: format!("fit{fit}"),
                    }
                } else {
                    aggregate_result
                }
            }
            other => other.try_map_children(|child| replace(child, param, slots, memo))?,
        };
        memo.insert(key, mapped.clone());
        Ok(mapped)
    }
    replaced = replace(&replaced, &result_param, &slot_indices, &mut HashMap::new())?;
    Ok(Some(Expr::ArrayMap {
        value: Arc::new(bundle),
        parameter: result_param,
        body: Arc::new(replaced),
    }))
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
    let udf = if reducer.function == "count_where" {
        if dtype != Some(&DataType::Boolean) {
            return Err(TableError::Invalid(
                "count_where requires Boolean input".into(),
            ));
        }
        datafusion_functions_aggregate::count::count_udaf()
    } else {
        factory_inner(reducer, dtype)?
    };
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
    if f == "__coordinate_bundle" {
        let Some(DataType::List(element)) = dtype else {
            return Err(TableError::Invalid(
                "coordinate bundle requires list input".into(),
            ));
        };
        let DataType::Struct(fields) = element.data_type() else {
            return Err(TableError::Invalid(
                "coordinate bundle requires struct elements".into(),
            ));
        };
        let descriptors = reducer
            .options
            .get("reducers")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                TableError::Invalid("coordinate bundle descriptors are missing".into())
            })?;
        let mut specs = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            let function = descriptor
                .get("function")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    TableError::Invalid("coordinate bundle reducer name is missing".into())
                })?;
            let options = descriptor
                .get("options")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let value_index = descriptor
                .get("value_index")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    TableError::Invalid("coordinate bundle value slot is missing".into())
                })? as usize;
            let filter_index = descriptor
                .get("filter_index")
                .and_then(serde_json::Value::as_u64)
                .map(|n| n as usize);
            let name = descriptor
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("value")
                .to_owned();
            let input = fields
                .get(value_index)
                .ok_or_else(|| {
                    TableError::Invalid("coordinate bundle value slot is out of bounds".into())
                })?
                .data_type();
            let aggregate =
                vail_execution::reducer::configured_udaf(function, Some(input), &options)?;
            let argument = ScalarValue::try_from(input)?;
            let physical = AggregateExprBuilder::new(
                aggregate.clone(),
                vec![Arc::new(Literal::new(argument))],
            )
            .schema(Arc::new(Schema::empty()))
            .alias(&name)
            .build()?;
            let mut empty = physical.create_accumulator()?.evaluate()?;
            if function == "sum" {
                empty = match aggregate.return_type(std::slice::from_ref(input))? {
                    DataType::Int64 => ScalarValue::Int64(Some(0)),
                    DataType::Float64 => ScalarValue::Float64(Some(0.0)),
                    _ => empty,
                };
            } else if function == "collect" {
                let DataType::List(output) = aggregate.return_type(std::slice::from_ref(input))?
                else {
                    return Err(TableError::Invalid("collect output must be a list".into()));
                };
                empty = ScalarValue::List(ScalarValue::new_list(&[], output.data_type(), true));
            }
            let identity = format!(
                "{function}:{options}:value={value_index}:filter={filter_index:?}:name={name}:empty={empty:?}"
            );
            specs.push(vail_execution::array_aggregate::CoordinateReducer {
                name,
                aggregate,
                input: input.clone(),
                value_index,
                filter_index,
                empty,
                identity,
            });
        }
        let udf = vail_execution::array_aggregate::coordinate_bundle_udaf(
            element.data_type().clone(),
            specs,
        )?;
        return Ok(udf);
    }
    if let Some(function) = f.strip_prefix("__coordinate_") {
        let Some(DataType::List(element)) = dtype else {
            return Err(TableError::Invalid(
                "coordinate aggregate requires list input".into(),
            ));
        };
        let input = element.data_type();
        let inner = match function {
            "sum" => vail_execution::aggregates::aggregate_udf("sum", Some(input))?,
            "min" => datafusion_functions_aggregate::min_max::min_udaf(),
            "max" => datafusion_functions_aggregate::min_max::max_udaf(),
            "mean" | "product" | "all" | "any" | "fraction" | "count_where" =>
                vail_execution::aggregates::aggregate_udf(function, Some(input))?,
            "count" => datafusion_functions_aggregate::count::count_udaf(),
            "stddev_pop" => vail_execution::aggregates::aggregate_udf(function, Some(input))?,
            "linreg" => {
                let inner = vail_execution::reducer::configured_udaf(
                    function,
                    Some(input),
                    &reducer.options,
                )?;
                // Preserve linreg's language-level empty result: its native
                // accumulator returns n=0 and null fit statistics.
                let argument = ScalarValue::try_from(input)?;
                let physical = AggregateExprBuilder::new(
                    inner.clone(),
                    vec![Arc::new(Literal::new(argument))],
                )
                .schema(Arc::new(Schema::empty()))
                .alias("coordinate_linreg")
                .build()?;
                let empty = physical.create_accumulator()?.evaluate()?;
                return Ok(vail_execution::array_aggregate::coordinate_udaf(
                    inner,
                    input.clone(),
                    empty,
                )?);
            }
            _ => {
                return Err(TableError::Invalid(
                    "unsupported coordinate aggregate".into(),
                ));
            }
        };
        // Ask the existing native accumulator for its language-level empty
        // value once during planning, including widened product and Boolean
        // identities, rather than maintaining a separate identity table.
        let argument = ScalarValue::try_from(input)?;
        let physical = AggregateExprBuilder::new(inner.clone(), vec![Arc::new(Literal::new(argument))])
            .schema(Arc::new(Schema::empty())).alias("coordinate_empty").build()?;
        let empty = physical.create_accumulator()?.evaluate()?;
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
        if let Expr::Aggregate {
            function, options, ..
        } = node
        {
            ordered |= function == "__coordinate_bundle"
                && options.get("ordered").and_then(serde_json::Value::as_bool) == Some(true);
            ordered |= function == "collect"
                && options.get("ordered").and_then(serde_json::Value::as_bool) == Some(true);
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
                    // Hail does not guarantee collect order. Its native
                    // accumulator merges partition lists by concatenation.
                    | "collect"
                    // Downsample state merges bounded per-partition grids.
                    // Representative identities may vary with partitioning,
                    // as they do in Hail's partial aggregator; its public
                    // result contract is the bounded set of sampled points.
                    | "downsample"
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
    contains_aggregate && !contains_collect(expression) && !needs_order(expression)
}

fn contains_collect(expression: &Expr) -> bool {
    let mut found = false;
    expression.visit_unique(&mut |node| {
        found |= matches!(node, Expr::Aggregate { function, .. } if function == "collect");
    });
    found
}

pub(crate) fn scope_plan(
    mut input: LogicalPlan,
    table: &TableRef,
    globals_schema: &Schema,
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
                    .dtype_with_globals(input.schema().as_arrow(), globals_schema)?
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
                    globals_schema,
                    bindings,
                )?;
                input = LogicalPlanBuilder::from(input).filter(value)?.build()?;
            }
            Step::Explode { value, parameter } => {
                let name = unique(used, "__vail_agg_explode");
                let expr = resolve(value, &scope)?;
                if !matches!(
                    expr.dtype_with_globals(input.schema().as_arrow(), globals_schema)?,
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
                    globals_schema,
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
    let mut native_collects = BTreeMap::new();
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
                globals_schema,
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
                            if i < globals_schema.fields().len() {
                                Expr::Global {
                                    name: globals_schema.field(i).name().clone(),
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
            lower_logical_with_bindings(value, input.schema(), globals_schema, bindings)?;
        let dtype = if matches!(reducer.function.as_str(), "count" | "count_where") {
            None
        } else {
            Some(
                value
                    .dtype_with_globals(input.schema().as_arrow(), globals_schema)?
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
        // Nullify false predicates; native COUNT then ignores both false and
        // missing values while supplying the zero empty identity and grouped
        // implementation.
        if reducer.function == "count_where" {
            logical_value =
                datafusion_functions::core::nullif().call(vec![logical_value, lit(false)]);
        }
        // Use DataFusion's grouped kernels rather than constructing one scalar
        // accumulator per group. Widen before summing as required by Hail.
        let native_sum_type = if reducer.function == "sum" {
            match dtype.as_ref() {
                Some(DataType::Boolean | DataType::Int32 | DataType::Int64) => {
                    Some(DataType::Int64)
                }
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
        let native_collect = reducer.function == "collect";
        let udf = match fold_udf {
            Some(udf) => udf,
            None if native_sums.contains_key(&reducer.name) => {
                datafusion_functions_aggregate::sum::sum_udaf()
            }
            None if reducer.function == "count" => {
                datafusion_functions_aggregate::count::count_udaf()
            }
            None if reducer.function == "count_where" => {
                datafusion_functions_aggregate::count::count_udaf()
            }
            // DataFusion's array_agg has native batch-oriented grouped and
            // scalar accumulators. Hail collect respects null elements and
            // leaves order unspecified unless an ordered input scope is used.
            None if native_collect => datafusion_functions_aggregate::array_agg::array_agg_udaf(),
            None => factory(reducer, dtype.as_ref())?,
        };
        let default = if native_collect {
            // A filtered/exploded scope may contribute no rows for a group.
            // join_scope restores that group with collect's valid empty-list
            // identity; array_agg's empty accumulator is null.
            ScalarValue::List(ScalarValue::new_list(
                &[],
                dtype
                    .as_ref()
                    .ok_or_else(|| TableError::Invalid("collect requires an input type".into()))?,
                true,
            ))
        } else {
            // Query the existing UDAF for its empty-input identity. This does
            // not evaluate input rows or build an executable aggregate plan.
            let argument = ScalarValue::try_from(dtype.as_ref().unwrap_or(&DataType::Int64))?;
            let dummy_schema = Arc::new(Schema::empty());
            let physical =
                AggregateExprBuilder::new(udf.clone(), vec![Arc::new(Literal::new(argument))])
                    .schema(dummy_schema)
                    .alias(&reducer.name)
                    .build()?;
            match native_sums.get(&reducer.name) {
                Some(zero) => zero.clone(),
                None => physical.create_accumulator()?.evaluate()?,
            }
        };
        if native_collect {
            native_collects.insert(reducer.name.clone(), default.clone());
        }
        defaults.insert(reducer.name.clone(), default);
        let aggregate = udf
            .call(vec![logical_value])
            .null_treatment(NullTreatment::RespectNulls)
            .build()?
            .alias(&reducer.name);
        aggregates.push(aggregate);
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
            None => match native_collects.get(&column.name) {
                Some(empty) => datafusion_functions::core::coalesce()
                    .call(vec![DFExpr::Column(column.clone()), lit(empty.clone())])
                    .alias(&column.name),
                None => DFExpr::Column(column),
            },
        })
        .collect::<Vec<_>>();
    let restored_keys = group_names
        .iter()
        .zip(key_names)
        .zip(&key_types)
        .map(|((group, value), dtype)| {
            // DataFusion keeps native grouping values. When its actual output
            // descriptor exactly matches the declared key type, restoration is
            // identity, including nested field metadata. Preserve the column's
            // ordering equivalence; changed descriptors still use restoration.
            let actual = result
                .schema()
                .field_with_unqualified_name(group)?
                .data_type();
            let restored = if actual == dtype {
                col(group)
            } else {
                crate::aggregation::logical_restore_group_key(col(group), dtype)?
            };
            Ok(restored.alias(value))
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
    // A grouped reduction that does not read globals must retain the row
    // stream's partitioning. Joining an unused singleton first can collapse
    // the stream and copy nested metadata before projection pruning.
    // Fold packing retains its positional globals contract.
    let mut needs_globals = false;
    for (_, expression) in keys.iter().chain(fields.iter()) {
        expression.visit_unique(&mut |node| {
            needs_globals |= matches!(node, Expr::Global { .. } | Expr::AggregateFold { .. });
        });
    }
    let child = if needs_globals {
        crate::logical_globals::bind_globals(child, table, context)?
    } else {
        child
    };
    let empty_input_globals = Schema::empty();
    let input_globals = if needs_globals {
        table.globals().schema().as_ref()
    } else {
        &empty_input_globals
    };
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
            lower_logical_with_bindings(expr, child.schema(), input_globals, &indices)?
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
            expr.dtype_with_globals(projected.schema().as_arrow(), input_globals)?;
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
    projected = crate::group_ranges::certify(table, keys, &group_names, projected)?;
    let (mut result, _, _) = scope_plan(
        projected.clone(),
        table,
        input_globals,
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
            input_globals,
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
    // This stage only needs table globals when an unreduced output expression
    // reads one. Aggregate inputs and grouping expressions were bound before
    // their scopes were planned; a final singleton join here would otherwise
    // erase the range ordering of the grouped result.
    let needs_table_globals = outputs.iter().any(|(_, expr)| {
        let mut found = false;
        expr.visit_unique(&mut |node| {
            found |= matches!(node, Expr::Global { .. });
        });
        found
    });
    if needs_table_globals {
        result = crate::logical_globals::bind_globals(result, table, context)?;
    }
    let (result, output_indices) =
        crate::logical_globals::bind_external(result, bindings, context)?;
    let empty_globals = Schema::empty();
    let output_globals = if needs_table_globals {
        table.globals().schema()
    } else {
        &empty_globals
    };
    let mut projection = keys
        .iter()
        .zip(&key_names)
        .map(|((name, _), private)| col(private).alias(name))
        .collect::<Vec<_>>();
    for (name, expr) in outputs {
        projection.push(
            lower_logical_with_bindings(&expr, result.schema(), output_globals, &output_indices)?
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
        // Public collect is unordered and may use grouped partial aggregation,
        // but total collects keep their existing source-order behavior.
        let order_sensitive = needs_order(expression) || contains_collect(expression);
        let child = if order_sensitive {
            crate::logical::ordered(child)
        } else {
            child
        };
        // Bind only globals read by this aggregate. The independent-consumer
        // boundary retains its input schema, so an unused singleton list joined
        // here would otherwise be copied onto every row before pruning.
        // Fold packing still uses its existing positional globals contract.
        let mut needs_globals = false;
        expression.visit_unique(&mut |node| {
            needs_globals |= matches!(node, Expr::Global { .. } | Expr::AggregateFold { .. });
        });
        let mut child = if needs_globals {
            crate::logical_globals::bind_globals(child, table, context)?
        } else { child };
        let (bound_child, indices) =
            crate::logical_globals::bind_external(child, bindings, context)?;
        child = bound_child;
        // Only split underparallelized input; retain each existing partition
        // and its Arrow buffers rather than coalescing raw nested arrays.
        let target = crate::logical::total_aggregate_target(table, expression);
        if crate::logical::should_fan_out_total(table, expression)
            && !table.known_n_partitions().is_some_and(|partitions| partitions >= target)
        {
            child = crate::logical::split_partitions(child, target);
        }
        let empty_globals = Schema::empty();
        let input_globals = if needs_globals { table.globals().schema().as_ref() } else { &empty_globals };
        for group in groups {
            let (right, _, _) = scope_plan(
                child.clone(),
                table,
                input_globals,
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
        table.globals().schema(),
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
        table.globals().schema(),
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
