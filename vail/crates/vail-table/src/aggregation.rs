//! Aggregate scope extraction, initializer binding, and shared native kernels.
//! Relational planning lives in logical_aggregation and logical_nested_aggregation.
use crate::{Result, TableError};
use arrow_array::StructArray;
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::{Expr, lower_with_bindings};

fn empty_struct_scalar(missing: bool) -> ScalarValue {
    let nulls = missing.then(|| NullBuffer::new_null(1));
    ScalarValue::Struct(Arc::new(StructArray::new_empty_fields(1, nulls)))
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
            ..
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
            *initial = Arc::new(Expr::RuntimeLiteral {
                value: bind("fold", initial)?,
            });
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

pub(crate) fn lower_fold(
    expr: &Expr,
    input: &Schema,
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
    let state_type = vail_execution::expressions::Type::from_arrow(&initial.data_type())?.to_arrow()?;
    let initial_array: arrow_array::ArrayRef = match initial {
        ScalarValue::List(array) => array.clone(),
        ScalarValue::LargeList(array) => array.clone(),
        ScalarValue::FixedSizeList(array) => array.clone(),
        ScalarValue::Struct(array) => array.clone(),
        value => value.to_array()?,
    };
    let initial_array = if vail_ordering::same_layout(initial_array.data_type(), &state_type) {
        vail_execution::logical_expr::restore_arrow_array_type(&initial_array, &state_type)?
    } else {
        let cast = arrow_cast::cast(&initial_array, &state_type)?;
        vail_execution::logical_expr::restore_arrow_array_type(&cast, &state_type)?
    };
    let initial = vail_execution::expressions::scalar_at(&initial_array, 0)?;
    let mut used = input.fields().iter().map(|f| f.name().clone()).collect();
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
    fn captures(node: &Expr, input: &Schema, bindings: &BTreeMap<String, usize>,
        seen: &mut HashSet<*const Expr>, used: &mut HashSet<usize>) {
        if !seen.insert(node as *const Expr) { return; }
        let index = match node {
            Expr::Column { name } | Expr::Global { name } => input.index_of(name).ok(),
            Expr::ExternalGlobal { reference, .. } => bindings.get(reference).copied(),
            _ => None,
        };
        if let Some(index) = index { used.insert(index); }
        node.visit_children(&mut |child| captures(child,input,bindings,seen,used));
    }
    let mut captured = HashSet::new();
    captures(&seq,input,indices,&mut HashSet::new(),&mut captured);
    let payload_schema = Arc::new(Schema::new(input.fields().iter().enumerate()
        .filter(|(index,_)| captured.contains(index)).map(|(_,field)| {
            let dtype = vail_execution::expressions::Type::from_arrow(field.data_type())?.to_arrow()?;
            Ok(Arc::new(field.as_ref().clone().with_data_type(dtype).with_nullable(true)))
        }).collect::<Result<Vec<_>>>()?));
    let mut fields = vec![Arc::new(Field::new(&state, state_type.clone(), true))];
    fields.extend(payload_schema.fields().iter().cloned());
    let seq_schema = Arc::new(Schema::new(fields));
    let seq_indices = indices
        .iter()
        .filter(|(_, index)| captured.contains(index))
        .map(|(name, i)| {
            Ok((
                name.clone(),
                payload_schema.index_of(input.field(*i).name())? + 1,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let seq = lower_with_bindings(&seq, &seq_schema, globals, &seq_indices)?;
    let seq = vail_execution::expressions::cast_physical_storage(seq, &seq_schema, state_type.clone())?;
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
    if combine.dtype(&combine_schema)?.to_arrow()? != state_type {
        return Err(TableError::Invalid("fold combine type mismatch".into()));
    }
    let combine = lower_with_bindings(
        &combine,
        &combine_schema,
        &Schema::empty(),
        &BTreeMap::new(),
    )?;
    let combine = vail_execution::expressions::cast_physical_storage(combine, &combine_schema, state_type.clone())?;
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
    let packed = lower_with_bindings(&packed, input, &Schema::empty(), &BTreeMap::new())?;
    let packed = vail_execution::expressions::cast_physical_storage(packed, input, DataType::Struct(payload_schema.fields().clone()))?;
    Ok((
        Arc::new(vail_execution::fold_aggregate::fold_udaf(
            payload_schema,
            initial,
            state_type,
            seq,
            combine,
        )?),
        packed,
    ))
}
