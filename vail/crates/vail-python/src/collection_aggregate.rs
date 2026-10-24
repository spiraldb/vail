//! Typed collection-aggregate rewriting over original borrowed DAG nodes.
use crate::value_error;
use arrow_schema::{DataType, Schema};
use pyo3::PyResult;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::{Expr, PrimitiveType as P, Type};

pub(crate) struct Context {
    pub rows: Schema,
    pub globals: Schema,
    pub axes: HashMap<String, Type>,
    pub lookups: HashMap<String, Type>,
    pub lexical: Vec<(String, Type)>,
}
pub(crate) fn schema(ty: &Type) -> PyResult<Schema> {
    let DataType::Struct(fields) = ty.to_arrow().map_err(value_error)? else {
        return Err(value_error(
            "collection callback environment must be a struct",
        ));
    };
    Ok(Schema::new(fields))
}
struct Rewrite<'a> {
    context: &'a Context,
    parameter: &'a str,
    element: Type,
    views: HashMap<(*const Expr, Vec<(String, Type)>), Expr>,
    types: HashMap<(*const Expr, Vec<(String, Type)>), Type>,
    memo: HashMap<(*const Expr, *const Expr), Expr>,
    retained_items: Vec<Arc<Expr>>,
}
impl Rewrite<'_> {
    fn type_view(&mut self, node: &Expr, scope: &[(String, Type)]) -> PyResult<Expr> {
        let key = (node as *const Expr, scope.to_vec());
        if let Some(value) = self.views.get(&key) {
            return Ok(value.clone());
        }
        // Annotate actual native nodes with the supplied frontend schema.
        // The execution type checker handles lexical binders over the real DAG;
        // no typed-null stand-in tree is needed for inquiry.
        let value = match node {
            Expr::MatrixField { axis, name, .. } => {
                let Some(Type::Struct { r#struct: fields }) = self.context.axes.get(axis) else {
                    return Err(value_error("unknown matrix callback axis"));
                };
                let dtype = fields
                    .iter()
                    .find(|(field, _)| field == name)
                    .ok_or_else(|| value_error("unknown matrix callback field"))?
                    .1
                    .clone();
                Expr::MatrixField {
                    axis: axis.clone(),
                    name: name.clone(),
                    dtype: Some(dtype),
                }
            }
            Expr::TableLookup {
                reference,
                keys,
                all_matches,
                query_interval,
                ..
            } => {
                let dtype = self
                    .context
                    .lookups
                    .get(reference)
                    .ok_or_else(|| value_error("unknown callback lookup"))?
                    .clone();
                Expr::TableLookup {
                    reference: reference.clone(),
                    keys: keys
                        .iter()
                        .map(|key| self.type_view(key, scope))
                        .collect::<PyResult<_>>()?,
                    all_matches: *all_matches,
                    query_interval: *query_interval,
                    dtype: Some(if *all_matches {
                        Type::Array {
                            array: Box::new(dtype),
                        }
                    } else {
                        dtype
                    }),
                }
            }
            _ => node.try_map_original_children(|child| self.type_view(child, scope))?,
        };
        self.views.insert(key, value.clone());
        Ok(value)
    }
    fn dtype(&mut self, node: &Expr) -> PyResult<Type> {
        self.dtype_scoped(node, &self.context.lexical.clone())
    }
    fn dtype_scoped(&mut self, node: &Expr, scope: &[(String, Type)]) -> PyResult<Type> {
        let key = (node as *const Expr, scope.to_vec());
        if let Some(ty) = self.types.get(&key) {
            return Ok(ty.clone());
        }
        let view = self.type_view(node, scope)?;
        let ty = view
            .dtype_with_lexical(&self.context.rows, &self.context.globals, scope)
            .map_err(value_error)?;
        self.types.insert(key, ty.clone());
        Ok(ty)
    }
    fn mapped(&self, items: Arc<Expr>, body: &Expr) -> PyResult<Expr> {
        self.mapped_as(items, body, self.parameter, &self.element)
    }
    fn mapped_as(
        &self,
        items: Arc<Expr>,
        body: &Expr,
        parameter: &str,
        element: &Type,
    ) -> PyResult<Expr> {
        let mut random = false;
        let mut seen = HashSet::new();
        fn contains(node: &Expr, seen: &mut HashSet<*const Expr>) -> bool {
            if !seen.insert(node as *const Expr) {
                return false;
            }
            if matches!(node,Expr::NestedFunction { function,.. } if function.starts_with("random_"))
            {
                return true;
            }
            let mut found = false;
            node.visit_children(&mut |child| {
                found |= contains(child, seen);
            });
            found
        }
        random |= contains(body, &mut seen);
        let (items, body) = if random {
            let variable = Arc::new(Expr::LambdaVariable {
                name: parameter.into(),
                dtype: None,
            });
            let value = Expr::GetField {
                value: variable.clone(),
                name: "__vail_tuple_1".into(),
            };
            let body = crate::random_binding::replace_parameter(body, parameter, &value)?;
            let index = Arc::new(Expr::Cast {
                dtype: Type::Primitive(P::Int64),
                value: Arc::new(Expr::GetField {
                    value: variable,
                    name: "__vail_tuple_0".into(),
                }),
            });
            let body = crate::random_binding::context(&body, &[index])?;
            let pair = Type::Tuple {
                tuple: vec![Type::Primitive(P::Int32), element.clone()],
            };
            (
                Arc::new(Expr::NestedFunction {
                    function: "__vail_index_values".into(),
                    args: vec![(*items).clone()],
                    dtype: Type::Array {
                        array: Box::new(pair),
                    },
                }),
                Arc::unwrap_or_clone(body),
            )
        } else {
            (items, body.clone())
        };
        Ok(Expr::ArrayMap {
            value: items,
            parameter: parameter.into(),
            body: Arc::new(body),
        })
    }
    fn validate_list_initializer(&self, value: &Expr, function: &str) -> PyResult<()> {
        // A per-list initializer is closed over the current collection element,
        // but may use its owning row or enclosing lexical callbacks.
        let allowed = self
            .context
            .lexical
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<HashSet<_>>();
        let mut invalid = free_parameters(value)
            .iter()
            .any(|name| name == self.parameter || !allowed.contains(name.as_str()));
        value.visit_unique(&mut |node| {
            invalid |= matches!(
                node,
                Expr::Aggregate { .. }
                    | Expr::AggregateFold { .. }
                    | Expr::AggFilter { .. }
                    | Expr::AggExplode { .. }
                    | Expr::AggGroupBy { .. }
                    | Expr::AggArray { .. }
            )
        });
        if invalid {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "{function} initialization expression depends on collection elements or aggregations"
            )));
        }
        Ok(())
    }
    fn walk(&mut self, node: &Expr, items: Arc<Expr>) -> PyResult<Expr> {
        let key = (node as *const Expr, Arc::as_ptr(&items));
        if let Some(value) = self.memo.get(&key) {
            return Ok(value.clone());
        }
        self.retained_items.push(items.clone());
        let result = match node {
            Expr::AggFilter { predicate, body } => {
                if self.dtype(predicate)? != Type::Primitive(P::Bool) {
                    return Err(value_error("collection filter requires bool"));
                }
                let values = if items.is_pure() && predicate.is_pure() && !has_random(body) {
                    // Apply the original predicate through DataFusion's native
                    // array_filter. No boolean-list/zip/unwrap intermediates.
                    // Random and potentially failing inputs retain their exact
                    // historical evaluation/UID context below.
                    Expr::ArrayFilter {
                        value: items,
                        parameter: self.parameter.into(),
                        predicate: predicate.clone(),
                    }
                } else {
                    // Mapping binds seeded callback positions just as ordinary array.map.
                    let selected = self.mapped(items.clone(), predicate)?;
                    let pair = Type::Struct {
                        r#struct: vec![
                            ("1".into(), self.element.clone()),
                            ("2".into(), Type::Primitive(P::Bool)),
                        ],
                    };
                    let zipped = if has_random(&items) || has_random(&selected) {
                        // Historical random zip captures the arrays inside an
                        // indexed map, adding that output position to their UID.
                        let used = free_parameters(node)
                            .into_iter()
                            .chain(free_parameters(&items))
                            .collect::<HashSet<_>>();
                        let name = (0..)
                            .map(|i| format!("{}_zip_{i}", self.parameter))
                            .find(|name| !used.contains(name))
                            .unwrap();
                        let index = Arc::new(Expr::LambdaVariable {
                            name: name.clone(),
                            dtype: None,
                        });
                        let tuple = Expr::Struct {
                            fields: vec![
                                (
                                    "1".into(),
                                    Expr::ArrayIndex {
                                        value: items.clone(),
                                        index: index.clone(),
                                    },
                                ),
                                (
                                    "2".into(),
                                    Expr::ArrayIndex {
                                        value: Arc::new(selected),
                                        index,
                                    },
                                ),
                            ],
                        };
                        let integer = |n| {
                            Arc::new(Expr::Literal {
                                dtype: Type::Primitive(P::Int32),
                                value: serde_json::json!(n),
                            })
                        };
                        let range = Arc::new(Expr::StreamRange {
                            start: integer(0),
                            stop: Arc::new(Expr::ArrayLength {
                                value: items.clone(),
                            }),
                            step: integer(1),
                        });
                        self.mapped_as(range, &tuple, &name, &Type::Primitive(P::Int32))?
                    } else {
                        Expr::NestedFunction {
                            function: "arrays_zip".into(),
                            args: vec![(*items).clone(), selected],
                            dtype: Type::Array {
                                array: Box::new(pair),
                            },
                        }
                    };
                    let variable = Arc::new(Expr::LambdaVariable {
                        name: self.parameter.into(),
                        dtype: None,
                    });
                    let filtered = Expr::ArrayFilter {
                        value: Arc::new(zipped),
                        parameter: self.parameter.into(),
                        predicate: Arc::new(Expr::GetField {
                            value: variable.clone(),
                            name: "2".into(),
                        }),
                    };
                    Expr::ArrayMap {
                        value: Arc::new(filtered),
                        parameter: self.parameter.into(),
                        body: Arc::new(Expr::GetField {
                            value: variable,
                            name: "1".into(),
                        }),
                    }
                };
                self.walk(body, Arc::new(values))?
            }
            Expr::Aggregate {
                function,
                value,
                options,
                result_type,
                init,
            } => {
                let input = value.as_deref().map(|v| self.dtype(v)).transpose()?;
                let initializer_values = init;
                let init = init
                    .iter()
                    .map(|(name, v)| Ok((name.clone(), self.dtype(v)?)))
                    .collect::<PyResult<Vec<_>>>()?;
                let result = vail_execution::aggregate_contract::result_type(
                    function,
                    input.as_ref(),
                    options,
                    &init,
                )
                .map_err(value_error)?;
                if result_type.as_ref().is_some_and(|ty| ty != &result) {
                    return Err(value_error("collection aggregate result type mismatch"));
                }
                if function == "count" {
                    Expr::Cast {
                        dtype: Type::Primitive(P::Int64),
                        value: Arc::new(Expr::ArrayLength { value: items }),
                    }
                } else {
                    let body = value
                        .as_deref()
                        .ok_or_else(|| value_error("collection reducer requires input"))?;
                    let mut args = if function == "call_stats" {
                        Vec::new()
                    } else {
                        vec![self.mapped(items.clone(), body)?]
                    };
                    let reducer = match function.as_str() {
                        "take" => {
                            let n = options
                                .get("n")
                                .and_then(serde_json::Value::as_u64)
                                .ok_or_else(|| value_error("take requires a nonnegative count"))?;
                            args.push(Expr::RuntimeLiteral {
                                value: datafusion_common::ScalarValue::Int64(Some(n as i64)),
                            });
                            if options.get("ordered").and_then(serde_json::Value::as_bool)
                                == Some(true)
                            {
                                "take_ordered"
                            } else {
                                "take"
                            }
                        }
                        "call_stats" => {
                            let initializer = if let Some((_, value)) = initializer_values
                                .iter()
                                .find(|(name, _)| name == "n_alleles")
                            {
                                self.validate_list_initializer(value, "call_stats")?;
                                value.clone()
                            } else {
                                let n = options
                                    .get("n_alleles")
                                    .and_then(serde_json::Value::as_u64)
                                    .and_then(|n| i32::try_from(n).ok())
                                    .ok_or_else(|| {
                                        value_error("invalid call_stats allele count")
                                    })?;
                                Expr::RuntimeLiteral {
                                    value: datafusion_common::ScalarValue::Int32(Some(n)),
                                }
                            };
                            // The direct list kernel accepts only Call values and
                            // takes the row/list initializer first. Avoid packing
                            // each list element into the generic reducer format.
                            let call = match body {
                                Expr::Struct { fields } => fields
                                    .iter()
                                    .find(|(name, _)| name == "__vail_genomic_call")
                                    .map(|(_, call)| call.clone())
                                    .ok_or_else(|| {
                                        value_error("call_stats aggregate is missing its Call input")
                                    })?,
                                _ if self.dtype(body)? == Type::Primitive(P::Call) => {
                                    body.clone()
                                }
                                _ => {
                                    return Err(value_error(
                                        "call_stats aggregate requires Call values",
                                    ));
                                }
                            };
                            let calls = self.mapped(items.clone(), &call)?;
                            args.push(initializer);
                            args.push(calls);
                            "__vail_list_call_stats"
                        }
                        other => other,
                    };
                    if function != "call_stats"
                        && !vail_execution::supports_list_reducer(reducer)
                    {
                        return Err(pyo3::exceptions::PyNotImplementedError::new_err(format!(
                            "per-list {function} reducer is not implemented"
                        )));
                    }
                    Expr::NestedFunction {
                        function: if function == "call_stats" {
                            reducer.into()
                        } else {
                            format!("__vail_list_reduce_{reducer}")
                        },
                        args,
                        dtype: result,
                    }
                }
            }
            Expr::AggExplode { .. }
            | Expr::AggArray { .. }
            | Expr::AggGroupBy { .. }
            | Expr::AggregateFold { .. } => {
                return Err(value_error("unsupported nested collection aggregation"));
            }
            _ => node.try_map_original_children(|child| self.walk(child, items.clone()))?,
        };
        self.memo.insert(key, result.clone());
        Ok(result)
    }
}
pub(crate) fn rewrite(
    value: Arc<Expr>,
    body: &Expr,
    parameter: &str,
    element: Type,
    context: &Context,
) -> PyResult<Expr> {
    Rewrite {
        context,
        parameter,
        element,
        views: HashMap::new(),
        types: HashMap::new(),
        memo: HashMap::new(),
        retained_items: Vec::new(),
    }
    .walk(body, value)
}

/// Free-name summaries use binder positions, never generated-name conventions.
pub(crate) fn free_parameters(root: &Expr) -> HashSet<String> {
    root.free_parameters()
}

fn has_random(root: &Expr) -> bool {
    fn visit(node: &Expr, seen: &mut HashSet<*const Expr>) -> bool {
        if !seen.insert(node as *const Expr) {
            return false;
        }
        if matches!(node,Expr::NestedFunction {function,..} if function.starts_with("random_")) {
            return true;
        }
        let mut yes = false;
        node.visit_children(&mut |child| yes |= visit(child, seen));
        yes
    }
    visit(root, &mut HashSet::new())
}

/// Initializer values are closed over globals, never current rows/reducer binders.
pub(crate) fn validate_initializer(
    node: &Expr,
    function: &str,
    allow_matrix_rows: bool,
) -> PyResult<()> {
    let mut invalid = false;
    node.visit_unique(&mut |node| {
        invalid |= match node {
            Expr::Column { .. }
            | Expr::Aggregate { .. }
            | Expr::AggregateFold { .. }
            | Expr::AggFilter { .. }
            | Expr::AggExplode { .. }
            | Expr::AggGroupBy { .. }
            | Expr::AggArray { .. }
            | Expr::MatrixColumnPosition {} => true,
            Expr::MatrixField { axis, .. } | Expr::MatrixScope { axis, .. } => {
                axis != "global" && !(allow_matrix_rows && axis == "row")
            }
            _ => false,
        }
    });
    if invalid || !free_parameters(node).is_empty() {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "{function} initialization expressions cannot depend on rows or aggregations"
        )));
    }
    Ok(())
}
