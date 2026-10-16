//! Typed collection-aggregate rewriting over original borrowed DAG nodes.
use crate::value_error;
use arrow_schema::{DataType, Schema};
use pyo3::PyResult;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::{ArrayReduction, Expr, PrimitiveType as P, Type, UnaryOperator};

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
}
impl Rewrite<'_> {
    fn type_view(&mut self, node: &Expr, scope: &[(String, Type)]) -> PyResult<Expr> {
        let key = (node as *const Expr, scope.to_vec());
        if let Some(value) = self.views.get(&key) {
            return Ok(value.clone());
        }
        let typed_null = |ty: &Type| -> PyResult<Expr> {
            Ok(Expr::Literal {
                dtype: ty.clone(),
                value: serde_json::Value::Null,
            })
        };
        let value = match node {
            Expr::MatrixField { axis, name } => {
                let Some(Type::Struct { r#struct: fields }) = self.context.axes.get(axis) else {
                    return Err(value_error("unknown matrix callback axis"));
                };
                typed_null(
                    &fields
                        .iter()
                        .find(|(n, _)| n == name)
                        .ok_or_else(|| value_error("unknown matrix callback field"))?
                        .1,
                )?
            }
            Expr::MatrixScope { value, .. } => self.type_view(value, scope)?,
            Expr::MatrixNumCols {} | Expr::MatrixColumnPosition {} => {
                typed_null(&Type::Primitive(P::Int32))?
            }
            Expr::TableLookup {
                reference,
                all_matches,
                ..
            } => {
                let ty = self
                    .context
                    .lookups
                    .get(reference)
                    .ok_or_else(|| value_error("unknown callback lookup"))?;
                typed_null(&if *all_matches {
                    Type::Array {
                        array: Box::new(ty.clone()),
                    }
                } else {
                    ty.clone()
                })?
            }
            _ => {
                let mut nested = scope.to_vec();
                let body_index = match node {
                    Expr::ArrayMap {
                        value, parameter, ..
                    }
                    | Expr::ArrayFilter {
                        value, parameter, ..
                    }
                    | Expr::ArrayPredicate {
                        value, parameter, ..
                    } => {
                        let ty = self.dtype_scoped(value, scope)?;
                        let element = match ty {
                            Type::Array { array } | Type::Set { set: array } => *array,
                            Type::Dict { dict } => Type::Struct {
                                r#struct: vec![("key".into(), dict.0), ("value".into(), dict.1)],
                            },
                            _ => return Err(value_error("callback map requires collection")),
                        };
                        nested.push((parameter.clone(), element));
                        Some(1)
                    }
                    Expr::ArrayFold {
                        value,
                        zero,
                        accumulator,
                        parameter,
                        ..
                    } => {
                        let Type::Array { array } = self.dtype_scoped(value, scope)? else {
                            return Err(value_error("callback fold requires array"));
                        };
                        nested.push((accumulator.clone(), self.dtype_scoped(zero, scope)?));
                        nested.push((parameter.clone(), *array));
                        Some(2)
                    }
                    Expr::Switch {
                        value, parameter, ..
                    } => {
                        nested.push((parameter.clone(), self.dtype_scoped(value, scope)?));
                        Some(usize::MAX)
                    }
                    _ => None,
                };
                let mut position = 0;
                node.try_map_original_children(|child| {
                    let index = position;
                    position += 1;
                    let child_scope = if body_index == Some(index)
                        || (body_index == Some(usize::MAX) && index > 0)
                    {
                        &nested[..]
                    } else {
                        scope
                    };
                    if matches!(child, Expr::Literal { .. } | Expr::RuntimeLiteral { .. }) {
                        Ok(child.clone())
                    } else {
                        typed_null(&self.dtype_scoped(child, child_scope)?)
                    }
                })?
            }
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
                body,
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
    fn walk(&mut self, node: &Expr, items: Arc<Expr>) -> PyResult<Expr> {
        let key = (node as *const Expr, Arc::as_ptr(&items));
        if let Some(value) = self.memo.get(&key) {
            return Ok(value.clone());
        }
        let result = match node {
            Expr::AggFilter { predicate, body } => {
                if self.dtype(predicate)? != Type::Primitive(P::Bool) {
                    return Err(value_error("collection filter requires bool"));
                }
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
                    let index = Arc::new(Expr::LambdaVariable { name: name.clone() });
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
                });
                let filtered = Expr::ArrayFilter {
                    value: Arc::new(zipped),
                    parameter: self.parameter.into(),
                    predicate: Arc::new(Expr::GetField {
                        value: variable.clone(),
                        name: "2".into(),
                    }),
                };
                let values = Expr::ArrayMap {
                    value: Arc::new(filtered),
                    parameter: self.parameter.into(),
                    body: Arc::new(Expr::GetField {
                        value: variable,
                        name: "1".into(),
                    }),
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
                    let body = if matches!(function.as_str(), "sum" | "product") {
                        Expr::Cast {
                            dtype: result.clone(),
                            value: Arc::new(body.clone()),
                        }
                    } else {
                        body.clone()
                    };
                    let mapped = Arc::new(self.mapped(items, &body)?);
                    match function.as_str() {
                        "collect" => (*mapped).clone(),
                        "collect_as_set" => Expr::NestedFunction {
                            function: "array_distinct".into(),
                            args: vec![(*mapped).clone()],
                            dtype: result,
                        },
                        "sum" | "product" | "min" | "max" => Expr::ArrayReduce {
                            operation: match function.as_str() {
                                "sum" => ArrayReduction::Sum,
                                "product" => ArrayReduction::Product,
                                "min" => ArrayReduction::Min,
                                _ => ArrayReduction::Max,
                            },
                            filter_missing: true,
                            value: mapped,
                        },
                        "any" | "all" => {
                            let variable = Arc::new(Expr::LambdaVariable {
                                name: self.parameter.into(),
                            });
                            let predicate = if function == "all" {
                                Arc::new(Expr::Unary {
                                    operator: UnaryOperator::Not,
                                    value: variable,
                                })
                            } else {
                                variable
                            };
                            let any = Expr::ArrayPredicate {
                                value: mapped,
                                parameter: self.parameter.into(),
                                predicate,
                                first: false,
                            };
                            if function == "all" {
                                Expr::Unary {
                                    operator: UnaryOperator::Not,
                                    value: Arc::new(any),
                                }
                            } else {
                                any
                            }
                        }
                        _ => {
                            return Err(value_error(format!(
                                "unsupported collection aggregate {function}"
                            )));
                        }
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
    }
    .walk(body, value)
}

/// Free-name summaries use binder positions, never generated-name conventions.
pub(crate) fn free_parameters(root: &Expr) -> HashSet<String> {
    fn summarize(node: &Expr, memo: &mut HashMap<*const Expr, HashSet<String>>) -> HashSet<String> {
        let key = node as *const Expr;
        if let Some(names) = memo.get(&key) {
            return names.clone();
        }
        let mut names = HashSet::new();
        if let Expr::LambdaVariable { name } = node {
            names.insert(name.clone());
        }
        let mut position = 0;
        node.visit_children(&mut |child| {
            let index = position;
            position += 1;
            let mut child_names = summarize(child, memo);
            match node {
                Expr::ArrayMap { parameter, .. }
                | Expr::ArrayFilter { parameter, .. }
                | Expr::ArrayPredicate { parameter, .. }
                | Expr::AggArray { parameter, .. }
                | Expr::AggExplode { parameter, .. }
                    if index == 1 =>
                {
                    child_names.remove(parameter);
                }
                Expr::ArrayFold {
                    accumulator,
                    parameter,
                    ..
                } if index == 2 => {
                    child_names.remove(accumulator);
                    child_names.remove(parameter);
                }
                Expr::AggregateFold {
                    state_parameter, ..
                } if index == 1 => {
                    child_names.remove(state_parameter);
                }
                Expr::AggregateFold {
                    left_parameter,
                    right_parameter,
                    ..
                } if index == 2 => {
                    child_names.remove(left_parameter);
                    child_names.remove(right_parameter);
                }
                Expr::Switch { parameter, .. } if index > 0 => {
                    child_names.remove(parameter);
                }
                _ => (),
            }
            names.extend(child_names);
        });
        memo.insert(key, names.clone());
        names
    }
    summarize(root, &mut HashMap::new())
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
