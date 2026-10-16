//! Context-aware rewrites over original borrowed expression DAG nodes.
use pyo3::PyResult;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};
use vail_execution::expressions::{Expr, PrimitiveType, Type};

fn depends(root: &Expr, parameters: &BTreeSet<String>) -> bool {
    fn visit(node: &Expr, parameters: &BTreeSet<String>, seen: &mut HashSet<*const Expr>) -> bool {
        if !seen.insert(node as *const Expr) {
            return false;
        }
        if matches!(node,Expr::LambdaVariable{name} if parameters.contains(name)) {
            return true;
        }
        let mut found = false;
        node.visit_children(&mut |child| {
            if !found {
                found = visit(child, parameters, seen);
            }
        });
        found
    }
    visit(root, parameters, &mut HashSet::new())
}
fn minimum(value: Expr) -> Expr {
    Expr::Aggregate {
        function: "min".into(),
        value: Some(Arc::new(value)),
        options: serde_json::json!({}),
        result_type: Some(Type::Primitive(PrimitiveType::Int64)),
        init: Vec::new(),
    }
}
pub(crate) fn rows_referenced(root: &Expr) -> bool {
    fn visit(node: &Expr, seen: &mut HashSet<*const Expr>) -> bool {
        if !seen.insert(node as *const Expr) {
            return false;
        }
        if matches!(node, Expr::Column { .. })
            || matches!(node,Expr::MatrixField{axis,..}|Expr::MatrixScope{axis,..} if axis!="global")
        {
            return true;
        }
        let mut found = false;
        node.visit_children(&mut |child| {
            if !found {
                found = visit(child, seen);
            }
        });
        found
    }
    visit(root, &mut HashSet::new())
}
#[derive(Clone, PartialEq, Eq, Hash)]
struct Scope {
    inside: bool,
    result_parameters: BTreeSet<String>,
}
struct Binder<'a> {
    row: Option<&'a Expr>,
    group: bool,
    prefix: &'a [Arc<Expr>],
    memo: HashMap<(*const Expr, Scope), Expr>,
}
impl Binder<'_> {
    fn rewrite(&mut self, node: &Expr, scope: &Scope) -> PyResult<Expr> {
        let key = (node as *const Expr, scope.clone());
        if let Some(value) = self.memo.get(&key) {
            return Ok(value.clone());
        }
        let yes = Scope {
            inside: true,
            ..scope.clone()
        };
        let no = Scope {
            inside: false,
            ..scope.clone()
        };
        let result = match node {
            Expr::Aggregate {
                function,
                value,
                options,
                result_type,
                init,
            } => Expr::Aggregate {
                function: function.clone(),
                value: value
                    .as_ref()
                    .map(|v| self.rewrite(v, &yes).map(Arc::new))
                    .transpose()?,
                options: options.clone(),
                result_type: result_type.clone(),
                init: init
                    .iter()
                    .map(|(name, value)| Ok((name.clone(), self.rewrite(value, &no)?)))
                    .collect::<PyResult<_>>()?,
            },
            Expr::AggregateFold {
                initial,
                seq,
                combine,
                state_parameter,
                left_parameter,
                right_parameter,
            } => Expr::AggregateFold {
                initial: Arc::new(self.rewrite(initial, &no)?),
                seq: Arc::new(self.rewrite(seq, &yes)?),
                combine: Arc::new(self.rewrite(combine, &no)?),
                state_parameter: state_parameter.clone(),
                left_parameter: left_parameter.clone(),
                right_parameter: right_parameter.clone(),
            },
            Expr::AggFilter { predicate, body } => Expr::AggFilter {
                predicate: Arc::new(self.rewrite(predicate, &yes)?),
                body: Arc::new(self.rewrite(body, &no)?),
            },
            Expr::AggExplode {
                value,
                parameter,
                body,
            } => Expr::AggExplode {
                value: Arc::new(self.rewrite(value, &yes)?),
                parameter: parameter.clone(),
                body: Arc::new(self.rewrite(body, &no)?),
            },
            Expr::AggArray {
                value,
                parameter,
                body,
            } => {
                let mut result_scope = no.clone();
                result_scope.result_parameters.insert(parameter.clone());
                Expr::AggArray {
                    value: Arc::new(self.rewrite(value, &yes)?),
                    parameter: parameter.clone(),
                    body: Arc::new(self.rewrite(body, &result_scope)?),
                }
            }
            Expr::AggGroupBy { key, body } => Expr::AggGroupBy {
                key: Arc::new(self.rewrite(key, &yes)?),
                body: Arc::new(self.rewrite(body, &no)?),
            },
            _ => node.try_map_original_children(|child| self.rewrite(child, scope))?,
        };
        let mut result = result;
        if let Expr::NestedFunction { function, args, .. } = &mut result {
            if function.starts_with("random_") {
                let Some(Expr::Array { elements, .. }) = args.get_mut(2) else {
                    return Err(crate::value_error(
                        "random UID must be an array constructor",
                    ));
                };
                let mut prefix = self
                    .prefix
                    .iter()
                    .map(|v| v.as_ref().clone())
                    .collect::<Vec<_>>();
                if let Some(row) = self.row {
                    if scope.inside {
                        prefix.push(row.clone());
                    } else if self.group {
                        prefix.push(minimum(row.clone()));
                    }
                    if !scope.inside {
                        for word in elements.iter_mut() {
                            let depends = depends(word, &scope.result_parameters);
                            if depends {
                                *word = minimum(word.clone());
                            }
                        }
                    }
                }
                prefix.append(elements);
                *elements = prefix;
            }
        }
        self.memo.insert(key, result.clone());
        Ok(result)
    }
}
pub(crate) fn bind_rows(root: &Expr, row: &Expr, mode: &str) -> PyResult<Expr> {
    if !matches!(mode, "row" | "global" | "total" | "group") {
        return Err(crate::value_error("invalid random binding mode"));
    }
    Binder {
        row: Some(row),
        group: mode == "group",
        prefix: &[],
        memo: HashMap::new(),
    }
    .rewrite(
        root,
        &Scope {
            inside: mode == "row",
            result_parameters: BTreeSet::new(),
        },
    )
}
pub(crate) fn context(root: &Expr, uid: &[Arc<Expr>]) -> PyResult<Expr> {
    // Context prefixing does not change aggregate input/result scopes.
    fn rewrite(
        node: &Expr,
        uid: &[Arc<Expr>],
        memo: &mut HashMap<*const Expr, Expr>,
    ) -> PyResult<Expr> {
        let key = node as *const Expr;
        if let Some(value) = memo.get(&key) {
            return Ok(value.clone());
        }
        let mut result = node.try_map_original_children(|child| rewrite(child, uid, memo))?;
        if let Expr::NestedFunction { function, args, .. } = &mut result {
            if function.starts_with("random_") {
                let Some(Expr::Array { elements, .. }) = args.get_mut(2) else {
                    return Err(crate::value_error(
                        "random UID must be an array constructor",
                    ));
                };
                let mut prefix = uid.iter().map(|v| v.as_ref().clone()).collect::<Vec<_>>();
                prefix.append(elements);
                *elements = prefix;
            }
        }
        memo.insert(key, result.clone());
        Ok(result)
    }
    rewrite(root, uid, &mut HashMap::new())
}
pub(crate) fn replace_parameter(root: &Expr, name: &str, value: &Expr) -> PyResult<Expr> {
    fn rewrite(
        node: &Expr,
        name: &str,
        value: &Expr,
        shadowed: bool,
        memo: &mut HashMap<(*const Expr, bool), Expr>,
    ) -> PyResult<Expr> {
        let key = (node as *const Expr, shadowed);
        if let Some(result) = memo.get(&key) {
            return Ok(result.clone());
        }
        let result =
            if !shadowed && matches!(node,Expr::LambdaVariable{name:variable} if variable==name) {
                value.clone()
            } else {
                let mut position = 0;
                node.try_map_original_children(|child| {
                    let index = position;
                    position += 1;
                    let binds = match node {
                        Expr::ArrayMap { parameter, .. }
                        | Expr::ArrayFilter { parameter, .. }
                        | Expr::ArrayPredicate { parameter, .. }
                        | Expr::AggArray { parameter, .. }
                        | Expr::AggExplode { parameter, .. } => index == 1 && parameter == name,
                        Expr::ArrayFold {
                            accumulator,
                            parameter,
                            ..
                        } => index == 2 && (accumulator == name || parameter == name),
                        _ => false,
                    };
                    rewrite(child, name, value, shadowed || binds, memo)
                })?
            };
        memo.insert(key, result.clone());
        Ok(result)
    }
    rewrite(root, name, value, false, &mut HashMap::new())
}
