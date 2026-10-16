//! Query-local random stream binding on native expression DAGs.
use crate::expressions::{Expr, ExprError, PrimitiveType, Type};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

#[derive(Clone, PartialEq, Eq, Hash)]
struct Scope {
    inside: bool,
    result_parameters: BTreeSet<String>,
    shadowed: bool,
}

struct Rewrite<'a> {
    mode: Option<&'a str>,
    uid: &'a [Arc<Expr>],
    replacement: Option<(&'a str, &'a Arc<Expr>)>,
    memo: HashMap<(usize, Scope), Arc<Expr>>,
}

fn binders(expr: &Expr, position: usize) -> Vec<&str> {
    match expr {
        Expr::ArrayMap { parameter, .. }
        | Expr::ArrayFilter { parameter, .. }
        | Expr::ArrayPredicate { parameter, .. }
        | Expr::AggArray { parameter, .. }
        | Expr::AggExplode { parameter, .. }
            if position == 1 =>
        {
            vec![parameter]
        }
        Expr::ArrayFold {
            accumulator,
            parameter,
            ..
        } if position == 2 => vec![accumulator, parameter],
        Expr::AggregateFold {
            state_parameter, ..
        } if position == 1 => vec![state_parameter],
        Expr::AggregateFold {
            left_parameter,
            right_parameter,
            ..
        } if position == 2 => vec![left_parameter, right_parameter],
        _ => vec![],
    }
}

fn depends(
    expr: &Expr,
    parameters: BTreeSet<String>,
    memo: &mut HashMap<(usize, BTreeSet<String>), bool>,
) -> bool {
    let key = (expr as *const Expr as usize, parameters.clone());
    if let Some(found) = memo.get(&key) {
        return *found;
    }
    let mut found = matches!(expr, Expr::LambdaVariable { name, .. } if parameters.contains(name));
    let mut position = 0;
    expr.for_each_child(&mut |child| {
        let mut parameters = parameters.clone();
        for name in binders(expr, position) {
            parameters.remove(name);
        }
        found |= depends(child, parameters, memo);
        position += 1;
    });
    memo.insert(key, found);
    found
}

fn minimum(value: Arc<Expr>) -> Expr {
    Expr::Aggregate {
        function: "min".into(),
        value: Some(value),
        options: serde_json::Value::Null,
        result_type: Some(Type::Primitive(PrimitiveType::Int64)),
        init: vec![],
    }
}

impl Rewrite<'_> {
    fn visit(&mut self, expr: &Expr, scope: &Scope) -> Result<Arc<Expr>, ExprError> {
        let key = (expr as *const Expr as usize, scope.clone());
        if let Some(value) = self.memo.get(&key) {
            return Ok(value.clone());
        }
        if let Some((parameter, replacement)) = self.replacement {
            if !scope.shadowed
                && matches!(expr, Expr::LambdaVariable { name, .. } if name == parameter)
            {
                let result = replacement.clone();
                self.memo.insert(key, result.clone());
                return Ok(result);
            }
        }
        let mut children = vec![];
        expr.for_each_child(&mut |child| children.push(child));
        let mut children = children.into_iter().enumerate();
        let mut result = expr.try_map_children(|_| {
            let (position, child) = children.next().expect("native child order");
            let mut nested = scope.clone();
            if let Some((parameter, _)) = self.replacement {
                nested.shadowed |= binders(expr, position).contains(&parameter);
            }
            if self.mode.is_some() {
                for name in binders(expr, position) {
                    nested.result_parameters.remove(name);
                }
                nested.inside = match expr {
                    Expr::Aggregate { value, .. } => position == 0 && value.is_some(),
                    Expr::AggregateFold { .. } => position == 1,
                    Expr::AggFilter { .. }
                    | Expr::AggExplode { .. }
                    | Expr::AggArray { .. }
                    | Expr::AggGroupBy { .. } => position == 0,
                    _ => nested.inside,
                };
                if position == 1
                    && let Expr::AggArray { parameter, .. } = expr
                {
                    nested.result_parameters.insert(parameter.clone());
                }
            }
            self.visit(child, &nested).map(|value| (*value).clone())
        })?;
        if let Expr::NestedFunction { function, args, .. } = &mut result {
            if function.starts_with("random_") {
                let Some(Expr::Array { elements, .. }) = args.get_mut(2) else {
                    return Err(ExprError::Invalid(
                        "random stream UID must be a native array".into(),
                    ));
                };
                let mut prefix: Vec<Expr> = if self.mode.is_none() || scope.inside {
                    self.uid.iter().map(|value| (**value).clone()).collect()
                } else if self.mode == Some("group") {
                    self.uid.iter().cloned().map(minimum).collect()
                } else {
                    vec![]
                };
                if !scope.inside && self.mode == Some("group") {
                    let mut dependency_memo = HashMap::new();
                    for word in elements.iter_mut() {
                        if depends(word, scope.result_parameters.clone(), &mut dependency_memo) {
                            *word = minimum(Arc::new(word.clone()));
                        }
                    }
                }
                prefix.append(elements);
                *elements = prefix;
            }
        }
        let result = Arc::new(result);
        self.memo.insert(key, result.clone());
        Ok(result)
    }
}

pub fn bind_context(
    expr: &Expr,
    uid: &[Arc<Expr>],
    replacement: Option<(&str, &Arc<Expr>)>,
) -> Result<Arc<Expr>, ExprError> {
    Rewrite {
        mode: None,
        uid,
        replacement,
        memo: HashMap::new(),
    }
    .visit(
        expr,
        &Scope {
            inside: false,
            result_parameters: BTreeSet::new(),
            shadowed: false,
        },
    )
}

pub fn bind_rows(expr: &Expr, row_uid: Arc<Expr>, mode: &str) -> Result<Arc<Expr>, ExprError> {
    if !matches!(mode, "row" | "group" | "total" | "global") {
        return Err(ExprError::Invalid("invalid random binding scope".into()));
    }
    Rewrite {
        mode: Some(mode),
        uid: &[row_uid],
        replacement: None,
        memo: HashMap::new(),
    }
    .visit(
        expr,
        &Scope {
            inside: mode == "row",
            result_parameters: BTreeSet::new(),
            shadowed: false,
        },
    )
}
