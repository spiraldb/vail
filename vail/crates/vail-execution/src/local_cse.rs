//! Lexically local common expressions in strict struct projections. The
//! existing Switch-with-no-cases binder evaluates an intermediate once and
//! exposes its Arrow result to the rest of this invocation. No runtime cache,
//! metadata expansion, or process-global evaluation state is introduced.
use crate::expressions::{BinaryOperator, Expr, ExprError};
use datafusion_common::ScalarValue;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

fn address(node: &Expr) -> usize {
    node as *const Expr as usize
}

// Only traverse eagerly evaluated expressions. In particular a conditional,
// switch, or nested lambda is opaque to this binding scope.
fn strict(node: &Expr) -> bool {
    match node {
        Expr::Struct { .. }
        | Expr::Tuple { .. }
        | Expr::GetField { .. }
        | Expr::Cast { .. }
        | Expr::Unary { .. }
        | Expr::IsMissing { .. }
        | Expr::ArrayLength { .. } => true,
        Expr::Binary { operator, .. } | Expr::IntegerBinary { operator, .. } => {
            !matches!(operator, BinaryOperator::And | BinaryOperator::Or)
        }
        _ => false,
    }
}

fn nonzero_small_integer(node: &Expr) -> bool {
    match node {
        Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some(value)),
        } => *value > 0,
        Expr::RuntimeLiteral {
            value: ScalarValue::Int64(Some(value)),
        } => *value > 0,
        _ => false,
    }
}

// Conservatively retain value-domain/error and random-context barriers.
// Positive integer literal divisors cannot introduce divide-by-zero or the
// signed MIN/-1 overflow. Their operands must themselves be safe to move.
fn safe_node(node: &Expr) -> bool {
    match node {
        Expr::Literal { .. }
        | Expr::RuntimeLiteral { .. }
        | Expr::Column { .. }
        | Expr::Global { .. }
        | Expr::ExternalGlobal { .. }
        | Expr::LambdaVariable { .. }
        | Expr::AlignedMapInput { .. }
        | Expr::GetField { .. }
        | Expr::ArrayLength { .. }
        | Expr::IsMissing { .. }
        | Expr::Unary { .. }
        | Expr::Struct { .. }
        | Expr::Tuple { .. } => true,
        Expr::Binary {
            operator, right, ..
        }
        | Expr::IntegerBinary {
            operator, right, ..
        } => match operator {
            BinaryOperator::Div | BinaryOperator::Floordiv | BinaryOperator::Mod => {
                nonzero_small_integer(right)
            }
            BinaryOperator::Pow => false,
            _ => true,
        },
        _ => false,
    }
}

pub(crate) fn share_struct(root: &Expr) -> Result<Option<Expr>, ExprError> {
    if !matches!(root, Expr::Struct { .. }) {
        return Ok(None);
    }
    let ids = crate::expression_identity::canonical_nodes(root);
    let mut nodes = HashMap::new();
    let mut counts = HashMap::<usize, usize>::new();
    let mut safe = HashMap::new();
    let mut order = Vec::new();
    fn visit<'a>(
        node: &'a Expr,
        ids: &HashMap<usize, usize>,
        nodes: &mut HashMap<usize, &'a Expr>,
        counts: &mut HashMap<usize, usize>,
        safe: &mut HashMap<usize, bool>,
        order: &mut Vec<usize>,
    ) {
        let id = ids[&address(node)];
        *counts.entry(id).or_default() += 1;
        if nodes.contains_key(&id) {
            return;
        }
        nodes.insert(id, node);
        let mut pure = safe_node(node);
        if strict(node) {
            node.for_each_child(&mut |child| {
                visit(child, ids, nodes, counts, safe, order);
                pure &= safe[&ids[&address(child)]];
            });
        } else if !matches!(
            node,
            Expr::Literal { .. }
                | Expr::RuntimeLiteral { .. }
                | Expr::Column { .. }
                | Expr::Global { .. }
                | Expr::ExternalGlobal { .. }
                | Expr::LambdaVariable { .. }
                | Expr::AlignedMapInput { .. }
        ) {
            pure = false;
        }
        safe.insert(id, pure);
        order.push(id);
    }
    visit(root, &ids, &mut nodes, &mut counts, &mut safe, &mut order);
    let candidates = order
        .into_iter()
        .filter(|id| {
            counts[id] > 1
                && safe[id]
                && matches!(
                    nodes[id],
                    Expr::Binary { .. }
                        | Expr::IntegerBinary { .. }
                        | Expr::Unary { .. }
                        | Expr::ArrayLength { .. }
                        | Expr::IsMissing { .. }
                )
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut used = HashSet::new();
    root.visit_unique(&mut |node| {
        if let Expr::LambdaVariable { name, .. } | Expr::Column { name } = node {
            used.insert(name.clone());
        }
        let mut index = 0;
        node.for_each_child(&mut |_| {
            used.extend(node.child_binders(index).into_iter().map(str::to_owned));
            index += 1;
        });
    });
    fn replace(
        node: &Expr,
        ids: &HashMap<usize, usize>,
        bound: &HashMap<usize, Expr>,
        memo: &mut HashMap<usize, Expr>,
    ) -> Result<Expr, ExprError> {
        let at = address(node);
        if let Some(value) = bound.get(&ids[&at]) {
            return Ok(value.clone());
        }
        if let Some(value) = memo.get(&at) {
            return Ok(value.clone());
        }
        let result = if strict(node) {
            node.try_map_original_children(|child| replace(child, ids, bound, memo))?
        } else {
            node.clone()
        };
        memo.insert(at, result.clone());
        Ok(result)
    }
    let mut bound = HashMap::new();
    let mut bindings = Vec::new();
    for id in candidates {
        let mut name = format!("__vail_local_cse_{}", bindings.len());
        while used.contains(&name) {
            name.push('_');
        }
        used.insert(name.clone());
        let argument = replace(nodes[&id], &ids, &bound, &mut HashMap::new())?;
        bound.insert(
            id,
            Expr::LambdaVariable {
                name: name.clone(),
                dtype: None,
            },
        );
        bindings.push((name, argument));
    }
    let mut result = replace(root, &ids, &bound, &mut HashMap::new())?;
    for (parameter, value) in bindings.into_iter().rev() {
        result = Expr::Switch {
            value: Arc::new(value),
            parameter,
            cases: vec![],
            default: Arc::new(result),
            missing: None,
        };
    }
    Ok(Some(result))
}
