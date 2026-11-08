//! Pull one index through a total, strict, null-propagating primitive map.
use crate::expressions::{BinaryOperator, Expr, PrimitiveType, Type, UnaryOperator};
use datafusion_common::ScalarValue;
use std::{collections::HashMap, sync::Arc};

pub(crate) fn primitive(dtype: &Type) -> bool {
    matches!(
        dtype,
        Type::Primitive(
            PrimitiveType::Int32
                | PrimitiveType::Int64
                | PrimitiveType::Float32
                | PrimitiveType::Float64
                | PrimitiveType::Bool
        )
    )
}
pub(crate) fn index(index: &Expr) -> bool {
    match index {
        Expr::RuntimeLiteral {
            value: ScalarValue::Int32(Some(index)),
        } => *index >= 0,
        Expr::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value,
        } => value
            .as_i64()
            .is_some_and(|i| i >= 0 && i <= i32::MAX as i64),
        _ => false,
    }
}
fn literal(dtype: &Type, value: &serde_json::Value) -> bool {
    if !primitive(dtype) {
        return false;
    }
    if value.is_null() {
        return true;
    }
    match dtype {
        Type::Primitive(PrimitiveType::Bool) => value.is_boolean(),
        Type::Primitive(PrimitiveType::Int32) => {
            value.as_i64().is_some_and(|i| i32::try_from(i).is_ok())
        }
        Type::Primitive(PrimitiveType::Int64) => value.as_i64().is_some(),
        _ => value.as_f64().is_some(),
    }
}

/// The caller certifies an array-backed canonical primitive source and the
/// original typed map/index result. Exactly one strict use retains indexing
/// demand without adding a binding or duplicating an expensive indexed read.
pub(crate) fn rewrite(
    input: Arc<Expr>,
    parameter: &str,
    body: &Arc<Expr>,
    index: Arc<Expr>,
    output: Type,
) -> Option<Expr> {
    if !self::index(&index) || !primitive(&output) {
        return None;
    }
    let replacement = Arc::new(Expr::ArrayIndex {
        value: input.clone(),
        index,
    });
    fn visit(
        expr: &Expr,
        parameter: &str,
        replacement: &Arc<Expr>,
        memo: &mut HashMap<usize, (u8, Arc<Expr>)>,
        remaining: &mut usize,
        depth: usize,
    ) -> Option<(u8, Arc<Expr>)> {
        let key = expr as *const Expr as usize;
        if let Some(result) = memo.get(&key) {
            return Some(result.clone());
        }
        if depth >= 64 || *remaining == 0 {
            return None;
        }
        *remaining -= 1;
        let result = match expr {
            Expr::LambdaVariable { name, .. } if name == parameter => (1, replacement.clone()),
            Expr::Literal { dtype, value } if literal(dtype, value) => (0, Arc::new(expr.clone())),
            Expr::RuntimeLiteral { value }
                if matches!(
                    value,
                    ScalarValue::Int32(_)
                        | ScalarValue::Int64(_)
                        | ScalarValue::Float32(_)
                        | ScalarValue::Float64(_)
                        | ScalarValue::Boolean(_)
                ) =>
            {
                (0, Arc::new(expr.clone()))
            }
            Expr::Binary {
                operator:
                    BinaryOperator::Add
                    | BinaryOperator::Sub
                    | BinaryOperator::Mul
                    | BinaryOperator::Eq
                    | BinaryOperator::Ne
                    | BinaryOperator::Lt
                    | BinaryOperator::Le
                    | BinaryOperator::Gt
                    | BinaryOperator::Ge,
                ..
            }
            | Expr::Unary {
                operator: UnaryOperator::Neg | UnaryOperator::Not,
                ..
            } => {
                let mut count = 0u8;
                let rebuilt = expr
                    .try_map_children_shared(|child, _| {
                        let (uses, child) =
                            visit(child, parameter, replacement, memo, remaining, depth + 1)
                                .ok_or(())?;
                        count = count.saturating_add(uses).min(2);
                        Ok::<_, ()>(child)
                    })
                    .ok()?;
                (count, Arc::new(rebuilt))
            }
            // Missingness tests, lazy branches, captures, casts, binders, errors and unknown
            // operators keep the original map, including all of its effects.
            _ => return None,
        };
        memo.insert(key, result.clone());
        Some(result)
    }
    let (uses, body) = visit(
        body,
        parameter,
        &replacement,
        &mut HashMap::new(),
        &mut 64,
        0,
    )?;
    if uses != 1 {
        return None;
    }
    // Every admitted operation propagates the indexed null. Missing source
    // lists therefore need no CASE, selected-row gather or additional binding.
    Some(body.as_ref().clone())
}
