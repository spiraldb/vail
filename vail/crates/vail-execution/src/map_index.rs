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
    fn certify(
        expr: &Expr,
        parameter: &str,
        memo: &mut HashMap<usize, u8>,
        remaining: &mut usize,
        depth: usize,
    ) -> Option<u8> {
        let key = expr as *const Expr as usize;
        if let Some(result) = memo.get(&key) {
            return Some(*result);
        }
        if depth >= 64 || *remaining == 0 {
            return None;
        }
        *remaining -= 1;
        let result = match expr {
            Expr::LambdaVariable { name, .. } if name == parameter => 1,
            Expr::Literal { dtype, value } if literal(dtype, value) => 0,
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
                0
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
                let mut count = Some(0u8);
                expr.for_each_child(&mut |child| {
                    count = count.and_then(|old| {
                        if old == 2 {
                            return Some(2);
                        }
                        Some(
                            old.saturating_add(certify(
                                child,
                                parameter,
                                memo,
                                remaining,
                                depth + 1,
                            )?)
                            .min(2),
                        )
                    });
                });
                count?
            }
            // Missingness tests, lazy branches, captures, casts, binders, errors and unknown
            // operators keep the original map, including all of its effects.
            _ => return None,
        };
        memo.insert(key, result);
        Some(result)
    }
    let mut uses = HashMap::new();
    if certify(body, parameter, &mut uses, &mut 64, 0)? != 1 {
        return None;
    }
    // Declined maps have not allocated a replacement expression. A successful
    // proof has exactly one parameter path; every other subtree keeps its Arc.
    let replacement = Arc::new(Expr::ArrayIndex {
        value: input,
        index,
    });
    fn substitute(
        expr: &Arc<Expr>,
        uses: &HashMap<usize, u8>,
        replacement: &Arc<Expr>,
    ) -> Option<Arc<Expr>> {
        if *uses.get(&(Arc::as_ptr(expr) as usize))? == 0 {
            return Some(expr.clone());
        }
        Some(match expr.as_ref() {
            Expr::LambdaVariable { .. } => replacement.clone(),
            Expr::Binary {
                operator,
                left,
                right,
            } => Arc::new(Expr::Binary {
                operator: *operator,
                left: substitute(left, uses, replacement)?,
                right: substitute(right, uses, replacement)?,
            }),
            Expr::Unary { operator, value } => Arc::new(Expr::Unary {
                operator: *operator,
                value: substitute(value, uses, replacement)?,
            }),
            _ => return None,
        })
    }
    let body = substitute(body, &uses, &replacement)?;
    // Every admitted operation propagates the indexed null. Missing source
    // lists therefore need no CASE, selected-row gather or additional binding.
    Some(body.as_ref().clone())
}
