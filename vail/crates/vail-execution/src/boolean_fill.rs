//! A Boolean null fallback is a truth test, not a selected-row computation.
use crate::{
    expression_effects::{Effects, own_effect},
    expressions::{Expr, UnaryOperator},
};
use datafusion_common::ScalarValue;
use std::{collections::HashMap, sync::Arc};

pub(crate) fn constant(expression: &Expr) -> Option<bool> {
    match expression {
        Expr::Literal { value, .. } => value.as_bool(),
        Expr::RuntimeLiteral {
            value: ScalarValue::Boolean(value),
        } => *value,
        _ => None,
    }
}

/// Match the same native value on both sides of its missingness test.
/// Purity is checked separately before removing its second evaluation.
pub(crate) fn candidate(expression: &Expr) -> Option<(&Arc<Expr>, bool)> {
    let Expr::IfElse {
        condition,
        consequent,
        alternate,
    } = expression
    else {
        return None;
    };
    let (test, defined) = match condition.as_ref() {
        Expr::Unary {
            operator: UnaryOperator::Not,
            value,
        } => (value, true),
        _ => (condition, false),
    };
    let Expr::IsMissing { value } = test.as_ref() else {
        return None;
    };
    let (returned, fallback) = if defined {
        (consequent, alternate)
    } else {
        (alternate, consequent)
    };
    (Arc::ptr_eq(value, returned)).then_some((value, constant(fallback)?))
}

pub(crate) fn pure(expression: &Expr, memo: &mut HashMap<usize, bool>) -> bool {
    let key = expression as *const Expr as usize;
    if let Some(&value) = memo.get(&key) {
        return value;
    }
    let mut value = own_effect(expression) == Effects::Pure;
    if value {
        expression.for_each_child(&mut |child| value &= pure(child, memo));
    }
    memo.insert(key, value);
    value
}
