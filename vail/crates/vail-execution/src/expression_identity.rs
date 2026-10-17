//! Structural identity over native expression DAGs. Each hash visits a shared
//! node once; equality memoizes node pairs and compares every actual field.
//! Digests accelerate lookup only. No text encoding or global memo is involved.
use crate::{
    expressions::{ArrayReduction, BinaryOperator, Expr, Type, UnaryOperator},
    reference::ReferenceDefinition,
};
use datafusion_common::ScalarValue;
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    mem::Discriminant,
    sync::Arc,
};

#[derive(PartialEq, Eq, Hash)]
enum Properties<'a> {
    None,
    Text(&'a str),
    Texts(Vec<&'a str>),
    Type(&'a Type),
    TextType(&'a str, &'a Type),
    OptionalTextType(&'a str, &'a Option<Type>),
    MatrixField(&'a str, &'a str, &'a Option<Type>),
    Lookup(&'a str, bool, bool, &'a Option<Type>),
    Scalar(&'a ScalarValue),
    Literal(&'a Type, &'a serde_json::Value),
    Aggregate(
        &'a str,
        bool,
        &'a serde_json::Value,
        &'a Option<Type>,
        Vec<&'a str>,
    ),
    ThreeTexts(&'a str, &'a str, &'a str),
    Fold(&'a str, &'a str, bool),
    Scan(&'a str, &'a serde_json::Value, &'a Type),
    Predicate(&'a str, bool),
    Switch(&'a str, bool),
    Binary(BinaryOperator),
    Unary(UnaryOperator),
    Slice(bool, bool, bool),
    Reduce(ArrayReduction, bool),
    Reference(&'a ReferenceDefinition),
    Csv(&'a str, &'a Option<String>, &'a [String], usize),
}
fn properties(expr: &Expr) -> (Discriminant<Expr>, Properties<'_>) {
    let properties = match expr {
        Expr::MatrixField { axis, name, dtype } => Properties::MatrixField(axis, name, dtype),
        Expr::MatrixScope { axis, .. } => Properties::Text(axis),
        Expr::MatrixNumCols { .. } | Expr::MatrixColumnPosition { .. } => Properties::None,
        Expr::TableLookup {
            reference,
            all_matches,
            query_interval,
            dtype,
            ..
        } => Properties::Lookup(reference, *all_matches, *query_interval, dtype),
        Expr::RuntimeLiteral { value } => Properties::Scalar(value),
        Expr::Literal { dtype, value } => Properties::Literal(dtype, value),
        Expr::Aggregate {
            function,
            value,
            options,
            result_type,
            init,
        } => Properties::Aggregate(
            function,
            value.is_some(),
            options,
            result_type,
            init.iter().map(|(name, _)| name.as_str()).collect(),
        ),
        Expr::AggregateFold {
            state_parameter,
            left_parameter,
            right_parameter,
            ..
        } => Properties::ThreeTexts(state_parameter, left_parameter, right_parameter),
        Expr::AggGroupBy { .. } | Expr::AggFilter { .. } => Properties::None,
        Expr::AggArray { parameter, .. } | Expr::AggExplode { parameter, .. } => {
            Properties::Text(parameter)
        }
        Expr::LambdaVariable { name, dtype } => Properties::OptionalTextType(name, dtype),
        Expr::NestedFunction {
            function, dtype, ..
        } => Properties::TextType(function, dtype),
        Expr::StringFunction { function, .. } => Properties::Text(function),
        Expr::CollectionCast { dtype, .. } | Expr::Cast { dtype, .. } | Expr::Die { dtype, .. } => {
            Properties::Type(dtype)
        }
        Expr::IntegerBinary { operator, .. } | Expr::Binary { operator, .. } => {
            Properties::Binary(*operator)
        }
        Expr::Tuple { .. } => Properties::None,
        Expr::ArrayMap { parameter, .. } | Expr::ArrayFilter { parameter, .. } => {
            Properties::Text(parameter)
        }
        Expr::ArrayAggregateScan {
            function,
            options,
            dtype,
            ..
        } => Properties::Scan(function, options, dtype),
        Expr::ArrayFold {
            accumulator,
            parameter,
            scan,
            ..
        } => Properties::Fold(accumulator, parameter, *scan),
        Expr::ArrayPredicate {
            parameter, first, ..
        } => Properties::Predicate(parameter, *first),
        Expr::StreamRange { .. } | Expr::ArrayGrouped { .. } => Properties::None,
        Expr::ExternalGlobal { reference, dtype } => Properties::TextType(reference, dtype),
        Expr::Global { name } | Expr::Column { name } => Properties::Text(name),
        Expr::Switch {
            parameter, missing, ..
        } => Properties::Switch(parameter, missing.is_some()),
        Expr::Unary { operator, .. } => Properties::Unary(*operator),
        Expr::ToString { .. } | Expr::IsMissing { .. } | Expr::IfElse { .. } => Properties::None,
        Expr::GetField { name, .. } => Properties::Text(name),
        Expr::Interval { .. } | Expr::IntervalContains { .. } | Expr::IntervalOverlaps { .. } => {
            Properties::None
        }
        Expr::Struct { fields } => {
            Properties::Texts(fields.iter().map(|(name, _)| name.as_str()).collect())
        }
        Expr::Array { element_type, .. } => Properties::Type(element_type),
        Expr::ArrayFlatten { .. } | Expr::ArrayLength { .. } | Expr::ArrayIndex { .. } => {
            Properties::None
        }
        Expr::ArraySlice {
            start, stop, step, ..
        } => Properties::Slice(start.is_some(), stop.is_some(), step.is_some()),
        Expr::ArrayReduce {
            operation,
            filter_missing,
            ..
        } => Properties::Reduce(*operation, *filter_missing),
        Expr::Locus { reference, .. }
        | Expr::LocusValid { reference, .. }
        | Expr::LocusFromGlobalPosition { reference, .. }
        | Expr::ParseLocus { reference, .. } => Properties::Reference(reference),
        Expr::LocusOp { operation, .. } => Properties::Text(operation),
        Expr::ParseCsvLine {
            delimiter,
            quote,
            missing,
            expected_fields,
            ..
        } => Properties::Csv(delimiter, quote, missing, *expected_fields),
    };
    (std::mem::discriminant(expr), properties)
}

fn fingerprint(expr: &Expr, memo: &mut HashMap<usize, u64>) -> u64 {
    let address = expr as *const Expr as usize;
    if let Some(value) = memo.get(&address) {
        return *value;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    properties(expr).hash(&mut hasher);
    let mut children = 0_usize;
    expr.for_each_child(&mut |child| {
        fingerprint(child, memo).hash(&mut hasher);
        children += 1;
    });
    children.hash(&mut hasher);
    let value = hasher.finish();
    memo.insert(address, value);
    value
}
fn equal(left: &Expr, right: &Expr, memo: &mut HashSet<(usize, usize)>) -> bool {
    if std::ptr::eq(left, right) {
        return true;
    }
    let addresses = (left as *const Expr as usize, right as *const Expr as usize);
    if !memo.insert(addresses) {
        return true;
    }
    if properties(left) != properties(right) {
        return false;
    }
    let mut left_children = Vec::new();
    let mut right_children = Vec::new();
    left.for_each_child(&mut |child| left_children.push(child));
    right.for_each_child(&mut |child| right_children.push(child));
    left_children.len() == right_children.len()
        && left_children
            .into_iter()
            .zip(right_children)
            .all(|(a, b)| equal(a, b, memo))
}
impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other, &mut HashSet::new())
    }
}
impl Eq for Expr {}
impl Hash for Expr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        fingerprint(self, &mut HashMap::new()).hash(state);
    }
}

/// Cache a root fingerprint for repeated optimizer hash-table probes. Retain its
/// native DAG for exact, collision-safe equality; ownership stays query-local.
#[derive(Clone)]
pub(crate) struct Identity {
    expression: Arc<Expr>,
    fingerprint: u64,
    nodes: usize,
}
impl Identity {
    pub(crate) fn new(expression: Expr) -> Self {
        let mut memo = HashMap::new();
        let fingerprint = fingerprint(&expression, &mut memo);
        Self {
            expression: Arc::new(expression),
            fingerprint,
            nodes: memo.len(),
        }
    }

    pub(crate) fn expression(&self) -> &Expr {
        &self.expression
    }
}
impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExpressionIdentity")
            .field("fingerprint", &format_args!("{:016x}", self.fingerprint))
            .field("nodes", &self.nodes)
            .finish()
    }
}
impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint && self.expression == other.expression
    }
}
impl Eq for Identity {}
impl Hash for Identity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}
