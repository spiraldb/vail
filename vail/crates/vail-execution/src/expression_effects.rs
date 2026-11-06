//! Effects constrain optimizer evaluation and movement independently of identity.
//! Seeded RNG is deterministic only within its row/partition/split context. A
//! deterministic operation that may fail also must not be hoisted across filters
//! or evaluated in an unselected conditional branch.
use crate::expressions::{ArrayReduction, BinaryOperator, Expr, PrimitiveType, Type};
use datafusion_common::ScalarValue;
use datafusion_expr::Volatility;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Effects {
    Pure,
    MayError,
    RandomContext,
}
impl Effects {
    pub(crate) fn volatility(self) -> Volatility {
        match self {
            Self::Pure => Volatility::Immutable,
            // DataFusion currently exposes no separate error-movement effect.
            Self::MayError | Self::RandomContext => Volatility::Volatile,
        }
    }
}
/// Registry entries describe value-domain failures after argument types have
/// been validated. New/unknown function names retain the error-order barrier.
fn string_effects(function: &str) -> Effects {
    match function {
        "lower" | "upper" | "reverse" | "btrim" | "concat" | "contains" | "starts_with"
        | "ends_with" | "character_length" | "hamming" | "entropy" | "strpos" | "translate"
        | "replace" | "string_to_array" | "array_to_string" | "gq_from_pl" | "downcode" => {
            Effects::Pure
        }
        _ => Effects::MayError,
    }
}
fn nested_effects(function: &str) -> Effects {
    if function.starts_with("random_") {
        return Effects::RandomContext;
    }
    match function {
        "isnan"
        | "array_distinct"
        | "array_union"
        | "array_intersect"
        | "array_except"
        | "array_concat"
        | "arrays_zip"
        | "array_compact"
        | "flatten"
        | "array_sum"
        | "array_product"
        | "array_min"
        | "array_max"
        | "__vail_list_reduce_collect"
        | "__vail_list_reduce_collect_as_set"
        | "__vail_list_reduce_sum"
        | "__vail_list_reduce_product"
        | "__vail_list_reduce_min"
        | "__vail_list_reduce_max"
        | "__vail_list_reduce_mean"
        | "__vail_list_reduce_stddev_pop"
        | "__vail_list_reduce_count_where"
        | "__vail_list_reduce_fraction"
        | "__vail_list_reduce_any"
        | "__vail_list_reduce_all" => Effects::Pure,
        _ => Effects::MayError,
    }
}
pub(crate) fn own_effect(node: &Expr) -> Effects {
    match node {
        Expr::NestedFunction { function, .. } => nested_effects(function),
        Expr::StringFunction { function, .. } => string_effects(function),
        // This virtual Call field rejects non-diploid values. A plain
        // struct field using the reserved name is conservatively covered.
        Expr::GetField { name, .. } if name == "__vail_call_unphased_diploid_gt_index" => {
            Effects::MayError
        }
        Expr::RuntimeLiteral { .. }
        | Expr::Literal { .. }
        | Expr::Column { .. }
        | Expr::Global { .. }
        | Expr::ExternalGlobal { .. }
        | Expr::LambdaVariable { .. }
        | Expr::MatrixField { .. }
        | Expr::MatrixScope { .. }
        | Expr::MatrixNumCols { .. }
        | Expr::MatrixColumnPosition { .. }
        | Expr::Struct { .. }
        | Expr::Tuple { .. }
        | Expr::GetField { .. }
        | Expr::Array { .. }
        | Expr::ArrayMap { .. }
        | Expr::AlignedMatrixMap { .. }
        | Expr::AlignedMapInput { .. }
        | Expr::ArrayFilter { .. }
        | Expr::ArrayPredicate { .. }
        | Expr::ArrayFold { .. }
        | Expr::ArrayLength { .. }
        | Expr::ArrayFlatten { .. }
        | Expr::CollectionCast { .. }
        | Expr::Unary { .. }
        | Expr::IsMissing { .. }
        | Expr::Switch { .. }
        | Expr::IfElse { .. }
        | Expr::ToString { .. }
        | Expr::Interval { .. }
        | Expr::IntervalContains { .. }
        | Expr::IntervalOverlaps { .. }
        | Expr::LocusValid { .. } => Effects::Pure,
        Expr::LocusOp { .. } => Effects::MayError,
        Expr::Binary { operator, .. } | Expr::IntegerBinary { operator, .. } => match operator {
            BinaryOperator::Div
            | BinaryOperator::Floordiv
            | BinaryOperator::Mod
            | BinaryOperator::Pow => Effects::MayError,
            _ => Effects::Pure,
        },
        // These kernels validate runtime values (bounds, shapes, parsing,
        // reducer options). Conservatively retain the error-order barrier.
        Expr::Die { .. }
        | Expr::Cast { .. }
        | Expr::ArrayIndex { .. }
        | Expr::ArraySlice { .. }
        | Expr::ArrayReduce { .. }
        | Expr::ArrayGrouped { .. }
        | Expr::StreamRange { .. }
        | Expr::Locus { .. }
        | Expr::LocusFromGlobalPosition { .. }
        | Expr::ParseLocus { .. }
        | Expr::ParseCsvLine { .. }
        | Expr::ArrayAggregateScan { .. }
        | Expr::Aggregate { .. }
        | Expr::AggregateFold { .. }
        | Expr::AggGroupBy { .. }
        | Expr::AggArray { .. }
        | Expr::AggFilter { .. }
        | Expr::AggExplode { .. }
        | Expr::TableLookup { .. } => Effects::MayError,
    }
}

pub(crate) fn effects(expr: &Expr) -> Effects {
    let mut effect = Effects::Pure;
    expr.visit_unique(&mut |node| effect = effect.max(own_effect(node)));
    effect
}

// Value-validation errors may be skipped on rows eliminated by a filter.
// Explicit errors, randomness and unknown functions retain their barrier.
fn registered_validation(node: &Expr, contracts: &mut HashMap<String, bool>) -> bool {
    let Expr::StringFunction { function, .. } = node else {
        return false;
    };
    *contracts.entry(function.clone()).or_insert_with(|| {
        crate::expressions::string_function(function)
            .is_ok_and(|(kernel, _, _)| crate::variant_kernels::validation_only(&kernel))
    })
}

pub(crate) fn can_defer_filtered_values(expr: &Expr) -> bool {
    let mut contracts = HashMap::new();
    let mut allowed = true;
    expr.visit_unique(&mut |node| {
        allowed &= own_effect(node) == Effects::Pure || registered_validation(node, &mut contracts);
    });
    allowed
}

fn literal_option(expression: &Expr, choices: &[&str]) -> bool {
    let value = match expression {
        Expr::Literal { value, .. } => value.as_str(),
        Expr::RuntimeLiteral {
            value:
                ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value),
        } => value.as_deref(),
        _ => None,
    };
    value.is_some_and(|value| choices.contains(&value))
}

// The reviewed selected-callback validation waiver. This remains independent
// from granting a native kernel permission to defer its own input validation.
fn selected_validation_node(node: &Expr) -> bool {
    own_effect(node) == Effects::Pure
        || match node {
            Expr::ArrayIndex { .. } => true,
            Expr::Cast {
                dtype: Type::Primitive(PrimitiveType::Call),
                ..
            } => true,
            Expr::Cast {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value,
            } => matches!(
                value.as_ref(),
                Expr::Binary {
                    operator: BinaryOperator::Eq
                        | BinaryOperator::Ne
                        | BinaryOperator::Lt
                        | BinaryOperator::Le
                        | BinaryOperator::Gt
                        | BinaryOperator::Ge,
                    ..
                }
            ),
            Expr::GetField { name, .. } => name == "__vail_call_unphased_diploid_gt_index",
            Expr::NestedFunction { function, args, .. } if function == "array_sort" => {
                matches!(args.as_slice(),[_,direction,nulls] if literal_option(direction,&["ASC","DESC"])&&literal_option(nulls,&["NULLS FIRST","NULLS LAST"]))
            }
            _ => false,
        }
}

pub(crate) fn can_defer_selected_body(expression: &Expr) -> bool {
    let mut allowed = true;
    expression.visit_unique(&mut |node| allowed &= selected_validation_node(node));
    allowed
}

/// Own-node query proof: callers share the registered-contract memo while
/// separately retaining expression identities across the complete action.
pub(crate) fn can_prefetch_node(node: &Expr, contracts: &mut HashMap<String, bool>) -> bool {
    selected_validation_node(node) || registered_validation(node, contracts)
}
pub(crate) fn can_prefetch_values(expression: &Expr) -> bool {
    let mut contracts = HashMap::new();
    let mut allowed = true;
    expression.visit_unique(&mut |node| allowed &= can_prefetch_node(node, &mut contracts));
    allowed
}

/// Block fusion preserves callback demand. Checked indexing and casts stay on
/// demanded lanes; unknown functions, explicit errors and RNG retain barriers.
pub(crate) fn can_block_list_callback(expr: &Expr) -> bool {
    let mut allowed = true;
    expr.visit_unique(&mut |node| {
        allowed &= can_block_list_node(node);
    });
    allowed
}

/// Whether changing this node's batch boundary preserves its effects contract.
pub(crate) fn can_block_list_node(node: &Expr) -> bool {
    own_effect(node) == Effects::Pure
        || matches!(node, Expr::ArrayIndex { .. } | Expr::Cast { .. })
        || matches!(node,Expr::NestedFunction{function,..} if function=="array_sort")
        // These constructed lists have a validated numeric element type.
        // Keep each list's ordinary fold and missing-value rules unchanged.
        || matches!(node, Expr::ArrayReduce {
            operation: ArrayReduction::Sum | ArrayReduction::Product | ArrayReduction::Min | ArrayReduction::Max,
            value, ..
        } if matches!(value.as_ref(), Expr::Array {
            element_type: Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64 | PrimitiveType::Float32 | PrimitiveType::Float64), elements
        } if elements.len() <= 8))
}
