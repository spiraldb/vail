//! Effects constrain optimizer evaluation and movement independently of identity.
//! Seeded RNG is deterministic only within its row/partition/split context. A
//! deterministic operation that may fail also must not be hoisted across filters
//! or evaluated in an unselected conditional branch.
use crate::expressions::{BinaryOperator, Expr};
use datafusion_expr::Volatility;

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
        | "replace" | "string_to_array" | "array_to_string" => Effects::Pure,
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
pub(crate) fn effects(expr: &Expr) -> Effects {
    let mut effect = Effects::Pure;
    expr.visit_unique(&mut |node| {
        let own = match node {
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
            Expr::Binary { operator, .. } | Expr::IntegerBinary { operator, .. } => {
                match operator {
                    BinaryOperator::Div
                    | BinaryOperator::Floordiv
                    | BinaryOperator::Mod
                    | BinaryOperator::Pow => Effects::MayError,
                    _ => Effects::Pure,
                }
            }
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
        };
        effect = effect.max(own);
    });
    effect
}
