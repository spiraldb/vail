//! A sizing policy for deferred nested scalars, independent of action contexts.
//!
//! This estimates broadcast pressure, not total query memory. A single row can
//! exceed the byte target. The caller must apply a selected cap to both source
//! batches and downstream execution operators through the same planned context.
use arrow_schema::DataType;
use datafusion_common::{
    Column, Result, ScalarValue,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_expr::{Expr, expr::ScalarFunction};
use datafusion_functions_nested::{array_avg::ArrayAvg, array_sum::ArraySum, length::ArrayLength};
use std::any::Any;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaterializationCost {
    /// No tracked large deferred value. This is not a claim of zero row size.
    None,
    Estimated {
        bytes_per_row: u64,
        // Largest List32 child count, not the sum across independent columns.
        list32_elements_per_row: u64,
    },
    /// A tracked large value reaches an unproven consumer or has unknown size.
    /// Ordinary untracked list columns must not default to this variant.
    Unknown,
}

impl MaterializationCost {
    /// A closed primitive repeat or range with a known result length. Count
    /// extraction/cast semantics remain lowering's responsibility. No values
    /// are generated or cast here; validity and List32 offsets are included.
    pub(crate) fn primitive_list(elements: u64, element_type: &DataType) -> Self {
        let Some(width) = element_type.primitive_width() else {
            return Self::Unknown;
        };
        let bytes = elements
            .checked_mul(width as u64)
            .and_then(|bytes| bytes.checked_add(elements.div_ceil(8)))
            .and_then(|bytes| bytes.checked_add(8));
        match bytes {
            Some(bytes_per_row) => Self::Estimated {
                bytes_per_row,
                list32_elements_per_row: elements,
            },
            None => Self::Unknown,
        }
    }

    /// Retained-buffer estimate for a primitive scalar list. Other nested
    /// layouts remain unknown rather than interpreting their offset domains.
    /// A slice may retain extra storage, but the List32 count is its selected
    /// value length, never the full backing buffer's length.
    pub(crate) fn nested_literal(value: &ScalarValue) -> Self {
        use arrow_array::Array;
        match value {
            ScalarValue::List(array)
                if array.len() == 1 && array.value_type().primitive_width().is_some() =>
            {
                Self::Estimated {
                    bytes_per_row: array.get_array_memory_size() as u64,
                    list32_elements_per_row: array.value_length(0) as u64,
                }
            }
            ScalarValue::LargeList(array)
                if array.len() == 1 && array.value_type().primitive_width().is_some() =>
            {
                Self::Estimated {
                    bytes_per_row: array.get_array_memory_size() as u64,
                    list32_elements_per_row: 0,
                }
            }
            _ => Self::Unknown,
        }
    }

    pub(crate) fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::None, cost) | (cost, Self::None) => cost,
            (
                Self::Estimated {
                    bytes_per_row: a,
                    list32_elements_per_row: x,
                },
                Self::Estimated {
                    bytes_per_row: b,
                    list32_elements_per_row: y,
                },
            ) => match a.checked_add(b) {
                Some(bytes_per_row) => Self::Estimated {
                    bytes_per_row,
                    list32_elements_per_row: x.max(y),
                },
                None => Self::Unknown,
            },
        }
    }

    pub(crate) fn row_limit(self, configured_rows: usize, target_bytes: u64) -> Result<usize> {
        let configured_rows = configured_rows.max(1);
        match self {
            Self::None => Ok(configured_rows),
            Self::Unknown => Ok(1),
            Self::Estimated {
                bytes_per_row,
                list32_elements_per_row,
            } => {
                if list32_elements_per_row > i32::MAX as u64 {
                    return datafusion_common::plan_err!(
                        "one array row requires {list32_elements_per_row} List32 child elements, exceeding {}",
                        i32::MAX
                    );
                }
                let byte_limit = target_bytes.checked_div(bytes_per_row).unwrap_or(u64::MAX);
                let offset_limit = (i32::MAX as u64)
                    .checked_div(list32_elements_per_row)
                    .unwrap_or(u64::MAX);
                Ok(usize::try_from(byte_limit.min(offset_limit))
                    .unwrap_or(usize::MAX)
                    .clamp(1, configured_rows))
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Summary {
    // A nested value still carried as ColumnarValue::Scalar: no broadcast yet.
    nested_scalar: MaterializationCost,
    // A consumer already required (or may require) a row-shaped nested input.
    materialized: MaterializationCost,
    scalar: bool,
}

/// Estimate one emitted expression, using typed kernel metadata supplied by
/// lowering. `deferred` must identify actual deferred kernels, never names.
/// `column_cost` supplies the caller's plan/schema lineage. A column is already
/// row-shaped, even when its producer started with a deferred scalar.
pub(crate) fn estimate_materialized_bytes_per_row(
    expression: &Expr,
    deferred: &impl Fn(&ScalarFunction) -> Option<MaterializationCost>,
    column_cost: &impl Fn(&Column) -> MaterializationCost,
) -> Result<MaterializationCost> {
    let result = summarize(expression, deferred, column_cost)?;
    Ok(result.materialized.combine(result.nested_scalar))
}

fn summarize(
    expression: &Expr,
    deferred: &impl Fn(&ScalarFunction) -> Option<MaterializationCost>,
    column_cost: &impl Fn(&Column) -> MaterializationCost,
) -> Result<Summary> {
    if let Expr::ScalarFunction(function) = expression {
        if let Some(cost) = deferred(function) {
            // Its dependency column prevents folding, but the kernel returns
            // a scalar regardless of that column's row-shaped representation.
            let mut inherited = MaterializationCost::None;
            for argument in &function.args {
                // Dependencies are evaluated before the scalar kernel is
                // invoked. Existing row-shaped inputs cannot be made free by
                // the kernel ignoring their contents.
                inherited =
                    inherited.combine(summarize(argument, deferred, column_cost)?.materialized);
            }
            return Ok(Summary {
                nested_scalar: cost,
                materialized: inherited,
                scalar: true,
            });
        }
    }
    if let Expr::Alias(alias) = expression {
        return summarize(&alias.expr, deferred, column_cost);
    }
    if let Expr::Column(column) = expression {
        return Ok(Summary {
            nested_scalar: MaterializationCost::None,
            materialized: column_cost(column),
            scalar: false,
        });
    }
    let mut nested = MaterializationCost::None;
    let mut materialized = MaterializationCost::None;
    let mut all_scalar = true;
    expression.apply_children(|child| {
        let child = summarize(child, deferred, column_cost)?;
        nested = nested.combine(child.nested_scalar);
        materialized = materialized.combine(child.materialized);
        all_scalar &= child.scalar;
        Ok(TreeNodeRecursion::Continue)
    })?;
    match expression {
        Expr::Literal(_, _) => Ok(Summary {
            nested_scalar: MaterializationCost::None,
            materialized,
            scalar: true,
        }),
        Expr::Cast(_) | Expr::TryCast(_) if all_scalar => {
            // Scalar casts do not broadcast. Their resulting nested width may
            // differ, so retain an unknown pending size until consumed/emitted.
            Ok(Summary {
                nested_scalar: if nested == MaterializationCost::None {
                    nested
                } else {
                    MaterializationCost::Unknown
                },
                materialized,
                scalar: true,
            })
        }
        Expr::ScalarFunction(function) if all_scalar && scalar_reducer(function) => Ok(Summary {
            nested_scalar: MaterializationCost::None,
            materialized,
            scalar: true,
        }),
        Expr::BinaryExpr(_) | Expr::Negative(_) | Expr::Not(_)
            if nested == MaterializationCost::None =>
        {
            Ok(Summary {
                nested_scalar: nested,
                materialized,
                scalar: all_scalar,
            })
        }
        _ => Ok(Summary {
            nested_scalar: MaterializationCost::None,
            materialized: if nested == MaterializationCost::None
                && materialized == MaterializationCost::None
            {
                MaterializationCost::None
            } else {
                MaterializationCost::Unknown
            },
            scalar: false,
        }),
    }
}

fn scalar_reducer(function: &ScalarFunction) -> bool {
    // These pinned DF kernels use make_scalar_function, preserving scalar
    // arguments. A row-shaped dimension/other argument defeats this exemption.
    let implementation = function.func.inner().as_ref() as &dyn Any;
    implementation.is::<ArrayLength>()
        || implementation.is::<ArraySum>()
        || implementation.is::<ArrayAvg>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::Column;
    use datafusion_expr::{Expr, lit};
    use datafusion_functions_nested::expr_fn::{array_length, array_repeat, array_sort, array_sum};
    use datafusion_functions_nested::repeat::ArrayRepeat;

    const CONFIGURED_ROWS: usize = 8192;
    const TARGET_BYTES: u64 = 64 * 1024;

    fn repeat(value: Expr, count: i64) -> Expr {
        array_repeat(value, lit(count))
    }

    // Lowering supplies metadata only for closed scalar-producing kernels.
    // A row-dependent function must not masquerade as a deferred scalar.
    fn deferred_repeat(function: &ScalarFunction) -> Option<MaterializationCost> {
        let implementation = function.func.inner().as_ref() as &dyn Any;
        if !implementation.is::<ArrayRepeat>() {
            return None;
        }
        if !function
            .args
            .iter()
            .all(|arg| matches!(arg, Expr::Literal(_, _)))
        {
            return None;
        }
        Some(MaterializationCost::Estimated {
            bytes_per_row: 65_540,
            list32_elements_per_row: 16_384,
        })
    }

    fn limit(expression: Expr) -> usize {
        estimate_materialized_bytes_per_row(&expression, &deferred_repeat, &|_| {
            MaterializationCost::None
        })
        .unwrap()
        .row_limit(CONFIGURED_ROWS, TARGET_BYTES)
        .unwrap()
    }

    fn limit_with_large_column(expression: Expr) -> usize {
        estimate_materialized_bytes_per_row(&expression, &deferred_repeat, &|_| {
            MaterializationCost::Estimated {
                bytes_per_row: 65_540,
                list32_elements_per_row: 16_384,
            }
        })
        .unwrap()
        .row_limit(CONFIGURED_ROWS, TARGET_BYTES)
        .unwrap()
    }

    #[test]
    fn repeat_broadcasts_but_scalar_reducers_do_not() {
        let large = repeat(lit(1_i32), 16_384);
        assert_eq!(limit(large.clone()), 1);
        assert_eq!(limit(array_length(large.clone())), CONFIGURED_ROWS);
        assert_eq!(
            limit(array_length(large) + Expr::Column(Column::new_unqualified("row"))),
            CONFIGURED_ROWS
        );
        assert_eq!(
            limit(array_sort(
                repeat(lit(1_i32), 16_384),
                lit(false),
                lit(false)
            )),
            1,
            "an unrecognized nested consumer must remain conservative"
        );
    }

    #[test]
    fn aliases_and_scalar_casts_keep_the_representation_boundary() {
        use arrow_schema::{DataType, Field};
        use std::sync::Arc;
        let large = repeat(lit(1_i32), 16_384).alias("large");
        assert_eq!(limit(large.clone()), 1);
        let cast = datafusion_expr::expr_fn::cast(
            large,
            DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
        );
        assert_eq!(limit(cast.clone()), 1);
        assert_eq!(limit(array_length(cast)), CONFIGURED_ROWS);
    }

    #[test]
    fn row_dependent_reducer_argument_requires_broadcast() {
        let expression = datafusion_functions_nested::length::array_length_udf().call(vec![
            repeat(lit(1_i32), 16_384),
            Expr::Column(Column::new_unqualified("dimension")),
        ]);
        assert_eq!(limit(expression), 1);
    }

    #[test]
    fn reducers_preserve_cost_of_already_materialized_columns() {
        let column = Expr::Column(Column::new_unqualified("large_array"));
        assert_eq!(limit_with_large_column(array_length(column.clone())), 1);
        assert_eq!(limit_with_large_column(array_sum(column)), 1);
        assert_eq!(
            limit(array_length(Expr::Column(Column::new_unqualified("small")))),
            CONFIGURED_ROWS
        );
    }

    #[test]
    fn costs_sum_bytes_maximize_list_offsets_and_handle_overflow() {
        assert_eq!(
            MaterializationCost::Estimated {
                bytes_per_row: 10,
                list32_elements_per_row: 3,
            }
            .combine(MaterializationCost::Estimated {
                bytes_per_row: 20,
                list32_elements_per_row: 5,
            }),
            MaterializationCost::Estimated {
                bytes_per_row: 30,
                list32_elements_per_row: 5,
            }
        );
        assert_eq!(
            MaterializationCost::Estimated {
                bytes_per_row: u64::MAX,
                list32_elements_per_row: 0,
            }
            .combine(MaterializationCost::Estimated {
                bytes_per_row: 1,
                list32_elements_per_row: 0,
            }),
            MaterializationCost::Unknown
        );
    }

    #[test]
    fn row_limit_respects_offsets_and_single_row_budget_overflow() {
        let zero_width = MaterializationCost::Estimated {
            bytes_per_row: 0,
            list32_elements_per_row: i32::MAX as u64,
        };
        assert_eq!(
            zero_width.row_limit(CONFIGURED_ROWS, TARGET_BYTES).unwrap(),
            1
        );
        let too_large_for_one = MaterializationCost::Estimated {
            bytes_per_row: TARGET_BYTES + 1,
            list32_elements_per_row: 0,
        };
        assert_eq!(
            too_large_for_one
                .row_limit(CONFIGURED_ROWS, TARGET_BYTES)
                .unwrap(),
            1
        );
    }
    #[test]
    fn primitive_list_estimates_reject_impossible_offsets_without_allocating_values() {
        let cost = MaterializationCost::primitive_list(100_000_000, &DataType::Int32);
        assert_eq!(cost.row_limit(CONFIGURED_ROWS, TARGET_BYTES).unwrap(), 1);
        let impossible = MaterializationCost::primitive_list(i32::MAX as u64 + 1, &DataType::Int32);
        assert!(impossible.row_limit(CONFIGURED_ROWS, TARGET_BYTES).is_err());
        assert_eq!(
            MaterializationCost::primitive_list(u64::MAX, &DataType::Float64),
            MaterializationCost::Unknown
        );
    }

    #[test]
    fn a_sliced_literal_counts_selected_elements_not_retained_backing_values() {
        use arrow_array::ListArray;
        use std::sync::Arc;
        let values = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
            Some((0..100).map(Some).collect::<Vec<_>>()),
            Some(vec![Some(7), None, Some(9)]),
        ]);
        let scalar = ScalarValue::List(Arc::new(values.slice(1, 1)));
        assert!(matches!(
            MaterializationCost::nested_literal(&scalar),
            MaterializationCost::Estimated {
                list32_elements_per_row: 3,
                ..
            }
        ));
    }
}
