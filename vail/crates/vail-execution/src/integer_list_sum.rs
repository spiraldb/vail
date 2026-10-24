//! Fuse a pure integer field projection into a segmented sum. No mapped or
//! widened child vector is allocated; ordinary expression lowering is fallback.
use super::*;
use arrow_buffer::{NullBuffer, bit_chunk_iterator::BitChunks};

fn path(expr: &Expr, parameter: &str) -> Option<Vec<String>> {
    match expr {
        Expr::LambdaVariable { name, .. } if name == parameter => Some(vec![]),
        Expr::GetField { value, name } => {
            let mut p = path(value, parameter)?;
            p.push(name.clone());
            Some(p)
        }
        _ => None,
    }
}
fn field_type<'a>(mut ty: &'a Type, path: &[String]) -> Option<&'a Type> {
    for name in path {
        let Type::Struct { r#struct: fields } = ty else {
            return None;
        };
        ty = &fields.iter().find(|(n, _)| n == name)?.1;
    }
    Some(ty)
}
fn projected(expr: &Expr, parameter: &str, element: &Type) -> Option<(Vec<String>, Type)> {
    if let Expr::Cast { dtype, value } = expr {
        let (p, input) = projected(value, parameter, element)?;
        if !matches!(
            (&input, dtype),
            (
                Type::Primitive(PrimitiveType::Int32),
                Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64)
            ) | (
                Type::Primitive(PrimitiveType::Int64),
                Type::Primitive(PrimitiveType::Int64)
            )
        ) {
            return None;
        }
        return Some((p, dtype.clone()));
    }
    let p = path(expr, parameter)?;
    let ty = field_type(element, &p)?.clone();
    matches!(
        ty,
        Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64)
    )
    .then_some((p, ty))
}

pub(super) fn projection(
    body: &Expr,
    parameter: &str,
    element: &Type,
    output: &DataType,
) -> Option<Vec<String>> {
    // A missing ancestor guard is redundant with field validity propagation.
    // Accept it only when its missing branch is an actual null literal.
    let (body, guard) = if let Expr::IfElse {
        condition,
        consequent,
        alternate,
    } = body
    {
        let missing = match alternate.as_ref() {
            Expr::Literal { value, .. } => value.is_null(),
            Expr::RuntimeLiteral { value } => value.is_null(),
            _ => false,
        };
        let Expr::Unary {
            operator: UnaryOperator::Not,
            value,
        } = condition.as_ref()
        else {
            return None;
        };
        let Expr::IsMissing { value } = value.as_ref() else {
            return None;
        };
        if !missing {
            return None;
        }
        (consequent.as_ref(), Some(path(value, parameter)?))
    } else {
        (body, None)
    };
    let (p, ty) = projected(body, parameter, element)?;
    if ty.to_arrow().ok()?.ne(output) || guard.is_some_and(|g| !p.starts_with(&g)) {
        return None;
    }
    Some(p)
}

type Evaluate = fn(&ArrayRef, &[usize]) -> Result<ArrayRef, ExprError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Kernel {
    Int32(bool),
    Int32ToInt64(bool),
    Int64(bool),
}

/// Typed kernel and field offsets are selected during expression lowering.
#[derive(Debug, Clone)]
pub(super) struct Prepared {
    path: Vec<usize>,
    identity: Kernel,
    evaluate: Evaluate,
}

// Function addresses are not semantic expression identities. The kernel tag
// and field positions uniquely determine the selected evaluator.
impl PartialEq for Prepared {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.identity == other.identity
    }
}
impl Eq for Prepared {}
impl std::hash::Hash for Prepared {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.path, state);
        std::hash::Hash::hash(&self.identity, state);
    }
}

impl Prepared {
    pub(super) fn new(
        element: &Type,
        names: Vec<String>,
        filter_missing: bool,
        output: &DataType,
    ) -> Result<Self, ExprError> {
        let mut leaf = element;
        let mut path = Vec::with_capacity(names.len());
        for name in names {
            let Type::Struct { r#struct: fields } = leaf else {
                return Err(ExprError::Invalid(
                    "integer list sum requires struct path".into(),
                ));
            };
            let index = fields
                .iter()
                .position(|(n, _)| n == &name)
                .ok_or_else(|| ExprError::Invalid("integer list sum field absent".into()))?;
            path.push(index);
            leaf = &fields[index].1;
        }
        let (identity, evaluate): (_, Evaluate) = match (leaf, output, filter_missing) {
            (Type::Primitive(PrimitiveType::Int32), DataType::Int32, true) => {
                (Kernel::Int32(true), evaluate_same::<Int32Type, true>)
            }
            (Type::Primitive(PrimitiveType::Int32), DataType::Int32, false) => {
                (Kernel::Int32(false), evaluate_same::<Int32Type, false>)
            }
            (Type::Primitive(PrimitiveType::Int32), DataType::Int64, true) => {
                (Kernel::Int32ToInt64(true), evaluate_wide::<true>)
            }
            (Type::Primitive(PrimitiveType::Int32), DataType::Int64, false) => {
                (Kernel::Int32ToInt64(false), evaluate_wide::<false>)
            }
            (Type::Primitive(PrimitiveType::Int64), DataType::Int64, true) => {
                (Kernel::Int64(true), evaluate_same::<Int64Type, true>)
            }
            (Type::Primitive(PrimitiveType::Int64), DataType::Int64, false) => {
                (Kernel::Int64(false), evaluate_same::<Int64Type, false>)
            }
            _ => return Err(ExprError::Invalid("integer list sum type mismatch".into())),
        };
        Ok(Self {
            path,
            identity,
            evaluate,
        })
    }
    pub(super) fn evaluate(&self, array: &ArrayRef) -> Result<ArrayRef, ExprError> {
        (self.evaluate)(array, &self.path)
    }
}

struct Input<'a, T: arrow_array::types::ArrowPrimitiveType> {
    lists: &'a ListArray,
    values: &'a arrow_array::PrimitiveArray<T>,
    list_nulls: Option<NullBuffer>,
    value_nulls: Option<NullBuffer>,
}

/// Batch-boundary downcasts validate the prepared types; they never select a
/// kernel. Only data-dependent logical masks are assembled here.
fn input<'a, T: arrow_array::types::ArrowPrimitiveType>(
    array: &'a ArrayRef,
    path: &[usize],
) -> Result<Input<'a, T>, ExprError> {
    let lists = list_array(array)?;
    let list_nulls = lists.logical_nulls().filter(|n| n.null_count() != 0);
    let mut values = lists.values();
    let mut parents = Vec::with_capacity(path.len() + 1);
    for &index in path {
        let parent = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| ExprError::Invalid("integer list sum struct type changed".into()))?;
        parents.push(parent.logical_nulls());
        values = parent
            .columns()
            .get(index)
            .ok_or_else(|| ExprError::Invalid("integer list sum field absent".into()))?;
    }
    parents.push(values.logical_nulls());
    let value_nulls = NullBuffer::union_many(parents.iter().map(Option::as_ref));
    let values = values
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .ok_or_else(|| ExprError::Invalid("integer list sum primitive type changed".into()))?;
    Ok(Input {
        lists,
        values,
        list_nulls,
        value_nulls,
    })
}

/// Arrow owns same-width nullable reduction, including its SIMD selection.
/// Headers and slices share primitive values; only logical bitmaps are combined.
fn evaluate_same<T: arrow_array::ArrowNumericType, const FILTER_MISSING: bool>(
    array: &ArrayRef,
    path: &[usize],
) -> Result<ArrayRef, ExprError> {
    let input = input::<T>(array, path)?;
    let values =
        arrow_array::PrimitiveArray::<T>::new(input.values.values().clone(), input.value_nulls);
    let mut output = Vec::with_capacity(input.lists.len());
    for row in 0..input.lists.len() {
        if input.list_nulls.as_ref().is_some_and(|n| n.is_null(row)) {
            output.push(None);
            continue;
        }
        let start = input.lists.value_offsets()[row] as usize;
        let len = input.lists.value_offsets()[row + 1] as usize - start;
        let row = values.slice(start, len);
        output.push(if !FILTER_MISSING && row.null_count() != 0 {
            None
        } else {
            Some(arrow_arith::aggregate::sum(&row).unwrap_or_default())
        });
    }
    Ok(Arc::new(arrow_array::PrimitiveArray::<T>::from_iter(
        output,
    )))
}

/// Arrow 59's primitive sum accumulates in the input width. This adapter covers
/// its missing Int32-to-Int64 case without allocating a widened child vector.
fn evaluate_wide<const FILTER_MISSING: bool>(
    array: &ArrayRef,
    path: &[usize],
) -> Result<ArrayRef, ExprError> {
    let input = input::<Int32Type>(array, path)?;
    let mut output = Vec::with_capacity(input.lists.len());
    for row in 0..input.lists.len() {
        if input.list_nulls.as_ref().is_some_and(|n| n.is_null(row)) {
            output.push(None);
            continue;
        }
        let start = input.lists.value_offsets()[row] as usize;
        let end = input.lists.value_offsets()[row + 1] as usize;
        let values = &input.values.values()[start..end];
        output.push(match &input.value_nulls {
            None => Some(sum_slice(values)),
            Some(nulls) => sum_nullable::<FILTER_MISSING>(values, nulls, start),
        });
    }
    Ok(Arc::new(Int64Array::from(output)))
}

#[inline]
fn sum_slice(values: &[i32]) -> i64 {
    values
        .iter()
        .fold(0_i64, |sum, &v| sum.wrapping_add(i64::from(v)))
}

/// Inspect validity one word at a time. All-valid words take the vectorizable
/// typed-slice reduction; mixed words visit set bits without a branch per value.
fn sum_nullable<const FILTER_MISSING: bool>(
    values: &[i32],
    nulls: &NullBuffer,
    start: usize,
) -> Option<i64> {
    let bits = BitChunks::new(nulls.validity(), nulls.offset() + start, values.len());
    let mut sum = 0_i64;
    for (chunk, mask) in values.chunks_exact(64).zip(bits.iter()) {
        if mask == u64::MAX {
            sum = sum.wrapping_add(sum_slice(chunk));
        } else if !FILTER_MISSING {
            return None;
        } else {
            sum = sum.wrapping_add(sum_selected(chunk, mask));
        }
    }
    let remainder = values.chunks_exact(64).remainder();
    if !remainder.is_empty() {
        let mask = bits.remainder_bits();
        if mask == (1_u64 << remainder.len()) - 1 {
            sum = sum.wrapping_add(sum_slice(remainder));
        } else if !FILTER_MISSING {
            return None;
        } else {
            sum = sum.wrapping_add(sum_selected(remainder, mask));
        }
    }
    Some(sum)
}

#[inline]
fn sum_selected(values: &[i32], mut mask: u64) -> i64 {
    let mut sum = 0_i64;
    while mask != 0 {
        let index = mask.trailing_zeros() as usize;
        sum = sum.wrapping_add(i64::from(values[index]));
        mask &= mask - 1;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::{NullBuffer, OffsetBuffer};

    fn fixture() -> ArrayRef {
        let leaf: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(i32::MAX),
            Some(i32::MAX),
            Some(17),
            None,
            Some(5),
            Some(6),
        ]));
        let entries = StructArray::new(
            vec![Arc::new(Field::new("number", DataType::Int32, true))].into(),
            vec![leaf],
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        );
        let slots = StructArray::new(
            vec![Arc::new(Field::new(
                "payload",
                entries.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(entries)],
            None,
        );
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", slots.data_type().clone(), true)),
            OffsetBuffer::new(vec![0_i32, 3, 3, 4, 6].into()),
            Arc::new(slots),
            Some(NullBuffer::from(vec![true, true, true, false])),
        ))
    }
    fn sum_expression(path: &[String], filter_missing: bool, dtype: &DataType) -> Expr {
        let mut body = Expr::LambdaVariable {
            name: "entry".into(),
            dtype: None,
        };
        for name in path {
            body = Expr::GetField {
                value: Arc::new(body),
                name: name.clone(),
            };
        }
        if dtype == &DataType::Int64 {
            body = Expr::Cast {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: Arc::new(body),
            };
        }
        Expr::ArrayReduce {
            operation: ArrayReduction::Sum,
            filter_missing,
            value: Arc::new(Expr::ArrayMap {
                value: Arc::new(Expr::Column {
                    name: "cells".into(),
                }),
                parameter: "entry".into(),
                body: Arc::new(body),
            }),
        }
    }
    fn public_sum(
        input: &ArrayRef,
        path: &[String],
        filter_missing: bool,
        dtype: &DataType,
    ) -> Result<ArrayRef, ExprError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "cells",
            input.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![input.clone()]).unwrap();
        lower(&sum_expression(path, filter_missing, dtype), &schema)?
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .map_err(Into::into)
    }
    fn results(value: ArrayRef) -> Vec<Option<i64>> {
        value
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }
    #[test]
    fn null_entries_empty_all_null_and_slices_preserve_sum_semantics() {
        let input = fixture();
        let p = vec!["payload".into(), "number".into()];
        assert_eq!(
            results(public_sum(&input, &p, true, &DataType::Int64).unwrap()),
            vec![Some(i64::from(i32::MAX) * 2), Some(0), Some(0), None]
        );
        assert_eq!(
            results(public_sum(&input, &p, false, &DataType::Int64).unwrap()),
            vec![None, Some(0), None, None]
        );
        assert_eq!(
            results(public_sum(&input.slice(1, 3), &p, true, &DataType::Int64).unwrap()),
            vec![Some(0), Some(0), None]
        );
        let selected =
            arrow_select::take::take(&input, &UInt32Array::from(vec![2, 0]), None).unwrap();
        assert_eq!(
            results(public_sum(&selected, &p, true, &DataType::Int64).unwrap()),
            vec![Some(0), Some(i64::from(i32::MAX) * 2)]
        );
    }
    #[test]
    fn nested_map_cast_sum_returns_wide_values() {
        let input = fixture();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "cells",
            input.data_type().clone(),
            true,
        )]));
        let body = Expr::GetField {
            value: Arc::new(Expr::GetField {
                value: Arc::new(Expr::LambdaVariable {
                    name: "entry".into(),
                    dtype: None,
                }),
                name: "payload".into(),
            }),
            name: "number".into(),
        };
        let expr = Expr::ArrayReduce {
            operation: ArrayReduction::Sum,
            filter_missing: true,
            value: Arc::new(Expr::ArrayMap {
                value: Arc::new(Expr::Column {
                    name: "cells".into(),
                }),
                parameter: "entry".into(),
                body: Arc::new(Expr::Cast {
                    dtype: Type::Primitive(PrimitiveType::Int64),
                    value: Arc::new(body),
                }),
            }),
        };
        let program = lower(&expr, &schema).unwrap();
        let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
        let result = program.evaluate(&batch).unwrap().into_array(4).unwrap();
        assert_eq!(
            results(result),
            vec![Some(i64::from(i32::MAX) * 2), Some(0), Some(0), None]
        );
    }

    #[test]
    fn captured_public_entry_sum_without_cast_uses_int32_row_semantics() {
        // The public aggregate_entries(sum(entry-field)) frontend currently
        // emits this no-cast map shape, with Int64 only on the outer aggregate.
        let expression: Expr = serde_json::from_value(serde_json::json!({
            "op": "array_reduce", "operation": "sum", "filter_missing": true,
            "value": {"op": "array_map", "value": {"op": "column", "name": "cells"},
                "parameter": "cell", "body": {"op": "get_field", "name": "number",
                    "value": {"op": "get_field", "name": "payload",
                        "value": {"op": "lambda_variable", "name": "cell"}}}}
        }))
        .unwrap();
        let input = fixture();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "cells",
            input.data_type().clone(),
            true,
        )]));
        let program = lower(&expression, &schema).unwrap();
        let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
        let result = program.evaluate(&batch).unwrap().into_array(4).unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-2), Some(0), Some(0), None]
        );
    }

    #[test]
    fn int64_sum_wraps_at_public_expression_boundary() {
        let slots = StructArray::new(
            vec![Arc::new(Field::new("n", DataType::Int64, true))].into(),
            vec![Arc::new(Int64Array::from(vec![i64::MAX, 1]))],
            None,
        );
        let lists: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", slots.data_type().clone(), true)),
            OffsetBuffer::new(vec![0_i32, 2].into()),
            Arc::new(slots),
            None,
        ));
        assert_eq!(
            results(public_sum(&lists, &["n".into()], true, &DataType::Int64).unwrap()),
            vec![Some(i64::MIN)]
        );
    }
    #[test]
    fn public_sums_preserve_null_outer_lists_slices_and_nullable_leaves() {
        for values in [
            Arc::new(Int32Array::from(vec![i32::MAX, 1, 7, 8])) as ArrayRef,
            Arc::new(Int64Array::from(vec![i64::MAX, 1, 7, 8])) as ArrayRef,
        ] {
            let expected = if values.data_type() == &DataType::Int32 {
                i64::from(i32::MAX) + 1
            } else {
                i64::MIN
            };
            let lists: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", values.data_type().clone(), true)),
                OffsetBuffer::new(vec![0_i32, 2, 4, 4].into()),
                values,
                Some(NullBuffer::from(vec![true, false, true])),
            ));
            assert_eq!(
                results(public_sum(&lists, &[], true, &DataType::Int64).unwrap()),
                vec![Some(expected), None, Some(0)]
            );
            assert_eq!(
                results(public_sum(&lists.slice(1, 2), &[], true, &DataType::Int64).unwrap()),
                vec![None, Some(0)]
            );
        }
        for values in [
            Arc::new(Int32Array::from(vec![Some(3), None, Some(5)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(3), None, Some(5)])) as ArrayRef,
        ] {
            let lists: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", values.data_type().clone(), true)),
                OffsetBuffer::new(vec![0_i32, 1, 3].into()),
                values,
                None,
            ));
            assert_eq!(
                results(public_sum(&lists.slice(1, 1), &[], true, &DataType::Int64).unwrap()),
                vec![Some(5)]
            );
            assert_eq!(
                results(public_sum(&lists.slice(1, 1), &[], false, &DataType::Int64).unwrap()),
                vec![None]
            );
        }
    }

    #[test]
    fn computed_map_bodies_preserve_narrowing_and_division_errors() {
        let input = fixture();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "cells",
            input.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![input]).unwrap();
        let leaf = Expr::GetField {
            name: "number".into(),
            value: Arc::new(Expr::GetField {
                name: "payload".into(),
                value: Arc::new(Expr::LambdaVariable {
                    name: "entry".into(),
                    dtype: None,
                }),
            }),
        };
        let map_sum = |body| Expr::ArrayReduce {
            operation: ArrayReduction::Sum,
            filter_missing: true,
            value: Arc::new(Expr::ArrayMap {
                value: Arc::new(Expr::Column {
                    name: "cells".into(),
                }),
                parameter: "entry".into(),
                body: Arc::new(body),
            }),
        };
        let narrowed = Expr::Cast {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: Arc::new(Expr::Cast {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: Arc::new(leaf.clone()),
            }),
        };
        let actual = lower(&map_sum(narrowed), &schema)
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(4)
            .unwrap();
        assert_eq!(
            actual
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-2), Some(0), Some(0), None]
        );
        let fallible = Expr::Binary {
            operator: BinaryOperator::Floordiv,
            left: Arc::new(Expr::Cast {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: Arc::new(leaf),
            }),
            right: Arc::new(Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: serde_json::json!(0),
            }),
        };
        assert!(
            lower(&map_sum(fallible), &schema)
                .unwrap()
                .evaluate(&batch)
                .is_err()
        );
    }
}
