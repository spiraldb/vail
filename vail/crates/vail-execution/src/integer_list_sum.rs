//! Fuse a pure integer field projection into a segmented sum. No mapped or
//! widened child vector is allocated; ordinary expression lowering is fallback.
use super::*;

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

pub(super) fn evaluate(
    array: &ArrayRef,
    path: &[String],
    filter_missing: bool,
    dtype: &DataType,
) -> Result<ArrayRef, ExprError> {
    let lists = list_array(array)?;
    let list_nulls = lists
        .logical_nulls()
        .filter(|nulls| nulls.null_count() != 0);
    let mut values = lists.values();
    let mut parents = Vec::with_capacity(path.len());
    for name in path {
        let parent = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| ExprError::Invalid("integer list sum requires struct path".into()))?;
        if let Some(nulls) = parent
            .logical_nulls()
            .filter(|nulls| nulls.null_count() != 0)
        {
            parents.push(nulls);
        }
        values = parent
            .column_by_name(name)
            .ok_or_else(|| ExprError::Invalid("integer list sum field absent".into()))?;
    }
    let value_nulls = values
        .logical_nulls()
        .filter(|nulls| nulls.null_count() != 0);
    let valid = |i| {
        value_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(i))
            && parents.iter().all(|nulls| nulls.is_valid(i))
    };
    let source32 = values.as_any().downcast_ref::<Int32Array>();
    let source64 = values.as_any().downcast_ref::<Int64Array>();
    if source32.is_none() && source64.is_none() {
        return Err(ExprError::Invalid(
            "integer list sum requires integer values".into(),
        ));
    }
    // Dispatch once per batch. All-valid typed slices allow LLVM to reduce
    // integer lanes without per-element virtual validity or type checks.
    macro_rules! typed {
        ($source:expr) => {{
            let source = $source;
            let mut output = Vec::with_capacity(lists.len());
            for row in 0..lists.len() {
                if list_nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                    output.push(None);
                    continue;
                }
                let start = lists.value_offsets()[row] as usize;
                let end = lists.value_offsets()[row + 1] as usize;
                let mut missing = false;
                let sum = if let Some(nulls) = &value_nulls {
                    let mut sum = 0_i64;
                    for (i, &v) in source.values()[start..end].iter().enumerate() {
                        if nulls.is_valid(start + i) {
                            sum = sum.wrapping_add(i64::from(v));
                        } else {
                            missing = true;
                        }
                    }
                    sum
                } else {
                    source.values()[start..end]
                        .iter()
                        .fold(0_i64, |sum, &v| sum.wrapping_add(i64::from(v)))
                };
                output.push((filter_missing || !missing).then_some(sum));
            }
            output
        }};
    }
    let output = if parents.is_empty() {
        if let Some(source) = source32 {
            typed!(source)
        } else {
            typed!(source64.expect("integer type checked above"))
        }
    } else {
        let mut output = Vec::with_capacity(lists.len());
        for row in 0..lists.len() {
            if list_nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                output.push(None);
                continue;
            }
            let mut sum = 0_i64;
            let mut missing = false;
            for i in lists.value_offsets()[row] as usize..lists.value_offsets()[row + 1] as usize {
                if !valid(i) {
                    missing = true;
                    continue;
                }
                let v = if let Some(v) = source32 {
                    i64::from(v.value(i))
                } else {
                    source64.unwrap().value(i)
                };
                sum = sum.wrapping_add(v);
            }
            output.push((filter_missing || !missing).then_some(sum));
        }
        output
    };
    Ok(match dtype {
        DataType::Int32 => Arc::new(Int32Array::from(
            output
                .into_iter()
                .map(|v| v.map(|v| v as i32))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(output)),
        _ => return Err(ExprError::Invalid("integer list sum output type".into())),
    })
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
