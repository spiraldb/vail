//! Typed DataFusion list kernels: no full-array numeric casts or scalar extraction.

use arrow_array::{Array, ArrayRef, Float64Array, Int32Array, ListArray, PrimitiveArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::{Arc, OnceLock};

pub(crate) fn range_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    UDF.get_or_init(|| {
        Arc::new(ScalarUDF::from(NumericList {
            operation: Operation::Range,
            signature: Signature::exact(vec![DataType::Int32; 3], Volatility::Immutable),
        }))
    })
    .clone()
}

pub(crate) fn mean_udf(strict: bool) -> Arc<ScalarUDF> {
    static FILTERED: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    static STRICT: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    (if strict { &STRICT } else { &FILTERED })
        .get_or_init(|| {
            Arc::new(ScalarUDF::from(NumericList {
                operation: Operation::Mean { strict },
                signature: Signature::any(1, Volatility::Immutable),
            }))
        })
        .clone()
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum Operation {
    Range,
    Mean { strict: bool },
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NumericList {
    operation: Operation,
    signature: Signature,
}

impl ScalarUDFImpl for NumericList {
    fn name(&self) -> &str {
        match self.operation {
            Operation::Range => "vail_range_int32",
            Operation::Mean { strict: false } => "vail_array_mean",
            Operation::Mean { strict: true } => "vail_array_mean_strict",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match self.operation {
            Operation::Range if args.len() == 3 && args.iter().all(|t| t == &DataType::Int32) => {
                Ok(DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Int32,
                    true,
                ))))
            }
            Operation::Mean { .. }
                if matches!(args, [DataType::List(f)] if matches!(f.data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                DataType::Float32 | DataType::Float64)) =>
            {
                Ok(DataType::Float64)
            }
            _ => plan_err!("invalid {} arguments: {args:?}", self.name()),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|v| v.clone().into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        let output: ArrayRef = match self.operation {
            Operation::Range => Arc::new(range_arrays(&arrays)?),
            Operation::Mean { strict } => Arc::new(mean_lists(&arrays[0], strict)?),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

fn range_length(start: i32, stop: i32, step: i32) -> Result<usize> {
    if step == 0 {
        return exec_err!("range step must not be zero");
    }
    let distance = if step > 0 {
        i64::from(stop) - i64::from(start)
    } else {
        i64::from(start) - i64::from(stop)
    };
    if distance <= 0 {
        return Ok(0);
    }
    let stride = i64::from(step).abs();
    let length = (distance - 1) / stride + 1;
    if length > i64::from(i32::MAX) {
        return exec_err!("range exceeds Arrow List's int32 offset capacity");
    }
    Ok(length as usize)
}

fn range_arrays(args: &[ArrayRef]) -> Result<ListArray> {
    let ints = args
        .iter()
        .map(|a| {
            a.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                datafusion_common::exec_datafusion_err!("range requires int32 arguments")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let rows = ints[0].len();
    let mut offsets = Vec::with_capacity(rows + 1);
    let mut valid = Vec::with_capacity(rows);
    let mut total = 0usize;
    offsets.push(0i32);
    for row in 0..rows {
        let defined = ints.iter().all(|a| a.is_valid(row));
        valid.push(defined);
        if defined {
            total = total
                .checked_add(range_length(
                    ints[0].value(row),
                    ints[1].value(row),
                    ints[2].value(row),
                )?)
                .filter(|n| *n <= i32::MAX as usize)
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!(
                        "range batch exceeds Arrow List's int32 offset capacity"
                    )
                })?;
        }
        offsets.push(total as i32);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(total)
        .map_err(|e| datafusion_common::exec_datafusion_err!("range allocation failed: {e}"))?;
    for row in 0..rows {
        let len = (offsets[row + 1] - offsets[row]) as usize;
        let start = i64::from(ints[0].value(row));
        let step = i64::from(ints[2].value(row));
        values.extend((0..len).map(|i| (start + i as i64 * step) as i32));
    }
    let nulls = if valid.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(valid))
    };
    Ok(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(Int32Array::from(values)),
        nulls,
    ))
}

fn mean_lists(array: &ArrayRef, strict: bool) -> Result<Float64Array> {
    use arrow_array::types::*;
    let lists = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("mean requires a numeric list"))?;
    macro_rules! reduce {
        ($ty:ty) => {{
            let values = lists
                .values()
                .as_any()
                .downcast_ref::<PrimitiveArray<$ty>>()
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!("mean child type mismatch")
                })?;
            let result = (0..lists.len()).map(|row| {
                if lists.is_null(row) {
                    return None;
                }
                let start = lists.value_offsets()[row] as usize;
                let end = lists.value_offsets()[row + 1] as usize;
                let (sum, count) = if values.null_count() == 0 {
                    (
                        values.values()[start..end]
                            .iter()
                            .map(|v| *v as f64)
                            .sum::<f64>(),
                        end - start,
                    )
                } else {
                    let slice = values.slice(start, end - start);
                    let count = slice.len() - slice.null_count();
                    if strict && count != slice.len() {
                        return None;
                    }
                    (slice.iter().flatten().map(|v| v as f64).sum::<f64>(), count)
                };
                Some(sum / count as f64)
            });
            Ok(Float64Array::from_iter(result))
        }};
    }
    match lists.value_type() {
        DataType::Int8 => reduce!(Int8Type),
        DataType::Int16 => reduce!(Int16Type),
        DataType::Int32 => reduce!(Int32Type),
        DataType::Int64 => reduce!(Int64Type),
        DataType::UInt8 => reduce!(UInt8Type),
        DataType::UInt16 => reduce!(UInt16Type),
        DataType::UInt32 => reduce!(UInt32Type),
        DataType::UInt64 => reduce!(UInt64Type),
        DataType::Float32 => reduce!(Float32Type),
        DataType::Float64 => {
            let values = lists
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion_common::exec_datafusion_err!("mean child type mismatch")
                })?;
            Ok(Float64Array::from_iter((0..lists.len()).map(|row| {
                if lists.is_null(row) {
                    return None;
                }
                let start = lists.value_offsets()[row] as usize;
                let end = lists.value_offsets()[row + 1] as usize;
                let slice = values.slice(start, end - start);
                if strict && slice.null_count() > 0 {
                    return None;
                }
                let count = slice.len() - slice.null_count();
                Some(
                    arrow_arith::aggregate::sum::<Float64Type>(&slice).unwrap_or(0.) / count as f64,
                )
            })))
        }
        _ => exec_err!("mean requires numeric elements"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float32Array, Int64Array, UInt64Array};

    #[test]
    fn range_type_offsets_nulls_and_signed_steps() {
        let input: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![
                Some(0),
                Some(5),
                None,
                Some(3),
                Some(i32::MAX),
            ])),
            Arc::new(Int32Array::from(vec![5, -2, 5, 3, i32::MIN])),
            Arc::new(Int32Array::from(vec![2, -3, 0, 1, i32::MIN])),
        ];
        let output = range_arrays(&input).unwrap();
        assert_eq!(output.value_offsets(), &[0, 3, 6, 6, 6, 8]);
        assert!(output.is_null(2));
        let values = output
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[0, 2, 4, 5, 2, -1, i32::MAX, -1]);
    }

    #[test]
    fn range_length_and_errors_cover_boundaries_without_allocating() {
        for start in -5..=5 {
            for stop in -5..=5 {
                for step in -5..=5 {
                    if step == 0 {
                        continue;
                    }
                    let mut expected = 0;
                    let mut n = start;
                    while if step > 0 { n < stop } else { n > stop } {
                        expected += 1;
                        n += step;
                    }
                    assert_eq!(range_length(start, stop, step).unwrap(), expected);
                }
            }
        }
        assert!(range_length(0, 0, 0).is_err());
        assert!(range_length(i32::MIN, i32::MAX, 1).is_err());
        assert_eq!(range_length(i32::MIN, i32::MAX, i32::MAX).unwrap(), 3);
    }

    fn check_mean(values: ArrayRef, expected: f64) {
        let list = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, values.len() as i32].into()),
            values,
            None,
        );
        let result = mean_lists(&(Arc::new(list) as ArrayRef), false).unwrap();
        assert_eq!(result.value(0), expected);
    }

    #[test]
    fn integral_mean_never_overflows_an_integer_accumulator() {
        check_mean(
            Arc::new(Int32Array::from(vec![i32::MAX, i32::MAX])),
            i32::MAX as f64,
        );
        check_mean(
            Arc::new(Int64Array::from(vec![i64::MAX, i64::MAX])),
            i64::MAX as f64,
        );
        check_mean(
            Arc::new(UInt64Array::from(vec![u64::MAX, u64::MAX])),
            u64::MAX as f64,
        );
        check_mean(Arc::new(Float32Array::from(vec![1., 2., 3.])), 2.);
    }

    #[test]
    fn mean_sliced_lists_missing_rows_members_and_empty_arrays() {
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 1, 4, 4, 5, 7].into()),
            Arc::new(Int32Array::from(vec![
                Some(99),
                Some(2),
                None,
                Some(4),
                None,
                Some(6),
                Some(8),
            ])),
            Some(NullBuffer::from(vec![true, true, true, true, false])),
        );
        let sliced: ArrayRef = Arc::new(list.slice(1, 4));
        let filtered = mean_lists(&sliced, false).unwrap();
        assert_eq!(filtered.value(0), 3.);
        assert!(filtered.value(1).is_nan());
        assert!(filtered.value(2).is_nan());
        assert!(filtered.is_null(3));
        let strict = mean_lists(&sliced, true).unwrap();
        assert!(strict.is_null(0));
        assert!(strict.value(1).is_nan());
        assert!(strict.is_null(2));
        assert!(strict.is_null(3));
    }

    #[test]
    fn float_mean_preserves_nan_and_infinities() {
        check_mean(
            Arc::new(Float64Array::from(vec![1., f64::INFINITY])),
            f64::INFINITY,
        );
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1., f64::NAN]));
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Float64, true)),
            OffsetBuffer::new(vec![0, 2].into()),
            values,
            None,
        );
        assert!(
            mean_lists(&(Arc::new(list) as ArrayRef), false)
                .unwrap()
                .value(0)
                .is_nan()
        );
    }
}
