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
