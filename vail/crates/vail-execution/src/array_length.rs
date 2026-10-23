//! Hail outer-list length: adjacent offsets, directly into Int32 output.
//! Child values are never sliced or inspected. A nested list still has an outer length.
use arrow_array::{Array, ArrayRef, Int32Array, LargeListArray, ListArray};
use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility};
use std::sync::{Arc, OnceLock};

#[derive(Debug, PartialEq, Eq, Hash)]
struct OuterLength;

pub fn udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    UDF.get_or_init(|| {
        Arc::new(crate::kernel_identity::scalar(
            "vail_array_length",
            OuterLength,
            Signature::any(1, Volatility::Immutable),
            DataType::Int32,
            Arc::new(|args| {
                let Some(input) = args.first() else {
                    return exec_err!("array length requires one input");
                };
                match input {
                    ColumnarValue::Array(array) => Ok(ColumnarValue::Array(lengths(array)?)),
                    ColumnarValue::Scalar(value) => {
                        // Scalar list values already own a one-row Arrow list. Read
                        // its offsets directly: no one-row output/argument arrays.
                        let (valid, length) = match value {
                            ScalarValue::List(a) => (!a.is_null(0), i64::from(a.value_length(0))),
                            ScalarValue::LargeList(a) => (!a.is_null(0), a.value_length(0)),
                            ScalarValue::FixedSizeList(a) => {
                                (!a.is_null(0), i64::from(a.value_length()))
                            }
                            _ => return exec_err!("array length requires list input"),
                        };
                        let length = if valid {
                            Some(i32::try_from(length).map_err(|_| {
                                datafusion_common::exec_datafusion_err!(
                                    "array length exceeds Hail Int32 range"
                                )
                            })?)
                        } else {
                            None
                        };
                        Ok(ColumnarValue::Scalar(ScalarValue::Int32(length)))
                    }
                }
            }),
        ))
    })
    .clone()
}

fn lengths(input: &ArrayRef) -> Result<ArrayRef> {
    let values: Vec<i32> = match input.data_type() {
        DataType::List(_) => {
            let lists = input.as_any().downcast_ref::<ListArray>().unwrap();
            lists
                .value_offsets()
                .windows(2)
                .map(|o| o[1] - o[0])
                .collect()
        }
        DataType::LargeList(_) => {
            let lists = input.as_any().downcast_ref::<LargeListArray>().unwrap();
            lists
                .value_offsets()
                .windows(2)
                .enumerate()
                .map(|(i, o)| {
                    if lists.is_null(i) {
                        Ok(0)
                    } else {
                        i32::try_from(o[1] - o[0]).map_err(|_| {
                            datafusion_common::exec_datafusion_err!(
                                "array length exceeds Hail Int32 range"
                            )
                        })
                    }
                })
                .collect::<Result<Vec<_>>>()?
        }
        DataType::FixedSizeList(_, width) => vec![*width; input.len()],
        _ => {
            return exec_err!(
                "array length requires list input, found {}",
                input.data_type()
            );
        }
    };
    Ok(Arc::new(Int32Array::new(
        values.into(),
        input.nulls().cloned(),
    )))
}
