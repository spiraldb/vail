//! Integer import kernels: traverse borrowed UTF-8 and write directly to Arrow.
//! Schema dispatch and builders are batch work. Inner loops only scan bytes,
//! convert primitive digits, and append to already allocated output buffers.
use crate::expressions::Type;
use arrow_array::builder::{ListBuilder, PrimitiveBuilder};
use arrow_array::types::{ArrowPrimitiveType, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility};
use std::{str::FromStr, sync::Arc};

fn invalid(array: bool, bits: usize) -> DataFusionError {
    DataFusionError::Execution(if array {
        format!("invalid JSON integer array for array<int{bits}>")
    } else {
        format!("cannot parse int{bits} from input string")
    })
}

fn whitespace(bytes: &[u8], p: &mut usize) {
    while bytes
        .get(*p)
        .is_some_and(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
    {
        *p += 1;
    }
}

fn decode<T: ArrowPrimitiveType>(
    strings: &StringArray,
    array: bool,
    bits: usize,
) -> Result<ArrayRef>
where
    T::Native: FromStr,
{
    if !array {
        let mut out = PrimitiveBuilder::<T>::with_capacity(strings.len());
        for value in strings.iter() {
            match value {
                None => out.append_null(),
                Some(value) => {
                    // Rust's signed parser accepts exactly the primitive import
                    // grammar [+-]?[0-9]+ and checks the target integer bounds.
                    out.append_value(value.parse().map_err(|_| invalid(false, bits))?);
                }
            }
        }
        return Ok(Arc::new(out.finish()));
    }
    let mut out = ListBuilder::with_capacity(PrimitiveBuilder::<T>::new(), strings.len());
    for value in strings.iter() {
        let Some(value) = value else {
            out.append(false);
            continue;
        };
        let bytes = value.as_bytes();
        let mut p = 0;
        whitespace(bytes, &mut p);
        if bytes.get(p) != Some(&b'[') {
            return Err(invalid(true, bits));
        }
        p += 1;
        whitespace(bytes, &mut p);
        if bytes.get(p) != Some(&b']') {
            loop {
                let start = p;
                if bytes.get(p..p + 4) == Some(b"null") {
                    out.values().append_null();
                    p += 4;
                } else {
                    if bytes.get(p) == Some(&b'-') {
                        p += 1;
                    }
                    match bytes.get(p) {
                        Some(b'0') => p += 1,
                        Some(b'1'..=b'9') => {
                            p += 1;
                            while bytes.get(p).is_some_and(u8::is_ascii_digit) {
                                p += 1;
                            }
                        }
                        _ => return Err(invalid(true, bits)),
                    }
                    out.values()
                        .append_value(value[start..p].parse().map_err(|_| invalid(true, bits))?);
                }
                whitespace(bytes, &mut p);
                match bytes.get(p) {
                    Some(b',') => {
                        p += 1;
                        whitespace(bytes, &mut p);
                    }
                    Some(b']') => break,
                    _ => return Err(invalid(true, bits)),
                }
            }
        }
        p += 1;
        whitespace(bytes, &mut p);
        if p != bytes.len() {
            return Err(invalid(true, bits));
        }
        out.append(true);
    }
    Ok(Arc::new(out.finish()))
}

pub(crate) fn make_udf(target: Type) -> Result<ScalarUDF> {
    let output = vail_ordering::without_type_metadata(
        &target
            .to_arrow()
            .map_err(|e| DataFusionError::Plan(e.to_string()))?,
    );
    let (array, element) = match &output {
        DataType::List(field) => (true, field.data_type()),
        other => (false, other),
    };
    let bits = match element {
        DataType::Int32 => 32,
        DataType::Int64 => 64,
        _ => {
            return Err(DataFusionError::Plan(
                "integer import requires int32/int64 or arrays thereof".into(),
            ));
        }
    };
    Ok(crate::kernel_identity::scalar(
        "parse_import_integer",
        (array, bits),
        Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
        output,
        Arc::new(move |args| {
            let [input] = args else {
                return Err(DataFusionError::Internal(
                    "integer import requires one input".into(),
                ));
            };
            let scalar = matches!(input, ColumnarValue::Scalar(_));
            let input = input.to_array(1)?;
            let strings = input
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("integer import requires strings".into())
                })?;
            let out = if bits == 32 {
                decode::<Int32Type>(strings, array, bits)?
            } else {
                decode::<Int64Type>(strings, array, bits)?
            };
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
            } else {
                Ok(ColumnarValue::Array(out))
            }
        }),
    ))
}
