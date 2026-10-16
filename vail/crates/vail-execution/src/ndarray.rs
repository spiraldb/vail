//! Checked one-dimensional ndarray construction with shared list payloads.
use crate::expressions::Type;
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
struct FromArray {
    signature: Signature,
}
pub(crate) fn udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(FromArray {
        signature: Signature::any(1, Volatility::Immutable),
    }))
}
impl ScalarUDFImpl for FromArray {
    fn name(&self) -> &str {
        "__vail_ndarray_from_array"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [DataType::List(item)] = args else {
            return plan_err!("ndarray constructor requires one numeric array");
        };
        if !matches!(
            item.data_type(),
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
        ) {
            return plan_err!("ndarray constructor requires numeric elements");
        }
        let element = Type::from_arrow(item.data_type())
            .map_err(|e| datafusion_common::plan_datafusion_err!("{e}"))?;
        Type::Ndarray {
            ndarray: Box::new((element, 1)),
        }
        .to_arrow()
        .map_err(|e| datafusion_common::plan_datafusion_err!("{e}"))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(&args.args[0], ColumnarValue::Scalar(_));
        // A scalar input needs one descriptor, irrespective of the batch size.
        let input = args.args[0]
            .clone()
            .into_array(if scalar { 1 } else { args.number_rows })?;
        let Some(values) = input.as_any().downcast_ref::<ListArray>() else {
            return exec_err!("ndarray constructor requires List storage");
        };
        let DataType::Struct(fields) = args.return_type() else {
            return exec_err!("ndarray constructor requires Struct output");
        };
        let data = crate::logical_expr::restore_arrow_array_type(&input, fields[0].data_type())
            .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?;
        let lengths = Int64Array::from_iter_values(
            values
                .value_offsets()
                .windows(2)
                .map(|pair| i64::from(pair[1] - pair[0])),
        );
        let DataType::List(shape_item) = fields[1].data_type() else {
            return exec_err!("ndarray shape requires List storage");
        };
        let shape: ArrayRef = Arc::new(ListArray::try_new(
            shape_item.clone(),
            OffsetBuffer::from_lengths(std::iter::repeat_n(1, values.len())),
            Arc::new(lengths),
            None,
        )?);
        let result: ArrayRef = Arc::new(StructArray::try_new(
            fields.clone(),
            vec![data, shape],
            values.nulls().cloned(),
        )?);
        if scalar {
            Ok(ColumnarValue::Scalar(
                crate::expressions::scalar_at(&result, 0)
                    .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?,
            ))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}
