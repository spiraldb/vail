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

/// Reshape only replaces descriptors; the contiguous element buffer is shared.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Reshape { signature: Signature, name: String, rank: usize }
pub(crate) fn reshape_udf(rank: usize) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(Reshape {
        signature: Signature::any(2, Volatility::Immutable),
        name: format!("__vail_ndarray_reshape_{rank}"), rank,
    }))
}
impl ScalarUDFImpl for Reshape {
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [DataType::Struct(fields), DataType::List(item)] = args else {
            return plan_err!("reshape requires ndarray and integer dimensions");
        };
        if fields.len() != 2 || fields[0].name() != "__vail_ndarray_data"
            || fields[1].name() != "__vail_ndarray_shape" || item.data_type() != &DataType::Int64 {
            return plan_err!("invalid ndarray reshape storage");
        }
        let DataType::List(element) = fields[0].data_type() else {
            return plan_err!("reshape requires ndarray data list");
        };
        let element = Type::from_arrow(element.data_type())
            .map_err(|e| datafusion_common::plan_datafusion_err!("{e}"))?;
        Type::Ndarray { ndarray: Box::new((element, self.rank)) }.to_arrow()
            .map_err(|e| datafusion_common::plan_datafusion_err!("{e}"))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use arrow_array::builder::{Int64Builder, ListBuilder};
        use arrow_buffer::NullBuffer;
        let scalar = args.args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let len = if scalar { 1 } else { args.number_rows };
        let input = args.args[0].clone().into_array(len)?;
        let dims = args.args[1].clone().into_array(len)?;
        let Some(input) = input.as_any().downcast_ref::<StructArray>() else {
            return exec_err!("reshape requires ndarray storage");
        };
        let Some(dims) = dims.as_any().downcast_ref::<ListArray>() else {
            return exec_err!("reshape requires dimension list");
        };
        let Some(data) = input.column(0).as_any().downcast_ref::<ListArray>() else {
            return exec_err!("reshape requires ndarray data list");
        };
        let Some(dim_values) = dims.values().as_any().downcast_ref::<Int64Array>() else {
            return exec_err!("reshape dimensions must be int64");
        };
        let mut shape = ListBuilder::new(Int64Builder::new());
        let mut valid = Vec::with_capacity(len);
        for row in 0..len {
            if input.is_null(row) || dims.is_null(row) {
                shape.append(false); valid.push(false); continue;
            }
            let begin = dims.value_offsets()[row] as usize;
            let end = dims.value_offsets()[row + 1] as usize;
            if end - begin != self.rank { return exec_err!("reshape dimension count disagrees with output rank"); }
            let mut product = 1i64;
            let mut inferred = None;
            for index in begin..end {
                if dim_values.is_null(index) { return exec_err!("reshape dimension is missing"); }
                let size = dim_values.value(index);
                if size == -1 {
                    if inferred.replace(index).is_some() { return exec_err!("reshape allows at most one inferred dimension"); }
                } else if size < 0 {
                    return exec_err!("reshape dimensions must be nonnegative or -1");
                } else {
                    product = product.checked_mul(size).ok_or_else(|| datafusion_common::exec_datafusion_err!("reshape shape overflow"))?;
                }
            }
            let count = i64::from(data.value_offsets()[row + 1] - data.value_offsets()[row]);
            let inferred_size = if inferred.is_some() {
                if product == 0 || count % product != 0 { return exec_err!("reshape dimensions do not match element count"); }
                count / product
            } else {
                if product != count { return exec_err!("reshape dimensions do not match element count"); }
                0
            };
            for index in begin..end {
                shape.values().append_value(if Some(index) == inferred { inferred_size } else { dim_values.value(index) });
            }
            shape.append(true); valid.push(true);
        }
        let DataType::Struct(fields) = args.return_type() else {
            return exec_err!("reshape requires ndarray output");
        };
        let data = crate::logical_expr::restore_arrow_array_type(input.column(0), fields[0].data_type())
            .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?;
        let result: ArrayRef = Arc::new(StructArray::try_new(
            fields.clone(), vec![data, Arc::new(shape.finish())],
            Some(NullBuffer::from(valid)),
        )?);
        if scalar {
            Ok(ColumnarValue::Scalar(crate::expressions::scalar_at(&result, 0)
                .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?))
        } else { Ok(ColumnarValue::Array(result)) }
    }
}
