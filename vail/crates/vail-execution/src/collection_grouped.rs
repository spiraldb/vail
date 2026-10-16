//! Consecutive array groups built as nested offsets over shared element buffers.
use arrow_array::{Array, ArrayRef, Int32Array, ListArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
struct Grouped {
    signature: Signature,
}
pub(crate) fn udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(Grouped {
        signature: Signature::any(2, Volatility::Immutable),
    }))
}
impl ScalarUDFImpl for Grouped {
    fn name(&self) -> &str {
        "__vail_array_grouped"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [input @ DataType::List(_), DataType::Int32] = args else {
            return plan_err!("grouped requires array and int32 size");
        };
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            input.clone(),
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("grouped requires two arguments");
        }
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let input = args.args[0].clone().into_array(rows)?;
        let sizes = args.args[1].clone().into_array(rows)?;
        self.return_type(&[input.data_type().clone(), sizes.data_type().clone()])?;
        let input = input.as_any().downcast_ref::<ListArray>().unwrap();
        let sizes = sizes.as_any().downcast_ref::<Int32Array>().unwrap();
        let begin = input.value_offsets()[0] as usize;
        let end = input.value_offsets()[rows] as usize;
        let mut groups = vec![0i32];
        let mut outer = Vec::with_capacity(rows + 1);
        let mut valid = Vec::with_capacity(rows);
        outer.push(0i32);
        for row in 0..rows {
            let start = input.value_offsets()[row] as usize - begin;
            let stop = input.value_offsets()[row + 1] as usize - begin;
            let present = input.is_valid(row) && sizes.is_valid(row);
            valid.push(present);
            if present {
                let size = sizes.value(row);
                if size <= 0 {
                    return exec_err!("group size must be positive");
                }
                let size = size as usize;
                let mut position = start;
                while position < stop {
                    position += size.min(stop - position);
                    groups.push(i32::try_from(position).map_err(|_| {
                        datafusion_common::exec_datafusion_err!(
                            "grouped element offset exceeds int32"
                        )
                    })?);
                }
            } else if start != stop {
                // Hidden children under a null parent still occupy a span.
                // One invisible group retains that span without copying values.
                groups.push(i32::try_from(stop).map_err(|_| {
                    datafusion_common::exec_datafusion_err!("grouped element offset exceeds int32")
                })?);
            }
            outer.push(i32::try_from(groups.len() - 1).map_err(|_| {
                datafusion_common::exec_datafusion_err!("group count exceeds int32")
            })?);
        }
        let DataType::List(item) = input.data_type() else {
            unreachable!()
        };
        let inner: ArrayRef = Arc::new(ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(groups.into()),
            input.values().slice(begin, end - begin),
            None,
        )?);
        let outer_type = Arc::new(Field::new("item", inner.data_type().clone(), true));
        let output = ListArray::try_new(
            outer_type,
            OffsetBuffer::new(outer.into()),
            inner,
            Some(NullBuffer::from(valid)),
        )?;
        if scalar {
            Ok(ColumnarValue::Scalar(
                crate::expressions::scalar_at(&(Arc::new(output) as ArrayRef), 0)
                    .map_err(|e| datafusion_common::exec_datafusion_err!("{e}"))?,
            ))
        } else {
            Ok(ColumnarValue::Array(Arc::new(output)))
        }
    }
}
