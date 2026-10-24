//! Establish the outer-list ownership boundary before a batch library kernel.
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray};
use arrow_buffer::OffsetBuffer;
use arrow_data::transform::MutableArrayData;
use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// A ListArray whose null parents own no child values. Its first-to-last offset
/// envelope therefore contains only defined outer-list payload. Nested child
/// ownership is unchanged; Arrow's existing nested row encoding still applies.
struct CompactDefinedLists(ArrayRef);

impl CompactDefinedLists {
    fn try_new(input: ArrayRef) -> Result<Self> {
        let lists = input
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| exec_datafusion_err!("list ownership guard requires List input"))?;
        let offsets = lists.value_offsets();
        // Actual masks/offsets are batch properties. Schema/type eligibility was
        // selected by the caller at query preparation, not inside these loops.
        if lists.null_count() == 0
            || (0..lists.len()).all(|row| !lists.is_null(row) || offsets[row] == offsets[row + 1])
        {
            return Ok(Self(input));
        }
        let mut lengths = Vec::with_capacity(lists.len());
        let mut selected = 0usize;
        for row in 0..lists.len() {
            let len = if lists.is_null(row) {
                0
            } else {
                (offsets[row + 1] - offsets[row]) as usize
            };
            selected = selected
                .checked_add(len)
                .ok_or_else(|| exec_datafusion_err!("list ownership child count overflow"))?;
            lengths.push(len);
        }
        let data = lists.values().to_data();
        let mut output = MutableArrayData::new(vec![&data], false, selected);
        let mut run: Option<(usize, usize)> = None;
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            match run {
                Some((first, last)) if last == start => run = Some((first, end)),
                Some((first, last)) => {
                    output.extend(0, first, last);
                    run = Some((start, end));
                }
                None => run = Some((start, end)),
            }
        }
        if let Some((first, last)) = run {
            output.extend(0, first, last);
        }
        let DataType::List(field) = lists.data_type() else {
            unreachable!()
        };
        let compact = ListArray::try_new(
            field.clone(),
            OffsetBuffer::from_lengths(lengths),
            arrow_array::make_array(output.freeze()),
            lists.nulls().cloned(),
        )?;
        Ok(Self(Arc::new(compact)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CompactDefinedListsUdf {
    signature: Signature,
    dtype: DataType,
}

/// Both logical and physical set lowering use this same prepared guard before
/// ordinary DataFusion array_distinct, which otherwise encodes hidden payload.
pub(crate) fn udf(dtype: DataType) -> Result<Arc<ScalarUDF>> {
    if !matches!(dtype, DataType::List(_)) {
        return Err(plan_datafusion_err!(
            "list ownership guard requires List input"
        ));
    }
    Ok(Arc::new(ScalarUDF::from(CompactDefinedListsUdf {
        signature: Signature::exact(vec![dtype.clone()], Volatility::Immutable),
        dtype,
    })))
}

impl ScalarUDFImpl for CompactDefinedListsUdf {
    fn name(&self) -> &str {
        "__vail_compact_defined_lists"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args.args.remove(0);
        match input {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(CompactDefinedLists::try_new(array)?.0))
            }
            ColumnarValue::Scalar(ScalarValue::List(array)) => {
                let compact = CompactDefinedLists::try_new(array)?;
                let lists = compact.0.as_any().downcast_ref::<ListArray>().unwrap();
                Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                    lists.clone(),
                ))))
            }
            _ => Err(exec_datafusion_err!(
                "list ownership guard requires List input"
            )),
        }
    }
}
