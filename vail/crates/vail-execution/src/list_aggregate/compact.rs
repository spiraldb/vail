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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StructArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;

    fn invoke(
        function: &ScalarUDF,
        input: ColumnarValue,
        dtype: &DataType,
        rows: usize,
    ) -> ColumnarValue {
        function
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![input],
                arg_fields: vec![Arc::new(Field::new("input", dtype.clone(), true))],
                number_rows: rows,
                return_field: Arc::new(Field::new("output", dtype.clone(), true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap()
    }

    #[test]
    fn public_guard_and_distinct_skip_growing_hidden_parent_payload() {
        for hidden in [0usize, 1, 63, 64, 65, 1_000_000] {
            let field = Arc::new(Field::new_list_field(DataType::Int32, true));
            let mut values = vec![7; hidden + 2];
            values[hidden + 1] = 9;
            let input = ListArray::new(
                field,
                OffsetBuffer::new(vec![0, 1, (hidden + 1) as i32, (hidden + 2) as i32].into()),
                Arc::new(Int32Array::from(values)),
                Some(NullBuffer::from(vec![true, false, true])),
            );
            let dtype = input.data_type().clone();
            let compact = invoke(
                &udf(dtype.clone()).unwrap(),
                ColumnarValue::Array(Arc::new(input)),
                &dtype,
                3,
            );
            let compact_array = compact.clone().into_array(3).unwrap();
            let compact_lists = compact_array.as_any().downcast_ref::<ListArray>().unwrap();
            // Public Arrow output capacity/offsets establish the boundary used
            // by DF; no counter, allocator hook or private helper oracle.
            assert_eq!(compact_lists.value_offsets(), &[0, 1, 1, 2]);
            assert_eq!(compact_lists.values().len(), 2);
            assert!(compact_lists.is_null(1));
            let distinct = invoke(
                &datafusion_functions_nested::set_ops::array_distinct_udf(),
                compact,
                &dtype,
                3,
            )
            .into_array(3)
            .unwrap();
            let lists = distinct.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(
                ScalarValue::try_from_array(&lists.value(0), 0).unwrap(),
                ScalarValue::Int32(Some(7))
            );
            assert!(lists.is_null(1));
            assert_eq!(
                ScalarValue::try_from_array(&lists.value(2), 0).unwrap(),
                ScalarValue::Int32(Some(9))
            );
        }
    }

    #[test]
    fn public_guard_preserves_slices_empty_null_scalar_and_nested_values() {
        let value_field = Arc::new(Field::new("x", DataType::Int32, true));
        let child: ArrayRef = Arc::new(StructArray::new(
            vec![value_field].into(),
            vec![Arc::new(Int32Array::from(vec![
                Some(88),
                Some(7),
                None,
                Some(99),
                Some(9),
                Some(77),
            ]))],
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        ));
        let field = Arc::new(Field::new_list_field(child.data_type().clone(), true));
        let input = ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 1, 3, 4, 4, 5, 6].into()),
            child,
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        )
        .slice(1, 4);
        let dtype = input.data_type().clone();
        let compact = invoke(
            &udf(dtype.clone()).unwrap(),
            ColumnarValue::Array(Arc::new(input)),
            &dtype,
            4,
        )
        .into_array(4)
        .unwrap();
        let lists = compact.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value_offsets(), &[0, 2, 2, 2, 3]);
        assert!(lists.is_null(1));
        assert!(!lists.is_null(2));
        assert_eq!(lists.value_length(2), 0);
        let first = lists.value(0);
        let structs = first.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            ScalarValue::try_from_array(structs.column(0), 0).unwrap(),
            ScalarValue::Int32(Some(7))
        );
        assert!(structs.is_null(1));
        let scalar_input = Arc::new(lists.slice(1, 1));
        let output = invoke(
            &udf(dtype.clone()).unwrap(),
            ColumnarValue::Scalar(ScalarValue::List(scalar_input)),
            &dtype,
            17,
        );
        let ColumnarValue::Scalar(ScalarValue::List(output)) = output else {
            panic!("scalar list preserved")
        };
        assert!(output.is_null(0));
        assert_eq!(output.value_length(0), 0);
        let empty = lists.slice(0, 0);
        let empty = invoke(
            &udf(dtype.clone()).unwrap(),
            ColumnarValue::Array(Arc::new(empty)),
            &dtype,
            0,
        )
        .into_array(0)
        .unwrap();
        assert_eq!(empty.len(), 0);
    }
}
