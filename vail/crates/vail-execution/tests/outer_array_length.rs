//! Public scalar UDF contracts for Hail's outer-list lengths.
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Int32Array, LargeListArray, ListArray, NullArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use datafusion_common::{ScalarValue, config::ConfigOptions};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use std::sync::Arc;

fn run(value: ColumnarValue) -> datafusion_common::Result<ColumnarValue> {
    let ty = value.data_type();
    vail_execution::array_length::udf().invoke_with_args(ScalarFunctionArgs {
        args: vec![value],
        arg_fields: vec![Arc::new(Field::new("x", ty, true))],
        number_rows: 99,
        return_field: Arc::new(Field::new("length", DataType::Int32, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
}
fn check(value: ArrayRef, expected: Vec<Option<i32>>) {
    let ColumnarValue::Array(result) = run(ColumnarValue::Array(value)).unwrap() else {
        panic!("array")
    };
    assert_eq!(result.data_type(), &DataType::Int32);
    let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(result.iter().collect::<Vec<_>>(), expected);
}
#[test]
fn outer_lengths_preserve_slices_nulls_empty_and_nested_lists() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
    let field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let nulls = Some(NullBuffer::from(vec![true, true, false, true]));
    let list = ListArray::new(
        field.clone(),
        OffsetBuffer::new(vec![0, 1, 1, 4, 6].into()),
        values.clone(),
        nulls.clone(),
    );
    let large = LargeListArray::new(
        field.clone(),
        OffsetBuffer::new(vec![0i64, 1, 1, 4, 6].into()),
        values.clone(),
        nulls,
    );
    for value in [Arc::new(list) as ArrayRef, Arc::new(large)] {
        check(value.clone(), vec![Some(1), Some(0), None, Some(2)]);
        check(value.slice(1, 3), vec![Some(0), None, Some(2)]);
        check(value.slice(2, 0), vec![]);
    }
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        field,
        2,
        values,
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    check(fixed.slice(1, 2), vec![None, Some(2)]);
    let child: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
        arrow_array::types::Int32Type,
        _,
        _,
    >(vec![Some(vec![Some(1)]), None, Some(vec![])]));
    let outer: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(child.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 2, 3].into()),
        child,
        None,
    ));
    check(outer, vec![Some(2), Some(1)]);
}
#[test]
fn scalar_lengths_stay_scalar_and_large_lengths_are_checked() {
    let array = Arc::new(ListArray::from_iter_primitive::<
        arrow_array::types::Int32Type,
        _,
        _,
    >(vec![Some(vec![Some(1), None]), None, Some(vec![])]));
    for (i, expected) in [(0, Some(2)), (1, None), (2, Some(0))] {
        let scalar = ScalarValue::try_from_array(array.as_ref(), i).unwrap();
        let output = run(ColumnarValue::Scalar(scalar)).unwrap();
        assert!(matches!(output,ColumnarValue::Scalar(ScalarValue::Int32(v)) if v==expected));
    }
    let huge = i32::MAX as usize + 1;
    let values: ArrayRef = Arc::new(NullArray::new(huge));
    let field = Arc::new(Field::new_list_field(DataType::Null, true));
    let offsets = OffsetBuffer::new(vec![0i64, huge as i64].into());
    let array: ArrayRef = Arc::new(LargeListArray::new(
        field.clone(),
        offsets.clone(),
        values.clone(),
        None,
    ));
    assert!(
        run(ColumnarValue::Array(array))
            .unwrap_err()
            .to_string()
            .contains("Int32")
    );
    let null: ArrayRef = Arc::new(LargeListArray::new(
        field,
        offsets,
        values,
        Some(NullBuffer::new_null(1)),
    ));
    check(null, vec![None]);
}
