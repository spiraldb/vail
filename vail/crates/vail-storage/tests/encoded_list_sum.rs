//! Public differential checks for encoded grouped reductions.
use vortex::{VortexSessionDefault, array::VortexSessionExecute, session::VortexSession};
use vortex::array::{IntoArray, assert_arrays_eq};
use vortex::array::aggregate_fn::{GroupedAccumulator, DynGroupedAccumulator, NumericalAggregateOpts};
use vortex::array::aggregate_fn::fns::sum_v2::SumV2;
use vortex::array::arrays::{PrimitiveArray, ListViewArray};
use vortex::array::validity::Validity;
use vortex::buffer::buffer;
use vortex::dtype::{DType,PType,Nullability::Nullable};
use vortex::error::VortexResult;
#[test]
fn grouped_dictionary_preserves_code_value_and_list_nulls() -> VortexResult<()> {
    use vortex::array::arrays::DictArray;
    let codes = PrimitiveArray::from_option_iter([
        Some(0u8), Some(1), None, Some(2), Some(3), Some(0), Some(3),
    ]).into_array();
    let values = PrimitiveArray::from_option_iter([
        Some(i32::MAX), None, Some(i32::MIN), Some(7),
    ]).into_array();
    let elements = DictArray::try_new(codes, values)?.into_array();
    // Overlapping/reordered ranges exercise ListView rather than only List offsets.
    let groups = ListViewArray::try_new(
        elements,
        buffer![0i32, 1, 3, 4, 0, 6].into_array(),
        buffer![7i32, 2, 1, 2, 0, 1].into_array(),
        Validity::from_iter([true, true, true, true, true, false]),
    )?.into_array();
    let mut accumulator = GroupedAccumulator::try_new(
        SumV2, NumericalAggregateOpts::default(),
        DType::Primitive(PType::I32, Nullable),
    )?;
    let mut ctx = VortexSession::default().create_execution_ctx();
    accumulator.accumulate_list(&groups, &mut ctx)?;
    let result = accumulator.finish()?;
    let expected = PrimitiveArray::from_option_iter([
        Some(i64::from(i32::MAX) * 2 + i64::from(i32::MIN) + 14),
        None,
        Some(i64::from(i32::MIN)),
        Some(i64::from(i32::MAX) + 7),
        None,
        None,
    ]).into_array();
    assert_arrays_eq!(&result, &expected, &mut ctx);
    Ok(())
}


#[test]
fn dictionary_code_widths_masks_and_sliced_ranges_match_plain_lists() -> VortexResult<()> {
    use vortex::array::arrays::DictArray;
    macro_rules! check_width {
        ($code:ty) => {
            for null_codes in [false, true] {
                for null_values in [false, true] {
                    let dictionary: Vec<Option<i32>> = (0..17).map(|i| {
                        if null_values && i % 4 == 0 { None }
                        else { Some((i - 8) * 12345) }
                    }).collect();
                    let codes: Vec<Option<$code>> = (0..257).map(|i| {
                        if null_codes && i % 7 == 0 { None }
                        else { Some(((i * 13) % 17) as $code) }
                    }).collect();
                    let spans = [(200usize, 31usize), (19, 70), (0, 0), (255, 2), (77, 40)];
                    let valid = [true, true, true, true, false];
                    let expected = spans.iter().zip(valid).map(|(&(offset, len), valid)| {
                        if !valid { return None; }
                        let flattened = codes[offset..offset+len].iter().filter_map(|code| {
                            code.and_then(|c| dictionary[c as usize]).map(i64::from)
                        }).collect::<Vec<_>>();
                        if flattened.is_empty() { None } else { Some(flattened.into_iter().sum::<i64>()) }
                    }).collect::<Vec<_>>();
                    let elements = DictArray::try_new(
                        PrimitiveArray::from_option_iter(codes).into_array(),
                        PrimitiveArray::from_option_iter(dictionary).into_array(),
                    )?.into_array();
                    let dtype = elements.dtype().clone();
                    let groups = ListViewArray::try_new(
                        elements,
                        PrimitiveArray::from_iter(spans.iter().map(|&(o,_)| o as i32)).into_array(),
                        PrimitiveArray::from_iter(spans.iter().map(|&(_,n)| n as i32)).into_array(),
                        Validity::from_iter(valid),
                    )?.into_array();
                    let mut ctx = VortexSession::default().create_execution_ctx();
                    let mut accumulator = GroupedAccumulator::try_new(SumV2, NumericalAggregateOpts::default(), dtype)?;
                    accumulator.accumulate_list(&groups, &mut ctx)?;
                    let result = accumulator.finish()?;
                    let expected = PrimitiveArray::from_option_iter(expected).into_array();
                    assert_arrays_eq!(&result, &expected, &mut ctx);
                }
            }
        };
    }
    check_width!(u8);
    check_width!(u16);
    check_width!(u32);
    check_width!(u64);
    check_width!(i8);
    check_width!(i16);
    check_width!(i32);
    check_width!(i64);
    Ok(())
}

#[test]
fn list_field_sum_preserves_nested_logical_nulls_and_zero_identity() -> VortexResult<()> {
    use vortex::array::arrays::{DictArray, StructArray};
    use vortex::dtype::FieldNames;
    use vortex::expr::{root, list_sum, fill_null, mask, is_not_null, lit};
    let values = DictArray::try_new(
        buffer![0u8,1,2,3,4,5].into_array(),
        PrimitiveArray::from_option_iter([Some(10i32),Some(20),None,Some(40),Some(50),Some(60)]).into_array(),
    )?.into_array();
    let entry = StructArray::try_new(
        FieldNames::from(["DP"]), vec![values], 6,
        Validity::from_iter([true,false,true,true,true,true]),
    )?.into_array();
    let elements = StructArray::try_new(
        FieldNames::from(["entry"]), vec![entry], 6,
        Validity::from_iter([true,true,true,false,true,true]),
    )?.into_array();
    let lists = ListViewArray::try_new(
        elements, buffer![0i32,3,0,0].into_array(), buffer![3i32,3,0,6].into_array(),
        Validity::from_iter([true,true,true,false]),
    )?.into_array();
    let field = vortex::array::scalar_fn::fns::list_field::list_field(
        root(), lists.dtype(), &["entry".into(), "DP".into()],
    )?;
    let expression=mask(fill_null(list_sum(field),lit(0_i64)),is_not_null(root()));
    let mut ctx=VortexSession::default().create_execution_ctx();
    let result=lists.apply(&expression)?;
    let expected=PrimitiveArray::from_option_iter([Some(10i64),Some(110),Some(0),None]).into_array();
    assert_arrays_eq!(&result,&expected,&mut ctx);
    Ok(())
}
