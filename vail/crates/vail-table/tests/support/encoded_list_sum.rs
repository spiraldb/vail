//! Public differential checks for encoded grouped reductions.
use vortex::array::IntoArray;
use vortex::array::aggregate_fn::fns::sum_v2::SumV2;
use vortex::array::aggregate_fn::{
    DynGroupedAccumulator, GroupedAccumulator, NumericalAggregateOpts,
};
use vortex::array::arrays::{ListViewArray, PrimitiveArray};
use vortex::array::validity::Validity;
use vortex::buffer::buffer;
use vortex::dtype::{DType, Nullability::Nullable, PType};
use vortex::error::VortexResult;
use vortex::{VortexSessionDefault, array::VortexSessionExecute, session::VortexSession};

#[test]
fn grouped_sum_with_encoded_logical_mask_matches_nullable_values() -> VortexResult<()> {
    use vortex::array::arrays::{BoolArray, DictArray};
    use vortex::array::builtins::ArrayBuiltins;
    let values = DictArray::try_new(
        buffer![0u8, 1, 2, 0].into_array(),
        PrimitiveArray::from_option_iter([Some(i32::MAX), Some(7), None]).into_array(),
    )?
    .into_array();
    let logical_mask = DictArray::try_new(
        buffer![0u8, 1, 0, 0].into_array(),
        BoolArray::from_iter([true, false]).into_array(),
    )?
    .into_array();
    let groups = ListViewArray::try_new(
        values.mask(logical_mask)?,
        buffer![0u32, 2, 1].into_array(),
        buffer![2u32, 2, 1].into_array(),
        Validity::NonNullable,
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut accumulator = GroupedAccumulator::try_new(
        SumV2,
        NumericalAggregateOpts::default(),
        DType::Primitive(PType::I32, Nullable),
    )?;
    accumulator.accumulate_list(&groups, &mut ctx)?;
    let result = accumulator.finish()?;
    let expected = PrimitiveArray::from_option_iter([
        Some(i64::from(i32::MAX)),
        Some(i64::from(i32::MAX)),
        None,
    ])
    .into_array();
    assert_arrays_eq(&result, &expected, &mut ctx);
    Ok(())
}

#[test]
fn nullable_dictionary_sizes_and_reuse_match_independent_sums() -> VortexResult<()> {
    use vortex::array::arrays::DictArray;
    for cardinality in [19usize, 3000] {
        for reuse in [1usize, 9] {
            let dictionary = (0..cardinality)
                .map(|i| (i % 11 != 0).then_some(i as i32 - 19))
                .collect::<Vec<_>>();
            let codes = (0..cardinality * reuse)
                .map(|i| (i % 7 != 0).then_some(((i * 13 + 7) % cardinality) as u32))
                .collect::<Vec<_>>();
            let spans = [
                (0, codes.len()),
                (codes.len() / 3, codes.len() / 3),
                (codes.len(), 0),
            ];
            let expected = spans
                .iter()
                .map(|&(offset, len)| {
                    let present = codes[offset..offset + len]
                        .iter()
                        .filter_map(|code| code.and_then(|i| dictionary[i as usize]))
                        .collect::<Vec<_>>();
                    (!present.is_empty()).then(|| present.into_iter().map(i64::from).sum::<i64>())
                })
                .collect::<Vec<_>>();
            let elements = DictArray::try_new(
                PrimitiveArray::from_option_iter(codes).into_array(),
                PrimitiveArray::from_option_iter(dictionary).into_array(),
            )?
            .into_array();
            let groups = ListViewArray::try_new(
                elements,
                PrimitiveArray::from_iter(spans.iter().map(|&(offset, _)| offset as u32))
                    .into_array(),
                PrimitiveArray::from_iter(spans.iter().map(|&(_, len)| len as u32)).into_array(),
                Validity::NonNullable,
            )?
            .into_array();
            let mut ctx = VortexSession::default().create_execution_ctx();
            let mut accumulator = GroupedAccumulator::try_new(
                SumV2,
                NumericalAggregateOpts::default(),
                DType::Primitive(PType::I32, Nullable),
            )?;
            accumulator.accumulate_list(&groups, &mut ctx)?;
            assert_arrays_eq(
                &accumulator.finish()?,
                &PrimitiveArray::from_option_iter(expected).into_array(),
                &mut ctx,
            );
        }
    }
    Ok(())
}

#[test]
fn sliced_list_field_preserves_logical_masks_and_ignores_hidden_owner_rows() -> VortexResult<()> {
    use vortex::array::arrays::{DictArray, StructArray};
    use vortex::dtype::FieldNames;
    use vortex::expr::{fill_null, is_not_null, list_sum, lit, mask, root};
    let owner = 8199usize;
    let values = DictArray::try_new(
        PrimitiveArray::from_iter((0..owner).map(|i| (i % 102) as u16)).into_array(),
        PrimitiveArray::from_option_iter((0..102).map(|i| (i % 13 != 0).then_some(i - 37)))
            .into_array(),
    )?
    .into_array();
    let entry = StructArray::try_new(
        FieldNames::from(["DP"]),
        vec![values],
        owner,
        Validity::from_iter((0..owner).map(|i| i % 11 != 0)),
    )?
    .into_array();
    let elements = StructArray::try_new(
        FieldNames::from(["value"]),
        vec![entry],
        owner,
        Validity::from_iter((0..owner).map(|i| i % 17 != 0)),
    )?
    .into_array();
    let list = ListViewArray::try_new(
        elements,
        PrimitiveArray::from_iter([owner as u32 - 256, owner as u32 - 128, 0, 0]).into_array(),
        PrimitiveArray::from_iter([128u32, 128, 0, owner as u32 - 1]).into_array(),
        Validity::from_iter([true, true, true, false]),
    )?
    .into_array();
    let leaf = vortex::array::scalar_fn::fns::list_field::list_field(
        root(),
        list.dtype(),
        &["value".into(), "DP".into()],
    )?;
    let expression = mask(fill_null(list_sum(leaf), lit(0_i64)), is_not_null(root()));
    let expected = [owner - 256, owner - 128].map(|start| {
        Some(
            (start..start + 128)
                .filter(|i| i % 11 != 0 && i % 17 != 0 && (i % 102) % 13 != 0)
                .map(|i| (i % 102) as i64 - 37)
                .sum::<i64>(),
        )
    });
    let mut ctx = VortexSession::default().create_execution_ctx();
    assert_arrays_eq(
        &list.clone().apply(&expression)?,
        &PrimitiveArray::from_option_iter([expected[0], expected[1], Some(0), None]).into_array(),
        &mut ctx,
    );
    let empty = list.slice(0..0)?;
    assert_eq!(
        empty
            .apply(&expression)?
            .execute::<PrimitiveArray>(&mut ctx)?
            .len(),
        0
    );
    Ok(())
}

#[test]
fn grouped_dictionary_preserves_code_value_and_list_nulls() -> VortexResult<()> {
    use vortex::array::arrays::DictArray;
    let codes = PrimitiveArray::from_option_iter([
        Some(0u8),
        Some(1),
        None,
        Some(2),
        Some(3),
        Some(0),
        Some(3),
    ])
    .into_array();
    let values = PrimitiveArray::from_option_iter([Some(i32::MAX), None, Some(i32::MIN), Some(7)])
        .into_array();
    let elements = DictArray::try_new(codes, values)?.into_array();
    // Overlapping/reordered ranges exercise ListView rather than only List offsets.
    let groups = ListViewArray::try_new(
        elements,
        buffer![0i32, 1, 3, 4, 0, 6].into_array(),
        buffer![7i32, 2, 1, 2, 0, 1].into_array(),
        Validity::from_iter([true, true, true, true, true, false]),
    )?
    .into_array();
    let mut accumulator = GroupedAccumulator::try_new(
        SumV2,
        NumericalAggregateOpts::default(),
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
    ])
    .into_array();
    assert_arrays_eq(&result, &expected, &mut ctx);
    Ok(())
}

#[test]
fn dictionary_code_widths_masks_and_sliced_ranges_match_plain_lists() -> VortexResult<()> {
    use vortex::array::arrays::DictArray;
    macro_rules! check_width {
        ($code:ty) => {
            for (null_codes, null_modulus) in [(false, 7), (true, 7), (true, 37)] {
                for null_values in [false, true] {
                    let dictionary: Vec<Option<i32>> = (0..17)
                        .map(|i| {
                            if null_values && i % 4 == 0 {
                                None
                            } else {
                                Some((i - 8) * 12345)
                            }
                        })
                        .collect();
                    let codes: Vec<Option<$code>> = (0..257)
                        .map(|i| {
                            if null_codes && i % null_modulus == 0 {
                                None
                            } else {
                                Some(((i * 13) % 17) as $code)
                            }
                        })
                        .collect();
                    let spans = [(200usize, 31usize), (19, 70), (0, 0), (255, 2), (77, 40)];
                    let valid = [true, true, true, true, false];
                    let expected = spans
                        .iter()
                        .zip(valid)
                        .map(|(&(offset, len), valid)| {
                            if !valid {
                                return None;
                            }
                            let flattened = codes[offset..offset + len]
                                .iter()
                                .filter_map(|code| {
                                    code.and_then(|c| dictionary[c as usize]).map(i64::from)
                                })
                                .collect::<Vec<_>>();
                            if flattened.is_empty() {
                                None
                            } else {
                                Some(flattened.into_iter().sum::<i64>())
                            }
                        })
                        .collect::<Vec<_>>();
                    // Preserve a non-byte-aligned dictionary validity slice,
                    // independently of the visible code envelope's offset.
                    let sliced_values = PrimitiveArray::from_option_iter(
                        std::iter::repeat_n(Some(999_i32), 7)
                            .chain(dictionary.iter().copied())
                            .chain(std::iter::repeat_n(None, 3)),
                    )
                    .into_array()
                    .slice(7..24)?;
                    let elements = DictArray::try_new(
                        PrimitiveArray::from_option_iter(codes).into_array(),
                        sliced_values,
                    )?
                    .into_array();
                    let dtype = elements.dtype().clone();
                    let groups = ListViewArray::try_new(
                        elements,
                        PrimitiveArray::from_iter(spans.iter().map(|&(o, _)| o as i32))
                            .into_array(),
                        PrimitiveArray::from_iter(spans.iter().map(|&(_, n)| n as i32))
                            .into_array(),
                        Validity::from_iter(valid),
                    )?
                    .into_array();
                    let mut ctx = VortexSession::default().create_execution_ctx();
                    let mut accumulator = GroupedAccumulator::try_new(
                        SumV2,
                        NumericalAggregateOpts::default(),
                        dtype,
                    )?;
                    accumulator.accumulate_list(&groups, &mut ctx)?;
                    let result = accumulator.finish()?;
                    let expected = PrimitiveArray::from_option_iter(expected).into_array();
                    assert_arrays_eq(&result, &expected, &mut ctx);
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
    use vortex::expr::{fill_null, is_not_null, list_sum, lit, mask, root};
    let values = DictArray::try_new(
        buffer![0u8, 1, 2, 3, 4, 5].into_array(),
        PrimitiveArray::from_option_iter([
            Some(10i32),
            Some(20),
            None,
            Some(40),
            Some(50),
            Some(60),
        ])
        .into_array(),
    )?
    .into_array();
    let entry = StructArray::try_new(
        FieldNames::from(["DP"]),
        vec![values],
        6,
        Validity::from_iter([true, false, true, true, true, true]),
    )?
    .into_array();
    let elements = StructArray::try_new(
        FieldNames::from(["entry"]),
        vec![entry],
        6,
        Validity::from_iter([true, true, true, false, true, true]),
    )?
    .into_array();
    let lists = ListViewArray::try_new(
        elements,
        buffer![0i32, 3, 0, 0].into_array(),
        buffer![3i32, 3, 0, 6].into_array(),
        Validity::from_iter([true, true, true, false]),
    )?
    .into_array();
    let field = vortex::array::scalar_fn::fns::list_field::list_field(
        root(),
        lists.dtype(),
        &["entry".into(), "DP".into()],
    )?;
    let expression = mask(fill_null(list_sum(field), lit(0_i64)), is_not_null(root()));
    let mut ctx = VortexSession::default().create_execution_ctx();
    let result = lists.apply(&expression)?;
    let expected =
        PrimitiveArray::from_option_iter([Some(10i64), Some(110), Some(0), None]).into_array();
    assert_arrays_eq(&result, &expected, &mut ctx);
    Ok(())
}

fn assert_arrays_eq(
    result: &vortex::array::ArrayRef,
    expected: &vortex::array::ArrayRef,
    ctx: &mut vortex::array::ExecutionCtx,
) {
    assert_eq!(result.len(), expected.len());
    for i in 0..result.len() {
        assert_eq!(
            result.execute_scalar(i, ctx).unwrap(),
            expected.execute_scalar(i, ctx).unwrap(),
            "index {i}"
        );
    }
}

#[test]
fn dictionary_all_missing_codes_or_values_yield_missing_group_sums() -> VortexResult<()> {
    use vortex::array::arrays::DictArray;
    for codes_missing in [false, true] {
        let codes = PrimitiveArray::from_option_iter((0..64).map(|i| {
            if codes_missing {
                None
            } else {
                Some((i % 2) as u8)
            }
        }))
        .into_array();
        let values = PrimitiveArray::from_option_iter(
            (0..2).map(|i| if codes_missing { Some(i as i32) } else { None }),
        )
        .into_array();
        let dict = DictArray::try_new(codes, values)?.into_array();
        let dtype = dict.dtype().clone();
        let groups = ListViewArray::try_new(
            dict,
            buffer![0i32, 32, 0].into_array(),
            buffer![32i32, 32, 0].into_array(),
            Validity::NonNullable,
        )?
        .into_array();
        let mut accumulator =
            GroupedAccumulator::try_new(SumV2, NumericalAggregateOpts::default(), dtype)?;
        let mut ctx = VortexSession::default().create_execution_ctx();
        accumulator.accumulate_list(&groups, &mut ctx)?;
        let result = accumulator.finish()?;
        let expected = PrimitiveArray::from_option_iter([None::<i64>, None, None]).into_array();
        assert_arrays_eq(&result, &expected, &mut ctx);
    }
    Ok(())
}
