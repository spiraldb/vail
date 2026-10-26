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

#[test]
fn distant_list_groups_and_large_retained_dictionary_preserve_logical_values() -> VortexResult<()> {
    use vortex::array::arrays::{DictArray, StructArray};
    use vortex::dtype::FieldNames;
    use vortex::expr::{fill_null, is_not_null, list_sum, lit, mask, root};
    for large_dictionary in [false, true] {
        let owner = 100_003usize;
        let cardinality = if large_dictionary { owner } else { 103 };
        let dictionary = PrimitiveArray::from_option_iter(
            (0..cardinality).map(|i| (i % 13 != 0).then_some((i % 101) as i32 - 50)),
        )
        .into_array();
        let codes = PrimitiveArray::from_option_iter(
            (0..owner).map(|i| (i % 11 != 0).then_some((i % cardinality) as u32)),
        )
        .into_array();
        let values = DictArray::try_new(codes, dictionary)?.into_array();
        let elements = StructArray::try_new(
            FieldNames::from(["DP"]),
            vec![values],
            owner,
            Validity::from_iter((0..owner).map(|i| i % 17 != 0)),
        )?
        .into_array();
        // Distant, reversed, overlapping, empty and missing groups. Missing
        // groups deliberately retain a huge range whose payload must not matter.
        let ranges = [(owner - 9, 7), (1, 5), (3, 8), (0, 0), (0, owner)];
        let groups = ListViewArray::try_new(
            elements,
            PrimitiveArray::from_iter(ranges.map(|(offset, _)| offset as u32)).into_array(),
            PrimitiveArray::from_iter(ranges.map(|(_, len)| len as u32)).into_array(),
            Validity::from_iter([true, true, true, true, false]),
        )?
        .into_array();
        let field = vortex::array::scalar_fn::fns::list_field::list_field(
            root(),
            groups.dtype(),
            &["DP".into()],
        )?;
        let expr = mask(fill_null(list_sum(field), lit(0_i64)), is_not_null(root()));
        let expected = PrimitiveArray::from_option_iter(ranges.into_iter().enumerate().map(
            |(row, (offset, len))| {
                (row != 4).then(|| {
                    (offset..offset + len)
                        .filter(|i| i % 11 != 0 && i % 17 != 0 && (i % cardinality) % 13 != 0)
                        .map(|i| ((i % cardinality) % 101) as i64 - 50)
                        .sum::<i64>()
                })
            },
        ))
        .into_array();
        let mut ctx = VortexSession::default().create_execution_ctx();
        assert_arrays_eq(&groups.apply(&expr)?, &expected, &mut ctx);
    }
    Ok(())
}

// Complete-list grouped API: public values/schema tests, no plan/cache assertions.
#[test]
fn nullable_grouped_count_distinguishes_missing_empty_and_nan() -> VortexResult<()> {
    use vortex::array::aggregate_fn::fns::count::Count;
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    let elements =
        PrimitiveArray::from_option_iter([Some(1.0f64), Some(f64::NAN), None, Some(7.0)])
            .into_array();
    let groups = ListViewArray::try_new(
        elements,
        buffer![0u32, 2, 0, 2].into_array(),
        buffer![3u32, 0, 3, 1].into_array(),
        Validity::from_iter([true, true, false, true]),
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    for (options, expected) in [
        (
            NumericalAggregateOpts::include_nans(),
            vec![Some(2u64), Some(0), None, Some(0)],
        ),
        (
            NumericalAggregateOpts::default(),
            vec![Some(1u64), Some(0), None, Some(0)],
        ),
    ] {
        let mut acc = ListGroupedAccumulator::try_new(Count, options, groups.dtype().clone())?;
        acc.accumulate_list(&groups, &mut ctx)?;
        let result = acc.finish()?;
        assert_eq!(result.dtype(), &DType::Primitive(PType::U64, Nullable));
        assert_arrays_eq(
            &result,
            &PrimitiveArray::from_option_iter(expected).into_array(),
            &mut ctx,
        );
    }
    Ok(())
}

#[test]
fn declared_nullable_all_valid_count_keeps_nullable_schema() -> VortexResult<()> {
    use vortex::array::aggregate_fn::fns::count::Count;
    let groups = ListViewArray::try_new(
        buffer![1i32, 2].into_array(),
        buffer![0u32, 2].into_array(),
        buffer![2u32, 0].into_array(),
        Validity::AllValid,
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let aggregate = vortex::array::aggregate_fn::AggregateFn::new(
        Count,
        NumericalAggregateOpts::include_nans(),
    )
    .erased();
    let mut acc = aggregate.accumulator_for_list(groups.dtype())?;
    acc.accumulate_list(&groups, &mut ctx)?;
    let result = acc.finish()?;
    assert_eq!(result.dtype(), &DType::Primitive(PType::U64, Nullable));
    assert_arrays_eq(
        &result,
        &PrimitiveArray::from_option_iter([Some(2u64), Some(0)]).into_array(),
        &mut ctx,
    );
    // Scalar Count retains its non-null zero identity.
    use vortex::array::aggregate_fn::{Accumulator, DynAccumulator};
    let mut scalar = Accumulator::try_new(
        Count,
        NumericalAggregateOpts::include_nans(),
        DType::Primitive(PType::I32, Nullable),
    )?;
    scalar.accumulate(
        &PrimitiveArray::from_option_iter(Vec::<Option<i32>>::new()).into_array(),
        &mut ctx,
    )?;
    let scalar = scalar.finish()?;
    assert_eq!(
        scalar,
        vortex::scalar::Scalar::primitive(0u64, vortex::dtype::Nullability::NonNullable)
    );
    Ok(())
}

#[test]
fn nullable_grouped_sum_preserves_present_null_and_outer_null() -> VortexResult<()> {
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    let groups = ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(5i32), None]).into_array(),
        buffer![0u32, 1, 1, 0].into_array(),
        buffer![1u32, 0, 1, 1].into_array(),
        Validity::from_iter([true, true, true, false]),
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut acc = ListGroupedAccumulator::try_new(
        SumV2,
        NumericalAggregateOpts::default(),
        groups.dtype().clone(),
    )?;
    acc.accumulate_list(&groups, &mut ctx)?;
    let result = acc.finish()?;
    assert_arrays_eq(
        &result,
        &PrimitiveArray::from_option_iter([Some(5i64), None, None, None]).into_array(),
        &mut ctx,
    );
    Ok(())
}

#[test]
fn nullable_grouped_flush_pairs_multiple_batches_and_resets() -> VortexResult<()> {
    use vortex::array::aggregate_fn::fns::count::Count;
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    let first = ListViewArray::try_new(
        buffer![1i32, 2].into_array(),
        buffer![0u32, 0, 2].into_array(),
        buffer![2u32, 2, 0].into_array(),
        Validity::from_iter([true, false, true]),
    )?
    .into_array();
    let second = ListViewArray::try_new(
        buffer![9i32].into_array(),
        buffer![0u32, 1].into_array(),
        buffer![1u32, 0].into_array(),
        Validity::AllValid,
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut acc = ListGroupedAccumulator::try_new(
        Count,
        NumericalAggregateOpts::include_nans(),
        first.dtype().clone(),
    )?;
    acc.accumulate_list(&first, &mut ctx)?;
    acc.accumulate_list(&second, &mut ctx)?;
    let flushed = acc.flush_groups()?;
    // Reuse before finalizing the independent old typed partials.
    acc.accumulate_list(&second, &mut ctx)?;
    let later = acc.finish()?;
    assert_arrays_eq(
        &later,
        &PrimitiveArray::from_option_iter([Some(1u64), Some(0)]).into_array(),
        &mut ctx,
    );
    assert_arrays_eq(
        &flushed.finish()?,
        &PrimitiveArray::from_option_iter([Some(2u64), None, Some(0), Some(1), Some(0)])
            .into_array(),
        &mut ctx,
    );
    let empty = acc.finish()?;
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.dtype(), &DType::Primitive(PType::U64, Nullable));
    Ok(())
}

#[test]
fn nullable_grouped_all_missing_and_zero_rows_have_real_null_outputs() -> VortexResult<()> {
    use vortex::array::aggregate_fn::fns::count::Count;
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    let groups = ListViewArray::try_new(
        buffer![7i32].into_array(),
        buffer![0u32, 0].into_array(),
        buffer![1u32, 1].into_array(),
        Validity::AllInvalid,
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut acc = ListGroupedAccumulator::try_new(
        Count,
        NumericalAggregateOpts::include_nans(),
        groups.dtype().clone(),
    )?;
    acc.accumulate_list(&groups, &mut ctx)?;
    assert_arrays_eq(
        &acc.finish()?,
        &PrimitiveArray::from_option_iter([None::<u64>, None]).into_array(),
        &mut ctx,
    );
    let zero = groups.slice(0..0)?;
    acc.accumulate_list(&zero, &mut ctx)?;
    let result = acc.finish()?;
    assert_eq!(result.len(), 0);
    assert_eq!(result.dtype(), &DType::Primitive(PType::U64, Nullable));
    Ok(())
}

#[test]
fn nullable_grouped_fixed_size_slice_and_logical_ancestor_masks() -> VortexResult<()> {
    use vortex::array::aggregate_fn::fns::count::Count;
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    use vortex::array::arrays::{FixedSizeListArray, StructArray};
    use vortex::expr::{get_item, root};
    let parent = StructArray::try_from_iter_with_validity(
        [(
            "x",
            PrimitiveArray::from_option_iter([
                Some(10i32),
                Some(20),
                None,
                Some(40),
                Some(50),
                Some(60),
            ])
            .into_array(),
        )],
        Validity::from_iter([true, false, true, true, true, false]),
    )?
    .into_array();
    let field = get_item("x", root()).bind(parent.dtype())?;
    let logical = parent.apply_bound(&field)?;
    let groups =
        FixedSizeListArray::try_new(logical, 2, Validity::from_iter([false, true, true]), 3)?
            .into_array()
            .slice(1..3)?;
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut acc = ListGroupedAccumulator::try_new(
        Count,
        NumericalAggregateOpts::include_nans(),
        groups.dtype().clone(),
    )?;
    acc.accumulate_list(&groups, &mut ctx)?;
    assert_arrays_eq(
        &acc.finish()?,
        &PrimitiveArray::from_option_iter([Some(1u64), Some(1)]).into_array(),
        &mut ctx,
    );
    Ok(())
}

#[test]
fn nullable_grouped_boolean_sum_checks_raw_missing_before_normalization() -> VortexResult<()> {
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    use vortex::array::arrays::BoolArray;
    let groups = ListViewArray::try_new(
        BoolArray::from_iter([Some(true), Some(false), None]).into_array(),
        buffer![0u32, 2, 2, 0].into_array(),
        buffer![2u32, 0, 1, 2].into_array(),
        Validity::from_iter([true, true, true, false]),
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut acc = ListGroupedAccumulator::try_new(
        SumV2,
        NumericalAggregateOpts::include_nans(),
        groups.dtype().clone(),
    )?;
    acc.accumulate_list(&groups, &mut ctx)?;
    assert_arrays_eq(
        &acc.finish()?,
        &PrimitiveArray::from_option_iter([Some(1u64), None, None, None]).into_array(),
        &mut ctx,
    );
    Ok(())
}

#[test]
fn complete_list_api_rejects_different_declared_schema() -> VortexResult<()> {
    use vortex::array::aggregate_fn::fns::count::Count;
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    let nonnull = ListViewArray::try_new(
        buffer![1i32].into_array(),
        buffer![0u32].into_array(),
        buffer![1u32].into_array(),
        Validity::NonNullable,
    )?
    .into_array();
    let nullable = ListViewArray::try_new(
        buffer![1i32].into_array(),
        buffer![0u32].into_array(),
        buffer![1u32].into_array(),
        Validity::AllValid,
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut acc = ListGroupedAccumulator::try_new(
        Count,
        NumericalAggregateOpts::include_nans(),
        nonnull.dtype().clone(),
    )?;
    assert!(acc.accumulate_list(&nullable, &mut ctx).is_err());
    acc.accumulate_list(&nonnull, &mut ctx)?;
    assert_arrays_eq(
        &acc.finish()?,
        &PrimitiveArray::from_iter([1u64]).into_array(),
        &mut ctx,
    );
    Ok(())
}

#[test]
fn nullable_grouped_combined_mean_preserves_count_and_sum_pairing() -> VortexResult<()> {
    use vortex::array::aggregate_fn::combined::PairOptions;
    use vortex::array::aggregate_fn::fns::mean::Mean;
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    let groups = ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(2.0f64), None, Some(6.0)]).into_array(),
        buffer![0u32, 1, 0, 1].into_array(),
        buffer![3u32, 0, 3, 1].into_array(),
        Validity::from_iter([true, true, false, true]),
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let mut acc = ListGroupedAccumulator::try_new(
        Mean::combined(),
        PairOptions(
            NumericalAggregateOpts::default(),
            NumericalAggregateOpts::default(),
        ),
        groups.dtype().clone(),
    )?;
    acc.accumulate_list(&groups, &mut ctx)?;
    assert_arrays_eq(
        &acc.finish()?,
        &PrimitiveArray::from_option_iter([Some(4.0f64), None, None, None]).into_array(),
        &mut ctx,
    );
    Ok(())
}

/// Public API resource experiment. Timings are observations, never assertions.
/// The fixed output has four groups; input construction/compression, canonical
/// acquisition, expression binding, context construction and RSS reads are untimed.
#[test]
fn nullable_grouped_public_resource_windows() -> VortexResult<()> {
    use std::time::Instant;
    use vortex::array::Canonical;
    use vortex::array::aggregate_fn::{DynListGroupedAccumulator, ListGroupedAccumulator};
    use vortex::array::arrays::BoolArray;
    let owner = std::env::var("VAIL_GROUPED_PCO_OWNER")
        .ok()
        .map(|v| v.parse::<usize>().expect("resource owner is an integer"))
        .unwrap_or(10_000);
    assert!(owner >= 256);
    let session = VortexSession::default();
    let mut setup = session.create_execution_ctx();
    let primitive = PrimitiveArray::from_iter((0..owner).map(|i| (i % 101) as f64));
    let child =
        vortex::encodings::pco::Pco::from_primitive(primitive.as_view(), 0, 1024, &mut setup)?
            .into_array();
    drop(primitive);
    let missing = ListViewArray::try_new(
        child,
        buffer![17u32, 81, 145, 209].into_array(),
        buffer![3u32, 3, 3, 3].into_array(),
        Validity::AllInvalid,
    )?
    .into_array()
    .execute::<Canonical>(&mut setup)?
    .into_array();
    let mixed = ListViewArray::try_new(
        BoolArray::from_iter([Some(true), Some(false), None]).into_array(),
        buffer![0u32, 2, 2, 0].into_array(),
        buffer![2u32, 0, 1, 2].into_array(),
        Validity::from_iter([true, true, true, false]),
    )?
    .into_array()
    .execute::<Canonical>(&mut setup)?
    .into_array();
    for (name, input, expected) in [
        (
            "mixed_bool_sum",
            mixed,
            vec![Some(1.0f64), None, None, None],
        ),
        ("all_missing_pco_sum", missing, vec![None, None, None, None]),
    ] {
        let alias = input.clone();
        let original_dtype = input.dtype().clone();
        let element_dtype = match input.dtype() {
            DType::List(e, _) => e.as_ref().clone(),
            _ => panic!("canonical list"),
        };
        let mut observations = Vec::new();
        for (window, legacy) in [true, false, false, true].into_iter().enumerate() {
            let rss_before = std::process::Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()
                .ok()
                .and_then(|v| String::from_utf8(v.stdout).ok())
                .and_then(|s| s.trim().parse::<u64>().ok());
            let mut times = Vec::new();
            for iteration in 0..9 {
                let mut ctx = session.create_execution_ctx();
                let start = Instant::now();
                let result = if legacy {
                    let mut acc = GroupedAccumulator::try_new(
                        SumV2,
                        NumericalAggregateOpts::include_nans(),
                        element_dtype.clone(),
                    )?;
                    acc.accumulate_list(&input, &mut ctx)?;
                    acc.finish()?
                } else {
                    // Call the public wrapper directly so a strict-expression
                    // optimizer cannot fold an all-missing list to a constant.
                    let mut acc = ListGroupedAccumulator::try_new(
                        SumV2,
                        NumericalAggregateOpts::include_nans(),
                        input.dtype().clone(),
                    )?;
                    acc.accumulate_list(&input, &mut ctx)?;
                    acc.finish()?
                };
                let result = result.execute::<PrimitiveArray>(&mut ctx)?;
                let seconds = start.elapsed().as_secs_f64();
                assert_eq!(result.len(), 4);
                let result_type = if name == "mixed_bool_sum" {
                    PType::U64
                } else {
                    PType::F64
                };
                assert_eq!(result.dtype(), &DType::Primitive(result_type, Nullable));
                let valid = result.validity()?.execute_mask(4, &mut ctx)?;
                let actual = (0..4)
                    .map(|i| {
                        valid.value(i).then(|| {
                            if result_type == PType::U64 {
                                result.as_slice::<u64>()[i] as f64
                            } else {
                                result.as_slice::<f64>()[i]
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                // Raw outputs, no fill, post-mask or benchmark-specific normalization.
                assert_eq!(actual, expected);
                if iteration >= 3 {
                    times.push(seconds);
                }
            }
            assert_eq!(alias.dtype(), &original_dtype);
            assert_eq!(alias.len(), 4);
            if name == "all_missing_pco_sum" {
                for i in 0..4 {
                    assert!(alias.execute_scalar(i, &mut setup)?.is_null());
                }
            } else {
                let expected_input = ListViewArray::try_new(
                    BoolArray::from_iter([Some(true), Some(false), None]).into_array(),
                    buffer![0u32, 2, 2, 0].into_array(),
                    buffer![2u32, 0, 1, 2].into_array(),
                    Validity::from_iter([true, true, true, false]),
                )?
                .into_array();
                assert_arrays_eq(&alias, &expected_input, &mut setup);
            }
            let rss_after = std::process::Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()
                .ok()
                .and_then(|v| String::from_utf8(v.stdout).ok())
                .and_then(|s| s.trim().parse::<u64>().ok());
            observations.push(
                serde_json::json!({"window":window,"legacy":legacy,"seconds":times,
                "rss_before_kib":rss_before,"rss_after_kib":rss_after}),
            );
        }
        println!(
            "GROUPED_RESOURCE {}",
            serde_json::json!({"case":name,"owner":owner,"output_groups":4,
            "canonical_acquired_before_timing":true,"raw_expected":expected,"windows":observations,
            "limitation":"post-canonical in-memory API experiment; no pre-canonical bound, IO or leak claim"})
        );
    }
    Ok(())
}

fn element_validity_fixture() -> VortexResult<vortex::array::ArrayRef> {
    use vortex::array::arrays::StructArray;
    let leaf = PrimitiveArray::from_option_iter([
        Some(f64::NAN),
        None,
        Some(3.0),
        Some(4.0),
        Some(5.0),
        Some(6.0),
        Some(7.0),
        Some(8.0),
    ])
    .into_array();
    let selected = StructArray::try_from_iter_with_validity(
        [("leaf", leaf)],
        Validity::from_iter([true, true, false, true, true, true, true, true]),
    )?
    .into_array();
    let entries = StructArray::try_from_iter_with_validity(
        [
            ("selected", selected),
            (
                "spare",
                buffer![10i32, 20, 30, 40, 50, 60, 70, 80].into_array(),
            ),
        ],
        Validity::from_iter([true, true, true, false, true, true, true, true]),
    )?
    .into_array();
    Ok(ListViewArray::try_new(
        entries,
        buffer![0u32, 2, 4, 0].into_array(),
        buffer![4u32, 0, 3, 4].into_array(),
        Validity::from_iter([true, true, true, false]),
    )?
    .into_array())
}

#[test]
fn element_validity_map_has_true_boolean_type_and_logical_ancestor_masks() -> VortexResult<()> {
    use vortex::array::arrays::BoolArray;
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    use vortex::expr::root;
    let input = element_validity_fixture()?;
    let expression =
        list_element_validity(root(), input.dtype(), &["selected".into(), "leaf".into()])?;
    let expected_dtype = DType::List(
        std::sync::Arc::new(DType::Bool(vortex::dtype::Nullability::NonNullable)),
        Nullable,
    );
    let bound = expression.bind(input.dtype())?;
    assert_eq!(bound.dtype(), &expected_dtype);
    let actual = input.apply_bound(&bound)?;
    assert_eq!(actual.dtype(), &expected_dtype);
    let expected = ListViewArray::try_new(
        BoolArray::from_iter([true, false, false, false, true, true, true, true]).into_array(),
        buffer![0u32, 2, 4, 0].into_array(),
        buffer![4u32, 0, 3, 4].into_array(),
        Validity::from_iter([true, true, true, false]),
    )?
    .into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    assert_arrays_eq(&actual, &expected, &mut ctx);
    Ok(())
}

#[test]
fn element_validity_sum_map_and_count_identity_preserve_outer_missingness() -> VortexResult<()> {
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    use vortex::expr::{fill_null, is_not_null, list_sum, lit, mask, root};
    let input = element_validity_fixture()?;
    let mapped = list_element_validity(root(), input.dtype(), &["selected".into(), "leaf".into()])?;
    let raw_sum = list_sum(mapped);
    let mut ctx = VortexSession::default().create_execution_ctx();
    assert_arrays_eq(
        &input.clone().apply_bound(&raw_sum.bind(input.dtype())?)?,
        &PrimitiveArray::from_option_iter([Some(1u64), None, Some(3), None]).into_array(),
        &mut ctx,
    );
    // This explicit count-style identity applies to a PRESENT empty list;
    // the independent outer logical mask retains a missing list.
    let count = mask(fill_null(raw_sum, lit(0u64)), is_not_null(root()));
    assert_arrays_eq(
        &input.clone().apply_bound(&count.bind(input.dtype())?)?,
        &PrimitiveArray::from_option_iter([Some(1u64), Some(0), Some(3), None]).into_array(),
        &mut ctx,
    );
    Ok(())
}

#[test]
fn element_validity_shared_body_raw_sibling_and_distinct_paths_keep_real_types() -> VortexResult<()>
{
    use vortex::array::builtins::ArrayBuiltins;
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    use vortex::expr::{list_sum, pack, root};
    let input = element_validity_fixture()?;
    let body = list_element_validity(root(), input.dtype(), &["selected".into(), "leaf".into()])?;
    let root_presence = list_element_validity(root(), input.dtype(), &[])?;
    let combined = pack(
        [
            ("mapped", body.clone()),
            ("sum", list_sum(body)),
            ("raw", root()),
            ("root_presence_sum", list_sum(root_presence)),
        ],
        vortex::dtype::Nullability::NonNullable,
    )
    .bind(input.dtype())?;
    let actual = input.clone().apply_bound(&combined)?;
    let mut ctx = VortexSession::default().create_execution_ctx();
    assert_arrays_eq(&actual.get_item("raw")?, &input, &mut ctx);
    assert_arrays_eq(
        &actual.get_item("sum")?,
        &PrimitiveArray::from_option_iter([Some(1u64), None, Some(3), None]).into_array(),
        &mut ctx,
    );
    assert_arrays_eq(
        &actual.get_item("root_presence_sum")?,
        &PrimitiveArray::from_option_iter([Some(3u64), None, Some(3), None]).into_array(),
        &mut ctx,
    );
    assert!(
        matches!(actual.get_item("mapped")?.dtype(), DType::List(element, _) if element.as_ref() == &DType::Bool(vortex::dtype::Nullability::NonNullable))
    );
    Ok(())
}

#[test]
fn element_validity_serialization_retains_exact_original_schema_and_no_capture() -> VortexResult<()>
{
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    use vortex::expr::proto::{ExprSerializeProtoExt, deserialize_expr_proto};
    use vortex::expr::root;
    let input = element_validity_fixture()?;
    let expression =
        list_element_validity(root(), input.dtype(), &["selected".into(), "leaf".into()])?;
    let session = VortexSession::default();
    let restored = deserialize_expr_proto(&expression.serialize_proto()?, &session)?;
    assert_eq!(expression, restored);
    let mut ctx = session.create_execution_ctx();
    assert_arrays_eq(
        &input.clone().apply_bound(&restored.bind(input.dtype())?)?,
        &input
            .clone()
            .apply_bound(&expression.bind(input.dtype())?)?,
        &mut ctx,
    );
    // Same positions/nullability but a different field name is a different
    // original schema, not a valid rebinding of a captured prepared path.
    let DType::List(element, nullable) = input.dtype() else {
        unreachable!()
    };
    let DType::Struct(fields, entry_nullable) = element.as_ref() else {
        unreachable!()
    };
    let renamed = DType::List(
        std::sync::Arc::new(DType::Struct(
            vortex::dtype::StructFields::new(
                vortex::dtype::FieldNames::from(["renamed", "spare"]),
                fields.fields().collect(),
            ),
            *entry_nullable,
        )),
        *nullable,
    );
    assert!(restored.bind(&renamed).is_err());
    Ok(())
}

/// Public writer/reader check: all expected values are independent of the map
/// implementation, and nullable ancestors are observed inside a present list.
#[tokio::test]
async fn element_validity_file_reader_preserves_boolean_domain_and_sum() -> VortexResult<()> {
    use futures::TryStreamExt;
    use vortex::array::arrays::BoolArray;
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::expr::{list_sum, root};
    use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
    let input = element_validity_fixture()?;
    let dtype = input.dtype().clone();
    let session = crate::element_validity_reader_io::structural_session()?;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("element-validity.vortex");
    session
        .write_options()
        .with_strategy(
            vortex::file::WriteStrategyBuilder::default()
                .with_list_layout()
                .build(),
        )
        .write(
            tokio::fs::File::create(&path).await.unwrap(),
            ArrayStreamAdapter::new(dtype.clone(), futures::stream::iter([Ok(input)])),
        )
        .await?;
    let file = session.open_options().open_path(&path).await?;
    let mapped = list_element_validity(root(), &dtype, &["selected".into(), "leaf".into()])?;
    let expected_map = ListViewArray::try_new(
        BoolArray::from_iter([true, false, false, false, true, true, true, true]).into_array(),
        buffer![0u32, 2, 4, 0].into_array(),
        buffer![4u32, 0, 3, 4].into_array(),
        Validity::from_iter([true, true, true, false]),
    )?
    .into_array();
    for (expression, expected) in [
        (mapped.clone(), expected_map),
        (
            list_sum(mapped),
            PrimitiveArray::from_option_iter([Some(1u64), None, Some(3), None]).into_array(),
        ),
    ] {
        let bound = expression.bind(file.dtype())?;
        let chunks = file
            .scan()?
            .with_projection(bound.clone())
            .into_array_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        let mut ctx = session.create_execution_ctx();
        let mut offset = 0;
        for chunk in chunks {
            assert_eq!(chunk.dtype(), bound.dtype());
            assert_arrays_eq(
                &chunk,
                &expected.slice(offset..offset + chunk.len())?,
                &mut ctx,
            );
            offset += chunk.len();
        }
        assert_eq!(offset, expected.len());
    }
    Ok(())
}

#[tokio::test]
async fn element_validity_file_reader_shared_body_and_raw_sibling_keep_original_values()
-> VortexResult<()> {
    use futures::TryStreamExt;
    use vortex::array::builtins::ArrayBuiltins;
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::expr::{list_sum, pack, root};
    use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
    let input = element_validity_fixture()?;
    let dtype = input.dtype().clone();
    let session = crate::element_validity_reader_io::structural_session()?;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("element-validity-siblings.vortex");
    session
        .write_options()
        .with_strategy(
            vortex::file::WriteStrategyBuilder::default()
                .with_list_layout()
                .build(),
        )
        .write(
            tokio::fs::File::create(&path).await.unwrap(),
            ArrayStreamAdapter::new(dtype.clone(), futures::stream::iter([Ok(input.clone())])),
        )
        .await?;
    let file = session.open_options().open_path(&path).await?;
    for raw in [false, true] {
        let body = list_element_validity(root(), &dtype, &["selected".into(), "leaf".into()])?;
        let mut fields = vec![
            ("first", list_sum(body.clone())),
            ("second", list_sum(body)),
        ];
        if raw {
            fields.push(("raw", root()));
        }
        let expression =
            pack(fields, vortex::dtype::Nullability::NonNullable).bind(file.dtype())?;
        let chunks = file
            .scan()?
            .with_projection(expression.clone())
            .into_array_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        let sums = PrimitiveArray::from_option_iter([Some(1u64), None, Some(3), None]).into_array();
        let mut ctx = session.create_execution_ctx();
        let mut offset = 0;
        for chunk in chunks {
            assert_eq!(chunk.dtype(), expression.dtype());
            let expected = sums.slice(offset..offset + chunk.len())?;
            assert_arrays_eq(&chunk.get_item("first")?, &expected, &mut ctx);
            assert_arrays_eq(&chunk.get_item("second")?, &expected, &mut ctx);
            if raw {
                assert_arrays_eq(
                    &chunk.get_item("raw")?,
                    &input.slice(offset..offset + chunk.len())?,
                    &mut ctx,
                );
            }
            offset += chunk.len();
        }
        assert_eq!(offset, 4);
    }
    Ok(())
}

#[tokio::test]
async fn element_validity_file_reader_distinct_paths_and_bounded_mask_are_correct()
-> VortexResult<()> {
    use vortex::array::MaskFuture;
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::expr::{list_sum, pack, root};
    use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
    let input = element_validity_fixture()?;
    let session = crate::element_validity_reader_io::structural_session()?;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("distinct-bounded.vortex");
    session
        .write_options()
        .with_strategy(
            vortex::file::WriteStrategyBuilder::default()
                .with_list_layout()
                .build(),
        )
        .write(
            tokio::fs::File::create(&path).await.unwrap(),
            ArrayStreamAdapter::new(
                input.dtype().clone(),
                futures::stream::iter([Ok(input.clone())]),
            ),
        )
        .await?;
    let file = session.open_options().open_path(&path).await?;
    let leaf = list_element_validity(root(), input.dtype(), &["selected".into(), "leaf".into()])?;
    let parent = list_element_validity(root(), input.dtype(), &[])?;
    let distinct = pack(
        [
            ("leaf", list_sum(leaf.clone())),
            ("parent", list_sum(parent)),
        ],
        vortex::dtype::Nullability::NonNullable,
    )
    .bind(file.dtype())?;
    let reader = file.layout_reader()?;
    let mut ctx = session.create_execution_ctx();
    let actual = reader
        .projection_evaluation(&(0..4), &distinct, MaskFuture::new_true(4))?
        .await?;
    use vortex::array::builtins::ArrayBuiltins;
    assert_arrays_eq(
        &actual.get_item("leaf")?,
        &PrimitiveArray::from_option_iter([Some(1u64), None, Some(3), None]).into_array(),
        &mut ctx,
    );
    assert_arrays_eq(
        &actual.get_item("parent")?,
        &PrimitiveArray::from_option_iter([Some(3u64), None, Some(3), None]).into_array(),
        &mut ctx,
    );
    let sum = list_sum(leaf).bind(file.dtype())?;
    let bounded = reader
        .projection_evaluation(
            &(1..4),
            &sum,
            MaskFuture::ready(vortex::mask::Mask::from_iter([false, true, true])),
        )?
        .await?;
    assert_eq!(bounded.dtype(), sum.dtype());
    assert_arrays_eq(
        &bounded,
        &PrimitiveArray::from_option_iter([Some(3u64), None]).into_array(),
        &mut ctx,
    );
    Ok(())
}

#[tokio::test]
async fn element_validity_reader_preparation_accepts_shared_expression_dags() -> VortexResult<()> {
    use vortex::array::MaskFuture;
    use vortex::array::scalar_fn::fns::list_element_validity::list_element_validity;
    use vortex::array::scalar_fn::{
        ScalarFnVTableExt,
        fns::{binary::Binary, operators::Operator},
    };
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::expr::{list_sum, root};
    use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
    let input = element_validity_fixture()?;
    let session = crate::element_validity_reader_io::structural_session()?;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("shared-dag.vortex");
    session
        .write_options()
        .with_strategy(
            vortex::file::WriteStrategyBuilder::default()
                .with_list_layout()
                .build(),
        )
        .write(
            tokio::fs::File::create(&path).await.unwrap(),
            ArrayStreamAdapter::new(
                input.dtype().clone(),
                futures::stream::iter([Ok(input.clone())]),
            ),
        )
        .await?;
    let file = session.open_options().open_path(&path).await?;
    let reader = file.layout_reader()?;
    let body = list_element_validity(root(), input.dtype(), &["selected".into(), "leaf".into()])?;
    let mut expression = list_sum(body).bind(file.dtype())?;
    for depth in 1..=30 {
        expression = Binary.try_new_bound_expr(Operator::Add, [expression.clone(), expression])?;
        if depth == 6 {
            let actual = reader
                .projection_evaluation(&(0..4), &expression, MaskFuture::new_true(4))?
                .await?;
            let mut ctx = session.create_execution_ctx();
            assert_arrays_eq(
                &actual,
                &PrimitiveArray::from_option_iter([Some(64u64), None, Some(192), None])
                    .into_array(),
                &mut ctx,
            );
        }
        if [8, 16, 30].contains(&depth) {
            // Public preparation only: dropping the unevaluated future deliberately
            // separates DAG preparation scalability from repeated arithmetic work.
            let start = std::time::Instant::now();
            let prepared =
                reader.projection_evaluation(&(0..4), &expression, MaskFuture::new_true(4))?;
            drop(prepared);
            eprintln!(
                "ELEMENT_VALIDITY_DAG_PREPARATION depth={depth} seconds={}",
                start.elapsed().as_secs_f64()
            );
        }
    }
    Ok(())
}
