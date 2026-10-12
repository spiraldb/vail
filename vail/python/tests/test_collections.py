"""Hail-compatible collection expressions supported by Vail."""

import math

import pytest
import vail as hl


def test_expression_valued_array_is_row_major_and_typed():
    table = hl.Table.parallelize(
        [{"a": 1, "b": 10}, {"a": 2, "b": 20}],
        schema=hl.tstruct(a=hl.tint32, b=hl.tint32),
    )

    result = table.annotate(values=[table.a, table.b]).values

    assert result.dtype == hl.tarray(hl.tint32)
    assert result.collect() == [[1, 10], [2, 20]]


def test_index_slice_length_preserve_outer_and_element_missingness():
    table = hl.Table.parallelize(
        [{"a": None}, {"a": []}, {"a": [None, 7, 9]}],
        schema=hl.tstruct(a=hl.tarray(hl.tint32)),
    )

    assert table.a.length().collect() == [None, 0, 3]
    assert table.a[::-1].collect() == [None, [], [9, 7, None]]

    nonempty = hl.Table.parallelize(
        [{"a": None}, {"a": [None, 7, 9]}],
        schema=hl.tstruct(a=hl.tarray(hl.tint32)),
    )
    assert nonempty.a[-1].collect() == [None, 9]
    assert nonempty.a[0].collect() == [None, None]
    with pytest.raises(RuntimeError, match="(?i)bound|index"):
        table.a[-1].collect()


def test_reductions_use_typed_identities_ignore_missing_and_wrap_in_type():
    table = hl.Table.parallelize(
        [
            {"a": []},
            {"a": [None]},
            {"a": [2**31 - 1, 1]},
            {"a": [None, 3, 4]},
            {"a": None},
        ],
        schema=hl.tstruct(a=hl.tarray(hl.tint32)),
    )

    assert hl.sum(table.a).dtype == hl.tint32
    assert hl.sum(table.a).collect() == [0, 0, -(2**31), 7, None]
    assert hl.product(table.a).collect() == [1, 1, (2**31 - 1), 12, None]
    assert hl.min(table.a).collect() == [None, None, 1, 3, None]
    assert hl.max(table.a).collect() == [None, None, 2**31 - 1, 4, None]
    assert hl.sum(table.a, filter_missing=False).collect() == [0, None, -(2**31), None, None]


def test_float_min_max_follow_hail_nan_and_signed_zero_rules():
    table = hl.Table.parallelize(
        [
            {"a": [0.0, -0.0]},
            {"a": [-0.0, 0.0]},
            {"a": [1.0, float("nan")]},
            {"a": [float("nan"), 1.0]},
        ],
        schema=hl.tstruct(a=hl.tarray(hl.tfloat64)),
    )

    minima = hl.min(table.a).collect()
    maxima = hl.max(table.a).collect()
    assert [math.copysign(1.0, value) for value in minima[:2]] == [-1.0, -1.0]
    assert [math.copysign(1.0, value) for value in maxima[:2]] == [1.0, 1.0]
    assert all(math.isnan(value) for value in minima[2:] + maxima[2:])


def test_invalid_index_and_slice_types_fail_during_construction():
    table = hl.Table.parallelize(
        [{"a": [1]}], schema=hl.tstruct(a=hl.tarray(hl.tint32))
    )

    with pytest.raises(TypeError, match="int32"):
        _ = table.a[1.0]
    with pytest.raises(TypeError, match="int32"):
        _ = table.a[::1.0]
    with pytest.raises(TypeError, match="numeric"):
        hl.sum(hl.literal([True]))
    with pytest.raises(TypeError, match="numeric"):
        hl.min(table.a, table.a)


def test_struct_field_named_length_is_not_shadowed_by_array_method():
    table = hl.Table.parallelize(
        [{"s": {"length": 7}}],
        schema=hl.tstruct(s=hl.tstruct(length=hl.tint32)),
    )

    assert table.s.length.collect() == [7]


def test_variadic_min_max_use_existing_numeric_array_reductions():
    assert hl.eval(hl.min(3, 1, 2)) == 1
    assert hl.eval(hl.max(3, 1, 2)) == 3
    assert hl.eval(hl.min(hl.missing(hl.tint32), 2)) == 2
    assert hl.eval(hl.min(hl.missing(hl.tint32), 2, filter_missing=False)) is None
    table = hl.range_table(5)
    assert table.annotate(n=hl.min(table.idx, 4-table.idx)).n.collect() == [0, 1, 2, 1, 0]
