"""Null-tested values keep their original demand and lexical domains."""
import math
import pytest
import vail as hl


def test_likelihood_quality_both_null_test_polarities():
    table = hl.Table.parallelize(
        [{'pl': xs} for xs in [[0, 8, 20], [9, None, 3], [], None, [4, 4], [None, None]]],
        schema=hl.tstruct(pl=hl.tarray(hl.tint32)))
    quality = hl.gq_from_pl(table.pl)
    query = table.select(
        defined=hl.if_else(hl.is_defined(quality), quality, 99),
        missing=hl.if_else(hl.is_missing(quality), 99, quality))
    for _ in range(2):
        assert [(row.defined, row.missing) for row in query.collect()] == [
            (8, 8), (6, 6), (0, 0), (99, 99), (0, 0), (0, 0)]


def test_bounds_stay_inside_the_outer_guard():
    table = hl.Table.parallelize(
        [{'xs': [4]}, {'xs': [None]}, {'xs': []}, {'xs': None}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)))
    value = table.xs[0]
    result = hl.if_else(hl.len(table.xs) > 0,
                        hl.if_else(hl.is_defined(value), value, 99), -1)
    assert table.select(value=result).value.collect() == [4, 99, -1, None]


def test_fallback_errors_remain_lazy():
    quality = hl.gq_from_pl(hl.array([0, 10, 20]))
    assert hl.eval(hl.if_else(hl.is_defined(quality), quality, 1 // hl.int32(0))) == 10
    missing = hl.gq_from_pl(hl.missing(hl.tarray(hl.tint32)))
    with pytest.raises(Exception, match='(?i)zero'):
        hl.eval(hl.if_else(hl.is_missing(missing), 1 // hl.int32(0), missing))


def test_signed_zero_and_reserved_column_name():
    table = hl.Table.parallelize(
        [{'x': -0.0, '__vail_condition_value': 7}, {'x': 0.0, '__vail_condition_value': 9}],
        schema=hl.tstruct(x=hl.tfloat64, __vail_condition_value=hl.tint32))
    value = hl.float32(table.x)
    other = hl.int64(table['__vail_condition_value'])
    rows = table.select(value=hl.if_else(hl.is_defined(value), value, hl.float32(99)),
                        other=hl.if_else(hl.is_missing(other), hl.int64(99), other)).collect()
    assert [math.copysign(1, row.value) for row in rows] == [-1, 1]
    assert [row.other for row in rows] == [7, 9]
