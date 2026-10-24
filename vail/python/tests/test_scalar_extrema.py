import math

import pytest
import vail as hl


@pytest.mark.parametrize('dtype,lo,hi', [
    (hl.tint32, -(2**31), 2**31 - 1),
    (hl.tint64, -(2**63), 2**63 - 1),
])
def test_integer_extrema_project_and_reduce(dtype, lo, hi):
    table = hl.Table.parallelize(
        [hl.Struct(x=lo), hl.Struct(x=-1), hl.Struct(x=None), hl.Struct(x=hi)],
        schema=hl.tstruct(x=dtype), n_partitions=3,
    )
    zero = hl.literal(0, dtype)
    table = table.annotate(
        lower=hl.min(zero, table.x), upper=hl.max(table.x, zero),
        strict=hl.min(table.x, zero, filter_missing=False),
        three=hl.max(hl.literal(lo, dtype), table.x, hl.literal(hi, dtype)),
    )
    assert [(r.lower, r.upper, r.strict, r.three) for r in table.collect()] == [
        (lo, 0, lo, hi), (-1, 0, -1, hi), (0, 0, None, hi), (0, hi, 0, hi),
    ]
    assert table.aggregate(hl.agg.count_where(table.upper >= 0)) == 4
    nullable = table.annotate(value=hl.min(table.x, hl.missing(dtype)))
    assert [r.value for r in nullable.collect()] == [lo, -1, None, hi]


def test_extrema_missing_empty_singleton_and_promotion():
    assert hl.eval(hl.min(hl.missing(hl.tint32), hl.missing(hl.tint32))) is None
    assert hl.eval(hl.max(hl.missing(hl.tint64), hl.missing(hl.tint64))) is None
    assert hl.eval(hl.min(hl.empty_array(hl.tint32))) is None
    assert hl.eval(hl.max(hl.missing(hl.tarray(hl.tint64)))) is None
    assert hl.eval(hl.min(hl.array([7]))) == 7
    expr = hl.max(hl.int32(-1), hl.int64(2**53 + 1))
    assert expr.dtype == hl.tint64
    assert hl.eval(expr) == 2**53 + 1
    assert hl.eval(hl.min(hl.int64(-(2**63)), hl.int32(0))) == -(2**63)
    with pytest.raises(ValueError):
        hl.min()


def test_integer_clamps_in_nested_matrix_queries():
    matrix = hl.utils.range_matrix_table(7, 5, n_partitions=3)
    matrix = matrix.annotate_entries(x=matrix.row_idx - matrix.col_idx)
    matrix = matrix.filter_entries((matrix.row_idx + matrix.col_idx) % 3 != 0)
    matrix = matrix.annotate_rows(
        total=hl.agg.sum(hl.min(2, hl.max(-2, matrix.x))),
        positive=hl.agg.count_where(hl.max(matrix.x, 0) > 0),
    )
    expected = [
        (sum(min(2, max(-2, r-c)) for c in range(5) if (r+c) % 3),
         sum(r-c > 0 for c in range(5) if (r+c) % 3))
        for r in range(7)
    ]
    assert [(r.total, r.positive) for r in matrix.rows().collect()] == expected


def test_float_and_strict_reductions_retain_contract():
    assert math.isnan(hl.eval(hl.min(float('nan'), 1.0)))
    assert math.isnan(hl.eval(hl.max(1.0, float('nan'))))
    assert math.isnan(hl.eval(hl.max(float('nan'), float('nan'))))
    assert hl.eval(hl.max(1, hl.missing(hl.tint32), filter_missing=False)) is None


def test_integer_extrema_preserves_conditional_errors_and_argument_evaluation():
    bad = hl.array([7])[2]
    assert hl.eval(hl.if_else(False, hl.min(5, bad), 11)) == 11
    with pytest.raises(Exception, match='out of bounds'):
        hl.eval(hl.min(5, bad))


def test_row_predicates_and_mixed_numeric_extrema():
    table = hl.utils.range_table(21, n_partitions=4)
    table = table.filter(table.idx % 2 == 0)
    table = table.annotate(value=hl.max(hl.int64(-5), hl.min(table.idx - 8, 5)))
    assert [r.value for r in table.collect()] == [-5, -5, -4, -2, 0, 2, 4, 5, 5, 5, 5]
    assert table.value.dtype == hl.tint64
    assert hl.eval(hl.min(hl.int64(5), hl.float64(2.5))) == 2.5
