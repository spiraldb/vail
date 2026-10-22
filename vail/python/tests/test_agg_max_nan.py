"""Hail's NaN-ignoring maximum through the public aggregation APIs."""
import math

import pytest
import vail as hl


def assert_max(actual, expected):
    if expected is not None and math.isnan(expected):
        assert math.isnan(actual)
    else:
        assert actual == expected


@pytest.mark.parametrize('dtype', [hl.tfloat32, hl.tfloat64])
@pytest.mark.parametrize('partitions', [1, 3, 7])
@pytest.mark.parametrize('values,expected', [
    ([math.nan, -4., None, -math.nan, -2.], -2.),
    ([-math.nan, -math.inf, math.nan], -math.inf),
    ([math.nan, math.inf, -math.nan], math.inf),
    ([math.nan, None, -math.nan], math.nan),
    ([None, None], None),
    ([], None),
    ([-0., math.nan, 0.], 0.),
])
def test_max_nan_global_and_local(dtype, partitions, values, expected):
    table = hl.Table.parallelize([{'x': x} for x in values],
                                schema=hl.tstruct(x=dtype), n_partitions=partitions)
    expression = hl.agg.max(table.x)
    assert expression.dtype == dtype
    assert_max(table.aggregate(expression), expected)
    local = hl.literal(values, hl.tarray(dtype)).aggregate(hl.agg.max)
    assert local.dtype == dtype
    assert_max(hl.eval(local), expected)
    if values and expected == 0.:
        assert math.copysign(1., table.aggregate(expression)) == 1.


@pytest.mark.parametrize('dtype', [hl.tfloat32, hl.tfloat64])
@pytest.mark.parametrize('partitions', [1, 3])
def test_max_nan_grouped_filtered_and_exploded(dtype, partitions):
    table = hl.Table.parallelize(
        [{'g': g, 'x': x} for g, x in [
            (0, math.nan), (1, None), (2, -math.nan), (0, -5.),
            (1, None), (2, math.nan), (0, -2.)]],
        schema=hl.tstruct(g=hl.tint32, x=dtype), n_partitions=partitions)
    grouped = table.group_by(table.g).aggregate(x=hl.agg.max(table.x)).collect()
    assert [r.g for r in grouped] == [0, 1, 2]
    for row, expected in zip(grouped, [-2., None, math.nan], strict=True):
        assert_max(row.x, expected)
    nested = table.aggregate(hl.agg.group_by(table.g, hl.agg.max(table.x)))
    for key, expected in enumerate([-2., None, math.nan]):
        assert_max(nested[key], expected)
    assert table.aggregate(hl.agg.filter(table.g == 0, hl.agg.max(table.x))) == -2.
    assert table.aggregate(hl.agg.filter(False, hl.agg.max(table.x))) is None
    assert table.aggregate(hl.agg.explode(hl.agg.max, hl.array([table.x, table.x]))) == -2.


@pytest.mark.parametrize('dtype', [hl.tfloat32, hl.tfloat64])
@pytest.mark.parametrize('partitions', [1, 3])
def test_max_nan_coordinates_and_exclusive_scans(dtype, partitions):
    table = hl.Table.parallelize(
        [{'x': x, 'xs': xs} for x, xs in [
            (None, [math.nan, None, -math.nan]),
            (math.nan, [None, None, math.nan]),
            (-5., [-5., None, -math.nan]),
            (-math.nan, [-2., None, math.nan]),
            (-2., [math.nan, None, None])]],
        schema=hl.tstruct(x=dtype, xs=hl.tarray(dtype)), n_partitions=partitions)
    coordinates = table.aggregate(hl.agg.array_agg(hl.agg.max, table.xs))
    for actual, expected in zip(coordinates, [-2., None, math.nan], strict=True):
        assert_max(actual, expected)
    scanned = table.annotate(prefix=hl.scan.max(table.x)).prefix.collect()
    for actual, expected in zip(scanned, [None, None, math.nan, -5., -5.], strict=True):
        assert_max(actual, expected)
    prefixes = table.annotate(prefix=hl.scan.array_agg(hl.scan.max, table.xs)).prefix.collect()
    assert prefixes[0] is None
    for actual, expected in zip(prefixes[1:], [
            [math.nan, None, math.nan], [math.nan, None, math.nan],
            [-5., None, math.nan], [-2., None, math.nan]], strict=True):
        assert len(actual) == len(expected)
        for x, y in zip(actual, expected, strict=True):
            assert_max(x, y)


def test_max_nan_matrix_axes_and_entry_filters():
    matrix = hl.range_matrix_table(3, 3, n_partitions=3)
    matrix = matrix.annotate_entries(x=hl.if_else(
        matrix.row_idx == matrix.col_idx, hl.float64(-matrix.row_idx - 1), math.nan))
    assert matrix.aggregate_entries(hl.agg.max(matrix.x)) == -1.
    assert matrix.annotate_rows(m=hl.agg.max(matrix.x)).rows().m.collect() == [-1., -2., -3.]
    assert matrix.annotate_cols(m=hl.agg.max(matrix.x)).cols().m.collect() == [-1., -2., -3.]
    filtered = matrix.filter_entries(matrix.row_idx != matrix.col_idx)
    assert math.isnan(filtered.aggregate_entries(hl.agg.max(filtered.x)))
