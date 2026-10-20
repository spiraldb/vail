import math
import pytest
import vail as hl


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_finite_mean_and_population_stdev(partitions):
    values, mean, stdev = [1.0, 3.0], 2.0, 1.0
    table = hl.Table.parallelize([{'x': x} for x in values], schema=hl.tstruct(x=hl.tfloat64), n_partitions=partitions)
    result = table.aggregate(hl.struct(mean=hl.agg.mean(table.x), stats=hl.agg.stats(table.x)))
    assert result.mean == pytest.approx(mean, rel=1e-14, abs=0.)
    assert result.stats.mean == pytest.approx(mean, rel=1e-14, abs=0.)
    assert result.stats.stdev == pytest.approx(stdev, rel=1e-14, abs=0.)
    assert result.stats.n == len(values)


# Hail's `hl.agg.stats` computes stdev as sqrt((sum(x**2) - mean * sum(x)) / n),
# while this path delegates to DataFusion's native population-stdev accumulator
# (Welford updates and partial-state merges). Their extreme subnormal/overflow
# results can differ; for finite inputs near 1e308, Hail's raw sum-of-squares
# may yield infinity while DataFusion's overflowing delta can yield NaN.
@pytest.mark.parametrize('partitions', [1, 3, 7])
@pytest.mark.parametrize('values,expected', [([math.inf, 1.], math.inf), ([-math.inf, 1.], -math.inf),
                                            ([math.inf, -math.inf], math.nan), ([math.nan, 1.], math.nan)])
def test_nonfinite_mean_and_variance_policy(partitions, values, expected):
    table = hl.Table.parallelize([{'x': x} for x in values], n_partitions=partitions)
    result = table.aggregate(hl.struct(mean=hl.agg.mean(table.x), stats=hl.agg.stats(table.x)))
    assert math.isnan(result.mean) if math.isnan(expected) else result.mean == expected
    assert math.isnan(result.stats.mean) if math.isnan(expected) else result.stats.mean == expected
    assert math.isnan(result.stats.stdev)
    assert result.stats.n == len(values)


@pytest.mark.parametrize('partitions', [1, 3, 7])
@pytest.mark.parametrize('dtype', [hl.tfloat32, hl.tfloat64])
def test_extrema_share_total_ieee_order_across_batches_and_merges(partitions, dtype):
    table = hl.Table.parallelize([{'x': x} for x in [-math.nan, -math.inf, -0., 0., math.inf, math.nan, None]],
                                schema=hl.tstruct(x=dtype), n_partitions=partitions)
    result = table.aggregate(hl.struct(lo=hl.agg.min(table.x), hi=hl.agg.max(table.x)))
    assert math.isnan(result.lo) and math.copysign(1., result.lo) == -1.
    assert math.isnan(result.hi) and math.copysign(1., result.hi) == 1.
    zeros = hl.Table.parallelize([{'x': -0.}, {'x': None}, {'x': 0.}], schema=hl.tstruct(x=dtype), n_partitions=partitions)
    result = zeros.aggregate(hl.struct(lo=hl.agg.min(zeros.x), hi=hl.agg.max(zeros.x)))
    assert math.copysign(1., result.lo) == -1. and math.copysign(1., result.hi) == 1.


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_boolean_null_identities_and_integer_wrapping(partitions):
    table = hl.Table.parallelize([{'b': True, 'x': 2**63 - 1}, {'b': None, 'x': 1}, {'b': False, 'x': None}],
                                schema=hl.tstruct(b=hl.tbool, x=hl.tint64), n_partitions=partitions)
    result = table.aggregate(hl.struct(n=hl.agg.count(), count=hl.agg.count_where(table.b),
                                      fraction=hl.agg.fraction(table.b), any=hl.agg.any(table.b),
                                      all=hl.agg.all(table.b), total=hl.agg.sum(table.x)))
    assert result == hl.Struct(n=3, count=1, fraction=1/3, any=True, all=False, total=-(2**63))
    product = hl.Table.parallelize([{'x': 2**62}, {'x': 4}], n_partitions=partitions)
    assert product.aggregate(hl.agg.product(product.x)) == 0


def test_complete_scalar_binders_are_valid_closed_initializers():
    table = hl.range_table(6)
    bins = [hl.fold(lambda acc, x: acc + x, 0, [1, 2]),
            hl.array([1, 2, 3]).find(lambda x: x == 3),
            hl.array([1, 2]).scan(lambda acc, x: acc + x, 0)[-1],
            hl.switch(2).when(1, 1).when(2, 3).default(1)]
    end = hl.if_else(hl.array([1, 2]).any(lambda x: x == 2), 6., 7.)
    for n in bins:
        assert table.aggregate(hl.agg.hist(table.idx, 0., end, n)).bin_freq == [2, 2, 2]


def test_prefix_population_stdev_uses_native_nonfinite_behavior():
    table = hl.Table.parallelize([{'x': x} for x in [-1e308, 1e308, -1e308, 1e308]], n_partitions=3)
    result = table.annotate(stats=hl.scan.stats(table.x)).collect()
    assert math.isnan(result[0].stats.mean) and math.isnan(result[0].stats.stdev)
    assert result[1].stats.mean == -1e308 and result[1].stats.stdev == 0.
    # The mean is still finite. Welford's value - mean delta overflows to
    # infinity, making the native M2 update invalid and the stdev NaN. Hail's
    # raw sum-of-squares formula yields infinity for the same prefix; this is an
    # accepted extreme-float divergence, not ordinary finite-range behavior.
    assert result[2].stats.mean == 0. and math.isnan(result[2].stats.stdev)


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_coordinate_means_follow_native_sum_overflow_semantics(partitions):
    table = hl.Table.parallelize(
        [{'xs': [1e308, -1e308, None]}, {'xs': [1e308, 1e308, 3.]}],
        schema=hl.tstruct(xs=hl.tarray(hl.tfloat64)), n_partitions=partitions)
    # Hail mean is sum(x) / count(x), like the native AVG implementation:
    # summing the two first-coordinate 1e308 values overflows before division.
    result = table.aggregate(hl.agg.array_agg(hl.agg.mean, table.xs))
    assert math.isinf(result[0]) and result[0] > 0.
    assert result[1:] == [0., 3.]
