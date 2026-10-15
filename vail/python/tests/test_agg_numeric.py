import unittest

import pytest
import vail as hl


class NumericAggregateApiTests(unittest.TestCase):
    def setUp(self):
        self.table = hl.Table.parallelize(
            [{"x": 1.0, "y": 2.0, "label": "a"}],
            schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64, label=hl.tstr),
        )

    def test_options_are_compile_time_and_inputs_are_expressions(self):
        hist = hl.agg.hist(self.table.x, 0, 10, 5)
        self.assertEqual(hist._ir["function"], "hist")
        self.assertEqual([name for name, _ in hist._ir["init"]], ["start", "end", "bins"])

        qs = hl.agg.approx_quantiles(self.table.x, [0, 0.5, 1], k=128)
        self.assertEqual(qs._ir["function"], "approx_quantiles")
        self.assertEqual(qs._ir["init"][0][0], "quantiles")
        self.assertEqual(qs._ir["init"][0][1]["op"], "array")
        self.assertEqual(qs._ir["options"]["k"], 128)

        corr = hl.agg.corr(self.table.x, self.table.y)
        self.assertEqual(corr._ir["function"], "corr")

    def test_hist_corr_and_quantile_execution(self):
        table = hl.Table.parallelize(
            [{"x": float(i), "y": 3.0 * i} for i in range(1, 101)],
            schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64), n_partitions=4,
        )
        hist = table.aggregate(hl.agg.hist(table.x, 0, 100, 10))
        self.assertEqual(hist.bin_freq, [9] + [10] * 8 + [11])
        self.assertEqual(hist.n_smaller, 0)
        self.assertEqual(hist.n_larger, 0)
        self.assertAlmostEqual(table.aggregate(hl.agg.corr(table.x, table.y)), 1.0)
        median = table.aggregate(hl.agg.approx_median(table.x, k=128))
        self.assertLessEqual(abs(median - 50.5), 8)
        quantiles = table.aggregate(hl.agg.approx_quantiles(table.x, [0, 0.5, 1], k=128))
        self.assertEqual(quantiles[0], 1.0)
        self.assertEqual(quantiles[-1], 100.0)

    def test_linear_regression_execution_and_empty_result(self):
        table = hl.Table.parallelize(
            [{"x": float(x), "y": 2.0 + 3.0 * x + noise}
             for x, noise in [(-2, .1), (-1, -.2), (0, .05), (1, .1), (2, -.05)]],
            schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64), n_partitions=2,
        )
        result = table.aggregate(hl.agg.linreg(table.y, [1.0, table.x]))
        self.assertAlmostEqual(result.beta[0], 2.0, places=2)
        self.assertAlmostEqual(result.beta[1], 3.0, places=2)
        self.assertEqual(result.n, 5)
        filtered = table.filter(table.x < -10)
        empty = filtered.aggregate(hl.agg.linreg(filtered.y, [1.0, filtered.x]))
        self.assertEqual(empty.n, 0)
        self.assertIsNone(empty.beta)

    def test_rejects_invalid_histogram_and_quantiles(self):
        with self.assertRaises(ValueError):
            self.table.aggregate(hl.agg.hist(self.table.x, 5, 5, 4))
        with self.assertRaises(ValueError):
            self.table.aggregate(hl.agg.approx_quantiles(self.table.x, [0.5, 1.1]))

    def test_regression_and_downsample_shapes(self):
        regression = hl.agg.linreg(self.table.y, [1.0, self.table.x])
        self.assertEqual(regression.dtype["n"], hl.tint64)
        self.assertEqual(regression._ir["options"]["nested_dim"], 1)
        points = hl.agg.downsample(self.table.x, self.table.y, self.table.label)
        self.assertEqual(points.dtype.element_type.types[2], hl.tarray(hl.tstr))


def test_numeric_empty_and_degenerate_execution():
    import math
    t = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tint32, y=hl.tint32))
    assert t.aggregate(hl.agg.corr(t.x, t.y)) is None
    assert t.aggregate(hl.agg.approx_quantiles(t.x, .5)) is None
    assert t.aggregate(hl.agg.approx_quantiles(t.x, [.5])) == [None]
    constant = hl.Table.parallelize([{"x": 1., "y": 2.}, {"x": 1., "y": 3.}],
                                    schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64))
    assert math.isnan(constant.aggregate(hl.agg.corr(constant.x, constant.y)))


def test_hist_right_endpoint_and_nan():
    import math
    t = hl.Table.parallelize([{"x": -1.}, {"x": 0.}, {"x": 2.},
                              {"x": 4.}, {"x": 5.}, {"x": float("nan")}],
                             schema=hl.tstruct(x=hl.tfloat64), n_partitions=3)
    h = t.aggregate(hl.agg.hist(t.x, 0, 4, 2))
    assert h.bin_edges == [0., 2., 4.]
    assert h.bin_freq == [1, 2]
    assert h.n_smaller == 1 and h.n_larger == 1


def test_linreg_null_model_and_negative_weight():
    import math
    rows = [{"y": 0., "w": 1.}, {"y": 1., "w": 1.}, {"y": 3., "w": 1.}]
    t = hl.Table.parallelize(rows, schema=hl.tstruct(y=hl.tfloat64, w=hl.tfloat64), n_partitions=2)
    fit = t.aggregate(hl.agg.linreg(t.y, [1.], nested_dim=1))
    assert fit.n == 3
    assert fit.beta == pytest.approx([4 / 3])
    assert fit.multiple_r_squared == pytest.approx(0.)
    assert fit.adjusted_r_squared == pytest.approx(0.)
    assert math.isnan(fit.f_stat)
    negative = hl.Table.parallelize([{"y": 0., "w": -1.}, {"y": 1., "w": 1.}, {"y": 3., "w": 1.}],
                                    schema=hl.tstruct(y=hl.tfloat64, w=hl.tfloat64), n_partitions=2)
    fit = negative.aggregate(hl.agg.linreg(negative.y, [1.], nested_dim=0, weight=negative.w))
    assert fit.n == 3
    assert math.isnan(fit.beta[0])
    assert math.isnan(fit.multiple_standard_error)


def test_linreg_columnar_predictors_inside_array_aggregation():
    table = hl.Table.parallelize(
        [{"x": x, "y": None if x is None else 1.0 + 2.0 * x, "w": 1.0}
         for x in [None, -2.0, -1.0, 0.0, 1.0, 2.0]],
        schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64, w=hl.tfloat64),
        n_partitions=3,
    )
    fits = table.aggregate(hl.agg.array_agg(
        lambda offset: hl.agg.linreg(table.y + offset, [1.0, table.x], weight=table.w),
        hl.range(3),
    ))
    for offset, fit in enumerate(fits):
        assert fit.n == 5
        assert fit.beta == pytest.approx([1.0 + offset, 2.0])

    # A missing predictor excludes the observation even when y is defined.
    table = table.annotate(y=hl.or_else(table.y, 99.0))
    assert table.aggregate(hl.agg.linreg(table.y, [1.0, table.x])).n == 5


def test_kll_rejects_int64_precision_loss():
    t = hl.Table.parallelize([{"x": 2**53 + 1}], schema=hl.tstruct(x=hl.tint64))
    with pytest.raises(Exception, match="2\\^53"):
        t.aggregate(hl.agg.approx_median(t.x))


@pytest.mark.parametrize('raw', [False, True])
def test_approx_cdf_is_rejected_when_constructed(raw):
    t = hl.range_table(4)
    with pytest.raises(NotImplementedError, match="approx_cdf is unsupported"):
        hl.agg.approx_cdf(t.idx, _raw=raw)


def test_native_approx_cdf_is_rejected():
    t = hl.range_table(4)
    expr = hl.agg._aggregate('approx_cdf', t.idx,
                             hl.tstruct(values=hl.tarray(hl.tint32), ranks=hl.tarray(hl.tint64)),
                             options={'k': 100, 'raw': False})
    with pytest.raises(ValueError, match="approx_cdf is unsupported"):
        t.aggregate(expr)


def test_downsample_missing_label_is_missing():
    t = hl.Table.parallelize([{"x": 0., "y": 0.}, {"x": 1., "y": 1.}],
                             schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64))
    points = t.aggregate(hl.agg.downsample(t.x, t.y, n_divisions=4))
    assert len(points) == 2
    assert all(point[2] is None for point in points)


@pytest.mark.parametrize("k", [1, 7])
def test_hail_accepts_k_below_datasketches_minimum(k):
    # Hail's ApproxCDFStateManager has an eight-item minimum level capacity,
    # but its public k is not required to be at least eight.
    table = hl.range_table(4)
    assert table.aggregate(hl.agg.approx_median(table.idx, k=k)) == 2
    assert table.aggregate(hl.agg.approx_quantiles(table.idx, [0, 0.5, 1], k=k)) == [0, 2, 3]


@pytest.mark.parametrize('function', [hl.agg.approx_quantiles, hl.agg.approx_median])
def test_quantile_k_above_sketch_capacity_is_rejected(function):
    table = hl.range_table(4)
    args = [table.idx, 0.5] if function is hl.agg.approx_quantiles else [table.idx]
    with pytest.raises(ValueError, match="maximum k"):
        table.aggregate(function(*args, k=65536))
