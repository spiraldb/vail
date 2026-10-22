import unittest

import pytest
import vail as hl


class NumericAggregateApiTests(unittest.TestCase):
    def setUp(self):
        self.table = hl.Table.parallelize(
            [{"x": 1.0, "y": 2.0, "label": "a"}],
            schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64, label=hl.tstr),
        )

    def test_numeric_parameters_execute_with_declared_result_types(self):
        histogram = self.table.aggregate(hl.agg.hist(self.table.x, 0, 10, 5))
        self.assertEqual(histogram.bin_edges, [0., 2., 4., 6., 8., 10.])
        self.assertEqual(histogram.bin_freq, [1, 0, 0, 0, 0])
        result = hl.agg.approx_quantiles(self.table.x, [0, .5, 1], k=128)
        self.assertEqual(result.dtype, hl.tarray(hl.tfloat64))
        self.assertEqual(self.table.aggregate(result), [1., 1., 1.])

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

    def test_regression_and_labeled_downsample_values(self):
        points = self.table.aggregate(hl.agg.downsample(self.table.x, self.table.y, self.table.label))
        self.assertEqual(points, [(1., 2., ['a'])])
        regression = hl.agg.linreg(self.table.y, [1.0, self.table.x])
        self.assertEqual(regression.dtype['n'], hl.tint64)
        self.assertIsNone(self.table.aggregate(regression).beta)


def test_numeric_empty_and_degenerate_execution():
    import math
    t = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tint32, y=hl.tint32))
    assert t.aggregate(hl.agg.corr(t.x, t.y)) is None
    assert t.aggregate(hl.agg.approx_quantiles(t.x, .5)) is None
    assert t.aggregate(hl.agg.approx_quantiles(t.x, [.5])) == [None]
    constant = hl.Table.parallelize([{"x": 1., "y": 2.}, {"x": 1., "y": 3.}],
                                    schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64))
    assert math.isnan(constant.aggregate(hl.agg.corr(constant.x, constant.y)))


def test_grouped_corr_uses_native_groups_with_hail_empty_and_degenerate_results():
    import math

    table = hl.Table.parallelize(
        [
            {"g": 0, "x": 1.0, "y": 2.0},
            {"g": 0, "x": 2.0, "y": 4.0},
            {"g": 1, "x": 1.0, "y": 2.0},
            {"g": 2, "x": None, "y": 2.0},
            {"g": 2, "x": 3.0, "y": None},
            {"g": 3, "x": 2.0, "y": 4.0},
            {"g": 3, "x": 2.0, "y": 8.0},
            {"g": 4, "x": math.nan, "y": 1.0},
            {"g": 4, "x": 2.0, "y": 3.0},
        ],
        schema=hl.tstruct(g=hl.tint32, x=hl.tfloat64, y=hl.tfloat64),
        n_partitions=4,
    )
    grouped = table.group_by(table.g).aggregate(corr=hl.agg.corr(table.x, table.y)).collect()
    by_group = {row.g: row.corr for row in grouped}
    assert by_group[0] == 1.0
    assert math.isnan(by_group[1])
    assert by_group[2] is None
    assert math.isnan(by_group[3])
    assert math.isnan(by_group[4])


def test_grouped_linreg_batches_rows_per_group_with_origins_weights_and_nulls():
    rows = []
    for group, offset in enumerate((0.0, 1_000_000.0)):
        for i in range(10_000):
            x = offset + i
            rows.append({
                "g": group,
                "x": None if i == 7 else x,
                "y": None if i == 13 else 4.0 + 2.5 * x,
                "w": 0.0 if i == 21 else 1.0 + (i % 3),
            })
    table = hl.Table.parallelize(
        rows,
        schema=hl.tstruct(g=hl.tint32, x=hl.tfloat64, y=hl.tfloat64, w=hl.tfloat64),
        n_partitions=1,
    )
    grouped = table.group_by(table.g).aggregate(
        fit=hl.agg.linreg(table.y, [1.0, table.x], weight=table.w)
    ).collect()
    assert len(grouped) == 2
    for row in grouped:
        assert row.fit.n == 9_998
        assert row.fit.beta == pytest.approx([4.0, 2.5], abs=1e-5)


def test_array_linreg_reuses_only_identical_predictor_batches():
    table = hl.range_table(10_000, n_partitions=1)
    fits = table.aggregate(hl.agg.array_agg(
        lambda offset: hl.agg.linreg(4.0 + 2.5 * table.idx + offset, [1.0, table.idx]),
        hl.range(3),
    ))
    for offset, fit in enumerate(fits):
        assert fit.n == 10_000
        assert fit.beta == pytest.approx([4.0 + offset, 2.5], abs=1e-8)

    masked = table.aggregate(hl.agg.array_agg(
        lambda group: hl.agg.linreg(
                hl.if_else(table.idx % 3 == group, hl.missing(hl.tfloat64), 4.0 + 2.5 * table.idx + group),
            [1.0, table.idx],
        ),
        hl.range(3),
    ))
    for group, fit in enumerate(masked):
        assert fit.n == (6_666 if group == 0 else 6_667)
        assert fit.beta == pytest.approx([4.0 + group, 2.5], abs=1e-8)


def test_array_linreg_cached_qr_preserves_tiny_residual_norm():
    table = hl.Table.parallelize(
        [
            {"x0": 1.0, "x1": 0.0, "y": 0.0},
            {"x0": 0.0, "x1": 1.0, "y": 1e-50},
            {"x0": 0.0, "x1": 0.0, "y": 1e-180},
        ],
        schema=hl.tstruct(x0=hl.tfloat64, x1=hl.tfloat64, y=hl.tfloat64),
        n_partitions=1,
    )
    fits = table.aggregate(hl.agg.array_agg(
        lambda scale: hl.agg.linreg(table.y * (scale + 1.0), [table.x0, table.x1]),
        hl.range(2),
    ))
    for scale, fit in enumerate(fits, start=1):
        assert fit.n == 3
        assert fit.multiple_standard_error == pytest.approx(scale * 1e-180, rel=1e-12, abs=0.0)


def test_linreg_extreme_column_range_preserves_tiny_residual():
    table = hl.Table.parallelize(
        [
            {"x0": 1.0, "x1": 0.0, "y": 0.0},
            {"x0": 1.0, "x1": 1.0, "y": 1e200},
            {"x0": 0.0, "x1": 0.0, "y": 1e-200},
        ],
        schema=hl.tstruct(x0=hl.tfloat64, x1=hl.tfloat64, y=hl.tfloat64),
        n_partitions=1,
    )
    fit = table.aggregate(hl.agg.linreg(table.y, [table.x0, table.x1]))
    assert fit.n == 3
    assert fit.beta == pytest.approx([0.0, 1e200])
    assert fit.multiple_standard_error > 0.0
    assert fit.multiple_standard_error == pytest.approx(1e-200, rel=1e-12, abs=0.0)


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

    nested_fits = table.aggregate(
        hl.agg.array_agg(
            lambda offset: hl.agg.linreg(
                table.y + offset, [1.0, table.x], nested_dim=1, weight=table.w
            ),
            hl.range(2),
        )
    )
    for offset, fit in enumerate(nested_fits):
        assert fit.n == 5
        assert fit.beta == pytest.approx([1.0 + offset, 2.0])

    # A missing predictor excludes the observation even when y is defined.
    table = table.annotate(y=hl.or_else(table.y, 99.0))
    assert table.aggregate(hl.agg.linreg(table.y, [1.0, table.x])).n == 5


def test_array_linreg_coordinates_preserve_null_empty_and_length_semantics():
    table = hl.Table.parallelize(
        [
            {"x": 1.0, "ys": [None, 2.0]},
            {"x": 2.0, "ys": None},
            {"x": 3.0, "ys": [4.0, 6.0]},
            {"x": 4.0, "ys": [6.0, 8.0]},
            {"x": 5.0, "ys": [8.0, 10.0]},
        ],
        schema=hl.tstruct(x=hl.tfloat64, ys=hl.tarray(hl.tfloat64)),
        n_partitions=3,
    )
    fits = table.aggregate(
        hl.agg.array_agg(lambda y: hl.agg.linreg(y, [1.0, table.x]), table.ys)
    )
    assert fits[0].n == 3
    assert fits[0].beta == pytest.approx([-2.0, 2.0])
    assert fits[1].n == 4
    assert fits[1].beta == pytest.approx([0.0, 2.0])

    defined_but_empty = hl.Table.parallelize(
        [
            {"x": 1.0, "ys": [None, None]},
            {"x": 2.0, "ys": [None, None]},
        ],
        schema=hl.tstruct(x=hl.tfloat64, ys=hl.tarray(hl.tfloat64)),
    )
    defined_but_empty_fits = defined_but_empty.aggregate(
        hl.agg.array_agg(
            lambda y: hl.agg.linreg(y, [1.0, defined_but_empty.x]), defined_but_empty.ys
        )
    )
    assert [fit.n for fit in defined_but_empty_fits] == [0, 0]
    assert all(fit.beta is None for fit in defined_but_empty_fits)

    empty_arrays = hl.Table.parallelize(
        [{"x": 1.0, "ys": []}, {"x": 2.0, "ys": None}],
        schema=hl.tstruct(x=hl.tfloat64, ys=hl.tarray(hl.tfloat64)),
    )
    assert empty_arrays.aggregate(
        hl.agg.array_agg(lambda y: hl.agg.linreg(y, [1.0, empty_arrays.x]), empty_arrays.ys)
    ) == []
    all_missing = hl.Table.parallelize(
        [{"x": 1.0, "ys": None}, {"x": 2.0, "ys": None}],
        schema=hl.tstruct(x=hl.tfloat64, ys=hl.tarray(hl.tfloat64)),
    )
    assert all_missing.aggregate(
        hl.agg.array_agg(lambda y: hl.agg.linreg(y, [1.0, all_missing.x]), all_missing.ys)
    ) is None

    mismatched = hl.Table.parallelize(
        [{"x": 1.0, "ys": [1.0]}, {"x": 2.0, "ys": [2.0, 3.0]}],
        schema=hl.tstruct(x=hl.tfloat64, ys=hl.tarray(hl.tfloat64)),
    )
    with pytest.raises(hl.utils.FatalError, match=r"array_agg requires equal array lengths"):
        mismatched.aggregate(
            hl.agg.array_agg(lambda y: hl.agg.linreg(y, [1.0, mismatched.x]), mismatched.ys)
        )

    # Filtered callback reducers keep the relational fallback and preserve the
    # per-position row mask instead of becoming an unfiltered coordinate UDAF.
    filtered = table.aggregate(
        hl.agg.array_agg(
            lambda y: hl.agg.filter(y > 0.0, hl.agg.linreg(y, [1.0, table.x])),
            table.ys,
        )
    )
    assert filtered[0].n == 3
    assert filtered[1].n == 4


def test_kll_rejects_int64_precision_loss():
    t = hl.Table.parallelize([{"x": 2**53 + 1}], schema=hl.tstruct(x=hl.tint64))
    with pytest.raises(Exception, match="2\\^53"):
        t.aggregate(hl.agg.approx_median(t.x))


@pytest.mark.parametrize('raw', [False, True])
def test_approx_cdf_is_rejected_when_constructed(raw):
    t = hl.range_table(4)
    with pytest.raises(NotImplementedError, match="approx_cdf is unsupported"):
        hl.agg.approx_cdf(t.idx, _raw=raw)


def test_downsample_missing_label_is_missing():
    t = hl.Table.parallelize([{"x": 0., "y": 0.}, {"x": 1., "y": 1.}],
                             schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64))
    points = t.aggregate(hl.agg.downsample(t.x, t.y, n_divisions=4))
    assert len(points) == 2
    assert all(point[2] is None for point in points)


def test_grouped_downsample_uses_bounded_state_per_group():
    rows = [
        {"group": group, "x": x, "y": x, "label": f"{group}:{x}"}
        for group, base in (("a", 0), ("b", 100))
        for x in range(base, base + 20)
    ]
    table = hl.Table.parallelize(
        rows,
        schema=hl.tstruct(group=hl.tstr, x=hl.tint32, y=hl.tint32, label=hl.tstr),
        n_partitions=5,
    )
    grouped = table.group_by("group").aggregate(
        points=hl.agg.downsample(table.x, table.y, table.label, n_divisions=4)
    ).collect()
    assert {row.group for row in grouped} == {"a", "b"}
    for row in grouped:
        start = 0 if row.group == "a" else 100
        source = {
            (float(x), float(x), (f"{row.group}:{x}",))
            for x in range(start, start + 20)
        }
        assert 1 <= len(row.points) <= 16
        assert {(x, y, tuple(label)) for x, y, label in row.points} <= source


def test_downsample_rebin_preserves_labels_at_bounds_and_partition_merge():
    import math

    rows = [
        {"x": 0., "y": 0., "label": "origin", "labels": ["origin"]},
        {"x": 10., "y": 10., "label": None, "labels": None},
        {"x": 9., "y": 9., "label": "collision", "labels": ["collision"]},
        {"x": -10., "y": -10., "label": "minimum", "labels": ["minimum"]},
        {"x": 0., "y": 10., "label": "upper", "labels": []},
        {"x": -10., "y": 10., "label": None, "labels": None},
        {"x": None, "y": 1., "label": "missing-x", "labels": ["missing-x"]},
        {"x": 1., "y": None, "label": "missing-y", "labels": ["missing-y"]},
        {"x": math.nan, "y": 2., "label": "nan", "labels": ["nan"]},
        {"x": math.inf, "y": 2., "label": "infinite", "labels": ["infinite"]},
    ]
    table = hl.Table.parallelize(
        rows,
        schema=hl.tstruct(x=hl.tfloat64, y=hl.tfloat64, label=hl.tstr, labels=hl.tarray(hl.tstr)),
        n_partitions=3,
    )
    expected_labels = {
        "label": {
            (0., 0.): ["origin"],
            (10., 10.): None,
            (9., 9.): ["collision"],
            (-10., -10.): ["minimum"],
            (0., 10.): ["upper"],
            (-10., 10.): None,
        },
        "labels": {
            (0., 0.): ["origin"],
            (10., 10.): None,
            (9., 9.): ["collision"],
            (-10., -10.): ["minimum"],
            (0., 10.): [],
            (-10., 10.): None,
        },
    }
    for name in ("label", "labels"):
        points = table.aggregate(hl.agg.downsample(table.x, table.y, table[name], n_divisions=4))
        by_coordinate = {(x, y): label for x, y, label in points}
        assert 1 <= len(points) <= 4**2
        assert len(by_coordinate) == len(points)
        assert set(by_coordinate) <= set(expected_labels[name])
        assert all(expected_labels[name][coordinate] == label for coordinate, label in by_coordinate.items())
        # Merge and rebin retain one input representative per final grid cell;
        # exact representative identity can depend on partitioning.
        cells = {
            (min(3, int((x + 10.) / 20. * 4)), min(3, int((y + 10.) / 20. * 4)))
            for x, y in by_coordinate
        }
        assert len(cells) == len(points)
        assert cells == {(0, 0), (2, 2), (3, 3), (2, 3), (0, 3)}


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


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_histogram_finite_extreme_spans_and_exact_endpoints(partitions):
    import math
    table = hl.Table.parallelize([{'x': x} for x in [-1e308, 0., 1e308, -math.inf, math.inf, math.nan, None]],
                                schema=hl.tstruct(x=hl.tfloat64), n_partitions=partitions)
    histogram = table.aggregate(hl.agg.hist(table.x, -1e308, 1e308, 4))
    assert histogram.bin_edges == pytest.approx([-1e308, -5e307, 0., 5e307, 1e308])
    assert histogram.bin_freq == [1, 0, 1, 1]
    assert (histogram.n_smaller, histogram.n_larger) == (1, 1)
    positive = table.aggregate(hl.agg.hist(table.x, 1e308, 1.5e308, 4))
    assert all(math.isfinite(x) for x in positive.bin_edges)
    assert positive.bin_edges[-1] == 1.5e308


def test_histogram_rejects_indistinguishable_adjacent_edges():
    import math
    table = hl.range_table(1)
    with pytest.raises(ValueError, match='represented distinctly'):
        table.aggregate(hl.agg.hist(table.idx, 1., math.nextafter(1., math.inf), 2))


@pytest.mark.parametrize('partitions', [1, 3, 7])
@pytest.mark.parametrize('weighted', [False, True])
def test_regression_translation_and_residuals_are_stable(partitions, weighted):
    rows = [{'x': float(i), 'shifted': 1e12 + i, 'y': 2. + 3. * i + ((i % 7) - 3) * .05,
             'w': float(i % 5)} for i in range(-50, 51)]
    table = hl.Table.parallelize(rows, n_partitions=partitions)
    weight = table.w if weighted else None
    origin = table.aggregate(hl.agg.linreg(table.y, [1., table.x], weight=weight))
    shifted = table.aggregate(hl.agg.linreg(table.y, [1., table.shifted], weight=weight))
    assert origin.n == shifted.n == 101
    assert shifted.beta[1] == pytest.approx(origin.beta[1], abs=2e-5)
    assert shifted.beta[0] + 1e12 * shifted.beta[1] == pytest.approx(origin.beta[0], abs=.05)
    assert shifted.standard_error[1] == pytest.approx(origin.standard_error[1], rel=.01)
    assert shifted.multiple_standard_error == pytest.approx(origin.multiple_standard_error, rel=.01)


@pytest.mark.parametrize('nested', [0, 2])
def test_regression_scaling_nested_models_and_rank_deficiency(nested):
    import math
    table = hl.range_table(20, n_partitions=3)
    y = 4. + 2. * table.idx + hl.float64(table.idx % 3) * .1
    fit = table.aggregate(hl.agg.linreg(y, [1., hl.float64(table.idx) * 1e-6], nested_dim=nested))
    assert fit.beta[1] == pytest.approx(2e6, rel=.01)
    assert math.isfinite(fit.multiple_standard_error)
    if nested == 2:
        assert fit.multiple_r_squared == 0.
        assert math.isnan(fit.f_stat)
    deficient = table.aggregate(hl.agg.linreg(y, [1., hl.float64(table.idx), hl.float64(table.idx)]))
    assert deficient.n == 20 and deficient.beta is None


@pytest.mark.parametrize('labels', [False, True])
def test_downsample_extreme_finite_coordinates_and_labels(labels):
    import sys
    values = [-sys.float_info.max, -1e308, 0., 1e308, sys.float_info.max]
    table = hl.Table.parallelize([{'x': x, 'label': str(i)} for i, x in enumerate(values)], n_partitions=3)
    label = hl.array([table.label, None]) if labels else table.label
    result = table.aggregate(hl.agg.downsample(table.x, table.x, label, n_divisions=4))
    assert 1 <= len(result) <= 16
    for x, y, text in result:
        assert x == y and x in values
        expected = str(values.index(x))
        assert text == ([expected, None] if labels else [expected])


def test_downsample_monotone_input_is_bounded_and_representatives_are_real():
    table = hl.range_table(20_000, n_partitions=3)
    points = table.aggregate(hl.agg.downsample(table.idx, table.idx, hl.str(table.idx), n_divisions=32))
    assert 1 <= len(points) <= 32**2
    assert all(x == y and x == int(x) and 0 <= x < 20_000
               and label == [str(int(x))] for x, y, label in points)


@pytest.mark.parametrize('partitions', [1, 3, 7])
@pytest.mark.parametrize('scale', [1e-200, 1e200])
def test_regression_response_scale_preserves_dimensionless_statistics(partitions, scale):
    table = hl.Table.parallelize([{'x': float(i), 'y': 2. + 3. * i + ((i % 7) - 3) * .05}
                                 for i in range(-20, 21)], n_partitions=partitions)
    origin = table.aggregate(hl.agg.linreg(table.y, [1., table.x]))
    scaled = table.aggregate(hl.agg.linreg(table.y * scale, [1., table.x]))
    assert [x / scale for x in scaled.beta] == pytest.approx(origin.beta, rel=1e-10, abs=1e-10)
    assert [x / scale for x in scaled.standard_error] == pytest.approx(origin.standard_error, rel=1e-10)
    assert scaled.multiple_standard_error / scale == pytest.approx(origin.multiple_standard_error, rel=1e-10)
    assert scaled.multiple_r_squared == pytest.approx(origin.multiple_r_squared, rel=1e-10)
    assert scaled.f_stat == pytest.approx(origin.f_stat, rel=1e-9)


@pytest.mark.parametrize('partitions', [1, 3, 7])
@pytest.mark.parametrize('nested', [0, 1, 2])
def test_regression_general_first_column_and_leading_zero_observations(partitions, nested):
    import numpy as np
    x = np.array([[0., 1.], [0., 1.], [1., 1.], [2., 1.], [3., 1.], [4., 1.]])
    y = np.array([2., 2.1, 5., 8.1, 11., 13.9])
    table = hl.Table.parallelize([{'a': a, 'b': b, 'y': yy} for (a, b), yy in zip(x.tolist(), y.tolist())], n_partitions=partitions)
    result = table.aggregate(hl.agg.linreg(table.y, [table.a, table.b], nested_dim=nested))
    beta = np.linalg.lstsq(x, y, rcond=None)[0]
    residual = np.linalg.norm(y - x @ beta)
    null_residual = np.linalg.norm(y if nested == 0 else y - x[:, :nested] @ np.linalg.lstsq(x[:, :nested], y, rcond=None)[0])
    assert result.beta == pytest.approx(beta.tolist(), abs=1e-10)
    assert result.multiple_standard_error == pytest.approx(residual / 2., rel=1e-10)
    assert result.multiple_r_squared == pytest.approx(1. - (residual / null_residual)**2, abs=1e-10)


def test_regression_zero_weights_rank_and_nonfinite_policies():
    import math
    table = hl.range_table(5, n_partitions=3)
    zero = table.aggregate(hl.agg.linreg(hl.float64(table.idx), [1., hl.float64(table.idx)], weight=0.))
    assert zero.n == 5 and zero.beta is None
    rank = table.aggregate(hl.agg.linreg(hl.float64(table.idx), [0., 1.]))
    assert rank.n == 5 and rank.beta is None
    invalid = table.aggregate(hl.agg.linreg(hl.if_else(table.idx == 2, math.nan, hl.float64(table.idx)), [1., hl.float64(table.idx)]))
    assert invalid.n == 5 and all(math.isnan(x) for x in invalid.beta)


def test_numeric_prefix_scans_merge_histogram_qr_and_geometric_grid_states():
    table = hl.range_table(20, n_partitions=3)
    result = table.annotate(hist=hl.scan.hist(table.idx, 0., 20., 4),
                            fit=hl.scan.linreg(2. + 3. * table.idx, [1., hl.float64(table.idx)]),
                            points=hl.scan.downsample(table.idx, table.idx, hl.str(table.idx), n_divisions=4))
    for row in result.collect():
        assert row.hist.bin_edges == [0., 5., 10., 15., 20.]
        assert sum(row.hist.bin_freq) == row.idx
        assert row.fit.n == row.idx
        if row.idx > 2:
            assert row.fit.beta == pytest.approx([2., 3.], abs=1e-10)
        assert len(row.points) <= 16
        assert all(x == y and 0 <= x < row.idx and label == [str(int(x))]
                   for x, y, label in row.points)
