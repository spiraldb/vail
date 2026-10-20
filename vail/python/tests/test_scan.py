import pytest
import vail as hl


def test_serial_scan_exclusive_across_partitions_and_filter():
    table = hl.Table.parallelize(
        [{"i": i, "x": x} for i, x in enumerate([1, None, 3, 4, None, 6])],
        n_partitions=3,
    )
    scanned = table.annotate(
        n=hl.scan.count(),
        total=hl.scan.sum(table.x),
        vals=hl.scan.collect(table.x),
        low=hl.scan.min(table.x),
        shifted=hl.scan.sum(table.x) + 1,
    )
    rows = scanned.collect()
    assert [r.n for r in rows] == [0, 1, 2, 3, 4, 5]
    assert [r.total for r in rows] == [0, 1, 1, 4, 8, 8]
    assert [r.vals for r in rows] == [[], [1], [1, None], [1, None, 3], [1, None, 3, 4], [1, None, 3, 4, None]]
    assert [r.low for r in rows] == [None, 1, 1, 1, 1, 1]
    assert [r.shifted for r in rows] == [1, 2, 2, 5, 9, 9]
    assert scanned.n_partitions() == 3
    middle = scanned.filter((scanned.i >= 2) & (scanned.i < 4)).collect()
    assert [r.i for r in middle] == [2, 3]
    assert scanned.filter(scanned.i == 5).collect()[0].n == 5


def test_scan_count_where_ignores_false_and_missing_predicates():
    table = hl.Table.parallelize(
        [{"i": i, "b": b} for i, b in enumerate([True, False, None, True, False])],
        schema=hl.tstruct(i=hl.tint32, b=hl.tbool),
        n_partitions=3,
    )
    assert table.annotate(n=hl.scan.count_where(table.b)).n.collect() == [0, 1, 1, 1, 2]


def test_scan_array_aggregate_count_where_with_null_and_filter_values():
    table = hl.Table.parallelize(
        [{"a": values} for values in ([True, None], [False, True], [None, False],
                                       [True, False], [False, False])],
        schema=hl.tstruct(a=hl.tarray(hl.tbool)), n_partitions=3)
    result = table.annotate(prefix=hl.scan.array_agg(
        lambda value: hl.scan.count_where(value), table.a)).prefix.collect()
    assert result == [None, [1, 0], [1, 1], [1, 1], [2, 1]]

    empty = hl.Table.parallelize(
        [{"a": []}, {"a": []}], schema=hl.tstruct(a=hl.tarray(hl.tbool)), n_partitions=2)
    assert empty.annotate(prefix=hl.scan.array_agg(
        lambda value: hl.scan.count_where(value), empty.a)).prefix.collect() == [None, []]

    missing = hl.Table.parallelize(
        [{"a": [None, None]}, {"a": [None, None]}],
        schema=hl.tstruct(a=hl.tarray(hl.tbool)), n_partitions=2)
    assert missing.annotate(prefix=hl.scan.array_agg(
        lambda value: hl.scan.count_where(value), missing.a)).prefix.collect() == [None, [0, 0]]


def test_scan_sum_vectorized_finish_preserves_nulls_overflow_and_empty_partitions():
    schema = hl.tstruct(i=hl.tint64, x=hl.tfloat64)
    table = hl.Table.parallelize(
        [
            {"i": 2**63 - 1, "x": 1.5},
            {"i": None, "x": None},
            {"i": 1, "x": -0.25},
            {"i": -3, "x": 2.0},
        ],
        schema=schema,
        n_partitions=7,
    )
    rows = table.annotate(prefix_i=hl.scan.sum(table.i), prefix_x=hl.scan.sum(table.x)).collect()
    assert [row.prefix_i for row in rows] == [0, 2**63 - 1, 2**63 - 1, -(2**63)]
    assert [row.prefix_x for row in rows] == [0.0, 1.5, 1.5, 1.25]
    assert table.n_partitions() == 7

    empty = hl.Table.parallelize([], schema=schema, n_partitions=3)
    assert empty.annotate(prefix=hl.scan.sum(empty.i)).collect() == []


def test_sum_and_count_scan_prefixes_preserve_null_and_partition_semantics():
    table = hl.Table.parallelize(
        [{"x": None}, {"x": 3}, {"x": None}, {"x": 5}],
        schema=hl.tstruct(x=hl.tint64),
        n_partitions=3,
    )
    scanned = table.annotate(count=hl.scan.count(), total=hl.scan.sum(table.x))
    rows = scanned.collect()
    assert [row.count for row in rows] == [0, 1, 2, 3]
    assert [row.total for row in rows] == [0, 0, 3, 3]

    all_missing = hl.Table.parallelize(
        [{"x": None}, {"x": None}],
        schema=hl.tstruct(x=hl.tint32),
        n_partitions=2,
    )
    assert all_missing.annotate(total=hl.scan.sum(all_missing.x)).total.collect() == [0, 0]


def test_native_prefix_summary_projection_keeps_public_scan_semantics():
    schema = hl.tstruct(keep=hl.tbool, x=hl.tint32, y=hl.tint32)
    table = hl.Table.parallelize(
        [
            {"keep": True, "x": 1, "y": 2},
            {"keep": False, "x": 100, "y": 1},
            {"keep": True, "x": None, "y": 5},
            {"keep": True, "x": 3, "y": 4},
            {"keep": True, "x": -2, "y": 1},
        ],
        schema=schema,
        n_partitions=4,
    ).annotate_globals(offset=hl.int64(10))
    filtered = table.filter(table.keep)
    argument = hl.int64(filtered.x) + hl.int64(filtered.y) + filtered.offset
    scanned = filtered.annotate(
        n=hl.scan.count(),
        total=hl.scan.sum(argument),
    )
    rows = scanned.collect()
    assert [row.n for row in rows] == [0, 1, 2, 3]
    # Sum arguments use two row columns, an Int32-to-Int64 cast, and a global;
    # the missing x contributes no value to the accumulated sum.
    assert [row.total for row in rows] == [0, 13, 13, 30]

    empty = hl.Table.parallelize([], schema=schema, n_partitions=3).annotate_globals(
        offset=hl.int64(10)
    )
    empty_scan = empty.annotate(
        n=hl.scan.count(),
        total=hl.scan.sum(hl.int64(empty.x) + hl.int64(empty.y) + empty.offset),
    )
    assert empty_scan.collect() == []


def test_native_prefix_force_count_still_materializes_retained_payload():
    table = hl.range_table(4, n_partitions=3)
    scanned = table.annotate(
        payload=hl.int32(hl.str(table.idx) + "invalid"),
        prefix=hl.scan.sum(table.idx),
    )
    with pytest.raises(RuntimeError, match="invalid|cast|parse"):
        scanned._force_count()


def test_count_scan_reuses_row_ordinals_instead_of_a_second_local_window():
    table = hl.range_table(100, n_partitions=3)
    scanned = table.annotate(n=hl.scan.count())
    assert scanned.n.collect() == list(range(100))


def test_native_prefix_sum_covers_range_table_int32_and_float32():
    # This is the exact input shape used by benchmarks/compare_hail.py:
    # range_table.idx is Int32, so the fast path must cover SUM(Int32).
    table = hl.range_table(8, n_partitions=3)
    scanned = table.annotate(prefix=hl.scan.sum(table.idx))
    assert [row.prefix for row in scanned.collect()] == [0, 0, 1, 3, 6, 10, 15, 21]
    assert scanned.prefix.dtype == hl.tint64

    floats = hl.Table.parallelize(
        [{"x": value} for value in [1.25, None, 2.5, -0.5]],
        schema=hl.tstruct(x=hl.tfloat32),
        n_partitions=3,
    )
    float_scan = floats.annotate(prefix=hl.scan.sum(floats.x))
    assert [row.prefix for row in float_scan.collect()] == [0.0, 1.25, 1.25, 3.75]
    assert float_scan.prefix.dtype == hl.tfloat64

    booleans = hl.Table.parallelize(
        [{"x": value} for value in [None, True, False]],
        schema=hl.tstruct(x=hl.tbool),
        n_partitions=2,
    )
    bool_scan = booleans.annotate(prefix=hl.scan.sum(booleans.x))
    assert [row.prefix for row in bool_scan.collect()] == [0, 0, 1]
    assert bool_scan.prefix.dtype == hl.tint64


def test_scan_prefixes_preserve_partition_count():
    table = hl.Table.parallelize([{"x": i} for i in range(12)], n_partitions=3)
    scanned = table.annotate(prefix=hl.scan.sum(table.x))
    assert [row.prefix for row in scanned.collect()] == [i*(i-1)//2 for i in range(12)]
    assert scanned.n_partitions() == 3


def test_scan_restores_empty_source_partitions():
    table = hl.Table.parallelize([{"x": 3}, {"x": None}, {"x": 5}], n_partitions=5)
    scanned = table.annotate(
        n=hl.scan.count(),
        total=hl.scan.sum(table.x),
        vals=hl.scan.collect(table.x),
    )
    rows = scanned.collect()
    assert [row.n for row in rows] == [0, 1, 2]
    assert [row.total for row in rows] == [0, 3, 3]
    assert [row.vals for row in rows] == [[], [3], [3, None]]
    assert scanned.n_partitions() == 5


def test_serial_scan_can_use_global_value():
    table = hl.Table.parallelize([{"x": 2}, {"x": 3}, {"x": 4}], n_partitions=2)
    table = table.annotate_globals(offset=10)
    scanned = table.annotate(total=hl.scan.sum(table.x + table.offset))
    assert [row.total for row in scanned.collect()] == [0, 12, 25]


def test_serial_scan_reuses_aggregate_combinators():
    table = hl.Table.parallelize([{"x": 1}, {"x": 4}, {"x": None}, {"x": 5}], n_partitions=2)
    scanned = table.annotate(
        seen=hl.scan.any(table.x > 2),
        all_defined=hl.scan.all(hl.is_defined(table.x)),
        stats=hl.scan.stats(table.x),
    )
    rows = scanned.collect()
    assert [row.seen for row in rows] == [False, False, True, True]
    assert [row.all_defined for row in rows] == [True, True, True, False]
    assert rows[1].stats.n == 1
    assert rows[1].stats.sum == 1.0


def test_serial_scan_uses_key_order_and_duplicate_key_order():
    table = hl.Table.parallelize(
        [{"k": k, "i": i} for i, k in enumerate([2, 1, 1, 3, 2])],
        n_partitions=3,
    ).key_by("k")
    scanned = table.annotate(prefix=hl.scan.collect(table.i))
    assert [row.prefix for row in scanned.collect()] == [[], [1], [1, 2], [1, 2, 0], [1, 2, 0, 4]]


def test_scan_filter_uses_filtered_prefix_and_partition_summaries():
    table = hl.Table.parallelize(
        [{"i": i, "x": x} for i, x in enumerate([1, 3, None, 4, 2, 5])],
        n_partitions=3,
    )
    scanned = table.annotate(
        total=hl.scan.filter(table.x > 2, hl.scan.sum(table.x)),
        collected=hl.scan.filter(table.x > 2, hl.scan.collect(table.x)),
        values_with_missing=hl.scan.filter(table.i > 0, hl.scan.collect(table.x)),
        stats=hl.scan.filter(table.x > 2, hl.scan.stats(table.x)),
    )
    rows = scanned.collect()
    assert [row.total for row in rows] == [0, 0, 3, 3, 7, 7]
    assert [row.collected for row in rows] == [[], [], [3], [3], [3, 4], [3, 4]]
    assert [row.values_with_missing for row in rows] == [[], [], [3], [3, None], [3, None, 4], [3, None, 4, 2]]
    assert [row.stats.n for row in rows] == [0, 0, 1, 1, 2, 2]
    assert scanned.n_partitions() == 3


def test_scan_filter_does_not_evaluate_excluded_aggregate_arguments():
    table = hl.Table.parallelize([{"i": 0, "s": "bad"}, {"i": 1, "s": "5"}], n_partitions=2)
    scanned = table.annotate(total=hl.scan.filter(table.i > 0, hl.scan.sum(hl.int32(table.s))))
    assert [row.total for row in scanned.collect()] == [0, 0]


def test_scan_filter_predicates_compose():
    table = hl.Table.parallelize([{"i": i} for i in range(4)], n_partitions=2)
    expr = hl.scan.filter(
        table.i > 0,
        hl.scan.filter(table.i < 3, hl.scan.sum(table.i)),
    )
    assert [row.total for row in table.annotate(total=expr).collect()] == [0, 0, 1, 3]



def test_scan_supports_ordered_take_and_set_state():
    table = hl.Table.parallelize([{"x": x} for x in [3, 1, 3, 2]], n_partitions=2)
    scanned = table.annotate(
        first=hl.scan.take(table.x, 2),
        seen=hl.scan.collect_as_set(table.x),
    )
    rows = scanned.collect()
    assert [row.first for row in rows] == [[], [3], [3, 1], [3, 1]]
    assert [row.seen for row in rows] == [set(), {3}, {1, 3}, {1, 3}]


def test_scan_ordered_take_uses_ordering_expression():
    table = hl.Table.parallelize([{"x": x} for x in [3, 1, 3, 2]], n_partitions=2)
    rows = table.annotate(first=hl.scan.take(table.x, 2, ordering=table.x)).collect()
    assert [row.first for row in rows] == [[], [3], [1, 3], [1, 3]]


def test_scan_info_score_matches_prefix_aggregate():
    table = hl.Table.parallelize(
        [{"i": i, "gp": gp} for i, gp in enumerate([[1., 0., 0.], [.25, .5, .25], None, [.1, .2, .7]])],
        schema=hl.tstruct(i=hl.tint32, gp=hl.tarray(hl.tfloat64)), n_partitions=3)
    _assert_scan_matches_prefix_aggregates(
        table, lambda t: hl.scan.info_score(t.gp), lambda t: hl.agg.info_score(t.gp))


def test_scan_fold_initializer_errors_are_lazy_and_filter_keeps_partitions():
    table = hl.Table.parallelize([{"i": i, "x": i + 1} for i in range(6)], n_partitions=4)
    folded = table.annotate(prefix=hl.scan.fold(10, lambda state: state + table.x, lambda a, b: a + b - 10))
    assert [row.prefix for row in folded.filter(folded.i != 0).collect()] == [11, 13, 16, 20, 25]
    assert folded.n_partitions() == 4

    configured = table.annotate_globals(bins="not-an-int")
    result = configured.annotate(hist=hl.scan.hist(configured.x, 0, 10, hl.int32(configured.bins)))
    import pytest
    with pytest.raises(Exception):
        result.collect()


def test_nested_scan_filters_preserve_short_circuit_for_bad_casts():
    table = hl.Table.parallelize(
        [{"i": 0, "s": "bad"}, {"i": 1, "s": "7"}, {"i": 2, "s": "bad"}],
        n_partitions=2)
    value = hl.scan.filter(table.i > 0,
        hl.scan.filter(table.i < 2, hl.scan.sum(hl.int32(table.s))))
    assert [row.value for row in table.annotate(value=value).collect()] == [0, 0, 7]


def test_scan_array_sum_rejects_mismatched_lengths():
    table = hl.Table.parallelize(
        [{"a": [1, 2]}, {"a": [3]}],
        schema=hl.tstruct(a=hl.tarray(hl.tint32)), n_partitions=2)
    import pytest
    for expression in (hl.scan.array_sum(table.a), hl.scan.array_agg(hl.scan.sum, table.a)):
        with pytest.raises(RuntimeError, match="arrays of identical length"):
            table.annotate(prefix=expression).collect()


def test_scan_supports_corr_and_ndarray_sum_states():
    import numpy as np
    table = hl.Table.parallelize(
        [{"x": x, "y": 2 * x + 1} for x in [1.0, 2.0, 3.0, 4.0]],
        n_partitions=2,
    )
    rows = table.annotate(c=hl.scan.corr(table.x, table.y)).collect()
    assert rows[2].c == 1.0
    assert rows[3].c == 1.0

    dtype = hl.tndarray(hl.tfloat64, 1)
    arrays = hl.Table.parallelize(
        [
            {"x": {"__vail_ndarray_data": [1.0, 2.0], "__vail_ndarray_shape": [2]}},
            {"x": {"__vail_ndarray_data": [3.0, 4.0], "__vail_ndarray_shape": [2]}},
        ],
        schema=hl.tstruct(x=dtype),
        n_partitions=2,
    )
    totals = arrays.annotate(total=hl.scan.ndarray_sum(arrays.x)).collect()
    assert totals[0].total is None
    np.testing.assert_array_equal(totals[1].total, np.array([1.0, 2.0]))


def test_scan_cannot_silently_become_total_or_grouped_aggregation():
    import pytest
    table = hl.range_table(3)
    with pytest.raises(ValueError, match='scan expressions cannot'):
        table.aggregate(hl.scan.count())
    with pytest.raises(ValueError, match='scan expressions cannot'):
        table.group_by(k=table.idx % 2).aggregate(n=hl.scan.count()).collect()


def _assert_scan_matches_prefix_aggregates(table, scan_make, aggregate_make):
    import numpy as np
    scanned = table.annotate(prefix=scan_make(table))
    rows = scanned.collect()
    for i, row in enumerate(rows):
        prefix = table.filter(table.i < i)
        expected = prefix.aggregate(aggregate_make(prefix))
        def compare(actual, expected):
            import math
            if isinstance(actual, hl.Struct):
                assert actual.keys() == expected.keys()
                for key in actual: compare(actual[key], expected[key])
            elif isinstance(actual, dict):
                assert actual.keys() == expected.keys()
                for key in actual: compare(actual[key], expected[key])
            elif isinstance(actual, (list, tuple)):
                assert len(actual) == len(expected)
                for a, e in zip(actual, expected): compare(a, e)
            elif isinstance(actual, float):
                assert (math.isnan(actual) and math.isnan(expected)) or math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-10)
            else:
                np.testing.assert_equal(actual, expected)
        compare(row.prefix, expected)
    assert scanned.n_partitions() == table.n_partitions()


def test_scan_configuration_expressions_and_statistical_reducers():
    table = hl.Table.parallelize([{"i": i, "x": float(i), "y": 2.0 * i + 1.0 + [0.1, -0.2, 0.05, 0.15, -0.1, 0.2][i]} for i in range(6)],
                                n_partitions=3).annotate_globals(lo=0.0, hi=6.0, bins=3, qs=[0.0, 0.5, 1.0])
    pairs = [
        (lambda t: hl.scan.hist(t.x, t.lo, t.hi, t.bins), lambda t: hl.agg.hist(t.x, t.lo, t.hi, t.bins)),
        (lambda t: hl.scan.approx_quantiles(t.x, t.qs), lambda t: hl.agg.approx_quantiles(t.x, t.qs)),
        (lambda t: hl.scan.approx_median(t.x), lambda t: hl.agg.approx_median(t.x)),
        (lambda t: hl.scan.linreg(t.y, [1.0, t.x]), lambda t: hl.agg.linreg(t.y, [1.0, t.x])),
        (lambda t: hl.scan.downsample(t.x, t.y), lambda t: hl.agg.downsample(t.x, t.y)),
    ]
    for scan, aggregate in pairs:
        _assert_scan_matches_prefix_aggregates(table, scan, aggregate)


def test_scan_exact_linear_fit_preserves_coefficients_and_negligible_residual():
    table = hl.range_table(6, n_partitions=3)
    rows = table.annotate(fit=hl.scan.linreg(2. * table.idx + 1., [1., hl.float64(table.idx)])).collect()
    for row in rows[3:]:
        assert row.fit.beta == pytest.approx([1., 2.], abs=1e-12)
        assert row.fit.multiple_standard_error == pytest.approx(0., abs=1e-12)
        assert row.fit.multiple_r_squared == pytest.approx(1., abs=1e-12)


def test_scan_genomic_reducers_reuse_aggregate_states():
    table = hl.Table.parallelize([{"i": i, "c": c} for i, c in enumerate([
        hl.Call([0, 0]), hl.Call([0, 1]), None, hl.Call([1, 1]), hl.Call([0, 1]),
    ])], schema=hl.tstruct(i=hl.tint32, c=hl.tcall), n_partitions=3).annotate_globals(alleles=["A", "C"])
    for scan, aggregate in [
        (lambda t: hl.scan.call_stats(t.c, t.alleles), lambda t: hl.agg.call_stats(t.c, t.alleles)),
        (lambda t: hl.scan.hardy_weinberg_test(t.c), lambda t: hl.agg.hardy_weinberg_test(t.c)),
        (lambda t: hl.scan.inbreeding(t.c, 0.5), lambda t: hl.agg.inbreeding(t.c, 0.5)),
    ]:
        _assert_scan_matches_prefix_aggregates(table, scan, aggregate)


def test_scan_explode_filters_and_nested_collections():
    table = hl.Table.parallelize([{"i": i, "a": a} for i, a in enumerate([[1, None, 2], [], None, [3], [4, 5]])],
                                schema=hl.tstruct(i=hl.tint32, a=hl.tarray(hl.tint32)), n_partitions=3)
    exploded_sum = table.annotate(prefix=hl.scan.explode(lambda x: hl.scan.sum(x), table.a))
    _assert_scan_matches_prefix_aggregates(table,
        lambda t: hl.scan.explode(lambda x: hl.scan.sum(x), t.a),
        lambda t: hl.agg.explode(lambda x: hl.agg.sum(x), t.a))
    _assert_scan_matches_prefix_aggregates(table,
        lambda t: hl.scan.explode(lambda x: hl.scan.filter(hl.is_defined(x), hl.scan.collect(x)), t.a),
        lambda t: hl.agg.explode(lambda x: hl.agg.filter(hl.is_defined(x), hl.agg.collect(x)), t.a))
    _assert_scan_matches_prefix_aggregates(table,
        lambda t: hl.scan.explode(lambda x: hl.scan.count(), t.a),
        lambda t: hl.agg.explode(lambda x: hl.agg.count(), t.a))


def test_scan_fold_configuration_and_merge():
    table = hl.Table.parallelize([{"i": i, "x": i + 1} for i in range(6)], n_partitions=3).annotate_globals(initial=7)
    _assert_scan_matches_prefix_aggregates(table,
        lambda t: hl.scan.fold(t.initial, lambda state: state + t.x, lambda left, right: left + right - 7),
        lambda t: hl.agg.fold(t.initial, lambda state: state + t.x, lambda left, right: left + right - 7))


def test_scan_grouped_counter_and_nested_array_scopes():
    table = hl.Table.parallelize([{"i": i, "k": k, "x": i + 1, "a": a} for i, (k, a) in enumerate([
        ("a", [1, 2]), ("b", [3, None]), (None, None), ("a", [5, 6]), ("b", [7, 8]),
    ])], schema=hl.tstruct(i=hl.tint32, k=hl.tstr, x=hl.tint32, a=hl.tarray(hl.tint32)), n_partitions=3).annotate_globals(offset=10)
    for scan, aggregate in [
        (lambda t: hl.scan.group_by(t.k, hl.scan.sum(t.x)), lambda t: hl.agg.group_by(t.k, hl.agg.sum(t.x))),
        (lambda t: hl.scan.counter(t.k), lambda t: hl.agg.counter(t.k)),
        (lambda t: hl.scan.counter(t.k, weight=t.x), lambda t: hl.agg.counter(t.k, weight=t.x)),
        (lambda t: hl.scan.group_by(t.k, hl.scan.stats(t.x)), lambda t: hl.agg.group_by(t.k, hl.agg.stats(t.x))),
        (lambda t: hl.scan.group_by(t.k, hl.scan.sum(t.x) + t.offset), lambda t: hl.agg.group_by(t.k, hl.agg.sum(t.x) + t.offset)),
        (lambda t: hl.scan.group_by(t.k, hl.scan.filter(t.x > 2, hl.scan.collect(t.x))), lambda t: hl.agg.group_by(t.k, hl.agg.filter(t.x > 2, hl.agg.collect(t.x)))),
        (lambda t: hl.scan.array_sum(t.a), lambda t: hl.agg.array_sum(t.a)),
        (lambda t: hl.scan.array_agg(lambda x: hl.scan.stats(x), t.a), lambda t: hl.agg.array_agg(lambda x: hl.agg.stats(x), t.a)),
        (lambda t: hl.scan.array_agg(lambda x: hl.scan.group_by(x, hl.scan.count()), t.a), lambda t: hl.agg.array_agg(lambda x: hl.agg.group_by(x, hl.agg.count()), t.a)),
        (lambda t: hl.scan.group_by(t.k, hl.scan.array_sum(t.a)), lambda t: hl.agg.group_by(t.k, hl.agg.array_sum(t.a))),
    ]:
        _assert_scan_matches_prefix_aggregates(table, scan, aggregate)


def test_scan_result_projection_uses_current_row():
    table = hl.range_table(5, n_partitions=3)
    result = hl.scan.group_by(table.idx % 3, hl.scan.collect(table.idx).append(table.idx))
    assert result.collect() == [
        {}, {0: [0, 1]}, {0: [0, 2], 1: [1, 2]},
        {0: [0, 3], 1: [1, 3], 2: [2, 3]},
        {0: [0, 3, 4], 1: [1, 4], 2: [2, 4]},
    ]
    result = hl.scan.array_agg(lambda x: hl.scan.sum(x) + table.idx, hl.array([table.idx, table.idx + 1]))
    assert result.collect() == [None, [1, 2], [3, 5], [6, 9], [10, 14]]


@pytest.mark.parametrize("partitions", [1, 3, 8])
def test_native_previous_nonnull_arrays_and_empty_partitions(partitions):
    table = hl.Table.parallelize(
        [{"x": x} for x in [None, [1], None, None, [2, 3], None]],
        schema=hl.tstruct(x=hl.tarray(hl.tint32)), n_partitions=partitions,
    )
    result = table.annotate(prev=hl.scan.prev_nonnull(table.x), n=hl.scan.count())
    assert [row.prev for row in result.collect()] == [None, None, [1], [1], [1], [2, 3]]
    assert result.n.collect() == list(range(6))
    assert result.n_partitions() == partitions

    all_missing = hl.Table.parallelize(
        [{"x": None} for _ in range(7)],
        schema=hl.tstruct(x=hl.tarray(hl.tint32)), n_partitions=partitions,
    )
    assert all_missing.annotate(prev=hl.scan.prev_nonnull(all_missing.x)).prev.collect() == [None] * 7


def test_previous_nonnull_optimization_does_not_change_nonmissing_fold_initial_state():
    table = hl.Table.parallelize([{"x": x} for x in [None, 1, None, 2]])
    result = hl.scan.fold(
        -7,
        lambda state: hl.if_else(hl.is_defined(table.x), table.x, state),
        lambda left, right: hl.if_else(hl.is_defined(right), right, left),
    )
    assert result.collect() == [-7, -7, 1, 1]


def test_scan_fold_preserves_missing_initializer_across_partition_boundaries():
    table = hl.Table.parallelize(
        [{"x": value} for value in [None, 2, None, 5]],
        schema=hl.tstruct(x=hl.tint64),
        n_partitions=6,
    )
    result = hl.scan.fold(
        hl.missing(hl.tint64),
        lambda state: hl.if_else(hl.is_defined(table.x), table.x, state),
        lambda left, right: hl.if_else(hl.is_defined(right), right, left),
    )
    assert result.collect() == [None, None, 2, 2]

    empty = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tint64), n_partitions=3)
    empty_result = hl.scan.fold(
        hl.missing(hl.tint64),
        lambda state: hl.if_else(hl.is_defined(empty.x), empty.x, state),
        lambda left, right: hl.if_else(hl.is_defined(right), right, left),
    )
    assert empty_result.collect() == []


def test_scan_prev_nonnull_and_dense_arrays():
    table = hl.range_table(17, n_partitions=8)
    table = table.annotate(x=hl.range(0, table.idx % 25))
    prev = hl.scan.prev_nonnull(hl.or_missing(table.idx % 3 != 0, table.row)).collect()
    assert [None if row is None else (row.idx, row.x) for row in prev] == [
        None if i < 2 else (
            (i - 2 if (i - 1) % 3 == 0 else i - 1),
            list(range((i - 2 if (i - 1) % 3 == 0 else i - 1) % 25)),
        )
        for i in range(17)
    ]

def test_configured_scan_preserves_empty_input():
    table = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tfloat64), n_partitions=3)
    table = table.annotate_globals(lo=0.0, hi=10.0, bins=3)
    scanned = table.annotate(histogram=hl.scan.hist(table.x, table.lo, table.hi, table.bins))
    assert scanned.collect() == []


def test_nested_scan_fold_internal_names_do_not_shadow_user_fields():
    table = hl.Table.parallelize([
        {"a": [1, 2], "__vail_scan_variable_0": 100},
        {"a": [3, 4], "__vail_scan_variable_0": 200},
        {"a": [5, 6], "__vail_scan_variable_0": 300},
    ], n_partitions=2)
    value = hl.scan.array_agg(
        lambda item: hl.scan.fold(0, lambda previous: previous + item + table.__vail_scan_variable_0,
                                 lambda left, right: left + right), table.a)
    assert [row.prefix for row in table.annotate(prefix=value).collect()] == [None, [101, 102], [304, 306]]



def test_grouped_take_of_scan_annotated_row():
    # Scan ordinals belong to the original row stream, before grouping.
    for partitions in (1, 2, 3):
        table = hl.range_table(3, partitions).key_by()
        table = table.annotate(ordinal=hl.scan.count())
        grouped = table.group_by(g=table.idx % 2).aggregate(
            data=hl.agg.take(table.row, 1)[0]
        )
        expected = [
            hl.Struct(g=0, data=hl.Struct(idx=0, ordinal=0)),
            hl.Struct(g=1, data=hl.Struct(idx=1, ordinal=1)),
        ]
        assert grouped.collect() == expected
        assert grouped.collect() == expected
        assert hl.range_table(0, partitions).group_by(g=hl.int32(0)).aggregate(
            data=hl.agg.take(hl.int32(1), 1)[0]
        ).collect() == []


def test_nested_scan_random_predicate_matches_selected_prefix_and_replays():
    table = hl.range_table(73, n_partitions=4)
    selected = hl.rand_bool(0.45, seed=902)
    result = table.annotate(
        selected=selected,
        prefix=hl.scan.filter(selected, hl.scan.filter(True, hl.scan.sum(table.idx + 1))),
    )
    first = result.collect()
    total = 0
    for row in first:
        assert row.prefix == total
        if row.selected:
            total += row.idx + 1
    assert result.collect() == first


def test_nested_scan_missing_outer_predicate_skips_erroring_inner_predicate():
    table = hl.range_table(5, n_partitions=3)
    table = table.annotate(text=hl.if_else(table.idx == 1, "bad", "1"))
    outer = hl.or_missing(table.idx != 1, table.idx % 2 == 0)
    inner = hl.int32(table.text) > 0
    result = table.annotate(prefix=hl.scan.filter(outer, hl.scan.filter(inner, hl.scan.sum(table.idx + 1))))
    assert [row.prefix for row in result.collect()] == [0, 1, 1, 4, 4]


def test_scan_marking_preserves_shared_arguments_and_closed_literal_payloads():
    table = hl.range_table(4, n_partitions=3)
    shared = table.idx
    for _ in range(10):
        shared = shared + shared
    initial = hl.Struct(op='aggregate', function='sum', options=hl.Struct(__vail_scan=False))
    value = hl.scan.filter(True, hl.scan.fold(initial, lambda state: state, lambda left, right: left))
    rows = table.annotate(total=hl.scan.sum(shared), state=value).collect()
    assert [row.total for row in rows] == [0, 0, 1024, 3072]
    assert [row.state for row in rows] == [initial] * 4


def test_scan_configuration_is_lazy_and_reread_on_each_action(tmp_path):
    path, moved = tmp_path / 'configuration', tmp_path / 'moved'
    hl.range_table(0).annotate_globals(bins=2).write(path)
    captured = hl.read_table(path).index_globals()
    table = hl.range_table(4, n_partitions=3)
    path.rename(moved)
    scanned = table.annotate(hist=hl.scan.hist(table.idx, 0., 4., captured.bins))
    assert scanned.head(0).collect() == []
    with pytest.raises(FileNotFoundError):
        scanned.collect()
    moved.rename(path)
    assert [row.hist.bin_freq for row in scanned.collect()] == [[0, 0], [1, 0], [2, 0], [2, 1]]
    path.rename(moved)
    with pytest.raises(FileNotFoundError):
        scanned.collect()


def test_scan_fold_configuration_preserves_nested_call_metadata():
    initial = hl.Struct(calls=[hl.Call([0, 1], phased=True), None], label='seed')
    table = hl.range_table(4, n_partitions=3).annotate_globals(initial=initial)
    expression = hl.scan.fold(table.initial, lambda state: state, lambda left, right: left)
    assert [row.prefix for row in table.annotate(prefix=expression).collect()] == [initial] * 4


@pytest.mark.parametrize('n,partitions', [(10000, 3), (100000, 64)])
@pytest.mark.parametrize('reducer', ['sum', 'mean'])
def test_scan_batch_crossing_partitions_preserve_source_order_and_boundaries(n, partitions, reducer):
    import math
    table = hl.range_table(n, n_partitions=partitions).key_by()
    table = table.annotate(x=table.idx % 13 - 6)
    expression = getattr(hl.scan, reducer)(table.x)
    scanned = table.annotate(prefix=expression)
    expected = []
    total = 0
    for i in range(n):
        expected.append(total if reducer == 'sum' else total / i if i else math.nan)
        total += i % 13 - 6
    def check(rows, start):
        for i, row in enumerate(rows, start):
            assert row.idx == i
            assert row.x == i % 13 - 6
            if math.isnan(expected[i]):
                assert math.isnan(row.prefix)
            else:
                assert row.prefix == pytest.approx(expected[i], rel=1e-11, abs=1e-11)
    for _ in range(2):
        rows = scanned.collect()
        assert len(rows) == n
        check(rows, 0)
        check(scanned.head(8).collect(), 0)
        check(scanned.tail(3).collect(), n - 3)


def test_scan_tail_handles_oversized_limits_without_ordinal_overflow():
    table = hl.range_table(5, n_partitions=3)
    scanned = table.annotate(prefix=hl.scan.sum(table.idx))
    expected = [(i, i*(i-1)//2) for i in range(5)]
    for limit in [0, 1, 5, 6, 2**63 - 1, 2**64 - 1]:
        rows = scanned.tail(limit).collect()
        assert [(row.idx, row.prefix) for row in rows] == (expected[-limit:] if limit else [])


def test_repeated_sum_scan_result_is_reused_in_scalar_expressions():
    table = hl.range_table(6, n_partitions=3)
    prefix = hl.scan.sum(table.idx)
    result = table.annotate(a=prefix, b=prefix + 1)
    for _ in range(2):
        rows = result.collect()
        assert [row.a for row in rows] == [0, 0, 1, 3, 6, 10]
        assert [row.b for row in rows] == [1, 1, 2, 4, 7, 11]
