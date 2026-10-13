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
    middle = scanned._filter_partitions([1]).collect()
    assert [r.i for r in middle] == [2, 3]
    assert scanned.filter(scanned.i == 5).collect()[0].n == 5


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


def test_scan_row_windows_use_datafusion_partitioned_execution():
    table = hl.Table.parallelize([{"x": i} for i in range(12)], n_partitions=3)
    scanned = table.annotate(prefix=hl.scan.sum(table.x))
    plan = scanned._native.physical_plan()
    assert "BoundedWindowAggExec" in plan
    assert "RepartitionExec: partitioning=Hash([__vail_partition" in plan
    assert "input_partitions=3" in plan


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
    assert "VailProtectedRangeRepartitionExec" in scanned._native.physical_plan()


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
    assert [row.prefix for row in folded._filter_partitions([1, 3]).collect()] == [11, 13, 20, 25]
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
    table = hl.Table.parallelize([{"i": i, "x": float(i), "y": 2.0 * i + 1.0} for i in range(6)],
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


def test_scan_prev_nonnull_and_dense_arrays():
    table = hl.range_table(17, n_partitions=8)
    prev = hl.scan._prev_nonnull(hl.or_missing(table.idx % 3 != 0, table.row)).collect()
    assert [None if row is None else row.idx for row in prev] == [
        None if i < 2 else i - 2 if (i - 1) % 3 == 0 else i - 1 for i in range(17)
    ]
    table = hl.range_table(6, n_partitions=4).annotate_globals(length=6)
    sparse = hl.range(6).map(lambda i: hl.or_missing(i == table.idx, i))
    result = table.annotate(dense=hl.scan._densify(table.length, sparse))
    assert [row.dense for row in result.collect()] == [[i if i < row else None for i in range(6)] for row in range(6)]


def test_configured_scan_preserves_empty_execution_partitions():
    table = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tfloat64), n_partitions=3)
    table = table.annotate_globals(lo=0.0, hi=10.0, bins=3)
    scanned = table.annotate(histogram=hl.scan.hist(table.x, table.lo, table.hi, table.bins))
    assert scanned.collect() == []
    assert scanned.n_partitions() == 3
    for partition in range(3):
        assert scanned._filter_partitions([partition]).collect() == []


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


def test_nonlocalized_aggregation_also_rejects_scans():
    import pytest
    table = hl.range_table(3)
    with pytest.raises(ValueError, match="scan expressions cannot"):
        table.aggregate(hl.scan.count(), _localize=False)


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
