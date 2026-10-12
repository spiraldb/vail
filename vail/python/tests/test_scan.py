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
