"""Population statistics should preserve aggregate semantics across partitions."""

import pytest
import vail as hl


def _physical_plan(table, expression):
    return table._native.aggregate_physical_plan(expression.to_ir())


def _assert_parallel_aggregate(plan):
    assert "mode=Partial" in plan
    assert "mode=Final" in plan
    assert "OrderedConcatExec" not in plan


def test_stats_uses_partitioned_native_aggregates_and_ignores_missing_values():
    table = hl.Table.parallelize(
        [{"x": 1.0}, {"x": None}, {"x": 3.0}, {"x": 4.0}],
        schema=hl.tstruct(x=hl.tfloat64),
        n_partitions=4,
    )
    expression = hl.agg.stats(table.x)

    result = table.aggregate(expression)
    assert result.n == 3
    assert result.sum == 8.0
    assert result.mean == pytest.approx(8.0 / 3.0)
    assert result.stdev == pytest.approx((14.0 / 9.0) ** 0.5)
    assert result.min == 1.0
    assert result.max == 4.0


def test_total_stats_components_and_empty_input():
    table = hl.range_table(12, 4)
    query = hl.struct(
        count=hl.agg.count_where(table.idx % 2 == 0),
        mean=hl.agg.mean(table.idx),
        stats=hl.agg.stats(table.idx),
    )
    result = table.aggregate(query)
    assert result.count == 6
    assert result.mean == pytest.approx(5.5)
    assert result.stats.n == 12
    assert result.stats.sum == 66.0
    assert result.stats.stdev == pytest.approx((143.0 / 12.0) ** 0.5)

    empty = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tfloat64), n_partitions=4)
    empty_stats = hl.agg.stats(empty.x)
    empty_result = empty.aggregate(empty_stats)
    assert empty_result.n == 0
    assert empty_result.sum == 0.0
    assert empty_result.min is None
    assert empty_result.max is None


def test_total_collect_preserves_source_sequence():
    table = hl.range_table(12, 4).key_by()
    assert table.aggregate(hl.agg.collect(table.idx)) == list(range(12))


def test_single_source_totals_preserve_globals_and_partition_semantics():
    table = hl.range_table(1_000_003, n_partitions=1).annotate_globals(offset=5)
    external = hl.range_table(0).annotate_globals(offset=7).index_globals()
    predictor = hl.float64(table.idx) / 1_000_003
    query = hl.struct(
        total=hl.agg.sum(table.idx) + table.offset + external.offset,
        stats=hl.agg.stats(table.idx),
        fit=hl.agg.linreg(2.0 + 3.0 * predictor, [1.0, predictor]),
    )
    result = table.aggregate(query)
    assert result.total == 1_000_003 * 1_000_002 // 2 + 12
    assert result.stats.n == 1_000_003
    assert result.fit.n == 1_000_003
    assert result.fit.beta[0] == pytest.approx(2.0, abs=1e-6)
    assert result.fit.beta[1] == pytest.approx(3.0, abs=1e-6)
    assert table.n_partitions() == 1

    # Scan values are computed in source order before aggregation fanout.
    scanned = hl.range_table(12, n_partitions=1)
    scanned = scanned.annotate(
        previous=hl.scan._prev_nonnull(
            hl.or_missing(scanned.idx % 3 != 0, scanned.idx)
        )
    )
    assert scanned.aggregate(hl.agg.sum(scanned.previous)) == 52
    assert scanned.n_partitions() == 1


def test_total_collect_and_take_preserve_input_order():
    table = hl.range_table(12, n_partitions=1).key_by()
    for expression, expected in (
        (hl.agg.collect(table.idx), list(range(12))),
        (hl.agg.take(table.idx, 5), list(range(5))),
    ):
        assert table.aggregate(expression) == expected


def test_totals_handle_empty_inputs_globals_and_initialized_reducers():
    empty = hl.range_table(0, n_partitions=1)
    assert empty.aggregate(hl.agg.sum(empty.idx)) == 0
    no_partitions = hl.range_table(12, n_partitions=3)._filter_partitions([])
    assert no_partitions.n_partitions() == 0
    assert no_partitions.aggregate(hl.agg.sum(no_partitions.idx)) == 0

    # Hist binds bin parameters through the initialized-aggregate planner;
    # the runtime lowering must still apply the same safe fanout.
    table = hl.range_table(97, n_partitions=1)
    histogram = hl.agg.hist(table.idx, 0, 97, 4)
    result = table.aggregate(histogram)
    assert result.bin_freq == [25, 24, 24, 24]


def test_nested_total_collect_preserves_input_order():
    table = hl.range_table(12, n_partitions=1).key_by()
    expression = hl.agg.array_agg(lambda i: hl.agg.collect(table.idx + i), hl.range(2))
    assert table.aggregate(expression) == [list(range(12)), list(range(1, 13))]
