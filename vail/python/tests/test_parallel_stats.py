"""Population statistics should use native partitioned aggregate kernels."""
import json

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

    _assert_parallel_aggregate(_physical_plan(table, expression))
    result = table.aggregate(expression)
    assert result.n == 3
    assert result.sum == 8.0
    assert result.mean == pytest.approx(8.0 / 3.0)
    assert result.stdev == pytest.approx((14.0 / 9.0) ** 0.5)
    assert result.min == 1.0
    assert result.max == 4.0


def test_parallel_stats_components_and_empty_input():
    table = hl.range_table(12, 4)
    query = hl.struct(
        count=hl.agg.count_where(table.idx % 2 == 0),
        mean=hl.agg.mean(table.idx),
        stats=hl.agg.stats(table.idx),
    )
    _assert_parallel_aggregate(_physical_plan(table, query))
    result = table.aggregate(query)
    assert result.count == 6
    assert result.mean == pytest.approx(5.5)
    assert result.stats.n == 12
    assert result.stats.sum == 66.0
    assert result.stats.stdev == pytest.approx((143.0 / 12.0) ** 0.5)

    empty = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tfloat64), n_partitions=4)
    empty_stats = hl.agg.stats(empty.x)
    _assert_parallel_aggregate(_physical_plan(empty, empty_stats))
    empty_result = empty.aggregate(empty_stats)
    assert empty_result.n == 0
    assert empty_result.sum == 0.0
    assert empty_result.min is None
    assert empty_result.max is None


def test_order_sensitive_aggregate_still_uses_ordered_input():
    table = hl.range_table(12, 4).key_by()
    plan = _physical_plan(table, hl.agg.collect(table.idx))
    assert "OrderedConcatExec" in plan
    assert "mode=Partial" not in plan
