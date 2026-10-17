"""Partition execution preserves Hail semantics while using parallel DF plans."""
import json

import pytest
import vail as hl


def test_total_reductions_use_native_partial_and_final_aggregation():
    t = hl.range_table(97, 40)
    t = t.annotate(value=hl.int64(t.idx) * 3)
    expression = hl.agg.sum(t.value)
    plan = t._native.aggregate_physical_plan(expression.to_ir())
    assert "mode=Partial" in plan
    assert "mode=Final" in plan
    assert "OrderedConcatExec" not in plan
    assert t.aggregate(expression) == sum(i * 3 for i in range(97))
    assert t.aggregate(hl.struct(n=hl.agg.count(), lo=hl.agg.min(t.idx), hi=hl.agg.max(t.idx))) == hl.Struct(n=97, lo=0, hi=96)


def test_ordered_collection_take_and_partition_boundaries_are_preserved():
    t = hl.range_table(97, 40).key_by()
    assert [r.idx for r in t.collect()] == list(range(97))
    assert t.aggregate(hl.agg.collect(t.idx)) == list(range(97))
    assert t.aggregate(hl.agg.take(t.idx, 7)) == list(range(7))
    assert t.n_partitions() == 40
    assert t._force_count() == 97
    assert [r.idx for r in t.head(3).collect()] == [0, 1, 2]
    assert t.aggregate(hl.agg.sum(t.idx), _localize=False).dtype == hl.tint64


def test_total_collect_keeps_order_and_grouped_collect_is_unordered():
    t = hl.range_table(97, 40).key_by()
    result = t.aggregate(hl.struct(total=hl.agg.sum(t.idx), rows=hl.agg.collect(t.idx)))
    assert result.total == sum(range(97))
    assert result.rows == list(range(97))
    g = t.group_by(parity=t.idx % 2).aggregate(total=hl.agg.sum(t.idx), rows=hl.agg.collect(t.idx))
    for row in g.collect():
        expected = list(range(row.parity, 97, 2))
        assert sorted(row.rows) == expected
        assert row.total == sum(expected)


def test_parallel_row_execution_propagates_errors_and_can_be_replayed():
    t = hl.range_table(97, 40)
    bad = t.annotate(value=hl.int32(hl.if_else(t.idx == 96, "invalid", hl.str(t.idx))))
    with pytest.raises(Exception, match="invalid|cast|parse"):
        bad.collect()
    with pytest.raises(Exception, match="invalid|cast|parse"):
        bad._force_count()
    assert [row.idx for row in t.collect()] == list(range(97))
    assert [row.idx for row in t.collect()] == list(range(97))


def test_parallel_execution_handles_empty_input_and_matrix_row_lists():
    empty = hl.range_table(0, 4)
    assert empty.collect() == []
    assert empty._force_count() == 0
    assert empty.aggregate(hl.agg.sum(empty.idx)) == 0
    assert empty.aggregate(hl.agg.collect(empty.idx)) == []
    m = hl.range_matrix_table(97, 5, 40)
    m = m.annotate_entries(value=m.row_idx + m.col_idx)
    assert m.aggregate_entries(hl.agg.sum(m.value)) == 5 * sum(range(97)) + 97 * sum(range(5))
    assert m._force_count_rows() == 97
