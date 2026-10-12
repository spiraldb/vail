"""Optimizer visibility and semantics across independently executable partitions."""
import vail as hl


def test_row_pipeline_is_one_logical_tree():
    table = hl.range_table(12, n_partitions=3)
    table = table.annotate(x=table.idx + 1)
    table = table.filter(table.x > 5).select('x')
    plan = table._native.logical_plan()
    assert 'Projection:' in plan
    assert 'Filter:' in plan
    assert 'TableScan:' in plan
    assert 'VailPartitionOperation: MapRows' not in plan
    assert 'VailPartitionOperation: Filter' not in plan
    assert [r.x for r in table.collect()] == list(range(6, 13))


def test_grouping_with_globals_uses_logical_aggregate():
    table = hl.range_table(12, n_partitions=3).annotate_globals(offset=10)
    grouped = table.group_by(g=table.idx % 2).aggregate(
        values=hl.agg.collect(table.idx + table.offset),
        n=hl.agg.count(),
    )
    plan = grouped._native.logical_plan()
    assert 'Aggregate:' in plan
    assert 'VailPartitionOperation: Aggregate' not in plan
    rows = grouped.collect()
    assert [(r.g, r.n, r["values"]) for r in rows] == [
        (g, 6, list(range(g + 10, 22, 2))) for g in range(2)
    ]


def test_total_collect_preserves_partition_order():
    table = hl.range_table(30, n_partitions=5)
    assert table.aggregate(hl.agg.collect(table.idx)) == list(range(30))
    assert table.aggregate(hl.agg.take(table.idx, 7)) == list(range(7))


def test_row_and_global_with_same_name_in_aggregation():
    table = hl.range_table(5, n_partitions=2).key_by().annotate_globals(members='global')
    grouped = table._group_within_partitions('members', 2)
    result = grouped.aggregate(hl.struct(
        global_value=grouped.globals.members,
        n=hl.agg.count(),
    ))
    assert result.global_value == 'global'
    assert result.n == 3


def test_global_only_aggregate_does_not_read_rows(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(4, n_partitions=2).annotate_globals(label='ready').write(path)
    table = hl.read_table(path)
    for fragment in path.rglob('*.vortex'):
        fragment.unlink()
    assert table.aggregate(table.globals.label) == 'ready'


def test_checked_cast_respects_preceding_filter():
    import pytest

    table = hl.Table.parallelize([{'value': 'header'}, {'value': '3'}])
    filtered = table.filter(table.value != 'header')
    parsed = filtered.annotate(number=hl.int32(filtered.value))
    assert parsed.filter(parsed.number > 1).collect() == [hl.Struct(value='3', number=3)]
    invalid = table.annotate(number=hl.int32(table.value))
    with pytest.raises(RuntimeError, match='[Cc]ast|pars'):
        invalid.collect()
