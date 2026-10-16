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


def test_stable_sort_head_and_tail_preserve_key_order():
    table = hl.range_table(18, n_partitions=3)
    table = table.annotate(group=table.idx % 3).key_by('group')
    first = table.head(4)
    assert [(row.group, row.idx) for row in first.collect()] == [(0, 0), (0, 3), (0, 6), (0, 9)]
    assert [(row.group, row.idx) for row in table.tail(4).collect()] == [(2, 8), (2, 11), (2, 14), (2, 17)]


def test_partition_stream_filter_and_map_preserve_values():
    table = hl.range_table(12, n_partitions=3).key_by()
    mapped = table._map_partitions(lambda rows: rows.filter(lambda row: row.idx > 4).map(lambda row: hl.struct(value=row.idx * 2)))
    assert [row.value for row in mapped.collect()] == list(range(10, 24, 2))


def test_coordinate_sum_preserves_float_results_and_type():
    table = hl.range_table(3, n_partitions=2).annotate(xs=[0.25, 0.75])
    expression = hl.agg.array_agg(lambda value: hl.agg.sum(value), table.xs)
    assert expression.dtype == hl.tarray(hl.tfloat64)
    assert table.aggregate(expression) == [0.75, 2.25]


def test_random_source_repartition_prepasses_and_repeated_actions():
    table = hl.range_table(40, n_partitions=3)
    table = table.annotate(random=hl.rand_unif(seed=41))
    table = table.filter(table.idx % 2 == 0).repartition(4)
    first = table.collect()
    assert [row.idx for row in first] == list(range(0, 40, 2))
    assert table.collect() == first


def test_native_sort_order_survives_later_globals_binding():
    table = hl.Table.parallelize([{'id': 0, 'x': 2}, {'id': 1, 'x': 1}],
                                key='id', n_partitions=2).annotate_globals(offset=10)
    ordered = table.order_by(table.x + table.offset)
    assert [row.id for row in ordered.collect()] == [1, 0]
    assert [row.id for row in ordered.annotate(g=ordered.offset).collect()] == [1, 0]


def test_coordinate_sum_widens_native_int32_without_frontend_cast():
    table = hl.range_table(3, n_partitions=2).annotate(xs=[1, 2])
    assert table.aggregate(hl.agg.array_agg(lambda value: hl.agg.sum(value), table.xs)) == [3, 6]


def test_independent_aggregate_fork_preserves_random_producer_fanout_after_action():
    table = hl.range_table(12, n_partitions=3).annotate(x=hl.rand_int64(seed=717))
    assert table._force_count() == 12
    table = table.annotate(r=hl.rand_int64(seed=718))
    expression = hl.struct(values=hl.agg.collect_as_set(table.r), count=hl.agg.count())
    first = table.aggregate(expression)
    assert first.count == 12
    assert len(first["values"]) == first.count
    assert table.aggregate(expression) == first
