import pytest
import vail as hl


def test_computed_interval_union_and_complement():
    table = hl.range_table(100, n_partitions=4)
    intervals = [hl.Interval(50, 60), hl.Interval(10, 30), hl.Interval(20, 35)]
    filtered = hl.filter_intervals(table, intervals)
    assert [r.idx for r in filtered.collect()] == list(range(10, 35)) + list(range(50, 60))
    assert hl.filter_intervals(filtered, [hl.Interval(25, 40)]).count() == 10
    assert hl.filter_intervals(table, intervals, keep=False).count() == 65
    assert hl.filter_intervals(table, []).count() == 0
    assert hl.filter_intervals(table, [], keep=False).count() == 100


def test_prefix_and_expression_intervals():
    table = hl.range_table(10).annotate(other=0).key_by('idx', 'other')
    assert hl.filter_intervals(table, [hl.Interval(2, 5)]).count() == 3
    intervals = hl.literal([hl.Interval(hl.Struct(idx=2), hl.Struct(idx=5))])
    assert hl.filter_intervals(table, intervals).count() == 3
    with pytest.raises(TypeError):
        hl.filter_intervals(table, [None])
    with pytest.raises(TypeError):
        hl.filter_intervals(table, [hl.Interval('a', 'z')])
    with pytest.raises(ValueError):
        hl.filter_intervals(table.key_by(), [hl.Interval(2, 5)])


def test_expression_interval_values_and_globals():
    table = hl.range_table(10).annotate_globals(start=2, stop=5)
    globals_ = table.index_globals()
    dynamic_interval = hl.interval(globals_.start, globals_.stop)
    assert [row.idx for row in hl.filter_intervals(table, [dynamic_interval]).collect()] == [2, 3, 4]
    mixed = [hl.Interval(0, 1), dynamic_interval]
    assert [row.idx for row in hl.filter_intervals(table, mixed).collect()] == [0, 2, 3, 4]

    keyed = table.annotate(other=0).key_by('idx', 'other')
    prefix_interval = hl.interval(
        hl.struct(idx=globals_.start), hl.struct(idx=globals_.stop))
    assert [row.idx for row in hl.filter_intervals(keyed, [prefix_interval]).collect()] == [2, 3, 4]
    assert hl.filter_intervals(table, [dynamic_interval], keep=False).count() == 7


def test_expression_intervals_reject_missing_and_row_values():
    table = hl.range_table(4)
    missing_array = hl.missing(hl.tarray(hl.tinterval(hl.tint32)))
    with pytest.raises(TypeError, match='missing array'):
        hl.filter_intervals(table, missing_array)
    with pytest.raises(TypeError, match='missing or non-Interval'):
        hl.filter_intervals(table, [hl.missing(hl.tinterval(hl.tint32))])
    with pytest.raises(ValueError, match='row-indexed'):
        hl.filter_intervals(table, [hl.interval(table.idx, table.idx + 1)])


def test_multifile_read_and_endpoints(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(20, n_partitions=4).write(path)
    table = hl.read_table(path)
    selected = hl.filter_intervals(table, [hl.Interval(5, 10, False, True)])
    assert selected._native.ir_json().find('TableReadIntervals') >= 0
    assert [r.idx for r in selected.collect()] == [6, 7, 8, 9, 10]
    assert hl.filter_intervals(table, [hl.Interval(5, 10)], keep=False).count() == 15


def test_interval_pruning_does_not_open_unrelated_files(tmp_path):
    import json
    from pathlib import Path

    path = tmp_path / 'source'
    hl.range_table(20, n_partitions=4).write(path)
    index_path = next(p for p in path.rglob('*.json')
                      if isinstance((value := json.loads(p.read_text())), dict)
                      and 'fragments' in value)
    fragments = json.loads(index_path.read_text())['fragments']
    assert len(fragments) >= 4
    for fragment in fragments[1:]:
        Path(fragment['identity']['file_path'].removeprefix('file://')).unlink()
    table = hl.read_table(path)
    assert [r.idx for r in hl.filter_intervals(table, [hl.Interval(1, 3)]).collect()] == [1, 2]
    with pytest.raises(FileNotFoundError):
        table.collect()


@pytest.mark.parametrize('point_type,values', [
    (hl.ttuple(hl.tint32, hl.tint32), [(1, 0), (2, 0), (3, 0)]),
    (hl.tarray(hl.tint32), [[1], [2], [3]]),
    (hl.tstr, ['a', 'b', 'c']),
])
def test_nested_first_key(point_type, values, tmp_path):
    table = hl.Table.parallelize([{'k': value} for value in values],
                                schema=hl.tstruct(k=point_type), key='k')
    intervals = [hl.Interval(values[0], values[2], point_type=point_type)]
    assert [r.k for r in hl.filter_intervals(table, intervals).collect()] == values[:2]
    path = tmp_path / 'table'
    table.write(path)
    assert [r.k for r in hl.filter_intervals(hl.read_table(path), intervals).collect()] == values[:2]


def test_empty_expression_still_validates_point_type():
    table = hl.range_table(3)
    assert hl.filter_intervals(table, hl.literal([], hl.tarray(hl.tinterval(hl.tint32)))).count() == 0
    with pytest.raises(TypeError):
        hl.filter_intervals(table, hl.literal([], hl.tarray(hl.tinterval(hl.tstr))))
    assert hl.utils.Interval is hl.Interval
