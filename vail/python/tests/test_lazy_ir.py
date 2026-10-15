"""Exercise laziness at observable IO boundaries, not just node names."""
import json
from pathlib import Path

import pytest
import vail as hl


def test_build_graph_with_unreadable_data(tmp_path):
    path = tmp_path / 'table'
    hl.range_table(12, n_partitions=3).write(path)
    files = list(path.rglob('*.vortex'))
    saved = [(p, p.read_bytes()) for p in files]
    for p in files:
        p.unlink()
    # Metadata/type inspection and every transformation must succeed even though
    # no data file exists. Actual execution must fail until the files return.
    source = hl.read_table(path)
    mapped = source.annotate(twice=source.idx * 2)
    filtered = mapped.filter(mapped.twice >= 8)
    result = filtered.select('twice').head(3)
    reverse = mapped.order_by(hl.desc(mapped.idx)).tail(2)
    assert source.n_partitions() == 3
    assert source.row.dtype == hl.tstruct(idx=hl.tint32)
    with pytest.raises(FileNotFoundError):
        result.collect()
    for p, data in saved:
        p.write_bytes(data)
    assert [r.twice for r in result.collect()] == [8, 10, 12]
    assert [r.idx for r in reverse.collect()] == [1, 0]
    assert [r.idx for r in source.collect()] == list(range(12))


def test_actions_reexecute_instead_of_caching(tmp_path):
    path = tmp_path / 'table'
    hl.range_table(3).write(path)
    table = hl.read_table(path)
    assert len(table.collect()) == 3
    next(path.rglob('*.vortex')).unlink()
    with pytest.raises(FileNotFoundError):
        table.collect()
    with pytest.raises(FileNotFoundError):
        table._force_count()
    with pytest.raises(FileNotFoundError):
        table.write(tmp_path / 'copy')
    assert not (tmp_path / 'copy').exists()


def test_read_pins_snapshot_across_overwrite(tmp_path):
    path = tmp_path / 'table'
    hl.range_table(7, n_partitions=3).write(path)
    original = hl.read_table(path)
    branch = original.annotate(y=original.idx + 10).tail(2)
    hl.range_table(2).write(path, overwrite=True)
    assert [r.idx for r in original.collect()] == list(range(7))
    assert [r.y for r in branch.collect()] == [15, 16]
    assert [r.idx for r in hl.read_table(path).collect()] == [0, 1]


def test_head_does_not_open_later_fragments(tmp_path):
    path = tmp_path / 'table'
    hl.range_table(12, n_partitions=3).write(path)
    # The certified index, not UUID/manifest enumeration, determines the order.
    index_path = next(p for p in path.rglob('*.json')
                      if isinstance((value := json.loads(p.read_text())), dict)
                      and 'fragments' in value)
    index = json.loads(index_path.read_text())
    for fragment in index['fragments'][1:]:
        Path(fragment['identity']['file_path'].removeprefix('file://')).unlink()
    table = hl.read_table(path)
    mapped = table.annotate(y=table.idx + 1)
    assert [r.y for r in mapped.head(3).collect()] == [1, 2, 3]
    assert mapped.head(0).collect() == []
    assert mapped.tail(0).collect() == []
    with pytest.raises(FileNotFoundError):
        mapped.collect()


def test_large_range_is_a_small_plan_and_head_pulls_bounded_batches():
    table = hl.range_table(2**31 - 1, n_partitions=1)
    branch = table.annotate(y=table.idx + 5)
    assert branch.count() == 2**31 - 1  # cardinality metadata, no execution
    assert [r.y for r in branch.head(3).collect()] == [5, 6, 7]
    ir = branch._native.ir()
    assert ir['node'] == 'TableMapRows'
    assert ir['child']['node'] == 'TableRange'
    assert ir['fields'][-1][1]['op'] == 'binary'


def test_parallelize_context_and_branches_are_immutable():
    rows = [{'x': 1}, {'x': 2}, {'x': 3}]
    table = hl.Table.parallelize(rows, n_partitions=2)
    rows[0]['x'] = 99
    left = table.annotate(y=table.x + 10)
    right = table.filter(table.x > 1)
    assert [r.x for r in table.collect()] == [1, 2, 3]
    assert [r.y for r in left.collect()] == [11, 12, 13]
    assert [r.x for r in right.collect()] == [2, 3]
    assert left._native.ir()['child']['node'] == 'TableParallelize'


def test_scalar_evaluation_is_deferred_until_an_action():
    table = hl.Table.parallelize([{'a': [1]}], schema=hl.tstruct(a=hl.tarray(hl.tint32)))
    invalid = table.annotate(x=table.a[10])
    assert invalid.row.dtype == hl.tstruct(a=hl.tarray(hl.tint32), x=hl.tint32)
    assert invalid.head(0).collect() == []
    with pytest.raises(RuntimeError, match='out of bounds'):
        invalid.collect()
