import json
import subprocess
import sys
from pathlib import Path

import pytest
import vail as hl


def _fragment_files(path):
    index = next(
        value for p in path.rglob('*.json')
        if isinstance(value := json.loads(p.read_text()), dict) and 'fragments' in value
    )
    return [Path(f['identity']['file_path'].removeprefix('file://')) for f in index['fragments']]


@pytest.mark.parametrize('partition', [0, 1, 2])
@pytest.mark.parametrize('narrow', [False, True])
def test_indexed_partition_does_not_open_other_partitions(tmp_path, partition, narrow):
    path = tmp_path / 'indexed'
    hl.range_table(12, 3).write(path)
    loaded = hl.read_table(path)
    files = _fragment_files(path)
    assert len(files) == 3
    for i, file in enumerate(files):
        if i != partition:
            file.unlink()
    lower, upper = (partition * 4 + 1, partition * 4 + 4) if narrow else (0, 12)
    filtered = loaded.filter(
        (loaded.idx >= lower) & (loaded.idx < upper) & (loaded.idx % 2 == 0))
    selected = filtered._filter_partitions([partition])
    assert 'VailIndexedRead' in selected._native.physical_plan()
    expected = [i for i in range(partition * 4, partition * 4 + 4)
                if lower <= i < upper and i % 2 == 0]
    assert [r.idx for r in selected.collect()] == expected
    if not narrow:
        with pytest.raises(FileNotFoundError):
            filtered.collect()


def test_indexed_partition_keeps_key_pruning_within_row_range(tmp_path):
    path = tmp_path / 'indexed'
    hl.range_table(8, 2).write(path)
    loaded = hl.read_table(path)
    for file in _fragment_files(path):
        file.unlink()
    # Partition zero's file is also absent, but cannot match this key query.
    selected = loaded.filter(loaded.idx >= 4)._filter_partitions([0])
    assert 'VailIndexedRead' in selected._native.physical_plan()
    assert selected.collect() == []


def test_multifile_roundtrip(tmp_path):
    path = tmp_path / 'table'
    table = hl.range_table(12, n_partitions=3)
    assert table.write(path) is None
    loaded = hl.read_table(path)
    assert loaded.collect() == table.collect()
    assert loaded.row.dtype == table.row.dtype
    assert loaded.key.dtype == table.key.dtype
    assert loaded.n_partitions() == 3
    assert len(list(path.rglob('*.vortex'))) == 3
    assert loaded.filter(loaded.idx >= 8).collect() == table.tail(4).collect()
    result = subprocess.run([sys.executable, '-c',
        'import vail, sys; t=vail.read_table(sys.argv[1]); assert [r.idx for r in t.collect()] == list(range(12))',
        str(path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('table', [
    hl.range_table(0),
    hl.range_table(5).key_by().select(),
    hl.range_table(0).key_by().select(),
    hl.Table.parallelize([{'s': 'β', 'a': [1, None], 'nested': {'b': True}},
                          {'s': None, 'a': [], 'nested': None}],
                        schema=hl.tstruct(s=hl.tstr, a=hl.tarray(hl.tint32), nested=hl.tstruct(b=hl.tbool)),
                        n_partitions=3),
])
def test_roundtrip_shapes(tmp_path, table):
    path = tmp_path / 'table'
    table.write(path)
    loaded = hl.read_table(path)
    assert loaded.collect() == table.collect()
    assert loaded.row.dtype == table.row.dtype
    assert loaded.key.dtype == table.key.dtype
    assert loaded.n_partitions() == table.n_partitions()
    assert loaded.count() == table.count()


def test_overwrite_and_failed_overwrite(tmp_path):
    path = tmp_path / 'table'
    hl.range_table(4, n_partitions=2).write(path)
    original = (path / '_vail_table.json').read_bytes()
    with pytest.raises(FileExistsError):
        hl.range_table(1).write(path)
    assert (path / '_vail_table.json').read_bytes() == original
    unicode_key = hl.Table.parallelize([{'s': 'β'}]).key_by('s')
    unicode_key.write(path, overwrite=True)
    assert hl.read_table(path).collect() == unicode_key.collect()
    hl.range_table(2).write(path, overwrite=True)
    assert hl.read_table(path).count() == 2
    assert len(list(path.rglob('*.vortex'))) == 4  # old generations remain readable


def test_paths_and_errors(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hl.range_table(2).write('relative')
    assert hl.read_table((tmp_path / 'relative').as_uri()).count() == 2
    with pytest.raises(FileNotFoundError):
        hl.read_table(tmp_path / 'missing')
    with pytest.raises(NotImplementedError):
        hl.range_table(1).write('s3://bucket/table')
    with pytest.raises(NotImplementedError):
        hl.range_table(1).write(tmp_path / 'table', stage_locally=True)
    repartitioned = hl.read_table('relative', _n_partitions=2)
    assert repartitioned.count() == 2
    assert repartitioned.n_partitions() <= 2
    entry = tmp_path / 'relative' / '_vail_table.json'
    value = json.loads(entry.read_text())
    value['partition_rows'] = [99]
    entry.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='row counts'):
        hl.read_table('relative')


def test_transformed_composite_keys(tmp_path):
    table = hl.Table.parallelize([
        {'nested': {'x': 1}, 'k': None, 'j': 0},
        {'nested': {'x': 2}, 'k': 1, 'j': 2},
        {'nested': None, 'k': 1, 'j': 1},
        {'nested': {'x': 4}, 'k': 1, 'j': 1},
    ], schema=hl.tstruct(nested=hl.tstruct(x=hl.tint32), k=hl.tint64, j=hl.tint32)).key_by('k', 'j')
    table = table.annotate(value=table.j + 10)
    path = tmp_path / 'table'
    table.write(path)
    loaded = hl.read_table(path)
    assert loaded.collect() == table.collect()
    assert loaded.key.dtype == table.key.dtype
    assert loaded.select('value').collect() == table.select('value').collect()


def test_filtered_multifile_and_empty_partitions(tmp_path):
    table = hl.range_table(12, n_partitions=3)
    table = table.filter((table.idx < 3) | (table.idx >= 9))
    path = tmp_path / 'table'
    table.write(path)
    loaded = hl.read_table(path)
    assert loaded.collect() == table.collect()
    assert loaded.n_partitions() == 3
    assert len(list(path.rglob('*.vortex'))) == 2


def test_corrupt_data_is_rejected(tmp_path):
    path = tmp_path / 'table'
    hl.range_table(3).write(path)
    next(path.rglob('*.vortex')).write_bytes(b'corrupted')
    table = hl.read_table(path)
    with pytest.raises(ValueError):
        table.collect()


def test_floating_values(tmp_path):
    import math
    table = hl.Table.parallelize([
        {'x': float('nan'), 'y': float('inf')},
        {'x': -0.0, 'y': float('-inf')},
        {'x': None, 'y': 1.25},
    ], schema=hl.tstruct(x=hl.tfloat32, y=hl.tfloat64))
    path = tmp_path / 'table'
    table.write(path)
    loaded = hl.read_table(path)
    rows = loaded.collect()
    assert math.isnan(rows[0].x) and rows[0].y == float('inf')
    assert math.copysign(1, rows[1].x) == -1 and rows[1].y == float('-inf')
    assert rows[2].x is None and rows[2].y == 1.25
    assert loaded.row.dtype == table.row.dtype


@pytest.mark.parametrize('dtype, values', [
    (hl.tfloat64, [float('nan'), 0.0, -0.0, None]),
    (hl.tset(hl.tint32), [{1, 2}, {2, 1}, {3}, None]),
    (hl.tdict(hl.tstr, hl.tint32), [{'b': 2, 'a': 1}, {'a': 1, 'b': 2}, None]),
    (hl.tarray(hl.tint32), [[1, None], [1, 2], None]),
    (hl.tcall, [hl.Call((0, 1)), hl.Call((1, 0), True), None]),
    (hl.tndarray(hl.tint32, 1),
     [{'__vail_ndarray_data': [2], '__vail_ndarray_shape': [1]},
      {'__vail_ndarray_data': [1], '__vail_ndarray_shape': [1]}, None]),
])
def test_ordered_complex_key_roundtrip(tmp_path, dtype, values):
    table = hl.Table.parallelize([{'k': value} for value in values],
                                 schema=hl.tstruct(k=dtype)).key_by('k')
    path = tmp_path / 'table'
    table.write(path)
    assert hl.read_table(path)._same(table)


def test_hail_key_sort_order_for_calls_unicode_and_nested_missing():
    calls = [hl.Call((0, 2)), hl.Call((1, 1)), hl.Call((0, 1), True), hl.Call((0, 1))]
    table = hl.Table.parallelize([{'k': value} for value in calls],
                                 schema=hl.tstruct(k=hl.tcall)).key_by('k')
    assert [row.k for row in table.collect()] == [
        hl.Call((0, 1)), hl.Call((0, 1), True), hl.Call((1, 1)), hl.Call((0, 2))]
    strings = hl.Table.parallelize([{'k': value} for value in ['β', None, '中', 'a']],
                                   schema=hl.tstruct(k=hl.tstr)).key_by('k')
    assert [row.k for row in strings.collect()] == ['a', 'β', '中', None]
    nested = hl.Table.parallelize([{'k': value} for value in
                                   [{'x': None}, {'x': 1}, None, {'x': -1}]],
                                  schema=hl.tstruct(k=hl.tstruct(x=hl.tint32))).key_by('k')
    assert [row.k for row in nested.collect()] == [
        hl.Struct(x=-1), hl.Struct(x=1), hl.Struct(x=None), None]


def test_same_uses_key_equivalence_for_nan_and_signed_zero():
    schema = hl.tstruct(k=hl.tfloat64)
    nan_a = hl.Table.parallelize([{'k': float('nan')}], schema=schema).key_by('k')
    nan_b = hl.Table.parallelize([{'k': float('nan')}], schema=schema).key_by('k')
    assert nan_a._same(nan_b)
    negative_zero = hl.Table.parallelize([{'k': -0.0}], schema=schema).key_by('k')
    positive_zero = hl.Table.parallelize([{'k': 0.0}], schema=schema).key_by('k')
    assert not negative_zero._same(positive_zero)


def test_write_rolls_one_logical_partition_into_multiple_files(tmp_path):
    # Exceed the default row cap. Construction stays lazy and the writer must
    # split the single logical partition without changing its read semantics.
    path = tmp_path / 'table'
    n = 1_048_577
    table = hl.range_table(n, n_partitions=1)
    table.write(path)
    assert len(list(path.rglob('*.vortex'))) == 2
    loaded = hl.read_table(path)
    assert loaded.count() == n
    assert loaded.n_partitions() == 1
    assert [r.idx for r in loaded.take(3)] == [0, 1, 2]
    assert [r.idx for r in loaded.tail(3).collect()] == [n - 3, n - 2, n - 1]
