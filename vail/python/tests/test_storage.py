import json
import subprocess
import sys

import pytest
import vail as hl


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
    # Non-ASCII keys are not supported by the current certified comparator.
    invalid = hl.Table.parallelize([{'s': 'β'}]).key_by('s')
    with pytest.raises(RuntimeError, match="non-ASCII"):
        invalid.write(path, overwrite=True)
    assert (path / '_vail_table.json').read_bytes() == original
    assert hl.read_table(path).count() == 4
    hl.range_table(2).write(path, overwrite=True)
    assert hl.read_table(path).count() == 2
    assert len(list(path.rglob('*.vortex'))) == 3  # old generations remain readable


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
    with pytest.raises(NotImplementedError):
        hl.read_table('relative', _n_partitions=2)
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
