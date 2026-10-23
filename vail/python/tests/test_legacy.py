"""Exercise the actual Hail compatibility corpus, not a matching Python writer."""
from pathlib import Path
import os
import gzip
import json
import runpy
import shutil
import struct
import tarfile

import numpy as np
import pytest
import vail as hl

RESOURCES = Path(os.environ.get('VAIL_HAIL_RESOURCES', Path(__file__).resolve().parents[3] / 'hail/hail/hail/test/resources'))
COMPAT = RESOURCES / 'backward_compatability'
TABLES = sorted(COMPAT.glob('*/table/*.ht'))
MATRICES = sorted(COMPAT.glob('*/matrix_table/*.hmt'))
EVIDENCE = Path(__file__).resolve().parents[2] / 'conformance/runs/20261022-legacy-storage'


def _all_values():
    # From Hail's python/test/hail/helpers.py:create_all_values, which generated
    # these fixtures. Earlier format versions omit some of these fields.
    return dict(
        f32=float(np.float32(3.14)), i64=-9, m=None,
        astruct=hl.Struct(a=None, b=5.5), mstruct=None,
        aset={'foo', 'bar', 'baz'}, mset=None,
        d={('a', 'b'): 0.5, ('x', None, 'z'): 0.3}, md=None,
        h38=hl.Locus('chr22', 33878978, 'GRCh38'), ml=None,
        i=hl.Interval(hl.Locus('1', 999), hl.Locus('1', 1001)),
        c=hl.Call([0, 1]), mc=None,
        t=(hl.Call([1, 2], phased=True), 'foo', None), mt=None,
        nd=np.arange(10, dtype=np.int32).reshape((2, 5)),
    )


def _assert_record(record, prefix='', *, version=(1, 8, 0), **indices):
    expected = {prefix + name: value for name, value in _all_values().items()}
    if version < (1, 5, 0):
        expected.pop(prefix + 'nd')
    expected.update(indices)
    assert set(record) == set(expected)
    for name, actual in record.items():
        if isinstance(actual, np.ndarray):
            np.testing.assert_array_equal(actual, expected[name])
        else:
            assert actual == expected[name], name


def _expected_schema(version, prefix='', **indices):
    # Independent schema of Hail's create_all_values fixture. NDArray fields
    # were introduced in the stored corpus at format 1.5.
    fields = dict(
        f32=hl.tfloat32, i64=hl.tint64, m=hl.tfloat64,
        astruct=hl.tstruct(a=hl.tint32, b=hl.tfloat64),
        mstruct=hl.tstruct(x=hl.tint32, y=hl.tstr),
        aset=hl.tset(hl.tstr), mset=hl.tset(hl.tfloat64),
        d=hl.tdict(hl.tarray(hl.tstr), hl.tfloat64), md=hl.tdict(hl.tint32, hl.tstr),
        h38=hl.tlocus('GRCh38'), ml=hl.tlocus('GRCh37'),
        i=hl.tinterval(hl.tlocus('GRCh37')), c=hl.tcall, mc=hl.tcall,
        t=hl.ttuple(hl.tcall, hl.tstr, hl.tstr), mt=hl.ttuple(hl.tlocus('GRCh37'), hl.tbool),
    )
    if version >= (1, 5, 0):
        fields['nd'] = hl.tndarray(hl.tint32, 2)
    return hl.tstruct(**indices, **{prefix + name: ty for name, ty in fields.items()})


def _id(path):
    return '/'.join(path.parts[-3:])


def test_compatibility_corpus_present():
    assert len(TABLES) == 56, 'initialize the pinned hail submodule'
    assert len(MATRICES) == 60, 'initialize the pinned hail submodule'


@pytest.mark.parametrize('path', TABLES, ids=_id)
def test_hail_table_versions_and_codecs(path):
    version = tuple(map(int, path.parts[-3].split('.')))
    table = hl.Table.read_legacy(path)
    assert list(table.key) == ['idx']
    assert table.row.dtype == _expected_schema(version, idx=hl.tint32)
    assert table.globals.dtype == _expected_schema(version, 'global_')
    rows = table.collect()
    assert len(rows) == 5
    for idx, row in enumerate(rows):
        _assert_record(row, version=version, idx=idx)
    _assert_record(hl.eval(table.index_globals()), 'global_', version=version)


@pytest.mark.parametrize('path', MATRICES, ids=_id)
def test_hail_matrix_versions_and_codecs(path):
    matrix = hl.MatrixTable.read_legacy(path)
    assert list(matrix.row_key) == ['row_idx']
    assert list(matrix.col_key) == ['col_idx']
    version = tuple(map(int, path.parts[-3].split('.')))
    localized = matrix.localize_entries('cells', 'columns')
    assert localized.row.dtype == hl.tstruct(
        **dict(_expected_schema(version, 'row_', row_idx=hl.tint32).items()),
        cells=hl.tarray(_expected_schema(version, 'entry_')),
    )
    assert localized.globals.dtype == hl.tstruct(
        **dict(_expected_schema(version, 'global_').items()),
        columns=hl.tarray(_expected_schema(version, 'col_', col_idx=hl.tint32)),
    )
    globals = hl.eval(localized.index_globals())
    columns = globals['columns']
    assert len(columns) == 2
    for idx, col in enumerate(columns):
        _assert_record(col, 'col_', version=version, col_idx=idx)
    rows = localized.collect()
    assert len(rows) == 3
    for idx, row in enumerate(rows):
        _assert_record({n: v for n, v in row.items() if n != 'cells'},
                       'row_', version=version, row_idx=idx)
        assert len(row.cells) == 2
        for cell in row.cells:
            _assert_record(cell, 'entry_', version=version)
    _assert_record({n: v for n, v in globals.items() if n != 'columns'},
                   'global_', version=version)


def test_table_docs_and_file_uri():
    path = RESOURCES.parents[2] / 'python/hail/docs/data/kt_example1.ht'
    table = hl.Table.read_legacy(path.as_uri())
    assert [r.ID for r in table.collect()] == [1, 2, 3, 4]
    assert [r.C3 for r in table.collect()] == [5, 1, -5, -10]
    assert hl.eval(table.index_globals()) == hl.Struct(global_field_1=5, global_field_2=10)


def test_custom_reference_definitions():
    table = hl.Table.read_legacy(RESOURCES / 'custom_references.t')
    rows = table.collect()
    assert len(rows) == 14
    assert rows[0].locus1.reference_genome.name == 'test_rg_0'
    assert rows[0].locus2.reference_genome.name == 'test_rg_1'


def test_truncated_partition_is_rejected(tmp_path):
    path = tmp_path / 'broken.ht'
    shutil.copytree(TABLES[0], path)
    part = next((path / 'rows/parts').iterdir())
    part.write_bytes(part.read_bytes()[:-1])
    with pytest.raises(hl.utils.FatalError, match='cannot read Hail partition'):
        hl.Table.read_legacy(path).collect()


def test_wrong_dataset_kind():
    with pytest.raises(ValueError, match='expected TableSpec'):
        hl.Table.read_legacy(MATRICES[0])
    with pytest.raises(ValueError, match='expected MatrixTableSpec'):
        hl.MatrixTable.read_legacy(TABLES[0])


def test_unsupported_future_version(tmp_path):
    with gzip.open(TABLES[0] / 'metadata.json.gz', 'rt') as stream:
        metadata = json.load(stream)
    metadata['file_version'] = 131072
    with gzip.open(tmp_path / 'metadata.json.gz', 'wt') as stream:
        json.dump(metadata, stream)
    with pytest.raises(ValueError, match='unsupported Hail native file version'):
        hl.Table.read_legacy(tmp_path)


def _small_table(path, virtual, encoded, records, buffer=None, raw=None):
    _write_rvd(path / 'rows', virtual, encoded, records, buffer=buffer, raw=raw)
    _write_rvd(path / 'globals', 'Struct{}', '+EBaseStruct{}', [b''])
    metadata = dict(name='TableSpec', file_version=67584,
                    table_type=f'Table{{global:Struct{{}},key:[],row:{virtual}}}',
                    components={n: dict(name='RVDComponentSpec', rel_path=n) for n in ('rows', 'globals')})
    with gzip.open(path / 'metadata.json.gz', 'wt') as stream:
        json.dump(metadata, stream)
    return path


def test_stream_v2_and_varint_lengths(tmp_path):
    # Independently encoded full dataset; the test only invokes public readers.
    payload = b'\x01\x00\x03abc\x02' + struct.pack('<ii', -7, 9) + b'\x00'
    framed = bytes([len(payload)]) + payload
    path = _small_table(tmp_path, 'Struct{s:String,a:Array[Int32]}',
        '+EBaseStruct{s:EBinary2,a:+EArray2[+EInt32]}', [],
        buffer={'name': 'BlockingBufferSpec', 'blockSize': 65536,
                'child': {'name': 'StreamBlockBufferSpec2'}}, raw=framed)
    assert hl.Table.read_legacy(path).collect() == [hl.Struct(s='abc', a=[-7, 9])]


@pytest.mark.parametrize('data', [b'', b'\x80', b'\xff' * 10, b'\xff' * 9 + b'\x02'])
def test_bad_varints(tmp_path, data):
    path = _small_table(tmp_path, 'Struct{x:Int64}', '+EBaseStruct{x:+EVarint}', [],
                        raw=b'\x01' + data)
    with pytest.raises(hl.utils.FatalError, match='cannot read Hail partition'):
        hl.Table.read_legacy(path).collect()


def test_signed_varint_widths(tmp_path):
    path = _small_table(tmp_path, 'Struct{x:Int64,y:Int32}',
        '+EBaseStruct{x:+EVarint,y:+EVarint}',
        [b'\xff' * 9 + b'\x01' + b'\xff' * 4 + b'\x0f'])
    table = hl.Table.read_legacy(path)
    assert table.row.dtype == hl.tstruct(x=hl.tint64, y=hl.tint32)
    assert table.collect() == [hl.Struct(x=-1, y=-1)]


@pytest.mark.parametrize(('virtual', 'encoded'), [
    ('Struct{x:Int32}', '+EBaseStruct{x:+EFloat64}'),
    ('Struct{x:String}', '+EBaseStruct{x:+EInt32}'),
    ('Struct{x:Array[Int32]}', '+EBaseStruct{x:+EArray2[+EFloat32]}'),
])
def test_incompatible_wire_and_virtual_types(tmp_path, virtual, encoded):
    path = _small_table(tmp_path, virtual, encoded, [])
    with pytest.raises(ValueError, match='incompatible'):
        hl.Table.read_legacy(path)


def _write_rvd(path, virtual, encoded, records, *, buffer=None, raw=None):
    (path / 'parts').mkdir(parents=True)
    spec = dict(name='OrderedRVDSpec2', _key=[], _partFiles=['part-0'],
                _codecSpec=dict(name='TypedCodecSpec', _vType=virtual, _eType=encoded,
                    _bufferSpec=buffer or dict(name='StreamBufferSpec')))
    with gzip.open(path / 'metadata.json.gz', 'wt') as stream:
        json.dump(spec, stream)
    (path / 'parts/part-0').write_bytes(raw if raw is not None else b''.join(b'\x01' + row for row in records) + b'\x00')


def _small_matrix(path, *, nrows=2, ncols=3):
    path.mkdir(exist_ok=True)
    row_type = 'Struct{id:Int32,__legacy_entries:Int32}'
    col_type = 'Struct{s:String}'
    entry_type = 'Struct{GT:Call}'
    global_type = 'Struct{__legacy_columns:Int32}'
    _write_rvd(path / 'rows', row_type,
               '+EBaseStruct{id:+EInt32,__legacy_entries:+EInt32}',
               [struct.pack('<ii', 1, i) for i in range(nrows)])
    _write_rvd(path / 'cols', col_type, '+EBaseStruct{s:+EBinary}',
               [struct.pack('<i', 1) + name for name in [b'z', b'a', b'a'][:ncols]])
    # The first row has two present entry structs: a 0/1 call and a missing GT.
    # The middle entry is filtered; every entry in the second row is filtered.
    first = struct.pack('<i', ncols) + (b'\x02\x00' + struct.pack('<i', 12) + b'\x01' if ncols else b'')
    rest = struct.pack('<i', ncols) + (b'\x07' if ncols else b'')
    _write_rvd(path / 'entries', 'Struct{cells:Array[' + entry_type + ']}',
               '+EBaseStruct{cells:+EArray[EBaseStruct{GT:EInt32}]}',
               ([first] + [rest] * (nrows - 1)) if nrows else [])
    _write_rvd(path / 'globals', global_type, '+EBaseStruct{__legacy_columns:+EInt32}',
               [struct.pack('<i', 42)])
    spec = dict(name='MatrixTableSpec', file_version=67328,
                matrix_type=f'Matrix{{global:{global_type},col_key:[s],col:{col_type},'
                            f'row_key:[[id]],row:{row_type},entry:{entry_type}}}',
                components={name: dict(name='RVDComponentSpec', rel_path=name)
                            for name in ('rows', 'cols', 'entries', 'globals')})
    spec['components']['partition_counts'] = dict(name='PartitionCountsComponentSpec', counts=[nrows])
    with gzip.open(path / 'metadata.json.gz', 'wt') as stream:
        json.dump(spec, stream)
    return path


def test_matrix_filtered_entries_column_order_and_name_collisions(tmp_path):
    matrix = hl.MatrixTable.read_legacy(_small_matrix(tmp_path))
    assert matrix.count() == (2, 3)
    localized = matrix.localize_entries('cells', 'columns')
    assert [c.s for c in hl.eval(localized.index_globals().columns)] == ['z', 'a', 'a']
    assert [r.__legacy_entries for r in matrix.rows().collect()] == [0, 1]
    assert hl.eval(matrix.index_globals()) == hl.Struct(__legacy_columns=42)
    entries = matrix.entries().collect()
    assert len(entries) == 2
    assert {r.s: r.GT for r in entries} == {'z': hl.Call([0, 1]), 'a': None}
    assert [r.cells for r in localized.collect()] == [
        [hl.Struct(GT=hl.Call([0, 1])), None, hl.Struct(GT=None)], [None, None, None]]


@pytest.mark.parametrize(('nrows', 'ncols'), [(0, 3), (2, 0), (0, 0)])
def test_empty_matrix_axes(tmp_path, nrows, ncols):
    matrix = hl.MatrixTable.read_legacy(_small_matrix(tmp_path, nrows=nrows, ncols=ncols))
    assert matrix.count() == (nrows, ncols)
    assert matrix.entries().count() == 0
    assert matrix.entry.dtype == hl.tstruct(GT=hl.tcall)


def test_mismatched_entry_partitions_rejected(tmp_path):
    path = _small_matrix(tmp_path)
    (path / 'entries/parts/part-0').write_bytes(b'\x00')
    with pytest.raises(hl.utils.FatalError, match='row and entry partition counts differ'):
        hl.MatrixTable.read_legacy(path).entries().collect()


def test_documentation_matrix_public_operations():
    path = RESOURCES.parents[2] / 'python/hail/docs/data/example_burden.mt'
    matrix = hl.MatrixTable.read_legacy(path)
    assert matrix.count() == (3, 6)
    assert [c.s for c in matrix.cols().collect()] == list('ABCDEF')
    first = matrix.entries().take(1)[0]
    assert first.locus == hl.Locus('1', 1)
    assert first.s == 'A'
    assert first.GT == hl.Call([0, 0])
    assert first.AD == [30, 0]


def test_migration_to_vail_storage(tmp_path):
    source = _small_matrix(tmp_path / 'legacy.mt')
    matrix = hl.MatrixTable.read_legacy(source)
    matrix.write(tmp_path / 'vail.mt')
    loaded = hl.read_matrix_table(tmp_path / 'vail.mt')
    assert loaded.count() == (2, 3)
    assert loaded.localize_entries('cells', 'columns').collect() == matrix.localize_entries('cells', 'columns').collect()
    table = hl.Table.read_legacy(TABLES[0]).select('i64')
    table.write(tmp_path / 'vail.ht')
    assert hl.read_table(tmp_path / 'vail.ht').collect() == table.collect()


def test_empty_table_and_typed_missing_globals(tmp_path):
    _write_rvd(tmp_path / 'rows', 'Struct{x:Int32}', '+EBaseStruct{x:EInt32}', [])
    _write_rvd(tmp_path / 'globals', 'Struct{g:Array[Int32]}', '+EBaseStruct{g:EArray[EInt32]}', [b'\x01'])
    metadata = dict(name='TableSpec', file_version=67328,
                    table_type='Table{global:Struct{g:Array[Int32]},key:[],row:Struct{x:Int32}}',
                    components={n: dict(name='RVDComponentSpec', rel_path=n) for n in ('rows', 'globals')})
    metadata['components']['partition_counts'] = dict(name='PartitionCountsComponentSpec', counts=[0])
    with gzip.open(tmp_path / 'metadata.json.gz', 'wt') as stream:
        json.dump(metadata, stream)
    table = hl.Table.read_legacy(tmp_path)
    assert table.collect() == []
    assert table.row.dtype == hl.tstruct(x=hl.tint32)
    assert table.globals.dtype == hl.tstruct(g=hl.tarray(hl.tint32))
    assert hl.eval(table.g) is None


@pytest.mark.parametrize('name', ['varied.ht', 'varied.mt'])
def test_live_hail_generated_datasets(tmp_path, name):
    # Data, values, keys, and schemas were produced by a separate live Hail
    # process. The retained archive makes the comparison reproducible without
    # requiring Hail/Spark in the Vail test environment.
    with tarfile.open(EVIDENCE / 'live-oracle.tar.gz') as archive:
        archive.extractall(tmp_path, filter='data')
    expected = json.loads((tmp_path / 'expected.json').read_text())['datasets'][name]
    public_snapshot = runpy.run_path(str(EVIDENCE / 'validate.py'))['snapshot']
    matrix = name.endswith('.mt')
    dataset = (hl.MatrixTable if matrix else hl.Table).read_legacy(tmp_path / name)
    assert public_snapshot(hl, dataset, matrix) == expected


def test_native_reader_is_lazy_and_stops_before_corrupt_tail(tmp_path):
    payload = b''.join(b'\x01' + struct.pack('<i', i) for i in range(40000)) + b'\xff'
    path = _small_table(tmp_path, 'Struct{x:Int32}', '+EBaseStruct{x:+EInt32}', [], raw=payload)
    table = hl.Table.read_legacy(path)
    assert table.take(1) == [hl.Struct(x=0)]
    with pytest.raises(hl.utils.FatalError, match='invalid Hail row marker'):
        table.collect()


def test_native_projection_skips_unselected_values(tmp_path):
    path = _small_table(tmp_path, 'Struct{x:Int32,s:String}',
                        '+EBaseStruct{x:+EInt32,s:+EBinary}',
                        [struct.pack('<ii', 7, 1) + b'\xff'])
    table = hl.Table.read_legacy(path)
    assert table.select('x').collect() == [hl.Struct(x=7)]
    assert table.aggregate(hl.agg.sum(table.x)) == 7
    with pytest.raises(hl.utils.FatalError, match='utf-8'):
        table.collect()


def test_native_files_are_independent_partitions(tmp_path):
    path = _small_table(tmp_path, 'Struct{x:Int32}', '+EBaseStruct{x:+EInt32}',
                        [struct.pack('<i', 0)])
    metadata_path = path / 'rows/metadata.json.gz'
    with gzip.open(metadata_path, 'rt') as stream:
        metadata = json.load(stream)
    metadata['_partFiles'] = [f'part-{i}' for i in range(16)]
    with gzip.open(metadata_path, 'wt') as stream:
        json.dump(metadata, stream)
    for i in range(16):
        (path / 'rows/parts' / f'part-{i}').write_bytes(b'\x01' + struct.pack('<i', i) + b'\x00')
    table = hl.Table.read_legacy(path)
    assert table.n_partitions() == 16
    assert [r.x for r in table.collect()] == list(range(16))
    # Construction reads metadata and singleton globals, without opening rows.
    (path / 'rows/parts/part-15').unlink()
    assert hl.Table.read_legacy(path).n_partitions() == 16


def test_native_vortex_filter_and_projection(tmp_path):
    path = _small_table(tmp_path, 'Struct{x:Int32,y:Int64}',
                        '+EBaseStruct{x:+EInt32,y:+EInt64}',
                        [struct.pack('<iq', i, -i) for i in range(20000)])
    metadata_path = path / 'metadata.json.gz'
    with gzip.open(metadata_path, 'rt') as stream:
        metadata = json.load(stream)
    metadata['components']['partition_counts'] = dict(name='PartitionCountsComponentSpec', counts=[20000])
    with gzip.open(metadata_path, 'wt') as stream:
        json.dump(metadata, stream)
    table = hl.Table.read_legacy(path)
    result = table.filter((table.x >= 19990) & (table.x < 19995)).select('y')
    assert result.collect() == [hl.Struct(y=-i) for i in range(19990, 19995)]


@pytest.mark.skipif(os.name != 'posix', reason='uses FIFO readers to verify concurrent file execution')
def test_native_files_execute_concurrently(tmp_path):
    import concurrent.futures
    import threading
    path = _small_table(tmp_path, 'Struct{x:Int32}', '+EBaseStruct{x:+EInt32}', [])
    metadata_path = path / 'rows/metadata.json.gz'
    with gzip.open(metadata_path, 'rt') as stream:
        metadata = json.load(stream)
    metadata['_partFiles'] = ['part-0', 'part-1']
    with gzip.open(metadata_path, 'wt') as stream:
        json.dump(metadata, stream)
    (path / 'rows/parts/part-0').unlink()
    for i in range(2):
        os.mkfifo(path / 'rows/parts' / f'part-{i}')
    ready = threading.Barrier(2, timeout=15)
    def write(i):
        import errno
        import time
        deadline = time.monotonic() + 15
        while True:
            try:
                fd = os.open(path / 'rows/parts' / f'part-{i}', os.O_WRONLY | os.O_NONBLOCK)
                break
            except OSError as error:
                if error.errno != errno.ENXIO or time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        os.set_blocking(fd, True)
        with os.fdopen(fd, 'wb', buffering=0) as stream:
            # Neither file can produce data until both readers have opened.
            # A serial file reader fails this barrier instead of passing the test.
            ready.wait()
            stream.write(b'\x01' + struct.pack('<i', i) + b'\x00')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as writers:
        pending = [writers.submit(write, i) for i in range(2)]
        assert hl.Table.read_legacy(path).collect() == [hl.Struct(x=0), hl.Struct(x=1)]
        for writer in pending:
            writer.result(timeout=15)


def _integer_partitioned_table(path, rows, bounds):
    _small_table(path, 'Struct{x:Int32}', '+EBaseStruct{x:+EInt32}', [])
    top_path = path / 'metadata.json.gz'
    with gzip.open(top_path, 'rt') as stream:
        top = json.load(stream)
    top['table_type'] = top['table_type'].replace('key:[]', 'key:[x]')
    top['components']['partition_counts'] = dict(name='PartitionCountsComponentSpec', counts=list(map(len, rows)))
    with gzip.open(top_path, 'wt') as stream:
        json.dump(top, stream)
    rvd_path = path / 'rows/metadata.json.gz'
    with gzip.open(rvd_path, 'rt') as stream:
        rvd = json.load(stream)
    rvd['_key'] = ['x']
    rvd['_partFiles'] = [f'part-{i}' for i in range(len(rows))]
    rvd['_jRangeBounds'] = bounds
    with gzip.open(rvd_path, 'wt') as stream:
        json.dump(rvd, stream)
    for i, values in enumerate(rows):
        (path / 'rows/parts' / f'part-{i}').write_bytes(
            b''.join(b'\x01' + struct.pack('<i', x) for x in values) + b'\x00')
    return hl.Table.read_legacy(path)


def test_native_partition_pruning_avoids_unneeded_files(tmp_path):
    table = _integer_partitioned_table(tmp_path, [[0], [5], [10]], [
        dict(start=dict(x=x), end=dict(x=x), includeStart=True, includeEnd=True)
        for x in [0, 5, 10]])
    (tmp_path / 'rows/parts/part-0').unlink()
    (tmp_path / 'rows/parts/part-2').unlink()
    assert table.filter(table.x == 5).collect() == [hl.Struct(x=5)]


def test_native_partition_pruning_keeps_touching_duplicates(tmp_path):
    table = _integer_partitioned_table(tmp_path, [[0, 5], [5, 10]], [
        dict(start=dict(x=start), end=dict(x=end), includeStart=True, includeEnd=True)
        for start, end in [(0, 5), (5, 10)]])
    assert table.filter(table.x == 5).collect() == [hl.Struct(x=5), hl.Struct(x=5)]
    assert table.filter((table.x < 1) | (table.x > 9)).collect() == [hl.Struct(x=0), hl.Struct(x=10)]


@pytest.fixture
def indexed_legacy_path(tmp_path):
    with tarfile.open(Path(__file__).parent / 'data/legacy-indexed.ht.tar.gz') as archive:
        archive.extractall(tmp_path, filter='data')
    return tmp_path / 'indexed.ht'


def _index_layout(path, layout):
    index = next(p for p in (path / 'index').glob('*.idx') if not p.name.startswith('.'))
    if layout == 'directory':
        payload = index.read_bytes()
        offset = int.from_bytes(payload[-8:], 'little', signed=True)
        metadata = json.loads(payload[offset:-8])
        metadata['indexPath'] = 'index'
        index.unlink()
        index.mkdir()
        (index / 'index').write_bytes(payload[:offset])
        with gzip.open(index / 'metadata.json.gz', 'wt') as stream:
            json.dump(metadata, stream)
    return index


@pytest.mark.parametrize('layout', ['inline', 'directory'])
def test_native_index_keeps_duplicates_across_leaf_boundary(indexed_legacy_path, layout):
    _index_layout(indexed_legacy_path, layout)
    table = hl.Table.read_legacy(indexed_legacy_path)
    # The index branching factor is 4096; these three rows cross leaf 0/1.
    result = table.filter(table.k == 1365).collect()
    assert result == [hl.Struct(k=1365, idx=i, x=3 * i, payload=str(i)) for i in range(4095, 4098)]


@pytest.mark.parametrize('layout', ['inline', 'directory'])
def test_native_index_seek_does_not_read_corrupt_prefix(indexed_legacy_path, layout):
    _index_layout(indexed_legacy_path, layout)
    part = next(p for p in (indexed_legacy_path / 'rows/parts').iterdir() if not p.name.startswith('.'))
    data = bytearray(part.read_bytes())
    data[:16] = b'\xff' * 16
    part.write_bytes(data)
    table = hl.Table.read_legacy(indexed_legacy_path)
    assert table.count() == 50000
    result = table.filter(table.k == 15000).collect()
    assert result == [hl.Struct(k=15000, idx=i, x=3 * i, payload=str(i)) for i in range(45000, 45003)]
    with pytest.raises(hl.utils.FatalError, match='cannot read Hail partition'):
        table._force_count()


def test_native_escaped_file_uri(indexed_legacy_path, tmp_path):
    spaced = tmp_path / 'legacy with spaces.ht'
    indexed_legacy_path.rename(spaced)
    for uri in [spaced.as_uri(), spaced.as_uri().replace('file:///', 'file://localhost/')]:
        table = hl.Table.read_legacy(uri)
        assert table.count() == 50000
        assert [row.idx for row in table.filter(table.k == 15000).collect()] == [45000, 45001, 45002]


def _projection_matrix(path, *, invalid_string=False):
    _small_matrix(path)
    entry_type = 'Struct{GT:Call,DP:Int32,text:String}'
    metadata_path = path / 'metadata.json.gz'
    with gzip.open(metadata_path, 'rt') as stream:
        metadata = json.load(stream)
    metadata['matrix_type'] = metadata['matrix_type'].replace('entry:Struct{GT:Call}', 'entry:' + entry_type)
    with gzip.open(metadata_path, 'wt') as stream:
        json.dump(metadata, stream)
    text = b'\xff' if invalid_string else b'x'
    first = (struct.pack('<i', 3) + b'\x02'
             + b'\x00' + struct.pack('<ii', 12, 12) + struct.pack('<i', len(text)) + text
             + b'\x01' + struct.pack('<i', 13) + struct.pack('<i', len(text)) + text)
    rest = struct.pack('<i', 3) + b'\x07'
    import shutil
    shutil.rmtree(path / 'entries')
    _write_rvd(path / 'entries', 'Struct{cells:Array[' + entry_type + ']}',
               '+EBaseStruct{cells:+EArray[EBaseStruct{GT:EInt32,DP:+EInt32,text:+EBinary}]}',
               [first, rest])
    return path


def test_native_matrix_entry_aggregate_avoids_row_files(tmp_path):
    path = _projection_matrix(tmp_path)
    (path / 'rows/parts/part-0').unlink()
    matrix = hl.MatrixTable.read_legacy(path)
    assert matrix.aggregate_entries(hl.agg.sum(matrix.DP)) == 25


def test_native_matrix_nested_projection_skips_invalid_sibling(tmp_path):
    matrix = hl.MatrixTable.read_legacy(_projection_matrix(tmp_path, invalid_string=True))
    projected = matrix.select_entries('GT')
    calls = [row.GT for row in projected.entries().collect()]
    assert len(calls) == 2
    assert hl.Call([0, 1]) in calls and None in calls
    assert matrix.aggregate_entries(hl.agg.sum(matrix.DP)) == 25
    with pytest.raises(hl.utils.FatalError, match='utf-8'):
        matrix.entries().collect()


def test_native_matrix_shape_projection_preserves_missing_entries(tmp_path):
    matrix = hl.MatrixTable.read_legacy(_projection_matrix(tmp_path, invalid_string=True))
    # Two present cells remain after excluding filtered entries; a missing GT
    # inside a present cell must not turn that cell into a filtered entry.
    assert matrix.aggregate_entries(hl.agg.count()) == 2
    assert matrix.aggregate_entries(hl.agg.count_where(hl.is_defined(matrix.GT))) == 1
    assert matrix.select_entries().entries().count() == 2


@pytest.mark.parametrize('distinct', [None, False, True])
def test_native_matrix_uses_hail_distinct_row_property(tmp_path, distinct):
    path = _small_matrix(tmp_path)
    # Match Hail's modern layout: a nested rows TableSpec wraps rows/rows RVD.
    rvd = path / 'rows/rows'
    rvd.mkdir()
    (path / 'rows/metadata.json.gz').rename(rvd / 'metadata.json.gz')
    (path / 'rows/parts').rename(rvd / 'parts')
    (rvd / 'parts/part-0').write_bytes(
        b'\x01' + struct.pack('<ii', 1, 0)
        + b'\x01' + struct.pack('<ii', 2, 1) + b'\x00')
    row_spec = dict(name='TableSpec', file_version=67328,
                    table_type='Table{global:Struct{},key:[id],row:Struct{id:Int32,__legacy_entries:Int32}}',
                    components=dict(rows=dict(name='RVDComponentSpec', rel_path='rows')))
    if distinct is not None:
        row_spec['components']['properties'] = dict(name='PropertiesSpec', properties=dict(distinctlyKeyed=distinct))
    with gzip.open(path / 'rows/metadata.json.gz', 'wt') as stream:
        json.dump(row_spec, stream)
    with gzip.open(path / 'metadata.json.gz', 'rt') as stream:
        spec = json.load(stream)
    spec['components']['rows']['rel_path'] = 'rows/rows'
    with gzip.open(path / 'metadata.json.gz', 'wt') as stream:
        json.dump(spec, stream)
    matrix = hl.MatrixTable.read_legacy(path)
    assert matrix._mir.row_key_unique is (distinct is True)
    assert len(matrix.entries().collect()) == 2
