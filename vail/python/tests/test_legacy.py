"""Exercise the actual Hail compatibility corpus, not a matching Python writer."""
from pathlib import Path
import gzip
import json
import runpy
import shutil
import struct
import tarfile

import numpy as np
import pytest
import vail as hl

RESOURCES = Path(__file__).resolve().parents[3] / 'hail/hail/hail/test/resources'
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
    with pytest.raises(ValueError, match='cannot read Hail partition'):
        hl.Table.read_legacy(path)


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
    with pytest.raises(ValueError, match='cannot read Hail partition'):
        hl.Table.read_legacy(path)


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
    with pytest.raises(ValueError, match='row and entry partition counts differ'):
        hl.MatrixTable.read_legacy(path)


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
