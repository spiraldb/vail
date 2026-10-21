"""Public Avro imports against Apache's writer, including container validation."""
import json
from datetime import datetime, timezone, timedelta
import pytest
import vail as hl
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from avro.schema import parse


def write(path, schema, rows, codec='null'):
    with DataFileWriter(open(path, 'wb'), DatumWriter(), parse(json.dumps(schema)), codec=codec) as writer:
        for row in rows:
            writer.append(row)
    return str(path)


def test_avro_primitives_and_nulls(tmp_path):
    schema = {'type': 'record', 'name': 'Primitives', 'fields': [
        {'name': 'i', 'type': 'int'}, {'name': 'l', 'type': 'long'},
        {'name': 'f', 'type': 'float'}, {'name': 'd', 'type': 'double'},
        {'name': 'b', 'type': 'boolean'}, {'name': 's', 'type': ['string', 'null']}]}
    rows = [dict(i=-7, l=2**50, f=1.25, d=-2.5, b=True, s='héllo'),
            dict(i=0, l=-2**50, f=0., d=3., b=False, s=None)]
    path = write(tmp_path / 'data.avro', schema, rows, 'deflate')
    table = hl.import_avro([path])
    assert table.row.dtype == hl.tstruct(i=hl.tint32, l=hl.tint64, f=hl.tfloat32,
                                        d=hl.tfloat64, b=hl.tbool, s=hl.tstr)
    assert [dict(r) for r in table.collect()] == rows


def test_avro_nested_arrays_maps_and_logical(tmp_path):
    schema = {'type': 'record', 'name': 'Outer', 'fields': [
        {'name': 'inner', 'type': ['null', {'type': 'record', 'name': 'Inner',
          'fields': [{'name': 'n', 'type': 'long'}]}]},
        {'name': 'items', 'type': {'type': 'array', 'items': ['null', 'int']}},
        {'name': 'mapping', 'type': {'type': 'map', 'values': 'string'}},
        {'name': 'timestamp', 'type': {'type': 'long', 'logicalType': 'timestamp-micros'}}]}
    path = write(tmp_path / 'nested.avro', schema,
                 [dict(inner={'n': 9}, items=[1, None, -2], mapping={'a': 'b'}, timestamp=datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=12345)),
                  dict(inner=None, items=[], mapping={}, timestamp=datetime(1970, 1, 1, tzinfo=timezone.utc) - timedelta(microseconds=5))])
    rows = hl.import_avro(path).collect()
    assert rows[0].inner.n == 9
    assert rows[0]["items"] == [1, None, -2]
    assert rows[0].mapping == {'a': 'b'}
    assert rows[0].timestamp == 12345
    assert rows[1].inner is None and rows[1].mapping == {}


def test_avro_schema_and_container_errors(tmp_path):
    with pytest.raises(ValueError):
        hl.import_avro([])
    with pytest.raises(ValueError, match='both'):
        hl.import_avro(['x'], key='x')
    bad = tmp_path / 'bad.avro'
    bad.write_bytes(b'not an Avro file')
    with pytest.raises(Exception):
        hl.import_avro(str(bad))
    schema = {'type': 'record', 'name': 'Union', 'fields': [{'name': 'x', 'type': ['int', 'string']}]}
    path = write(tmp_path / 'union.avro', schema, [{'x': 1}])
    with pytest.raises(ValueError, match='union'):
        hl.import_avro(path)


def test_avro_multifile_and_lazy_corruption(tmp_path):
    schema = {'type': 'record', 'name': 'Row', 'fields': [{'name': 'x', 'type': 'long'}]}
    a = write(tmp_path / 'a.avro', schema, [{'x': 1}])
    b = write(tmp_path / 'b.avro', schema, [{'x': 2}])
    assert sorted(r.x for r in hl.import_avro([a, b]).collect()) == [1, 2]
    table = hl.import_avro(a)
    data = bytearray(open(a, 'rb').read())
    data[-1] ^= 1
    open(a, 'wb').write(data)
    with pytest.raises(Exception):
        table.collect()


def test_avro_batches_projection_and_intervals(tmp_path):
    schema = {'type': 'record', 'name': 'BatchRow', 'fields': [
        {'name': 'x', 'type': 'long'}, {'name': 's', 'type': 'string'}]}
    path = write(tmp_path / 'batches.avro', schema,
                 (dict(x=i, s='row') for i in range(17000)))
    table = hl.import_avro(path)
    assert table.count() == 17000
    assert table.select('x').take(2) == [hl.Struct(x=0), hl.Struct(x=1)]
    selected = hl.import_avro(path, key='x', intervals=[hl.Interval(5, 8, point_type=hl.tint64)])
    assert [r.x for r in selected.collect()] == [5, 6, 7]
