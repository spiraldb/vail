"""Lazy native Avro object-container imports."""
import json

from .types import tarray, tdict, tstruct, tstr, tbool, tint32, tint64, tfloat32, tfloat64
from .text import _paths, _read


def _schema_type(schema, names=None, namespace='', active=()):
    names = {} if names is None else names
    primitives = {'boolean': tbool, 'int': tint32, 'long': tint64, 'float': tfloat32,
                  'double': tfloat64, 'string': tstr}
    if isinstance(schema, list):
        choices = [s for s in schema if s != 'null' and s != {'type': 'null'}]
        if len(schema) != 2 or len(choices) != 1:
            raise ValueError('Avro unions must contain null and exactly one non-null type')
        return _schema_type(choices[0], names, namespace, active)
    if isinstance(schema, str):
        if schema in primitives:
            return primitives[schema]
        fullname = schema if '.' in schema else f'{namespace}.{schema}' if namespace else schema
        if fullname in active:
            raise ValueError('recursive Avro records cannot be represented as Hail types')
        if fullname in names:
            return _schema_type(names[fullname], names, namespace, active)
        raise ValueError(f'unsupported Avro type {schema!r}')
    kind = schema['type']
    if kind in ('record', 'enum', 'fixed'):
        name = schema['name']
        ns = name.rpartition('.')[0] if '.' in name else schema.get('namespace', namespace)
        fullname = name if '.' in name else f'{ns}.{name}' if ns else name
        names[fullname] = schema
        if kind == 'record':
            if fullname in active:
                raise ValueError('recursive Avro records cannot be represented as Hail types')
            return tstruct(**{f['name']: _schema_type(f['type'], names, ns, (*active, fullname))
                              for f in schema['fields']})
        if kind == 'enum':
            return tstr
    if kind == 'array':
        return tarray(_schema_type(schema['items'], names, namespace, active))
    if kind == 'map':
        return tdict(tstr, _schema_type(schema['values'], names, namespace, active))
    return _schema_type(kind, names, namespace, active)


def import_avro(paths, *, key=None, intervals=None):
    """Read Avro container records lazily, preserving primitive and nested types.

    ``intervals`` supplies one declared partition bound per file, as in Hail;
    it does not filter records. Keys are sorted by Vail's normal key operator.
    """
    from . import _native_session
    if (key is None) != (intervals is None):
        raise ValueError('key and intervals must either be both defined or both undefined')
    paths = _paths(paths)
    schemas = [json.loads(_native_session().avro_schema(path)) for path in paths]
    schema = _schema_type(schemas[0])
    if not isinstance(schema, tstruct):
        raise ValueError('Avro root schema must be a record')
    if any(_schema_type(s) != schema for s in schemas[1:]):
        raise ValueError('Avro input schemas must match')
    table = _read(paths, schema, 'avro', {})
    if key is not None:
        keys = [key] if isinstance(key, str) else list(key)
        if len(intervals) != len(paths):
            raise ValueError('There must be one partition interval per Avro file')
        from .types import Interval
        for bound in intervals:
            if not isinstance(bound, Interval):
                raise TypeError('intervals must contain Interval values')
            point_types = (list(bound.point_type.values()) if isinstance(bound.point_type, tstruct)
                           else [bound.point_type])
            if point_types != [schema[name] for name in keys]:
                raise TypeError('Avro partition interval point type must match the key')
        table = table.key_by(*keys)
    return table
