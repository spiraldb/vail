"""Lower collection aggregations with checked native lexical/DAG rewriting."""
from uuid import uuid4
from .expressions import Expression, _IR, _merge_sources, as_expr
from .types import tstruct, type_to_wire


def aggregate(value, f):
    value = as_expr(value)
    if not callable(f):
        raise TypeError('collection aggregate callback must be callable')
    parameter = uuid4().hex
    variable = Expression(value.dtype.element_type, value.source,
                          {'op': 'lambda_variable', 'name': parameter})
    result = as_expr(f(variable))
    source = _merge_sources(value.source, result.source)
    refs = dict(value._global_refs) | dict(result._global_refs)
    lookups = dict(value._lookup_refs) | dict(result._lookup_refs)
    if source is not None and hasattr(source, '_mir'):
        rows = tstruct()
        axes = [(axis, type_to_wire(source._axis_type(axis)))
                for axis in ('row', 'col', 'entry', 'global')]
        globals = source._mir.global_type
    else:
        rows = source._schema if source is not None else tstruct()
        globals = source._global_schema if source is not None else tstruct()
        axes = []
    from . import _schema_from_native
    lookup_types = []
    for name, table in lookups.items():
        schema = _schema_from_native(table)
        keys = set(table.key_names())
        lookup_types.append((name, type_to_wire(tstruct(**{
            field: ty for field, ty in schema.items() if field not in keys}))))
    rewritten = result._native_expr.collection_aggregate(
        value._native_expr, parameter, type_to_wire(value.dtype.element_type),
        type_to_wire(rows), type_to_wire(globals), axes, lookup_types,
        [(name, type_to_wire(ty)) for name, ty in result._lexical_types.items()])
    lexical_types = dict(value._lexical_types) | dict(result._lexical_types)
    lexical_types.pop(parameter, None)
    return Expression(result.dtype, source, _IR(rewritten, refs, lookups,
                      row_indexed=value._row_indexed or result._row_indexed,
                      lexical_types=lexical_types))
