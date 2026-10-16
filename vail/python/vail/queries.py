"""Lazy keyed table queries used inside scalar and row expressions."""
from __future__ import annotations

from .expressions import Expression, ExpressionException, _IR, as_expr, if_else, is_missing, missing, struct
from .types import Interval, Struct, tarray, tbool, tinterval, tstruct, ttuple


def _normalize_point(point, key_names, key_types):
    point = as_expr(point)
    if point.dtype == key_types[0]:
        values = [point]
    elif isinstance(point.dtype, tstruct):
        size = len(point.dtype)
        if size == 0:
            raise ValueError("query point value cannot be an empty struct")
        if size > len(key_names):
            raise ValueError(f"query point type has {size} field(s), but key only has {len(key_names)} field(s)")
        fields = tuple(point.dtype.items())
        values = [Expression(ty, point.source, _IR(
            {'op':'get_field', 'value':point.to_ir(), 'name':name},
            point._global_refs, point._lookup_refs, point._row_indexed)) for name,ty in fields]
        for index, (value, key_type) in enumerate(zip(values, key_types)):
            if value.dtype != key_type:
                raise ValueError(
                    f"mismatch at key field {index} ({fields[index][0]!r}): "
                    f"query type is {value.dtype}, key type is {key_type}"
                )
    else:
        raise ValueError(
            f"key mismatch: cannot use query point type {point.dtype} to query a table "
            f"with key of ({', '.join(map(str, key_types))})"
        )
    return struct(**{name: value for name, value in zip(key_names, values)}), len(values)


def query_table(path, point_or_interval):
    """Return full matching rows as an expression array.

    Established literal bounds use interval filtering and fragment pruning.
    Computed scalar inputs remain deferred singleton dependencies. Row-indexed queries lower through the native
    all-matches lookup node; dynamic interval queries currently read the right
    provider partitions because their bounds vary by left row.
    """
    from . import read_table

    if not isinstance(path, str):
        raise TypeError("query_table path must be a string")
    table = read_table(path)
    return _query_rows(table, point_or_interval)


def query_matrix_table_rows(path, point_or_interval, entries_name='entries_array'):
    """Query stored matrix rows, localizing entries in column order.

    Accepts a key point or an interval over a key prefix, including expressions
    belonging to another Table. Filtered entries appear as missing structs.
    """
    from .matrixtable import read_matrix_table

    if not isinstance(path, str):
        raise TypeError("query_matrix_table_rows path must be a string")
    if not isinstance(entries_name, str):
        raise TypeError("entries_name must be a string")
    matrix = read_matrix_table(path)
    if entries_name in matrix.row.dtype:
        raise ValueError(
            f'field "{entries_name}" is present in matrix table row fields, '
            'use a different `entries_name`'
        )
    return _query_rows(matrix.localize_entries(entries_name), point_or_interval)


_NOT_CONSTANT = object()


def _constant_value(node, dtype):
    """Read owned literals/constructors, never execute an expression or source."""
    op = node.get('op')
    if op == 'literal':
        from ._native import NativeArrow
        from .arrow import decode_rows
        value = node['value']
        if isinstance(value, NativeArrow):
            return decode_rows(value, tstruct(value=dtype))[0].value
        from .types import decode_value
        return decode_value(value, dtype)
    if op == 'struct' and isinstance(dtype, tstruct):
        fields = dict(dtype.items())
        values = {name: _constant_value(value, fields[name]) for name, value in node['fields']}
        return _NOT_CONSTANT if any(value is _NOT_CONSTANT for value in values.values()) else Struct(**values)
    if op in ('tuple', 'array') and isinstance(dtype, (ttuple, tarray)):
        nodes = node['elements']
        types = dtype.types if isinstance(dtype, ttuple) else (dtype.element_type,) * len(nodes)
        values = [_constant_value(value, ty) for value, ty in zip(nodes, types)]
        if any(value is _NOT_CONSTANT for value in values):
            return _NOT_CONSTANT
        return tuple(values) if isinstance(dtype, ttuple) else values
    if op == 'interval' and isinstance(dtype, tinterval):
        values = [_constant_value(node[name], ty) for name, ty in (
            ('start', dtype.point_type), ('end', dtype.point_type),
            ('includes_start', tbool), ('includes_end', tbool))]
        if any(value is _NOT_CONSTANT for value in values):
            return _NOT_CONSTANT
        if values[2] is None or values[3] is None:
            return None
        return Interval(*values, dtype.point_type)
    return _NOT_CONSTANT


def _constant_point(value, dtype, key_names, key_types, count):
    if dtype == key_types[0]:
        return Struct(**{key_names[0]: value})
    names = tuple(dtype)
    return Struct(**{key: None if value is None else value[name]
                     for key, name in zip(key_names[:count], names)})


def _query_rows(table, point_or_interval):
    from . import filter_intervals

    query = as_expr(point_or_interval)
    if not table._keys:
        raise ValueError("cannot query unkeyed table")
    key_names = list(table._keys)
    schema_fields = dict(table._schema.items())
    key_types = [schema_fields[name] for name in key_names]
    range_query = isinstance(query.dtype, tinterval) and query.dtype != key_types[0]

    if range_query:
        start, count = _normalize_point(query.start, key_names, key_types)
        end, end_count = _normalize_point(query.end, key_names, key_types)
        if count != end_count:
            raise TypeError("interval endpoints must have the same key prefix")
        query_key = Expression(
            tinterval(start.dtype),
            query.source,
            {"op": "interval", "start": start.to_ir(), "end": end.to_ir(),
             "includes_start": query.includes_start.to_ir(), "includes_end": query.includes_end.to_ir()},
        )
    else:
        query_key, count = _normalize_point(query, key_names, key_types)

    result_type = tarray(table._schema)
    if not query._row_indexed:
        value = _constant_value(query.to_ir(), query.dtype)
        if value is _NOT_CONSTANT:
            # A computed scalar is a lazy singleton dependency. The row query
            # path evaluates it once at the action, preserving lazy source IO.
            from . import Table
            from .matrixtable import MatrixTable
            if isinstance(query.source, MatrixTable):
                query = query.source._lower(query, query.source.globals_table())
            left = Table.parallelize([struct(argument=query)])
            result = left.annotate(matches=_query_rows(table, left.argument))
            return result.matches.collect(_localize=False)[0]
        if range_query:
            if value is None:
                return missing(result_type)
            point_type = query.dtype.point_type
            start_value = _constant_point(value.start, point_type, key_names, key_types, count)
            end_value = _constant_point(value.end, point_type, key_names, key_types, count)
            interval_value = Interval(start_value, end_value, value.includes_start,
                                      value.includes_end, query_key.dtype.point_type)
        else:
            point = _constant_point(value, query.dtype, key_names, key_types, count)
            interval_value = Interval(point, point, True, True, query_key.dtype)
        return filter_intervals(table, [interval_value]).collect(_localize=False)

    if query.source is None:
        raise ExpressionException("row-indexed query must belong to a Table")
    packed_name = "__vail_query_key"
    payload_name = "__vail_query_payload"
    occupied = set(table._schema) | set(table._global_schema)
    while packed_name in occupied:
        packed_name += "_"
    while payload_name in occupied or payload_name == packed_name:
        payload_name += "_"
    from . import Table, _schema_from_native, _keys_from_native
    native = table._native.query_prefix(count, packed_name, payload_name)
    right = Table(native, _schema_from_native(native), _keys_from_native(native))
    indexed = right.index(query_key, all_matches=True, query_interval=range_query)
    result = indexed.map(lambda item: item[payload_name])
    return if_else(is_missing(query), missing(result_type), result) if range_query else result
