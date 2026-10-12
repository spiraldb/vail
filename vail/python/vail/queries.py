"""Lazy keyed table queries used inside scalar and row expressions."""
from __future__ import annotations

from .expressions import Expression, ExpressionException, as_expr, if_else, is_missing, missing, struct
from .types import Interval, tarray, tinterval, tstruct


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
        values = [point[name] for name in point.dtype]
        for index, (value, key_type) in enumerate(zip(values, key_types)):
            if value.dtype != key_type:
                raise ValueError(
                    f"mismatch at key field {index} ({list(point.dtype)[index]!r}): "
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

    Unindexed queries use interval filtering so read-source fragment pruning can
    use their constant bounds. Row-indexed queries lower through the native
    all-matches lookup node; dynamic interval queries currently read the right
    provider partitions because their bounds vary by left row.
    """
    from . import eval, filter_intervals, read_table

    if not isinstance(path, str):
        raise TypeError("query_table path must be a string")
    if not isinstance(point_or_interval, Expression):
        point_or_interval = as_expr(point_or_interval)
    table = read_table(path)
    if not table._keys:
        raise ValueError("cannot query unkeyed table")
    key_names = list(table._keys)
    key_types = [table._schema[name] for name in key_names]
    query = point_or_interval
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
        value = eval(query_key)
        if range_query:
            if value is None:
                return missing(result_type)
            interval_value = Interval(value.start, value.end, value.includes_start,
                                      value.includes_end, query_key.dtype.point_type)
        else:
            point_type = query_key.dtype
            interval_value = Interval(value, value, True, True, point_type)
        return filter_intervals(table, [interval_value]).collect(_localize=False)

    if query.source is None:
        raise ExpressionException("row-indexed query must belong to a Table")
    packed_name = "__vail_query_key"
    payload_name = "__vail_query_payload"
    while packed_name in table._schema:
        packed_name += "_"
    while payload_name in table._schema or payload_name == packed_name:
        payload_name += "_"
    right = table.annotate(**{payload_name: table.row})
    packed_key = struct(**{name: right[name] for name in key_names[:count]})
    right = right.key_by(**{packed_name: packed_key})
    right = right.select(**{payload_name: right[payload_name]})
    indexed = right.index(query_key, all_matches=True, query_interval=range_query)
    result = indexed.map(lambda item: item[payload_name])
    return if_else(is_missing(query), missing(result_type), result) if range_query else result
