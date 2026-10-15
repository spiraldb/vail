"""Arrow C Stream bridge sharing typed buffers across the Python boundary."""
from __future__ import annotations

from collections.abc import Mapping
from math import prod

from .types import (
    Interval, Struct, tarray, tcall, tdict, tinterval, tlocus, tndarray,
    tset, tstruct, ttuple,
)


def _pa():
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise ImportError("Vail's Python data bridge requires pyarrow") from exc
    return pa


TYPE_VERSION = b"vail.type.version"
TYPE_DESCRIPTOR = b"vail.type"


def same_arrow_layout(left, right):
    """Validate buffers and names, allowing Arrow's large-offset variants."""
    pa = _pa()
    if pa.types.is_struct(left) and pa.types.is_struct(right):
        return left.num_fields == right.num_fields and all(
            a.name == b.name and same_arrow_layout(a.type, b.type) for a, b in zip(left, right))
    if (pa.types.is_list(left) or pa.types.is_large_list(left)) and pa.types.is_list(right):
        return same_arrow_layout(left.value_type, right.value_type)
    if pa.types.is_large_string(left) and pa.types.is_string(right):
        return True
    return left.equals(right)


def arrow_type(dtype):
    from .types import type_to_wire
    return _pa().table(type_to_wire(dtype).arrow_type()).schema.field(0).type


def _physical(value, dtype):
    if value is None:
        return None
    if isinstance(dtype, tlocus):
        return {"__vail_locus_ordinal": dtype.reference_genome.contigs.index(value.contig), "__vail_locus_contig": value.contig,
                "__vail_locus_position": value.position}
    if dtype == tcall:
        from .types import _pack_call
        return {"__vail_call_word": _pack_call(value)}
    if isinstance(dtype, tinterval):
        return {"__vail_interval_start": _physical(value.start, dtype.point_type),
                "__vail_interval_end": _physical(value.end, dtype.point_type),
                "__vail_interval_start_exclusive": not value.includes_start,
                "__vail_interval_end_inclusive": value.includes_end}
    if isinstance(dtype, ttuple):
        dtype._validate_value(value)
        if not dtype.types:
            return {"__vail_tuple_empty": False}
        return {f"__vail_tuple_{i}": _physical(value[i], ty) for i, ty in enumerate(dtype.types)}
    if isinstance(dtype, tdict):
        return [{"key": _physical(k, dtype.key_type), "value": _physical(v, dtype.value_type)}
                for k, v in value.items()]
    if isinstance(dtype, tndarray):
        import numpy as np
        if isinstance(value, Mapping) and "__vail_ndarray_data" in value:
            data, shape = value["__vail_ndarray_data"], value["__vail_ndarray_shape"]
            if len(shape) != dtype.ndim:
                raise ValueError("ndarray rank does not match dtype")
            if any(type(size) is not int or size < 0 for size in shape):
                raise ValueError("ndarray shape must contain nonnegative integers")
            if len(data) != prod(shape):
                raise ValueError("ndarray data size does not match shape")
            return {"__vail_ndarray_data": [_physical(v, dtype.element_type) for v in data],
                    "__vail_ndarray_shape": list(shape)}
        arr = np.asarray(value, dtype=str(dtype.element_type))
        if arr.ndim != dtype.ndim:
            raise ValueError("ndarray rank does not match dtype")
        return {"__vail_ndarray_data": [_physical(v, dtype.element_type) for v in arr.ravel().tolist()],
                "__vail_ndarray_shape": list(arr.shape)}
    if isinstance(dtype, (tarray, tset)):
        return [_physical(v, dtype.element_type) for v in value]
    if isinstance(dtype, tstruct):
        return {name: _physical(value[name], ty) for name, ty in dtype.items()}
    if isinstance(value, Struct):
        return dict(value)
    return value


def rows_to_arrow(rows, schema):
    pa = _pa()
    arrow_schema = pa.schema([pa.field(name, arrow_type(ty), nullable=True) for name, ty in schema.items()])
    converted = [{name: _physical(row[name], ty) for name, ty in schema.items()} for row in rows]
    if not schema:
        batch = pa.RecordBatch.from_struct_array(pa.array([{} for _ in rows], type=pa.struct([])))
        table = pa.Table.from_batches([batch], schema=arrow_schema)
    else:
        table = pa.Table.from_pylist(converted, schema=arrow_schema)
    return table


def as_arrow(value):
    pa = _pa()
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    if isinstance(value, pa.Table):
        return value
    raise TypeError("rows must be an iterable, pyarrow.Table, or pyarrow.RecordBatch")


def read_arrow(data):
    # Import Arrow C Stream buffers directly from the native owner.
    table = _pa().table(data)
    return table.schema, table


def decode_rows(data, schema):
    _arrow_schema, table = read_arrow(data)
    rows = table.to_pylist()
    return [Struct(**{name: _logical(row[name], ty) for name, ty in schema.items()}) for row in rows]


def _logical(value, dtype):
    from .types import decode_value
    if value is None:
        return None
    if isinstance(dtype, tlocus):
        logical = {"__vail_locus_ordinal": value["__vail_locus_ordinal"],
                   "__vail_locus_contig": value["__vail_locus_contig"],
                   "__vail_locus_position": value["__vail_locus_position"]}
        return decode_value(logical, dtype)
    if dtype == tcall:
        return decode_value(value, dtype)
    if isinstance(dtype, tinterval):
        return Interval(_logical(value["__vail_interval_start"], dtype.point_type),
                        _logical(value["__vail_interval_end"], dtype.point_type),
                        not value["__vail_interval_start_exclusive"], value["__vail_interval_end_inclusive"],
                        dtype.point_type)
    if isinstance(dtype, ttuple):
        if not dtype.types: return ()
        return tuple(_logical(value[f"__vail_tuple_{i}"], ty) for i, ty in enumerate(dtype.types))
    if isinstance(dtype, tdict):
        return {_hashable(pair["key"], dtype.key_type): _logical(pair["value"], dtype.value_type)
                for pair in value}
    if isinstance(dtype, tndarray):
        import numpy as np
        values = [_logical(v, dtype.element_type) for v in value["__vail_ndarray_data"]]
        return np.asarray(values, dtype=str(dtype.element_type)).reshape(value["__vail_ndarray_shape"])
    if isinstance(dtype, tset):
        return set(_hashable(v, dtype.element_type) for v in value)
    if isinstance(dtype, tarray):
        return [_logical(v, dtype.element_type) for v in value]
    if isinstance(dtype, tstruct):
        return Struct(**{name: _logical(value[name], ty) for name, ty in dtype.items()})
    return decode_value(value, dtype)


def _hashable(value, dtype):
    if value is None:
        return None
    if isinstance(dtype, tset):
        return frozenset(_hashable(v, dtype.element_type) for v in value)
    if isinstance(dtype, tdict):
        return frozenset((_hashable(pair["key"], dtype.key_type),
                          _hashable(pair["value"], dtype.value_type)) for pair in value)
    if isinstance(dtype, tarray):
        return tuple(_hashable(v, dtype.element_type) for v in value)
    if isinstance(dtype, tstruct):
        return Struct(**{name: _hashable(value[name], ty) for name, ty in dtype.items()})
    return _logical(value, dtype)


def dtype_from_arrow(ty):
    """Infer through the native type implementation using Arrow schema memory."""
    from ._native import NativeType
    from .types import type_from_wire
    pa = _pa()
    try:
        native = NativeType.from_arrow(pa.table({'value': pa.array([], type=ty)}))
    except ValueError as error:
        raise TypeError(str(error)) from error
    return type_from_wire(native)
