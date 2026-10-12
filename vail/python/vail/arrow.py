"""Arrow IPC bridge for moving typed table data across the Python boundary."""
from __future__ import annotations

from collections.abc import Mapping
import json
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


def arrow_type(dtype):
    pa = _pa()
    if isinstance(dtype, tstruct):
        return pa.struct([pa.field(name, arrow_type(ty), nullable=True) for name, ty in dtype.items()])
    if isinstance(dtype, ttuple):
        fields = ([pa.field("__vail_tuple_empty", pa.bool_())] if not dtype.types else
                  [pa.field(f"__vail_tuple_{i}", arrow_type(ty)) for i, ty in enumerate(dtype.types)])
        return pa.struct(fields)
    if isinstance(dtype, tlocus):
        return pa.struct([
            pa.field("__vail_locus_ordinal", pa.int32()),
            pa.field("__vail_locus_contig", pa.string(), metadata={"vail.reference": json.dumps(dtype.reference_genome.to_wire(), separators=(",", ":"))}),
            pa.field("__vail_locus_position", pa.int32()),
        ])
    if dtype == tcall:
        return pa.struct([pa.field("__vail_call_alleles", pa.list_(pa.int32())),
                          pa.field("__vail_call_phased", pa.bool_())])
    if isinstance(dtype, tinterval):
        return pa.struct([pa.field("__vail_interval_start", arrow_type(dtype.point_type)),
                          pa.field("__vail_interval_start_exclusive", pa.bool_()),
                          pa.field("__vail_interval_end", arrow_type(dtype.point_type)),
                          pa.field("__vail_interval_end_inclusive", pa.bool_())])
    if isinstance(dtype, tdict):
        return pa.list_(pa.field("__vail_dict_entry", pa.struct([
            pa.field("key", arrow_type(dtype.key_type)), pa.field("value", arrow_type(dtype.value_type))])))
    if isinstance(dtype, tndarray):
        return pa.struct([pa.field("__vail_ndarray_data", pa.list_(pa.field(f"__vail_ndarray_item_{dtype.ndim}", arrow_type(dtype.element_type)))),
                          pa.field("__vail_ndarray_shape", pa.list_(pa.int64()))])
    if isinstance(dtype, (tarray, tset)):
        name = "__vail_set_item" if isinstance(dtype, tset) else "item"
        return pa.list_(pa.field(name, arrow_type(dtype.element_type)))
    primitives = {"int32": pa.int32(), "int64": pa.int64(), "float32": pa.float32(),
                  "float64": pa.float64(), "bool": pa.bool_(), "str": pa.string()}
    try:
        return primitives[dtype.name]
    except (AttributeError, KeyError):
        raise TypeError(f"unsupported Arrow type {dtype!r}") from None


def _physical(value, dtype):
    if value is None:
        return None
    if isinstance(dtype, tlocus):
        return {"__vail_locus_ordinal": dtype.reference_genome.contigs.index(value.contig), "__vail_locus_contig": value.contig,
                "__vail_locus_position": value.position}
    if dtype == tcall:
        return {"__vail_call_alleles": list(value.alleles), "__vail_call_phased": value.phased}
    if isinstance(dtype, tinterval):
        return {"__vail_interval_start": _physical(value.start, dtype.point_type),
                "__vail_interval_end": _physical(value.end, dtype.point_type),
                "__vail_interval_start_exclusive": not value.includes_start,
                "__vail_interval_end_inclusive": value.includes_end}
    if isinstance(dtype, ttuple):
        if not dtype.types:
            return {"__vail_tuple_empty": False}
        return {f"__vail_tuple_{i}": _physical(v, ty) for i, (v, ty) in enumerate(zip(value, dtype.types))}
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


def rows_to_ipc(rows, schema):
    pa = _pa()
    arrow_schema = pa.schema([pa.field(name, arrow_type(ty), nullable=True) for name, ty in schema.items()])
    converted = [{name: _physical(row[name], ty) for name, ty in schema.items()} for row in rows]
    if not schema:
        batch = pa.RecordBatch.from_struct_array(pa.array([{} for _ in rows], type=pa.struct([])))
        table = pa.Table.from_batches([batch], schema=arrow_schema)
    else:
        table = pa.Table.from_pylist(converted, schema=arrow_schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, arrow_schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def as_ipc(value):
    pa = _pa()
    if isinstance(value, pa.RecordBatch):
        value = pa.Table.from_batches([value])
    if isinstance(value, pa.Table):
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, value.schema) as writer:
            for batch in value.to_batches():
                writer.write_batch(batch)
        return sink.getvalue().to_pybytes()
    raise TypeError("rows must be an iterable, pyarrow.Table, or pyarrow.RecordBatch")


def read_ipc(data):
    pa = _pa()
    reader = pa.ipc.open_stream(data)
    return reader.schema, reader.read_all()


def decode_rows(data, schema):
    _arrow_schema, table = read_ipc(data)
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
    """Infer Vail's logical type from an Arrow type, recognizing Vail storage structs."""
    pa = _pa()
    from .types import (tbool, tfloat32, tfloat64, tint32, tint64, tstr, tarray, tset,
                        tcall, tinterval, tdict, ttuple, tndarray, tstruct)
    if pa.types.is_int32(ty): return tint32
    if pa.types.is_int64(ty): return tint64
    if pa.types.is_float32(ty): return tfloat32
    if pa.types.is_float64(ty): return tfloat64
    if pa.types.is_boolean(ty): return tbool
    if pa.types.is_string(ty) or pa.types.is_large_string(ty): return tstr
    if pa.types.is_list(ty) or pa.types.is_large_list(ty):
        field = ty.value_field
        if field.name == "__vail_set_item": return tset(dtype_from_arrow(field.type))
        if field.name == "__vail_dict_entry" and pa.types.is_struct(field.type):
            return tdict(dtype_from_arrow(field.type.field("key").type),
                         dtype_from_arrow(field.type.field("value").type))
        return tarray(dtype_from_arrow(field.type))
    if pa.types.is_struct(ty):
        names = [f.name for f in ty]
        if names == ["__vail_call_alleles", "__vail_call_phased"]: return tcall
        if names == ["__vail_interval_start", "__vail_interval_start_exclusive", "__vail_interval_end", "__vail_interval_end_inclusive"]:
            return tinterval(dtype_from_arrow(ty.field(0).type))
        if names == ["__vail_ndarray_data", "__vail_ndarray_shape"]:
            field = ty.field(0).type.value_field
            try: rank = int(field.name.rsplit("_", 1)[1])
            except (ValueError, IndexError): raise TypeError("ndarray Arrow schema is missing its rank") from None
            return tndarray(dtype_from_arrow(field.type), rank)
        if names == ["__vail_tuple_empty"]: return ttuple()
        if names and all(name == f"__vail_tuple_{i}" for i, name in enumerate(names)):
            return ttuple(*(dtype_from_arrow(f.type) for f in ty))
        if names == ["__vail_locus_ordinal", "__vail_locus_contig", "__vail_locus_position"]:
            from .types import tlocus, type_from_wire
            metadata = ty.field(1).metadata or {}
            wire = metadata.get(b"vail.reference")
            if wire is None: raise TypeError("locus Arrow schema is missing reference metadata")
            return type_from_wire({"locus": json.loads(wire)})
        return tstruct(**{f.name: dtype_from_arrow(f.type) for f in ty})
    raise TypeError(f"cannot infer a Vail type from Arrow type {ty}")
