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
    """Validate buffers and names, allowing supported Arrow string and large-offset variants."""
    pa = _pa()
    if pa.types.is_struct(left) and pa.types.is_struct(right):
        return left.num_fields == right.num_fields and all(
            a.name == b.name and same_arrow_layout(a.type, b.type) for a, b in zip(left, right))
    if (pa.types.is_list(left) or pa.types.is_large_list(left)) and pa.types.is_list(right):
        return same_arrow_layout(left.value_type, right.value_type)
    if (pa.types.is_large_string(left) or pa.types.is_string_view(left)) and pa.types.is_string(right):
        return True
    return left.equals(right)


def arrow_type(dtype):
    from .types import type_to_wire
    return _pa().table(type_to_wire(dtype).arrow_type()).schema.field(0).type


def _nullable(convert):
    """Lift a converter for present values to one that preserves missingness."""
    return lambda value: None if value is None else convert(value)


def _physical_converter(dtype):
    """Resolve type/reference metadata once before converting a batch's values."""
    if isinstance(dtype, tlocus):
        reference = dtype.reference_genome
        ordinals = {contig: index for index, contig in enumerate(reference.contigs)}
        return _nullable(lambda value: {
            "__vail_locus_ordinal": ordinals[value.contig],
            "__vail_locus_contig": value.contig,
            "__vail_locus_position": value.position,
        })
    if dtype == tcall:
        from .types import _pack_call
        return _nullable(lambda value: {"__vail_call_word": _pack_call(value)})
    if isinstance(dtype, tinterval):
        point = _physical_converter(dtype.point_type)
        return _nullable(lambda value: {
            "__vail_interval_start": point(value.start),
            "__vail_interval_end": point(value.end),
            "__vail_interval_start_exclusive": not value.includes_start,
            "__vail_interval_end_inclusive": value.includes_end,
        })
    if isinstance(dtype, ttuple):
        children = tuple(_physical_converter(ty) for ty in dtype.types)
        def convert_tuple(value):
            if not isinstance(value, tuple):
                raise TypeError("expected a Python tuple")
            if len(value) != len(children):
                raise TypeError(f"expected a tuple with {len(children)} elements, found {len(value)}")
            if not children:
                return {"__vail_tuple_empty": False}
            return {f"__vail_tuple_{index}": convert(item)
                    for index, (convert, item) in enumerate(zip(children, value))}
        return _nullable(convert_tuple)
    if isinstance(dtype, tdict):
        key = _physical_converter(dtype.key_type)
        item = _physical_converter(dtype.value_type)
        return _nullable(lambda value: [{"key": key(k), "value": item(v)} for k, v in value.items()])
    if isinstance(dtype, tndarray):
        import numpy as np
        element_type, rank = dtype.element_type, dtype.ndim
        element = _physical_converter(element_type)
        numpy_type = str(element_type)
        def convert_ndarray(value):
            if isinstance(value, Mapping) and "__vail_ndarray_data" in value:
                data, shape = value["__vail_ndarray_data"], value["__vail_ndarray_shape"]
                if len(shape) != rank:
                    raise ValueError("ndarray rank does not match dtype")
                if any(type(size) is not int or size < 0 for size in shape):
                    raise ValueError("ndarray shape must contain nonnegative integers")
                if len(data) != prod(shape):
                    raise ValueError("ndarray data size does not match shape")
                return {"__vail_ndarray_data": [element(v) for v in data],
                        "__vail_ndarray_shape": list(shape)}
            array = np.asarray(value, dtype=numpy_type)
            if array.ndim != rank:
                raise ValueError("ndarray rank does not match dtype")
            return {"__vail_ndarray_data": [element(v) for v in array.ravel().tolist()],
                    "__vail_ndarray_shape": list(array.shape)}
        return _nullable(convert_ndarray)
    if isinstance(dtype, (tarray, tset)):
        element = _physical_converter(dtype.element_type)
        return _nullable(lambda value: [element(v) for v in value])
    if isinstance(dtype, tstruct):
        fields = tuple((name, _physical_converter(ty)) for name, ty in dtype.items())
        return _nullable(lambda value: {name: convert(value[name]) for name, convert in fields})
    return lambda value: value


def _logical_converter(dtype, *, key=False):
    """Compile localization; key converters return recursively hashable values.

    In key context arrays become tuples and sets/dicts become frozensets.
    Tuple/struct children inherit that context. Dispatch and child metadata
    resolution happen here, outside the returned converters' value loops.
    """
    if isinstance(dtype, tlocus):
        from .reference_genome import Locus
        reference = dtype.reference_genome
        contigs = reference.contigs
        def convert_locus(value):
            contig, ordinal = value["__vail_locus_contig"], value["__vail_locus_ordinal"]
            if type(ordinal) is not int or not 0 <= ordinal < len(contigs) or contigs[ordinal] != contig:
                raise ValueError("locus contig ordinal does not match reference genome order")
            return Locus(contig, value["__vail_locus_position"], reference)
        return _nullable(convert_locus)
    if dtype == tcall:
        from .types import _unpack_call
        return _nullable(lambda value: _unpack_call(value["__vail_call_word"]))
    if isinstance(dtype, tinterval):
        point_type = dtype.point_type
        point = _logical_converter(point_type, key=key)
        return _nullable(lambda value: Interval(
            point(value["__vail_interval_start"]), point(value["__vail_interval_end"]),
            not value["__vail_interval_start_exclusive"], value["__vail_interval_end_inclusive"],
            point_type,
        ))
    if isinstance(dtype, ttuple):
        children = tuple((f"__vail_tuple_{index}", _logical_converter(ty, key=key))
                         for index, ty in enumerate(dtype.types))
        return _nullable(lambda value: tuple(convert(value[name]) for name, convert in children))
    if isinstance(dtype, tdict):
        convert_key = _logical_converter(dtype.key_type, key=True)
        item = _logical_converter(dtype.value_type, key=key)
        if key:
            return _nullable(lambda value: frozenset((convert_key(v["key"]), item(v["value"])) for v in value))
        return _nullable(lambda value: {convert_key(v["key"]): item(v["value"]) for v in value})
    if isinstance(dtype, tndarray):
        import numpy as np
        element_type = dtype.element_type
        element = _logical_converter(element_type)
        numpy_type = str(element_type)
        rank = dtype.ndim
        def localize_ndarray(value):
            data, shape = value["__vail_ndarray_data"], value["__vail_ndarray_shape"]
            if data is None or shape is None:
                raise ValueError("ndarray data and shape must be defined")
            if len(shape) != rank:
                raise ValueError("ndarray rank does not match dtype")
            if any(type(size) is not int or size < 0 for size in shape):
                raise ValueError("ndarray shape must contain nonnegative integers")
            if prod(shape) != len(data):
                raise ValueError("ndarray data size does not match shape")
            if numpy_type in ("int32", "int64") and any(v is None for v in data):
                raise ValueError("integer ndarray elements must be defined")
            return np.asarray([element(v) for v in data], dtype=numpy_type).reshape(shape)
        return _nullable(localize_ndarray)
    if isinstance(dtype, tset):
        element = _logical_converter(dtype.element_type, key=True)
        collection = frozenset if key else set
        return _nullable(lambda value: collection(element(v) for v in value))
    if isinstance(dtype, tarray):
        element = _logical_converter(dtype.element_type, key=key)
        collection = tuple if key else list
        return _nullable(lambda value: collection(element(v) for v in value))
    if isinstance(dtype, tstruct):
        fields = tuple((name, _logical_converter(ty, key=key)) for name, ty in dtype.items())
        return _nullable(lambda value: Struct(**{name: convert(value[name]) for name, convert in fields}))
    return lambda value: value


def _physical(value, dtype):
    return _physical_converter(dtype)(value)


def _logical(value, dtype):
    return _logical_converter(dtype)(value)


def _hashable(value, dtype):
    """Localize a supported key with recursively hashable children."""
    return _logical_converter(dtype, key=True)(value)


def rows_to_arrow(rows, schema):
    pa = _pa()
    arrow_schema = pa.schema([pa.field(name, arrow_type(ty), nullable=True) for name, ty in schema.items()])
    convert = _physical_converter(schema)
    converted = [convert(row) for row in rows]
    if not schema:
        batch = pa.RecordBatch.from_struct_array(pa.array(converted, type=pa.struct([])))
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
    convert = _logical_converter(schema)
    return [convert(row) for row in rows]


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


def ndarray_literal_arrow(value, dtype):
    """Snapshot NumPy payload once, then lend its contiguous storage to Arrow."""
    import numpy as np
    pa = _pa()
    owned = np.array(value, dtype=str(dtype.element_type), order='C', copy=True)
    if owned.ndim != dtype.ndim:
        raise ValueError('ndarray rank does not match dtype')
    storage = arrow_type(dtype)
    data_field, shape_field = storage[0], storage[1]
    data = pa.ListArray.from_arrays(pa.array([0, owned.size], type=pa.int32()),
                                    pa.array(owned.reshape(-1), type=data_field.type.value_type),
                                    type=data_field.type)
    shape = pa.array([list(owned.shape)], type=shape_field.type)
    array = pa.StructArray.from_arrays([data, shape], fields=list(storage))
    return pa.Table.from_arrays([array], names=['value'])
