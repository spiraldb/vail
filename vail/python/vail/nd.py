"""NumPy-backed constructors for lazy Vail ndarray literals."""
from .types import tndarray, tint32, tint64, tfloat32, tfloat64
from .expressions import Expression


def array(value, dtype=None):
    import numpy as np
    a = np.asarray(value, dtype=None if dtype is None else str(dtype))
    if dtype is None and not isinstance(value, np.ndarray) and a.dtype == np.dtype("int64"):
        if a.size == 0 or (a.min() >= -(2**31) and a.max() < 2**31):
            a = a.astype(np.int32)
    types = {"int32": tint32, "int64": tint64, "float32": tfloat32, "float64": tfloat64}
    if str(a.dtype) not in types:
        raise TypeError("nd.array requires int32/int64/float32/float64 data")
    from .inputs import arrow_literal
    return arrow_literal(a, tndarray(types[str(a.dtype)], a.ndim))


def arange(start, stop=None, step=1):
    """Build a lazy one-dimensional int32 ndarray from a range expression."""
    from .expressions import array, cast, struct, is_defined, or_missing
    from .streams import stream_range
    from .collections import length
    from .types import type_to_wire

    values = stream_range(start, stop, step).to_array()
    # Ndarray uses the same Arrow struct storage as these two fields. Casting
    # only attaches the logical rank and element type metadata; range creation
    # and array length remain native DataFusion expressions.
    packed = struct(__vail_ndarray_data=values,
                    __vail_ndarray_shape=array([cast(length(values), tint64)]))
    result = Expression(tndarray(tint32, 1), values.source,
                        {"op": "cast", "dtype": type_to_wire(tndarray(tint32, 1)),
                         "value": packed.to_ir()})
    return or_missing(is_defined(values), result)
