"""Array fold and scan expression constructors."""
import uuid

from .expressions import Expression, as_expr, cast, _merge_sources, is_missing, if_else, missing, struct
from .types import tarray, tset, tint32, tint64, tfloat32, tfloat64, unify_types


def _fold(f, zero, collection, scan):
    if not callable(f):
        raise TypeError("array fold requires a callable")
    collection, zero = as_expr(collection), as_expr(zero)
    if not isinstance(collection.dtype, tarray):
        raise TypeError("array fold requires an array")
    source = _merge_sources(collection.source, zero.source)
    accumulator, parameter = uuid.uuid4().hex, uuid.uuid4().hex
    element = Expression(collection.dtype.element_type, source,
                         {"op": "lambda_variable", "name": parameter})
    def callback(dtype):
        acc = Expression(dtype, source, {"op": "lambda_variable", "name": accumulator})
        return as_expr(f(acc, element))
    body = callback(zero.dtype)
    dtype = unify_types(zero.dtype, body.dtype)
    if dtype is None:
        raise TypeError("array fold initial value and callback result types are incompatible")
    if dtype != zero.dtype:
        zero = cast(zero, dtype)
        body = callback(dtype)
    if body.dtype != dtype:
        body = cast(body, dtype)
    source = _merge_sources(source, body.source)
    from .randomness import index_callback
    collection, body = index_callback(collection, parameter, body)
    return Expression(tarray(dtype) if scan else dtype, source,
                      {"op": "array_fold", "value": collection.to_ir(), "zero": zero.to_ir(),
                       "accumulator": accumulator, "parameter": parameter,
                       "body": body.to_ir(), "scan": scan})


def fold(f, zero, collection):
    """Apply a function successively to an initial value and array elements."""
    return _fold(f, zero, collection, False)


def array_scan(f, zero, a):
    """Return the initial value followed by every successive accumulator."""
    return _fold(f, zero, a, True)


def _numeric_array(collection, *, ordered=True):
    from .collections import to_array
    collection = as_expr(collection)
    if isinstance(collection.dtype, tset):
        if ordered:
            collection = to_array(collection)
        else:
            from .collections import _cast
            collection = _cast(collection,tarray(collection.dtype.element_type))
    if not isinstance(collection.dtype, tarray) or collection.dtype.element_type not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("expected a numeric array or set")
    return collection


def mean(collection, filter_missing=True):
    """Mean of numeric elements; empty collections return NaN."""
    if type(filter_missing) is not bool:
        raise TypeError("filter_missing must be bool")
    collection = _numeric_array(collection)
    from .expressions import bind
    def present(values):
        defined = values.filter(lambda x: ~is_missing(x))
        result = bind(lambda defined:
                      defined.map(lambda x: cast(x,tfloat64))._reduce('sum') / cast(defined.length(),tfloat64),defined)
        return result if filter_missing else if_else(values.any(is_missing),missing(tfloat64),result)
    return bind(present,collection)


def median(collection):
    """Lower middle numeric value after removing missing elements."""
    from .collections import sorted_
    collection = _numeric_array(collection,ordered=False)
    ordered = sorted_(collection.filter(lambda x: ~is_missing(x)))
    from .expressions import bind
    return bind(lambda ordered: if_else(ordered.length()>0,ordered[(ordered.length()-1)//2],
                                       missing(collection.dtype.element_type)),ordered)


def group_by(f, collection):
    """Group collection elements using DataFusion's native key equality."""
    from . import collections as ops
    if not callable(f):
        raise TypeError('group_by requires a callable')
    collection = as_expr(collection)
    if not isinstance(collection.dtype, (tarray, tset)):
        raise TypeError('group_by requires an array or set')
    pairs = collection._array_map(lambda item: struct(key=f(item), value=item))
    _ensure_group_key(pairs.dtype.element_type["key"])
    result_type = tarray(collection.dtype.element_type)
    from .types import tdict, tstruct
    groups = ops._function("__vail_collection_group", [pairs],
        tarray(tstruct(key=pairs.dtype.element_type["key"], value=result_type)))
    if isinstance(collection.dtype, tset):
        result_type = tset(collection.dtype.element_type)
        groups = groups.map(lambda group: struct(key=group.key, value=ops._cast(group.value, result_type)))
    return ops._cast(groups, tdict(pairs.dtype.element_type["key"], result_type))


def _ensure_group_key(dtype):
    from .collections import _ensure_key_supported
    _ensure_key_supported(dtype)
