"""Public collection conveniences composed from Vail's typed expressions.

Signatures and overload dispatch follow Hail's Python expr/functions.py.
Adapted portions are distributed under the MIT notice in third_party/HAIL_LICENSE.
Copyright (c) 2015-2023, Hail Authors. All rights reserved.
"""
import builtins

from .expressions import (Expression, as_expr, array, tuple_, literal, missing,
                          if_else, is_missing, float64, array_sum, array_min, array_max)
from .types import tarray, tset, ttuple, tbool, tint32, tfloat64, tstruct, type_to_wire
from . import collections as ops


def _collection(value):
    value = as_expr(value)
    if not isinstance(value.dtype, (tarray, tset)):
        raise TypeError('expected an array or set expression')
    return value


def _callable(f):
    if not callable(f):
        raise TypeError('expected a callable')


def zip(*arrays, fill_missing=False):
    """Zip arrays, truncating to the shortest or padding to the longest."""
    if type(fill_missing) is not bool:
        raise TypeError('fill_missing must be bool')
    if not arrays:
        raise ValueError('zip requires at least one array')
    arrays = [as_expr(a) for a in arrays]
    if not builtins.all(isinstance(a.dtype, tarray) for a in arrays):
        raise TypeError('zip requires arrays')
    result_type = tarray(ttuple(*(a.dtype.element_type for a in arrays)))
    from .expressions import bind
    def zipped(*bound):
        def combine(values):
            physical = tarray(tstruct(**{str(i+1): a.dtype.element_type
                                        for i,a in builtins.enumerate(values)}))
            pairs = ops._function('arrays_zip', list(values), physical)
            return pairs.map(lambda pair: tuple_(pair[str(i+1)] for i in builtins.range(len(values))))
        if fill_missing:
            return combine(bound)
        size = array_min(array([ops.length(a) for a in bound]), filter_missing=False)
        return bind(lambda size: combine([a[:size] for a in bound]), size)
    result = bind(zipped, *arrays)
    # Preserve missing-input short circuiting before evaluating the zip body.
    absent = literal(False)
    for a in arrays:
        absent = absent | is_missing(a)
    return if_else(absent, missing(result_type), result)


def map(f, *collections):
    """Map one collection or zip multiple arrays and map corresponding elements."""
    _callable(f)
    if not collections:
        raise ValueError('map requires at least one collection')
    if builtins.len(collections) == 1:
        value = _collection(collections[0])
        return ops.set_map(value, f) if isinstance(value.dtype, tset) else value._array_map(f)
    return starmap(f, zip(*collections))


def starmap(f, collection):
    _callable(f)
    value = _collection(collection)
    if not isinstance(value.dtype.element_type, ttuple):
        raise TypeError('starmap requires tuple elements')
    callback = lambda item: f(*(item[i] for i in builtins.range(builtins.len(item.dtype.types))))
    return map(callback, value)


def _boolean_reduction(args, every):
    base = literal(every)
    if not args:
        return base
    if builtins.len(args) == 2 and callable(args[0]):
        f, value = args[0], _collection(args[1])
        return ~ops.predicate(value, lambda x: ~as_expr(f(x))) if every else ops.predicate(value, f)
    if builtins.len(args) == 1:
        value = as_expr(args[0])
        if isinstance(value.dtype, (tarray, tset)):
            return _boolean_reduction((lambda x: x, value), every)
        args = (value,)
    for arg in args:
        value = as_expr(arg)
        if value.dtype != tbool:
            raise TypeError('all/any requires boolean expressions or a collection predicate')
        base = base & value if every else base | value
    return base


def any(*args):
    return _boolean_reduction(args, False)


def all(*args):
    return _boolean_reduction(args, True)


def find(f, collection):
    _callable(f)
    return ops.predicate(_collection(collection), f, first=True)


def filter(f, collection):
    _callable(f)
    return _collection(collection)._array_filter(f)


def flatmap(f, collection):
    _callable(f)
    value = _collection(collection)
    return ops.flatmap(value, f) if isinstance(value.dtype, tset) else value._array_flatmap(f)


def flatten(collection):
    return flatmap(lambda x: x, collection)


def enumerate(a, start=0, *, index_first=True):
    from . import range
    a = as_expr(a)
    start = as_expr(start)
    if not isinstance(a.dtype, tarray) or start.dtype != tint32:
        raise TypeError('enumerate requires an array and int32 start')
    if type(index_first) is not bool:
        raise TypeError('index_first must be bool')
    indexed = ops._function('__vail_index_values', [a], tarray(ttuple(tint32,a.dtype.element_type)))
    return indexed.map(lambda pair: tuple_((pair[0]+start,pair[1]) if index_first else (pair[1],pair[0]+start)))


def zip_with_index(a, index_first=True):
    return enumerate(a, index_first=index_first)


def corr(x, y):
    """Pearson correlation, excluding pairs with a missing member."""
    x, y = as_expr(x), as_expr(y)
    numeric = ('int32', 'int64', 'float32', 'float64')
    if not builtins.all(isinstance(a.dtype, tarray) and str(a.dtype.element_type) in numeric for a in (x, y)):
        raise TypeError('corr requires numeric arrays')
    from .expressions import bind, coalesce, struct

    def run(x, y):
        pairs = zip(x, y).map(lambda pair: struct(x=pair[0], y=pair[1]))
        result = ops._function("__vail_list_reduce_corr", [pairs], tfloat64)
        # Public array correlation returns NaN for a nonempty all-missing
        # collection; the table reducer's empty-state result is missing.
        result = if_else(ops.length(x) == 0, missing(tfloat64),
                         coalesce(result, literal(float("nan"))))
        error = Expression(tfloat64, None, {"op": "die", "message": literal("corr requires arrays of equal length").to_ir(), "dtype": type_to_wire(tfloat64)})
        return if_else(ops.length(x) == ops.length(y), result, error)

    return bind(run, x, y)


def or_else(a, b):
    """Return b when a is missing, promoting compatible branch types."""
    from .expressions import coalesce
    return coalesce(a, b)
