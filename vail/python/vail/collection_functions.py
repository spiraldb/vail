"""Public collection conveniences composed from Vail's typed expressions.

Signatures and overload dispatch follow Hail's Python expr/functions.py.
Adapted portions are distributed under the MIT notice in third_party/HAIL_LICENSE.
Copyright (c) 2015-2023, Hail Authors. All rights reserved.
"""
import builtins

from .expressions import (Expression, as_expr, array, tuple_, literal, missing,
                          if_else, is_missing, float64, array_sum, array_min, array_max)
from .types import tarray, tset, ttuple, tbool, tint32, tfloat64, type_to_wire
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
    from . import range
    if type(fill_missing) is not bool:
        raise TypeError('fill_missing must be bool')
    if not arrays:
        raise ValueError('zip requires at least one array')
    arrays = [as_expr(a) for a in arrays]
    if not builtins.all(isinstance(a.dtype, tarray) for a in arrays):
        raise TypeError('zip requires arrays')
    lengths = [ops.length(a) for a in arrays]
    size = (array_max if fill_missing else array_min)(array(lengths), filter_missing=False)
    result = range(size)._array_map(lambda i: tuple_(
        if_else(i < n, a[i], missing(a.dtype.element_type)) if fill_missing else a[i]
        for a, n in builtins.zip(arrays, lengths)))
    # A missing input array makes the whole zip missing, even with fill_missing.
    absent = literal(False)
    for a in arrays:
        absent = absent | is_missing(a)
    return if_else(absent, missing(result.dtype), result)


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
    return range(ops.length(a))._array_map(lambda i: tuple_((i + start, a[i]) if index_first else (a[i], i + start)))


def zip_with_index(a, index_first=True):
    return enumerate(a, index_first=index_first)


def corr(x, y):
    """Pearson correlation, excluding pairs with a missing member."""
    x, y = as_expr(x), as_expr(y)
    numeric = ('int32', 'int64', 'float32', 'float64')
    if not builtins.all(isinstance(a.dtype, tarray) and str(a.dtype.element_type) in numeric for a in (x, y)):
        raise TypeError('corr requires numeric arrays')
    pairs = zip(x, y)._array_filter(lambda p: ~is_missing(p[0]) & ~is_missing(p[1]))
    n = float64(ops.length(pairs))
    mx = array_sum(pairs._array_map(lambda p: float64(p[0]))) / n
    my = array_sum(pairs._array_map(lambda p: float64(p[1]))) / n
    numerator = array_sum(pairs._array_map(lambda p: (float64(p[0]) - mx) * (float64(p[1]) - my)))
    sx = array_sum(pairs._array_map(lambda p: (float64(p[0]) - mx) ** 2))
    sy = array_sum(pairs._array_map(lambda p: (float64(p[1]) - my) ** 2))
    result = if_else(ops.length(x) == 0, missing(tfloat64), numerator / (sx * sy) ** 0.5)
    error = Expression(tfloat64, None, {'op': 'die', 'message': literal('corr requires arrays of equal length').to_ir(), 'dtype': type_to_wire(tfloat64)})
    return if_else(ops.length(x) == ops.length(y), result, error)


def or_else(a, b):
    """Return b when a is missing, promoting compatible branch types."""
    from .expressions import coalesce
    return coalesce(a, b)
