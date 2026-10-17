"""Scalar table aggregation expressions."""
from __future__ import annotations

from uuid import uuid4

from .expressions import Expression, _merge_sources, as_expr, cast, is_missing
from .types import tarray, tbool, tfloat32, tfloat64, tint32, tint64, tset


def _validate_initializer(expr, function):
    """Ensure an aggregate initializer is independent of rows and aggregation state."""
    # Matrix row aggregation owns a fixed row while reducing its entries.
    # Its initializer scope is checked again by the selected matrix operation.
    matrix_rows = expr.source is not None and hasattr(expr.source, "_mir")
    if expr._row_indexed and not matrix_rows:
        raise TypeError(f"{function} initialization expressions cannot depend on rows or aggregations")
    expr._native_expr.validate_initializer(function, allow_matrix_rows=matrix_rows)


def _aggregate(function, value=None, result_type=None, *, options=None, init=None):
    value_expr = None if value is None else as_expr(value)
    init_exprs = [] if init is None else [(name, as_expr(expr)) for name, expr in init]
    source = None if value_expr is None else value_expr.source
    for _, expr in init_exprs:
        source = _merge_sources(source, expr.source)
        _validate_initializer(expr, function)
    if result_type is None:
        result_type = value_expr.dtype
    from .types import type_to_wire
    options = dict(options or {})
    options["__vail_result_type"] = type_to_wire(result_type)
    return Expression(result_type, source,
                      {"op": "aggregate", "function": function,
                       "value": None if value_expr is None else value_expr.to_ir(),
                       "options": options,
                       "init": [[name, expr.to_ir()] for name, expr in init_exprs]})


def count():
    return _aggregate("count", result_type=tint64)


def sum(expr):
    value = as_expr(expr)
    if value.dtype == tbool:
        value = cast(value, tint64)
    if value.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("sum requires a numeric expression")
    return _aggregate("sum", value, tint64 if value.dtype in (tint32, tint64) else tfloat64)


def product(expr):
    value = as_expr(expr)
    if value.dtype == tbool:
        value = cast(value, tint64)
    if value.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("product requires a numeric expression")
    return _aggregate("product", value, tint64 if value.dtype in (tint32, tint64) else tfloat64)


def min(expr):
    value = as_expr(expr)
    if value.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("min requires a numeric expression")
    return _aggregate("min", value)


def max(expr):
    value = as_expr(expr)
    if value.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("max requires a numeric expression")
    return _aggregate("max", value)


def mean(expr):
    value = as_expr(expr)
    if value.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("mean requires a numeric expression")
    return _aggregate("mean", value, tfloat64)


def collect(expr):
    value = as_expr(expr)
    return _aggregate("collect", value, tarray(value.dtype))


def _ordered_collect(expr):
    """Collect in source order for internal layouts that depend on positions."""
    value = as_expr(expr)
    return _aggregate("collect", value, tarray(value.dtype), options={"ordered": True})


def count_where(condition):
    predicate = as_expr(condition)
    if predicate.dtype != tbool:
        raise TypeError("count_where predicate must be bool")
    return _aggregate("count_where", predicate, tint64)


def fraction(predicate):
    predicate = as_expr(predicate)
    if predicate.dtype != tbool:
        raise TypeError("fraction predicate must be bool")
    return _aggregate("fraction", predicate, tfloat64)


def any(condition):
    predicate = as_expr(condition)
    if predicate.dtype != tbool:
        raise TypeError("any requires a bool expression")
    return _aggregate("any", predicate, tbool)


def all(condition):
    predicate = as_expr(condition)
    if predicate.dtype != tbool:
        raise TypeError("all requires a bool expression")
    return _aggregate("all", predicate, tbool)


def stats(expr):
    value = as_expr(expr)
    if value.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("stats requires a numeric expression")
    # Hail's stats result uses doubles for all measurements and counts only
    # defined observations. stddev_pop is an internal aggregate kernel.
    value64 = cast(value, tfloat64)
    n = count_where(~is_missing(value))
    total = sum(value64)
    average = mean(value)
    return as_expr({
        "mean": average,
        "stdev": _aggregate("stddev_pop", value64, tfloat64),
        "min": cast(min(value), tfloat64),
        "max": cast(max(value), tfloat64),
        "n": n,
        "sum": cast(total, tfloat64),
    })


def filter(condition, aggregation):
    predicate = condition
    body = aggregation
    predicate = as_expr(predicate)
    if predicate.dtype != tbool:
        raise TypeError("agg.filter predicate must be bool")
    body = as_expr(body)
    source = _merge_sources(predicate.source, body.source)
    return Expression(body.dtype, source,
                      {"op": "agg_filter", "predicate": predicate.to_ir(), "body": body.to_ir()})


def explode(f, array_agg_expr):
    value = as_expr(array_agg_expr)
    if not isinstance(value.dtype, (tarray, tset)):
        raise TypeError("agg.explode requires an array or set expression")
    if not callable(f):
        raise TypeError("agg.explode requires a callable")
    parameter = uuid4().hex
    variable = Expression(value.dtype.element_type, value.source,
                          {"op": "lambda_variable", "name": parameter})
    body = as_expr(f(variable))
    source = _merge_sources(value.source, body.source)
    from .randomness import index_callback
    value, body = index_callback(value, parameter, body)
    return Expression(body.dtype, source,
                      {"op": "agg_explode", "value": value.to_ir(),
                       "parameter": parameter, "body": body.to_ir()})



def group_by(group, agg_expr):
    from .types import tdict
    key, body = as_expr(group), as_expr(agg_expr)
    from .types import _ensure_orderable
    _ensure_orderable(key.dtype)
    return Expression(tdict(key.dtype, body.dtype), _merge_sources(key.source, body.source),
                      {"op": "agg_group_by", "key": key.to_ir(), "body": body.to_ir()})


def array_agg(f, array):
    value = as_expr(array)
    if not isinstance(value.dtype, tarray):
        raise TypeError("array_agg requires an array expression")
    parameter = uuid4().hex
    variable = Expression(value.dtype.element_type, value.source, {"op": "lambda_variable", "name": parameter})
    body = as_expr(f(variable))
    from .randomness import index_callback
    value, body = index_callback(value, parameter, body)
    return Expression(tarray(body.dtype), _merge_sources(value.source, body.source),
                      {"op": "agg_array", "value": value.to_ir(), "parameter": parameter, "body": body.to_ir()})

from .agg_collections import collect_as_set, counter, take, array_sum
from .agg_numeric import corr, hist, linreg, approx_cdf, approx_quantiles, approx_median, downsample
from .agg_genomics import hardy_weinberg_test, inbreeding, call_stats, info_score, ndarray_sum

from .agg_fold import fold

__all__ = [
    "all", "any", "approx_cdf", "approx_median", "approx_quantiles", "array_agg",
    "array_sum", "call_stats", "collect", "collect_as_set", "corr", "count",
    "count_where", "counter", "downsample", "explode", "filter", "fold",
    "fraction", "group_by", "hardy_weinberg_test", "hist", "inbreeding",
    "info_score", "linreg", "max", "mean", "min", "ndarray_sum", "product",
    "stats", "sum", "take",
]
