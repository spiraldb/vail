"""Exclusive, table-order scans over the built-in aggregate reducers."""
from __future__ import annotations

from .expressions import IR_NODE_TYPES

import builtins
from . import agg as _agg
from .expressions import Expression, ExpressionException, _IR

def _mark(result):
    ir = result.to_ir()
    def mark(value):
        if isinstance(value, IR_NODE_TYPES):
            if value.get("op") == "aggregate" and value.get("function") == "__scan_fold":
                return value
            value = {k: mark(v) for k, v in value.items()}
            if value.get("op") == "aggregate_fold":
                return {"op": "aggregate", "function": "__scan_fold", "value": value,
                        "options": {"__vail_scan": True}}
            if value.get("op") == "aggregate":
                options = value.get("options")
                options = {} if options is None else dict(options)
                options["__vail_scan"] = True
                value["options"] = options
            return value
        if isinstance(value, list):
            return [mark(v) for v in value]
        return value
    if not _has_aggregate(ir):
        raise ExpressionException("scan requires at least one built-in aggregate")
    rewritten = _IR(mark(ir), ir.references, ir.lookups, ir.row_indexed)
    return Expression(result.dtype, result.source, rewritten)


def _scan(function, *args, **kwargs):
    return _mark(getattr(_agg, function)(*args, **kwargs))


def _has_aggregate(value):
    if isinstance(value, IR_NODE_TYPES):
        return value.get("op") in ("aggregate", "aggregate_fold") or builtins.any(_has_aggregate(v) for v in value.values())
    if isinstance(value, list): return builtins.any(_has_aggregate(v) for v in value)
    return False


def count():
    return _scan("count")


def sum(expr):
    return _scan("sum", expr)


def product(expr):
    return _scan("product", expr)


def min(expr):
    return _scan("min", expr)


def max(expr):
    return _scan("max", expr)


def mean(expr):
    return _scan("mean", expr)


def collect(expr):
    return _scan("collect", expr)


def count_where(predicate):
    return _scan("count_where", predicate)


def fraction(predicate):
    return _scan("fraction", predicate)


def any(condition):
    return _scan("any", condition)


def all(condition):
    return _scan("all", condition)


def stats(expr):
    return _scan("stats", expr)


def collect_as_set(expr):
    return _scan("collect_as_set", expr)


def take(expr, n, ordering=None):
    return _scan("take", expr, n, ordering)


def corr(x, y):
    return _scan("corr", x, y)


def linreg(y, x, nested_dim=1, weight=None):
    return _scan("linreg", y, x, nested_dim, weight)


def downsample(x, y, label=None, n_divisions=500):
    return _scan("downsample", x, y, label, n_divisions)


def hardy_weinberg_test(expr, one_sided=False):
    return _scan("hardy_weinberg_test", expr, one_sided)


def inbreeding(expr, prior):
    return _scan("inbreeding", expr, prior)


def info_score(gp):
    return _scan("info_score", gp)


def ndarray_sum(expr):
    return _scan("ndarray_sum", expr)


def hist(expr, start, end, bins):
    return _scan("hist", expr, start, end, bins)


def approx_quantiles(expr, qs, k=100):
    return _scan("approx_quantiles", expr, qs, k)


def approx_median(expr, k=100):
    return _scan("approx_median", expr, k)


def filter(predicate, scan_expr):
    return _mark(_agg.filter(predicate, scan_expr))


def explode(function, scan_expr):
    return _mark(_agg.explode(function, scan_expr))


def call_stats(call, alleles):
    return _scan("call_stats", call, alleles)


def fold(initial_value, seq_op, comb_op):
    return _mark(_agg.fold(initial_value, seq_op, comb_op))

def group_by(group, scan_expr):
    return _mark(_agg.group_by(group, scan_expr))

def counter(expr, *, weight=None):
    return _mark(_agg.counter(expr, weight=weight))

def array_agg(function, array):
    return _mark(_agg.array_agg(function, array))

def array_sum(expr):
    return _mark(_agg.array_sum(expr))

def approx_cdf(expr, k=100, *, _raw=False):
    return _agg.approx_cdf(expr, k=k, _raw=_raw)


def _prev_nonnull(expr):
    from .expressions import as_expr, if_else, is_defined, missing
    value = as_expr(expr)
    return fold(missing(value.dtype),
                lambda previous: if_else(is_defined(value), value, previous),
                lambda left, right: if_else(is_defined(right), right, left))


def _densify(length, expr):
    from . import range as range_
    from .expressions import Expression, as_expr, if_else, is_defined, is_missing, missing, literal
    from .types import tarray, tint32, type_to_wire
    value, length = as_expr(expr), as_expr(length)
    if not isinstance(value.dtype, tarray) or length.dtype != tint32:
        raise TypeError("scan._densify requires an int32 length and array expression")
    failure = Expression(value.dtype, None, {"op": "die", "message": literal("scan._densify requires a nonnegative length and arrays of that length").to_ir(), "dtype": type_to_wire(value.dtype)})
    initial = if_else(length >= 0, range_(length).map(lambda _: missing(value.dtype.element_type)), failure)
    def merge(left, right):
        return range_(length).map(lambda i: if_else(is_defined(right[i]), right[i], left[i]))
    def sequence(previous):
        return if_else(is_missing(value), previous,
                       if_else(value.length() == length, merge(previous, value), failure))
    return fold(initial, sequence, merge)
