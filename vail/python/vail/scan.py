"""Exclusive, table-order scans over the built-in aggregate reducers."""
from __future__ import annotations

import builtins
from . import agg as _agg
from .expressions import Expression, ExpressionException, _IR

def _mark(result):
    ir = result.to_ir()
    supported = {"count", "count_where", "fraction", "any", "all", "sum", "product", "min", "max", "mean", "stddev_pop", "collect", "take", "collect_as_set", "corr", "ndarray_sum"}
    def validate(value):
        if isinstance(value, dict):
            if value.get("op") == "aggregate" and (value.get("function") not in supported or value.get("init")):
                raise NotImplementedError("this scan reducer requires additional initialization or scope support")
            for child in value.values(): validate(child)
        elif isinstance(value, list):
            for child in value: validate(child)
    validate(ir)
    def mark(value):
        if isinstance(value, dict):
            value = {k: mark(v) for k, v in value.items()}
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
    if isinstance(value, dict):
        return value.get("op") == "aggregate" or builtins.any(_has_aggregate(v) for v in value.values())
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
    raise NotImplementedError("scan.explode is not supported")

