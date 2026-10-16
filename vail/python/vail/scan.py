"""Exclusive, table-order scans over the built-in aggregate reducers."""
from __future__ import annotations

from . import agg as _agg
from .expressions import Expression, ExpressionException, _IR


def _mark(result):
    ir = result.to_ir()
    try:
        marked = result._native_expr.mark_scan()
    except ValueError as error:
        raise ExpressionException(str(error)) from error
    rewritten = _IR(marked, ir.references, ir.lookups, ir.row_indexed,
                    lexical_types=ir.lexical_types)
    return Expression(result.dtype, result.source, rewritten)


def _scan(function, *args, **kwargs):
    return _mark(getattr(_agg, function)(*args, **kwargs))


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
