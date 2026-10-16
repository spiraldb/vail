"""Collection aggregate constructors.

Options are kept in aggregate IR so the native executor can pack auxiliary
expressions into the single physical aggregate argument.
"""
from __future__ import annotations

from .expressions import Expression, as_expr
from .types import tarray, tbool, tfloat32, tfloat64, tint32, tint64, tset


def _aggregate(function, value, result_type, options):
    value = as_expr(value)
    from .types import type_to_wire
    options = dict(options)
    options["__vail_result_type"] = type_to_wire(result_type)
    return Expression(
        result_type, value.source,
        {"op": "aggregate", "function": function, "value": value.to_ir(), "options": options},
    )


def collect_as_set(expr):
    value = as_expr(expr)
    return _aggregate("collect_as_set", value, tset(value.dtype), {})


def counter(expr, *, weight=None):
    value = as_expr(expr)
    from . import agg
    if weight is None:
        return agg.group_by(value, agg.count())
    weight = as_expr(weight)
    if weight.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("counter weight must be numeric")
    return agg.group_by(value, agg.sum(weight))


def take(expr, n, ordering=None):
    value = as_expr(expr)
    if type(n) is not int or n < 0:
        raise ValueError("take n must be a non-negative integer")
    options = {"n": n, "ordered": ordering is not None}
    packed = value
    if ordering is not None:
        if callable(ordering):
            ordering = ordering(value)
        ordering = as_expr(ordering)
        packed = as_expr({"value": value, "ordering": ordering})
    from .types import type_to_wire
    options["__vail_result_type"] = type_to_wire(tarray(value.dtype))
    return Expression(tarray(value.dtype), packed.source, {
        "op": "aggregate", "function": "take", "value": packed.to_ir(), "options": options,
    })


def array_sum(expr):
    value = as_expr(expr)
    if not isinstance(value.dtype, tarray) or value.dtype.element_type not in (
        tint32, tint64, tfloat32, tfloat64, tbool
    ):
        raise TypeError("array_sum requires an array of numeric or Boolean values")
    from . import agg
    return agg.array_agg(agg.sum, value)
