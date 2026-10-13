"""Conditional builders composed from native expression primitives."""
from .expressions import (
    CaseBuilder, Expression, ExpressionException, as_expr, cast, if_else,
    is_missing, missing,
)
from .types import tstr, type_to_wire, unify_types


class SwitchBuilder(CaseBuilder):
    """Match an expression against ordered values; missing comparisons propagate."""

    def __init__(self, base):
        super().__init__()
        self._base = as_expr(base)
        self._when_missing_case = None

    def when(self, value, then):
        value, then = as_expr(value), as_expr(then)
        if unify_types(self._base.dtype, value.dtype) is None:
            raise TypeError(f"cannot compare expressions of type '{self._base.dtype}' and '{value.dtype}'")
        self._unify_type(then.dtype)
        self._cases.append((value, then))
        return self

    def when_missing(self, then):
        if self._when_missing_case is not None:
            raise ExpressionException("'when_missing' can only be called once")
        then = as_expr(then)
        self._unify_type(then.dtype)
        self._when_missing_case = then
        return self

    def _finish(self, default):
        result = cast(default, self._ret_type)
        for value, then in reversed(self._cases):
            result = if_else(self._base == value, cast(then, self._ret_type), result)
        if self._when_missing_case is not None:
            result = if_else(is_missing(self._base), cast(self._when_missing_case, self._ret_type), result)
        return result

    def default(self, then):
        then = as_expr(then)
        if not self._cases and self._when_missing_case is None:
            return then
        self._unify_type(then.dtype)
        return self._finish(then)

    def or_error(self, message):
        if not self._cases:
            raise ExpressionException("'or_error' cannot be called without at least one 'when' call")
        message = as_expr(message)
        if message.dtype != tstr:
            raise TypeError("switch error message must have str type")
        error = Expression(self._ret_type, message.source,
                           {"op": "die", "message": message.to_ir(), "dtype": type_to_wire(self._ret_type)})
        return self._finish(error)


def switch(expr):
    """Start a conditional expression matching ``expr`` against ordered values."""
    return SwitchBuilder(expr)
