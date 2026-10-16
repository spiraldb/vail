"""Conditional builders composed from native expression primitives."""
from .expressions import (
    CaseBuilder, Expression, ExpressionException, as_expr, cast,
    missing, _merge_sources,
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
        from uuid import uuid4
        parameter = uuid4().hex
        base = Expression(self._base.dtype, self._base.source,
                          {'op': 'lambda_variable', 'name': parameter})
        cases = [(base == value, cast(then, self._ret_type)) for value, then in self._cases]
        default = cast(default, self._ret_type)
        absent = None if self._when_missing_case is None else cast(self._when_missing_case, self._ret_type)
        source = _merge_sources(self._base.source, default.source)
        if absent is not None:
            source = _merge_sources(source, absent.source)
        for condition, result in cases:
            source = _merge_sources(source, condition.source)
            source = _merge_sources(source, result.source)
        return Expression(self._ret_type, source, {
            'op': 'switch', 'value': self._base.to_ir(), 'parameter': parameter,
            'cases': [[condition.to_ir(), result.to_ir()] for condition, result in cases],
            'default': default.to_ir(), 'missing': None if absent is None else absent.to_ir(),
        })

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
