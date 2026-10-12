"""Typed lazy stream expressions built from Vail array expression IR."""
from __future__ import annotations

from .expressions import Expression, as_expr
from .types import tarray, tint32


class StreamExpression:
    """A stream-shaped view over a lazy array expression.

    The Python wrapper only builds expression IR. It never evaluates a callback
    against table rows.
    """

    __slots__ = ("_array",)

    def __init__(self, array_expression):
        if not isinstance(array_expression, Expression) or not isinstance(array_expression.dtype, tarray):
            raise TypeError("StreamExpression requires an array expression")
        self._array = array_expression

    @property
    def dtype(self):
        return self._array.dtype

    @property
    def source(self):
        return self._array.source

    def _as_array_expr(self):
        return self._array

    def map(self, f):
        if not callable(f):
            raise TypeError("stream.map requires a callable")
        return StreamExpression(self._array._array_map(f))

    def filter(self, f):
        if not callable(f):
            raise TypeError("stream.filter requires a callable")
        return StreamExpression(self._array._array_filter(f))

    def flatmap(self, f):
        if not callable(f):
            raise TypeError("stream.flatmap requires a callable")

        def to_array(value):
            result = f(value)
            if isinstance(result, StreamExpression):
                result = result.to_array()
            result = as_expr(result)
            if not isinstance(result.dtype, tarray):
                raise TypeError("stream.flatmap callback must return a stream or array expression")
            return result

        return StreamExpression(self._array._array_flatmap(to_array))

    def grouped(self, n):
        size = as_expr(n)
        if size.dtype != tint32:
            raise TypeError("group size must have int32 type")
        return StreamExpression(self._array.grouped(size))

    def to_array(self):
        return self._array


def stream_range(start, stop=None, step=1):
    """Build a lazy int32 range stream, matching Hail's ``_stream_range``."""
    if stop is None:
        stop = start
        start = 0
    start, stop, step = as_expr(start), as_expr(stop), as_expr(step)
    if start.dtype != tint32 or stop.dtype != tint32 or step.dtype != tint32:
        raise TypeError("_stream_range start, stop, and step must have int32 type")
    from .expressions import _merge_sources

    source = _merge_sources(_merge_sources(start.source, stop.source), step.source)
    return StreamExpression(Expression(
        tarray(tint32), source,
        {"op": "stream_range", "start": start.to_ir(), "stop": stop.to_ir(), "step": step.to_ir()},
    ))
