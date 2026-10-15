"""Typed lazy stream expressions built from Vail array expression IR."""

from __future__ import annotations

from .expressions import IR_NODE_TYPES

from .expressions import Expression, as_expr
from .types import tarray, tint32


class StreamExpression:
    """A stream-shaped view over a lazy array expression.

    The Python wrapper only builds expression IR. It never evaluates a callback
    against table rows.
    """

    __slots__ = ("_array",)

    def __init__(self, array_expression):
        if not isinstance(array_expression, Expression) or not isinstance(
            array_expression.dtype, tarray
        ):
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
                raise TypeError(
                    "stream.flatmap callback must return a stream or array expression"
                )
            return result

        return StreamExpression(self._array._array_flatmap(to_array))

    def zip_with_index(self, start=0):
        from .collection_functions import enumerate

        return StreamExpression(enumerate(self._array, start))

    def fold(self, f, zero):
        from .array_methods import fold

        return fold(f, zero, self._array)

    def scan(self, f, zero):
        from .array_methods import array_scan

        return StreamExpression(array_scan(f, zero, self._array))

    def aggregate(self, f):
        from .array_aggregate import aggregate

        return aggregate(self._array, f)

    def _aggregate_scan(self, f):
        from uuid import uuid4
        from .expressions import Expression, _IR, cast
        from .types import tint32, tint64
        from .array_aggregate import aggregate
        from .randomness import _is_random, bind_context

        position_name, value_name = uuid4().hex, uuid4().hex
        position = Expression(
            tint32, self.source, {"op": "lambda_variable", "name": position_name}
        )
        variable = Expression(
            self.dtype.element_type,
            self.source,
            {"op": "lambda_variable", "name": value_name},
        )
        result = as_expr(f(variable))
        current = self._array[position]
        prefix = self._array[:position]

        def reduce(element):
            def rewrite(node, inside=False):
                if isinstance(node, list):
                    return [rewrite(v, inside) for v in node]
                if not isinstance(node, IR_NODE_TYPES) or node.get("op") == "literal":
                    return node
                if node.get("op") == "lambda_variable" and node["name"] == value_name:
                    return (element if inside else current).to_ir()
                if node.get("op") == "aggregate":
                    return {
                        k: (
                            {a: b for a, b in v.items() if a != "__vail_scan"}
                            if k == "options"
                            else rewrite(v, k == "value")
                        )
                        for k, v in node.items()
                    }
                output = {k: rewrite(v, inside) for k, v in node.items()}
                if _is_random(node) and not inside:
                    output = bind_context(output, [cast(position, tint64).to_ir()])
                return output

            return Expression(
                result.dtype,
                result.source,
                _IR(rewrite(result.to_ir()), result._global_refs, result._lookup_refs),
            )

        body = aggregate(prefix, reduce)
        # Prefix reducers use input-element identities; only scalar output
        # randomness uses the output position. A generic map would add its
        # position to both and resample earlier inputs for every prefix.
        return StreamExpression(
            Expression(
                tarray(body.dtype),
                body.source,
                {
                    "op": "array_map",
                    "value": stream_range(self._array.length()).to_array().to_ir(),
                    "parameter": position_name,
                    "body": body.to_ir(),
                },
            )
        )

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
    return StreamExpression(
        Expression(
            tarray(tint32),
            source,
            {
                "op": "stream_range",
                "start": start.to_ir(),
                "stop": stop.to_ir(),
                "step": step.to_ir(),
            },
        )
    )


def zip_streams(*streams, fill_missing=False):
    from .collection_functions import zip

    return StreamExpression(
        zip(*(s.to_array() for s in streams), fill_missing=fill_missing)
    )
