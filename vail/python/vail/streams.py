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

    def aggregate_scan(self, f):
        """Return exclusive prefix reductions, evaluating each input once.

        Fixed-size reducers retain one state per reducer. Collection reducers
        necessarily retain their growing output. As with collection aggregate,
        nested explode/group reducers are not supported.
        """
        from uuid import uuid4
        from .array_methods import array_scan
        from .expressions import (_IR, if_else, is_missing, literal,
                                  missing, struct, array_min, array_max)
        from .types import type_from_wire, type_to_wire, tbool, tint64, tset

        if not callable(f):
            raise TypeError("stream.aggregate_scan requires a callable")
        parameter = uuid4().hex
        variable = Expression(self.dtype.element_type, self.source,
                              {"op": "lambda_variable", "name": parameter})
        result = as_expr(f(variable))
        reducers = []

        def plan(node, filters=()):
            if isinstance(node, list):
                return [plan(v, filters) for v in node]
            if not isinstance(node, IR_NODE_TYPES) or node.get("op") == "literal":
                return node
            op = node.get("op")
            if op == "agg_filter":
                return plan(node["body"], (*filters, node["predicate"]))
            if op in ("agg_explode", "agg_group_by", "agg_array", "aggregate_fold"):
                raise NotImplementedError(f"stream aggregate scan {op}")
            if op == "aggregate":
                function = node["function"]
                if function not in ("count", "sum", "product", "min", "max",
                                    "all", "any", "collect", "collect_as_set"):
                    raise NotImplementedError(f"stream aggregate scan {function}")
                name = f"state_{len(reducers)}"
                dtype = type_from_wire(node["options"]["__vail_result_type"])
                reducers.append((name, function, dtype, node["value"], filters))
                # This temporary marker is replaced before constructing native IR.
                return {"scan_state": name}
            return {k: plan(v, filters) for k, v in node.items()}

        output = plan(result.to_ir())

        def substitute(node, element, state):
            if isinstance(node, list):
                return [substitute(v, element, state) for v in node]
            if not isinstance(node, IR_NODE_TYPES):
                return node
            if "scan_state" in node:
                return state[node["scan_state"]].to_ir()
            if node.get("op") == "literal":
                return node
            if node.get("op") == "lambda_variable" and node["name"] == parameter:
                return element.to_ir()
            return {k: substitute(v, element, state) for k, v in node.items()}

        def raw(node, dtype, element, state):
            return Expression(dtype, result.source,
                              _IR(substitute(node, element, state),
                                  result._global_refs, result._lookup_refs))

        initial = {}
        for name, function, dtype, _, _ in reducers:
            if function in ("min", "max"):
                initial[name] = missing(dtype)
            elif function in ("collect", "collect_as_set"):
                initial[name] = literal([] if function == "collect" else set(), dtype)
            else:
                initial[name] = literal(
                    True if function == "all" else False if function == "any"
                    else 1 if function == "product" else 0, dtype)
        initial["remaining"] = self._array.length() - 1
        initial["output"] = missing(result.dtype)

        def step(state, element):
            updated = {}
            for name, function, dtype, value, filters in reducers:
                old = state[name]
                selected = state.remaining > 0
                for predicate in filters:
                    # Nested filters short circuit, including erroneous values.
                    selected = if_else(selected, raw(predicate, tbool, element, state), False,
                                       missing_false=True)
                if function == "count":
                    new = old + literal(1, tint64)
                else:
                    value_type = dtype.element_type if isinstance(dtype, (tarray, tset)) else dtype
                    # Explicit native cast: aggregate sums widen their input.
                    value = Expression(value_type, result.source, _IR(
                        {"op": "cast", "value": substitute(value, element, state),
                         "dtype": type_to_wire(value_type)},
                        result._global_refs, result._lookup_refs))
                    if function == "sum":
                        new = if_else(is_missing(value), old, old + value)
                    elif function == "product":
                        new = if_else(is_missing(value), old, old * value)
                    elif function == "min":
                        new = array_min(old, value)
                    elif function == "max":
                        new = array_max(old, value)
                    elif function == "all":
                        new = old & value
                    elif function == "any":
                        new = old | value
                    elif function == "collect":
                        new = old.append(value)
                    else:
                        new = old.add(value)
                updated[name] = if_else(selected, new, old, missing_false=True)
            updated["remaining"] = state.remaining - 1
            updated["output"] = raw(output, result.dtype, element, state)
            return struct(**updated)

        # The fold binds input-position randomness once to the entire step,
        # including nested random parameters and scalar output expressions.
        states = array_scan(step, struct(**initial), self._array)
        return StreamExpression(states[1:].map(lambda state: state.output))

    _aggregate_scan = aggregate_scan

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
        raise TypeError("stream_range start, stop, and step must have int32 type")
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
