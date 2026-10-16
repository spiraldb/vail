"""Typed lazy stream expressions built from Vail array expression IR."""

from __future__ import annotations

from .expressions import IR_NODE_TYPES
from ._native import NativeExpr

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
        """Emit each callback result using the preceding elements' scan state.

        The callback receives the current element and may use ``hl.scan``
        reducers. The first element sees each reducer's initial state; the
        current element is included starting at the following output position.
        Missing input arrays produce missing arrays, and empty inputs produce
        empty outputs.
        """
        from uuid import uuid4
        from .expressions import Expression, _IR, _merge_sources, bind, cast, if_else, literal, missing, struct
        from .types import tint64, tbool, tstruct, ttuple, type_from_wire
        from .randomness import _is_random

        if not callable(f):
            raise TypeError("stream.aggregate_scan requires a callable")
        parameter, position_name = uuid4().hex, uuid4().hex
        variable = Expression(self.dtype.element_type, self.source,
                              {"op": "lambda_variable", "name": parameter})
        result = as_expr(f(variable))
        position = Expression(tint32, self.source,
                              {"op": "lambda_variable", "name": position_name})
        source = _merge_sources(self.source, result.source)
        lexical_types = dict(self._array._lexical_types) | dict(result._lexical_types)
        lexical_types[position_name] = tint32
        local_names = {parameter, position_name}
        refs = dict(self._array._global_refs) | dict(result._global_refs)
        lookups = dict(self._array._lookup_refs) | dict(result._lookup_refs)

        def raw(node, dtype, *, variables=None):
            return Expression(dtype, source, _IR(
                node, refs, lookups,
                row_indexed=self._array._row_indexed or result._row_indexed,
                lexical_types=lexical_types if variables is None else variables))

        def substitute(node, replacement):
            return NativeExpr(node).replace_parameter(parameter, NativeExpr(replacement))

        def with_input(items):
            scans = []
            placeholders = []

            def walk(node, predicates=()):
                if isinstance(node, list):
                    return [walk(child, predicates) for child in node]
                if not isinstance(node, IR_NODE_TYPES) or node.get("op") == "literal":
                    return node
                op = node.get("op")
                if op == "agg_filter":
                    return walk(node["body"], predicates + (node["predicate"],))
                if op in ("agg_explode", "agg_array", "agg_group_by", "aggregate_fold"):
                    raise NotImplementedError(f"stream aggregate scan scope {op}")
                if op == "aggregate":
                    options = dict(node.get("options") or {})
                    if options.get("__vail_scan") is not True:
                        from .expressions import ExpressionException
                        raise ExpressionException("local stream scan cannot aggregate")
                    dtype = type_from_wire(options.pop("__vail_result_type"))
                    options.pop("__vail_scan", None)
                    if node.get("init"):
                        raise NotImplementedError("stream aggregate scan initializers")
                    predicate = literal(True)
                    for condition in reversed(predicates):
                        predicate = if_else(raw(condition, tbool), predicate,
                                            False, missing_false=True)
                    # Scoped filters evaluate inner predicates only for selected
                    # inputs, including when an outer predicate is missing.
                    if node["function"] == "count":
                        argument = literal(1, tint64)
                    else:
                        # The native expression reports its declared aggregate
                        # input type through the typed callback expression.
                        source_schema = getattr(source, "_schema", {})
                        global_schema = getattr(source, "_global_schema", {})
                        columns = [(name, dtype._native_type) for name, dtype in source_schema.items()]
                        globals = [(name, dtype._native_type) for name, dtype in global_schema.items()]

                        variables = dict(lexical_types)
                        variables[parameter] = items.dtype.element_type
                        input_type = node["value"].infer_type(
                            columns, globals, [(name, ty._native_type) for name, ty in variables.items()])
                        argument = raw(node["value"], type_from_wire(input_type))
                    def prepare(indexed_item, limit):
                        item = indexed_item[1]
                        selected = raw(substitute(predicate.to_ir(), item.to_ir()), tbool)
                        # The final input contributes to no exclusive prefix.
                        # Guard its predicate as well as its reducer argument.
                        selected = if_else(indexed_item[0] < limit,
                                           selected, False, missing_false=True)
                        item_argument = raw(substitute(argument.to_ir(), item.to_ir()), argument.dtype)
                        return bind(lambda flag: struct(
                            selected=flag,
                            value=if_else(flag, item_argument, missing(argument.dtype)),
                        ), selected)

                    def reduce(prepared):
                        return raw({"op": "array_aggregate_scan", "function": node["function"],
                                    "value": prepared.map(lambda item: item.value).to_ir(),
                                    "predicate": prepared.map(lambda item: item.selected).to_ir(),
                                    "options": options, "dtype": tarray(dtype)._native_type}, tarray(dtype))

                    # Bind the selected flag once per input, then retain the
                    # prepared array once for both reducer inputs.
                    from .collections import _function
                    indexed = _function("__vail_index_values", [items],
                                        tarray(ttuple(tint32, items.dtype.element_type)))
                    # Bind only the scalar bound outside the element map.
                    # Capturing items.length() inside it would retain the whole
                    # input list as a per-element callback capture.
                    scan = bind(lambda limit: bind(
                        reduce, indexed.map(lambda item: prepare(item, limit))),
                        items.length() - 1)
                    scans.append(scan)
                    name = uuid4().hex
                    placeholder = Expression(dtype, result.source,
                                             {"op": "lambda_variable", "name": name})
                    lexical_types[name] = dtype
                    local_names.add(name)
                    placeholders.append(name)
                    return placeholder.to_ir()
                rewritten = {key: walk(child, predicates) for key, child in node.items()}
                if _is_random(node):
                    # Children have already received their own output context.
                    # Prepend only to this call, rather than recursively adding
                    # the same position to nested random parameters again.
                    words = rewritten["args"][2]["elements"]
                    rewritten["args"][2]["elements"] = [cast(position, tint64).to_ir(), *words]
                return rewritten

            body = walk(result.to_ir())
            from .collections import _function
            packed = items
            for values in [*scans, stream_range(items.length()).to_array()]:
                packed = _function("arrays_zip", [packed, values], tarray(tstruct(**{
                    "1": packed.dtype.element_type, "2": values.dtype.element_type,
                })))
            output_name = uuid4().hex
            output = Expression(packed.dtype.element_type, result.source,
                                {"op": "lambda_variable", "name": output_name})
            replacements = {position_name: output["2"].to_ir()}
            current = output["1"]
            for name in reversed(placeholders):
                replacements[name] = current["2"].to_ir()
                current = current["1"]
            replacements[parameter] = current.to_ir()

            # Native lexical substitution respects shadowing in nested binders
            # and preserves sharing in the expression DAG.
            body = NativeExpr(body)
            for name, replacement in replacements.items():
                body = body.replace_parameter(name, NativeExpr(replacement))
            variables = {name: ty for name, ty in lexical_types.items()
                         if name not in local_names}

            # Zip aligns already computed state snapshots with current inputs.
            # No callback captures/indexes a growing array of prior states.
            return raw({"op": "array_map", "value": packed.to_ir(),
                        "parameter": output_name, "body": body}, tarray(result.dtype),
                       variables=variables)

        return StreamExpression(bind(with_input, self._array))

    _aggregate_scan = aggregate_scan

    def grouped(self, n):
        size = as_expr(n)
        if size.dtype != tint32:
            raise TypeError("group size must have int32 type")
        return StreamExpression(self._array.grouped(size))

    def to_array(self):
        return self._array


def stream_range(start, stop=None, step=1):
    """Build a lazy int32 stream from start (inclusive) to stop (exclusive).

    With one argument, start is zero. Step must be nonzero and may be negative.
    Use ``to_array()`` to obtain an array expression.
    """
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
