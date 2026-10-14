"""Lower collection aggregations to native list expressions."""

from uuid import uuid4
from .expressions import Expression, _IR, as_expr, cast
from .types import tint64, tbool, tarray, tset


def aggregate(value, f):
    value = as_expr(value)
    parameter = uuid4().hex
    variable = Expression(
        value.dtype.element_type,
        value.source,
        {"op": "lambda_variable", "name": parameter},
    )
    result = as_expr(f(variable))
    refs = dict(value._global_refs) | dict(result._global_refs)
    lookups = dict(value._lookup_refs) | dict(result._lookup_refs)

    def substitute(node, name, replacement):
        if isinstance(node, list):
            return [substitute(v, name, replacement) for v in node]
        if not isinstance(node, dict) or node.get("op") == "literal":
            return node
        if node.get("op") == "lambda_variable" and node["name"] == name:
            return replacement
        return {k: substitute(v, name, replacement) for k, v in node.items()}

    def raw(node, dtype):
        return Expression(dtype, value.source, _IR(node, refs, lookups))

    def mapped(items, node, dtype, name):
        return items.map(lambda x: raw(substitute(node, name, x.to_ir()), dtype))

    def walk(node, dtype, items=value, name=parameter):
        if not isinstance(node, dict) or node.get("op") == "literal":
            return node
        op = node.get("op")
        if op == "agg_explode":
            # The element type is available through the callback's array input
            # for the supported collection expressions.
            raise NotImplementedError("nested explode in collection aggregate")
        if op == "agg_filter":
            selected = mapped(items, node["predicate"], tbool, name)
            from .collection_functions import zip

            filtered = zip(items, selected).filter(lambda x: x[1]).map(lambda x: x[0])
            return walk(node["body"], dtype, filtered, name)
        if op == "aggregate":
            function = node["function"]
            if function == "count":
                return cast(items.length(), tint64).to_ir()
            element_type = (
                dtype.element_type if isinstance(dtype, (tarray, tset)) else dtype
            )
            values = mapped(items, node["value"], element_type, name)
            if function == "collect":
                return values.to_ir()
            if function == "collect_as_set":
                from .collections import set_

                return set_(values).to_ir()
            if function in ("sum", "product", "min", "max"):
                return values._reduce(function).to_ir()
            if function in ("all", "any"):
                return (
                    values.all(lambda x: x)
                    if function == "all"
                    else values.any(lambda x: x)
                ).to_ir()
            raise NotImplementedError(f"collection aggregate {function}")
        if op == "struct":
            return dict(
                node,
                fields=[[n, walk(v, dtype[n], items, name)] for n, v in node["fields"]],
            )
        return {
            k: walk(v, dtype, items, name)
            if isinstance(v, dict)
            else [walk(x, dtype, items, name) if isinstance(x, dict) else x for x in v]
            if isinstance(v, list)
            else v
            for k, v in node.items()
        }

    return Expression(
        result.dtype,
        value.source,
        _IR(walk(result.to_ir(), result.dtype), refs, lookups),
    )
