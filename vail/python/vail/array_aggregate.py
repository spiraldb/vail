"""Lower collection aggregations to native list expressions."""

from .expressions import IR_NODE_TYPES

from uuid import uuid4
from .expressions import Expression, _IR, as_expr, cast
from .types import tint32, tint64, tbool, tarray, tset, type_from_wire


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
        if not isinstance(node, IR_NODE_TYPES) or node.get("op") == "literal":
            return node
        if node.get("op") == "lambda_variable" and node["name"] == name:
            return replacement
        return {k: substitute(v, name, replacement) for k, v in node.items()}

    def raw(node, dtype):
        return Expression(dtype, value.source, _IR(node, refs, lookups))

    def mapped(items, node, name):
        # The input and result of a reducer often have different types. Query
        # the original native IR using its actual lexical/source environment.
        columns = [(field, dtype._native_type) for field, dtype in getattr(value.source, "_schema", {}).items()]
        globals = [(field, dtype._native_type) for field, dtype in getattr(value.source, "_global_schema", {}).items()]
        input_type = type_from_wire(node.infer_type(columns, globals, [(name, items.dtype.element_type._native_type)]))
        return items.map(lambda x: raw(substitute(node, name, x.to_ir()), input_type))

    def walk(node, dtype, items=value, name=parameter):
        if not isinstance(node, IR_NODE_TYPES) or node.get("op") == "literal":
            return node
        op = node.get("op")
        if op == "agg_explode":
            # The element type is available through the callback's array input
            # for the supported collection expressions.
            raise NotImplementedError("nested explode in collection aggregate")
        if op == "agg_filter":
            filtered = items.filter(
                lambda x: raw(substitute(node["predicate"], name, x.to_ir()), tbool)
            )
            return walk(node["body"], dtype, filtered, name)
        if op == "aggregate":
            function = node["function"]
            if function == "count":
                return cast(items.length(), tint64).to_ir()
            options = node.get("options") or {}
            result_type = type_from_wire(options["__vail_result_type"])
            values = mapped(items, node["value"], name)
            from .collections import _function
            from .expressions import literal

            arguments = [values]
            if function == "call_stats":
                initial = dict(node.get("init", []))["n_alleles"]
                arguments.append(raw(initial, tint32))
            if function == "take":
                arguments.append(literal(options["n"], tint64))
                if options.get("ordered"):
                    function = "take_ordered"
            return _function(
                "__vail_list_reduce_" + function, arguments, result_type
            ).to_ir()
        if op == "struct":
            return dict(
                node,
                fields=[[n, walk(v, dtype[n], items, name)] for n, v in node["fields"]],
            )
        return {
            k: walk(v, dtype, items, name)
            if isinstance(v, IR_NODE_TYPES)
            else [walk(x, dtype, items, name) if isinstance(x, IR_NODE_TYPES) else x for x in v]
            if isinstance(v, list)
            else v
            for k, v in node.items()
        }

    return Expression(
        result.dtype,
        value.source,
        _IR(walk(result.to_ir(), result.dtype), refs, lookups),
    )
