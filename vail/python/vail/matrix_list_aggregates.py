"""Lower matrix aggregation within an owning row to native list expressions.

No partition materialization, row labels, coordinate regrouping, or Python cell
execution. Unsupported shared list kernels are explicit capability boundaries.
"""

from .expressions import Expression, _IR, cast
from .types import tarray, tbool, tint32, tint64, tfloat64, type_from_wire


def reduce_list(matrix, expr, base, contexts, outside=None, bound=None):
    from . import struct, literal, missing, if_else, is_defined, len as length
    from . import (
        sum as array_sum,
        product as array_product,
        min as array_min,
        max as array_max,
    )
    from . import set as make_set, dict as make_dict, tuple as make_tuple, case

    bound = dict(bound or {})
    globals = matrix.index_globals()
    refs = (
        dict(expr._global_refs)
        | dict(contexts._global_refs)
        | dict(globals._global_refs)
    )
    lookups = dict(expr._lookup_refs) | dict(contexts._lookup_refs)

    def raw(node, dtype=tint32):
        return Expression(dtype, base, _IR(node, refs, lookups))

    def typed(value):
        # Schema inference only; this does not evaluate or collect the list.
        node = value.to_ir()
        free = set()

        def variables(node, lexical=frozenset()):
            if isinstance(node, list):
                for v in node:
                    variables(v, lexical)
            elif isinstance(node, dict) and node.get("op") != "literal":
                if node.get("op") == "lambda_variable":
                    if node["name"] not in lexical:
                        free.add(node["name"])
                elif node.get("op") in ("array_map", "array_filter"):
                    variables(node["value"], lexical)
                    variables(
                        node["body"]
                        if node["op"] == "array_map"
                        else node["predicate"],
                        lexical | {node["parameter"]},
                    )
                else:
                    for v in node.values():
                        variables(v, lexical)

        variables(node)
        bindings = {name: bound[name] for name in free if name in bound}
        for name, dtype in bindings.items():
            node = {
                "op": "array_map",
                "value": literal([None], tarray(dtype)).to_ir(),
                "parameter": name,
                "body": node,
            }
        probe = base.key_by()._project([("__matrix_type_probe", raw(node))], keys=())
        dtype = probe._schema["__matrix_type_probe"]
        for _ in bindings:
            dtype = dtype.element_type
        return Expression(dtype, base, value.to_ir())

    def bind(node, context):
        if isinstance(node, list):
            return [bind(v, context) for v in node]
        if not isinstance(node, dict) or node.get("op") == "literal":
            return node
        if node.get("op") == "matrix_scope":
            return bind(node["value"], context)
        if node.get("op") == "matrix_column_position":
            return context.position.to_ir()
        if node.get("op") == "matrix_field":
            axis, name = node["axis"], node["name"]
            if axis == "global":
                value = globals[name]
            elif axis == "row":
                value = base[matrix._ROW_NAME][name]
            elif axis == "col" and "position" in context.dtype:
                value = matrix._mir.column_records[context.position][
                    "__vail_matrix_col"
                ][name]
            else:
                value = context[axis][name]
            refs.update(value._global_refs)
            lookups.update(value._lookup_refs)
            return value.to_ir()
        if (
            node.get("op") == "lambda_variable"
            and node["name"] in context.dtype["bindings"]
        ):
            return context["bindings"][node["name"]].to_ir()
        return {k: bind(v, context) for k, v in node.items()}

    def mapped(items, node):
        return typed(items.map(lambda ctx: raw(bind(node, ctx))))

    def with_binding(ctx, name, value):
        return struct(
            **{n: ctx[n] for n in ctx.dtype if n != "bindings"},
            bindings=struct(
                **{n: ctx.bindings[n] for n in ctx.bindings.dtype}, **{name: value}
            ),
        )

    def same(left, right):
        return if_else(
            is_defined(left), is_defined(right) & (left == right), ~is_defined(right)
        )

    def walk(node, items, fixed):
        if isinstance(node, list):
            return [walk(v, items, fixed) for v in node]
        if not isinstance(node, dict) or node.get("op") == "literal":
            return node
        op = node.get("op")
        if op == "agg_filter":
            selected = items.filter(
                lambda ctx: raw(bind(node["predicate"], ctx), tbool)
            )
            return walk(node["body"], selected, fixed)
        if op == "agg_explode":
            value_type = mapped(items, node["value"]).dtype.element_type
            if not isinstance(value_type, tarray):
                from .types import tset

                if not isinstance(value_type, tset):
                    raise TypeError("agg.explode requires a collection")

            def expand(ctx):
                value = raw(bind(node["value"], ctx), value_type)
                if not isinstance(value_type, tarray):
                    value = value.to_array()
                return value.map(lambda v: with_binding(ctx, node["parameter"], v))

            return walk(node["body"], items.flatmap(expand), fixed)
        if op == "agg_array":
            values = mapped(items, node["value"])
            value_type = values.dtype.element_type
            present = items.filter(
                lambda ctx: is_defined(raw(bind(node["value"], ctx), value_type))
            )
            lengths = values.filter(is_defined).map(length)
            maximum = array_max(lengths)
            width = if_else(is_defined(maximum), maximum, literal(0, tint32))
            valid = length(lengths.filter(lambda n: n != width)) == 0
            width = (
                case()
                .when(valid, width)
                .or_error("agg.array_agg requires equal array lengths")
            )
            from . import range as array_range

            def at(i):
                bound[i.to_ir()["name"]] = i.dtype
                selected = present.map(
                    lambda ctx: with_binding(
                        ctx,
                        node["parameter"],
                        raw(bind(node["value"], ctx), value_type)[i],
                    )
                )
                return typed(raw(walk(node["body"], selected, fixed)))

            result = array_range(width).map(at)
            return typed(result).to_ir()
        if op == "agg_group_by":
            keys = mapped(items, node["key"])
            from .collections import to_array

            unique = to_array(make_set(keys))

            def group(key):
                bound[key.to_ir()["name"]] = key.dtype
                selected = items.filter(
                    lambda ctx: same(
                        raw(bind(node["key"], ctx), keys.dtype.element_type), key
                    )
                )
                value = typed(raw(walk(node["body"], selected, fixed)))
                return make_tuple([key, value])

            return typed(make_dict(unique.map(group))).to_ir()
        if op == "aggregate_fold":
            raise NotImplementedError(
                "matrix list aggregation requires a native list-fold kernel for agg.fold"
            )
        if op == "aggregate" and not matrix._is_scan_node(node):
            options = node.get("options") or {}
            dtype = type_from_wire(options["__vail_result_type"])
            function = node["function"]
            if function == "count":
                return cast(length(items), tint64).to_ir()
            values = mapped(items, node["value"])
            if function == "collect":
                result = values
            elif function == "collect_as_set":
                result = make_set(values)
            elif function == "take":
                if options.get("ordered"):
                    raise NotImplementedError(
                        "ordered matrix list take requires native list sorting by an expression"
                    )
                result = values[: options["n"]]
            elif function == "call_stats":
                from . import range as array_range

                calls = values.map(lambda v: v["__vail_genomic_call"]).filter(
                    is_defined
                )
                alleles = calls.map(
                    lambda call: raw(
                        {
                            "op": "get_field",
                            "value": call.to_ir(),
                            "name": "__vail_call_alleles",
                        },
                        tarray(tint32),
                    )
                )
                initial = dict(node.get("init", []))["n_alleles"]
                n = raw(bind(initial, fixed), tint32)
                n = (
                    case()
                    .when(is_defined(n) & (n >= 0), n)
                    .or_error("call_stats requires a defined nonnegative allele count")
                )
                flattened = alleles.flatmap(lambda a: a)
                valid = length(flattened.filter(lambda a: (a < 0) | (a >= n))) == 0
                n = (
                    case()
                    .when(valid, n)
                    .or_error("call_stats allele index outside allele count")
                )
                an = cast(array_sum(alleles.map(length)), tint32)
                ac = array_range(n).map(
                    lambda a: cast(
                        array_sum(flattened.map(lambda v: if_else(v == a, 1, 0))),
                        tint32,
                    )
                )
                hom = array_range(n).map(
                    lambda a: cast(
                        array_sum(
                            alleles.map(
                                lambda call: if_else(
                                    (length(call) == 2)
                                    & (length(call.filter(lambda v: v != a)) == 0),
                                    1,
                                    0,
                                )
                            )
                        ),
                        tint32,
                    )
                )
                af = ac.map(
                    lambda count: if_else(an > 0, count / an, missing(tfloat64))
                )
                result = struct(AC=ac, AF=af, AN=an, homozygote_count=hom)
            elif function in ("sum", "product", "min", "max", "mean", "stddev_pop"):
                numbers = values.map(
                    lambda v: cast(
                        v, dtype if function not in ("mean", "stddev_pop") else tfloat64
                    )
                )
                defined = numbers.filter(is_defined)
                n = length(defined)
                if function in ("sum", "product", "min", "max"):
                    result = {
                        "sum": array_sum,
                        "product": array_product,
                        "min": array_min,
                        "max": array_max,
                    }[function](numbers)
                else:
                    mean = array_sum(defined) / n
                    if function == "mean":
                        result = if_else(n > 0, mean, missing(tfloat64))
                    else:
                        variance = array_sum(defined.map(lambda v: (v - mean) ** 2)) / n
                        result = if_else(n > 0, variance**0.5, missing(tfloat64))
            elif function in ("count_where", "fraction", "any", "all"):
                yes = array_sum(
                    values.map(
                        lambda v: if_else(
                            is_defined(v) & v, literal(1, tint64), literal(0, tint64)
                        )
                    )
                )
                if function == "count_where":
                    result = yes
                elif function == "any":
                    result = yes > 0
                elif function == "all":
                    no = array_sum(
                        values.map(
                            lambda v: if_else(
                                is_defined(v) & ~v,
                                literal(1, tint64),
                                literal(0, tint64),
                            )
                        )
                    )
                    result = no == 0
                else:
                    result = if_else(
                        length(items) > 0,
                        yes / length(items),
                        literal(float("nan"), tfloat64),
                    )
            else:
                raise NotImplementedError(
                    f"matrix list aggregation requires a native per-list {function} reducer"
                )
            return (
                cast(result, dtype).to_ir() if result.dtype != dtype else result.to_ir()
            )
        if op in ("matrix_field", "matrix_scope"):
            return bind(node, fixed)
        return {k: walk(v, items, fixed) for k, v in node.items()}

    fixed = (
        outside
        if outside is not None
        else struct(
            row=base[matrix._ROW_NAME], col=struct(), entry=struct(), bindings=struct()
        )
    )
    result = walk(expr.to_ir(), contexts, fixed)
    return Expression(expr.dtype, base, _IR(result, refs, lookups))
