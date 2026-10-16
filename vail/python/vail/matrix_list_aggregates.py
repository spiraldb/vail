"""Lower matrix aggregation within an owning row to native list expressions.

No partition materialization, row labels, coordinate regrouping, or Python cell
execution. Unsupported shared list kernels are explicit capability boundaries.
"""

from .expressions import IR_NODE_TYPES

from .expressions import Expression, _IR, cast
from .types import tarray, tbool, tint32, tint64, tfloat64, type_from_wire


def reduce_list(
    matrix, expr, base, contexts, outside=None, bound=None, *, direct_entry_sum=False
):
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
    refs = dict(expr._global_refs) | dict(globals._global_refs)
    lookups = dict(expr._lookup_refs)
    if contexts is not None:
        refs.update(contexts._global_refs)
        lookups.update(contexts._lookup_refs)

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
            elif isinstance(node, IR_NODE_TYPES) and node.get("op") != "literal":
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
        if not isinstance(node, IR_NODE_TYPES) or node.get("op") == "literal":
            return node
        if node.get("op") == "matrix_num_cols":
            value = matrix._mir.column_records.length()
            refs.update(value._global_refs)
            return value.to_ir()
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
            elif axis == "col" and "col" in context.dtype:
                value = context.col[name]
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

    if direct_entry_sum:
        from . import if_else, missing, sum as array_sum
        from .matrixtable import _CELLS, _PRESENT, _VALUE

        aggregate = expr.to_ir()
        if aggregate.get("op") != "aggregate" or aggregate.get("function") != "sum":
            raise ValueError("direct entry sum requires a plain sum aggregate")

        def bind_entry(node, cell):
            if isinstance(node, list):
                return [bind_entry(value, cell) for value in node]
            if not isinstance(node, IR_NODE_TYPES) or node.get("op") == "literal":
                return node
            if node.get("op") == "matrix_num_cols":
                value = matrix._mir.column_records.length()
                refs.update(value._global_refs)
                return value.to_ir()
            if node.get("op") == "matrix_scope":
                return bind_entry(node["value"], cell)
            if node.get("op") == "matrix_field":
                axis, name = node["axis"], node["name"]
                if axis == "global":
                    value = globals[name]
                elif axis == "row":
                    value = base[matrix._ROW_NAME][name]
                elif axis == "entry":
                    entry = (
                        cell[_VALUE]
                        if len(matrix.entry.dtype)
                        else struct()
                    )
                    value = entry[name]
                else:
                    raise ValueError("direct entry sum cannot bind column fields")
                refs.update(value._global_refs)
                lookups.update(value._lookup_refs)
                return value.to_ir()
            if node.get("op") == "matrix_column_position":
                raise ValueError("direct entry sum cannot bind column position")
            return {key: bind_entry(value, cell) for key, value in node.items()}

        result_type = expr.dtype
        cells = base[_CELLS]
        values = cells.map(
            lambda cell: if_else(
                cell[_PRESENT],
                cast(raw(bind_entry(aggregate["value"], cell), result_type), result_type),
                missing(result_type),
            )
        )
        result = array_sum(values)
        return Expression(expr.dtype, base, _IR(result.to_ir(), refs, lookups))

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
        if not isinstance(node, IR_NODE_TYPES) or node.get("op") == "literal":
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
            body = node["body"]
            parameter = node["parameter"]
            required = {"entry": set(), "col": set(), "bindings": set()}
            needs_position = False

            def context_uses(expression, lexical=frozenset()):
                nonlocal needs_position
                if isinstance(expression, list):
                    for child in expression:
                        context_uses(child, lexical)
                    return
                if not isinstance(expression, IR_NODE_TYPES) or expression.get("op") == "literal":
                    return
                op = expression.get("op")
                if op == "matrix_field":
                    axis = expression["axis"]
                    if axis in ("entry", "col"):
                        required[axis].add(expression["name"])
                    return
                if op == "matrix_column_position":
                    needs_position = True
                    return
                if op == "lambda_variable":
                    name = expression["name"]
                    context_type = items.dtype.element_type
                    bindings_type = context_type["bindings"] if "bindings" in context_type else None
                    if name not in lexical and bindings_type is not None and name in bindings_type:
                        required["bindings"].add(name)
                    return
                if op in ("array_map", "array_filter"):
                    context_uses(expression["value"], lexical)
                    callback = expression.get("body", expression.get("predicate"))
                    context_uses(callback, lexical | {expression["parameter"]})
                    return
                if op in ("agg_array", "agg_explode"):
                    context_uses(expression["value"], lexical)
                    context_uses(expression["body"], lexical | {expression["parameter"]})
                    return
                for child in expression.values():
                    context_uses(child, lexical)

            context_uses(body, frozenset({parameter}))

            def project_context(ctx):
                fields = {}
                for axis in ("entry", "col"):
                    if required[axis] and axis in ctx.dtype:
                        source = ctx[axis]
                        fields[axis] = struct(
                            **{name: source[name] for name in sorted(required[axis])}
                        )
                if needs_position and "position" in ctx.dtype:
                    fields["position"] = ctx.position
                if "bindings" in ctx.dtype:
                    source = ctx.bindings
                    fields["bindings"] = struct(
                        **{
                            name: source[name]
                            for name in sorted(required["bindings"])
                        }
                    )
                else:
                    fields["bindings"] = struct()
                # The sum lowering uses this bit to ignore absent sparse cells.
                if "present" in ctx.dtype:
                    fields["present"] = ctx.present
                return struct(**fields)

            inputs = items.map(
                lambda ctx: struct(
                    context=project_context(ctx),
                    value=raw(bind(node["value"], ctx), value_type),
                )
            ).filter(lambda item: is_defined(item.value))
            from .collections import _function
            from .types import tstruct

            input_item = inputs.dtype.element_type
            position_items = tstruct(
                context=input_item["context"], value=value_type.element_type
            )
            transposed_type = tarray(tarray(position_items))
            positions = _function("__vail_matrix_transpose", [inputs], transposed_type)
            bound[parameter] = value_type.element_type

            def at(position):
                bound[position.to_ir()["name"]] = position.dtype
                selected = position.map(
                    lambda item: with_binding(item.context, parameter, item.value)
                )
                return typed(raw(walk(body, selected, fixed)))

            result = positions.map(at)
            return typed(result).to_ir()
        if op == "agg_group_by":
            keys = mapped(items, node["key"])
            from .collections import _function, _cast, _ensure_key_supported
            from .types import tstruct, tdict
            _ensure_key_supported(keys.dtype.element_type)
            pairs = items.map(lambda ctx: struct(
                key=raw(bind(node["key"], ctx), keys.dtype.element_type), value=ctx,
            ))
            groups = _function("__vail_collection_group", [pairs],
                tarray(tstruct(key=keys.dtype.element_type, value=items.dtype)))

            def group(record):
                bound[record.to_ir()["name"]] = record.dtype
                value = typed(raw(walk(node["body"], record.value, fixed)))
                return struct(key=record.key, value=value)

            result = groups.map(group)
            dtype = result.dtype.element_type
            return _cast(result, tdict(dtype["key"], dtype["value"])).to_ir()
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
            # Entry-list contexts retain the cell-presence bit so a plain
            # matrix entry sum can reduce each row without first filtering and
            # materializing the full cell struct. Missing cells contribute a
            # missing scalar; Arrow's typed array sum ignores null elements.
            if function == "sum" and "present" in contexts.dtype.element_type:
                element_type = values.dtype.element_type
                values = typed(
                    items.map(
                        lambda ctx: if_else(
                            ctx.present,
                            raw(bind(node["value"], ctx), element_type),
                            missing(element_type),
                        )
                    )
                )
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
                calls = values.map(lambda value: value["__vail_genomic_call"])
                initial = dict(node.get("init", []))["n_alleles"]
                n = raw(bind(initial, fixed), tint32)
                result = raw({"op": "scalar_function", "function": "genomic_list_call_stats",
                              "args": [calls.to_ir(), n.to_ir()]}, dtype)
                # Matrix row reductions historically return element-wise missing
                # frequencies for AN=0; table aggregates return a missing array.
                from . import bind as bind_value, range as array_range
                result = bind_value(lambda stats: struct(
                    AC=stats.AC,
                    AF=if_else(stats.AN > 0, stats.AF,
                               array_range(n).map(lambda _: missing(tfloat64))),
                    AN=stats.AN, homozygote_count=stats.homozygote_count), result)
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
        if op in ("matrix_field", "matrix_scope", "matrix_num_cols"):
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
