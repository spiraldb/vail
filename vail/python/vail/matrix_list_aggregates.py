"""Lower matrix aggregation within an owning row to native list expressions.

No partition materialization, row labels, coordinate regrouping, or Python cell
execution. Unsupported shared list kernels are explicit capability boundaries.
"""

from .expressions import IR_NODE_TYPES

from .expressions import Expression, _IR, cast
from .types import tarray, tbool, tint32, tint64, type_from_wire


def reduce_row_predicate(matrix, expr, base):
    """Reduce a primitive predicate without filtering or rebuilding cell structs.

    The general context path is needed for aggregate combinators. Plain boolean
    reductions can instead apply entry presence inside the primitive projection,
    leaving DataFusion to map the predicate and reduce the resulting list.
    """
    from . import if_else, literal, missing, struct
    from .collections import _function
    from .types import tfloat64
    from .matrixtable import _CELLS, _COL, _PRESENT, _VALUE, _zip_arrays

    node = expr.to_ir()
    functions = {"count_where", "fraction", "any", "all"}
    if (node.get("op") != "aggregate" or node.get("function") not in functions
            or matrix._is_scan_node(node)):
        return None
    function = node["function"]
    predicate = Expression(tbool, matrix, _IR(node["value"], expr._global_refs, expr._lookup_refs))
    # Filtering first supplies random expressions' present-entry UID context.
    # Lookup expressions also retain the established position-sensitive path.
    if predicate.to_ir().contains_random() or predicate._lookup_refs:
        return None
    entries, columns, needs_position = predicate.to_ir().matrix_dependencies()
    required = {"entry": set(entries), "col": set(columns)}
    # Position-sensitive lookups retain the established indexed-context path.
    if needs_position:
        return None
    cells = base[_CELLS]
    if required["col"]:
        # Project before zipping: arrays_zip must not replicate unrelated column
        # annotations, and the predicate never needs a full entry context.
        cells = cells.map(lambda cell: struct(**{
            _PRESENT: cell[_PRESENT],
            _VALUE: struct(**{name: cell[_VALUE][name]
                             for name in matrix.entry.dtype if name in required["entry"]}),
        }))
        columns = matrix._mir.column_records.map(lambda column: struct(**{
            name: column[_COL][name]
            for name in matrix.col.dtype if name in required["col"]
        }))
        items = _zip_arrays(cells, columns)

        def project(item):
            cell = item["1"]
            value = matrix._lower_cell(predicate, base, cell, item["2"])
            return if_else(cell[_PRESENT], value, missing(tbool))

    else:
        items = cells

        def project(cell):
            value = matrix._lower_cell(predicate, base, cell, struct())
            return if_else(cell[_PRESENT], value, missing(tbool))

    values = items.map(project)
    if function == "fraction":
        count = _function("__vail_list_reduce_count_where", [values], tint64)
        present = _function("__vail_list_reduce_count_where",
                            [base[_CELLS].map(lambda cell: cell[_PRESENT])], tint64)
        return if_else(present > 0, count / present, literal(float("nan"), tfloat64))
    return _function("__vail_list_reduce_" + function, [values], expr.dtype)


def reduce_list(
    matrix, expr, base, contexts, outside=None, bound=None, *, direct_entry_sum=False
):
    from . import struct, literal, missing, if_else, is_defined, len as length

    bound = dict(bound or {})
    globals = matrix.index_globals()
    refs = dict(expr._global_refs) | dict(globals._global_refs)
    lookups = dict(expr._lookup_refs)
    if contexts is not None:
        refs.update(contexts._global_refs)
        lookups.update(contexts._lookup_refs)

    lexical_types = dict(expr._lexical_types)
    if contexts is not None:
        lexical_types.update(contexts._lexical_types)
    if outside is not None:
        lexical_types.update(outside._lexical_types)

    def raw(node, dtype=tint32):
        return Expression(dtype, base, _IR(
            node, refs, lookups, lexical_types=lexical_types | bound))

    def typed(value):
        # Query the original native graph with its real schema and lexical
        # types. No fake values or executable probe plans are constructed.
        free = set()

        def lexical_inputs(node, lexical=frozenset()):
            if isinstance(node, list):
                for child in node:
                    lexical_inputs(child, lexical)
            elif isinstance(node, IR_NODE_TYPES) and node.get("op") != "literal":
                if node.get("op") == "lambda_variable":
                    if node["name"] not in lexical:
                        free.add(node["name"])
                elif node.get("op") in ("array_map", "array_filter"):
                    lexical_inputs(node["value"], lexical)
                    body = node["body"] if node["op"] == "array_map" else node["predicate"]
                    lexical_inputs(body, lexical | {node["parameter"]})
                else:
                    for child in node.values():
                        lexical_inputs(child, lexical)

        lexical_inputs(value.to_ir())
        dtype = type_from_wire(value.to_ir().infer_type(
            [(name, dtype._native_type) for name, dtype in base._schema.items()],
            [(name, dtype._native_type) for name, dtype in base._global_schema.items()],
            [(name, dtype._native_type) for name, dtype in bound.items() if name in free],
        ))
        return Expression(dtype, base, _IR(
            value.to_ir(), value._global_refs, value._lookup_refs,
            lexical_types=dict(value._lexical_types) | bound))

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
            body = node.get("body")
            fused_reducers = {
                "count", "count_where", "sum", "product", "min", "max",
                "mean", "stddev_pop", "fraction", "any", "all",
            }
            aggregate_nodes = []
            has_unsupported_aggregation = False

            def same_node(left, right):
                if left is right:
                    return True
                try:
                    return left == right
                except (TypeError, ValueError):
                    return False

            def find_aggregates(value, filters=()):
                nonlocal has_unsupported_aggregation
                if isinstance(value, list):
                    for child in value:
                        find_aggregates(child, filters)
                    return
                if not isinstance(value, IR_NODE_TYPES):
                    return
                op = value.get("op")
                if op == "agg_filter":
                    find_aggregates(value["body"], filters + (value["predicate"],))
                    return
                if op == "aggregate":
                    aggregate_nodes.append((value, filters))
                    return
                elif isinstance(op, str) and op.startswith("agg_"):
                    has_unsupported_aggregation = True
                for child in value.values():
                    find_aggregates(child, filters)

            find_aggregates(body)
            supported_aggregates = [
                (aggregate, filters)
                for aggregate, filters in aggregate_nodes
                if aggregate.get("function") in fused_reducers and len(filters) <= 1
            ]
            if (
                supported_aggregates
                and len(supported_aggregates) == len(aggregate_nodes)
                and not has_unsupported_aggregation
            ):
                functions = []
                arguments = [keys]
                output_fields = {}
                filtered_items = []

                def filtered_input(predicate):
                    for existing, selected, selected_keys in filtered_items:
                        if same_node(existing, predicate):
                            return selected, selected_keys
                    selected = items.filter(
                        lambda ctx: raw(bind(predicate, ctx), tbool)
                    )
                    # The outer grouping keys define the output key universe,
                    # including keys whose reducers select no values. Evaluate
                    # the key once for every item, then select that existing
                    # key list with the filter bitmap.
                    from .matrixtable import _zip_arrays

                    predicate_values = mapped(items, predicate)
                    selected_keys = _zip_arrays(keys, predicate_values).filter(
                        lambda pair: pair["2"]
                    ).map(lambda pair: pair["1"])
                    filtered_items.append((predicate, selected, selected_keys))
                    return selected, selected_keys

                for index, (aggregate, filters) in enumerate(supported_aggregates):
                    reducer = aggregate["function"]
                    functions.append(reducer + ("_filtered" if filters else ""))
                    aggregate_items = items
                    if filters:
                        aggregate_items, selected_keys = filtered_input(filters[0])
                        arguments.append(selected_keys)
                    if reducer != "count":
                        arguments.append(mapped(aggregate_items, aggregate["value"]))
                    aggregate_options = aggregate.get("options") or {}
                    output_fields[f"__agg_{index}"] = type_from_wire(
                        aggregate_options["__vail_result_type"]
                    )
                multiple = len(supported_aggregates) > 1
                if multiple:
                    result_value_type = tstruct(**output_fields)
                    function_name = (
                        "__vail_collection_group_aggregate_multi__" + "__".join(functions)
                    )
                else:
                    result_value_type = next(iter(output_fields.values()))
                    function_name = "__vail_collection_group_aggregate_" + functions[0]
                grouped_type = tarray(
                    tstruct(key=keys.dtype.element_type, value=result_value_type)
                )
                groups = _function(
                    function_name,
                    arguments,
                    grouped_type,
                )
                if len(supported_aggregates) == 1 and same_node(body, supported_aggregates[0][0]):
                    return _cast(
                        groups,
                        tdict(keys.dtype.element_type, result_value_type),
                    ).to_ir()

                def project(record):
                    bound[record.to_ir()["name"]] = record.dtype
                    aggregate_fields = [
                        (
                            aggregate,
                            filters,
                            (
                                record.value[f"__agg_{index}"] if multiple else record.value
                            ).to_ir(),
                        )
                        for index, (aggregate, filters) in enumerate(supported_aggregates)
                    ]

                    def substitute(value, filters=()):
                        if isinstance(value, IR_NODE_TYPES):
                            for aggregate, aggregate_filters, replacement in aggregate_fields:
                                if same_node(value, aggregate) and len(filters) == len(aggregate_filters) and all(
                                    same_node(left, right)
                                    for left, right in zip(filters, aggregate_filters)
                                ):
                                    return replacement
                        if isinstance(value, IR_NODE_TYPES) and value.get("op") == "agg_filter":
                            return substitute(value["body"], filters + (value["predicate"],))
                        if isinstance(value, list):
                            return [substitute(child, filters) for child in value]
                        if isinstance(value, IR_NODE_TYPES):
                            return {
                                key: substitute(child, filters)
                                for key, child in value.items()
                            }
                        return value

                    projected = substitute(body)
                    value = typed(raw(walk(projected, items, fixed)))
                    return struct(key=record.key, value=value)

                projected_groups = groups.map(project)
                projected_type = projected_groups.dtype.element_type
                return _cast(
                    projected_groups,
                    tdict(projected_type["key"], projected_type["value"]),
                ).to_ir()
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
            from .collections import _function

            arguments = [values]
            if function == "call_stats":
                initial = dict(node.get("init", []))["n_alleles"]
                arguments.append(raw(bind(initial, fixed), tint32))
            if function == "take":
                arguments.append(literal(options["n"], tint64))
                if options.get("ordered"):
                    function = "take_ordered"
            result = _function(
                "__vail_list_reduce_" + function, arguments, dtype
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
    return Expression(expr.dtype, base, _IR(
        result, refs, lookups, lexical_types=lexical_types | bound))
