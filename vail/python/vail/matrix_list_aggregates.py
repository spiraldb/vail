"""Lower matrix aggregation within an owning row to native list expressions.

No partition materialization, row labels, coordinate regrouping, or Python cell
execution. Unsupported shared list kernels are explicit capability boundaries.
"""

from .expressions import IR_NODE_TYPES, NativeExpr

from .expressions import Expression, _IR, cast
from .types import tarray, tbool, tcall, tint32, tint64, type_from_wire, type_to_wire



def _entry_field_input(node):
    """Whether a direct entry-field access already carries entry validity.

    Do not infer this merely from entry dependence: constants, coalesce and
    missingness predicates can produce defined values for an absent entry.
    Keep the conservative presence guard for every computed expression.
    """
    return node.get("op") == "matrix_field" and node.get("axis") == "entry"


def reduce_row_predicate(matrix, expr, base):
    """Reduce a primitive predicate without filtering or rebuilding cell structs.

    The general context path is needed for aggregate combinators. Plain boolean
    reductions can instead apply entry presence inside the primitive projection,
    leaving DataFusion to map the predicate and reduce the resulting list.
    """
    from . import if_else, is_defined, literal, missing, struct
    from .collections import _function
    from .types import tfloat64, tstruct
    from .matrixtable import _CELLS, _COL, _VALUE, _zip_arrays

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
        projected_type = tstruct(**{
            name: matrix.entry.dtype[name]
            for name in matrix.entry.dtype if name in required["entry"]
        })
        cells = cells.map(lambda cell: struct(**{
            _VALUE: if_else(
                is_defined(cell[_VALUE]),
                struct(**{name: cell[_VALUE][name]
                          for name in matrix.entry.dtype if name in required["entry"]}),
                missing(projected_type),
            ),
        }))
        columns = matrix._mir.column_records.map(lambda column: struct(**{
            name: column[_COL][name]
            for name in matrix.col.dtype if name in required["col"]
        }))
        items = _zip_arrays(cells, columns)

        def project(item):
            cell = item["1"]
            value = matrix._lower_cell(predicate, base, cell, item["2"])
            return if_else(is_defined(cell[_VALUE]), value, missing(tbool))

    else:
        items = cells

        def project(cell):
            value = matrix._lower_cell(predicate, base, cell, struct())
            return if_else(is_defined(cell[_VALUE]), value, missing(tbool))

    values = items.map(project)
    if function == "fraction":
        count = _function("__vail_list_reduce_count_where", [values], tint64)
        present = _function("__vail_list_reduce_count_where",
                            [base[_CELLS].map(lambda cell: is_defined(cell[_VALUE]))], tint64)
        return if_else(present > 0, count / present, literal(float("nan"), tfloat64))
    return _function("__vail_list_reduce_" + function, [values], expr.dtype)


def reduce_aligned_plain(matrix, expr, base):
    """Map a plain row aggregate input directly over aligned entry slots.

    This avoids constructing per-entry context structs for the common
    row/column/entry field expression. Aggregate combinators keep using the
    established context binder below.
    """
    node = expr.to_ir()
    from .matrixtable import (
        _contains_matrix_num_cols,
        _is_scan,
        _uses_axis_field,
        _uses_column_position,
    )
    if (node.get("op") != "aggregate" or _is_scan(node)
            or node.get("value") is None
            or set(node.get("options") or {}) - {"__vail_result_type"}
            or expr._lookup_refs):
        return None
    function = node.get("function")
    if function == "call_stats":
        # call_stats carries a row initializer (the allele count) beside the
        # packed call/alleles input. Keep that count at row granularity and map
        # only the Call field over aligned entry slots.
        initial = dict(node.get("init") or [])
        fields = dict(node["value"].get("fields") or [])
        call_node = fields.get("__vail_genomic_call")
        n_alleles_node = initial.get("n_alleles")
        if call_node is None or n_alleles_node is None:
            return None
        call_ir = Expression(
            type_from_wire(call_node.infer_type(
                [(name, dtype._native_type) for name, dtype in base._schema.items()],
                [(name, dtype._native_type) for name, dtype in base._global_schema.items()],
                [(name, dtype._native_type) for name, dtype in expr._lexical_types.items()],
            )),
            matrix,
            _IR(call_node, expr._global_refs, expr._lookup_refs),
        )
        if call_ir.dtype != tcall:
            return None
        if (call_node.contains_random() or _uses_column_position(call_node)):
            return None
        n_alleles_type = type_from_wire(n_alleles_node.infer_type(
            [(name, dtype._native_type) for name, dtype in base._schema.items()],
            [(name, dtype._native_type) for name, dtype in base._global_schema.items()],
            [(name, dtype._native_type) for name, dtype in expr._lexical_types.items()],
        ))
        if n_alleles_type != tint32:
            return None
        n_alleles = Expression(
            n_alleles_type,
            matrix,
            _IR(n_alleles_node, expr._global_refs, expr._lookup_refs),
        )
        from . import if_else, is_defined, missing, struct
        from .matrixtable import _CELLS, _VALUE

        n_alleles = matrix._lower(n_alleles, base)
        has_outer_capture = any((
            _uses_axis_field(call_node, "row"),
            _uses_axis_field(call_node, "col"),
            _uses_axis_field(call_node, "global"),
            _contains_matrix_num_cols(call_node),
        ))
        if has_outer_capture:
            calls = matrix._aligned_matrix_map(
                base, call_node, call_ir.dtype, preserve_cell=False
            )
        else:
            def project_call(cell):
                call = matrix._lower_cell(call_ir, base, cell, struct())
                if _entry_field_input(call_node):
                    return call
                return if_else(is_defined(cell[_VALUE]), call, missing(tcall))

            calls = base[_CELLS].map(project_call)
        from .collections import _function
        return _function("__vail_list_call_stats", [n_alleles, calls], expr.dtype)
    if function not in {
        "sum", "product", "min", "max", "mean", "stddev_pop",
        "count_where", "any", "all",
    }:
        return None
    value_node = node["value"]
    if value_node.contains_random() or _uses_column_position(value_node):
        return None
    if not any((
        _uses_axis_field(value_node, "row"),
        _uses_axis_field(value_node, "col"),
        _uses_axis_field(value_node, "global"),
        _contains_matrix_num_cols(value_node),
    )):
        return None

    # Infer from the actual row/global schema. The native compiler understands
    # matrix_field types here, before the field references are replaced by
    # aligned input leaves.
    value_type = type_from_wire(value_node.infer_type(
        [(name, dtype._native_type) for name, dtype in base._schema.items()],
        [(name, dtype._native_type) for name, dtype in base._global_schema.items()],
        [(name, dtype._native_type) for name, dtype in expr._lexical_types.items()],
    ))
    values = matrix._aligned_matrix_map(
        base, value_node, value_type, preserve_cell=False
    )
    from .collections import _function
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
        from .matrixtable import _CELLS, _VALUE

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
        def project_sum(cell):
            # raw's declared Python type is the result type, so cast(raw(...))
            # would elide the required Int32/Float32 aggregate widening.
            value = raw({
                "op": "cast",
                "dtype": type_to_wire(result_type),
                "value": bind_entry(aggregate["value"], cell),
            }, result_type)
            if _entry_field_input(aggregate["value"]):
                return value
            return if_else(is_defined(cell[_VALUE]), value, missing(result_type))

        values = cells.map(project_sum)
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

    # Preserve shared aggregate/expression nodes while changing their scope.
    # Rebuilding a shared leaf separately for each reference turns one reducer
    # into multiple physical UDFs (notably composed genomic summaries).
    rewritten_nodes, retained_nodes = {}, {}

    def walk(node, items, fixed):
        if not isinstance(node, IR_NODE_TYPES) or isinstance(node, dict) and "op" not in node:
            return walk_uncached(node, items, fixed)
        item_node, fixed_node = items.to_ir(), fixed.to_ir()
        identity = node.node_identity() if isinstance(node, NativeExpr) else id(node)
        key = (identity, item_node.node_identity(), fixed_node.node_identity())
        if key in rewritten_nodes:
            return rewritten_nodes[key]
        retained_nodes[key] = (node, item_node, fixed_node)
        result = walk_uncached(node, items, fixed)
        if isinstance(result, dict) and "op" in result:
            result = NativeExpr(result)
        rewritten_nodes[key] = result
        return result

    def walk_uncached(node, items, fixed):
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

            def node_identity(value):
                identity = getattr(value, "node_identity", None)
                return identity() if identity is not None else id(value)

            def same_node(left, right):
                return node_identity(left) == node_identity(right)

            # NativeExpr.view() materializes wrappers for inline Expr children.
            # Keep each first view alive so discovery and substitution share
            # those child handles instead of receiving fresh Arc wrappers.
            native_views = {}
            native_view_nodes = []

            def node_view(value):
                if not isinstance(value, NativeExpr):
                    return value
                identity = node_identity(value)
                if identity not in native_views:
                    native_views[identity] = value.view()
                    native_view_nodes.append(value)
                return native_views[identity]

            visited_aggregates = {}
            visited_nodes = []

            def find_aggregates(value, filters=()):
                nonlocal has_unsupported_aggregation
                if isinstance(value, list):
                    for child in value:
                        find_aggregates(child, filters)
                    return
                if not isinstance(value, IR_NODE_TYPES):
                    return
                view = node_view(value)
                op = view.get("op")
                if op == "literal":
                    return
                visit_key = (node_identity(value), tuple(map(node_identity, filters)))
                if visit_key in visited_aggregates:
                    return
                visited_aggregates[visit_key] = True
                visited_nodes.append(value)
                if op == "agg_filter":
                    find_aggregates(view["body"], filters + (view["predicate"],))
                    return
                if op == "aggregate":
                    aggregate_nodes.append((value, filters))
                    return
                elif isinstance(op, str) and op.startswith("agg_"):
                    has_unsupported_aggregation = True
                for child in view.values():
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
                unfiltered_arguments = []
                output_fields = {}
                filter_groups = []
                for index, (aggregate, filters) in enumerate(supported_aggregates):
                    reducer = aggregate["function"]
                    if filters:
                        group = next(
                            (
                                group_index
                                for group_index, (predicate, _) in enumerate(filter_groups)
                                if same_node(predicate, filters[0])
                            ),
                            None,
                        )
                        if group is None:
                            group = len(filter_groups)
                            filter_groups.append((filters[0], []))
                        filter_groups[group][1].append(index)
                        functions.append(f"{reducer}_filtered{group}")
                    else:
                        functions.append(reducer)
                        if reducer != "count":
                            unfiltered_arguments.append(mapped(items, aggregate["value"]))
                    aggregate_options = aggregate.get("options") or {}
                    output_fields[f"__agg_{index}"] = type_from_wire(
                        aggregate_options["__vail_result_type"]
                    )
                if filter_groups:
                    from .matrixtable import _zip_arrays

                    for predicate, aggregate_indices in filter_groups:
                        # Keys are computed for all items above so outer-key
                        # errors and empty filtered groups retain Hail semantics.
                        # The one selected pair list shares predicate evaluation,
                        # keys, and reducer contexts for this filter scope.
                        selected_pairs = _zip_arrays(keys, items).filter(
                            lambda pair: raw(bind(predicate, pair["2"]), tbool)
                        )

                        def project_pair(pair, indices=aggregate_indices):
                            fields = {"key": pair["1"]}
                            for aggregate_index in indices:
                                aggregate = supported_aggregates[aggregate_index][0]
                                if aggregate["function"] != "count":
                                    fields[f"__value_{aggregate_index}"] = raw(
                                        bind(aggregate["value"], pair["2"])
                                    )
                            return struct(**fields)

                        arguments.append(selected_pairs.map(project_pair))
                arguments.extend(unfiltered_arguments)
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
                    aggregate_replacements = {
                        (node_identity(aggregate), tuple(map(node_identity, filters))): replacement
                        for aggregate, filters, replacement in aggregate_fields
                    }

                    def substitute(value, filters=()):
                        view = node_view(value) if isinstance(value, IR_NODE_TYPES) else value
                        if isinstance(value, IR_NODE_TYPES) and view.get("op") == "literal":
                            return value
                        key = (node_identity(value), tuple(map(node_identity, filters)))
                        if key in rewrite_memo:
                            return rewrite_memo[key]
                        if isinstance(value, IR_NODE_TYPES) and key in aggregate_replacements:
                            return aggregate_replacements[key]
                        if isinstance(value, IR_NODE_TYPES) and view.get("op") == "agg_filter":
                            result = substitute(view["body"], filters + (view["predicate"],))
                        elif isinstance(value, list):
                            result = [substitute(child, filters) for child in value]
                        elif isinstance(value, IR_NODE_TYPES):
                            result = {
                                key: substitute(child, filters)
                                for key, child in view.items()
                            }
                        else:
                            return value
                        rewrite_memo[key] = result
                        rewrite_nodes.append(value)
                        return result

                    rewrite_memo = {}
                    rewrite_nodes = []
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
            if function == "call_stats":
                fields = dict(node["value"].get("fields") or [])
                call_node = fields.get("__vail_genomic_call")
                if call_node is None:
                    raise ValueError("call_stats aggregate is missing its Call input")
                values = mapped(items, call_node)
            else:
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
                n_alleles = raw(bind(initial, fixed), tint32)
                arguments = [n_alleles, values]
            if function == "take":
                arguments.append(literal(options["n"], tint64))
                if options.get("ordered"):
                    function = "take_ordered"
            if function == "linreg":
                arguments.append(literal(options.get("nested_dim", 1), tint64))
            function_name = (
                "__vail_list_call_stats"
                if function == "call_stats"
                else "__vail_list_reduce_" + function
            )
            result = _function(
                function_name, arguments, dtype
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


def reduce_owning_coordinate_sum(matrix, expr):
    """Admit one complete global identity-coordinate integer SUM query.

    No eligible-leaf rewriting: mixed aggregates and computed/effectful inputs
    retain the complete original entry relation and its evaluation domain.
    """
    from .agg import _aggregate
    from .matrixtable import _CELLS, _VALUE

    root = expr.to_ir()
    if (expr._lookup_refs or not isinstance(root, IR_NODE_TYPES)
            or root.get("op") != "agg_array" or expr.dtype != tarray(tint64)):
        return None
    source = root.get("value")
    body = root.get("body")
    if (not isinstance(source, IR_NODE_TYPES) or not _entry_field_input(source)
            or not isinstance(body, IR_NODE_TYPES) or body.get("op") != "aggregate"
            or body.get("function") != "sum" or body.get("init")
            or set(body.get("options") or {}) != {"__vail_result_type"}
            or type_from_wire(body["options"]["__vail_result_type"]) != tint64):
        return None
    variable = body.get("value")
    if (not isinstance(variable, IR_NODE_TYPES)
            or variable.get("op") != "lambda_variable"
            or variable.get("name") != root.get("parameter")):
        return None
    name = source.get("name")
    # A direct field's original schema descriptor must agree before admission;
    # the executable adapter resolves it again against the final Arrow schema.
    dtype = matrix.entry.dtype[name]
    if dtype not in (tarray(tint32), tarray(tint64)):
        return None
    core = matrix._mir.core
    total = _aggregate("__coordinate_owning_sum", core[_CELLS], expr.dtype,
                       options={"path": [_VALUE, name]})
    return core, total


def reduce_composite_totals(matrix, expr):
    """Lower exact associative integer total bundles to ordinary DF aggregates.

    Eligibility and rewriting preserve DAG sharing. Floating-point sums keep
    the original aggregation tree because regrouping can change their results.
    """
    from . import is_defined
    from . import agg
    from .collections import _function
    from .matrixtable import _CELLS, _VALUE, _uses_axis_field, _uses_column_position

    root = expr.to_ir()
    if root.get("op") not in {"struct", "tuple"} or expr._lookup_refs:
        return None
    core = matrix._mir.core
    leaves = {}
    eligibility = {}
    # Retain every native handle whose identity is cached throughout rewriting.
    retained = {}
    views = {}

    def view(node):
        identity = node.node_identity()
        if identity not in views:
            retained[identity] = node
            # Struct children are inline Expr values: view() creates fresh Arc
            # wrappers. Retain one view so discovery and substitution see the
            # same child identities, as in the existing row-reducer walker.
            views[identity] = node.view()
        return views[identity]

    def eligible(node):
        if not isinstance(node, IR_NODE_TYPES):
            return False
        identity = node.node_identity()
        if identity in eligibility:
            return eligibility[identity]
        retained[identity] = node
        data = view(node)
        op = data.get("op")
        if op == "struct":
            result = all(eligible(value) for _, value in data["fields"])
        elif op == "tuple":
            result = all(eligible(value) for value in data["elements"])
        elif (op != "aggregate" or matrix._is_scan_node(node)
                or set(node.get("options") or {}) != {"__vail_result_type"}
                or node.get("init")):
            result = False
        elif node.get("function") == "count":
            result = True
            leaves[identity] = node
        elif node.get("function") == "sum":
            value = node.get("value")
            dtype = type_from_wire(node["options"]["__vail_result_type"])
            result = (dtype == tint64 and value.is_pure()
                      and not _uses_axis_field(value, "col")
                      and not _uses_column_position(value))
            if result:
                leaves[identity] = node
        else:
            result = False
        eligibility[identity] = result
        return result

    if not eligible(root) or not leaves:
        return None

    replacements = {}
    refs = dict(expr._global_refs)
    for identity, node in leaves.items():
        dtype = type_from_wire(node["options"]["__vail_result_type"])
        if node["function"] == "count":
            present = core[_CELLS].map(lambda cell: is_defined(cell[_VALUE]))
            row_value = _function("__vail_list_reduce_count_where", [present], tint64)
        else:
            leaf = Expression(dtype, matrix, _IR(
                node, expr._global_refs, expr._lookup_refs,
                lexical_types=expr._lexical_types,
            ))
            row_value = reduce_list(matrix, leaf, core, None, direct_entry_sum=True)
        refs.update(row_value._global_refs)
        replacements[identity] = agg.sum(row_value).to_ir()

    rebuilt = dict(replacements)

    def rebuild(node):
        identity = node.node_identity()
        if identity in rebuilt:
            return rebuilt[identity]
        data = view(node)
        if data.get("op") == "struct":
            value = {"op": "struct", "fields": [
                [name, rebuild(value)] for name, value in data["fields"]]}
        else:
            value = {"op": "tuple", "elements": [rebuild(value) for value in data["elements"]]}
        # Native handles preserve shared children; a repeated Python dictionary
        # alone would be expanded again by the native expression parser.
        result = NativeExpr(value)
        rebuilt[identity] = result
        return result

    total = Expression(expr.dtype, core, _IR(
        rebuild(root), refs, expr._lookup_refs,
        lexical_types=expr._lexical_types,
    ))
    return core, total
