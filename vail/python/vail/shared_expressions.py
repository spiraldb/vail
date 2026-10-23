"""Demand-preserving bindings for shared, already-lowered list reductions."""
from collections import Counter
from .expressions import Expression, NativeExpr, _IR, IR_NODE_TYPES
from .types import type_from_wire


def bind_shared_list_results(expression, table):
    """Bind shared work only where an existing strict path already demands it.

    Conditionals expose only their condition as strict; lambda bodies and RNG
    stay opaque. The existing native bind implementation evaluates vectors once,
    not a Python callback per row. This is query construction only.
    """
    if expression.to_ir().contains_random():
        return expression
    retained, rewritten = {}, {}

    def identity(node):
        return node.node_identity()

    def opaque(node):
        op = node.get('op')
        return op in {'array_map', 'array_filter', 'array_fold', 'array_predicate'} or (
            op == 'nested_function' and str(node.get('function', '')).startswith('__vail_list_'))

    def map_value(value, visit):
        if isinstance(value, NativeExpr):
            return visit(value)
        if isinstance(value, list):
            return [map_value(child, visit) for child in value]
        if isinstance(value, dict):
            return {name: map_value(child, visit) for name, child in value.items()}
        return value

    def view(node):
        values = dict(node.items())
        children = []
        def add(child):
            children.append(child)
            return child
        map_value(values, add)
        return values, children

    def expensive(node, seen=None):
        seen = set() if seen is None else seen
        key = identity(node)
        if key in seen:
            return False
        seen.add(key)
        if node.get('op') == 'nested_function' and str(node.get('function', '')).startswith('__vail_list_reduce_'):
            return True
        if node.get('op') == 'array_map':
            # A lexical bind's source can itself contain a reduction, while
            # its callback remains a separate scope that must stay opaque.
            return expensive(node.get('value'), seen)
        if opaque(node):
            return False
        return any(expensive(child, seen) for child in view(node)[1])

    def strict_children(node, values, children):
        op = node.get('op')
        if op == 'if_else':
            return [values['condition']]
        if op in {'binary', 'integer_binary'}:
            # Logical conjunction/disjunction may short-circuit the right side.
            return [values['left']] if values.get('operator') in {'and', 'or'} else children
        if op == 'nested_function' and values.get('function') == 'coalesce':
            return values['args'][:1]
        if op in {'struct', 'tuple', 'cast', 'get_field', 'is_missing', 'unary'}:
            return children
        return []

    def typed(node):
        dtype = type_from_wire(node.infer_type(
            [(n, t._native_type) for n, t in table._schema.items()],
            [(n, t._native_type) for n, t in table._global_schema.items()],
            [(n, t._native_type) for n, t in expression._lexical_types.items()]))
        return Expression(dtype, table, _IR(node, expression._global_refs,
            expression._lookup_refs, lexical_types=expression._lexical_types))

    def bind_at(node):
        nodes, edges, counts = {}, {}, Counter()
        pending = [node]
        while pending:
            current = pending.pop()
            key = identity(current)
            if key in nodes:
                continue
            nodes[key] = current
            values, children = view(current)
            edges[key] = (values, children)
            if not opaque(current):
                counts.update(identity(child) for child in children)
                pending.extend(children)
        # Only unconditional demand paths can justify moving a may-error node.
        strict, pending = set(), [node]
        while pending:
            current = pending.pop()
            key = identity(current)
            if key in strict:
                continue
            strict.add(key)
            if not opaque(current):
                pending.extend(strict_children(current, *edges[key]))
        candidates = {key for key in strict if counts[key] > 1 and expensive(nodes[key])}
        if not candidates:
            return node
        # Bind outer shared nodes; their inner work remains owned by that binding.
        selected = []
        pending, seen = [node], set()
        while pending:
            current = pending.pop()
            key = identity(current)
            if key in seen:
                continue
            seen.add(key)
            if key in candidates:
                selected.append(current)
            elif not opaque(current):
                pending.extend(edges[key][1])
        if not selected:
            return node
        from . import bind
        dtype = typed(node).dtype
        def body(*variables):
            replacements = {identity(value): variable.to_ir() for value, variable in zip(selected, variables)}
            memo = {}
            def replace(current):
                key = identity(current)
                if key in replacements:
                    return replacements[key]
                if key not in memo:
                    memo[key] = current if opaque(current) else NativeExpr(map_value(dict(current.items()), replace))
                return memo[key]
            return Expression(dtype, table, _IR(replace(node), expression._global_refs,
                expression._lookup_refs, lexical_types=expression._lexical_types))
        return bind(body, *(typed(value) for value in selected)).to_ir()

    def rewrite(node):
        key = identity(node)
        if key in rewritten:
            return rewritten[key]
        retained[key] = node
        if opaque(node) or not expensive(node):
            result = node
        else:
            result = bind_at(NativeExpr(map_value(dict(node.items()), rewrite)))
        rewritten[key] = result
        return result

    result = rewrite(expression.to_ir())
    return Expression(expression.dtype, table, _IR(result, expression._global_refs,
        expression._lookup_refs, lexical_types=expression._lexical_types))
