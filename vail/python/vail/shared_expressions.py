"""Demand-preserving bindings for shared, already-lowered list reductions."""
from collections import Counter
from .expressions import Expression, NativeExpr, _IR
from .types import type_from_wire


def bind_shared_list_results(expression, table):
    """Reuse vectors through the existing native lexical binder.

    Analyze the retained DAG once. Bit summaries track shared expensive nodes,
    repeated demand, and unconditional demand. No descendant walk is repeated
    for each ancestor. Bit operations scale with the number of shared candidates
    (one bit per candidate), rather than the expanded expression tree.
    """
    root = expression.to_ir()
    if root.contains_random():
        return expression
    nodes, views, edges, postorder, seen = {}, {}, {}, [], set()

    def key(node):
        return node.node_identity()

    def opaque(node):
        # Switch/callback variables belong to their binder, not the root schema.
        return node.get('op') in {'switch', 'array_map', 'array_filter',
            'array_fold', 'array_predicate'} or (node.get('op') == 'nested_function'
            and str(node.get('function', '')).startswith('__vail_list_'))

    def map_value(value, visit):
        if isinstance(value, NativeExpr):
            return visit(value)
        if isinstance(value, list):
            return [map_value(child, visit) for child in value]
        if isinstance(value, dict):
            return {name: map_value(child, visit) for name, child in value.items()}
        return value

    # Retain each inspection view: inline Vec children have fresh wrapper owners
    # on a second view(), so replacement must reuse these same child handles.
    pending = [(root, False)]
    counts = Counter()
    while pending:
        node, finish = pending.pop()
        identity = key(node)
        if finish:
            postorder.append(identity)
            continue
        if identity in seen:
            continue
        seen.add(identity)
        nodes[identity] = node
        values, children = dict(node.items()), []
        def add(child):
            children.append(child)
            return child
        if not opaque(node):
            map_value(values, add)
        views[identity], edges[identity] = values, children
        counts.update(key(child) for child in children)
        pending.append((node, True))
        pending.extend((child, False) for child in reversed(children))

    expensive = {}
    for identity in postorder:
        node = nodes[identity]
        expensive[identity] = (node.get('op') == 'nested_function'
            and str(node.get('function', '')).startswith('__vail_list_reduce_')) or any(
                expensive[key(child)] for child in edges[identity])
    candidates = [identity for identity in postorder if counts[identity] > 1 and expensive[identity]]
    if not candidates:
        return expression
    bits = {identity: 1 << index for index, identity in enumerate(candidates)}
    by_bit = {bit: identity for identity, bit in bits.items()}

    def strict_children(identity):
        node, values, children = nodes[identity], views[identity], edges[identity]
        op = node.get('op')
        if op == 'if_else':
            return [values['condition']]
        if op in {'binary', 'integer_binary'}:
            return [values['left']] if values.get('operator') in {'and', 'or'} else children
        if op == 'nested_function' and values.get('function') == 'coalesce':
            return values['args'][:1]
        if op in {'struct', 'tuple', 'cast', 'get_field', 'is_missing', 'unary'}:
            return children
        return []

    summaries = {}
    for identity in postorder:
        once, twice = bits.get(identity, 0), 0
        for child in edges[identity]:
            child_once, child_twice, _ = summaries[key(child)]
            twice |= child_twice | (once & child_once)
            once |= child_once
        strict = bits.get(identity, 0)
        for child in strict_children(identity):
            strict |= summaries[key(child)][2]
        summaries[identity] = once, twice, strict

    def typed(node, environment):
        lexical = dict(expression._lexical_types)
        for variable in environment.values():
            lexical.update(variable._lexical_types)
        dtype = type_from_wire(node.infer_type(
            [(n, t._native_type) for n, t in table._schema.items()],
            [(n, t._native_type) for n, t in table._global_schema.items()],
            [(n, t._native_type) for n, t in lexical.items()]))
        return Expression(dtype, table, _IR(node, expression._global_refs,
            expression._lookup_refs, lexical_types=lexical))

    replaced, rewritten = {}, {}
    def environment_key(environment):
        return tuple((identity, key(variable.to_ir())) for identity, variable in environment.items())

    def replace_bound(node, environment):
        identity = key(node)
        if identity in environment:
            return environment[identity].to_ir()
        cache_key = identity, environment_key(environment)
        if cache_key not in replaced:
            replaced[cache_key] = node if opaque(node) else NativeExpr(
                map_value(views[identity], lambda child: replace_bound(child, environment)))
        return replaced[cache_key]

    def rewrite(node, environment, bound_bits):
        identity = key(node)
        if identity in environment:
            return environment[identity].to_ir()
        cache_key = identity, environment_key(environment)
        if cache_key in rewritten:
            return rewritten[cache_key]
        _, twice, strict = summaries[identity]
        available = twice & strict & ~bound_bits
        if available:
            # Bind dependencies first, then their users in the extended lexical
            # environment; eager bind arguments never duplicate one another.
            choice = None
            remaining = available
            while remaining:
                bit = remaining & -remaining
                candidate = by_bit[bit]
                if not (summaries[candidate][0] & available & ~bit):
                    choice = candidate, bit
                    break
                remaining ^= bit
            assert choice is not None  # retained graph is acyclic
            candidate, bit = choice
            argument = typed(replace_bound(nodes[candidate], environment), environment)
            from . import bind
            def body(variable):
                nested = {**environment, candidate: variable}
                return typed(rewrite(node, nested, bound_bits | bit), nested)
            result = bind(body, argument).to_ir()
        else:
            result = node if opaque(node) else NativeExpr(map_value(
                views[identity], lambda child: rewrite(child, environment, bound_bits)))
        rewritten[cache_key] = result
        return result

    result = rewrite(root, {}, 0)
    return Expression(expression.dtype, table, _IR(result, expression._global_refs,
        expression._lookup_refs, lexical_types=expression._lexical_types))
