"""Resolve captured table lookups into lazy native TableIR dependencies."""
from uuid import uuid4
import json

from .expressions import Expression, ExpressionException, _IR, _global_bindings, _merge_sources
from .types import tinterval, tstruct, ttuple


def index(table, expressions, all_matches=False, query_interval=False):
    if type(all_matches) is not bool:
        raise TypeError("all_matches must be a bool")
    if type(query_interval) is not bool:
        raise TypeError("query_interval must be a bool")
    if not expressions:
        raise ValueError("Require at least one expression to index")
    if any(not isinstance(expr, Expression) for expr in expressions):
        raise TypeError("Index arguments must be expressions")
    if not table._keys:
        raise ValueError("Cannot index an unkeyed table")
    expected = [table._schema[name] for name in table._keys]
    if [expr.dtype for expr in expressions] != expected:
        if len(expected) == len(expressions) == 1 and isinstance(expected[0], tinterval) and expressions[0].dtype == expected[0].point_type:
            interval_point = True
        elif (query_interval and len(expected) == len(expressions) == 1
              and isinstance(expressions[0].dtype, tinterval)
              and expressions[0].dtype.point_type == expected[0]):
            interval_point = True
        else:
            interval_point = False
        if not interval_point and len(expressions) == 1:
            packed = expressions[0]
            if isinstance(packed.dtype, tstruct):
                return index(table, tuple(packed[name] for name in packed.dtype), all_matches)
            if isinstance(packed.dtype, ttuple):
                return index(table, tuple(packed[i] for i in range(len(packed.dtype.types))), all_matches)
        if not interval_point:
            raise ExpressionException(f"lookup key type mismatch: expected {expected}, found {[expr.dtype for expr in expressions]}")
    source = None
    for expr in expressions:
        source = _merge_sources(source, expr.source)
    from . import _referenced_columns

    from .matrixtable import _axes
    if source is None or not any(_referenced_columns(expr.to_ir()) or _axes(expr.to_ir()) for expr in expressions):
        raise ExpressionException("Cannot index with a scalar expression")
    reference = uuid4().hex
    dtype = tstruct(**{name: ty for name, ty in table._schema.items() if name not in table._keys})
    if all_matches:
        from .types import tarray
        dtype = tarray(dtype)
    return Expression(dtype, source, _IR(
        {"op": "table_lookup", "reference": reference, "keys": [expr.to_ir() for expr in expressions], "all_matches": all_matches, "query_interval": query_interval},
        {}, {reference: table._native},
    ))


def resolve(native, expressions, *, random_scope="row"):
    """Append lookup DAG nodes, replacing each lookup with its private column."""
    from .randomness import resolve as resolve_randomness
    native, expressions = resolve_randomness(native, list(expressions), random_scope)
    targets = {}
    for expr in expressions:
        targets.update(expr._lookup_refs)
    if not targets:
        return native, expressions
    occupied = {name for name, _ in json.loads(native.schema_json())}
    occupied.update(name for name, _ in json.loads(native.global_schema_json()))
    resolved = {}
    bindings = _global_bindings(expressions)

    def rewrite(node):
        nonlocal native
        if isinstance(node, list):
            return [rewrite(child) for child in node]
        if not isinstance(node, dict) or node.get("op") == "literal":
            return node
        if node.get("op") == "table_lookup":
            reference = node["reference"]
            if reference not in resolved:
                keys = [rewrite(key) for key in node["keys"]]
                name = "__vail_lookup"
                while name in occupied:
                    name += "_"
                occupied.add(name)
                native = native.lookup_json(targets[reference], json.dumps(keys), name, node.get("all_matches", False), bindings, node.get("query_interval", False))
                resolved[reference] = name
            return {"op": "column", "name": resolved[reference]}
        return {name: rewrite(child) for name, child in node.items()}

    output = [Expression(expr.dtype, expr.source, _IR(rewrite(expr.to_ir()), expr._global_refs),
                         selection_name=expr._selection_name, top_level_name=expr._top_level_name)
              for expr in expressions]
    return native, output
