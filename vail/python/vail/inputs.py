"""Construct lazy, unindexed table inputs with Arrow-backed literal bindings."""

from .expressions import IR_NODE_TYPES
from collections.abc import Mapping

from .expressions import (Expression, ExpressionException, _IR, _external_globals,
                          array, cast, struct, tuple_)
from .types import (decode_value, infer_type, tarray, tdict, tset, tstruct, ttuple,
                    type_from_wire, unify_types)


def contains_expression(value):
    if isinstance(value, Expression):
        return True
    if isinstance(value, Mapping):
        return any(contains_expression(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(contains_expression(item) for item in value)
    return False


def infer_input_type(values):
    present = [value for value in values if value is not None]
    if not present:
        raise ValueError("cannot infer a type from empty or missing values; provide schema")
    if all(isinstance(value, Mapping) or isinstance(value, Expression) and isinstance(value.dtype, tstruct)
           for value in present):
        def names(value):
            return list(value.dtype) if isinstance(value, Expression) else list(value)
        fields = names(present[0])
        if any(set(names(value)) != set(fields) for value in present):
            raise ValueError("all rows must have the same fields")
        return tstruct(**{name: infer_input_type([value[name] for value in present]) for name in fields})
    if all(isinstance(value, list) for value in present):
        return tarray(infer_input_type([item for value in present for item in value]))
    if all(isinstance(value, tuple) for value in present):
        width = len(present[0])
        if any(len(value) != width for value in present):
            raise TypeError("input tuples must have the same length")
        return ttuple(*(infer_input_type([value[i] for value in present]) for i in range(width)))
    types = [value.dtype if isinstance(value, Expression) else infer_type(value) for value in present]
    result = types[0]
    for ty in types[1:]:
        result = unify_types(result, ty)
        if result is None:
            raise TypeError("input expressions have incompatible types")
    return result


def arrow_literal(value, dtype):
    from . import _native_session, _check_value
    from .arrow import rows_to_arrow
    _check_value(value, dtype, 'value')
    schema = tstruct(value=dtype)
    native = _native_session().scalar_arrow(rows_to_arrow([{'value': value}], schema))
    return _external_globals(native, schema)['value']


def input_expression(value, dtype):
    if isinstance(value, Expression):
        if value.dtype != dtype:
            value = cast(value, dtype)
        return value
    if not contains_expression(value):
        return arrow_literal(value, dtype)
    if isinstance(dtype, tstruct) and isinstance(value, Mapping):
        if set(value) != set(dtype):
            raise ValueError("row fields must match schema")
        return struct(**{name: input_expression(value[name], ty) for name, ty in dtype.items()})
    if isinstance(dtype, tarray) and isinstance(value, (list, tuple)):
        return array([input_expression(item, dtype.element_type) for item in value])
    if isinstance(dtype, ttuple) and isinstance(value, tuple):
        dtype._validate_value(value)
        return tuple_([input_expression(item, ty) for item, ty in zip(value, dtype.types)])
    if isinstance(dtype, tdict) and isinstance(value, Mapping):
        from .collections import _cast
        entries = array([struct(key=input_expression(key, dtype.key_type),
                                value=input_expression(item, dtype.value_type)) for key, item in value.items()])
        return _cast(entries, dtype)
    if isinstance(dtype, tset) and isinstance(value, (set, frozenset)):
        from .collections import _cast
        return _cast(array([input_expression(item, dtype.element_type) for item in value]), dtype)
    raise TypeError(f"cannot construct an expression input of type {dtype}")


def bind_input(expression):
    """Lift literal data and table globals into Arrow-backed native references."""
    from . import _ir_has_aggregate
    ir = expression.to_ir()
    if _ir_has_aggregate(ir):
        raise ExpressionException("parallelize requires an evaluated or lazy scalar aggregate result")
    if expression._row_indexed or expression._lookup_refs:
        raise ExpressionException("parallelize requires unindexed expressions; row fields are not allowed")
    references = dict(expression._global_refs)
    source_globals = expression.source.index_globals() if expression.source is not None else None

    def visit(node):
        if isinstance(node, IR_NODE_TYPES):
            if node.get('op') == 'literal':
                return node
            if node.get('op') == 'global':
                if source_globals is None:
                    raise ExpressionException("unbound input global field")
                bound = source_globals[node['name']]
                references.update(bound._global_refs)
                return bound.to_ir()
            if node.get('op') == 'column':
                raise ExpressionException("parallelize requires unindexed expressions")
            return {name: visit(value) for name, value in node.items()}
        if isinstance(node, list):
            return [visit(value) for value in node]
        return node

    result = visit(ir)
    return Expression(expression.dtype, None, _IR(result, references))
