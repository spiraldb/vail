"""Construct lazy, unindexed table inputs with native literal ownership."""

from collections.abc import Mapping

from .expressions import (Expression, ExpressionException, _IR,
                          array, cast, struct, tuple_)
from .types import (infer_type, tarray, tdict, tset, tstruct, ttuple,
                    unify_types)


def contains_expression(value):
    if isinstance(value, Expression):
        return True
    if isinstance(value, Mapping):
        return any(contains_expression(key) or contains_expression(item) for key, item in value.items())
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
    from . import _check_value
    from .expressions import literal
    _check_value(value, dtype, 'value')
    # Native literals own scalar/Arrow storage directly. Literal leaves are not
    # singleton globals tables, so mixed inputs have no per-leaf dependencies.
    return literal(value, dtype)


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
    """Validate an unindexed native DAG and bind referenced table globals."""
    ir = expression.to_ir()
    try:
        names = ir.input_global_names()
    except ValueError as error:
        raise ExpressionException(str(error)) from None
    if expression._row_indexed or expression._lookup_refs:
        raise ExpressionException("parallelize requires unindexed expressions; row fields are not allowed")
    references = dict(expression._global_refs)
    replacements = []
    if names:
        if expression.source is None:
            raise ExpressionException("unbound input global field")
        # Project before boxing: unused global definitions must stay lazy.
        source_globals = expression.source.select_globals(*names).index_globals()
        for name in names:
            bound = source_globals[name]
            references.update(bound._global_refs)
            replacements.append((name, bound.to_ir()))
    result = ir.bind_input_globals(replacements)
    return Expression(expression.dtype, None, _IR(result, references, row_indexed=False))
