"""Collection APIs composed from DataFusion functions and typed expressions."""
import builtins
import uuid

from .expressions import (Expression, _merge_sources, as_expr, if_else, is_missing,
                          literal, missing, struct)
from .types import (tarray, tset, tdict, ttuple, tstruct, tint32, tbool, tstr,
                    tfloat32, tfloat64, tint64, type_to_wire)


def _cast(value, dtype):
    return Expression(dtype, value.source,
                      {"op": "collection_cast", "value": value.to_ir(), "dtype": type_to_wire(dtype)})


def _function(name, args, dtype):
    source = None
    for arg in args:
        source = _merge_sources(source, arg.source)
    return Expression(dtype, source, {"op": "nested_function", "function": name,
                                     "args": [arg.to_ir() for arg in args], "dtype": type_to_wire(dtype)})


def _distinct_supported(dtype):
    # DataFusion normalizes signed float zeros. RowConverter does not canonicalize
    # nested sets/dictionaries using Vail's logical key equivalence, either.
    if dtype in (tfloat32, tfloat64) or isinstance(dtype, (tset, tdict)):
        return False
    if isinstance(dtype, tarray):
        return _distinct_supported(dtype.element_type)
    if isinstance(dtype, tstruct):
        return builtins.all(_distinct_supported(dtype[name]) for name in dtype)
    if isinstance(dtype, ttuple):
        return builtins.all(_distinct_supported(ty) for ty in dtype.types)
    from .types import tinterval, tndarray
    if isinstance(dtype, tinterval):
        return _distinct_supported(dtype.point_type)
    return not isinstance(dtype, tndarray)


def set_(collection):
    if isinstance(collection, builtins.set):
        return literal(collection)
    value = as_expr(collection)
    if isinstance(value.dtype, tset):
        return value
    if not isinstance(value.dtype, tarray):
        raise TypeError("set expects an array or set expression")
    if not _distinct_supported(value.dtype.element_type):
        raise NotImplementedError("DataFusion array_distinct does not preserve Vail key equality for this element type")
    return _function("array_distinct", [value], tset(value.dtype.element_type))


def empty_set(element_type):
    return literal(builtins.set(), tset(element_type))


def dict_(collection):
    if isinstance(collection, builtins.dict):
        from .expressions import array
        key_type = array(list(collection.keys())).dtype.element_type
        value_type = array(list(collection.values())).dtype.element_type
        if not builtins.any(isinstance(item, Expression) for pair in collection.items() for item in pair):
            return literal(collection, tdict(key_type, value_type))
        entries = array([struct(key=_coerce(key, key_type), value=_coerce(value, value_type))
                         for key, value in collection.items()])
        return _dict_from_entries(entries, key_type, value_type)
    value = as_expr(collection)
    if isinstance(value.dtype, tdict):
        return value
    if isinstance(value.dtype, tset):
        value = _cast(value, tarray(value.dtype.element_type))
    if not isinstance(value.dtype, tarray):
        raise TypeError("dict expects a mapping or an array of key-value pairs")
    pair_type = value.dtype.element_type
    literal_array = _direct_literal_array(value)
    if isinstance(pair_type, ttuple) and len(pair_type.types) == 2:
        key_type, value_type = pair_type.types
        present = value._array_filter(lambda pair: ~is_missing(pair))
        entries = present._array_map(lambda pair: struct(key=pair[0], value=pair[1]))
    elif isinstance(pair_type, tstruct) and "key" in pair_type and "value" in pair_type:
        key_type, value_type = pair_type["key"], pair_type["value"]
        present = value._array_filter(lambda pair: ~is_missing(pair))
        entries = present._array_map(lambda pair: struct(key=pair.key, value=pair.value))
    else:
        raise TypeError("dict requires an array of two-element tuples or key/value structs")
    return _dict_from_entries(entries, key_type, value_type, last_wins=literal_array)


def _canonical_key(value):
    return Expression(tstr, value.source,
                      {"op": "nested_function", "function": "canonical_key",
                       "args": [value.to_ir()], "dtype": type_to_wire(tstr)})


def _literal_container_ir(node):
    if not isinstance(node, dict):
        return True
    op = node.get("op")
    if op == "literal":
        return True
    if op == "cast":
        return _literal_container_ir(node.get("value"))
    if op == "unary":
        return _literal_container_ir(node.get("value"))
    if op == "binary":
        return _literal_container_ir(node.get("left")) and _literal_container_ir(node.get("right"))
    if op not in ("array", "tuple", "struct"):
        return False
    if op in ("array", "tuple"):
        children = node.get("elements", [])
    else:
        fields = node.get("fields", {})
        children = [entry[1] for entry in fields] if isinstance(fields, list) else fields.values()
    return builtins.all(_literal_container_ir(child) for child in children)


def _direct_literal_array(value):
    """Whether an array is a direct literal container, eligible for Hail's
    constant-folded ToDict duplicate resolution."""
    node = value.to_ir()
    if node.get("op") == "literal":
        return True
    return node.get("op") == "array" and builtins.all(
        _literal_container_ir(item) for item in node.get("elements", [])
    )


def _dict_from_entries(entries, key_type, value_type, *, last_wins=False):
    """Sort and deduplicate canonical keys using Hail's source-dependent tie behavior."""
    keys = entries._array_map(lambda entry: _canonical_key(entry.key))
    keys = _function("array_distinct", [keys], keys.dtype)
    keys = _function("array_sort", [keys, literal("ASC"), literal("NULLS LAST")], keys.dtype)

    def selected_entry(key):
        matches = entries._array_filter(lambda entry: _canonical_key(entry.key) == key)
        index = -1 if last_wins else 1
        return _function("array_element", [matches, literal(index, tint64)], entries.dtype.element_type)

    unique_entries = keys._array_map(selected_entry)
    return _cast(unique_entries, tdict(key_type, value_type))


def empty_dict(key_type, value_type):
    return literal({}, tdict(key_type, value_type))


def to_array(value):
    if isinstance(value.dtype, tarray):
        return value
    if isinstance(value.dtype, tset):
        return _cast(value, tarray(value.dtype.element_type))
    if isinstance(value.dtype, tdict):
        return value._array_map(lambda entry: as_expr((entry.key, entry.value)))
    raise TypeError("array conversion requires an array, set, or dictionary")


def length(value):
    value = as_expr(value)
    from .types import tstr
    if value.dtype == tstr:
        from .strings import length as string_length
        return string_length(value)
    if not isinstance(value.dtype, (tarray, tset, tdict)):
        raise TypeError("len requires an array, set, or dictionary expression")
    return Expression(tint32, value.source, {"op": "array_length", "value": value.to_ir()})


def _coerce(value, dtype):
    value = missing(dtype) if value is None else as_expr(value)
    if value.dtype != dtype:
        from .expressions import cast
        numeric = (tbool, tint32, tint64, tfloat32, tfloat64)
        if dtype not in numeric or value.dtype not in numeric or numeric.index(value.dtype) > numeric.index(dtype):
            raise TypeError(f"expected {dtype}, found {value.dtype}")
        value = cast(value, dtype)
    return value


def _equal(left, right):
    # The key codec defines equality recursively, including missing children,
    # signed zero, and canonical set/dictionary representations.
    return _canonical_key(left) == _canonical_key(right)


def contains(value, key):
    if isinstance(value.dtype, tdict):
        key = _coerce(key, value.dtype.key_type)
        matches = value._array_filter(lambda entry: _equal(entry.key, key))
    elif isinstance(value.dtype, tset):
        key = _coerce(key, value.dtype.element_type)
        matches = value._array_filter(lambda item: _equal(item, key))
    else:
        raise TypeError("contains requires a set or dictionary")
    return length(matches) > 0


def get(value, key, default=None):
    key = _coerce(key, value.dtype.key_type)
    default = _coerce(default, value.dtype.value_type)
    matches = value._array_filter(lambda entry: _equal(entry.key, key))
    values = matches._array_map(lambda entry: entry.value)
    first = _function("array_element", [values, literal(1, tint64)], value.dtype.value_type)
    return if_else(length(matches) > 0, first, default)


def keys(value):
    return value._array_map(lambda entry: entry.key)


def values(value):
    return value._array_map(lambda entry: entry.value)


def key_set(value):
    # Dictionary keys are already unique; no deduplication is needed.
    return _cast(keys(value), tset(value.dtype.key_type))


def map_values(value, f):
    if not callable(f):
        raise TypeError("map_values requires a callable")
    result = value._array_map(lambda entry: struct(key=entry.key, value=f(entry.value)))
    entry = result.dtype.element_type
    return _cast(result, tdict(entry['key'], entry['value']))


def set_binary(left, right, operation):
    right = literal(right, left.dtype) if isinstance(right, builtins.set) else as_expr(right)
    if right.dtype != left.dtype:
        raise TypeError("set operands must have the same type")
    _merge_sources(left.source, right.source)
    if operation == 'is_subset':
        return length(set_binary(left, right, 'difference')) == 0
    if _distinct_supported(left.dtype.element_type):
        function = {'difference': 'array_except', 'intersection': 'array_intersect', 'union': 'array_union'}[operation]
        return _function(function, [left, right], left.dtype)
    # Built-in SQL set kernels merge signed zeros. Compose DataFusion's filter,
    # comparison and concat expressions to retain the existing key equivalence.
    if operation == 'difference':
        result = left._array_filter(lambda item: ~contains(right, item))
    elif operation == 'intersection':
        result = left._array_filter(lambda item: contains(right, item))
    else:
        extra = right._array_filter(lambda item: ~contains(left, item))
        result = _function('array_concat', [left, extra], left.dtype)
    # SQL set kernels propagate missing inputs; the composed path must do so too.
    return if_else(is_missing(left) | is_missing(right), missing(left.dtype), result)


def add(value, item):
    item = _coerce(item, value.dtype.element_type)
    from .expressions import array
    singleton = _cast(array([item]), value.dtype)
    return set_binary(value, singleton, 'union')


def remove(value, item):
    item = _coerce(item, value.dtype.element_type)
    return value._array_filter(lambda member: ~_equal(member, item))


def set_map(value, f):
    return set_(value._array_map(f))


def flatmap(value, f):
    def callback(item):
        result = as_expr(f(item))
        if not isinstance(result.dtype, tset):
            raise TypeError("set.flatmap callback must return a set")
        return to_array(result)
    mapped = value._array_map(callback)
    compact = _function('array_compact', [mapped], mapped.dtype)
    flattened = _function('flatten', [compact], mapped.dtype.element_type)
    return set_(flattened)


def predicate(value, f, first=False):
    if not callable(f):
        raise TypeError("collection predicate requires a callable")
    parameter = uuid.uuid4().hex
    variable = Expression(value.dtype.element_type, value.source, {"op": "lambda_variable", "name": parameter})
    body = as_expr(f(variable))
    if body.dtype != tbool:
        raise TypeError("collection predicate must return bool")
    if first:
        body = ~is_missing(variable) & body
    return Expression(value.dtype.element_type if first else tbool, _merge_sources(value.source, body.source),
                      {"op": "array_predicate", "value": value.to_ir(), "parameter": parameter,
                       "predicate": body.to_ir(), "first": first})


def starmap(value, f):
    if not isinstance(value.dtype.element_type, ttuple):
        raise TypeError("starmap requires tuple elements")
    return set_map(value, lambda item: f(*(item[i] for i in builtins.range(len(item.dtype.types)))))


def unsupported(name):
    def reject(*args, **kwargs):
        raise NotImplementedError(f"DataFusion has no Hail-compatible collection {name} function")
    return reject


def reduce(value, operation, filter_missing=True):
    if type(filter_missing) is not bool:
        raise TypeError("filter_missing must be bool")
    element = value.dtype.element_type
    if operation in ('sum', 'product') and element != tfloat64:
        raise NotImplementedError("DataFusion array_sum/product use float64; exact integer and float32 set reductions are unsupported")
    if operation in ('min', 'max') and element not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError("set reductions require numeric elements")
    result = _function('array_' + operation, [to_array(value)], element)
    if operation in ('sum', 'product'):
        # SQL returns null for empty/all-null collections; Hail returns identity.
        compact = _function('array_compact', [to_array(value)], tarray(element))
        result = if_else(length(compact) == 0, literal(0.0 if operation == 'sum' else 1.0, element), result)
    if not filter_missing:
        result = if_else(predicate(value, lambda item: is_missing(item)), missing(element), result)
    return result


def sorted_(collection, key=None, reverse=False):
    value = as_expr(collection)
    if not isinstance(value.dtype, (tarray, tset)):
        raise TypeError("sorted expects an array or set")
    if type(reverse) is not bool:
        raise TypeError("reverse must be a bool")
    if key is not None:
        raise NotImplementedError("expression-valued sorted keys are not yet implemented")
    from .types import tstr
    if value.dtype.element_type not in (tint32, tint64, tbool, tstr):
        raise NotImplementedError("DataFusion array_sort does not preserve Vail total ordering for this element type")
    if isinstance(value.dtype, tset):
        value = _cast(value, tarray(value.dtype.element_type))
    return _function("array_sort", [value, literal("DESC" if reverse else "ASC"), literal("NULLS LAST")], value.dtype)

