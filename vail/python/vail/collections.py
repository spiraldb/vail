"""Collection APIs composed from DataFusion functions and typed expressions."""

from .expressions import IR_NODE_TYPES
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
    if name.startswith("__vail_list_reduce_"):
        from ._native import supports_list_reducer
        function = name.removeprefix("__vail_list_reduce_")
        if not supports_list_reducer(function):
            raise NotImplementedError(f"per-list {function} reducer is not supported")
    source = None
    for arg in args:
        source = _merge_sources(source, arg.source)
    return Expression(dtype, source, {"op": "nested_function", "function": name,
                                     "args": [arg.to_ir() for arg in args], "dtype": type_to_wire(dtype)})


def _native_sort(value):
    return _function("array_sort", [value, literal("ASC"), literal("NULLS LAST")], value.dtype)


def _contains_unordered_or_ndarray(dtype, *, include_ndarray=True):
    from .types import tinterval, tndarray
    if isinstance(dtype, (tset, tdict)):
        return True
    if isinstance(dtype, tndarray):
        return include_ndarray
    if isinstance(dtype, (tarray, tinterval)):
        return _contains_unordered_or_ndarray(
            dtype.element_type if isinstance(dtype, tarray) else dtype.point_type,
            include_ndarray=include_ndarray,
        )
    if isinstance(dtype, tstruct):
        return builtins.any(
            _contains_unordered_or_ndarray(ty, include_ndarray=include_ndarray)
            for _, ty in dtype.items()
        )
    if isinstance(dtype, ttuple):
        return builtins.any(
            _contains_unordered_or_ndarray(ty, include_ndarray=include_ndarray)
            for ty in dtype.types
        )
    return False


def _native_equality_value(value):
    """Replace unordered collection levels with values DataFusion can compare."""
    dtype = value.dtype
    from .types import tinterval, tndarray
    if isinstance(dtype, tndarray):
        raise NotImplementedError("ndarray equality is unavailable in DataFusion")
    if isinstance(dtype, tset):
        array = _cast(value, tarray(dtype.element_type))
        mapped = array._array_map(_native_equality_value)
        result = _native_sort(mapped)
    elif isinstance(dtype, tdict):
        _ensure_key_supported(dtype.key_type)
        entries = value._array_map(
            lambda entry: struct(key=entry.key, value=_native_equality_value(entry.value))
        )
        result = _native_sort(entries)
    elif isinstance(dtype, tarray):
        result = value._array_map(_native_equality_value)
    elif isinstance(dtype, tstruct):
        result = struct(**{name: _native_equality_value(value[name]) for name in dtype})
    elif isinstance(dtype, ttuple):
        from .expressions import tuple_
        result = tuple_(_native_equality_value(value[i]) for i in builtins.range(len(dtype.types)))
    elif isinstance(dtype, tinterval):
        result = value
    else:
        return value
    return if_else(is_missing(value), missing(result.dtype), result)


def _distinct_supported(dtype):
    # Use DataFusion's native distinct semantics for supported scalar/struct
    # values. Nested unordered collections need logical equality support that
    # array_distinct does not provide.
    if isinstance(dtype, (tset, tdict)):
        return False
    if isinstance(dtype, tarray):
        return _distinct_supported(dtype.element_type)
    if isinstance(dtype, tstruct):
        return builtins.all(_distinct_supported(ty) for _, ty in dtype.items())
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


def empty_array(element_type):
    """Construct a defined empty array with the specified element type."""
    from .types import dtype

    if isinstance(element_type, builtins.str):
        element_type = dtype(element_type)
    return literal([], tarray(element_type))


def dict_(collection):
    if isinstance(collection, IR_NODE_TYPES):
        from .expressions import array
        key_type = array(list(collection.keys())).dtype.element_type
        value_type = array(list(collection.values())).dtype.element_type
        if not builtins.any(isinstance(item, Expression) for pair in collection.items() for item in pair):
            _ensure_key_supported(key_type)
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


def _ensure_key_supported(dtype):
    from .types import tinterval, tndarray
    if isinstance(dtype, (tset, tdict, tndarray)):
        raise NotImplementedError("Set/Dictionary/ndarray ordering and keying are unsupported")
    if isinstance(dtype, tarray):
        _ensure_key_supported(dtype.element_type)
    elif isinstance(dtype, tstruct):
        for _, ty in dtype.items():
            _ensure_key_supported(ty)
    elif isinstance(dtype, ttuple):
        for item in dtype.types:
            _ensure_key_supported(item)
    elif isinstance(dtype, tinterval):
        _ensure_key_supported(dtype.point_type)


def _direct_literal_array(value):
    """Retain literal/computed duplicate precedence using original-node DAG analysis."""
    from .expressions import _native_expression
    return _native_expression(value.to_ir()).literal_dict_precedence()


def _dict_from_entries(entries, key_type, value_type, *, last_wins=False):
    """Deduplicate keys with DataFusion's native equality while preserving entries."""
    _ensure_key_supported(key_type)
    function = "__vail_dict_last" if last_wins else "__vail_dict_first"
    unique_entries = _function(function, [entries], entries.dtype)
    return _cast(unique_entries, tdict(key_type, value_type))


def empty_dict(key_type, value_type):
    _ensure_key_supported(key_type)
    return literal({}, tdict(key_type, value_type))


def to_array(value):
    if isinstance(value.dtype, tarray):
        return value
    if isinstance(value.dtype, tset):
        _ensure_key_supported(value.dtype.element_type)
        return _native_sort(_cast(value, tarray(value.dtype.element_type)))
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
    # Collection membership and grouping use key equality, where two missing
    # values are equal. Keep ordinary expression equality null-propagating.
    return if_else(
        is_missing(left),
        is_missing(right),
        if_else(is_missing(right), literal(False, tbool), left == right),
    )


def _lookup(value, key):
    return _function('__vail_dict_lookup', [value, key],
                     tstruct(found=tbool, value=value.dtype.value_type))


def contains(value, key):
    if isinstance(value.dtype, tdict):
        key = _coerce(key, value.dtype.key_type)
        return _lookup(value, key).found
    if isinstance(value.dtype, tset):
        key = _coerce(key, value.dtype.element_type)
        canonical_key = _native_equality_value(key)
        pairs = value._array_map(lambda item: struct(
            key=_native_equality_value(item), value=True))
        dictionary = _cast(pairs, tdict(canonical_key.dtype, tbool))
        return _lookup(dictionary, canonical_key).found
    raise TypeError("contains requires a set or dictionary")


def get(value, key, default=None):
    key = _coerce(key, value.dtype.key_type)
    default = _coerce(default, value.dtype.value_type)
    result = _lookup(value, key)
    return if_else(result.found, result.value, default)


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
    # Canonical values make nested unordered levels ordinary Arrow keys. Group
    # both inputs once rather than probing the opposite collection per member.
    left_pairs = left._array_map(
        lambda item: struct(key=_native_equality_value(item),
                            value=struct(item=item, left=True)))
    right_pairs = right._array_map(
        lambda item: struct(key=_native_equality_value(item),
                            value=struct(item=item, left=False)))
    pairs = _function('array_concat', [left_pairs, right_pairs], left_pairs.dtype)
    entry_type = pairs.dtype.element_type
    grouped_type = tarray(tstruct(key=entry_type['key'],
                                 value=tarray(entry_type['value'])))
    groups = _function('__vail_collection_group', [pairs], grouped_type)
    if operation == 'difference':
        groups = groups._array_filter(
            lambda group: group.value.any(lambda entry: entry.left)
            & ~group.value.any(lambda entry: ~entry.left))
    elif operation == 'intersection':
        groups = groups._array_filter(
            lambda group: group.value.any(lambda entry: entry.left)
            & group.value.any(lambda entry: ~entry.left))
    result = groups._array_map(lambda group: group.value[0].item)
    result = _cast(result, left.dtype)
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
    from .randomness import index_callback, contains
    indexed = contains(body.to_ir())
    value, body = index_callback(value, parameter, body)
    result = Expression(value.dtype.element_type if first else tbool, _merge_sources(value.source, body.source),
                        {"op": "array_predicate", "value": value.to_ir(), "parameter": parameter,
                         "predicate": body.to_ir(), "first": first})
    return result[1] if first and indexed else result


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
    # Extrema are independent of iteration order. Floating sum/product retain
    # their established sorted input order and SQL reduction arithmetic.
    view = _cast(value, tarray(element))
    if operation in ('min', 'max'):
        result = _function('array_' + operation, [view], element)
    else:
        identity = literal(0.0 if operation == 'sum' else 1.0, element)
        from .expressions import bind
        def reduce_sorted(ordered):
            reduced = _function('array_' + operation, [ordered], element)
            defined = bind(lambda result: if_else(is_missing(result), identity, result), reduced)
            return if_else(is_missing(ordered), missing(element), defined)
        result = bind(reduce_sorted, to_array(value))
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
    from .types import tstr, tfloat32, tfloat64, _ensure_orderable
    _ensure_orderable(value.dtype.element_type)
    if value.dtype.element_type not in (tint32, tint64, tfloat32, tfloat64, tbool, tstr):
        raise NotImplementedError("array_sort is not yet enabled for this element type")
    if isinstance(value.dtype, tset):
        value = _cast(value, tarray(value.dtype.element_type))
    return _function("array_sort", [value, literal("DESC" if reverse else "ASC"), literal("NULLS LAST")], value.dtype)


def append(collection, value):
    from .expressions import array, cast
    from .types import unify_types
    collection, value = as_expr(collection), as_expr(value)
    if not isinstance(collection.dtype, tarray):
        raise TypeError("append requires an array expression")
    common = unify_types(collection.dtype.element_type, value.dtype)
    if common is None:
        raise TypeError("array append element types are incompatible")
    if collection.dtype.element_type != common:
        collection = collection.map(lambda item: cast(item, common))
    value = _coerce(value, common)
    result = _function("array_concat", [collection, array([value])], tarray(common))
    return if_else(is_missing(collection), missing(tarray(common)), result)
