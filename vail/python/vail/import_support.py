"""Expression prerequisites shared by public import/export workflows.

Semantics follow pinned Hail e10cc355, expr/functions.py and typed_expressions.py.
"""
from .expressions import (Expression, as_expr, bind, cast, if_else, is_missing,
                          literal, missing, struct, array, or_missing, is_defined)
from .types import tarray, tndarray, tint32, tint64, tfloat32, tfloat64, tstr, tcall, type_to_wire


def parse_call(s):
    """Parse Hail's zero-, haploid-, or diploid call string notation."""
    value = as_expr(s)
    if value.dtype != tstr:
        raise TypeError('parse_call requires a string expression')
    def parse(text):
        phased = text.contains('|')
        valid = text.matches(r'\s*(?:-|\|-|-?[0-9]+|\|\s*-?[0-9]+|-?[0-9]+\s*[/|]\s*-?[0-9]+)\s*', full_match=True)
        compact = text.replace(r'\s+', '')
        alleles = if_else((compact == '-') | (compact == '|-'), literal([], tarray(tint32)),
                         compact.replace(r'^\|', '').split(r'[/|]').map(lambda a: cast(a, tint32)))
        packed = struct(__vail_call_alleles=alleles, __vail_call_phased=phased)
        result = Expression(tcall, packed.source, {'op': 'cast', 'dtype': 'call', 'value': packed.to_ir()})
        error = Expression(tcall, None, {'op': 'die', 'message': literal('invalid call string').to_ir(),
                                        'dtype': type_to_wire(tcall)})
        return if_else(valid, result, error)
    return bind(parse, value)


def abs(x):
    """Return the absolute value, preserving the numeric element type."""
    value = as_expr(x)
    if isinstance(value.dtype, tarray):
        if value.dtype.element_type not in (tint32, tint64, tfloat32, tfloat64):
            raise TypeError('abs requires numeric array elements')
        return value.map(abs)
    if isinstance(value.dtype, tndarray):
        data = Expression(tarray(value.dtype.element_type), value.source,
                          {'op': 'get_field', 'value': value.to_ir(), 'name': '__vail_ndarray_data'})
        shape = Expression(tarray(tint64), value.source,
                           {'op': 'get_field', 'value': value.to_ir(), 'name': '__vail_ndarray_shape'})
        flat = Expression(tndarray(value.dtype.element_type, 1), value.source,
                          {'op': 'nested_function', 'function': '__vail_ndarray_from_array',
                           'args': [data.map(abs).to_ir()],
                           'dtype': type_to_wire(tndarray(value.dtype.element_type, 1))})
        return or_missing(is_defined(value), flat.reshape(tuple(shape[i] for i in range(value.dtype.ndim))))
    if value.dtype not in (tint32, tint64, tfloat32, tfloat64):
        raise TypeError('abs requires numeric values')
    def scalar(item):
        result = Expression(item.dtype, item.source,
                            {'op': 'nested_function', 'function': 'abs', 'args': [item.to_ir()],
                             'dtype': type_to_wire(item.dtype)})
        # Hail's signed integer abs wraps at MIN_VALUE; DataFusion checks it.
        if item.dtype in (tint32, tint64):
            minimum = literal(-(1 << (31 if item.dtype == tint32 else 63)), item.dtype)
            return if_else(item == minimum, item, result)
        return result
    return bind(scalar, value)


def triangle(n):
    """Return n * (n + 1) / 2 using Hail's signed int32 arithmetic."""
    value = as_expr(n)
    if value.dtype != tint32:
        raise TypeError('triangle requires int32')
    return bind(lambda item: (item * (item + 1)) // 2, value)
