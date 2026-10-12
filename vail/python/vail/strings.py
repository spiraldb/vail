"""String expressions lowered exclusively to DataFusion's built-in kernels."""
import builtins
from collections.abc import Mapping
from .expressions import Expression, _merge_sources, as_expr, cast, if_else, is_missing, literal, missing, array
from .types import tstr, tint32, tint64, tbool, tarray, tset, tdict


_TYPES = {
    'lower': tstr, 'upper': tstr, 'reverse': tstr, 'btrim': tstr,
    'concat': tstr, 'repeat': tstr, 'contains': tbool, 'starts_with': tbool,
    'ends_with': tbool, 'character_length': tint32, 'regexp_count': tint64,
    'regexp_like': tbool, 'strpos': tint32, 'substr': tstr, 'translate': tstr,
    'replace': tstr, 'string_to_array': tarray(tstr), 'array_to_string': tstr,
}


def _call(function, *args):
    source = None
    for arg in args:
        source = _merge_sources(source, arg.source)
    return Expression(_TYPES[function], source,
                      {'op': 'string_function', 'function': function, 'args': [arg.to_ir() for arg in args]})


def _text(value):
    value = as_expr(value)
    if value.dtype != tstr:
        raise TypeError('string operation requires str arguments')
    return value


def _integer(operator, left, right):
    left, right = cast(as_expr(left), tint64), cast(as_expr(right), tint64)
    return Expression(tbool if operator in ('eq', 'lt', 'le') else tint64,
                      _merge_sources(left.source, right.source),
                      {'op': 'integer_binary', 'operator': operator, 'left': left.to_ir(), 'right': right.to_ir()})


def length(value):
    # Count UTF-16 units, including the extra unit for each supplementary scalar.
    points = _call('character_length', value)
    extra = _call('regexp_count', value, literal(r'[\x{10000}-\x{10FFFF}]'))
    return cast(_integer('add', points, extra), tint32)


def concat(left, right):
    left, right = _text(left), _text(right)
    return if_else(is_missing(left) | is_missing(right), missing(tstr), _call('concat', left, right))


def repeat(value, count):
    count = as_expr(count)
    if count.dtype != tint32:
        raise TypeError('string repeat count must be int32')
    # Hail maps over an empty range for non-positive counts, even for missing s.
    return if_else(_integer('le', count, 0), '', _call('repeat', value, cast(count, tint64)))


def strip(value):
    # Java String.trim removes U+0000..U+0020, not general Unicode whitespace.
    return _call('btrim', value, literal(''.join(builtins.chr(i) for i in builtins.range(33))))


def _bmp_literal(value):
    ir = value.to_ir()
    if ir.get('op') == 'literal' and ir.get('value') is None:
        return None
    if ir.get('op') != 'literal' or not isinstance(ir.get('value'), builtins.str):
        raise NotImplementedError('DataFusion uses Unicode scalar positions; general Hail UTF-16 string slicing/indexing/reversal is unsupported')
    if builtins.any(ord(char) > 0xFFFF for char in ir['value']):
        raise NotImplementedError('DataFusion cannot reproduce Hail UTF-16 slicing/indexing/reversal of supplementary characters')
    return ir['value']


def reverse(value):
    _bmp_literal(value)
    return _call('reverse', value)


def _bound(value, size):
    value = as_expr(value)
    if value.dtype != tint32:
        raise TypeError('string slice bounds must be int32')
    value = cast(value, tint64)
    adjusted = if_else(_integer('lt', value, 0), _integer('add', size, value), value)
    return if_else(_integer('lt', adjusted, 0), literal(0, tint64),
                   if_else(_integer('lt', size, adjusted), size, adjusted))


def slice_(value, selection):
    if selection.step is not None:
        raise NotImplementedError('Hail string slicing does not support a step')
    if selection.start is selection.stop is None:
        return value
    _bmp_literal(value)
    size = cast(length(value), tint64)
    start = literal(0, tint64) if selection.start is None else _bound(selection.start, size)
    stop = size if selection.stop is None else _bound(selection.stop, size)
    count = if_else(_integer('lt', stop, start), literal(0, tint64), _integer('sub', stop, start))
    return _call('substr', value, _integer('add', start, 1), count)


def index(value, selection):
    text = _bmp_literal(value)
    selection = as_expr(selection)
    if selection.dtype != tint32:
        raise TypeError('string index must be int32')
    ir = selection.to_ir()
    if ir.get('op') != 'literal' or not isinstance(ir.get('value'), builtins.int):
        raise NotImplementedError('DataFusion substring does not raise Hail index-out-of-bounds errors for dynamic indices')
    if text is None:
        return missing(tstr)
    position = ir['value']
    if not -len(text) <= position < len(text):
        raise IndexError('string index out of bounds')
    position = position if position >= 0 else len(text) + position
    return _call('substr', value, literal(position + 1, tint64), literal(1, tint64))


def find(value, sub, start=None, end=None):
    if start is not None or end is not None:
        raise NotImplementedError('bounded string find requires general Hail UTF-16 slicing support')
    position = _call('strpos', value, _text(sub))
    prefix = _call('substr', value, literal(1, tint64), _integer('sub', position, 1))
    return if_else(_integer('eq', position, 0), literal(-1, tint32), length(prefix))


def _literal_regex(pattern):
    pattern = _text(pattern)
    ir = pattern.to_ir()
    if ir.get('op') != 'literal' or not isinstance(ir.get('value'), builtins.str):
        raise NotImplementedError('expression-valued Java regular expressions are unsupported by DataFusion')
    text = ir['value']
    if builtins.any(char in r'.^$*+?{}[]\|()' for char in text):
        raise NotImplementedError('Java regular-expression semantics differ from DataFusion; only literal patterns are supported')
    return text


def matches(value, regex, full_match=False):
    if type(full_match) is not bool:
        raise TypeError('full_match must be a bool')
    pattern = _literal_regex(regex)
    if full_match:
        pattern = r'\A' + pattern + r'\z'
    return _call('regexp_like', value, literal(pattern))


def replace(value, pattern, replacement):
    pattern = _literal_regex(pattern)
    replacement = _text(replacement)
    ir = replacement.to_ir()
    if ir.get('op') != 'literal' or not isinstance(ir.get('value'), builtins.str) or builtins.any(char in '$\\' for char in ir['value']):
        raise NotImplementedError('Java replacement templates are unsupported; replacement must be a plain literal')
    if not pattern:
        raise NotImplementedError('empty-pattern replacement needs Java zero-width match semantics')
    return _call('replace', value, literal(pattern), replacement)


def split(value, delim, n=None):
    if n is not None:
        raise NotImplementedError('DataFusion has no Java regex split-with-limit function')
    delim = _literal_regex(delim)
    if not delim:
        raise NotImplementedError('empty-pattern splitting needs Java zero-width match semantics')
    result = _call('string_to_array', value, literal(delim))
    empty = _call('regexp_like', value, literal(r'\A\z'))
    return if_else(empty, array(['']), result)


def translate(value, mapping):
    if isinstance(mapping, Expression) and mapping.dtype == tdict(tstr, tstr):
        ir = mapping.to_ir()
        if ir.get('op') == 'literal' and isinstance(ir.get('value'), list):
            mapping = {entry['key']: entry['value'] for entry in ir['value']}
    if not isinstance(mapping, Mapping):
        raise NotImplementedError('DataFusion translate does not accept expression-valued dictionaries')
    preserved, removed = [], []
    for key, replacement in mapping.items():
        if not isinstance(key, builtins.str) or len(key) != 1 or ord(key) > 0xFFFF:
            raise ValueError('translate mapping keys must be one UTF-16 character')
        if not isinstance(replacement, builtins.str) or len(replacement) > 1 or (replacement and ord(replacement) > 0xFFFF):
            raise NotImplementedError('DataFusion translate cannot replace a character with an arbitrary string')
        (preserved if replacement else removed).append((key, replacement))
    ordered = preserved + removed
    return _call('translate', value, literal(''.join(key for key, _ in ordered)),
                 literal(''.join(replacement for _, replacement in ordered)))


def join(value, collection):
    collection = as_expr(collection)
    if not isinstance(collection.dtype, (tarray, tset)) or collection.dtype.element_type != tstr:
        raise TypeError('join requires an array or set of strings')
    from .collections import to_array
    return _call('array_to_string', to_array(collection), value, literal('null'))


def delimit(collection, delimiter=','):
    collection = as_expr(collection)
    if isinstance(collection.dtype, (tarray, tset)) and collection.dtype.element_type != tstr:
        raise NotImplementedError('DataFusion casts do not provide Hail formatting for non-string delimit')
    return join(_text(delimiter), collection)


def reverse_complement(value, rna=False):
    if type(rna) is not bool:
        raise TypeError('rna must be a bool')
    value = _text(value)
    _bmp_literal(value)
    result = _call('reverse', value)
    mapping = {'A': 'U' if rna else 'T', 'T': 'A', 'G': 'C', 'C': 'G'}
    if rna:
        mapping['U'] = 'A'
    mapping.update({key.lower(): replacement.lower() for key, replacement in list(mapping.items())})
    return translate(result, mapping)


def unsupported(name):
    def reject(*args, **kwargs):
        raise NotImplementedError(f'DataFusion has no Hail-compatible {name} implementation')
    return reject
