"""UTF-8 strings indexed by Unicode scalar values, deliberately unlike Hail UTF-16."""
import builtins
from collections.abc import Mapping
from .expressions import Expression, _merge_sources, as_expr, cast, if_else, is_missing, literal, missing, case, bind
from .types import tstr, tint32, tint64, tfloat64, tbool, tarray, tset, tdict


_TYPES = {
    'lower': tstr, 'upper': tstr, 'reverse': tstr, 'btrim': tstr,
    'concat': tstr, 'repeat': tstr, 'contains': tbool, 'starts_with': tbool,
    'ends_with': tbool, 'character_length': tint32, 'regexp_count': tint64,
    'regexp_like': tbool, 'strpos': tint32, 'substr': tstr, 'translate': tstr,
    'replace': tstr, 'string_to_array': tarray(tstr), 'array_to_string': tstr,
    'regexp_replace': tstr, 'regex_split': tarray(tstr), 'first_match_in': tarray(tstr),
    'hamming': tint32, 'entropy': tfloat64,
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
    return _call('character_length', _text(value))


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


def reverse(value):
    return _call('reverse', _text(value))


def reversed_(value):
    value = as_expr(value)
    if value.dtype == tstr:
        return reverse(value)
    if isinstance(value.dtype, tarray):
        return value[::-1]
    raise TypeError('reversed requires a string or array')


def _bound(value, size):
    value = as_expr(value)
    if value.dtype != tint32:
        raise TypeError('string slice bounds must be int32')
    value = cast(value, tint64)
    adjusted = if_else(_integer('lt', value, 0), _integer('add', size, value), value)
    return if_else(_integer('lt', adjusted, 0), literal(0, tint64),
                   if_else(_integer('lt', size, adjusted), size, adjusted))


def slice_(value, selection):
    value = _text(value)
    if selection.start is selection.stop is selection.step is None:
        return value
    endpoints = (selection.start, selection.stop, selection.step)
    def build(text, *bounds):
        values = iter(bounds)
        return _slice_bound(text, slice(*(None if bound is None else next(values) for bound in endpoints)))
    return bind(build, value, *(as_expr(bound) for bound in endpoints if bound is not None))


def _slice_bound(value, selection):
    if selection.step is not None:
        from .streams import stream_range
        positions = stream_range(length(value)).to_array()[selection]
        return join(literal(''), positions.map(lambda i: _call('substr', value, cast(i, tint64) + literal(1, tint64), literal(1, tint64))))
    if selection.start is selection.stop is None:
        return value
    size = cast(length(value), tint64)
    start = literal(0, tint64) if selection.start is None else _bound(selection.start, size)
    stop = size if selection.stop is None else _bound(selection.stop, size)
    count = if_else(_integer('lt', stop, start), literal(0, tint64), _integer('sub', stop, start))
    return _call('substr', value, _integer('add', start, 1), count)


def index(value, selection):
    value = _text(value)
    selection = as_expr(selection)
    if selection.dtype != tint32:
        raise TypeError('string index must be int32')
    return bind(_index_bound, value, selection)


def _index_bound(value, selection):
    size = cast(length(value), tint64)
    position = cast(selection, tint64)
    position = if_else(_integer('lt', position, 0), _integer('add', size, position), position)
    result = _call('substr', value, _integer('add', position, 1), literal(1, tint64))
    return case().when(is_missing(value) | is_missing(selection), missing(tstr)).when(
        _integer('le', 0, position) & _integer('lt', position, size), result
    ).or_error('string index out of bounds')


def find(value, sub, start=None, end=None):
    value, sub = _text(value), _text(sub)
    size = cast(length(value), tint64)
    lower = literal(0, tint64) if start is None else _bound(start, size)
    upper = size if end is None else _bound(end, size)
    count = if_else(_integer('lt', upper, lower), literal(0, tint64), _integer('sub', upper, lower))
    part = _call('substr', value, _integer('add', lower, 1), count)
    position = _call('strpos', part, sub)
    found = cast(_integer('add', lower, _integer('sub', position, 1)), tint32)
    result = if_else(_integer('eq', position, 0) | _integer('lt', upper, lower), literal(-1, tint32), found)
    return if_else(is_missing(value) | is_missing(sub), missing(tint32), result)


def matches(value, regex, full_match=False):
    if type(full_match) is not bool:
        raise TypeError('full_match must be a bool')
    pattern = _text(regex)
    if full_match:
        pattern = concat(concat(literal(r'\A(?:'), pattern), literal(r')\z'))
    return _call('regexp_like', _text(value), pattern)


def replace(value, pattern, replacement):
    return _call('regexp_replace', _text(value), _text(pattern), _text(replacement), literal('g'))


def split(value, delim, n=None):
    n = literal(-1, tint32) if n is None else as_expr(n)
    if n.dtype != tint32:
        raise TypeError('split limit must be int32')
    return _call('regex_split', _text(value), _text(delim), n)


def first_match_in(value, regex):
    return _call('first_match_in', _text(value), _text(regex))


def translate(value, mapping):
    value = _text(value)
    if isinstance(mapping, Expression) and mapping.dtype == tdict(tstr, tstr):
        ir = mapping.to_ir()
        if ir.get('op') == 'literal':
            from . import eval
            mapping = eval(mapping)
    if isinstance(mapping, Expression):
        if mapping.dtype != tdict(tstr, tstr):
            raise TypeError('translate requires a dict<str, str>')
        return _translate_dynamic(value, mapping)
    if not isinstance(mapping, Mapping):
        raise TypeError('translate requires a dictionary')
    preserved, removed = [], []
    for key, replacement in mapping.items():
        if not isinstance(key, builtins.str) or len(key) != 1:
            from .utils import FatalError
            raise FatalError('translate mapping keys must be one character; expected one Unicode codepoint')
        if not isinstance(replacement, builtins.str):
            raise TypeError('translate mapping values must be strings')
        if len(replacement) > 1:
            return _translate_dynamic(value, literal(mapping, tdict(tstr, tstr)))
        (preserved if replacement else removed).append((key, replacement))
    ordered = preserved + removed
    return _call('translate', value, literal(''.join(key for key, _ in ordered)),
                 literal(''.join(replacement for _, replacement in ordered)))


def _translate_dynamic(value, mapping):
    from .streams import stream_range
    def translate_bound(text, translations):
        characters = stream_range(length(text)).to_array().map(lambda i: index(text, i))
        result = join(literal(''), characters.map(lambda c: translations.get(c, c)))
        return case().when(is_missing(text) | is_missing(translations), missing(tstr)).when(
            translations.keys().all(lambda key: length(key) == 1), result
        ).or_error('translate mapping keys must be one Unicode codepoint')
    return bind(translate_bound, value, mapping)


def join(value, collection):
    collection = as_expr(collection)
    if not isinstance(collection.dtype, (tarray, tset)):
        raise TypeError('join requires an array or set of strings')
    if collection.dtype.element_type != tstr:
        raise TypeError(f'Expected str collection, {collection.dtype.element_type} found')
    from .collections import to_array
    return _call('array_to_string', to_array(collection), value, literal('null'))


def delimit(collection, delimiter=','):
    collection = as_expr(collection)
    if isinstance(collection.dtype, (tarray, tset)) and collection.dtype.element_type != tstr:
        from .expressions import str_
        collection = collection.map(str_)
    return join(_text(delimiter), collection)


def reverse_complement(value, rna=False):
    if type(rna) is not bool:
        raise TypeError('rna must be a bool')
    value = _text(value)
    result = _call('reverse', value)
    mapping = {'A': 'U' if rna else 'T', 'T': 'A', 'G': 'C', 'C': 'G'}
    if rna:
        mapping['U'] = 'A'
    mapping.update({key.lower(): replacement.lower() for key, replacement in list(mapping.items())})
    return translate(result, mapping)


def hamming(left, right):
    return _call('hamming', _text(left), _text(right))


def entropy(value):
    return _call('entropy', _text(value))


def unsupported(name):
    def reject(*args, **kwargs):
        raise NotImplementedError(f'DataFusion has no Hail-compatible {name} implementation')
    return reject
