import pytest

import vail as hl


@pytest.mark.parametrize('text, expected', [
    ('tint', hl.tint32), ('float', hl.tfloat64), ('tfloat32', hl.tfloat32),
    ('tbool', hl.tbool), ('tstr', hl.tstr), ('tcall', hl.tcall),
    (' array < str > ', hl.tarray(hl.tstr)),
    ('tset < tarray<int64> >', hl.tset(hl.tarray(hl.tint64))),
    ('dict<str, tuple(int32, array<float64>)>', hl.tdict(hl.tstr, hl.ttuple(hl.tint32, hl.tarray(hl.tfloat64)))),
    ('tuple()', hl.ttuple()), ('tstruct {}', hl.tstruct()),
    ('struct{a: tuple(int32, str), `x,:>{`: set<str>}', hl.tstruct(a=hl.ttuple(hl.tint32, hl.tstr), **{'x,:>{': hl.tset(hl.tstr)})),
    (r'struct{`a\`b`: int32, `c\n`: str}', hl.tstruct(**{'a`b': hl.tint32, 'c\n': hl.tstr})),
    ('interval<locus<GRCh37>>', hl.tinterval(hl.tlocus('GRCh37'))),
    ('tndarray < float64, 2 >', hl.tndarray(hl.tfloat64, 2)),
])
def test_concrete_type_grammar(text, expected):
    assert hl.dtype(text) == expected
    assert hl.dtype(expected) is expected


@pytest.mark.parametrize('text', [
    '', 'array<str', 'array<str>>', 'array<str,int32>', 'dict<str>',
    'tuple(int32,)', 'struct{a int32}', 'struct{a:int32,a:str}',
    'struct{`unterminated: str}', 'ndarray<float64,-1>', 'ndarray<float64,2x>',
    'int32 trailing', 'mystery',
])
def test_malformed_type_strings(text):
    with pytest.raises(ValueError, match='invalid type string'):
        hl.dtype(text)


def test_literal_and_missing_accept_nested_type_strings():
    expr = hl.literal(['x', None], dtype='array<str>')
    assert expr.dtype == hl.tarray(hl.tstr)
    assert hl.eval(expr) == ['x', None]
    value = hl.literal({'x': (1, ['a'])}, dtype='struct{x: tuple(int64, array<str>)}')
    assert hl.eval(value) == hl.Struct(x=(1, ['a']))
    assert hl.eval(hl.missing('array<str>')) is None
    assert hl.literal(['x'], dtype=hl.tarray(hl.tstr)).dtype == expr.dtype


def test_non_string_non_type_rejected():
    with pytest.raises(TypeError, match='dtype expects'):
        hl.dtype(42)
    with pytest.raises(TypeError, match='literal dtype'):
        hl.literal(1, dtype=42)
