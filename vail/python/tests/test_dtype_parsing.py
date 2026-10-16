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


@pytest.mark.parametrize('text,name', [
    ('struct{`é漢😀`: int32}', 'é漢😀'),
    (r'struct{`é\u6f22\U0001f600`: int32}', 'é漢😀'),
    (r'struct{`é\`\\\n\t`: int32}', 'é`\\\n\t'),
    (r'struct{`é\x41\101`: int32}', 'éAA'),
])
def test_quoted_identifiers_preserve_unicode_and_decode_escapes(text, name):
    parsed = hl.dtype(text)
    assert list(parsed) == [name]
    assert parsed[name] == hl.tint32
    assert hl.eval(hl.literal({name: 7}, parsed)) == hl.Struct(**{name: 7})


@pytest.mark.parametrize('name', ['', 'a b', 'é', '℘', 'é漢😀', 'a`b', 'a\\b', 'a\n\t\0b', '"quoted"'])
def test_nested_type_text_roundtrip(name):
    dtype = hl.tstruct(**{name: hl.ttuple(
        hl.tdict(hl.tstr, hl.tndarray(hl.tfloat64, 2)),
        hl.tarray(hl.tinterval(hl.tint32)))})
    assert hl.dtype(str(dtype)) == dtype


@pytest.mark.parametrize('text', [r'struct{`bad\xZ0`: int32}',
                                  r'struct{`bad\u12`: int32}',
                                  r'struct{`bad\U99999999`: int32}'])
def test_malformed_identifier_escapes(text):
    with pytest.raises(ValueError, match='invalid escaped identifier'):
        hl.dtype(text)


def test_duplicate_arrow_struct_names_rejected():
    import pyarrow as pa
    nested = pa.struct([('x', pa.int32()), ('x', pa.string())])
    raw = pa.table({'value': pa.array([], type=nested)})
    with pytest.raises((TypeError, ValueError), match='duplicate'):
        hl.Table.parallelize(raw)


def test_recursive_numeric_array_cast_and_invalid_logical_casts():
    from vail.expressions import cast
    assert hl.eval(cast([[1, None], [2]], hl.tarray(hl.tarray(hl.tfloat64)))) == [[1., None], [2.]]
    for value, target in [(True, hl.tarray(hl.tint32)), (1, hl.tlocus()),
                          ([1], hl.tset(hl.tint32)), (hl.Call([0, 1]), hl.tint32)]:
        with pytest.raises(TypeError, match='cannot cast'):
            cast(value, target)


@pytest.mark.parametrize('k', [-(2**31), -1, 0, 7, 8, 65535])
def test_quantile_parameter_preserves_supported_signed_range(k):
    table = hl.utils.range_table(5, 2)
    assert table.aggregate(hl.agg.approx_quantiles(table.idx, [0., 1.], k=k)) == [0, 4]
    scanned = table.annotate(median=hl.scan.approx_median(table.idx, k=k))
    values = scanned.median.collect()
    assert values[0] is None
    assert all(value is None or 0 <= value <= 4 for value in values)


@pytest.mark.parametrize('k', [-(2**31)-1, 65536])
def test_quantile_parameter_rejects_unsupported_range(k):
    table = hl.utils.range_table(5)
    with pytest.raises((ValueError, RuntimeError), match='k|quantile'):
        table.aggregate(hl.agg.approx_median(table.idx, k=k))


def test_inbreeding_result_has_integer_observed_homozygotes():
    table = hl.Table.parallelize([{'gt': hl.Call([0, 0])}, {'gt': hl.Call([0, 1])}])
    result = table.aggregate(hl.agg.inbreeding(table.gt, .5))
    assert type(result.observed_homs) is int
    assert result.observed_homs == 1
    scanned = table.annotate(prefix=hl.scan.inbreeding(table.gt, .5))
    assert scanned.prefix.observed_homs.dtype == hl.tint64
    assert [r.prefix.observed_homs for r in scanned.collect()] == [0, 1]


@pytest.mark.parametrize('weight', [None, 2.0])
def test_public_scalar_regression_predictor_and_weight(weight):
    table = hl.utils.range_table(5, 2)
    result = table.aggregate(hl.agg.linreg(table.idx, 1., weight=weight))
    assert result.beta == pytest.approx([2.])
    assert result.n == 5
    scanned = table.annotate(prefix=hl.scan.linreg(table.idx, 1., weight=weight))
    assert scanned.prefix.n.collect() == [0, 1, 2, 3, 4]
