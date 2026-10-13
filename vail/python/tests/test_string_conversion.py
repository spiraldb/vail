import json

import pytest
import vail as hl


@pytest.mark.parametrize('value, dtype, expected', [
    (-42, hl.tint32, '-42'),
    (2**40, hl.tint64, '1099511627776'),
    (True, hl.tbool, 'true'),
    (False, hl.tbool, 'false'),
    (1.25, hl.tfloat32, '1.25000e+00'),
    (1.25, hl.tfloat64, '1.25000e+00'),
    (-0.0, hl.tfloat64, '-0.00000e+00'),
    (1e-9, hl.tfloat64, '1.00000e-09'),
    (1e20, hl.tfloat64, '1.00000e+20'),
    (1.234565, hl.tfloat64, '1.23457e+00'),
    (1.234575, hl.tfloat64, '1.23458e+00'),
    (123456.5, hl.tfloat64, '1.23457e+05'),
    (-123456.5, hl.tfloat64, '-1.23457e+05'),
    (float('nan'), hl.tfloat64, 'NaN'),
    (float('inf'), hl.tfloat64, 'Infinity'),
    (float('-inf'), hl.tfloat64, '-Infinity'),
    ([1, None, 3], hl.tarray(hl.tint32), '[1,null,3]'),
    ([1.25, float('nan'), float('inf'), -0.0], hl.tarray(hl.tfloat64), '[1.25,"NaN","Infinity",-0.0]'),
    ([0.1], hl.tarray(hl.tfloat32), '[0.10000000149011612]'),
    ([1e-9, 1e20, 1e7, 1e-3, 1e-4, 1e6, 5e-324], hl.tarray(hl.tfloat64),
     '[1.0E-9,1.0E20,1.0E7,0.001,1.0E-4,1000000.0,4.9E-324]'),
    ((1, 'x', None), hl.ttuple(hl.tint32, hl.tstr, hl.tint32), '[1,"x",null]'),
    (hl.Call((0, 1)), hl.tcall, '0/1'),
    (hl.Call((1, 0), phased=True), hl.tcall, '1|0'),
    (hl.Call(()), hl.tcall, '-'),
    (hl.Call((), phased=True), hl.tcall, '|-'),
    (hl.Call((2,)), hl.tcall, '2'),
    (hl.Locus('1', 7, hl.get_reference('GRCh37')), hl.tlocus('GRCh37'), '1:7'),
    (hl.Interval(1, 3), hl.tinterval(hl.tint32), '[1-3)'),
    (hl.Interval(1.25, 3.5), hl.tinterval(hl.tfloat64), '[1.25-3.5)'),
    (hl.Interval('a', 'z', False, True), hl.tinterval(hl.tstr), '(a-z]'),
    (hl.Interval(None, 3, point_type=hl.tint32), hl.tinterval(hl.tint32), '[null-3)'),
    (hl.Interval(1e-9, 1e20), hl.tinterval(hl.tfloat64), '[1.0E-9-1.0E20)'),
])
def test_string_conversion_hail_formats(value, dtype, expected):
    expression = hl.str(hl.literal(value, dtype))
    assert expression.dtype == hl.tstr
    assert hl.eval(expression) == expected


def test_string_expression_is_identity_and_unescaped():
    expression = hl.literal('a\n"b')
    assert hl.str(expression) is expression
    assert hl.eval(hl.str(expression)) == 'a\n"b'


@pytest.mark.parametrize('dtype', [hl.tint32, hl.tstr, hl.tarray(hl.tint32), hl.tstruct(x=hl.tint32), hl.tlocus('GRCh37')])
def test_string_conversion_preserves_missing(dtype):
    assert hl.eval(hl.str(hl.missing(dtype))) is None


def test_string_struct_schema_order_and_nested_special_types():
    expression = hl.struct(
        z=5, a='a\n"b', n=hl.missing(hl.tint32),
        c=hl.call(0, 1), l=hl.locus('1', 7), i=hl.interval(1, 3),
    )
    assert hl.eval(hl.str(expression)) == (
        '{"z":5,"a":"a\\n\\\"b","n":null,"c":"0/1",'
        '"l":{"contig":"1","position":7},'
        '"i":{"start":1,"end":3,"includeStart":true,"includeEnd":false}}'
    )


def test_string_sets_and_dictionaries_export_logical_values():
    assert sorted(json.loads(hl.eval(hl.str(hl.literal({3, 1, 2}))))) == [1, 2, 3]
    entries = json.loads(hl.eval(hl.str(hl.literal({2: 'b', 1: 'a'}, hl.tdict(hl.tint32, hl.tstr)))))
    assert sorted(entries, key=lambda entry: entry['key']) == [{'key': 1, 'value': 'a'}, {'key': 2, 'value': 'b'}]


def test_string_ndarray_top_level_and_nested_format():
    import vail.nd as nd

    expression = nd.array([[1, 2], [3, 4]], hl.tint32)
    assert hl.eval(hl.str(expression)) == 'ndarray{shape=(2, 2), data=[[1, 2], [3, 4]]}'
    assert hl.eval(hl.str(hl.struct(n=expression))) == '{"n":{"shape":[2,2],"data":[1,2,3,4]}}'


def test_string_conversion_in_lazy_table_expressions_and_globals():
    table = hl.range_table(3).annotate_globals(suffix='!')
    result = table.annotate(text=hl.str(table.idx), suffix_text=hl.str(table.suffix), nested=hl.str(hl.struct(idx=table.idx)))
    assert [(row.text, row.suffix_text, row.nested) for row in result.collect()] == [
        ('0', '!', '{"idx":0}'), ('1', '!', '{"idx":1}'), ('2', '!', '{"idx":2}'),
    ]


@pytest.mark.parametrize('point_type, start, end, expected', [
    (hl.tarray(hl.tint32), [1, 2], [3, 4], '[[1,2]-[3,4])'),
    (hl.tstruct(x=hl.tint32, y=hl.tstr), hl.Struct(x=1, y='a'), hl.Struct(x=2, y='b'),
     '[{x: 1, y: a}-{x: 2, y: b})'),
    (hl.ttuple(hl.tint32, hl.tstr), (1, 'a'), (2, 'b'), '[(1, a)-(2, b))'),
    (hl.tcall, hl.Call((0, 1)), hl.Call((1, 1)), '[12-20)'),
    (hl.tcall, hl.Call((30000, 30000)), hl.Call((30000, 30000)), '[-694607292--694607292)'),
    (hl.tinterval(hl.tint32), hl.Interval(1, 2), hl.Interval(3, 4), '[[1-2)-[3-4))'),
])
def test_string_interval_complex_point_types(point_type, start, end, expected):
    expression = hl.literal(hl.Interval(start, end, point_type=point_type))
    assert hl.eval(hl.str(expression)) == expected


def test_interval_string_invalid_packed_call_errors_without_overflow():
    with pytest.raises(ValueError, match='allele representation'):
        hl.Call((2**31 - 1, 2**31 - 1))
    source = hl.range_table(1)
    with pytest.raises((ValueError, RuntimeError), match='allele representation'):
        source.annotate(call=hl.call(hl.int32(source.idx + 2**31 - 1), source.idx)).collect()


def test_string_export_does_not_shadow_internal_python_type_checks():
    table = hl.Table.parallelize([{'x': 1}], key='x')
    assert table.collect() == [hl.Struct(x=1)]
    incompatible = hl.Table.parallelize([{'x': 'one'}], key='x')
    with pytest.raises(ValueError, match='key'):
        table.join(incompatible)
