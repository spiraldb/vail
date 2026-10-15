from vail.expressions import _native_expression, _native_projection
"""Typed array lambdas execute in DataFusion, including lexical captures."""
import pytest
import vail as hl


def test_missing_empty_and_missing_elements():
    t = hl.Table.parallelize([{'a': None}, {'a': []}, {'a': [None, 2, 3]}],
                             schema=hl.tstruct(a=hl.tarray(hl.tint32)))
    assert t.a.map(lambda x: x * 2).collect() == [None, [], [None, 4, 6]]
    assert t.a.map(lambda x: hl.if_else(hl.is_missing(x), 7, x)).collect() == [None, [], [7, 2, 3]]
    assert t.a.map(lambda x: 9).collect() == [None, [], [9, 9, 9]]


def test_nested_maps_capture_rows_globals_and_outer_variables():
    external = hl.range_table(0).annotate_globals(offset=10).index_globals()
    t = hl.range_table(3).annotate_globals(g=100)
    expression = hl.array([1, 2]).map(lambda x: hl.array([3, 4]).map(
        lambda y: x + y + t.idx + t.g + external.offset))
    result = t.annotate(a=expression)
    assert [r.a for r in result.collect()] == [
        [[114+i, 115+i], [115+i, 116+i]] for i in range(3)]
    assert 'array_transform' in result._native.physical_plan()


def test_callback_constructs_ir_once_and_can_change_element_type():
    calls = []
    def body(x):
        calls.append(x)
        return hl.struct(value=hl.float64(x), pair=[x, x + 1])
    expr = hl.array([1, 2]).map(body)
    assert len(calls) == 1
    assert hl.eval(expr) == [hl.Struct(value=1.0, pair=[1, 2]), hl.Struct(value=2.0, pair=[2, 3])]
    assert hl.eval(hl.array([1, 2]).map(lambda x: hl.struct())) == [hl.Struct(), hl.Struct()]
    assert len(calls) == 1
    with pytest.raises(ValueError, match='unbound lambda'):
        hl.eval(calls[0] + 1)


def test_empty_and_missing_arrays_do_not_evaluate_body():
    for array in [hl.literal([], hl.tarray(hl.tint32)), hl.missing(hl.tarray(hl.tint32))]:
        assert hl.eval(array.map(lambda x: hl.array([1])[10])) == hl.eval(array)
    bad = hl.range_table(2).annotate(a=hl.array([1]).map(lambda x: hl.array([1])[10]))
    assert bad.head(0).collect() == []
    with pytest.raises(RuntimeError, match='out of bounds'):
        bad.collect()


def test_map_rejects_noncallable_and_mixed_row_sources():
    with pytest.raises(TypeError, match='callable'):
        hl.array([1]).map(3)
    a, b = hl.range_table(2), hl.range_table(2)
    with pytest.raises(ValueError):
        hl.array([a.idx]).map(lambda x: x + b.idx)
    with pytest.raises(AttributeError):
        hl.literal(1).map(lambda x: x)


def test_variable_length_nested_arrays_and_round_trip(tmp_path):
    t = hl.Table.parallelize(
        [{'id': 10, 'a': [[1, None], [], None]},
         {'id': 20, 'a': None}, {'id': 30, 'a': []},
         {'id': 40, 'a': [[2], [3, 4]]}],
        schema=hl.tstruct(id=hl.tint32, a=hl.tarray(hl.tarray(hl.tint32))), key='id')
    mapped = t.annotate(b=t.a.map(lambda xs: xs.map(lambda x: x + t.id)))
    expected = [[[11, None], [], None], None, [], [[42], [43, 44]]]
    assert mapped.b.collect() == expected
    path = str(tmp_path / 'mapped')
    mapped.write(path)
    restored = hl.read_table(path)
    assert restored.b.collect() == expected
    assert restored.a.map(lambda xs: xs.map(lambda x: x + restored.id)).collect() == expected


def test_native_ir_validates_lambda_scope_and_input_type():
    import json
    t = hl.range_table(1)
    variable = {'op': 'lambda_variable', 'name': 'x'}
    array = hl.array([1]).to_ir()
    for expression, message in [
        (variable, 'unbound lambda'),
        ({'op': 'array_map', 'value': hl.literal(1).to_ir(), 'parameter': 'x',
          'body': variable}, 'requires array'),
        ({'op': 'array_map', 'value': array, 'parameter': 'x', 'body':
          {'op': 'array_map', 'value': array, 'parameter': 'x', 'body': variable}},
         'duplicate lambda'),
        ({'op': 'array_map', 'value': variable, 'parameter': 'x', 'body': variable},
         'unbound lambda'),
    ]:
        with pytest.raises(ValueError, match=message):
            t._native.project(_native_projection([['bad', expression]]))
