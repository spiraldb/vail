from vail.expressions import _native_expression, _native_projection
"""Typed array filtering runs as a DataFusion higher-order expression."""
import json

import pytest
import vail as hl


def test_filter_preserves_order_and_array_nullability():
    t = hl.Table.parallelize(
        [{'a': None}, {'a': []}, {'a': [None, 1, 2, 3, None, 4]}],
        schema=hl.tstruct(a=hl.tarray(hl.tint32)))
    assert t.a.filter(lambda x: x > 2).collect() == [None, [], [3, 4]]
    assert t.a.filter(lambda x: hl.is_missing(x)).collect() == [None, [], [None, None]]
    assert 'array_filter' in t.select(a=t.a.filter(lambda x: x > 2))._native.physical_plan()


def test_filter_null_predicate_discards_element_and_can_capture_scope():
    t = hl.range_table(4).annotate_globals(cutoff=1)
    expr = hl.array([None, 0, 1, 2, 3]).filter(
        lambda x: x > t.idx + t.cutoff)
    assert t.annotate(a=expr).a.collect() == [[2, 3], [3], [], []]
    nested = hl.literal([[0, 1, 2], None, [], [3, 4]],
                        hl.tarray(hl.tarray(hl.tint32))).map(
        lambda xs: xs.filter(lambda x: x > t.idx))
    assert t.annotate(a=nested).a.collect() == [
        [[1, 2], None, [], [3, 4]], [[2], None, [], [3, 4]],
        [[], None, [], [3, 4]], [[], None, [], [4]],
    ]


def test_filter_captures_indexed_globals():
    indexed = hl.range_table(0).annotate_globals(offset=10).index_globals()
    t = hl.range_table(3)
    values = hl.array([1, 2, 3]).filter(
        lambda x: x + indexed.offset > t.idx + 10)
    assert t.annotate(a=values).a.collect() == [[1, 2, 3], [2, 3], [3]]


def test_filter_callback_is_built_once_and_empty_arrays_do_not_evaluate_body():
    calls = []
    expr = hl.array([1, 2]).filter(lambda x: calls.append(x) or x > 1)
    assert len(calls) == 1
    assert hl.eval(expr) == [2]
    for array in [hl.literal([], hl.tarray(hl.tint32)),
                  hl.missing(hl.tarray(hl.tint32))]:
        assert hl.eval(array.filter(lambda x: hl.array([True])[10])) == hl.eval(array)
    bad = hl.range_table(1).annotate(
        a=hl.array([1]).filter(lambda x: hl.array([True])[10]))
    assert bad.head(0).collect() == []
    with pytest.raises(RuntimeError, match='out of bounds'):
        bad.collect()


def test_filter_rejects_bad_callbacks_and_native_ir_types():
    with pytest.raises(TypeError, match='callable'):
        hl.array([1]).filter(2)
    with pytest.raises(TypeError, match='bool'):
        hl.array([1]).filter(lambda x: x + 1)
    with pytest.raises(AttributeError):
        hl.literal(1).filter(lambda x: x > 0)

    t = hl.range_table(1)
    malformed = {
        'op': 'array_filter', 'value': hl.array([1]).to_ir(),
        'parameter': 'x', 'predicate': hl.literal(1).to_ir(),
    }
    with pytest.raises(ValueError, match='requires bool'):
        t._native.project(_native_projection([['bad', malformed]]))


def test_filter_rejects_unbound_or_duplicate_lambda_binders_and_mixed_sources():
    import vail as hl

    t = hl.range_table(1)
    array = hl.array([1]).to_ir()
    x = {'op': 'lambda_variable', 'name': 'x'}
    unbound = {'op': 'array_filter', 'value': array, 'parameter': 'x',
               'predicate': {'op': 'lambda_variable', 'name': 'missing'}}
    with pytest.raises(ValueError, match='unbound lambda'):
        t._native.project(_native_projection([['bad', unbound]]))

    duplicate = {
        'op': 'array_map', 'value': array, 'parameter': 'x',
        'body': {'op': 'array_filter', 'value': array, 'parameter': 'x',
                 'predicate': {'op': 'binary', 'operator': 'gt',
                               'left': x, 'right': hl.literal(0).to_ir()}},
    }
    with pytest.raises(ValueError, match='duplicate lambda'):
        t._native.project(_native_projection([['bad', duplicate]]))

    left, right = hl.range_table(2), hl.range_table(2)
    with pytest.raises(ValueError, match='source mismatch'):
        hl.array([left.idx]).filter(lambda x: x > right.idx)
