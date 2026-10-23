"""Public regressions for numeric promotion and literal collection inference."""
import operator

import pytest

import vail as hl


@pytest.mark.parametrize('op', [operator.add, operator.sub, operator.mul, operator.truediv,
                                operator.floordiv, operator.mod, operator.pow])
@pytest.mark.parametrize('left_type,right_type', [(hl.tint32, hl.tint64), (hl.tint64, hl.tfloat32),
                                                  (hl.tfloat32, hl.tfloat64)])
def test_array_arithmetic_promotes_before_zip(op, left_type, right_type):
    left = hl.literal([8, None, 12], hl.tarray(left_type))
    right = hl.literal([2, 3, None], hl.tarray(right_type))
    result = op(left, right)
    common = hl.tint64 if left_type == hl.tint32 else hl.tfloat32 if right_type == hl.tfloat32 else hl.tfloat64
    expected_type = hl.tfloat64 if op is operator.pow or op is operator.truediv and common == hl.tint64 else common
    assert result.dtype == hl.tarray(expected_type)
    assert hl.eval(result) == [op(8, 2), None, None]
    assert hl.eval(op(hl.missing(left.dtype), right)) is None
    assert hl.eval(op(hl.literal([], left.dtype), hl.literal([], right.dtype))) == []


def test_nested_numeric_comparisons_preserve_parent_missingness():
    left_type = hl.tstruct(x=hl.ttuple(hl.tbool, hl.tarray(hl.tint32)))
    right_type = hl.tstruct(x=hl.ttuple(hl.tfloat64, hl.tarray(hl.tfloat64)))
    table = hl.Table.parallelize([
        hl.Struct(left=hl.Struct(x=(True, [2, None])), right=hl.Struct(x=(1., [2., None]))),
        hl.Struct(left=None, right=hl.Struct(x=(1., [2., None]))),
        hl.Struct(left=hl.Struct(x=None), right=hl.Struct(x=None)),
    ], hl.tstruct(left=left_type, right=right_type))
    assert [row.equal for row in table.select(equal=table.left == table.right).collect()] == [True, None, True]
    assert hl.eval(hl.tuple([False, 2.]) < hl.tuple([1., 2.])) is True


def test_dictionary_inference_distinguishes_struct_and_missing_values():
    assert hl.literal({'cat': 3, 'dog': 7}).dtype == hl.tdict(hl.tstr, hl.tint32)
    assert hl.eval(hl.literal({1: None, 2: 4})) == {1: None, 2: 4}
    assert hl.literal(hl.Struct(cat=3)).dtype == hl.tstruct(cat=hl.tint32)
    key = hl.Struct()
    assert hl.eval(hl.literal({key: 7})) == {key: 7}
    assert hl.eval(hl.literal({}, hl.tdict(hl.tint32, hl.tfloat64))) == {}
    assert hl.eval(hl.literal({'key': None}, hl.tdict(hl.tstr, hl.tarray(hl.tint32)))) == {'key': None}
    table = hl.utils.range_table(3)
    table = table.annotate(mapping={'first': table.idx, 'missing': None})
    assert [r.mapping for r in table.collect()] == [{'first': i, 'missing': None} for i in range(3)]
    table = table.annotate(nested={'left': [(table.idx, 1.)], 'right': [(hl.int64(2), 3.)]})
    assert table.nested.dtype == hl.tdict(hl.tstr, hl.tarray(hl.ttuple(hl.tint64, hl.tfloat64)))
    assert [r.nested for r in table.collect()] == [
        {'left': [(i, 1.)], 'right': [(2, 3.)]} for i in range(3)]


def test_nested_expression_literals_capture_constants_once():
    value = hl.literal({1: [hl.int32(3), None], 2: [hl.int32(4)]})
    assert hl.eval(value) == {1: [3, None], 2: [4]}
    assert hl.eval(hl.literal({hl.str('a'), hl.str('b')})) == {'a', 'b'}
    table = hl.utils.range_table(3)
    with pytest.raises(hl.expr.ExpressionException, match='scalar or global'):
        hl.literal([table.idx])
    with pytest.raises(ValueError, match='free variables'):
        hl.array([1]).map(lambda x: hl.literal(x))
