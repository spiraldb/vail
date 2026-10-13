import pytest

import vail as hl


def test_if_else_missing_false_scalar_and_row_conditions():
    for flag in (False, True):
        assert hl.eval(hl.if_else(True, 1, 2, flag)) == 1
        assert hl.eval(hl.if_else(False, 1, 2, missing_false=flag)) == 2
        assert hl.eval(hl.if_else(hl.missing(hl.tbool), 1, 2, flag)) == (2 if flag else None)
    table = hl.Table.parallelize(
        [{'p': True}, {'p': False}, {'p': None}], schema=hl.tstruct(p=hl.tbool)
    )
    assert hl.if_else(table.p, 1, 2).collect() == [1, 2, None]
    assert hl.if_else(table.p, 1, 2, missing_false=True).collect() == [1, 2, 2]


def test_if_else_missing_false_preserves_branch_missingness_and_laziness():
    assert hl.eval(hl.if_else(hl.missing(hl.tbool), 1, hl.missing(hl.tint32), True)) is None
    table = hl.Table.parallelize(
        [{'p': True, 's': 'invalid'}, {'p': False, 's': '4'}, {'p': None, 's': '5'}],
        schema=hl.tstruct(p=hl.tbool, s=hl.tstr),
    )
    assert hl.if_else(table.p, 7, hl.int32(table.s), True).collect() == [7, 4, 5]


def test_if_else_missing_false_preserves_type_and_source_validation():
    expression = hl.if_else(hl.missing(hl.tbool), 1, hl.int64(2), True)
    assert expression.dtype == hl.tint64
    assert hl.eval(expression) == 2
    with pytest.raises(TypeError, match='missing_false must be a bool'):
        hl.if_else(True, 1, 2, missing_false=1)
    with pytest.raises(TypeError, match='condition must have bool type'):
        hl.if_else(1, 1, 2, True)
    with pytest.raises(TypeError, match='incompatible types'):
        hl.if_else(True, 1, 'bad', True)
    table, other = hl.range_table(2), hl.range_table(2)
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        hl.if_else(table.idx == 0, other.idx, 0, True)
