import pytest

import vail as hl


@pytest.mark.parametrize('value, dtype', [
    (7, hl.tint32),
    ('value', hl.tstr),
    (True, hl.tbool),
    ([1, None, 3], hl.tarray(hl.tint32)),
    ({'a': [1, 2]}, hl.tstruct(a=hl.tarray(hl.tint32))),
])
def test_eval_typed_python_values(value, dtype):
    result, result_type = hl.eval_typed(value)
    assert result == value
    assert result_type == dtype


def test_eval_typed_nested_expression_and_missing():
    expression = hl.struct(x=hl.tuple([1, 's']), y=hl.missing(hl.tarray(hl.tfloat64)))
    value, dtype = hl.eval_typed(expression)
    assert value == hl.Struct(x=(1, 's'), y=None)
    assert dtype is expression.dtype
    assert hl.eval_typed(hl.missing(hl.tint64)) == (None, hl.tint64)
    assert hl.eval_typed(hl.int64(2) + 3) == (5, hl.tint64)


def test_eval_typed_globals():
    table = hl.range_table(2).annotate_globals(g=hl.struct(x=3))
    matrix = hl.range_matrix_table(2, 2).annotate_globals(g=hl.array([4, 5]))
    assert hl.eval_typed(table.g.x + 1) == (4, hl.tint32)
    assert hl.eval_typed(matrix.g) == ([4, 5], hl.tarray(hl.tint32))


def test_eval_typed_rejects_indices_like_eval():
    table = hl.range_table(2)
    matrix = hl.range_matrix_table(2, 2)
    for expression in (table.idx, matrix.row_idx, matrix.col_idx,
                       matrix.row_idx + matrix.col_idx):
        with pytest.raises(ValueError) as expected:
            hl.eval(expression)
        with pytest.raises(ValueError, match=str(expected.value)):
            hl.eval_typed(expression)


def test_eval_typed_expression_function_namespace():
    assert hl.expr.functions.eval_typed is hl.eval_typed
