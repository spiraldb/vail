"""Public API contracts used by Hail import/export regression fixtures.

Pinned Hail e10cc355: expr/functions.py parse_call/abs;
expr/expressions/{base_expression,typed_expressions}.py show/reshape.
"""
import math
import numpy as np
import pytest
import vail as hl


@pytest.mark.parametrize('text,alleles,phased', [
    ('-', [], False), ('|-', [], True), ('2', [2], False), ('|2', [2], True),
    ('2/0', [0, 2], False), ('2|0', [2, 0], True), (' 1 / 0 ', [0, 1], False),
])
def test_parse_call_notation(text, alleles, phased):
    assert hl.eval(hl.parse_call(text)) == hl.Call(alleles, phased)


def test_parse_call_missing_invalid_and_indexed():
    assert hl.eval(hl.parse_call(hl.missing(hl.tstr))) is None
    table = hl.Table.parallelize([{'s': '1/0'}, {'s': None}],
                                 schema=hl.tstruct(s=hl.tstr))
    assert table.select(c=hl.parse_call(table.s)).collect() == [
        hl.Struct(c=hl.Call([0, 1])), hl.Struct(c=None)]
    for text in ['1 2', '1/2/3', '0/', '.', '-1', '65536', '1|2/0']:
        with pytest.raises(hl.utils.FatalError):
            hl.eval(hl.parse_call(text))


def test_abs_scalar_array_ndarray_preserves_types_and_missing():
    for dtype in (hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64):
        expr = hl.abs(hl.literal(-2, dtype))
        assert expr.dtype == dtype
        assert hl.eval(expr) == 2
        assert hl.eval(hl.abs(hl.missing(dtype))) is None
    assert hl.eval(hl.abs(hl.literal(-(1 << 31), hl.tint32))) == -(1 << 31)
    assert hl.eval(hl.abs(hl.literal(-(1 << 63), hl.tint64))) == -(1 << 63)
    assert math.copysign(1, hl.eval(hl.abs(-0.0))) == 1
    assert hl.eval(hl.abs(hl.literal([-2, None, 3], hl.tarray(hl.tint32)))) == [2, None, 3]
    np.testing.assert_array_equal(hl.eval(hl.abs(hl.nd.array([[-2, 3]]))), [[2, 3]])
    assert hl.eval(hl.abs(hl.missing(hl.tndarray(hl.tint32, 2)))) is None


def test_reshape_shapes_inference_missing_and_indexed():
    vector = hl.nd.array([0, 1, 2, 3, 4, 5])
    for shape in [(2, 3), (-1, 3), (2, -1)]:
        expr = vector.reshape(shape)
        assert expr.dtype == hl.tndarray(hl.tint32, 2)
        np.testing.assert_array_equal(hl.eval(expr), np.arange(6).reshape(2, 3))
    np.testing.assert_array_equal(hl.eval(vector.reshape(2, 3)), np.arange(6).reshape(2, 3))
    np.testing.assert_array_equal(hl.eval(vector.reshape(6)), np.arange(6))
    np.testing.assert_array_equal(hl.eval(hl.nd.array([4]).reshape(())), np.array(4))
    assert hl.eval(hl.nd.array([], dtype=hl.tint32).reshape((2, 0))).shape == (2, 0)
    assert hl.eval(hl.missing(hl.tndarray(hl.tint32, 1)).reshape((1,))) is None
    assert hl.eval(vector.reshape(hl.missing(hl.ttuple(hl.tint32, hl.tint64)))) is None
    table = hl.range_table(2)
    rows = table.select(a=vector.reshape((table.idx + 1, -1))).collect()
    np.testing.assert_array_equal(rows[0].a, np.arange(6).reshape(1, 6))
    np.testing.assert_array_equal(rows[1].a, np.arange(6).reshape(2, 3))
    for shape in [(4, 2), (-1, -1), (-2, 3), (0, -1), (hl.missing(hl.tint32), 3)]:
        with pytest.raises(hl.utils.FatalError):
            hl.eval(vector.reshape(shape))


def test_expression_show_scalar_row_global_and_entry():
    output = []
    hl.literal(123).show(handler=output.append)
    assert '<expr>' in output[-1] and 'int32' in output[-1] and '123' in output[-1]
    table = hl.range_table(2).annotate_globals(label='global')
    (table.idx + 3).show(n=1, handler=output.append)
    assert 'idx' in output[-1] and '3' in output[-1] and 'top 1' in output[-1]
    table.label.show(handler=output.append)
    assert 'global' in output[-1]
    matrix = hl.utils.range_matrix_table(2, 2).annotate_globals(label='matrix')
    matrix.label.show(handler=output.append)
    assert 'matrix' in output[-1]
    hl.tuple((matrix.row_idx, matrix.col_idx)).show(n_rows=1, n_cols=1, handler=output.append)
    assert '(0, 0)' in output[-1] and '(1, 0)' not in output[-1]


def test_triangle_signed_int32_and_missing():
    for value, expected in [(0, 0), (1, 1), (3, 6), (-3, 3),
                            (65535, -32768), (2147483647, -1073741824)]:
        result = hl.triangle(value)
        assert result.dtype == hl.tint32
        assert hl.eval(result) == expected
    assert hl.eval(hl.triangle(hl.missing(hl.tint32))) is None
    with pytest.raises(TypeError):
        hl.triangle(hl.int64(3))


def test_unphased_diploid_gt_index_calls_arrays_rows_and_missing():
    calls = [hl.Call([0, 0]), hl.Call([0, 1]), hl.Call([1, 1]),
             hl.Call([2, 0]), hl.Call([1, 2]), hl.Call([2, 2]), hl.Call([2, 0], phased=True)]
    assert hl.eval(hl.literal(calls).map(lambda c: c.unphased_diploid_gt_index())) == [0, 1, 2, 3, 4, 5, 3]
    assert hl.eval(hl.missing(hl.tcall).unphased_diploid_gt_index()) is None
    table = hl.Table.parallelize([{'c': calls[3]}, {'c': None}], schema=hl.tstruct(c=hl.tcall))
    assert table.select(index=table.c.unphased_diploid_gt_index()).collect() == [
        hl.Struct(index=3), hl.Struct(index=None)]
    for call in (hl.Call([]), hl.Call([1])):
        with pytest.raises(hl.utils.FatalError, match='ploidy == 2'):
            hl.eval(hl.literal(call).unphased_diploid_gt_index())
    # Accepted current UInt16 calls use a wide triangular intermediate, then
    # the public int32 result wraps only at its final representation boundary.
    assert hl.eval(hl.call(65535, 65535).unphased_diploid_gt_index()) == -2147450881
    assert hl.eval(hl.call(0, 65535, phased=True).unphased_diploid_gt_index()) == 2147450880
    for index in (0, 1, 2, 3, 1000000, 2147483647):
        assert hl.eval(hl.unphased_diploid_gt_index_call(index).unphased_diploid_gt_index()) == index
