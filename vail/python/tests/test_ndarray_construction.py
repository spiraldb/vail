import numpy as np
import pytest
import vail as hl


@pytest.mark.parametrize('dtype', [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64])
def test_ndarray_literals_preserve_shape_dtype_and_strides(dtype):
    original = np.arange(24, dtype=str(dtype)).reshape(4, 6)[:, ::2].T
    result = hl.eval(hl.nd.array(original))
    np.testing.assert_array_equal(result, original)
    assert result.dtype == original.dtype
    for value in [np.array(3, dtype=str(dtype)), np.empty((2, 0, 3), dtype=str(dtype))]:
        result = hl.eval(hl.nd.array(value))
        assert result.shape == value.shape
        assert result.dtype == value.dtype


def test_ndarray_arange_ragged_rows_missing_and_empty():
    table = hl.Table.parallelize([{'n': 3}, {'n': None}, {'n': 0}, {'n': 1}],
                                schema=hl.tstruct(n=hl.tint32))
    result = table.select(a=hl.nd.arange(table.n)).collect()
    assert [None if row.a is None else row.a.tolist() for row in result] == [[0, 1, 2], None, [], [0]]
    assert hl.eval(hl.nd.arange(5, -2, -2)).tolist() == [5, 3, 1, -1]
    assert hl.eval(hl.nd.arange(4, 4)).shape == (0,)


def test_ndarray_arange_storage_and_strict_struct_cast(tmp_path):
    table = hl.range_table(3)
    table = table.select(a=hl.nd.arange(table.idx))
    path = str(tmp_path / 'ndarray.ht')
    table.write(path)
    result = hl.read_table(path)
    assert result.a.dtype == hl.tndarray(hl.tint32, 1)
    assert [row.a.tolist() for row in result.collect()] == [[], [0], [0, 1]]
    packed = hl.struct(__vail_ndarray_data=[1], __vail_ndarray_shape=[1])
    with pytest.raises((TypeError, ValueError), match='cast'):
        hl.cast(packed, hl.tndarray(hl.tint32, 1))


def test_numpy_literal_is_a_snapshot():
    for source in [np.arange(12, dtype=np.int64).reshape(3, 4),
                   np.asfortranarray(np.arange(12, dtype=np.float64).reshape(3, 4)),
                   np.arange(24, dtype=np.int32).reshape(4, 6)[:, ::2]]:
        expected = source.copy()
        expression = hl.nd.array(source)
        source[:] = -1
        np.testing.assert_array_equal(hl.eval(expression), expected)


def test_block_matrix_checked_dimensions_and_row_major_entries():
    from dataclasses import FrozenInstanceError
    from vail.linalg import BlockMatrix
    matrix = BlockMatrix.fill(2, 3, 2.5)
    assert [(r.i, r.j, r.entry) for r in matrix.entries().collect()] == [
        (i, j, 2.5) for i in range(2) for j in range(3)]
    assert list(matrix.entries().key.dtype) == ['i', 'j']
    with pytest.raises(FrozenInstanceError):
        matrix.n_cols = 0
    for rows, cols in [(0, 4), (4, 0), (0, 0)]:
        assert BlockMatrix(rows, cols, 1).entries().collect() == []
    for rows, cols in [(-1, 2), (2, -1), (2**30, 2), (2**31, 0)]:
        with pytest.raises(ValueError):
            BlockMatrix(rows, cols, 1)
    for rows in [True, 1.5]:
        with pytest.raises(TypeError):
            BlockMatrix.fill(rows, 2, 1)
    with pytest.raises(NotImplementedError, match='block_size'):
        BlockMatrix.fill(2, 2, 1, block_size=16)


@pytest.mark.parametrize('data,shape,error', [
    ([1, 2, 3, 4], [2, 2], 'rank'),
    ([1, 2], [-1], 'nonnegative'),
    ([1, 2], [3], 'size'),
    ([1, 2], [None], 'nonnegative'),
    (None, [2], 'defined'),
    ([1, 2], None, 'defined'),
    ([1, None], [2], 'elements'),
])
def test_imported_ndarray_rejects_malformed_storage(data, shape, error):
    import pyarrow as pa
    valid = hl.Table.parallelize([{'a': np.array([1, 2], dtype=np.int32)}],
                                schema=hl.tstruct(a=hl.tndarray(hl.tint32, 1)))
    storage = valid.to_arrow().schema.field('a').type
    array = pa.array([{'__vail_ndarray_data': data, '__vail_ndarray_shape': shape}], type=storage)
    table = hl.Table.parallelize(pa.table({'a': array}))
    with pytest.raises(ValueError, match=error):
        table.collect()
