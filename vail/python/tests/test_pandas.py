import struct

import numpy as np
import pandas as pd
import pytest

import vail as hl


def test_pandas_nullable_dtypes_keys_and_index_are_preserved():
    frame = pd.DataFrame({'id': pd.Series([2, 1, None], dtype='Int32'),
                          'big': pd.Series([2**54, None, 3], dtype='Int64'),
                          's': pd.Series(['a', None, 'c'], dtype='string'),
                          'b': pd.Series([True, False, None], dtype='boolean')})
    frame.index = [20, 30, 40]
    table = hl.Table.from_pandas(frame, key='id')
    assert table.row.dtype == hl.tstruct(id=hl.tint32, big=hl.tint64, s=hl.tstr, b=hl.tbool)
    assert list(table.key.dtype) == ['id']
    result = table.to_pandas()
    pd.testing.assert_frame_equal(result, frame.iloc[[1, 0, 2]].reset_index(drop=True))


def test_pandas_float_nan_bits_are_values_and_nullable_masks_are_missing():
    nan = struct.unpack('>d', bytes.fromhex('7ff8000000000042'))[0]
    frame = pd.DataFrame({'x': np.array([-0., nan, np.inf]),
                          'y': pd.Series([1., None, 3.], dtype='Float32')})
    table = hl.Table.from_pandas(frame)
    values = table.x.collect()
    assert struct.pack('>d', values[0]) == struct.pack('>d', -0.)
    assert struct.pack('>d', values[1]) == struct.pack('>d', nan)
    assert table.y.collect() == [1., None, 3.]
    result = table.to_pandas()
    assert not result['x'].isna().any()
    assert result['y'].isna().tolist() == [False, True, False]
    assert str(result['y'].dtype) == 'Float32'


def test_pandas_nested_flatten_and_dtype_overrides():
    table = hl.Table.parallelize([{'s': {'x': 3}, 'a': [1, None]}, {'s': None, 'a': []}],
                                schema=hl.tstruct(s=hl.tstruct(x=hl.tint32), a=hl.tarray(hl.tint32)))
    nested = table.to_pandas(flatten=False)
    assert nested['s'].tolist() == [hl.Struct(x=3), None]
    assert nested['a'].tolist() == [[1, None], []]
    flat = table.to_pandas(types={hl.tint32: 'Float64'})
    assert list(flat.columns) == ['s.x', 'a']
    assert str(flat['s.x'].dtype) == 'Float64'
    assert flat['s.x'].isna().tolist() == [False, True]


def test_pandas_empty_frames_keep_schema_and_zero_column_row_counts():
    frame = pd.DataFrame({'x': pd.Series([], dtype='Int64'), 's': pd.Series([], dtype='string')})
    pd.testing.assert_frame_equal(hl.Table.from_pandas(frame).to_pandas(), frame)
    table = hl.Table.from_pandas(pd.DataFrame(index=range(3)))
    assert table.count() == 3
    assert len(table.to_pandas()) == 3
    with pytest.raises(ValueError, match='unique strings'):
        hl.Table.from_pandas(pd.DataFrame([[1, 2]], columns=['x', 'x']))


def test_pandas_object_columns_keep_hail_collection_types():
    frame = pd.DataFrame({'a': [[1, None], [3, 4]],
                          'd': [{'a': 2}, {'b': 3}],
                          's': [{1, 2}, {3}],
                          'n': [np.array([[1], [2]], dtype=np.int32), np.array([[3], [4]], dtype=np.int32)]})
    table = hl.Table.from_pandas(frame)
    assert table.row.dtype == hl.tstruct(a=hl.tarray(hl.tint32), d=hl.tdict(hl.tstr, hl.tint32),
                                        s=hl.tset(hl.tint32), n=hl.tndarray(hl.tint32, 2))
    result = table.to_pandas()
    assert result['a'].tolist() == frame['a'].tolist()
    assert result['d'].tolist() == frame['d'].tolist()
    assert result['s'].tolist() == frame['s'].tolist()
    for actual, expected in zip(result['n'], frame['n']):
        np.testing.assert_array_equal(actual, expected)
    with pytest.raises(hl.ExpressionException, match='cannot impute array elements'):
        hl.Table.from_pandas(pd.DataFrame({'a': [[1, 2], {1, 2}]}))
