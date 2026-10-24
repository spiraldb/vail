import math

import pyarrow as pa
import pytest
import vail as hl


def _table(mode, dtype=pa.int64()):
    # Both masks represent the same defined rows. Hidden children deliberately
    # have arbitrary lengths/values; outer slicing retains nonzero offsets.
    values, offsets, missing = [99], [0, 1], [True]
    for row, valid in enumerate(([1, 2, 3], None, [4, None, 6], None, [7, 8, 9])):
        if valid is None:
            values.extend([None] * (65537 if mode == 'hidden' and row == 1 else 5 if mode == 'hidden' else 0))
        else:
            values.extend(valid)
        offsets.append(len(values))
        missing.append(valid is None)
    values.append(88)
    offsets.append(len(values))
    missing.append(True)
    array = pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()), pa.array(values, type=dtype), mask=pa.array(missing)).slice(1, 5)
    return hl.Table.parallelize(pa.table({'xs': array}))


@pytest.mark.parametrize('mode', ['zero', 'hidden'])
@pytest.mark.parametrize('dtype', [pa.int32(), pa.int64(), pa.float32(), pa.float64()])
def test_nullable_coordinate_single_and_bundle(mode, dtype):
    table = _table(mode, dtype)
    assert table.aggregate(hl.agg.array_agg(lambda x: hl.agg.sum(x), table.xs)) == [12, 10, 18]
    result = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(
        s=hl.agg.sum(x), n=hl.agg.count_where(hl.is_defined(x)),
        selected=hl.agg.filter(x > 3, hl.agg.sum(x)), lo=hl.agg.min(x)), table.xs))
    assert [dict(x) for x in result] == [
        dict(s=12, n=3, selected=11, lo=1),
        dict(s=10, n=2, selected=8, lo=2),
        dict(s=18, n=3, selected=15, lo=3)]


def test_nullable_coordinate_empty_and_width_validation():
    schema = hl.tstruct(xs=hl.tarray(hl.tint64))
    for values, expected in [([None, None], None), ([None, [], None, []], [])]:
        table = hl.Table.parallelize([{'xs': x} for x in values], schema=schema)
        assert table.aggregate(hl.agg.array_agg(lambda x: hl.agg.sum(x), table.xs)) == expected
    table = hl.Table.parallelize([{'xs': [1, 2]}, {'xs': None}, {'xs': [3]}], schema=schema)
    with pytest.raises(Exception, match='equal array lengths'):
        table.aggregate(hl.agg.array_agg(lambda x: hl.agg.sum(x), table.xs))


def test_nullable_coordinate_float_order_nan_and_zero():
    table = hl.Table.parallelize([{'xs': x} for x in
        [[1e16, -0., float('nan')], None, [1., 0., 2.], [-1e16, -0., 3.]]],
        schema=hl.tstruct(xs=hl.tarray(hl.tfloat64)), n_partitions=1)
    result = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(s=hl.agg.sum(x), lo=hl.agg.min(x)), table.xs))
    assert result[0].s == 0.
    assert result[1].s == 0.
    assert result[1].lo == 0.
    assert math.isnan(result[2].s)
    assert result[2].lo == 2.
