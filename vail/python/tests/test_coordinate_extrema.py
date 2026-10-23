"""Primitive extrema use native Arrow ordering across aligned coordinates."""
import math
import pytest
import vail as hl


@pytest.mark.parametrize('partitions', [1, 3])
@pytest.mark.parametrize('dtype', [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64])
def test_coordinate_extrema_null_filters_and_partitions(dtype, partitions):
    rows = [[3, None, 9], [1, None, 4], [2, None, None]]
    table = hl.Table.parallelize([{'xs': xs} for xs in rows],
        schema=hl.tstruct(xs=hl.tarray(dtype)), n_partitions=partitions)
    result = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(
        lo=hl.agg.min(x), hi=hl.agg.max(x),
        selected=hl.agg.filter(x > 1, hl.agg.min(x)),
        empty=hl.agg.filter(False, hl.agg.max(x))), table.xs))
    assert [(x.lo, x.hi, x.selected, x.empty) for x in result] == [
        (1, 3, 2, None), (None, None, None, None), (4, 9, 4, None)]


@pytest.mark.parametrize('dtype', [hl.tfloat32, hl.tfloat64])
@pytest.mark.parametrize('partitions', [1, 3])
def test_coordinate_extrema_native_total_float_order(dtype, partitions):
    rows = [[float('nan'), float('inf'), -0.0], [2., float('inf'), 0.0],
            [float('-inf'), None, -0.0]]
    table = hl.Table.parallelize([{'xs': xs} for xs in rows],
        schema=hl.tstruct(xs=hl.tarray(dtype)), n_partitions=partitions)
    result = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(
        lo=hl.agg.min(x), hi=hl.agg.max(x)), table.xs))
    assert result[0].lo == float('-inf') and result[0].hi == 2.
    assert result[1].lo == result[1].hi == float('inf')
    assert math.copysign(1, result[2].lo) == -1
    assert math.copysign(1, result[2].hi) == 1
