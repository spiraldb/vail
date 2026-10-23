"""Coordinate products and Boolean reductions preserve native identities."""
import ctypes
import math
import pytest
import vail as hl


@pytest.mark.parametrize('width', [3, 65])
@pytest.mark.parametrize('partitions', [1, 3])
def test_coordinate_boolean_bit_offsets_and_filtered_products(width, partitions):
    values = [[None if (row + column) % 4 == 0 else (row + column) % 3 != 0
               for column in range(width)] for row in range(5)]
    table = hl.Table.parallelize([{'xs': row} for row in values],
        schema=hl.tstruct(xs=hl.tarray(hl.tbool)), n_partitions=partitions)
    result = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(
        all=hl.agg.all(x), any=hl.agg.any(x), product=hl.agg.product(hl.int32(x)),
        empty_all=hl.agg.filter(False, hl.agg.all(x)),
        empty_any=hl.agg.filter(False, hl.agg.any(x)),
        selected=hl.agg.filter(x, hl.agg.product(hl.int32(x)))), table.xs))
    for column, output in enumerate(result):
        defined = [row[column] for row in values if row[column] is not None]
        assert output.all == all(defined) and output.any == any(defined)
        assert output.product == math.prod(map(int, defined))
        assert output.empty_all is True and output.empty_any is False
        assert output.selected == 1


@pytest.mark.parametrize('dtype,values,expected', [
    (hl.tint32, [2147483647, 2], 4294967294),
    (hl.tint64, [9223372036854775807, 2], -2),
    (hl.tfloat32, [.1, .1], ctypes.c_float(.1).value ** 2),
    (hl.tfloat64, [.1, .1], .1 ** 2),
])
def test_coordinate_product_widening_wrapping_and_missing_identity(dtype, values, expected):
    table = hl.Table.parallelize([{'xs': [value, None]} for value in values],
        schema=hl.tstruct(xs=hl.tarray(dtype)), n_partitions=2)
    output = table.aggregate(hl.agg.array_agg(lambda x: hl.agg.product(x), table.xs))
    assert output == [expected, 1]


def test_coordinate_product_preserves_int32_map_wrapping_before_widening():
    table = hl.Table.parallelize([{'xs': [2147483647]}, {'xs': [2147483647]}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)), n_partitions=2)
    output = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(
        product=hl.agg.product(x + 1)), table.xs))
    assert output[0].product == 4611686018427387904


@pytest.mark.parametrize('partitions', [1, 3])
def test_coordinate_true_count_simple_and_composite(partitions):
    table = hl.Table.parallelize([{'xs': xs} for xs in [[True, None, False], [False, None, True], [True, None, None]]],
        schema=hl.tstruct(xs=hl.tarray(hl.tbool)), n_partitions=partitions)
    assert table.aggregate(hl.agg.array_agg(lambda x: hl.agg.count_where(x), table.xs)) == [2,0,1]
    output = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(
        count=hl.agg.count_where(x), filtered=hl.agg.filter(False, hl.agg.count_where(x))), table.xs))
    assert [(x.count,x.filtered) for x in output] == [(2,0),(0,0),(1,0)]
