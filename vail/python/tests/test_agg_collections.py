import pytest
import vail as hl


def test_collect_as_set_keeps_element_type_and_values():
    table = hl.Table.parallelize([{'x': 1}, {'x': 1}, {'x': None}, {'x': 2}], schema=hl.tstruct(x=hl.tint32))
    result = hl.agg.collect_as_set(table.x)
    assert result.dtype == hl.tset(hl.tint32)
    assert table.aggregate(result) == {1, 2, None}


def test_take_validates_n_and_executes_ordering():
    table = hl.range_table(5, n_partitions=3)
    result = hl.agg.take(table.idx, 3, -hl.int64(table.idx))
    assert result.dtype == hl.tarray(hl.tint32)
    assert table.aggregate(result) == [4, 3, 2]
    assert table.aggregate(hl.agg.take(table.idx, 0)) == []
    with pytest.raises(ValueError):
        hl.agg.take(table.idx, -1)


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_array_sum_and_coordinate_fallback_across_partitions(partitions):
    table = hl.Table.parallelize([{'x': [1., None, 2.]}, {'x': None}, {'x': [3., None, 6.]}],
                                schema=hl.tstruct(x=hl.tarray(hl.tfloat64)), n_partitions=partitions)
    result = table.aggregate(hl.struct(
        total=hl.agg.array_sum(table.x),
        count=hl.agg.array_agg(lambda x: hl.agg.count_where(hl.is_defined(x)), table.x),
        stats=hl.agg.array_agg(lambda x: hl.agg.stats(x), table.x),
        stdev=hl.agg.array_agg(lambda x: hl.agg.stats(x)['stdev'], table.x),
        n=hl.agg.array_agg(lambda x: hl.agg.count(), table.x)))
    assert result.total == [4., 0., 8.]
    assert result.count == [2, 0, 2]
    assert [s.n for s in result.stats] == [2, 0, 2]
    assert result.stats[0].stdev == 1.
    assert result.stats[2].stdev == 2.
    assert result.stdev[0] == 1.
    assert result.stdev[2] == 2.
    assert result.n == [2, 2, 2]
    with pytest.raises(TypeError):
        hl.agg.array_sum('x')
