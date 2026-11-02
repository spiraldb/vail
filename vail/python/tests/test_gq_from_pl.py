"""Hail's genotype-quality calculation caps its two minima at 99."""
import pytest
import vail as hl


@pytest.mark.parametrize('pl,expected', [
    (None, None), ([], 0), ([None, None], 0), ([0], 99),
    ([0, 69, 1035], 69), ([7, 7, 20], 0), ([120, 150], 0),
    ([None, 8, 3, None], 5), ([-10, -5], 5),
    ([-2147483648, 2147483647], -2147483549),
])
def test_gq_from_pl(pl, expected):
    assert hl.eval(hl.gq_from_pl(hl.literal(pl, hl.tarray(hl.tint32)))) == expected


def test_gq_from_pl_maps_nullable_lists():
    t = hl.range_table(5, n_partitions=2)
    values = hl.literal([[0, 20, 30], None, [], [None, 8, 3], [120, 150]],
                        hl.tarray(hl.tarray(hl.tint32)))
    t = t.annotate(gq=hl.gq_from_pl(values[hl.int32(t.idx)]))
    assert [row.gq for row in t.collect()] == [20, None, 0, 5, 0]
