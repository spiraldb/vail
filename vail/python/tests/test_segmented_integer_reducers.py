"""Integer collection reductions through public expression/table/matrix APIs."""
from collections import Counter

import pytest
import vail as hl


@pytest.mark.parametrize('dtype', [hl.tint32, hl.tint64])
def test_segmented_integer_empty_missing_and_sliced_lists(dtype):
    rows = [
        {'idx': 0, 'xs': [99, 1, None, -3, 7, 88]},
        {'idx': 1, 'xs': [99, None, None, 88]},
        {'idx': 2, 'xs': [99, 88]},
        {'idx': 3, 'xs': None},
    ]
    t = hl.Table.parallelize(rows, schema=hl.tstruct(idx=hl.tint32, xs=hl.tarray(dtype)), n_partitions=2)
    xs = t.xs[1:-1]
    result = t.select(idx=t.idx, value=xs.aggregate(lambda x: hl.struct(
        total=hl.agg.sum(x), product=hl.agg.product(x),
        lo=hl.agg.min(x), hi=hl.agg.max(x))))
    values = {r.idx: r.value for r in result.collect()}
    assert values[0] == hl.Struct(total=5, product=-21, lo=-3, hi=7)
    assert values[1] == hl.Struct(total=0, product=1, lo=None, hi=None)
    assert values[2] == hl.Struct(total=0, product=1, lo=None, hi=None)
    assert values[3] == hl.Struct(total=None, product=None, lo=None, hi=None)


def test_segmented_integer_widening_and_wrapping():
    wide32 = hl.literal([2**31 - 1, 2**31 - 1], hl.tarray(hl.tint32))
    assert hl.eval(wide32.aggregate(lambda x: hl.agg.sum(x))) == 2**32 - 2
    assert hl.eval(wide32.aggregate(lambda x: hl.agg.product(x))) == (2**31 - 1)**2
    wide64 = hl.literal([2**63 - 1, 1], hl.tarray(hl.tint64))
    assert hl.eval(wide64.aggregate(lambda x: hl.agg.sum(x))) == -(2**63)
    product64 = hl.literal([2**62, 4], hl.tarray(hl.tint64))
    assert hl.eval(product64.aggregate(lambda x: hl.agg.product(x))) == 0


@pytest.mark.parametrize('dtype', [hl.tint32, hl.tint64])
def test_segmented_integer_filtered_and_nested_entries(dtype):
    cast = hl.int32 if dtype == hl.tint32 else hl.int64
    m = hl.range_matrix_table(5, 4)
    m = m.annotate_entries(xs=hl.array([cast(m.row_idx),
                                      hl.missing(dtype), cast(m.col_idx)]))
    m = m.annotate_entries(score=m.xs.aggregate(lambda x: hl.agg.filter(
        x > 0, hl.agg.sum(x))))
    assert Counter(e.score for e in m.entries().collect()) == Counter(r + c for r in range(5) for c in range(4))
    assert m.aggregate_entries(hl.agg.sum(m.score)) == 70


@pytest.mark.parametrize('dtype', [hl.tint32, hl.tint64])
def test_segmented_integer_validity_across_bitmap_words(dtype):
    rows = [{'idx': r, 'xs': [None if (j + r) % 11 == 0 else j - 70
                              for j in range(130 + r % 7)]} for r in range(70)]
    table = hl.Table.parallelize(rows, schema=hl.tstruct(idx=hl.tint32, xs=hl.tarray(dtype)),
                                 n_partitions=3)
    source = table.xs[3:-2]
    result = table.select(idx=table.idx, value=source.aggregate(lambda x: hl.struct(
        total=hl.agg.sum(x), lo=hl.agg.min(x), hi=hl.agg.max(x))))
    expected = {}
    for row in rows:
        values = [x for x in row['xs'][3:-2] if x is not None]
        expected[row['idx']] = hl.Struct(total=sum(values), lo=min(values), hi=max(values))
    assert {row.idx: row.value for row in result.collect()} == expected
