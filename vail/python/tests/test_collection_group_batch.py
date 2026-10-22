"""Grouping independent lists preserves row ownership and arbitrary list spans."""
import vail as hl


def test_counter_keeps_outer_lists_independent():
    t = hl.Table.parallelize([
        {'xs': [2, 1, 2, None]},
        {'xs': []},
        {'xs': [1, 1, 3]},
        {'xs': None},
        {'xs': [2]},
    ], hl.tstruct(xs=hl.tarray(hl.tint32)))
    out = t.annotate(c=t.xs.aggregate(lambda x: hl.agg.counter(x))).collect()
    assert [r.c for r in out] == [{None: 1, 1: 1, 2: 2}, {}, {1: 2, 3: 1}, None, {2: 1}]


def test_grouped_filtered_reducers_preserve_nulls_and_owners():
    t = hl.Table.parallelize([
        {'xs': [1, 2, 1, 3]}, {'xs': []}, {'xs': [2, 2, 3]},
    ], hl.tstruct(xs=hl.tarray(hl.tint32)))
    result = t.annotate(g=t.xs.aggregate(lambda x: hl.agg.group_by(
        x % 2, hl.struct(n=hl.agg.count(), total=hl.agg.filter(x > 1, hl.agg.sum(x)))
    ))).collect()
    assert [r.g for r in result] == [
        {0: hl.Struct(n=1, total=2), 1: hl.Struct(n=3, total=3)},
        {},
        {0: hl.Struct(n=2, total=4), 1: hl.Struct(n=1, total=3)},
    ]
