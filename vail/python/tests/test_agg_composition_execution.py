import math

import pytest
import vail as hl


def test_nested_group_by_counter_and_weighted_counter():
    t = hl.Table.parallelize([dict(k='a', x=1), dict(k='b', x=2), dict(k='a', x=3), dict(k=None, x=None)],
                            schema=hl.tstruct(k=hl.tstr, x=hl.tint32), n_partitions=3)
    assert t.aggregate(hl.agg.counter(t.k)) == {'a': 2, 'b': 1, None: 1}
    assert t.aggregate(hl.agg.counter(t.k, weight=t.x)) == {'a': 4, 'b': 2, None: 0}
    result = t.aggregate(hl.agg.group_by(t.k, hl.struct(n=hl.agg.count(), values=hl.agg.collect(t.x))))
    assert result == {'a': hl.Struct(n=2, values=[1, 3]), 'b': hl.Struct(n=1, values=[2]), None: hl.Struct(n=1, values=[None])}
    result = t.aggregate(hl.agg.group_by(t.k, hl.agg.group_by(hl.is_missing(t.x), hl.agg.count())))
    assert result == {'a': {False: 2}, 'b': {False: 1}, None: {True: 1}}
    empty = t.head(0)
    assert empty.aggregate(hl.agg.counter(empty.k)) == {}


def test_array_agg_empty_missing_mismatch_and_scopes():
    t = hl.range_table(10)
    t = t.annotate(xs=[t.idx, 2 * t.idx])
    assert t.aggregate(hl.agg.array_sum(t.xs)) == [45, 90]
    assert t.aggregate(hl.agg.array_agg(lambda x: hl.agg.sum(x) + hl.agg.filter(x % 3 == 0, hl.agg.sum(x)), t.xs)) == [63, 126]
    empty = t.head(0)
    assert empty.aggregate(hl.agg.array_sum(empty.xs)) is None
    t = hl.Table.parallelize([dict(xs=[1, None]), dict(xs=None), dict(xs=[3, None])], schema=hl.tstruct(xs=hl.tarray(hl.tint32)))
    assert t.aggregate(hl.agg.array_sum(t.xs)) == [4, 0]
    t = hl.Table.parallelize([dict(xs=[1]), dict(xs=[])], schema=hl.tstruct(xs=hl.tarray(hl.tint32)))
    with pytest.raises(hl.utils.FatalError, match=r'array_agg requires equal array lengths, found 1 and 0'):
        t.aggregate(hl.agg.array_sum(t.xs))

    # Other native execution errors retain their ordinary RuntimeError type.
    with pytest.raises(RuntimeError, match='equal length'):
        hl.eval(hl.corr([1], [1, 2]))


def test_array_agg_within_table_groups_and_nested_dictionary():
    t = hl.range_table(6)
    t = t.annotate(xs=[t.idx, t.idx % 2])
    rows = t.group_by(k=t.idx % 2).aggregate(xs=hl.agg.array_sum(t.xs), d=hl.agg.counter(t.idx % 3)).collect()
    assert rows == [hl.Struct(k=0, xs=[6, 0], d={0: 1, 1: 1, 2: 1}), hl.Struct(k=1, xs=[9, 3], d={0: 1, 1: 1, 2: 1})]


def test_collection_reducers_execute():
    t = hl.Table.parallelize([dict(x=3), dict(x=None), dict(x=1), dict(x=3)], schema=hl.tstruct(x=hl.tint32), n_partitions=3)
    assert t.aggregate(hl.agg.collect_as_set(t.x)) == {None, 1, 3}
    assert t.aggregate(hl.agg.take(t.x, 2)) == [3, None]
    assert t.aggregate(hl.agg.take(t.x, 3, ordering=lambda x: x)) == [1, 3, 3]
    assert t.aggregate(hl.agg.take(t.x, 2, ordering=-t.x)) == [3, 3]
    assert t.aggregate(hl.agg.take(t.x, 0)) == []
    empty = t.head(0)
    assert empty.aggregate(hl.agg.collect_as_set(empty.x)) == set()


def test_fold_executes_compiled_expressions():
    t = hl.range_table(10, n_partitions=4)
    assert t.aggregate(hl.agg.fold(0, lambda state: state + t.idx, lambda a, b: a + b)) == 45
    assert t.group_by(k=t.idx % 2).aggregate(s=hl.agg.fold(0, lambda state: state + t.idx, lambda a, b: a + b)).collect() == [hl.Struct(k=0, s=20), hl.Struct(k=1, s=25)]
    empty = t.head(0)
    assert empty.aggregate(hl.agg.fold(7, lambda state: state + empty.idx, lambda a, b: a + b)) == 7


def test_numeric_libraries_execute():
    t = hl.range_table(100)
    assert t.aggregate(hl.agg.corr(t.idx, 2 * t.idx)) == pytest.approx(1.0)
    h = t.aggregate(hl.agg.hist(t.idx, 0, 100, 10))
    assert h.bin_freq == [10] * 10 and h.n_smaller == h.n_larger == 0
    assert h.bin_edges == [float(x) for x in range(0, 101, 10)]
    qs = t.aggregate(hl.agg.approx_quantiles(t.idx, [0, .5, 1]))
    assert qs[0] == 0 and qs[2] == 99 and 40 <= qs[1] <= 60
    assert 40 <= t.aggregate(hl.agg.approx_median(t.idx)) <= 60
    fit = t.aggregate(hl.agg.linreg(2.0 + 3 * t.idx, [1.0, hl.float64(t.idx)]))
    assert fit.n == 100 and fit.beta == pytest.approx([2, 3])


def test_tuple_and_struct_aggregate_values_round_trip(tmp_path):
    t = hl.range_table(3)
    t = t.annotate(pair=(t.idx, t.idx + 1))
    assert t.aggregate(hl.agg.collect(t.pair)) == [(0, 1), (1, 2), (2, 3)]
    assert t.aggregate(hl.agg.sum(t.pair[1])) == 6
    result = t.aggregate(hl.agg.fold(
        hl.struct(n=0, total=0),
        lambda state: hl.struct(n=state.n + 1, total=state.total + t.idx),
        lambda left, right: hl.struct(n=left.n + right.n, total=left.total + right.total)))
    assert result == hl.Struct(n=3, total=3)
    grouped = t.group_by(k=t.idx % 2).aggregate(d=hl.agg.counter(t.idx), values=hl.agg.collect(t.pair))
    path = str(tmp_path / "aggregate.viceberg")
    grouped.write(path)
    assert hl.read_table(path).collect() == grouped.collect()
