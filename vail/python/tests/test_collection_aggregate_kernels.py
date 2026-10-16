"""Public collection aggregate values across chunks, ordering and partitions."""
import math
import pytest
import vail as hl


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_take_limits_and_orderings_coexist_without_configuration_aliasing(partitions):
    table = hl.range_table(17, n_partitions=partitions)
    query = hl.struct(one=hl.agg.take(table.idx, 1), three=hl.agg.take(table.idx, 3),
                      zero=hl.agg.take(table.idx, 0),
                      last=hl.agg.take(table.idx, 3, -table.idx),
                      first=hl.agg.take(table.idx, 3, table.idx))
    expected = hl.Struct(one=[0], three=[0, 1, 2], zero=[], last=[16, 15, 14], first=[0, 1, 2])
    assert table.aggregate(query) == expected
    assert table.aggregate(query) == expected
    with pytest.raises(ValueError):
        hl.agg.take(table.idx, True)


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_collect_and_take_preserve_missing_nested_tagged_values(partitions, tmp_path):
    dtype = hl.tstruct(i=hl.tint32, s=hl.tset(hl.tint32),
                      d=hl.tdict(hl.tstr, hl.tarray(hl.tint32)), locus=hl.tlocus('GRCh37'))
    values = [hl.Struct(i=2, s={1, 2}, d={'a': [1, None]}, locus=hl.Locus('2', 3, 'GRCh37')),
              None,
              hl.Struct(i=1, s=set(), d={}, locus=hl.Locus('10', 1, 'GRCh37'))]
    table = hl.Table.parallelize([{'x': x, 'order': i} for i, x in enumerate(values)],
                                schema=hl.tstruct(x=dtype, order=hl.tint32), n_partitions=partitions)
    result = table.aggregate(hl.struct(all=hl.agg.collect(table.x),
                                      first=hl.agg.take(table.x, 2),
                                      reverse=hl.agg.take(table.x, 2, -table.order)))
    assert result.all == values
    assert result.first == values[:2]
    assert result.reverse == list(reversed(values))[:2]
    if partitions == 1:
        # Partition-local empty nested streams hit the excluded Vortex writer
        # defect; Vail aggregate partition behavior is checked above for all p.
        path = str(tmp_path / 'nested.ht')
        table.write(path)
        restored = hl.read_table(path)
        assert restored.aggregate(hl.agg.collect(restored.x)) == values


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_collect_as_set_deduplicates_ieee_values_and_keeps_one_missing(partitions):
    table = hl.Table.parallelize([{'x': x} for x in [None, -0., 0., math.nan, math.nan, 1.]],
                                schema=hl.tstruct(x=hl.tfloat64), n_partitions=partitions)
    values = table.aggregate(hl.agg.collect_as_set(table.x))
    assert len(values) == 4
    assert None in values and 0. in values and 1. in values
    assert sum(isinstance(x, float) and math.isnan(x) for x in values) == 1


def test_configured_aggregates_with_different_parameters_coexist_on_wide_named_values():
    table = hl.range_table(5)
    value = hl.struct(**{'α|"': table.idx, 'nested': [table.idx, None]})
    query = hl.struct(a=hl.agg.take(value, 1), b=hl.agg.take(value, 3),
                      h1=hl.agg.hist(table.idx, 0., 4., 2),
                      h2=hl.agg.hist(table.idx, 0., 8., 4),
                      q1=hl.agg.approx_quantiles(table.idx, [0., 1.], k=8),
                      q2=hl.agg.approx_quantiles(table.idx, [.5], k=16))
    result = table.aggregate(query)
    assert [v['α|"'] for v in result.a] == [0]
    assert [v['α|"'] for v in result.b] == [0, 1, 2]
    assert result.h1.bin_freq == [2, 3]
    assert result.h2.bin_freq == [2, 2, 1, 0]
    assert result.q1 == [0, 4] and result.q2 == [2]


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_counter_preserves_missing_keys_and_weighted_sums(partitions):
    table = hl.Table.parallelize([{'x': None, 'w': 1.}, {'x': 1, 'w': 2.},
                                 {'x': 1, 'w': None}, {'x': 2, 'w': -1.}],
                                schema=hl.tstruct(x=hl.tint32, w=hl.tfloat64), n_partitions=partitions)
    result = table.aggregate(hl.struct(count=hl.agg.counter(table.x),
                                      weight=hl.agg.counter(table.x, weight=table.w)))
    assert result.count == {None: 1, 1: 2, 2: 1}
    assert result.weight == {None: 1., 1: 2., 2: -1.}


def test_collect_as_set_keeps_call_phase_metadata():
    values = [hl.Call([0, 1]), hl.Call([0, 1]), hl.Call([0, 1], phased=True), None]
    table = hl.Table.parallelize([{'x': value} for value in values],
                                schema=hl.tstruct(x=hl.tcall), n_partitions=3)
    assert table.aggregate(hl.agg.collect_as_set(table.x)) == {values[0], values[2], None}


def test_empty_struct_values_keep_cardinality_across_aggregate_merges():
    table = hl.range_table(5, n_partitions=3)
    result = table.aggregate(hl.struct(s=hl.agg.collect_as_set(hl.struct()),
                                      u=hl.agg.take(hl.struct(), 3),
                                      o=hl.agg.take(hl.struct(), 3, -table.idx)))
    assert result.s == {hl.Struct()}
    assert result.u == result.o == [hl.Struct()] * 3
