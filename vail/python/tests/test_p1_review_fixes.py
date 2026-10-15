"""Regression tests for data validation and variance accuracy review findings."""
import math
import statistics

import pytest
import vail as hl
from vail.arrow import rows_to_arrow


@pytest.mark.parametrize('value', [(), (1,), (1, 'x', 3), [1, 'x'], {'a': 1}])
def test_parallelize_rejects_tuple_arity_or_representation_mismatch(value):
    schema = hl.tstruct(x=hl.ttuple(hl.tint32, hl.tstr))
    with pytest.raises(TypeError, match='tuple'):
        hl.Table.parallelize([{'x': value}], schema=schema)


@pytest.mark.parametrize('value', [(), (1,), (1, 'x', 3), [1, 'x']])
def test_arrow_bridge_and_literals_cannot_truncate_or_pad_tuples(value):
    dtype = hl.ttuple(hl.tint32, hl.tstr)
    with pytest.raises(TypeError, match='tuple'):
        rows_to_arrow([{'x': value}], hl.tstruct(x=dtype))
    with pytest.raises(TypeError, match='tuple'):
        hl.literal(value, dtype)


def test_empty_and_missing_tuples_round_trip_without_losing_members():
    schema = hl.tstruct(x=hl.ttuple(hl.tint32, hl.tstr), e=hl.ttuple())
    rows = [{'x': (None, 'x'), 'e': ()}, {'x': None, 'e': None}]
    assert hl.Table.parallelize(rows, schema=schema).collect() == [hl.Struct(**row) for row in rows]
    with pytest.raises(TypeError, match='0 elements'):
        hl.Table.parallelize([{'e': (1,)}], schema=hl.tstruct(e=hl.ttuple()))


@pytest.mark.parametrize('dtype,value', [
    (hl.tarray(hl.ttuple(hl.tint32)), [(1, 2)]),
    (hl.tset(hl.ttuple(hl.tint32)), {(1, 2)}),
    (hl.tdict(hl.ttuple(hl.tint32), hl.tint32), {(1, 2): 7}),
    (hl.tdict(hl.tstr, hl.ttuple(hl.tint32)), {'a': (1, 2)}),
    (hl.ttuple(hl.tstruct(x=hl.ttuple(hl.tint32))), ({'x': ()},)),
])
def test_nested_tuple_widths_are_validated(dtype, value):
    with pytest.raises(TypeError, match='tuple'):
        hl.Table.parallelize([{'x': value}], schema=hl.tstruct(x=dtype))


def test_tuple_members_are_recursively_type_checked():
    dtype = hl.ttuple(hl.tint32, hl.tdict(hl.tstr, hl.ttuple(hl.tint32)))
    with pytest.raises(TypeError, match='int32'):
        hl.Table.parallelize([{'x': (1, {'a': ('wrong',)})}], schema=hl.tstruct(x=dtype))
    with pytest.raises(OverflowError, match='int32'):
        hl.Table.parallelize([{'x': (2**31, {})}], schema=hl.tstruct(x=dtype))


@pytest.mark.parametrize('partitions', [1, 3, 7])
@pytest.mark.parametrize('base,step', [(1e12, 1.0), (1e16, 2.0), (-1e16, 2.0)])
def test_total_grouped_and_scan_stats_use_stable_population_variance(base, step, partitions):
    values = [base + step * i for i in range(4)]
    table = hl.Table.parallelize(
        [{'i': i, 'g': 0, 'x': value} for i, value in enumerate(values)],
        schema=hl.tstruct(i=hl.tint32, g=hl.tint32, x=hl.tfloat64), n_partitions=partitions)
    expected = statistics.pstdev(values)
    assert table.aggregate(hl.agg.stats(table.x)).stdev == pytest.approx(expected, rel=1e-12)
    grouped = table.group_by(table.g).aggregate(stats=hl.agg.stats(table.x)).collect()
    assert len(grouped) == 1
    assert grouped[0].stats.stdev == pytest.approx(expected, rel=1e-12)
    rows = table.annotate(stats=hl.scan.stats(table.x)).collect()
    assert math.isnan(rows[0].stats.stdev)
    for i, row in enumerate(rows[1:], 1):
        assert row.stats.stdev == pytest.approx(statistics.pstdev(values[:i]), rel=1e-12)


@pytest.mark.parametrize('values', [[], [None, None], [None, 7.0], [float('nan')], [float('inf')]])
def test_stats_keep_empty_missing_and_nonfinite_behavior(values):
    table = hl.Table.parallelize([{'x': value} for value in values],
                                schema=hl.tstruct(x=hl.tfloat64), n_partitions=3)
    stdev = table.aggregate(hl.agg.stats(table.x)).stdev
    if values == [None, 7.0]:
        assert stdev == 0.0
    else:
        assert math.isnan(stdev)
