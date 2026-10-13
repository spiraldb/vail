"""Intervals through the existing Table execution paths."""

import numpy as np
import pytest
import vail as hl


def test_interval_keys_grouping_and_multifile_storage(tmp_path):
    interval_type = hl.tinterval(hl.tint32)
    values = [
        hl.Interval(1, 3, True, False),
        hl.Interval(1, 3, False, False),
        hl.Interval(1, 3, True, True),
        hl.Interval(2, 4),
    ]
    table = hl.Table.parallelize(
        [{'region': value, 'score': i + 1} for i, value in enumerate(reversed(values))],
        schema=hl.tstruct(region=interval_type, score=hl.tint32), n_partitions=2,
        globals={'genome_region': hl.Interval(1, 10)},
    )
    ordered = table.key_by('region')
    assert [r.region for r in ordered.collect()] == [values[i] for i in (0, 2, 1, 3)]
    grouped = table.group_by('region').aggregate(n=hl.agg.count())
    assert [(r.region, r.n) for r in grouped.collect()] == [(v, 1) for v in (values[0], values[2], values[1], values[3])]
    assert ordered.union(ordered).distinct().count() == 4
    assert hl.eval(ordered.genome_region) == hl.Interval(1, 10)

    path = tmp_path / 'intervals'
    ordered.write(path)
    read = hl.read_table(path)
    assert read.row.dtype == ordered.row.dtype
    assert read._same(ordered)
    assert [r.region for r in read.collect()] == [values[i] for i in (0, 2, 1, 3)]

    expanded = read.expand_types()
    assert expanded.key.dtype == hl.tstruct()
    assert expanded.row.dtype['region'] == hl.tstruct(
        start=hl.tint32, end=hl.tint32,
        includesStart=hl.tbool, includesEnd=hl.tbool,
    )
    assert expanded.collect()[0].region == hl.Struct(
        start=1, end=3, includesStart=True, includesEnd=False
    )
    assert hl.eval(expanded.genome_region) == hl.Struct(
        start=1, end=10, includesStart=True, includesEnd=False
    )

    multifile = hl.range_table(12, n_partitions=3)
    multifile = multifile.annotate(region=hl.interval(multifile.idx, multifile.idx + 2))
    multi_path = tmp_path / 'multifile_intervals'
    multifile.write(multi_path)
    assert len(list(multi_path.rglob('*.vortex'))) == 3
    assert hl.read_table(multi_path)._same(multifile)


@pytest.mark.parametrize('point_type,start,end', [
    (hl.tbool, False, True),
    (hl.tint32, -2, 3),
    (hl.tint64, 2**40, 2**40 + 1),
    (hl.tfloat32, -1.25, 2.5),
    (hl.tfloat64, -0.0, 1.5),
    (hl.tstr, 'α', 'β'),
    (hl.tarray(hl.tint32), [1, None], [1, 2]),
    (hl.tset(hl.tint32), {1}, {1, 2}),
    (hl.ttuple(hl.tint32, hl.tstr), (1, 'a'), (2, 'b')),
    (hl.tstruct(a=hl.tint32, b=hl.tstr), {'a': 1, 'b': 'a'}, {'a': 2, 'b': 'b'}),
    (hl.tdict(hl.tstr, hl.tint32), {'a': 1}, {'b': 2}),
    (hl.tcall, hl.Call((0, 1)), hl.Call((1, 1))),
    (hl.tndarray(hl.tint32, 1), np.array([1], dtype=np.int32), np.array([2], dtype=np.int32)),
    (hl.tinterval(hl.tint32), hl.Interval(1, 2), hl.Interval(2, 3)),
])
def test_interval_point_types_roundtrip(tmp_path, point_type, start, end):
    interval_type = hl.tinterval(point_type)
    interval = hl.Interval(start, end, point_type=point_type)
    table = hl.Table.parallelize(
        [{'region': interval}, {'region': None}],
        schema=hl.tstruct(region=interval_type), n_partitions=2,
    )
    if isinstance(point_type, (hl.tset, hl.tdict)):
        with pytest.raises(NotImplementedError, match='Set/Dictionary'):
            table.key_by('region')
        table.write(tmp_path / 'unkeyed-intervals')
        assert hl.read_table(tmp_path / 'unkeyed-intervals')._same(table)
        return
    table = table.key_by('region')
    assert isinstance(table.collect()[0].region, hl.Interval)
    path = tmp_path / 'intervals'
    table.write(path)
    read = hl.read_table(path)
    assert read.row.dtype == table.row.dtype
    assert read._same(table)
    point = hl.literal(start, point_type)
    singleton = hl.interval(point, point, includes_end=True)
    assert hl.eval(singleton.contains(point)) is True
    assert hl.eval(singleton.overlaps(singleton)) is True
    assert hl.eval(singleton == singleton) is True


def test_interval_expression_projection_and_missing():
    table = hl.Table.parallelize(
        [{'region': hl.Interval(1, 3), 'label': 'a'}, {'region': None, 'label': 'missing'}],
        schema=hl.tstruct(region=hl.tinterval(hl.tint32), label=hl.tstr),
    )
    selected = table.select('region', start=table.region.start, end=table.region.end)
    assert [(r.start, r.end) for r in selected.collect()] == [(1, 3), (None, None)]
    expanded = table.expand_types()
    assert expanded.collect()[1].region == hl.Struct(
        start=None, end=None, includesStart=None, includesEnd=None
    )

    annotated = table.annotate(
        contains_two=table.region.contains(2),
        overlaps=table.region.overlaps(hl.interval(2, 4)),
    )
    assert [(r.contains_two, r.overlaps) for r in annotated.collect()] == [
        (True, True), (None, None)
    ]
    assert [r.label for r in table.filter(table.region < hl.interval(2, 4)).collect()] == ['a']
    assert hl.eval(hl.interval(1, 3).overlaps(hl.interval(3, 5))) is False
    assert hl.eval(hl.interval(1, 3, includes_end=True).overlaps(hl.interval(3, 5))) is True
    assert hl.Interval(1, 3).contains(2) is True
    assert hl.Interval(1, 3).overlaps(hl.Interval(3, 5)) is False


def test_interval_missing_endpoint_is_distinct_from_missing_interval(tmp_path):
    ty = hl.tinterval(hl.tint32)
    with_missing_start = hl.Interval(None, 3, point_type=hl.tint32)
    table = hl.Table.parallelize(
        [{'region': with_missing_start}, {'region': hl.Interval(1, 3)}, {'region': None}],
        schema=hl.tstruct(region=ty), n_partitions=2,
    ).key_by('region')
    assert [r.region for r in table.collect()] == [hl.Interval(1, 3), with_missing_start, None]
    path = tmp_path / 'nullable_endpoint'
    table.write(path)
    assert hl.read_table(path)._same(table)

    projected = table.annotate(
        contains_zero=table.region.contains(0),
        contains_two=table.region.contains(2),
    )
    assert [(r.contains_zero, r.contains_two) for r in projected.collect()] == [
        (False, True), (None, None), (None, None)
    ]


def test_interval_predicates_preserve_hail_endpoint_evaluation_order():
    missing = hl.missing(hl.tint32)
    assert hl.eval(hl.interval(missing, 3).contains(3)) is None
    assert hl.eval(hl.interval(1, missing).contains(0)) is False
    assert hl.eval(hl.interval(1, missing).contains(2)) is None
    # Hail checks left.end against right.start before left.start against right.end.
    assert hl.eval(hl.interval(10, missing).overlaps(hl.interval(0, 3))) is None
    assert hl.eval(hl.interval(missing, 1).overlaps(hl.interval(3, 5))) is False


def test_explode_interval_collection_and_nested_projection():
    values = [hl.Interval(1, 3), hl.Interval(4, 7)]
    table = hl.Table.parallelize(
        [{'nested': {'regions': values}}],
        schema=hl.tstruct(nested=hl.tstruct(regions=hl.tarray(hl.tinterval(hl.tint32)))),
    )
    table = table.explode(table.nested.regions)
    selected = table.select(region=table.nested.regions, start=table.nested.regions.start)
    assert selected.collect() == [hl.Struct(region=value, start=value.start) for value in values]


def test_interval_arrow_layout_preserves_public_api(tmp_path):
    from vail.arrow import arrow_type, dtype_from_arrow

    interval_type = hl.tinterval(hl.tint32)
    storage = arrow_type(interval_type)
    assert [f.name for f in storage] == [
        '__vail_interval_start', '__vail_interval_start_exclusive',
        '__vail_interval_end', '__vail_interval_end_inclusive',
    ]
    assert dtype_from_arrow(storage) == interval_type
    values = [hl.Interval(1, 100, True, True), hl.Interval(1, 2, False, False), None]
    table = hl.Table.parallelize(
        [{'region': v} for v in values], schema=hl.tstruct(region=interval_type), n_partitions=2,
    ).key_by('region')
    projected = table.annotate(
        start=table.region.start, end=table.region.end,
        includes_start=table.region.includes_start, includes_end=table.region.includes_end,
        text=hl.str(table.region),
    )
    rows = projected.collect()
    assert [r.region for r in rows] == values
    assert [(r.start, r.end, r.includes_start, r.includes_end) for r in rows] == [
        (1, 100, True, True), (1, 2, False, False), (None, None, None, None),
    ]
    assert [r.text for r in rows[:2]] == ['[1-100]', '(1-2)']
    path = tmp_path / 'new_layout'
    projected.write(path)
    assert hl.read_table(path)._same(projected)
    assert hl.eval(hl.interval(1, 2, includes_start=False).includes_start) is False
    assert hl.eval(hl.interval(1, 2, includes_end=True).includes_end) is True
