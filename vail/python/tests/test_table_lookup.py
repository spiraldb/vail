import json
import pytest
import vail as hl


@pytest.mark.parametrize('n_partitions', [1, 2, 4])
@pytest.mark.parametrize('n_rows', [0, 1, 3, 17])
def test_lookup_from_shared_scan_derived_key(n_rows, n_partitions):
    source = hl.range_table(n_rows, n_partitions).key_by()
    source = source.annotate(ordinal=hl.scan.count(), prefix=hl.scan.sum(source.idx))
    target = source.key_by('ordinal')
    result = source.annotate(match=target[source.ordinal])
    expected = [
        (i, i, i * (i - 1) // 2, hl.Struct(idx=i, prefix=i * (i - 1) // 2))
        for i in range(n_rows)
    ]
    # Each action must replay both branches without sharing mutable scan state.
    for _ in range(2):
        assert [(row.idx, row.ordinal, row.prefix, row.match) for row in result.collect()] == expected
    assert result.n_partitions() == source.n_partitions()


@pytest.mark.parametrize('n_partitions', [1, 2, 4])
def test_lookup_from_filtered_scan_target_keeps_ordinals(n_partitions):
    source = hl.range_table(9, n_partitions).key_by()
    source = source.annotate(ordinal=hl.scan.count())
    target = source.filter(source.idx % 2 == 0).key_by('ordinal')
    result = source.annotate(match=target[source.ordinal])
    assert [(row.ordinal, row.match) for row in result.collect()] == [
        (i, hl.Struct(idx=i) if i % 2 == 0 else None) for i in range(9)
    ]


def test_lookup_from_scan_over_union_preserves_every_branch():
    base = hl.range_table(3, 2)
    source = base.union(base.annotate(x=1), unify=True).key_by()
    source = source.annotate(ordinal=hl.scan.count())
    target = source.key_by('ordinal')
    result = source.annotate(match=target[source.ordinal])
    expected = [
        (i, ordinal, hl.Struct(idx=i, x=x))
        for ordinal, (i, x) in enumerate((i, x) for i in range(3) for x in (None, 1))
    ]
    for _ in range(2):
        assert [(r.idx, r.ordinal, r.match) for r in result.collect()] == expected


def people(rows=None):
    return hl.Table.parallelize(
        rows if rows is not None else [{'person_id': 1, 'age': 30}, {'person_id': 2, 'age': None}],
        schema=hl.tstruct(person_id=hl.tint32, age=hl.tint32), key='person_id',
    )


def visits():
    return hl.Table.parallelize(
        [{'person_id': person_id, 'visit': i} for i, person_id in enumerate([2, 1, 9, None, 1])],
        schema=hl.tstruct(person_id=hl.tint32, visit=hl.tint32), n_partitions=3,
    )


def test_lookup_example_missing_rows_and_missing_values():
    target, source = people(), visits()
    lookup = target[source.person_id]
    assert lookup.dtype == hl.tstruct(age=hl.tint32)
    result = source.annotate(age=lookup.age, person=lookup, absent=hl.is_missing(lookup))
    assert [(row.age, row.person, row.absent) for row in result.collect()] == [
        (None, hl.Struct(age=None), False), (30, hl.Struct(age=30), False),
        (None, None, True), (None, None, True), (30, hl.Struct(age=30), False),
    ]
    assert result.n_partitions() == source.n_partitions()
    assert result.count() == source.count()
    assert set(result.row.dtype) == {'person_id', 'visit', 'age', 'person', 'absent'}
    assert json.loads(result._native.ir_json())['child']['node'] == 'TableLookup'


def test_duplicate_target_keys_never_multiply_source_rows():
    target = people([{'person_id': 1, 'age': 30}, {'person_id': 1, 'age': 31}, {'person_id': None, 'age': 90}])
    source = visits()
    rows = source.annotate(person=target[source.person_id]).collect()
    assert len(rows) == 5
    assert [row.visit for row in rows] == list(range(5))
    assert rows[1].person.age in (30, 31)
    assert rows[4].person.age in (30, 31)
    assert rows[3].person is None


def test_composite_lookup_tuple_struct_and_index_method():
    target = hl.Table.parallelize(
        [{'a': 1, 'b': 'x', 'value': 7}, {'a': 2, 'b': None, 'value': 8}],
        schema=hl.tstruct(a=hl.tint32, b=hl.tstr, value=hl.tint32), key=('a', 'b'),
    )
    source = hl.Table.parallelize(
        [{'x': 1, 'y': 'x'}, {'x': 2, 'y': None}], schema=hl.tstruct(x=hl.tint32, y=hl.tstr),
    )
    for expression in (
        target[source.x, source.y], target.index(source.x, source.y),
        target[hl.struct(first=source.x, second=source.y)],
        target[hl.as_expr((source.x, source.y))],
    ):
        assert [row.match for row in source.annotate(match=expression).collect()] == [hl.Struct(value=7), None]


def test_lookup_in_projection_filter_computed_keys_and_aggregation():
    target, source = people(), visits()
    match = target[source.person_id]
    assert [row.age for row in source.select(age=match.age).collect()] == [None, 30, None, None, 30]
    assert [row.visit for row in source.filter(match.age > 20).collect()] == [1, 4]
    assert set(source.filter(match.age > 20).row.dtype) == set(source.row.dtype)
    assert source.aggregate(hl.agg.sum(match.age)) == 60
    grouped = source.group_by(age=match.age).aggregate(n=hl.agg.count()).collect()
    assert {(row.age, row.n) for row in grouped} == {(30, 2), (None, 3)}
    assert [row.age for row in source.key_by(age=match.age).collect()] == [30, 30, None, None, None]
    assert [row.visit for row in source.order_by(match.age).collect()] == [1, 4, 0, 2, 3]


def test_nested_lookups_and_captured_globals():
    first = hl.Table.parallelize([{'id': 1, 'other': 10}], key='id')
    second = hl.Table.parallelize([{'id': 11, 'value': 42}], key='id').annotate_globals(offset=1)
    source = hl.range_table(2).annotate_globals(bias=1)
    result = source.annotate(value=second[first[source.idx + source.bias].other + second.index_globals().offset].value)
    assert [row.value for row in result.collect()] == [42, None]
    assert hl.eval(result.bias) == 1
    assert set(result.globals.dtype) == {'bias'}


@pytest.mark.parametrize('key_type, values', [
    (hl.tstruct(x=hl.tint32), [hl.Struct(x=None), hl.Struct(x=1), None]),
    (hl.tarray(hl.tint32), [[1, None], [], None]),
    (hl.tset(hl.tint32), [{1, 2}, set(), None]),
    (hl.tdict(hl.tstr, hl.tint32), [{'x': None}, {}, None]),
    (hl.ttuple(hl.tint32, hl.tstr), [(1, None), (2, 'x'), None]),
    (hl.tinterval(hl.tint32), [hl.Interval(1, 4), hl.Interval(5, 6), None]),
    (hl.tcall, [hl.Call((0, 1)), hl.Call((1, 1)), None]),
    (hl.tlocus('GRCh37'), [hl.Locus('1', 1, hl.get_reference('GRCh37')), hl.Locus('2', 2, hl.get_reference('GRCh37')), None]),
])
def test_lookup_uses_shared_nested_key_equality(key_type, values):
    target = hl.Table.parallelize(
        [{'key': key, 'value': i} for i, key in enumerate(values)],
        schema=hl.tstruct(key=key_type, value=hl.tint32), key='key',
    )
    source = hl.Table.parallelize([{'probe': key} for key in values], schema=hl.tstruct(probe=key_type))
    assert [row.value for row in source.annotate(value=target[source.probe].value).collect()] == [0, 1, None]


def test_lookup_empty_target_and_key_only_target():
    source = visits()
    assert all(row.person is None for row in source.annotate(person=people([])[source.person_id]).collect())
    target = people().select()
    rows = source.annotate(person=target[source.person_id]).collect()
    assert [hl.Struct() if row.person is not None else None for row in rows] == [hl.Struct(), hl.Struct(), None, None, hl.Struct()]


def test_lookup_float_keys_use_ieee_total_order_identity():
    import struct

    nan = struct.unpack('>d', bytes.fromhex('7ff8000000000001'))[0]
    other_nan = struct.unpack('>d', bytes.fromhex('7ff8000000000002'))[0]
    keys = [0.0, -0.0, nan, other_nan]
    target = hl.Table.parallelize(
        [{'key': key, 'value': i} for i, key in enumerate(keys)],
        schema=hl.tstruct(key=hl.tfloat64, value=hl.tint32), key='key',
    )
    source = hl.Table.parallelize([{'probe': key} for key in keys], schema=hl.tstruct(probe=hl.tfloat64))
    assert [row.value for row in source.annotate(value=target[source.probe].value).collect()] == [0, 1, 2, 3]


def test_dictionary_lookup_matches_independently_of_entry_order():
    dtype = hl.tdict(hl.tstr, hl.tint32)
    target = hl.Table.parallelize([{'key': {'a': 1, 'b': 2}, 'value': 3}],
                                  schema=hl.tstruct(key=dtype, value=hl.tint32), key='key')
    source = hl.Table.parallelize([{'probe': {'b': 2, 'a': 1}}], schema=hl.tstruct(probe=dtype))
    assert source.annotate(value=target[source.probe].value).collect()[0].value == 3


def test_lookup_reads_multiple_files_lazily_and_writes_without_private_fields(tmp_path):
    target = people([{'person_id': i, 'age': i + 20} for i in range(9)]).repartition(3)
    path = tmp_path / 'people'
    target.write(path)
    files = list(path.rglob('*.vortex'))
    assert len(files) >= 2
    loaded = hl.read_table(path)
    source = visits()
    moved = files[0].with_suffix('.hidden')
    files[0].rename(moved)
    try:
        result = source.annotate(age=loaded[source.person_id].age)
        assert result.n_partitions() == source.n_partitions()
        with pytest.raises((ValueError, RuntimeError, FileNotFoundError)):
            result.collect()
    finally:
        moved.rename(files[0])
    assert [row.age for row in result.collect()] == [22, 21, None, None, 21]
    output = tmp_path / 'visits'
    result.write(output)
    restored = hl.read_table(output)
    assert restored.row.dtype == result.row.dtype
    assert restored.collect() == result.collect()


def test_lookup_rejects_invalid_keys_and_scalar_indexing():
    target, source = people(), visits()
    with pytest.raises(hl.ExpressionException):
        target[source.person_id, source.visit]
    with pytest.raises(hl.ExpressionException):
        target[hl.str(source.person_id)]
    with pytest.raises(hl.ExpressionException, match='scalar'):
        target[hl.literal(1)]
    with pytest.raises(ValueError, match='unkeyed'):
        visits()[source.person_id]
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        source.annotate(age=target[visits().person_id].age)


def test_interval_lookup_overlaps_boundaries_and_missing_probes():
    points = hl.Table.parallelize(
        [{'point': p} for p in [0, 1, 2, 3, 4, 5, None]],
        schema=hl.tstruct(point=hl.tint32), n_partitions=3,
    )
    regions = hl.Table.parallelize([
        {'region': hl.Interval(1, 4, True, False), 'label': 'a'},
        {'region': hl.Interval(2, 5, False, True), 'label': 'b'},
    ], key='region', n_partitions=2)
    single = points.annotate(hit=regions[points.point])
    rows = single.collect()
    assert len(rows) == 7
    assert single.n_partitions() == 3
    assert [r.hit is None for r in rows] == [True, False, False, False, False, False, True]
    assert rows[3].hit.label in ('a', 'b')
    multiple = points.annotate(hits=regions.index(points.point, all_matches=True))
    assert multiple.n_partitions() == 3
    assert [[h.label for h in r.hits] for r in multiple.collect()] == [
        [], ['a'], ['a'], ['a', 'b'], ['b'], ['b'], [],
    ]


def test_interval_lookup_accepts_struct_valued_points():
    points = hl.range_table(5)
    intervals = hl.range_table(1).key_by(
        interval=hl.interval(hl.struct(x=hl.literal(1)), hl.struct(x=hl.literal(3)))
    )
    result = points.annotate(hit=intervals.index(hl.struct(x=points.idx)))
    assert [None if row.hit is None else row.hit.idx for row in result.collect()] == [None, 0, 0, None, None]


def test_product_lookup_key_only_payload_empty_right_and_duplicates():
    points = hl.range_table(3, n_partitions=2)
    keys = hl.Table.parallelize([{'k': 1}, {'k': 1}], key='k')
    result = points.annotate(hits=keys.index(points.idx, all_matches=True))
    assert [len(r.hits) for r in result.collect()] == [0, 2, 0]
    empty = keys.filter(False)
    result = points.annotate(hits=empty.index(points.idx, all_matches=True))
    assert [r.hits for r in result.collect()] == [[], [], []]


def test_interval_all_matches_preserves_empty_structs_in_left_and_right():
    table = hl.range_table(50).annotate(
        cells=hl.array([hl.struct(id=0, present=True, value=hl.struct())])
    )
    intervals = hl.range_table(25)
    intervals = intervals.key_by(interval=hl.interval(
        1 + (intervals.idx // 5) * 10 + intervals.idx % 5,
        (1 + intervals.idx // 5) * 10 - intervals.idx % 5,
    ))
    intervals = intervals.annotate(i=intervals.idx % 5, empty=hl.struct())
    table = table.annotate(matches=intervals.index(table.key, all_matches=True))
    rows = table.collect()
    assert len(rows) == 50
    for row in rows:
        assert row.cells == [hl.Struct(id=0, present=True, value=hl.Struct())]
        assert sorted(match.i for match in row.matches) == list(range(min(row.idx % 10, 10 - row.idx % 10)))
        assert all(match.empty == hl.Struct() for match in row.matches)
    dropped = table.drop('cells')
    assert dropped.all(
        hl.sorted(dropped.matches.map(lambda x: x.i))
        == hl.range(0, hl.min(dropped.idx % 10, 10 - dropped.idx % 10))
    )
    assert table.collect() == rows


@pytest.mark.parametrize('packed', ['scalar', 'struct', 'tuple'])
def test_lookup_key_shape_mismatch_raises_expression_exception(packed):
    target = hl.range_table(3)
    target = target.key_by(target.idx, j=target.idx + 1)
    source = hl.range_table(3)
    probe = source.idx
    if packed == 'struct':
        probe = hl.struct(k=source.idx)
    elif packed == 'tuple':
        probe = hl.tuple([source.idx])
    with pytest.raises(hl.ExpressionException, match='lookup key type mismatch'):
        target.index(probe)


def test_lookup_interval_and_key_type_mismatches_raise_expression_exception():
    source = hl.range_table(3)
    target = hl.range_table(3)
    with pytest.raises(hl.ExpressionException, match='lookup key type mismatch'):
        target.index(hl.str(source.idx))
    point_interval = target.key_by(k=hl.interval(target.idx, target.idx, True, True))
    with pytest.raises(hl.ExpressionException, match='lookup key type mismatch'):
        point_interval.index(hl.struct(idx=source.idx, j=source.idx + 1))
    product_interval = target.key_by(k=hl.interval(target.key, target.key, True, True))
    with pytest.raises(hl.ExpressionException, match='lookup key type mismatch'):
        product_interval.index(source.idx)
