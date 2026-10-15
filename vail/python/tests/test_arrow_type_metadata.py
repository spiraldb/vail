import pyarrow as pa
import pytest

import vail as hl
from vail.arrow import TYPE_DESCRIPTOR, TYPE_VERSION, arrow_type, as_arrow, dtype_from_arrow, read_arrow


def untagged(ty):
    if pa.types.is_struct(ty):
        return pa.struct([pa.field(f.name, untagged(f.type), nullable=f.nullable) for f in ty])
    if pa.types.is_list(ty):
        return pa.list_(pa.field(ty.value_field.name, untagged(ty.value_type)))
    return ty


@pytest.mark.parametrize('dtype,value,ordinary', [
    (hl.ttuple(hl.tint32), {'__vail_tuple_0': 1}, hl.tstruct(__vail_tuple_0=hl.tint32)),
    (hl.ttuple(), {'__vail_tuple_empty': False}, hl.tstruct(__vail_tuple_empty=hl.tbool)),
    (hl.tlocus('GRCh37'), {'__vail_locus_ordinal': 0, '__vail_locus_contig': '1', '__vail_locus_position': 2},
     hl.tstruct(__vail_locus_ordinal=hl.tint32, __vail_locus_contig=hl.tstr, __vail_locus_position=hl.tint32)),
    (hl.tcall, {'__vail_call_word': 36}, hl.tstruct(__vail_call_word=hl.tint32)),
    (hl.tinterval(hl.tint32), {'__vail_interval_start': 1, '__vail_interval_start_exclusive': False,
                            '__vail_interval_end': 3, '__vail_interval_end_inclusive': False},
     hl.tstruct(__vail_interval_start=hl.tint32, __vail_interval_start_exclusive=hl.tbool,
                __vail_interval_end=hl.tint32, __vail_interval_end_inclusive=hl.tbool)),
    (hl.tndarray(hl.tint32, 1), {'__vail_ndarray_data': [1, 2], '__vail_ndarray_shape': [2]},
     hl.tstruct(__vail_ndarray_data=hl.tarray(hl.tint32), __vail_ndarray_shape=hl.tarray(hl.tint64))),
    (hl.tset(hl.tint32), [2, 1, 2], hl.tarray(hl.tint32)),
    (hl.tdict(hl.tint32, hl.tstr), [{'key': 1, 'value': 'a'}],
     hl.tarray(hl.tstruct(key=hl.tint32, value=hl.tstr))),
])
def test_untagged_sentinel_names_are_ordinary_arrow_types(dtype, value, ordinary, tmp_path):
    raw = pa.table({'x': pa.array([value], type=untagged(arrow_type(dtype)))})
    for schema in (None, hl.tstruct(x=ordinary)):
        table = hl.Table.parallelize(raw, schema=schema)
        assert table.x.dtype == ordinary
        # Exercise both native inference and comparison rather than just the Python codec.
        assert table.key_by('x').distinct().count() == 1
        expected = hl.Table.parallelize([{'x': value}], schema=hl.tstruct(x=ordinary))
        assert table._same(expected)
    path = tmp_path / 'ordinary'
    table.write(path)
    reopened = hl.read_table(path)
    assert reopened.x.dtype == ordinary
    assert reopened._same(table)


@pytest.mark.parametrize('dtype,value', [
    (hl.ttuple(), ()),
    (hl.ttuple(hl.tint32, hl.tstr), (2, 'a')),
    (hl.tarray(hl.ttuple(hl.tint32)), [(1,), None, (2,)]),
    (hl.tset(hl.ttuple(hl.tint32)), {(1,), (2,)}),
    (hl.tdict(hl.ttuple(hl.tint32), hl.tarray(hl.ttuple(hl.tint32))), {(1,): [(2,)]}),
    (hl.tcall, hl.Call((1, 2), True)),
    (hl.tndarray(hl.tint32, 2), {'__vail_ndarray_data': [1, 2, 3, 4], '__vail_ndarray_shape': [2, 2]}),
    (hl.tinterval(hl.tint32), hl.Interval(1, 3)),
    (hl.tlocus('GRCh37'), hl.Locus('1', 2, hl.get_reference('GRCh37'))),
])
def test_tagged_types_survive_execution_export_and_storage(dtype, value, tmp_path):
    table = hl.Table.parallelize([{'x': value}, {'x': None}], schema=hl.tstruct(x=dtype), n_partitions=2)
    table = table.select(x=table.x)
    schema, exported = read_arrow(table._native.collect_arrow())
    assert dtype_from_arrow(schema.field('x').type) == dtype
    imported = hl.Table.parallelize(exported)
    assert imported.x.dtype == dtype
    assert imported._same(table)
    path = tmp_path / 'tagged'
    table.write(path)
    loaded = hl.read_table(path)
    assert loaded.x.dtype == dtype
    assert loaded._same(table)


def test_computed_tuples_and_nested_sets_restore_metadata():
    table = hl.range_table(3, 2).key_by()
    table = table.select(x=hl.tuple([table.idx, hl.set([table.idx])]))
    schema, exported = read_arrow(table._native.collect_arrow())
    expected = hl.ttuple(hl.tint32, hl.tset(hl.tint32))
    assert dtype_from_arrow(schema.field('x').type) == expected
    assert hl.Table.parallelize(exported)._same(table)


def test_explicit_tuple_schema_can_type_an_untagged_physical_struct():
    ty = hl.ttuple(hl.tint32)
    raw = pa.table({'x': pa.array([{'__vail_tuple_0': 7}], type=untagged(arrow_type(ty)))})
    table = hl.Table.parallelize(raw, schema=hl.tstruct(x=ty))
    assert table.collect() == [hl.Struct(x=(7,))]


@pytest.mark.parametrize('metadata,match', [
    ({TYPE_VERSION: b'2', TYPE_DESCRIPTOR: b'{"tuple":["int32"]}'}, 'version'),
    ({TYPE_VERSION: b'1'}, 'incomplete'),
    ({TYPE_VERSION: b'1', TYPE_DESCRIPTOR: b'not-json'}, 'invalid'),
    ({TYPE_VERSION: b'1', TYPE_DESCRIPTOR: b'{"tuple":["str"]}'}, 'layout'),
])
def test_invalid_metadata_rejected_by_python_and_native_arrow_inputs(metadata, match):
    ty = pa.struct([pa.field('__vail_tuple_0', pa.int32(), metadata=metadata)])
    raw = pa.table({'x': pa.array([{'__vail_tuple_0': 7}], type=ty)})
    with pytest.raises(TypeError, match=match):
        hl.Table.parallelize(raw)
    # Bypass frontend validation to verify Rust enforces the same contract.
    from vail import _native_session
    with pytest.raises(ValueError, match=match):
        _native_session().parallelize_arrow(as_arrow(raw), 1)


def test_local_global_collections_keep_metadata_when_broadcast():
    table = hl.range_table(3).annotate_globals(members={1, 2}, mapping={'a': (7,)})
    table = table.annotate(copied=table.members, mapped=table.mapping)
    assert [(r.copied, r.mapped) for r in table.collect()] == [({1, 2}, {'a': (7,)})] * 3
    assert table._same(table)
    _, exported = read_arrow(table._native.collect_arrow())
    # The Arrow input does not carry table globals; restore them explicitly.
    assert hl.Table.parallelize(exported, key='idx').annotate_globals(
        members={1, 2}, mapping={'a': (7,)})._same(table)


def test_colliding_struct_names_work_as_join_and_lookup_keys():
    ordinary = hl.tstruct(__vail_tuple_0=hl.tint32)
    raw = pa.table({'x': pa.array([{'__vail_tuple_0': 1}, {'__vail_tuple_0': 2}],
                                 type=arrow_type(ordinary))})
    left = hl.Table.parallelize(raw, key='x')
    right = hl.Table.parallelize([{'x': {'__vail_tuple_0': 1}, 'value': 5}],
                                 schema=hl.tstruct(x=ordinary, value=hl.tint32), key='x')
    assert [r.value for r in left.join(right, how='left').collect()] == [5, None]
    assert [r.hit for r in left.annotate(hit=right[left.x]).collect()] == [hl.Struct(value=5), None]


def test_unknown_nested_metadata_version_is_rejected():
    ty = arrow_type(hl.ttuple(hl.tset(hl.tint32)))
    nested = ty.field(0).type
    metadata = dict(nested.value_field.metadata)
    metadata[TYPE_VERSION] = b'2'
    nested = pa.list_(nested.value_field.with_metadata(metadata))
    ty = pa.struct([ty.field(0).with_type(nested)])
    raw = pa.table({'x': pa.array([{'__vail_tuple_0': [1]}], type=ty)})
    with pytest.raises(TypeError, match='version'):
        hl.Table.parallelize(raw)
    from vail import _native_session
    with pytest.raises(ValueError, match='version'):
        _native_session().parallelize_arrow(as_arrow(raw), 1)
