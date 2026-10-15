import struct

import pytest

import vail as hl
from vail.arrow import arrow_type, decode_rows, rows_to_arrow


def _float_bits(value, width):
    return struct.pack(">f" if width == 32 else ">d", value)


def test_arrow_parallelize_collect_preserves_primitives_nulls_and_float_bits():
    nan = struct.unpack(">d", bytes.fromhex("7ff8000000000042"))[0]
    schema = hl.tstruct(i=hl.tint64, x=hl.tfloat64, s=hl.tstr, b=hl.tbool)
    rows = [
        {"i": 2**40, "x": -0.0, "s": "x", "b": True},
        {"i": None, "x": nan, "s": None, "b": None},
    ]
    table = hl.Table.parallelize(rows, schema=schema, n_partitions=2)
    actual = table.collect()
    assert actual[0].i == 2**40
    assert _float_bits(actual[0].x, 64) == _float_bits(-0.0, 64)
    assert actual[1].i is None and actual[1].s is None and actual[1].b is None
    assert _float_bits(actual[1].x, 64) == _float_bits(nan, 64)


def test_arrow_nested_physical_types_round_trip():
    schema = hl.tstruct(
        nested=hl.tstruct(name=hl.tstr, values=hl.tarray(hl.tint32)),
        tupled=hl.ttuple(hl.tint32, hl.tstr),
        call=hl.tcall,
        interval=hl.tinterval(hl.tint32),
        set=hl.tset(hl.tint32),
        dict=hl.tdict(hl.tstr, hl.tint32),
        array=hl.tarray(hl.tarray(hl.tint32)),
    )
    rows = [{
        "nested": {"name": "n", "values": [1, None]},
        "tupled": (2, "t"), "call": hl.Call((1, 2), True),
        "interval": hl.Interval(1, 4, True, False), "set": {4, 2},
        "dict": {"b": 2, "a": 1}, "array": [[1, 2], None],
    }, {"nested": None, "tupled": None, "call": None, "interval": None,
        "set": None, "dict": None, "array": None}]
    table = hl.Table.parallelize(rows, schema=schema)
    assert table.collect() == [hl.Struct(**rows[0]), hl.Struct(**rows[1])]


def test_arrow_helper_supports_zero_field_rows_and_type_mapping():
    schema = hl.tstruct()
    ipc = rows_to_arrow([{}, {}, {}], schema)
    assert len(decode_rows(ipc, schema)) == 3
    assert arrow_type(hl.tfloat32).bit_width == 32


def test_from_arrow_table_is_accepted():
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch([pa.array([1, None], type=pa.int32())], names=["x"])
    table = hl.Table.parallelize(batch, n_partitions=2)
    assert table.collect() == [hl.Struct(x=1), hl.Struct(x=None)]


@pytest.mark.parametrize('make_member,expected', [
    (lambda: hl.tuple([hl.array([1, 2])]), ((1, 2),)),
    (lambda: hl.tuple([hl.set([1, 2])]), (frozenset({1, 2}),)),
    (lambda: hl.tuple([hl.dict([(1, 'a')])]), (frozenset({(1, 'a')}),)),
    (lambda: hl.tuple([]), ()),
    (lambda: hl.missing(hl.ttuple(hl.tarray(hl.tint32))), None),
    (lambda: hl.tuple([hl.missing(hl.tarray(hl.tint32))]), (None,)),
])
def test_tuple_collection_keys_localize_recursively_hashable_values(make_member, expected):
    import pyarrow as pa
    member = make_member()
    # Export one ordinary tuple value, then wrap that physical Arrow value as
    # collection data. This exercises supported Arrow import/localization even
    # for types whose expression-level distinct/key operations are unsupported.
    source = rows_to_arrow([{'member': hl.eval(member)}], hl.tstruct(member=member.dtype))
    physical = source['member'].to_pylist()[0]
    set_type, dict_type = hl.tset(member.dtype), hl.tdict(member.dtype, hl.tint32)
    raw = pa.table({
        'members': pa.array([[physical]], type=arrow_type(set_type)),
        'mapping': pa.array([[{'key': physical, 'value': 7}]], type=arrow_type(dict_type)),
    })
    schema = hl.tstruct(members=set_type, mapping=dict_type)
    expected_rows = [hl.Struct(members={expected}, mapping={expected: 7})]
    assert decode_rows(raw, schema) == expected_rows
    assert hl.Table.parallelize(raw).collect() == expected_rows


def test_parallelize_tuple_array_set_members_roundtrip():
    schema = hl.tstruct(x=hl.tset(hl.ttuple(hl.tarray(hl.tint32))))
    rows = [{'x': {((1, 2),)}}, {'x': None}]
    assert hl.Table.parallelize(rows, schema=schema).collect() == [hl.Struct(**row) for row in rows]


def test_nested_missing_and_empty_values_survive_prepared_converters():
    schema = hl.tstruct(
        x=hl.tstruct(a=hl.tarray(hl.ttuple(hl.tint32, hl.tset(hl.tint32)))),
        empty=hl.tstruct(),
    )
    rows = [
        {'x': {'a': [(1, {2, 3}), None, (None, set())]}, 'empty': {}},
        {'x': None, 'empty': None},
        {'x': {'a': []}, 'empty': {}},
    ]
    assert decode_rows(rows_to_arrow(rows, schema), schema) == [
        hl.Struct(x=hl.Struct(a=[(1, {2, 3}), None, (None, set())]), empty=hl.Struct()),
        hl.Struct(x=None, empty=None),
        hl.Struct(x=hl.Struct(a=[]), empty=hl.Struct()),
    ]


def test_empty_struct_row_generator_preserves_cardinality():
    schema = hl.tstruct()
    assert len(decode_rows(rows_to_arrow(({} for _ in range(3)), schema), schema)) == 3


@pytest.mark.parametrize('wrap', [False, True])
def test_interval_collection_keys_preserve_hashable_array_endpoints(wrap):
    point = hl.tarray(hl.tint32)
    interval_type = hl.tinterval(point)
    interval = hl.Interval((1,), (2,), point_type=point)
    key_type = hl.ttuple(interval_type) if wrap else interval_type
    key = (interval,) if wrap else interval
    schema = hl.tstruct(members=hl.tset(key_type), mapping=hl.tdict(key_type, hl.tint32))
    rows = [{'members': {key}, 'mapping': {key: 7}}]
    assert decode_rows(rows_to_arrow(rows, schema), schema) == [hl.Struct(**rows[0])]
    assert hl.Table.parallelize(rows, schema=schema).collect() == [hl.Struct(**rows[0])]
