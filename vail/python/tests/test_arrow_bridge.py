import struct

import pytest

import vail as hl
from vail.arrow import arrow_type, decode_rows, rows_to_ipc


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
    ipc = rows_to_ipc([{}, {}, {}], schema)
    assert len(decode_rows(ipc, schema)) == 3
    assert arrow_type(hl.tfloat32).bit_width == 32


def test_from_arrow_table_is_accepted():
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch([pa.array([1, None], type=pa.int32())], names=["x"])
    table = hl.Table.parallelize(batch, n_partitions=2)
    assert table.collect() == [hl.Struct(x=1), hl.Struct(x=None)]
