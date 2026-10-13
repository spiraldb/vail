import math
import struct

import pytest

import vail as hl


def test_distinct_uses_native_group_equality_for_float_edges():
    nan_payload = struct.unpack(">d", bytes.fromhex("7ff8000000000001"))[0]
    table = hl.Table.parallelize(
        [{"k": value, "payload": i} for i, value in enumerate(
            [-0.0, 0.0, float("nan"), nan_payload, None, None]
        )],
        schema=hl.tstruct(k=hl.tfloat64, payload=hl.tint32),
        key="k",
        n_partitions=3,
    )
    values = [row.k for row in table.distinct().collect()]
    assert len(values) == 4
    assert values[0] == 0.0
    assert [struct.pack('>d', value).hex() for value in values[1:3]] == [
        '7ff8000000000000', '7ff8000000000001',
    ]
    assert values[3] is None


@pytest.mark.parametrize("partitions", [1, 2, 4])
def test_distinct_preserves_empty_struct_payload(partitions):
    table = hl.range_table(4, partitions).annotate(value=hl.struct())
    table = table.key_by(k=table.idx // 2)
    result = table.distinct()
    assert result.count() == 2
    rows = result.collect()
    assert [row.k for row in rows] == [0, 1]
    assert all(row.value == hl.Struct() for row in rows)
    assert all(row.idx // 2 == row.k for row in rows)
    assert result.collect() == rows


def test_distinct_preserves_nested_and_missing_empty_structs():
    table = hl.Table.parallelize(
        [hl.Struct(k=0, value=hl.Struct(inner=hl.Struct())),
         hl.Struct(k=0, value=None),
         hl.Struct(k=1, value=None)],
        schema=hl.tstruct(k=hl.tint32, value=hl.tstruct(inner=hl.tstruct())),
        key="k",
    )
    rows = table.distinct().collect()
    assert len(rows) == 2
    assert rows[0] == hl.Struct(k=0, value=hl.Struct(inner=hl.Struct()))
    assert rows[1] == hl.Struct(k=1, value=None)
