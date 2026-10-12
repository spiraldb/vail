import math
import struct

import vail as hl


def test_distinct_uses_ieee_total_key_equality_for_float_edges():
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
    assert len(values) == 5
    assert math.copysign(1.0, values[0]) == -1.0
    assert math.copysign(1.0, values[1]) == 1.0
    assert [struct.pack('>d', value).hex() for value in values[2:4]] == [
        '7ff8000000000000', '7ff8000000000001',
    ]
    assert values[4] is None
