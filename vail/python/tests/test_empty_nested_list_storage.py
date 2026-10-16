"""Persist nested collections even when an element stream contains no rows."""

import pyarrow as pa
import pytest
import vail as hl

from vail.arrow import arrow_type


@pytest.mark.parametrize('dtype,values', [
    (hl.tarray(hl.tarray(hl.tint32)), [None, [], [None], [[]], [[], None]]),
    (hl.tdict(hl.tint32, hl.tarray(hl.tint32)), [None, {}, {1: []}, {2: None}]),
    (hl.tdict(hl.ttuple(hl.tint32), hl.tarray(hl.ttuple(hl.tint32))),
     [None, {}, {(1,): []}, {(2,): None}, {(3,): [(4,), None]}]),
    (hl.tdict(hl.tint32, hl.tarray(hl.tint32)), [None, None, None]),
    (hl.tdict(hl.tint32, hl.tarray(hl.tint32)), [{}, {}, {}]),
    (hl.tarray(hl.tarray(hl.tint32)), [None, None, None]),
    (hl.tarray(hl.tarray(hl.tint32)), [[], [], []]),
])
@pytest.mark.parametrize('partitions', [1, 3])
def test_empty_nested_collection_streams_round_trip(dtype, values, partitions, tmp_path):
    table = hl.Table.parallelize(
        [dict(id=i, value=value) for i, value in enumerate(values)],
        schema=hl.tstruct(id=hl.tint32, value=dtype), n_partitions=partitions)
    expected = table.to_arrow()
    table.write(tmp_path / 'collections')
    restored = hl.read_table(tmp_path / 'collections')
    assert restored.value.dtype == dtype
    assert restored.to_arrow().equals(expected, check_metadata=True)


def test_sliced_multibatch_empty_nested_lists_round_trip(tmp_path):
    dtype = hl.tarray(hl.tarray(hl.tint32))
    schema = pa.schema([pa.field('id', pa.int32()), pa.field('value', arrow_type(dtype))])
    # Nonzero slice offsets and independently empty element buffers must survive
    # batch/file boundaries, alongside a later batch containing actual values.
    batches = []
    for ids, values in [([-1, 0, 1, -2], [[[99]], [], None, [[98]]]),
                        ([-1, 2, 3, -2], [[[99]], [[]], [None], [[98]]]),
                        ([-1, 4, 5, -2], [[[99]], [[7]], [], [[98]]])]:
        batch = pa.RecordBatch.from_arrays(
            [pa.array(ids, type=pa.int32()), pa.array(values, type=arrow_type(dtype))],
            schema=schema)
        batches.append(batch.slice(1, 2))
    table = hl.Table.parallelize(pa.Table.from_batches(batches, schema=schema))
    expected = table.to_arrow()
    table.write(tmp_path / 'sliced')
    assert hl.read_table(tmp_path / 'sliced').to_arrow().equals(expected, check_metadata=True)
