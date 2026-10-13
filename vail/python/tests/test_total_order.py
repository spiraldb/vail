"""Native DataFusion execution semantics and versioned storage ordering."""
import struct

import numpy as np
import pytest
import vail as hl


def float64(bits):
    return struct.unpack('>d', struct.pack('>Q', bits))[0]


def bits64(value):
    return struct.unpack('>Q', struct.pack('>d', value))[0]


BITS = [
    0xFFF8000000000002, 0xFFF8000000000001, 0xFFF0000000000000,
    0xBFF0000000000000, 0x8000000000000000, 0,
    0x3FF0000000000000, 0x7FF0000000000000,
    0x7FF8000000000001, 0x7FF8000000000002,
]


def test_float_sort_group_scoped_join_distinct_and_storage(tmp_path):
    values = [float64(bits) for bits in BITS]
    table = hl.Table.parallelize(
        [{'k': v} for v in reversed(values)] + [{'k': values[0]}, {'k': None}],
        schema=hl.tstruct(k=hl.tfloat64), n_partitions=3,
    )
    ordered = table.key_by('k')
    rows = ordered.collect()
    assert [bits64(row.k) for row in rows[:-1]] == [BITS[0]] + BITS
    assert rows[-1].k is None
    grouped = table.group_by('k').aggregate(
        n=hl.agg.count(), scoped=hl.agg.filter(True, hl.agg.count()),
    ).collect()
    assert len(grouped) == len(BITS)
    assert sum(row.n for row in grouped) == len(values) + 2
    assert next(row.n for row in grouped if row.k == 0.0) == 2
    assert [row.scoped for row in grouped] == [row.n for row in grouped]
    assert ordered.distinct().count() == len(BITS)
    path = tmp_path / 'total-order'
    ordered.write(path)
    restored = hl.read_table(path).collect()
    assert [bits64(row.k) for row in restored[:-1]] == [BITS[0]] + BITS
    assert restored[-1].k is None


def test_native_expression_comparison_preserves_nulls():
    table = hl.Table.parallelize(
        [{'a': float64(a), 'b': float64(b)} for a, b in zip(BITS, BITS[1:])]
        + [{'a': None, 'b': 0.0}],
        schema=hl.tstruct(a=hl.tfloat64, b=hl.tfloat64),
    )
    expected_lt = [True] * (len(BITS) - 1) + [None]
    expected_eq = [False] * (len(BITS) - 1) + [None]
    expected_lt[4], expected_eq[4] = False, True
    assert (table.a < table.b).collect() == expected_lt
    assert (table.a == table.b).collect() == expected_eq
    assert (table.a == table.a).collect() == [True] * (len(BITS) - 1) + [None]


@pytest.mark.parametrize('kind', ['array', 'struct', 'tuple', 'dict', 'interval'])
def test_nested_order_uses_the_same_float_comparator(kind):
    values = [float64(bits) for bits in BITS]
    constructors = {
        'array': (hl.tarray(hl.tfloat64), lambda x: [x]),
        'struct': (hl.tstruct(x=hl.tfloat64), lambda x: {'x': x}),
        'tuple': (hl.ttuple(hl.tfloat64), lambda x: (x,)),
        'dict': (hl.tdict(hl.tstr, hl.tfloat64), lambda x: {'x': x}),
        'interval': (hl.tinterval(hl.tfloat64), lambda x: hl.Interval(x, x)),
    }
    dtype, wrap = constructors[kind]
    table = hl.Table.parallelize(
        [{'k': wrap(x), 'ordinal': i} for i, x in reversed(list(enumerate(values)))],
        schema=hl.tstruct(k=dtype, ordinal=hl.tint32),
    )
    if kind == 'dict':
        with pytest.raises(NotImplementedError, match='Set/Dictionary'):
            table.key_by('k')
    else:
        assert [row.ordinal for row in table.key_by('k').collect()] == list(range(len(BITS)))
    assert (table.k == table.k).collect() == [True] * len(BITS)


def test_set_ingestion_keeps_signed_zeros_and_distinct_nan_payloads():
    dtype = hl.tset(hl.tfloat64)
    # Use typed list input because Python sets already merge signed zeros.
    a = hl.literal([0.0, -0.0, float64(BITS[0]), float64(BITS[1])], dtype)
    b = hl.literal([0.0, float64(BITS[0])], dtype)
    assert hl.eval(a == b) is False
    assert hl.eval(a == hl.literal([float64(BITS[1]), -0.0, 0.0, float64(BITS[0])], dtype)) is True
    table = hl.range_table(2)
    table = table.annotate(k=hl.if_else(table.idx == 0, a, b))
    with pytest.raises(NotImplementedError, match='Set/Dictionary'):
        table.group_by('k')


def test_float32_keys_keep_payloads_at_the_declared_width():
    bits = np.array([0xFFC00002, 0xFFC00001, 0x80000000, 0,
                     0x7FC00001, 0x7FC00002], dtype=np.uint32)
    values = bits.view(np.float32)
    table = hl.Table.parallelize(
        [{'k': float(x)} for x in values[::-1]], schema=hl.tstruct(k=hl.tfloat32),
    )
    result = np.asarray(table.key_by('k').k.collect(), dtype=np.float32).view(np.uint32)
    np.testing.assert_array_equal(result, bits)


def test_ndarray_shape_precedes_row_major_elements():
    dtype = hl.tndarray(hl.tfloat64, 1)
    table = hl.Table.parallelize([
        {'k': np.array([-100.0, -100.0]), 'i': 2},
        {'k': np.array([float64(BITS[-1])]), 'i': 1},
        {'k': np.array([float64(BITS[0])]), 'i': 0},
    ], schema=hl.tstruct(k=dtype, i=hl.tint32))
    assert [row.i for row in table.key_by('k').collect()] == [0, 1, 2]
