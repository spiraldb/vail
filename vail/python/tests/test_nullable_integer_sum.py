"""Public reductions over sliced, nested and nullable integer lists."""
import random

import pytest
import vail as hl


def wrap(value, width):
    return (value + (1 << (width - 1))) % (1 << width) - (1 << (width - 1))


@pytest.mark.parametrize('dtype,width', [(hl.tint32, 32), (hl.tint64, 64)])
@pytest.mark.parametrize('length', [0, 1, 7, 63, 64, 65, 127, 128, 129, 257])
def test_nested_sliced_sum_matches_python(dtype, width, length):
    rng = random.Random(1103 + length)
    scalar = lambda: rng.randint(-(1 << (width - 1)), (1 << (width - 1)) - 1)
    schema = hl.tarray(hl.tstruct(payload=hl.tstruct(depth=dtype)))
    records = []
    for _ in range(length + 17):
        choice = rng.randrange(7)
        records.append(None if choice == 0 else hl.Struct(payload=None) if choice == 1 else
                       hl.Struct(payload=hl.Struct(depth=None if choice == 2 else scalar())))
    array = hl.literal(records, schema)
    for offset in (0, 1, 7, 8, 15):
        selected = records[offset:offset + length]
        values = [None if x is None or x.payload is None else x.payload.depth for x in selected]
        expr = array[offset:offset + length].map(lambda x: x.payload.depth)
        expected = wrap(sum(x for x in values if x is not None), width)
        assert hl.eval(hl.sum(expr)) == expected
        strict = None if any(x is None for x in values) else expected
        assert hl.eval(hl.sum(expr, filter_missing=False)) == strict
        # Widened accumulation is in registers, including Int32 overflow inputs.
        assert hl.eval(hl.sum(array[offset:offset + length].map(lambda x: hl.int64(x.payload.depth)))) == wrap(sum(x for x in values if x is not None), 64)


@pytest.mark.parametrize('dtype', [hl.tint32, hl.tint64])
def test_empty_missing_outer_lists_and_all_missing(dtype):
    schema = hl.tarray(hl.tstruct(n=dtype))
    for records, expected, strict in [([], 0, 0), (None, None, None),
                                       ([None] * 128, 0, None),
                                       ([hl.Struct(n=None)] * 65, 0, None)]:
        values = hl.literal(records, schema).map(lambda x: x.n)
        assert hl.eval(hl.sum(values)) == expected
        assert hl.eval(hl.sum(values, filter_missing=False)) == strict


@pytest.mark.parametrize('dtype,width', [(hl.tint32, 32), (hl.tint64, 64)])
def test_sum_overflow_contract_with_nontrivial_validity(dtype, width):
    hi = (1 << (width - 1)) - 1
    lo = -(1 << (width - 1))
    for values in ([hi, 1, None], [lo, -1, None], [hi] * 65 + [None]):
        a = hl.literal([hl.Struct(n=v) for v in values], hl.tarray(hl.tstruct(n=dtype)))
        assert hl.eval(hl.sum(a.map(lambda x: x.n))) == wrap(sum(v for v in values if v is not None), width)


@pytest.mark.parametrize('missing_mod', [2, 7, 65])
def test_native_matrix_sum_after_filters_and_nested_missingness(tmp_path, missing_mod):
    rows, cols = 19, 131
    m = hl.utils.range_matrix_table(rows, cols, 3)
    m = m.annotate_entries(payload=hl.or_missing(m.col_idx % missing_mod != 0,
        hl.struct(depth=hl.or_missing((m.row_idx + m.col_idx) % 11 != 0,
                                    hl.int32(m.row_idx * 301 - m.col_idx * 13)))))
    m = m.filter_entries((m.row_idx + m.col_idx) % 17 != 0)
    expected = sum(r * 301 - c * 13 for r in range(rows) for c in range(cols)
                   if c % missing_mod != 0 and (r+c) % 11 != 0 and (r+c) % 17 != 0)
    assert m.aggregate_entries(hl.agg.sum(m.payload.depth)) == expected
    path = str(tmp_path / 'nested.mt')
    m.write(path)
    loaded = hl.read_matrix_table(path)
    assert loaded.aggregate_entries(hl.agg.sum(loaded.payload.depth)) == expected
    assert sum(x.total for x in loaded.annotate_rows(total=hl.agg.sum(loaded.payload.depth)).rows().collect()) == expected


def test_nested_fields_keep_declared_positions_and_sibling_sums():
    schema = hl.tarray(hl.tstruct(prefix=hl.tstr,
        payload=hl.tstruct(other=hl.tint64, depth=hl.tint32), suffix=hl.tint32))
    records = [hl.Struct(prefix='x', payload=hl.Struct(other=1 << 40, depth=3), suffix=19),
               hl.Struct(prefix='y', payload=None, suffix=23), None,
               hl.Struct(prefix='z', payload=hl.Struct(other=7, depth=None), suffix=29)]
    a = hl.literal(records, schema)
    result = hl.eval(hl.struct(depth=hl.sum(a.map(lambda x: hl.int64(x.payload.depth))),
                              other=hl.sum(a.map(lambda x: x.payload.other)),
                              suffix=hl.sum(a.map(lambda x: x.suffix))))
    assert result == hl.Struct(depth=3, other=(1 << 40) + 7, suffix=71)


def test_arrow_list_slice_with_large_hidden_child_owner():
    import numpy as np
    import pyarrow as pa
    values = np.arange(131 * 1024, dtype=np.int32)
    depth = pa.array(values, mask=(values % 7 == 0))
    payload = pa.StructArray.from_arrays([depth], names=['depth'])
    cells = pa.StructArray.from_arrays([payload], names=['payload'], mask=pa.array(values % 11 == 0))
    lists = pa.ListArray.from_arrays(pa.array(np.arange(1025, dtype=np.int32) * 131), cells)
    # Retain the large owner behind an interior two-row slice, with nonzero
    # Arrow child/bitmap offsets. Test through the public Arrow import API.
    selected = lists.slice(777, 2)
    t = hl.Table.parallelize(pa.Table.from_arrays([selected], names=['items']))
    result = t.select(total=hl.sum(t.items.map(lambda x: hl.int64(x.payload.depth)))).collect()
    expected = [sum(i for i in range(row * 131, (row + 1) * 131) if i % 7 and i % 11)
                for row in (777, 778)]
    assert [x.total for x in result] == expected
    empty = hl.Table.parallelize(pa.Table.from_arrays([lists.slice(777, 0)], names=['items']))
    assert empty.select(total=hl.sum(empty.items.map(lambda x: hl.int64(x.payload.depth)))).collect() == []
