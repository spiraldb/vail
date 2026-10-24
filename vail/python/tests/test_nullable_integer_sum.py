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
