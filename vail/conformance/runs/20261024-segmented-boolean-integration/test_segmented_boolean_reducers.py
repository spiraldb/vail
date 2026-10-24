"""Public Boolean aggregate contracts, including packed-bitmap boundaries."""
import pytest
import vail as hl


def _expected(values):
    if values is None:
        return hl.Struct(some=None, every=None, n=None, fraction=None)
    present = [v for v in values if v is not None]
    n = sum(v is True for v in values)
    return hl.Struct(some=any(present), every=all(present), n=n,
                     fraction=n / len(values) if values else None)


def _summarize(values):
    return values.aggregate(lambda x: hl.struct(
        some=hl.agg.any(x), every=hl.agg.all(x), n=hl.agg.count_where(x),
        fraction=hl.agg.fraction(x)))


@pytest.mark.parametrize('sliced', [False, True])
def test_segmented_boolean_nulls_empty_and_word_boundaries(sliced):
    arrays = [None, [], [None, None], [True, False, None], [True, None], [False, None],
              [True] * 64, [True] * 65, [True] * 63 + [False] + [True] * 65,
              [None] * 129,
              [None if j % 11 == 0 else j % 3 != 0 for j in range(137)]]
    rows = [{'idx': i, 'xs': xs} for i, xs in enumerate(arrays)]
    table = hl.Table.parallelize(rows, schema=hl.tstruct(idx=hl.tint32, xs=hl.tarray(hl.tbool)),
                                 n_partitions=3)
    values = table.xs[1:-1] if sliced else table.xs
    result = table.select(idx=table.idx, metric=_summarize(values))
    expected = {i: _expected(xs[1:-1] if sliced and xs is not None else xs)
                for i, xs in enumerate(arrays)}
    assert {row.idx: row.metric for row in result.collect()} == expected


def test_segmented_boolean_dense_tail_padding_and_short_circuit():
    arrays = [[True] * n for n in [0, 1, 63, 64, 65, 127, 128, 129]]
    arrays += [[True] + [False] * 130, [False] + [True] * 130]
    table = hl.Table.parallelize([{'idx': i, 'xs': xs} for i, xs in enumerate(arrays)],
                                 schema=hl.tstruct(idx=hl.tint32, xs=hl.tarray(hl.tbool)),
                                 n_partitions=2)
    result = table.select(idx=table.idx, metric=_summarize(table.xs))
    assert {row.idx: row.metric for row in result.collect()} == {
        i: _expected(xs) for i, xs in enumerate(arrays)}


def test_segmented_boolean_native_sparse_matrix_entries(tmp_path):
    matrix = hl.range_matrix_table(5, 4, n_partitions=2)
    matrix = matrix.annotate_entries(flags=hl.or_missing(
        (matrix.row_idx + matrix.col_idx) % 4 != 0,
        hl.array([matrix.row_idx > 1, hl.missing(hl.tbool), matrix.col_idx > 1])))
    matrix = matrix.filter_entries((matrix.row_idx + matrix.col_idx) % 3 != 0)
    path = str(tmp_path / 'boolean-entries.mt')
    matrix.write(path)
    expected = {}
    for r in range(5):
        for c in range(4):
            if (r + c) % 3 != 0:
                values = None if (r + c) % 4 == 0 else [r > 1, None, c > 1]
                expected[r, c] = _expected(values)
    for source in [matrix, hl.read_matrix_table(path)]:
        result = source.annotate_entries(metric=_summarize(source.flags)).entries()
        assert {(e.row_idx, e.col_idx): e.metric for e in result.collect()} == expected
