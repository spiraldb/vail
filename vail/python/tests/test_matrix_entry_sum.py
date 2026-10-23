"""Matrix entry-sum reductions over row-local Arrow lists."""

import vail as hl


def test_integer_entry_sum_preserves_missing_cells_and_int64_precision():
    narrow = hl.utils.range_matrix_table(1, 2, 1)
    narrow = narrow.annotate_entries(
        value=hl.int32(narrow.row_idx + narrow.col_idx + 1)
    )
    assert narrow.aggregate_entries(hl.agg.sum(narrow.value)) == 3

    matrix = hl.utils.range_matrix_table(3, 4, 2)
    matrix = matrix.annotate_entries(
        value=hl.if_else(
            (matrix.row_idx + matrix.col_idx) % 2 == 0,
            hl.int64(matrix.row_idx + matrix.col_idx),
            hl.missing(hl.tint64),
        )
    )
    assert matrix.aggregate_entries(hl.agg.sum(matrix.value)) == 14

    filtered = matrix.filter_entries(matrix.col_idx % 2 == 0)
    assert filtered.aggregate_entries(hl.agg.sum(filtered.value)) == 8

    precise = hl.utils.range_matrix_table(1, 2, 1)
    precise = precise.annotate_entries(
        value=hl.if_else(
            precise.col_idx == 0,
            hl.int64(9_007_199_254_740_993),
            hl.int64(2),
        )
    )
    assert precise.aggregate_entries(hl.agg.sum(precise.value)) == 9_007_199_254_740_995

    floating = hl.utils.range_matrix_table(2, 2, 1)
    floating = floating.annotate_entries(
        value=hl.float64(floating.row_idx + floating.col_idx)
    )
    assert floating.aggregate_entries(hl.agg.sum(floating.value)) == 4.0

    float32 = hl.utils.range_matrix_table(1, 2, 1)
    float32 = float32.annotate_entries(value=hl.float32(0.1))
    assert float32.aggregate_entries(hl.agg.sum(float32.value)) == 0.20000000298023224


def test_integer_entry_sum_empty_axes_globals_and_nonlocalized_result():
    empty_rows = hl.utils.range_matrix_table(0, 3, 1)
    assert empty_rows.aggregate_entries(hl.agg.sum(hl.int64(1))) == 0

    empty_cols = hl.utils.range_matrix_table(2, 0, 1)
    assert empty_cols.aggregate_entries(hl.agg.sum(hl.int64(1))) == 0

    missing = hl.utils.range_matrix_table(2, 2, 1)
    missing = missing.annotate_entries(value=hl.missing(hl.tint64))
    assert missing.aggregate_entries(hl.agg.sum(missing.value)) == 0

    matrix = hl.utils.range_matrix_table(2, 2, 1).annotate_globals(offset=7)
    matrix = matrix.annotate_entries(
        value=hl.int64(matrix.row_idx + matrix.col_idx + matrix.offset)
    )
    assert matrix.aggregate_entries(hl.agg.sum(matrix.value)) == 32
    lazy = matrix.aggregate_entries(hl.agg.sum(matrix.value), _localize=False)
    assert isinstance(lazy, hl.Expression)
    assert hl.eval(lazy) == 32


def test_entry_sum_preserves_axis_values_types_and_replay():
    matrix = hl.utils.range_matrix_table(3, 4, 2)
    value = hl.int64(matrix.row_idx + matrix.col_idx)
    expressions = [
        (hl.agg.sum(hl.int64(matrix.row_idx)), 12),
        (hl.agg.count(), 12),
        (hl.agg.sum(value), 30),
        (hl.agg.sum(hl.int64(matrix.col_idx)), 18),
    ]
    for expression, expected in expressions:
        lazy = matrix.aggregate_entries(expression, _localize=False)
        assert lazy.dtype == hl.tint64
        for _ in range(2):
            assert matrix.aggregate_entries(expression) == expected
            assert hl.eval(lazy) == expected

    sparse = matrix.filter_entries(matrix.col_idx % 2 == 0)
    assert sparse.aggregate_entries(hl.agg.count()) == 6
    assert sparse.aggregate_entries(hl.agg.sum(hl.int64(sparse.row_idx))) == 6
    assert sparse.aggregate_entries(hl.agg.sum(hl.int64(sparse.col_idx))) == 6

    with_globals = matrix.annotate_globals(offset=7)
    expression = hl.agg.sum(hl.int64(with_globals.row_idx + with_globals.offset))
    lazy = with_globals.aggregate_entries(expression, _localize=False)
    assert lazy.dtype == hl.tint64
    assert hl.eval(lazy) == 96
    assert hl.eval(lazy) == 96

    for rows, columns in [(0, 4), (3, 0)]:
        empty = hl.utils.range_matrix_table(rows, columns, 2)
        lazy = empty.aggregate_entries(hl.agg.sum(hl.int64(empty.row_idx)), _localize=False)
        assert lazy.dtype == hl.tint64
        assert hl.eval(lazy) == 0
        assert hl.eval(lazy) == 0


def test_integer_entry_sum_uses_wrapping_int64_arithmetic():
    matrix = hl.utils.range_matrix_table(1, 2, 1)
    matrix = matrix.annotate_entries(
        value=hl.if_else(
            matrix.col_idx == 0,
            hl.int64(9_223_372_036_854_775_807),
            hl.int64(1),
        )
    )
    assert matrix.aggregate_entries(hl.agg.sum(matrix.value)) == -9_223_372_036_854_775_808


def test_array_sum_arrow_kernel_preserves_integer_and_missing_semantics():
    values = hl.literal(
        [9_223_372_036_854_775_807, 1], hl.tarray(hl.tint64)
    )
    assert hl.eval(hl.sum(values)) == -9_223_372_036_854_775_808
    assert hl.eval(hl.sum(hl.literal([], hl.tarray(hl.tint64)))) == 0
    assert hl.eval(hl.sum(hl.literal([None, None], hl.tarray(hl.tint64)))) == 0
    assert hl.eval(
        hl.sum(hl.literal([1, None], hl.tarray(hl.tint64)), filter_missing=False)
    ) is None


def test_column_sum_empty_axes_and_missing_entries_keep_aligned_shape():
    for rows, columns in [(0, 4), (3, 0), (3, 4)]:
        matrix = hl.range_matrix_table(rows, columns, n_partitions=3)
        matrix = matrix.annotate_entries(value=matrix.row_idx + matrix.col_idx)
        result = matrix.annotate_cols(total=hl.agg.sum(matrix.value))
        assert [(c.col_idx, c.total) for c in result.cols().collect()] == [
            (c, rows * (rows - 1) // 2 + rows * c) for c in range(columns)]
        missing = matrix.filter_entries(False)
        result = missing.annotate_cols(total=hl.agg.sum(missing.value))
        assert [c.total for c in result.cols().collect()] == [0] * columns
