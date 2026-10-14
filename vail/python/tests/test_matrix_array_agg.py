"""Matrix agg.array_agg transposes per-entry arrays without retaining them."""

import vail as hl


def test_matrix_array_agg_transposes_entry_arrays_and_keeps_row_captures():
    matrix = hl.utils.range_matrix_table(2, 3, 1)
    matrix = matrix.annotate_entries(
        values=[matrix.row_idx + matrix.col_idx, 10 * matrix.row_idx + matrix.col_idx],
        extra=matrix.row_idx + 2 * matrix.col_idx,
    )

    annotated = matrix.annotate_rows(
        result=hl.agg.array_agg(
            lambda item: hl.agg.sum(
                item + matrix.row_idx + matrix.extra + matrix.col_idx
            ),
            matrix.values,
        )
    )
    assert annotated.result.collect() == [[12, 12], [21, 48]]


def test_matrix_nested_array_agg_transposes_each_level():
    matrix = hl.utils.range_matrix_table(2, 3, 1)
    matrix = matrix.annotate_entries(
        values=[matrix.row_idx + matrix.col_idx, 10 * matrix.row_idx + matrix.col_idx]
    )

    annotated = matrix.annotate_rows(
        result=hl.agg.array_agg(
            lambda item: hl.agg.array_agg(
                lambda nested: hl.agg.sum(nested), [item, item + 1]
            ),
            matrix.values,
        )
    )
    assert annotated.result.collect() == [[[3, 6], [3, 6]], [[6, 9], [33, 36]]]


def test_matrix_array_agg_skips_missing_arrays_and_preserves_empty_result():
    matrix = hl.utils.range_matrix_table(1, 3, 1)
    matrix = matrix.annotate_entries(
        values=hl.if_else(
            matrix.col_idx == 1,
            hl.missing(hl.tarray(hl.tint32)),
            [matrix.col_idx, matrix.col_idx + 1],
        )
    )
    assert matrix.annotate_rows(
        result=hl.agg.array_agg(lambda item: hl.agg.sum(item), matrix.values)
    ).result.collect() == [[2, 4]]

    missing = hl.utils.range_matrix_table(1, 2, 1).annotate_entries(
        values=hl.missing(hl.tarray(hl.tint32))
    )
    assert missing.annotate_rows(
        result=hl.agg.array_agg(lambda item: hl.agg.sum(item), missing.values)
    ).result.collect() == [[]]
