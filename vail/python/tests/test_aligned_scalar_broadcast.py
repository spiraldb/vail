import pytest
import vail as hl


@pytest.mark.parametrize('rows,partitions', [(0, 1), (1, 1), (2, 1), (7, 1), (7, 3)])
def test_constant_entry_lists_broadcast_to_every_matrix_row(rows, partitions):
    matrix = hl.utils.range_matrix_table(rows, 3, partitions)
    matrix = matrix.annotate_entries(x=hl.float64(matrix.row_idx + matrix.col_idx) / 4)
    expected = sum(r + c for r in range(rows) for c in range(3)) / 4
    assert matrix.aggregate_entries(hl.struct(s=hl.agg.sum(matrix.x), twice=hl.agg.sum(matrix.x * 2))) == hl.Struct(s=expected, twice=expected * 2)


def test_scalar_row_records_and_entry_lists_keep_shared_column_alignment():
    matrix = hl.utils.range_matrix_table(5, 3, 1)
    matrix = matrix.key_rows_by().select_rows()
    matrix = matrix.annotate_entries(x=hl.float64(matrix.col_idx))
    assert matrix.aggregate_entries(hl.struct(s=hl.agg.sum(matrix.x), n=hl.agg.count())) == hl.Struct(s=15, n=15)
