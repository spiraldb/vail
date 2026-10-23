"""Column callbacks reduce aligned entry positions, including empty axes."""
import math
import pytest
import vail as hl


@pytest.mark.parametrize('partitions', [1, 3])
def test_composite_column_callbacks_keep_missing_entry_positions(partitions):
    matrix = hl.utils.range_matrix_table(3, 4, n_partitions=partitions)
    matrix = matrix.annotate_entries(x=matrix.row_idx + matrix.col_idx)
    matrix = matrix.filter_entries((matrix.row_idx == 1) & (matrix.col_idx != 3))
    matrix = matrix.annotate_cols(result=hl.struct(
        n=hl.agg.count(), total=hl.agg.sum(matrix.x), mean=hl.agg.mean(matrix.x),
        fraction=hl.agg.fraction(matrix.x > 1),
        row_total=hl.agg.sum(matrix.row_idx),
        col_total=hl.agg.sum(matrix.col_idx)))
    rows = matrix.result.collect()
    assert [x.n for x in rows] == [1, 1, 1, 0]
    assert [x.total for x in rows] == [1, 2, 3, 0]
    assert [x.mean for x in rows[:3]] == [1., 2., 3.]
    assert math.isnan(rows[3].mean)
    assert [x.fraction for x in rows] == [0., 1., 1., None]
    assert [x.row_total for x in rows] == [1, 1, 1, 0]
    assert [x.col_total for x in rows] == [0, 1, 2, 0]


@pytest.mark.parametrize('rows,cols', [(0, 3), (3, 0)])
def test_composite_column_callbacks_preserve_empty_axis_identities(rows, cols):
    matrix = hl.utils.range_matrix_table(rows, cols, n_partitions=1)
    matrix = matrix.annotate_entries(x=matrix.row_idx + matrix.col_idx)
    matrix = matrix.annotate_cols(result=hl.struct(
        n=hl.agg.count(), total=hl.agg.sum(matrix.x), mean=hl.agg.mean(matrix.x),
        fraction=hl.agg.fraction(matrix.x > 0), product=hl.agg.product(matrix.x),
        all=hl.agg.all(matrix.x > 0), any=hl.agg.any(matrix.x > 0)))
    values = matrix.result.collect()
    assert len(values) == cols
    for value in values:
        assert value.n == 0 and value.total == 0 and value.product == 1
        assert math.isnan(value.mean) and value.fraction is None
        assert value.all is True and value.any is False
