"""Packed counter semantics across row widths, nulls, filters and partials."""
import math
import pytest
import vail as hl


@pytest.mark.parametrize('rows,width', [
    (0, 0), (0, 65), (1, 2535), (17, 1), (263, 7), (263, 31),
    (263, 32), (263, 63), (263, 64), (263, 65), (263, 1000), (263, 2535),
])
def test_column_true_counts_and_fraction_with_missing_filtered_entries(rows, width):
    matrix = hl.utils.range_matrix_table(rows, width, n_partitions=3)
    matrix = matrix.annotate_entries(
        predicate=hl.or_missing((matrix.row_idx + 2 * matrix.col_idx) % 11 != 0,
            (3 * matrix.row_idx + matrix.col_idx) % 7 < 3),
        number=hl.or_missing((3 * matrix.row_idx + matrix.col_idx) % 13 != 0,
            matrix.row_idx))
    matrix = matrix.filter_entries((matrix.row_idx + matrix.col_idx) % 5 != 0)
    matrix = matrix.annotate_cols(
        n=hl.agg.count(), yes=hl.agg.count_where(matrix.predicate),
        fraction=hl.agg.fraction(matrix.predicate),
        mean=hl.agg.mean(matrix.number))
    values = matrix.cols().collect()
    assert len(values) == width
    for col, value in enumerate(values):
        active = [r for r in range(rows) if (r + col) % 5 != 0]
        yes = sum((r + 2 * col) % 11 != 0 and (3 * r + col) % 7 < 3 for r in active)
        numbers = [r for r in active if (3 * r + col) % 13 != 0]
        assert value.n == len(active)
        assert value.yes == yes
        assert value.fraction == (yes / len(active) if active else None)
        if numbers:
            assert value.mean == sum(numbers) / len(numbers)
        else:
            assert math.isnan(value.mean)


def test_array_aggregation_repeated_batches_and_missing_predicates():
    table = hl.range_table(1031, n_partitions=7)
    xs = hl.range(0, 65).map(lambda col: hl.or_missing((table.idx + col) % 9 != 0,
        (table.idx + col) % 4 == 0))
    expression = hl.agg.array_agg(lambda x: hl.struct(
        yes=hl.agg.count_where(x), fraction=hl.agg.fraction(x), n=hl.agg.count()), xs)
    result = table.aggregate(expression)
    assert result == table.aggregate(expression)
    for col, value in enumerate(result):
        yes = sum((row + col) % 9 != 0 and (row + col) % 4 == 0 for row in range(1031))
        assert value.yes == yes and value.n == 1031
        assert value.fraction == yes / 1031
