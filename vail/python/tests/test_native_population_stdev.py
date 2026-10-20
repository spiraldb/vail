"""Public Hail-style paths for the native population standard deviation."""
import math

import pytest
import vail as hl


def test_table_grouped_filtered_coordinate_and_list_stdev_paths():
    table = hl.Table.parallelize(
        [
            {"g": 0, "x": 1.0, "xs": [1.0, 3.0]},
            {"g": 0, "x": None, "xs": [3.0, 5.0]},
            {"g": 0, "x": 3.0, "xs": [None, 7.0]},
            {"g": 1, "x": None, "xs": [1.0, None]},
        ],
        schema=hl.tstruct(
            g=hl.tint32,
            x=hl.tfloat64,
            xs=hl.tarray(hl.tfloat64),
        ),
        n_partitions=3,
    )

    result = table.aggregate(
        hl.struct(
            stats=hl.agg.stats(table.x),
            filtered=hl.agg.filter(table.x < 3.0, hl.agg.stats(table.x)),
            coordinate=hl.agg.array_agg(
                lambda value: hl.agg.stats(value).stdev, table.xs
            ),
        )
    )
    assert result.stats.stdev == 1.0
    assert result.filtered.stdev == 0.0
    assert result.coordinate == pytest.approx(
        [math.sqrt(8.0 / 9.0), math.sqrt(8.0 / 3.0)]
    )

    grouped = table.group_by(table.g).aggregate(stats=hl.agg.stats(table.x)).collect()
    by_group = {row.g: row.stats for row in grouped}
    assert by_group[0].stdev == 1.0
    assert by_group[0].n == 2
    assert math.isnan(by_group[1].stdev)
    assert by_group[1].n == 0

    assert hl.eval(
        hl.array([1.0, 3.0]).aggregate(
            lambda value: hl.agg.stats(value).stdev
        )
    ) == 1.0


@pytest.mark.parametrize("values", [[], [None, None], [None, 7.0]])
def test_empty_and_missing_population_stdev_results_are_nan_or_zero(values):
    table = hl.Table.parallelize(
        [{"x": value} for value in values],
        schema=hl.tstruct(x=hl.tfloat64),
        n_partitions=3,
    )
    stdev = table.aggregate(hl.agg.stats(table.x)).stdev
    if values == [None, 7.0]:
        assert stdev == 0.0
    else:
        assert math.isnan(stdev)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_single_nonfinite_population_stdev_is_nan(value):
    table = hl.Table.parallelize(
        [{"x": value}], schema=hl.tstruct(x=hl.tfloat64), n_partitions=1
    )
    assert math.isnan(table.aggregate(hl.agg.stats(table.x)).stdev)


def test_filtered_nonfinite_and_missing_rows_do_not_reach_the_accumulator():
    table = hl.Table.parallelize(
        [{"x": 1.0}, {"x": None}, {"x": math.nan}, {"x": 3.0}],
        schema=hl.tstruct(x=hl.tfloat64),
        n_partitions=3,
    )
    stdev = table.aggregate(
        hl.agg.filter(table.x < 4.0, hl.agg.stats(table.x)).stdev
    )
    assert stdev == 1.0


def test_matrix_row_and_column_annotations_use_population_stdev():
    matrix = hl.range_matrix_table(2, 3, n_partitions=2)
    matrix = matrix.annotate_entries(x=hl.float64(matrix.row_idx + matrix.col_idx))

    rows = matrix.annotate_rows(stdev=hl.agg.stats(matrix.x).stdev).rows().collect()
    cols = matrix.annotate_cols(stdev=hl.agg.stats(matrix.x).stdev).cols().collect()
    assert [row.stdev for row in rows] == pytest.approx(
        [math.sqrt(2.0 / 3.0)] * 2
    )
    assert [col.stdev for col in cols] == pytest.approx([0.5] * 3)


def test_population_stdev_scan_uses_empty_and_singleton_identities():
    table = hl.Table.parallelize(
        [{"x": 1.0}, {"x": 3.0}], schema=hl.tstruct(x=hl.tfloat64), n_partitions=2
    )
    rows = table.annotate(stats=hl.scan.stats(table.x)).collect()
    assert math.isnan(rows[0].stats.stdev)
    assert rows[1].stats.stdev == 0.0
