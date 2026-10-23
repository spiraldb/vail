"""Coordinate population standard deviation preserves native state semantics."""
import math
import statistics
import pytest
import vail as hl


@pytest.mark.parametrize('dtype', [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64])
@pytest.mark.parametrize('partitions', [1, 3])
def test_coordinate_stddev_missing_filtered_and_partial_states(dtype, partitions):
    rows = [[1, None, 7], [2, None, None], [9, None, 7], [4, None, None]]
    table = hl.Table.parallelize([{'xs': row} for row in rows],
        schema=hl.tstruct(xs=hl.tarray(dtype)), n_partitions=partitions)
    result = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(
        sd=hl.agg.stats(x).stdev,
        selected=hl.agg.filter(x > 1, hl.agg.stats(x)).stdev), table.xs))
    assert result[0].sd == pytest.approx(statistics.pstdev([1,2,9,4]))
    assert result[0].selected == pytest.approx(statistics.pstdev([2,9,4]))
    assert math.isnan(result[1].sd) and math.isnan(result[1].selected)
    assert result[2].sd == result[2].selected == 0


@pytest.mark.parametrize('partitions', [1, 3])
def test_coordinate_stddev_nonfinite_poison_and_filtered_nonfinite(partitions):
    rows = [[float('inf'), float('nan'), 5.], [2., 3., None]]
    table = hl.Table.parallelize([{'xs': row} for row in rows],
        schema=hl.tstruct(xs=hl.tarray(hl.tfloat64)), n_partitions=partitions)
    result = table.aggregate(hl.agg.array_agg(lambda x: hl.struct(
        sd=hl.agg.stats(x).stdev,
        finite=hl.agg.filter(((x > float('-inf')) & (x < float('inf'))), hl.agg.stats(x)).stdev), table.xs))
    assert math.isnan(result[0].sd) and math.isnan(result[1].sd)
    assert [x.finite for x in result] == [0., 0., 0.]


@pytest.mark.parametrize('partitions', [1, 3])
def test_matrix_coordinate_stats_and_true_count_composite(partitions):
    matrix = hl.utils.range_matrix_table(13, 5, n_partitions=partitions)
    matrix = matrix.annotate_entries(x=hl.or_missing(matrix.row_idx % 3 != 0, matrix.row_idx + matrix.col_idx))
    matrix = matrix.annotate_cols(summary=hl.struct(stats=hl.agg.stats(matrix.x), selected=hl.agg.count_where(matrix.x > 7)))
    result = matrix.summary.collect()
    for column, output in enumerate(result):
        defined = [row + column for row in range(13) if row % 3 != 0]
        assert output.stats.min == min(defined) and output.stats.max == max(defined)
        assert output.stats.n == len(defined)
        assert output.stats.stdev == pytest.approx(statistics.pstdev(defined))
        assert output.selected == sum(value > 7 for value in defined)
