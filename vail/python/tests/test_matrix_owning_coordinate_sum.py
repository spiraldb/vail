"""Public global coordinate reductions preserve entry and array missingness."""
import pytest
import vail as hl


def test_global_coordinate_sum_with_missing_arrays_and_filtered_entries():
    mt = hl.utils.range_matrix_table(3, 4, 3)
    mt = mt.annotate_entries(arbitrary=hl.or_missing(mt.col_idx != 1, [mt.row_idx, mt.col_idx]))
    mt = mt.filter_entries(mt.col_idx != 2)
    assert mt.aggregate_entries(hl.agg.array_agg(lambda x: hl.agg.sum(x), mt.arbitrary)) == [6, 9]
    # Mixed queries and captures retain the complete original route.
    assert mt.aggregate_entries(hl.struct(
        n=hl.agg.count(),
        xs=hl.agg.array_agg(lambda x: hl.agg.sum(x), mt.arbitrary),
    )) == hl.Struct(n=9, xs=[6, 9])
    assert mt.aggregate_entries(hl.agg.array_agg(lambda x: hl.agg.sum(x + 1), mt.arbitrary)) == [12, 15]


def test_global_coordinate_empty_missing_null_and_wrapping():
    mt = hl.utils.range_matrix_table(2, 2, 2)
    mt = mt.annotate_entries(arbitrary=hl.missing(hl.tarray(hl.tint32)))
    assert mt.aggregate_entries(hl.agg.array_agg(lambda x: hl.agg.sum(x), mt.arbitrary)) is None
    mt = mt.annotate_entries(arbitrary=hl.empty_array(hl.tint32))
    assert mt.aggregate_entries(hl.agg.array_agg(lambda x: hl.agg.sum(x), mt.arbitrary)) == []
    mt = mt.annotate_entries(arbitrary=[hl.missing(hl.tint32), hl.int32(2147483647)])
    assert mt.aggregate_entries(hl.agg.array_agg(lambda x: hl.agg.sum(x), mt.arbitrary)) == [0, 8589934588]


def test_global_coordinate_sum_reports_ragged_partition_widths():
    mt = hl.utils.range_matrix_table(2, 2, 2)
    mt = mt.annotate_entries(arbitrary=hl.if_else(mt.row_idx == 0, hl.empty_array(hl.tint32), [1]))
    with pytest.raises(Exception, match="(?i)(length|width)"):
        mt.aggregate_entries(hl.agg.array_agg(lambda x: hl.agg.sum(x), mt.arbitrary))
