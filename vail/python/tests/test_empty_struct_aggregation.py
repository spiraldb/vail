"""Empty structs retain cardinality and missingness through native aggregation."""

import vail as hl


def test_ordered_collect_preserves_nested_empty_structs():
    table = hl.range_table(5, n_partitions=3)
    table = table.annotate(
        record=hl.struct(
            index=table.idx,
            empty=hl.if_else(table.idx % 2 == 0, hl.struct(), hl.missing(hl.tstruct())),
            nested=[hl.struct(), hl.struct()],
        )
    )
    records = table.aggregate(hl.agg.collect(table.record))
    assert [row.index for row in records] == list(range(5))
    assert [row.empty for row in records] == [hl.Struct(), None, hl.Struct(), None, hl.Struct()]
    assert all(row.nested == [hl.Struct(), hl.Struct()] for row in records)


def test_grouped_matrix_row_aggregation_with_empty_entry_records():
    matrix = hl.range_matrix_table(10, 5, n_partitions=5)
    matrix = matrix.annotate_entries(GT=hl.call(0, 1)).annotate_globals(alleles=['G', 'C'])
    matrix = matrix.annotate_rows(group=matrix.row_idx < 3, GT_row=hl.call(0, 1))
    table = matrix.group_rows_by(matrix.group).aggregate_rows(
        call_stats=hl.agg.call_stats(matrix.GT_row, matrix.alleles)
    ).result().entries()
    assert table.count() == 10
    expected_small = hl.struct(AC=[3, 3], AF=[0.5, 0.5], AN=6, homozygote_count=[0, 0])
    expected_large = hl.struct(AC=[7, 7], AF=[0.5, 0.5], AN=14, homozygote_count=[0, 0])
    assert table.all(hl.is_defined(table.call_stats) &
                     (table.call_stats == hl.if_else(table.group, expected_small, expected_large)))
