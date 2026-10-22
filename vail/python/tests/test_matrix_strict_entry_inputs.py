"""Entry validity propagates through direct fields without losing presence semantics."""

import vail as hl


def test_direct_entry_inputs_and_non_strict_predicates():
    mt = hl.range_matrix_table(1, 4)
    mt = mt.annotate_rows(alleles=hl.array(['A', 'C']))
    mt = mt.annotate_entries(
        x=hl.or_missing(mt.col_idx != 2, hl.int64(mt.col_idx + 1)),
        GT=hl.or_missing(mt.col_idx != 2, hl.call(0, 1)),
    )
    mt = mt.filter_entries(mt.col_idx != 1)
    assert mt.aggregate_entries(hl.agg.sum(mt.x)) == 5
    # Computed non-strict inputs must still skip filtered entries.
    assert mt.aggregate_entries(hl.agg.sum(hl.or_else(mt.x, 10))) == 15
    rows = mt.annotate_rows(
        stats=hl.agg.call_stats(mt.GT, mt.alleles),
        present=hl.agg.count_where(True),
        missing_gt=hl.agg.count_where(hl.is_missing(mt.GT)),
        coalesced=hl.agg.count_where(hl.or_else(mt.x > 0, True)),
    ).rows().collect()
    assert rows[0].stats.AC == [2, 2]
    assert rows[0].stats.AN == 4
    assert (rows[0].present, rows[0].missing_gt, rows[0].coalesced) == (3, 1, 3)


def test_direct_entry_inputs_all_filtered_and_empty_struct_presence():
    mt = hl.range_matrix_table(2, 3)
    mt = mt.annotate_rows(alleles=hl.array(['A', 'C']))
    mt = mt.annotate_entries(x=hl.missing(hl.tint64), GT=hl.missing(hl.tcall))
    assert mt.aggregate_entries(hl.agg.sum(mt.x)) == 0
    mt = mt.filter_entries(False)
    assert mt.aggregate_entries(hl.agg.sum(mt.x)) == 0
    rows = mt.annotate_rows(stats=hl.agg.call_stats(mt.GT, mt.alleles)).rows().collect()
    assert [(row.stats.AC, row.stats.AN) for row in rows] == [([0, 0], 0)] * 2
    empty = hl.range_matrix_table(1, 3).filter_entries(False)
    assert empty.annotate_rows(n=hl.agg.count_where(True)).rows().collect()[0].n == 0


def test_column_sum_direct_and_computed_entry_validity():
    mt = hl.range_matrix_table(3, 4, 2)
    mt = mt.annotate_entries(x=hl.or_missing(mt.row_idx != 1, mt.row_idx + mt.col_idx))
    mt = mt.filter_entries((mt.row_idx + mt.col_idx) % 2 == 0)
    result = mt.annotate_cols(
        direct=hl.agg.sum(mt.x),
        computed=hl.agg.sum(hl.or_else(mt.x, 10)),
        constant=hl.agg.sum(1),
    ).cols().collect()
    assert [(x.direct, x.computed, x.constant) for x in result] == [
        (2, 2, 2), (0, 10, 1), (6, 6, 2), (0, 10, 1),
    ]


def test_aligned_annotation_preserves_lazy_filtered_entries():
    mt = hl.range_matrix_table(3, 4, 2)
    mt = mt.annotate_entries(divisor=mt.col_idx)
    mt = mt.filter_entries(mt.col_idx != 0)
    mt = mt.annotate_entries(value=12 // mt.divisor)
    result = mt.annotate_cols(total=hl.agg.sum(mt.value)).cols().collect()
    assert [x.total for x in result] == [0, 36, 18, 12]
    dense = hl.range_matrix_table(3, 4, 2)
    dense = dense.annotate_entries(x=dense.row_idx + dense.col_idx)
    assert [x.total for x in dense.annotate_cols(total=hl.agg.sum(dense.x)).cols().collect()] == [3, 6, 9, 12]
