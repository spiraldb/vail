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
