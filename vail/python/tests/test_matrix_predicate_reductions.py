"""Row predicate reductions preserve entry presence and column alignment."""

import math

import pytest
import vail as hl


def test_predicate_reductions_with_missing_values_and_filtered_entries():
    mt = hl.range_matrix_table(2, 4, 2)
    mt = mt.annotate_cols(male=mt.col_idx % 2 == 0)
    mt = mt.annotate_entries(p=hl.if_else(mt.col_idx == 2, hl.missing(hl.tbool),
                                        (mt.col_idx + mt.row_idx) % 3 == 0))
    mt = mt.filter_entries(mt.col_idx != 1)
    rows = mt.annotate_rows(
        n_male=hl.agg.count_where(mt.p & mt.male),
        n_female=hl.agg.count_where(mt.p & ~mt.male),
        n=hl.agg.count_where(True),
        any=hl.agg.any(mt.p),
        all=hl.agg.all(mt.p),
        fraction=hl.agg.fraction(mt.p),
    ).rows().collect()
    assert [(r.n_male, r.n_female, r.n, r.any, r.all) for r in rows] == [
        (1, 1, 3, True, True), (0, 0, 3, False, False)]
    assert [r.fraction for r in rows] == pytest.approx([2 / 3, 0])


@pytest.mark.parametrize("columns", [0, 3])
def test_predicate_reductions_over_empty_or_all_filtered_entries(columns):
    mt = hl.range_matrix_table(2, columns, 2).filter_entries(False)
    rows = mt.annotate_rows(n=hl.agg.count_where(True), any=hl.agg.any(True),
                            all=hl.agg.all(False), fraction=hl.agg.fraction(True)).rows().collect()
    assert [(r.n, r.any, r.all) for r in rows] == [(0, False, True)] * 2
    assert all(math.isnan(r.fraction) for r in rows)


def test_predicate_reductions_after_column_selection_and_reordering():
    mt = hl.range_matrix_table(2, 5, 2)
    mt = mt.annotate_cols(male=mt.col_idx % 2 == 0)
    mt = mt.annotate_entries(p=mt.col_idx == mt.row_idx)
    mt = mt.choose_cols([4, 1, 0])
    result = mt.annotate_rows(n=hl.agg.count_where(mt.p & mt.male)).rows().collect()
    assert [r.n for r in result] == [1, 0]


def test_filtered_entries_do_not_evaluate_failing_predicates():
    mt = hl.range_matrix_table(2, 3, 2).filter_entries(False)
    # Checked integer conversion must never execute for absent cells.
    predicate = hl.int32(hl.str(mt.col_idx) + "invalid") > mt.row_idx
    result = mt.annotate_rows(n=hl.agg.count_where(predicate)).rows().collect()
    assert [r.n for r in result] == [0, 0]


def test_random_predicates_retain_filtered_entry_context():
    mt = hl.range_matrix_table(3, 7, 2)
    mt = mt.filter_entries(mt.col_idx % 3 != 1).choose_cols([6, 1, 0, 5])
    predicate = hl.rand_bool(0.5, seed=17)
    result = mt.annotate_rows(
        direct=hl.agg.count_where(predicate),
        wrapped=hl.agg.filter(True, hl.agg.count_where(predicate)),
    ).rows()
    first = result.collect()
    assert first == result.collect()
    assert all(row.direct == row.wrapped for row in first)


def test_predicate_reduction_with_shared_expression_nodes():
    mt = hl.range_matrix_table(1, 1)
    mt = mt.annotate_entries(p=True)
    predicate = mt.p
    for _ in range(8):
        predicate = predicate & predicate
    reduced = mt.annotate_rows(n=hl.agg.count_where(predicate))
    assert reduced.n.dtype == hl.tint64
    assert reduced.n.collect() == [1]
