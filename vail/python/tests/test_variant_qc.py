import pytest
import vail as hl


def test_num_cols_is_lazy_matrix_scoped_and_counts_filtered_slots(monkeypatch):
    mt = hl.range_matrix_table(2, 4)
    monkeypatch.setattr(hl.MatrixTable, 'count_cols', lambda *args, **kwargs: pytest.fail('eager count'))
    count = mt._num_cols()
    assert count.source is mt
    assert count.to_ir() == {'op': 'matrix_num_cols'}
    assert hl.eval(count) == 4
    assert mt.annotate_rows(n=count).rows().collect()[0].n == 4
    filtered = mt.filter_cols(mt.col_idx < 2)
    assert hl.eval(filtered._num_cols()) == 2
    empty = mt.filter_rows(False)
    assert hl.eval(empty._num_cols()) == 4
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        filtered.annotate_rows(n=count)


def test_variant_qc_filtered_missing_and_multiallelic():
    mt = hl.range_matrix_table(2, 4)
    mt = mt.annotate_rows(alleles=hl.if_else(mt.row_idx == 0, ['A', 'T'], ['A', 'T', 'C']))
    calls = hl.literal([hl.Call([0, 0]), hl.Call([1, 1]), hl.Call([0, 1]), None], hl.tarray(hl.tcall))
    mt = mt.annotate_entries(GT=calls[mt.col_idx], DP=hl.int32(mt.col_idx))
    mt = mt.filter_entries(mt.col_idx != 1)
    rows = hl.variant_qc(mt).rows().collect()
    qc = rows[0].variant_qc
    assert (qc.n_called, qc.n_not_called, qc.n_filtered) == (2, 1, 1)
    assert qc.call_rate == .5
    assert qc.AC == [3, 1]
    assert qc.AN == 4
    assert qc.n_het == 1
    assert qc.n_non_ref == 1
    assert qc.dp_stats.mean == pytest.approx(5/3)
    assert rows[1].variant_qc.p_value_hwe is None
    assert 'gq_stats' not in qc
    all_filtered = mt.filter_entries(False)
    qc = hl.variant_qc(all_filtered).rows().collect()[0].variant_qc
    assert (qc.n_called, qc.n_not_called, qc.n_filtered) == (0, 0, 4)
    assert qc.call_rate == 0


def test_variant_qc_validation():
    mt = hl.range_matrix_table(1, 1)
    with pytest.raises(ValueError, match="requires a field 'alleles'"):
        hl.variant_qc(mt)
    mt = mt.annotate_rows(alleles=['A', 'T'])
    with pytest.raises(ValueError, match="entry field 'GT'"):
        hl.variant_qc(mt)


def test_hwe_counts_missing_invalid_and_modes():
    assert hl.eval(hl.hardy_weinberg_test(1, 1, 1)).p_value == pytest.approx(.7)
    assert hl.eval(hl.hardy_weinberg_test(1, 1, 1, True)).p_value == pytest.approx(.7)
    assert hl.eval(hl.hardy_weinberg_test(hl.missing(hl.tint32), 1, 1)) is None
    with pytest.raises(RuntimeError, match='nonnegative'):
        hl.eval(hl.hardy_weinberg_test(-1, 1, 1))


def test_parse_variant_and_struct_select():
    parsed = hl.eval(hl.parse_variant('1:100:A:T,C'))
    assert parsed.locus.position == 100
    assert parsed.alleles == ['A', 'T', 'C']
    assert hl.eval(hl.parse_variant(hl.missing(hl.tstr))) is None
    with pytest.raises(RuntimeError, match='parse_variant expects'):
        hl.eval(hl.parse_variant('1:100:A'))
    assert hl.eval(hl.struct(a=1, b=2).select('b', c=3)) == hl.Struct(b=2, c=3)
    absent = hl.missing(hl.tstruct(a=hl.tint32, b=hl.tint32))
    assert hl.eval(absent.select('a')) is None


def test_num_cols_inside_entry_expressions_and_reductions():
    mt = hl.range_matrix_table(2, 3)
    annotated = mt.annotate_entries(n=mt._num_cols())
    assert annotated.n.collect() == [3] * 6
    result = mt.annotate_rows(n=hl.agg.sum(mt._num_cols())).rows().collect()
    assert [row.n for row in result] == [9, 9]
    assert mt.aggregate_entries(hl.agg.sum(mt._num_cols())) == 18


def test_hwe_scalar_broadcast_and_global_mode():
    table = hl.range_table(3).annotate_globals(mode=True)
    result = table.annotate(hwe=hl.hardy_weinberg_test(1, table.idx, 1, table.mode)).collect()
    for row in result:
        assert row.hwe == hl.eval(hl.hardy_weinberg_test(1, row.idx, 1, True))


def test_variant_qc_empty_axes_and_optional_fields():
    import math
    mt = hl.range_matrix_table(1, 0).annotate_rows(alleles=['A', 'T'])
    mt = mt.annotate_entries(GT=hl.missing(hl.tcall), GQ=hl.missing(hl.tint32))
    qc = hl.variant_qc(mt, 'qc').rows().collect()[0].qc
    assert qc.AC == [0, 0]
    # CallStatsAggregator marks the whole AF field missing when AN is zero.
    assert qc.AF is None
    assert qc.n_filtered == 0
    assert math.isnan(qc.call_rate)
    assert math.isnan(qc.het_freq_hwe)
    assert qc.p_value_hwe == .5
    assert 'gq_stats' in qc and 'dp_stats' not in qc
    assert hl.variant_qc(mt.head(0)).count_rows() == 0
