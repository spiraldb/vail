import pytest
import vail as hl


def test_variant_qc_tracks_filtered_columns_and_empty_rows():
    mt = hl.range_matrix_table(2, 4).annotate_rows(alleles=['A', 'C'])
    mt = mt.annotate_entries(GT=hl.call(0, 1))
    filtered = mt.filter_cols(mt.col_idx < 2)
    qc = hl.variant_qc(filtered).rows().collect()[0].variant_qc
    assert (qc.n_called, qc.n_not_called, qc.n_filtered) == (2, 0, 0)
    assert qc.call_rate == 1
    empty = hl.variant_qc(filtered.filter_rows(False))
    assert empty.count() == (0, 2)
    assert empty.rows().collect() == []


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


def test_variant_qc_mixed_ploidy_uses_diploid_hwe_and_direct_call_counts():
    calls = [hl.Call([0, 0]), hl.Call([0, 1], True), hl.Call([1, 1]),
             hl.Call([0]), hl.Call([1]), hl.Call([]), None]
    mt = hl.range_matrix_table(1, len(calls))
    mt = mt.annotate_rows(alleles=['A', 'C'])
    values = hl.literal(calls, hl.tarray(hl.tcall))
    mt = mt.annotate_entries(GT=values[mt.col_idx])
    qc = hl.variant_qc(mt).rows().collect()[0].variant_qc
    assert (qc.n_called, qc.n_not_called, qc.n_filtered) == (6, 1, 0)
    assert (qc.n_het, qc.n_non_ref) == (1, 3)
    assert qc.AC == [4, 4] and qc.AN == 8
    assert qc.homozygote_count == [1, 1]
    assert qc.het_freq_hwe == pytest.approx(hl.eval(hl.hardy_weinberg_test(1, 1, 1)).het_freq_hwe)
    assert qc.p_value_hwe == pytest.approx(hl.eval(hl.hardy_weinberg_test(1, 1, 1)).p_value)
    assert qc.p_value_excess_het == pytest.approx(hl.eval(hl.hardy_weinberg_test(1, 1, 1, True)).p_value)


@pytest.mark.parametrize('n_alleles', [0, 1, 2, 7])
def test_matrix_call_stats_matches_public_table_reducer(n_alleles):
    calls = [None, hl.Call([])] if n_alleles == 0 else [hl.Call([0]), hl.Call([0, n_alleles - 1]), None, hl.Call([])]
    table = hl.Table.parallelize([{'GT': c} for c in calls], schema=hl.tstruct(GT=hl.tcall), n_partitions=3)
    expected = table.aggregate(hl.agg.call_stats(table.GT, n_alleles))
    mt = hl.range_matrix_table(3, len(calls), n_partitions=2)
    values = hl.literal(calls, hl.tarray(hl.tcall))
    mt = mt.annotate_entries(GT=values[mt.col_idx])
    mt = mt.annotate_rows(stats=hl.agg.call_stats(mt.GT, n_alleles))
    assert [r.stats for r in mt.rows().collect()] == [expected] * 3


def test_matrix_call_stats_empty_filtered_and_invalid_calls():
    mt = hl.range_matrix_table(1, 2).annotate_entries(GT=hl.call(0, 1))
    for empty in [mt.filter_entries(False), mt.filter_cols(False)]:
        row = empty.annotate_rows(stats=hl.agg.call_stats(empty.GT, 3)).rows().collect()[0]
        assert row.stats == hl.Struct(AC=[0, 0, 0], AF=None, AN=0, homozygote_count=[0, 0, 0])
    bad = mt.annotate_entries(GT=hl.call(0, 3))
    with pytest.raises(RuntimeError, match='allele index'):
        bad.annotate_rows(stats=hl.agg.call_stats(bad.GT, 2)).rows().collect()
    for n in [-1, hl.missing(hl.tint32)]:
        with pytest.raises(RuntimeError, match='allele count'):
            mt.annotate_rows(stats=hl.agg.call_stats(mt.GT, n)).rows().collect()


def test_matrix_call_stats_row_initializer_and_aligned_call_captures():
    mt = hl.range_matrix_table(2, 3, n_partitions=2)
    calls = hl.literal([
        hl.Call([0, 0]), hl.Call([0, 1]), None,
        hl.Call([0, 2]), hl.Call([1, 2]), hl.Call([1]),
    ], hl.tarray(hl.tcall))
    mt = mt.annotate_rows(n_alleles=hl.if_else(mt.row_idx == 0, 2, 3))
    mt = mt.annotate_entries(GT=calls[mt.row_idx * 3 + mt.col_idx])
    mt = mt.filter_entries(~((mt.row_idx == 0) & (mt.col_idx == 1)))

    call_with_axis_captures = hl.if_else(
        mt.col_idx == 2, hl.call(mt.row_idx), mt.GT
    )
    rows = mt.annotate_rows(
        stats=hl.agg.call_stats(call_with_axis_captures, mt.n_alleles)
    ).rows().collect()
    assert [row.stats for row in rows] == [
        hl.Struct(AC=[3, 0], AF=[1.0, 0.0], AN=3,
                  homozygote_count=[1, 0]),
        hl.Struct(AC=[1, 2, 2], AF=[0.2, 0.4, 0.4], AN=5,
                  homozygote_count=[0, 0, 0]),
    ]
