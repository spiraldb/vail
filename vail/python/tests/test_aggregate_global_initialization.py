"""Aggregate state initialization must evaluate table globals once per action."""

import pytest
import vail as hl


def test_hist_quantiles_and_call_stats_initialize_from_globals():
    table = hl.Table.parallelize(
        [{"x": 0.0, "gt": hl.Call((0, 1))}, {"x": 1.0, "gt": hl.Call((1, 1))},
         {"x": 2.0, "gt": hl.Call((0, 0))}, {"x": 3.0, "gt": hl.Call((0, 1))}],
        schema=hl.tstruct(x=hl.tfloat64, gt=hl.tcall), n_partitions=2,
        globals={"start": 0.0, "end": 4.0, "bins": 2,
                 "qs": [0.0, 0.5, 1.0], "alleles": ["A", "C"]},
    )

    hist = table.aggregate(hl.agg.hist(table.x, table.start, table.end, table.bins))
    assert hist.bin_edges == [0.0, 2.0, 4.0]
    assert hist.bin_freq == [2, 2]
    assert table.aggregate(hl.agg.approx_quantiles(table.x, table.qs, k=128)) == [0.0, 2.0, 3.0]
    stats = table.aggregate(hl.agg.call_stats(table.gt, table.alleles))
    assert stats.AC == [4, 4]
    assert stats.AN == 8


def test_global_initialized_aggregates_on_empty_filtered_and_grouped_tables():
    table = hl.range_table(6, n_partitions=3).annotate_globals(start=0.0, end=6.0, bins=3,
                                                                 qs=[0.0, 1.0], initial=10)
    empty = table.filter(table.idx < 0)
    h = empty.aggregate(hl.agg.hist(empty.idx, empty.start, empty.end, empty.bins))
    assert h.bin_edges == [0.0, 2.0, 4.0, 6.0]
    assert h.bin_freq == [0, 0, 0]
    assert empty.aggregate(hl.agg.approx_quantiles(empty.idx, empty.qs)) == [None, None]
    assert empty.aggregate(hl.agg.fold(empty.initial, lambda state: state + empty.idx,
                                       lambda left, right: left + right)) == 10

    empty_calls = hl.Table.parallelize([], schema=hl.tstruct(gt=hl.tcall),
                                       globals={"alleles": ["A", "C"]})
    empty_calls = empty_calls.annotate(gt=hl.missing(hl.tcall))
    call_stats = empty_calls.aggregate(hl.agg.call_stats(empty_calls.gt, empty_calls.alleles))
    assert call_stats.AC == [0, 0]
    assert call_stats.AF is None
    assert call_stats.AN == 0

    filtered = table.filter(table.idx >= 3)
    assert filtered.aggregate(hl.agg.hist(filtered.idx, filtered.start, filtered.end,
                                          filtered.bins)).bin_freq == [0, 1, 2]
    grouped = table.group_by(parity=table.idx % 2).aggregate(
        total=hl.agg.fold(table.initial, lambda state: state + table.idx,
                          lambda left, right: left + right))
    assert {row.parity: row.total for row in grouped.collect()} == {0: 16, 1: 19}


def test_aggregate_initializers_keep_captured_external_globals_lazy():
    globals_table = hl.range_table(1).annotate_globals(start=0.0, end=4.0, bins=2)
    captured = globals_table.index_globals()
    source = hl.range_table(4)
    # Indexing globals captures the native singleton relation without evaluating it.
    start = captured["start"]
    end = captured["end"]
    bins = captured["bins"]
    assert source.aggregate(hl.agg.hist(hl.float64(source.idx), start, end, bins)).bin_freq == [2, 2]


def test_global_and_mapped_quantile_initializers_execute():
    table = hl.range_table(2).annotate_globals(qs=[0.25, 0.75], start=0.0, end=2.0, bins=2)
    assert table.aggregate(hl.agg.approx_quantiles(table.idx, table.qs)) == [0, 1]
    assert table.aggregate(hl.agg.hist(table.idx, table.start, table.end, table.bins)).bin_freq == [1, 1]
    assert table.aggregate(hl.agg.approx_quantiles(table.idx, table.qs.map(lambda q: q + 0.0))) == [0, 1]


def test_missing_global_quantiles_and_allele_shape_follow_hail():
    table = hl.range_table(4).annotate_globals(
        q=hl.missing(hl.tfloat64),
        qs=[0.0, hl.missing(hl.tfloat64), 1.0],
    )
    assert table.aggregate(hl.agg.approx_quantiles(table.idx, table.q)) is None
    assert table.aggregate(hl.agg.approx_quantiles(table.idx, table.qs)) == [0, None, 3]

    no_alleles = hl.Table.parallelize([], schema=hl.tstruct(gt=hl.tcall),
                                      globals={"alleles": hl.missing(hl.tarray(hl.tstr))})
    with pytest.raises(ValueError, match="n_alleles may not be missing"):
        no_alleles.aggregate(hl.agg.call_stats(no_alleles.gt, no_alleles.alleles))


def test_row_dependent_initializers_are_rejected():
    table = hl.range_table(3)
    with pytest.raises(TypeError, match="initialization expressions cannot depend on rows"):
        hl.agg.hist(hl.float64(table.idx), 0.0, 3.0, table.idx)
    with pytest.raises(TypeError, match="cannot depend on rows"):
        hl.agg.fold(table.idx, lambda state: state + table.idx, lambda a, b: a + b)


def test_initializer_literal_payload_resembling_ir_is_ordinary_data():
    initial = hl.literal(hl.Struct(op='column', name='payload'))
    table = hl.range_table(3)
    assert table.aggregate(hl.agg.fold(initial, lambda state: state, lambda left, right: left)) == hl.Struct(op='column', name='payload')
