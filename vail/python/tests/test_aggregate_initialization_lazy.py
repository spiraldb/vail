"""Initialization follows Hail init_op_args, not the surviving row stream.

Expected identities follow aggregators.py and CallStatsAggregator.scala's
result: allele counts are zero and AF is missing when AN is zero.
"""
import pytest
import vail as hl


def test_initializers_read_globals_only_when_action_runs(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(4).annotate_globals(start=0., end=4., bins=2, q=.5, initial=7,
                                      alleles=['A', 'C']).write(path)
    table = hl.read_table(path)
    globals_file = next(path.rglob('globals.arrow'))
    saved = globals_file.read_bytes()
    globals_file.unlink()
    try:
        expression = table.aggregate(hl.struct(
            hist=hl.agg.hist(table.idx, table.start, table.end, table.bins),
            q=hl.agg.approx_quantiles(table.idx, table.q),
            fold=hl.agg.fold(table.initial, lambda s: s + table.idx, lambda a, b: a + b),
            calls=hl.agg.call_stats(hl.call(0, 1), table.alleles)), _localize=False)
        with pytest.raises(Exception, match='(?i)(not found|no such file)'):
            hl.eval(expression)
    finally:
        globals_file.write_bytes(saved)
    result = hl.eval(expression)
    assert result.hist.bin_freq == [2, 2]
    assert result.q == 2
    assert result.fold == 13
    assert result.calls.AC == [4, 4]
    # The expression remains a plan: a second action must read globals again.
    globals_file.unlink()
    try:
        with pytest.raises(Exception, match='(?i)(not found|no such file)'):
            hl.eval(expression)
    finally:
        globals_file.write_bytes(saved)


def test_empty_aggregate_scopes_keep_global_initialized_identities():
    table = hl.range_table(4).annotate_globals(start=0., end=4., bins=2,
                                             initial=7, alleles=['A', 'C'])
    result = table.group_by(k=table.idx % 2).aggregate(
        h=hl.agg.filter(False, hl.agg.hist(table.idx, table.start, table.end, table.bins)),
        f=hl.agg.filter(False, hl.agg.fold(table.initial, lambda s: s + table.idx,
                                         lambda a, b: a + b)),
        cs=hl.agg.filter(False, hl.agg.call_stats(hl.call(0, 1), table.alleles)))
    rows = result.collect()
    assert [row.k for row in rows] == [0, 1]
    for row in rows:
        assert row.h.bin_edges == [0., 2., 4.]
        assert row.h.bin_freq == [0, 0]
        assert row.f == 7
        assert row.cs == hl.Struct(AC=[0, 0], AF=None, AN=0, homozygote_count=[0, 0])


def test_external_initializers_do_not_scan_their_source_rows(tmp_path):
    path = tmp_path / 'external'
    hl.range_table(4).annotate_globals(initial=hl.struct(n=3, total=7), bins=2).write(path)
    captured = hl.read_table(path).index_globals()
    for file in path.rglob('*.vortex'):
        file.unlink()
    table = hl.range_table(0)
    result = table.aggregate(hl.agg.fold(captured.initial, lambda s: s,
                                        lambda a, b: hl.struct(n=a.n+b.n, total=a.total+b.total)))
    assert result == hl.Struct(n=3, total=7)


def test_initializers_can_depend_on_a_lazy_aggregate_relation():
    source = hl.range_table(4)
    parameters = source.aggregate(hl.struct(
        start=hl.agg.min(source.idx), end=hl.agg.max(source.idx) + 1,
        bins=hl.int32(hl.agg.count()), initial=hl.agg.sum(source.idx)), _localize=False)
    empty = hl.range_table(0)
    result = empty.aggregate(hl.struct(
        hist=hl.agg.hist(empty.idx, parameters.start, parameters.end, parameters.bins),
        fold=hl.agg.fold(parameters.initial, lambda state: state + empty.idx,
                         lambda a, b: a + b)))
    assert result.hist.bin_edges == [0., 1., 2., 3., 4.]
    assert result.hist.bin_freq == [0, 0, 0, 0]
    assert result.fold == 6
