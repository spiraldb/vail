"""Call indexing preserves list semantics without requiring an allele list."""
from collections import Counter

import pytest

import vail as hl


@pytest.mark.parametrize('index', [0, 1, -1, -2])
def test_call_index_native_roundtrip_and_nested_arrays(tmp_path, index):
    calls = [None, hl.Call([65535, 0], True), hl.Call([0, 32767])]
    table = hl.Table.parallelize([{'GT': c, 'nested': [c, None]} for c in calls],
                                schema=hl.tstruct(GT=hl.tcall, nested=hl.tarray(hl.tcall)))
    path = str(tmp_path / 'calls.ht')
    table.write(path)
    for source in [table, hl.read_table(path)]:
        rows = source.select(a=source.GT[index],
                             b=source.GT.alleles[index],
                             nested=source.nested.map(lambda c: c[index])).collect()
        assert rows == [hl.Struct(a=None if c is None else c.alleles[index],
                                 b=None if c is None else c.alleles[index],
                                 nested=[None if c is None else c.alleles[index], None]) for c in calls]


def test_call_index_dynamic_haploid_missing_and_filtered_errors():
    values = [(hl.Call([65535]), -1), (hl.Call([65535, 2], True), 1),
              (None, 2147483647), (hl.Call([]), None)]
    table = hl.Table.parallelize([{'GT': c, 'i': i} for c, i in values],
                                schema=hl.tstruct(GT=hl.tcall, i=hl.tint32))
    assert table.select(value=table.GT[table.i]).collect() == [
        hl.Struct(value=65535), hl.Struct(value=2), hl.Struct(value=None), hl.Struct(value=None)]
    selected = table.filter(hl.is_defined(table.GT) & (table.GT.ploidy > 0))
    assert selected.select(value=selected.GT[selected.i]).collect() == [
        hl.Struct(value=65535), hl.Struct(value=2)]
    assert table.select(value=hl.if_else(table.GT.ploidy == 2, table.GT[1], -1)).collect() == [
        hl.Struct(value=-1), hl.Struct(value=2), hl.Struct(value=None), hl.Struct(value=-1)]


@pytest.mark.parametrize('call,index', [(hl.Call([]), 0), (hl.Call([1]), 1),
                                      (hl.Call([1]), -2), (hl.Call([1, 2]), 2),
                                      (hl.Call([1, 2]), -3)])
def test_call_index_preserves_bounds_errors(call, index):
    with pytest.raises((ValueError, RuntimeError), match='index out of bounds'):
        hl.eval(hl.literal(call)[index])


def test_call_index_matrix_aggregates_and_sparse_entries(tmp_path):
    mt = hl.range_matrix_table(7, 5, n_partitions=3)
    mt = mt.annotate_entries(GT=hl.call(mt.row_idx, mt.col_idx, phased=True))
    mt = mt.filter_entries((mt.row_idx + mt.col_idx) % 3 != 0)
    path = str(tmp_path / 'calls.mt')
    mt.write(path)
    for matrix in [mt, hl.read_matrix_table(path)]:
        assert matrix.aggregate_entries(hl.agg.sum(matrix.GT[0])) == sum(
            r for r in range(7) for c in range(5) if (r + c) % 3 != 0)
        assert matrix.aggregate_entries(hl.agg.sum(matrix.GT[-1])) == sum(
            c for r in range(7) for c in range(5) if (r + c) % 3 != 0)
        actual = {row.row_idx: Counter(row["values"]) for row in matrix.annotate_rows(
            values=hl.agg.collect(matrix.GT[1])).rows().collect()}
        assert actual == {r: Counter(c for c in range(5) if (r + c) % 3 != 0) for r in range(7)}
