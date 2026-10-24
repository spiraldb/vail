"""Public total bundles retain sparse semantics without scalar special cases."""
import pytest
import vail as hl


@pytest.mark.parametrize("rows,cols", [(0, 4), (3, 0), (3, 4)])
def test_composite_totals_nested_sparse_and_lazy(rows, cols):
    m = hl.utils.range_matrix_table(rows, cols, 2).annotate_globals(offset=7)
    m = m.annotate_entries(x=hl.or_missing(m.col_idx != 2, hl.int32(m.row_idx + m.col_idx)))
    m = m.filter_entries(m.col_idx != 1)
    query = hl.struct(
        n=hl.agg.count(),
        totals=hl.tuple([hl.agg.sum(m.x), hl.agg.sum(hl.or_else(m.x, 5))]),
        row=hl.agg.sum(m.row_idx),
        global_value=hl.agg.sum(m.offset),
    )
    present = [(r,c) for r in range(rows) for c in range(cols) if c != 1]
    expected = hl.Struct(n=len(present), totals=(sum(r+c for r,c in present if c != 2),
        sum(r+c if c != 2 else 5 for r,c in present)), row=sum(r for r,c in present),
        global_value=7*len(present))
    assert m.aggregate_entries(query) == expected
    lazy = m.aggregate_entries(query, _localize=False)
    assert hl.eval(lazy) == expected
    assert hl.eval(lazy) == expected


def test_composite_integer_precision_wrap_and_native_roundtrip(tmp_path):
    m = hl.utils.range_matrix_table(2, 3, 2)
    m = m.annotate_entries(x=hl.if_else(m.col_idx == 0, hl.int64(9007199254740993), hl.int64(2)))
    path = str(tmp_path / 'matrix.mt')
    m.write(path)
    m = hl.read_matrix_table(path)
    assert m.aggregate_entries(hl.struct(s=hl.agg.sum(m.x), n=hl.agg.count())) == hl.Struct(s=18014398509481994,n=6)


def test_composite_unsupported_reducers_and_columns_keep_results():
    m = hl.utils.range_matrix_table(2, 3, 2)
    m = m.annotate_entries(x=hl.int32(m.row_idx + m.col_idx))
    assert m.aggregate_entries(hl.struct(s=hl.agg.sum(m.x), mean=hl.agg.mean(m.x))) == hl.Struct(s=9,mean=1.5)
    assert m.aggregate_entries(hl.struct(s=hl.agg.sum(hl.int64(m.col_idx)), n=hl.agg.count())) == hl.Struct(s=6,n=6)
    assert m.aggregate_entries(hl.struct(s=hl.agg.filter(m.col_idx > 0, hl.agg.sum(m.x)), n=hl.agg.count())) == hl.Struct(s=8,n=6)
