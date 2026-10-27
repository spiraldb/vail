"""Tiny public API correctness check; not a throughput benchmark."""
import vail as h

def test_full_projection_constructor_preserves_all_fields_and_full_result():
    m=h.range_matrix_table(2,3)
    m=m.annotate_entries(GQ=m.row_idx*10+m.col_idx,payload=h.range(m.col_idx+1))
    t=m.entries()
    full=t.select(*[name for name in t.row.dtype if name not in t.key.dtype])
    assert set(full.row.dtype)=={'row_idx','col_idx','GQ','payload'}
    rows=full.collect()
    assert sorted((r.row_idx,r.col_idx,r.GQ,r.payload) for r in rows)==[
        (r,c,r*10+c,list(range(c+1))) for r in range(2) for c in range(3)]
    expected=h.Struct(total=36,present=6,high=0)
    for table in (t.select('GQ'),full):
        assert table.aggregate(h.struct(total=h.agg.sum(table.GQ),
            present=h.agg.count_where(h.is_defined(table.GQ)),high=h.agg.count_where(table.GQ>=20)))==expected
