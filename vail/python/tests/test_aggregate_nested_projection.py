"""Consumed aggregate inputs retain nested values, shape and logical validity."""
import vail as hl


def test_native_matrix_totals_keep_parent_validity_with_missing_selected_children(tmp_path):
    m = hl.utils.range_matrix_table(3, 130, 2)
    m = m.annotate_entries(
        x=hl.or_missing(m.col_idx % 5 != 1, hl.int32(m.row_idx * 130 + m.col_idx)),
        y=hl.or_missing(m.col_idx % 5 != 1, hl.int32(2)),
        unused=hl.range(12),
    ).filter_entries(m.col_idx % 3 != 0)
    path = str(tmp_path / 'matrix.mt')
    m.write(path)
    m = hl.read_matrix_table(path)
    present = [(r,c) for r in range(3) for c in range(130) if c % 3 != 0]
    expected = hl.Struct(x=sum(r*130+c for r,c in present if c % 5 != 1),
                         y=2*sum(c % 5 != 1 for r,c in present),n=len(present))
    assert m.aggregate_entries(hl.struct(x=hl.agg.sum(m.x),y=hl.agg.sum(m.y),n=hl.agg.count())) == expected
    assert m.aggregate_entries(hl.agg.count()) == len(present)


def _native_table(tmp_path):
    t = hl.utils.range_table(4, 2)
    t = t.annotate(g=t.idx % 2, xs=hl.range(130).map(lambda i: hl.struct(
        x=hl.or_missing(i % 7 != 0, i),y=2*i,unused=hl.str(i))))
    path = str(tmp_path / 'table.ht')
    t.write(path)
    return hl.read_table(path)


def test_native_grouped_nested_inputs_keep_group_keys(tmp_path):
    t = _native_table(tmp_path)
    grouped = t.group_by(t.g).aggregate(s=hl.agg.sum(hl.sum(t.xs.map(lambda e:e.x))))
    expected = 2*sum(i for i in range(130) if i % 7 != 0)
    assert sorted((r.g,r.s) for r in grouped.collect()) == [(0,expected),(1,expected)]


def test_independent_aggregate_consumers_do_not_share_pruned_scan_cache(tmp_path):
    t = _native_table(tmp_path)
    x = t.aggregate(hl.agg.sum(hl.sum(t.xs.map(lambda e:e.x))),_localize=False)
    y = t.aggregate(hl.agg.sum(hl.sum(t.xs.map(lambda e:e.y))),_localize=False)
    assert hl.eval(x+y) == 4*sum(i for i in range(130) if i % 7 != 0)+8*sum(range(130))
    # A later whole-container consumer must still receive the original fields.
    rows = t.collect()
    assert rows[0].xs[1] == hl.Struct(x=1,y=2,unused='1')


def test_nested_array_shape_and_filtered_inputs_keep_semantics(tmp_path):
    t = _native_table(tmp_path)
    t = t.filter(t.g == 1)
    assert t.aggregate(hl.agg.sum(hl.len(t.xs))) == 260
    assert t.aggregate(hl.agg.sum(hl.sum(t.xs.map(lambda e:e.y)))) == 4*sum(range(130))
