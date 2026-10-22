"""Lexical bindings preserve nested callback scopes without singleton list copies."""
import pytest
import vail as hl


def test_bind_null_struct_arrays_and_zero_rows():
    table = hl.Table.parallelize([
        {'x': 2, 'a': [1, None, 3]}, {'x': None, 'a': []}, {'x': 4, 'a': None},
    ], schema=hl.tstruct(x=hl.tint32, a=hl.tarray(hl.tint32)))
    result = table.annotate(value=hl.bind(
        lambda x, a: hl.struct(x=x, a=a, empty=hl.struct()), table.x, table.a,
    )).collect()
    assert [r.value.x for r in result] == [2, None, 4]
    assert [r.value.a for r in result] == [[1, None, 3], [], None]
    assert all(r.value.empty == hl.Struct() for r in result)
    empty = table.head(0)
    assert empty.annotate(value=hl.bind(lambda x: x + 1, empty.x)).collect() == []


def test_bind_nested_maps_cases_and_outer_captures():
    t = hl.range_table(4)
    result = t.annotate(value=hl.range(3).map(lambda i: hl.bind(
        lambda x: hl.if_else(x % 2 == 0,
            hl.bind(lambda y: y + t.idx, x * 2), x + i), t.idx + i,
    ))).value.collect()
    assert result == [[(r+i)*2+r if (r+i)%2 == 0 else r+2*i for i in range(3)]
                      for r in range(4)]


def test_bind_does_not_evaluate_unselected_branch():
    t = hl.range_table(4)
    result = t.annotate(value=hl.if_else(t.idx < 2,
        hl.bind(lambda x: 10 // x, 2-t.idx),
        hl.bind(lambda x: x + 1, t.idx),
    )).value.collect()
    assert result == [5, 10, 3, 4]


def test_dosage_with_nested_likelihood_lists_and_missing_entries():
    m = hl.utils.range_matrix_table(3, 3)
    pl = hl.array([m.row_idx * 10, m.col_idx * 10, 20])
    m = m.annotate_entries(PL=hl.or_missing(m.row_idx != m.col_idx, pl))
    rows = m.select_entries(dosage=hl.pl_dosage(m.PL)).localize_entries('entries', 'cols').collect()
    for r, row in enumerate(rows):
        for c, entry in enumerate(row.entries):
            if r == c:
                assert entry.dosage is None
            else:
                p = [10**(-r), 10**(-c), 0.01]
                assert entry.dosage == pytest.approx((p[1]+2*p[2])/sum(p))
