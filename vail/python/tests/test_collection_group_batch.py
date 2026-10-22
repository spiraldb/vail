"""Batched list grouping keeps row ownership and filtered input alignment."""
import vail as hl


def test_counter_keeps_outer_lists_independent():
    m = hl.range_matrix_table(5, 7, 1)
    m = m.annotate_entries(x=hl.or_missing(m.col_idx != 3, (m.row_idx + m.col_idx) % 3))
    m = m.filter_entries((m.row_idx != 1) & ((m.row_idx != 4) | (m.col_idx == 0)))
    result = m.annotate_rows(c=hl.agg.counter(m.x)).c.collect()
    expected = []
    for r in range(5):
        counts = {}
        for c in range(7):
            if r == 1 or (r == 4 and c != 0):
                continue
            x = None if c == 3 else (r + c) % 3
            counts[x] = counts.get(x, 0) + 1
        expected.append(counts)
    assert result == expected


def test_grouped_filtered_reducers_preserve_owners():
    m = hl.range_matrix_table(5, 7, 1)
    m = m.annotate_entries(x=m.row_idx * 10 + m.col_idx)
    m = m.filter_entries((m.row_idx != 1) & (m.col_idx != 2))
    result = m.annotate_rows(g=hl.agg.group_by(
        m.col_idx % 2,
        hl.struct(n=hl.agg.count(), total=hl.agg.filter(m.col_idx > 3, hl.agg.sum(m.x))),
    )).g.collect()
    expected = []
    for r in range(5):
        groups = {}
        for c in range(7):
            if r == 1 or c == 2:
                continue
            key = c % 2
            n, total = groups.get(key, (0, 0))
            groups[key] = (n + 1, total + (r * 10 + c if c > 3 else 0))
        expected.append({key: hl.Struct(n=n, total=total) for key, (n, total) in groups.items()})
    assert result == expected
