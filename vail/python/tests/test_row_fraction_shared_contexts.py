"""Named row reductions share entry contexts without changing empty identities."""
import math
import pytest
import vail as hl

@pytest.mark.parametrize('columns', [0, 7])
@pytest.mark.parametrize('filtered', [False, True])
def test_named_fractions_preserve_sparse_and_empty_scopes(columns, filtered):
    m = hl.utils.range_matrix_table(3, columns, 1)
    m = m.annotate_entries(x=hl.or_missing(m.col_idx % 3 != 0, m.row_idx + m.col_idx))
    if filtered:
        m = m.filter_entries((m.row_idx != 1) & (m.col_idx % 2 == 0))
    result = m.annotate_rows(
        low=hl.agg.fraction(m.x > 1), high=hl.agg.fraction(m.x > 4),
        count=hl.agg.count(), total=hl.agg.sum(m.x), mean=hl.agg.mean(m.x),
    ).rows().collect()
    for r in result:
        cols = [c for c in range(columns) if not filtered or (r.row_idx != 1 and c % 2 == 0)]
        values = [r.row_idx + c for c in cols if c % 3 != 0]
        assert r.count == len(cols)
        assert r.total == sum(values)
        for actual, threshold in [(r.low, 1), (r.high, 4)]:
            if cols:
                assert actual == sum(v > threshold for v in values) / len(cols)
            else:
                assert math.isnan(actual)
        if values:
            assert r.mean == sum(values) / len(values)
        else:
            assert math.isnan(r.mean)


def test_shared_fraction_keeps_dead_errors_and_filtered_scope():
    m = hl.utils.range_matrix_table(2, 4, 1)
    m = m.filter_entries(m.col_idx != 0)
    safe = hl.if_else(m.col_idx == 0, hl.int32('not-an-int'), m.col_idx)
    r = m.annotate_rows(
        f=hl.agg.fraction(safe > 1),
        g=hl.agg.fraction(safe > 2),
        subset=hl.agg.filter(m.col_idx > 2, hl.agg.fraction(safe > 2)),
    ).rows().collect()
    for row in r:
        assert row.f == 2 / 3
        assert row.g == 1 / 3
        assert row.subset == 1.0
