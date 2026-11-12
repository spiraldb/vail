"""String producers can be shared without changing codepoint or null semantics."""
import math
import pytest
import vail as hl


@pytest.mark.parametrize('partitions', [1, 3])
def test_unicode_length_siblings_keep_nulls_and_filtered_domains(partitions):
    texts = ['', 'a', 'é', '🙂', 'e\u0301', '東京', None, 'Ångström']
    values = hl.literal(texts, hl.tarray(hl.tstr))
    m = hl.range_matrix_table(3, 257, n_partitions=partitions)
    m = m.annotate_entries(payload=hl.or_missing(
        m.col_idx % 7 != 0, hl.struct(text=values[m.col_idx % len(texts)])))
    m = m.filter_entries((m.row_idx != 2) & ((m.row_idx == 0) | (m.col_idx % 5 == 0)))
    lengths = hl.len(m.payload.text)
    rows = m.annotate_rows(
        short=hl.agg.count_where(lengths <= 1),
        long=hl.agg.count_where(lengths > 1),
        fraction=hl.agg.fraction(lengths > 1),
    ).rows().collect()
    for row in rows:
        chosen = [c for c in range(257) if row.row_idx != 2 and (row.row_idx == 0 or c % 5 == 0)]
        defined = [texts[c % len(texts)] for c in chosen if c % 7 != 0 and texts[c % len(texts)] is not None]
        assert row.short == sum(len(text) <= 1 for text in defined)
        assert row.long == sum(len(text) > 1 for text in defined)
        assert row.fraction == row.long / len(chosen) if chosen else math.isnan(row.fraction)
