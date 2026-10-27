"""Entry consumers retain multiplicity, logical missingness and key order."""
import pytest
import vail as hl


@pytest.mark.parametrize('select_axis', ['none', 'entries', 'rows', 'both'])
def test_projected_entry_consumers(tmp_path, select_axis):
    m = hl.range_matrix_table(12, 5, 4)
    m = m.annotate_rows(label=hl.str(m.row_idx))
    m = m.annotate_entries(
        number=m.row_idx * 10 + m.col_idx,
        payload=hl.range(m.col_idx + 1),
        text=hl.str(m.row_idx) + ':' + hl.str(m.col_idx),
        nested=hl.struct(x=m.row_idx, y=hl.float64(m.col_idx)),
        absent=hl.missing(hl.tint32),
    )
    # Duplicate row and column keys require ordered run expansion, rather than
    # the unique-key fast path. Filtered entries differ from missing fields.
    m = m.key_rows_by(r=m.row_idx // 2)
    m = m.key_cols_by(c=m.col_idx // 2)
    m = m.filter_entries((m.row_idx + m.col_idx) % 3 != 0)
    path = str(tmp_path / 'source.mt')
    m.write(path)
    m = hl.read_matrix_table(path)
    wide_entries = m.entries()
    wide = wide_entries.select('text', 'payload', 'nested').collect()
    expected_wide = sorted((r // 2, c // 2, str(r) + ':' + str(c), list(range(c + 1)), r, float(c))
                           for r in range(12) for c in range(5) if (r + c) % 3 != 0)
    assert sorted((x.r, x.c, x.text, x.payload, x.nested.x, x.nested.y) for x in wide) == expected_wide
    if select_axis in ('entries', 'both'):
        m = m.select_entries('number', 'absent')
    if select_axis in ('rows', 'both'):
        m = m.select_rows('row_idx')
    entries = m.entries()
    expected = sorted((r // 2, c // 2, r * 10 + c)
                      for r in range(12) for c in range(5) if (r + c) % 3 != 0)
    actual = [(x.r, x.c, x.number) for x in entries.select('number').collect()]
    assert sorted(actual) == expected
    assert [(x[0], x[1]) for x in actual] == sorted((r, c) for r, c, _ in expected)
    assert entries.aggregate(hl.agg.sum(entries.number)) == sum(x[2] for x in expected)
    assert entries.aggregate(hl.agg.count_where(hl.is_missing(entries.absent))) == len(expected)
    assert entries.select().count() == len(expected)
    assert [(x.r, x.c) for x in entries.select().head(7).collect()] == [(r, c) for r, c, _ in actual[:7]]


def test_empty_entry_projection_retains_cardinality():
    m = hl.range_matrix_table(3, 4, 2)
    m = m.key_rows_by(r=m.row_idx // 2)
    m = m.filter_entries(m.col_idx != 1)
    rows = m.entries().key_by().select().collect()
    assert len(rows) == 9
    assert all(row == hl.Struct() for row in rows)


def test_changed_keys_keep_sorted_entry_semantics(tmp_path):
    m = hl.range_matrix_table(9, 3, 4)
    m = m.annotate_entries(value=m.row_idx * 10 + m.col_idx)
    path = str(tmp_path / 'rekey.mt')
    m.write(path)
    m = hl.read_matrix_table(path)
    m = m.key_rows_by(reversed=-m.row_idx)
    m = m.select_entries('value').select_rows()
    actual = m.entries().select('value').collect()
    assert [(x.reversed, x.col_idx, x.value) for x in actual] == [
        (-r, c, r * 10 + c) for r in reversed(range(9)) for c in range(3)
    ]


def test_entry_projection_aliases_do_not_mutate_shared_consumer(tmp_path):
    m = hl.range_matrix_table(7, 4, 3)
    m = m.annotate_rows(label=hl.str(m.row_idx))
    m = m.annotate_entries(value=m.row_idx * 10 + m.col_idx,
                           unused=hl.struct(payload=hl.range(m.col_idx + 2)))
    m = m.key_rows_by(r=m.row_idx // 2)
    m = m.key_cols_by(c=m.col_idx // 2)
    m = m.filter_entries((m.row_idx + m.col_idx) % 3 != 0)
    path = str(tmp_path / 'shared.mt')
    m.write(path)
    source = hl.read_matrix_table(path).entries()
    narrow = source.select(alias=source.value)
    wide_before = source.collect()
    assert sorted((x.r, x.c, x.alias) for x in narrow.collect()) == sorted(
        (r // 2, c // 2, r * 10 + c)
        for r in range(7) for c in range(4) if (r + c) % 3 != 0)
    wide_after = source.collect()
    assert wide_before == wide_after
    assert all(x.unused.payload == list(range(x.value % 10 + 2)) for x in wide_after)
    # A computed consumer is outside the simple-column admission and must
    # preserve its existing semantics independently of the shared projection.
    computed = source.select(value=source.value + 1)
    assert sorted((x.r, x.c, x.value) for x in computed.collect()) == sorted(
        (r // 2, c // 2, r * 10 + c + 1)
        for r in range(7) for c in range(4) if (r + c) % 3 != 0)


@pytest.mark.parametrize('rows', [129, 4097])
def test_projected_entry_values_match_full_nullable_skewed_output(rows):
    m = hl.range_matrix_table(rows, 3, 3)
    m = m.annotate_entries(
        value=hl.or_missing((m.row_idx + m.col_idx) % 5 != 0, m.row_idx * 10 + m.col_idx),
        payload=hl.or_missing(m.row_idx % 7 != 0,
                              hl.if_else(m.row_idx == 1, 'x' * 32768, 'y')),
    )
    m = m.key_rows_by(r=m.row_idx // 2)
    m = m.filter_entries((m.row_idx + m.col_idx) % 11 != 0)
    source = m.entries()
    full = source.collect()
    projected = source.select('value').collect()
    assert [(x.r, x.col_idx, x.value) for x in projected] == [
        (x.r, x.col_idx, x.value) for x in full]
    assert sorted((x.r, x.col_idx, x.value) for x in projected if x.value is not None) == sorted(
        (r // 2, c, r * 10 + c) for r in range(rows) for c in range(3)
        if (r + c) % 11 != 0 and (r + c) % 5 != 0)
