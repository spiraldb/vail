"""Public semantics of entry filtering with captured matrix globals."""
import pytest
import vail as hl


@pytest.mark.parametrize('keep', [True, False])
@pytest.mark.parametrize('global_missing', [True, False])
def test_global_entry_filter_preserves_slots_metadata_and_column_occurrences(keep, global_missing):
    m = hl.range_matrix_table(4, 3, 3)
    m = m.annotate_globals(cutoff=hl.missing(hl.tint32) if global_missing else 2)
    m = m.annotate_entries(
        x=m.row_idx * 10 + m.col_idx,
        call=hl.call(0, 1, phased=True),
        nested=hl.struct(values=hl.array([hl.int32(1), hl.missing(hl.tint32)])),
    )
    m = m.filter_entries(m.col_idx != 1).choose_cols([2, 0, 2])
    selected = m.filter_entries((m.x + m.row_idx + m.col_idx) % 5 < m.cutoff, keep=keep)
    rows = selected.localize_entries('entries', 'cols').collect()
    assert len(rows) == 4
    for r, row in enumerate(rows):
        assert len(row.entries) == 3
        for c, entry in zip([2, 0, 2], row.entries):
            chosen = not global_missing and (((r * 10 + c + r + c) % 5 < 2) == keep)
            if chosen:
                assert entry.x == r * 10 + c
                assert entry.call == hl.Call([0, 1], phased=True)
                assert entry.nested["values"] == [1, None]
            else:
                assert entry is None
    assert selected.count() == (4, 3)
    assert selected.entry.dtype == m.entry.dtype


@pytest.mark.parametrize('rows,cols', [(0, 3), (3, 0), (0, 0)])
def test_global_entry_filter_empty_axes(rows, cols):
    m = hl.range_matrix_table(rows, cols, 2).annotate_globals(keep=True)
    result = m.filter_entries(m.keep)
    assert result.count() == (rows, cols)
    localized = result.localize_entries('entries', 'cols').collect()
    assert len(localized) == rows
    assert all(len(row.entries) == cols for row in localized)


def test_global_entry_filter_does_not_evaluate_removed_entry_predicate():
    m = hl.range_matrix_table(3, 3, 2).annotate_globals(cutoff=0)
    m = m.annotate_entries(x=m.col_idx)
    m = m.filter_entries(m.col_idx != 0)
    result = m.filter_entries((10 // m.x) > m.cutoff)
    assert result.x.collect() == [1, 2] * 3


def test_global_entry_filter_preserves_defined_empty_entry_structs():
    m = hl.range_matrix_table(2, 3, 2).annotate_globals(keep=True)
    m = m.filter_entries(m.col_idx != 1)
    result = m.filter_entries(m.keep)
    assert result.entry.dtype == hl.tstruct()
    assert result.aggregate_entries(hl.agg.count()) == 4
    rows = result.localize_entries('entries', 'cols').collect()
    assert [[entry is not None for entry in row.entries] for row in rows] == [[True, False, True]] * 2


def test_global_entry_filter_retains_lookup_fallback():
    m = hl.range_matrix_table(4, 2, 2).annotate_globals(offset=1)
    thresholds = hl.Table.parallelize([{'row_idx': i, 'limit': i % 2} for i in range(4)], key='row_idx')
    m = m.annotate_entries(x=m.row_idx * 10 + m.col_idx)
    filtered = m.filter_entries(m.col_idx + m.offset > thresholds[m.row_idx].limit)
    assert filtered.x.collect() == [0, 1, 11, 20, 21, 31]
