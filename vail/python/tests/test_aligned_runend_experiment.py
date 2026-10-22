"""Public behavior checks for the experimental whole-batch aligned map."""
import pytest
import vail as hl


@pytest.mark.parametrize("rows,cols", [(0, 3), (3, 0), (0, 0), (7, 5)])
def test_batch_captures_empty_and_missing_slots(rows, cols):
    m = hl.range_matrix_table(rows, cols, 3)
    m = m.annotate_globals(g=hl.struct(shift=17, words=hl.array(["global", "suffix"])))
    m = m.annotate_rows(meta=hl.struct(
        offset=m.row_idx * 10,
        label=hl.if_else(m.row_idx % 2 == 0, hl.str(m.row_idx), hl.missing(hl.tstr)),
        array=hl.array([m.row_idx, m.row_idx + 1]),
    ))
    m = m.annotate_cols(coef=m.col_idx * 3)
    m = m.annotate_entries(x=m.row_idx + m.col_idx)
    m = m.filter_entries((m.row_idx + m.col_idx) % 3 != 0)
    m = m.annotate_entries(
        y=m.x + m.meta.offset + m.coef + m.g.shift,
        label=hl.or_else(m.meta.label, "missing") + ":" + m.g.words[1],
        values=m.meta.array,
    )
    result = m.localize_entries("entries", "cols").collect()
    assert len(result) == rows
    for row in result:
        assert len(row.entries) == cols
        r = row.row_idx
        for c, entry in enumerate(row.entries):
            if (r + c) % 3 == 0:
                assert entry is None
            else:
                assert entry.y == 11 * r + 4 * c + 17
                assert entry.label == (str(r) if r % 2 == 0 else "missing") + ":suffix"
                assert entry["values"] == [r, r + 1]


def test_case_filtered_captures_follow_selected_entry_rows():
    m = hl.range_matrix_table(9, 4, 3)
    m = m.annotate_globals(shift=31)
    m = m.annotate_rows(offset=m.row_idx * 100)
    m = m.filter_entries((m.row_idx + m.col_idx) % 2 == 0)
    m = m.annotate_entries(value=hl.if_else(
        m.col_idx % 3 == 0,
        m.offset + m.shift + m.col_idx,
        -m.offset - m.shift - m.col_idx,
    ))
    result = m.localize_entries("entries", "cols").collect()
    assert len(result) == 9
    assert sorted(row.row_idx for row in result) == list(range(9))
    for row in result:
        assert len(row.entries) == 4
        for c, entry in enumerate(row.entries):
            if (row.row_idx + c) % 2:
                assert entry is None
            else:
                v = row.row_idx * 100 + 31 + c
                assert entry.value == (v if c % 3 == 0 else -v)


def test_column_occurrences_keep_metadata_alignment_after_gather():
    m = hl.range_matrix_table(7, 5, 3)
    m = m.choose_cols([4, 1, 4, 0, 3])
    m = m.annotate_globals(shift=7)
    m = m.filter_entries((m.row_idx + m.col_idx) % 3 != 0)
    m = m.annotate_entries(value=m.row_idx * 100 + m.col_idx + m.shift)
    result = m.localize_entries("entries", "cols").collect()
    assert len(result) == 7
    assert sorted(row.row_idx for row in result) == list(range(7))
    for row in result:
        assert len(row.entries) == 5
        for c, entry in zip([4, 1, 4, 0, 3], row.entries, strict=True):
            assert (entry is None) == ((row.row_idx + c) % 3 == 0)
            if entry is not None:
                assert entry.value == row.row_idx * 100 + c + 7


def test_multiple_same_schema_aligned_maps_keep_distinct_bodies():
    m = hl.range_matrix_table(7, 5, 3)
    m = m.annotate_entries(value=-1)
    for offset in range(1, 9):
        m = m.annotate_entries(value=m.row_idx * 100 + m.col_idx * 3 + offset)
    result = m.localize_entries("entries", "cols").collect()
    assert len(result) == 7
    assert sorted(row.row_idx for row in result) == list(range(7))
    for row in result:
        assert len(row.entries) == 5
        assert [entry.value for entry in row.entries] == [row.row_idx * 100 + c * 3 + 8 for c in range(5)]
