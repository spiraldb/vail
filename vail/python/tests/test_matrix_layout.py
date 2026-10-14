"""Column occurrences stay aligned with entry slots through lazy transforms."""

import pytest
import vail as hl
from vail.matrixtable import (
    MatrixIR,
    MatrixTable,
    _CID,
    _ROW,
    _COL,
    _CELLS,
    _PRESENT,
    _VALUE,
)


def assert_layout(matrix, original_columns):
    columns = hl.eval(matrix._mir.column_records)
    assert [record[_COL].col_idx for record in columns] == original_columns
    identities = [record[_CID] for record in columns]
    assert len(set(identities)) == len(identities)
    for row in matrix._mir.core.collect():
        cells = row[_CELLS]
        assert len(cells) == len(columns)
        assert [cell[_CID] for cell in cells] == identities
        for column, cell in zip(original_columns, cells):
            assert cell[_VALUE].marker == row[_ROW].row_idx * 100 + column
            assert cell[_PRESENT] == ((row[_ROW].row_idx + column) % 3 != 0)


@pytest.mark.parametrize("partitions", [1, 3, 7])
def test_chained_column_masks_gathers_metadata_and_missing_slots(partitions, tmp_path):
    m = hl.range_matrix_table(6, 5, partitions)
    m = m.key_cols_by(sample=m.col_idx % 2)
    m = m.annotate_entries(marker=m.row_idx * 100 + m.col_idx)
    m = m.filter_entries((m.row_idx + m.col_idx) % 3 != 0)
    m = m.choose_cols([4, 1, 4, 0, 3])
    assert_layout(m, [4, 1, 4, 0, 3])
    annotated = m.annotate_cols(label=hl.str(m.col_idx))
    assert_layout(annotated, [4, 1, 4, 0, 3])
    target = hl.Table.parallelize([{"sample": 0}]).key_by("sample")
    assert_layout(annotated.semi_join_cols(target), [4, 4, 0])
    assert_layout(annotated.anti_join_cols(target), [1, 3])
    assert_layout(annotated.distinct_by_col(), [4, 1])
    filtered = annotated.filter_cols(annotated.col_idx % 2 == 0)
    assert_layout(filtered, [4, 4, 0])
    assert_layout(filtered.head(n_rows=2, n_cols=2), [4, 4])
    assert_layout(filtered.tail(n_rows=2, n_cols=1), [0])
    assert_layout(filtered.choose_cols([]), [])
    assert_layout(filtered.repartition(2), [4, 4, 0])
    assert_layout(filtered.union_rows(filtered), [4, 4, 0])
    path = tmp_path / "matrix.mt"
    filtered.write(path)
    reread = hl.read_matrix_table(path)
    assert_layout(reread, [4, 4, 0])
    # The column key does not dictate internal order; its keyed public Table
    # view is allowed to sort, as Hail does. Localization uses canonical order.
    localized = reread.localize_entries("entries", "columns")
    assert [c.col_idx for c in hl.eval(localized.columns)] == [4, 4, 0]
    unlocalized = localized._unlocalize_entries("entries", "columns", ["sample"])
    assert unlocalized._same(reread)


@pytest.mark.parametrize("action", ["count", "_force_count_rows", "collect"])
def test_untrusted_layout_rejects_wrong_lengths_and_positional_ids_lazily(action):
    m = hl.range_matrix_table(2, 3, 2)
    base = m._mir.core
    for cells in (
        base[_CELLS][:-1],
        base[_CELLS].map(
            lambda cell: hl.struct(
                **{
                    _CID: cell[_CID] + 1,
                    _PRESENT: cell[_PRESENT],
                    _VALUE: cell[_VALUE],
                }
            )
        ),
    ):
        corrupt = base.annotate(**{_CELLS: cells})
        bad = MatrixTable(
            MatrixIR(corrupt, row_key=m._mir.row_key, col_key=m._mir.col_key)
        )
        with pytest.raises(Exception, match="alignment violation"):
            if action == "collect":
                bad._mir.core.collect()
            else:
                getattr(bad, action)()


def test_untrusted_columns_reject_duplicate_ids_even_without_rows():
    m = hl.range_matrix_table(0, 3, 1)
    cols = m._mir.cols
    badcols = cols.annotate(**{_CID: hl.int64(0)})
    bad = MatrixTable(MatrixIR(m._mir.core, badcols, col_key=m._mir.col_key))
    with pytest.raises(Exception, match="identities must be unique"):
        bad.count_cols()


def test_exploded_columns_gather_parent_slots_in_order():
    m = hl.range_matrix_table(3, 3, 2)
    m = m.annotate_entries(marker=m.row_idx * 100 + m.col_idx)
    m = m.annotate_cols(tags=hl.range(m.col_idx))
    expanded = m.explode_cols("tags")
    assert [
        record[_COL].col_idx for record in hl.eval(expanded._mir.column_records)
    ] == [1, 2, 2]
    for row in expanded._mir.core.collect():
        assert [cell[_VALUE].marker for cell in row[_CELLS]] == [
            row[_ROW].row_idx * 100 + i for i in [1, 2, 2]
        ]


def test_column_name_matching_internal_position_name_is_supported():
    m = hl.range_matrix_table(2, 3)
    m = m.key_cols_by(__vail_matrix_position=m.col_idx)
    target = hl.Table.parallelize([{"__vail_matrix_position": 2}]).key_by(
        "__vail_matrix_position"
    )
    assert m.semi_join_cols(target).col_idx.collect() == [2]


def test_grouped_rows_preserve_column_positions_when_some_columns_have_no_entries():
    m = hl.range_matrix_table(4, 5, 3)
    m = m.key_cols_by(sample=m.col_idx % 2)
    m = m.annotate_entries(marker=m.row_idx * 100 + m.col_idx)
    m = m.choose_cols([4, 1, 4, 0, 3])
    m = m.filter_entries(m.col_idx % 2 == 0)
    grouped = m.group_rows_by(g=m.row_idx % 2).aggregate(total=hl.agg.sum(m.marker))
    columns = hl.eval(grouped._mir.column_records)
    assert [record[_COL].col_idx for record in columns] == [4, 1, 4, 0, 3]
    for row in grouped._mir.core.collect():
        assert [cell[_CID] for cell in row[_CELLS]] == [
            record[_CID] for record in columns
        ]
        expected = [208, 0, 208, 200, 0] if row[_ROW].g == 0 else [408, 0, 408, 400, 0]
        assert [cell[_VALUE].total for cell in row[_CELLS]] == expected


@pytest.mark.parametrize("action", ["count", "collect"])
def test_unlocalize_rejects_misaligned_entry_lengths(action):
    table = hl.Table.parallelize([{"entries": [hl.Struct(x=1)]}])
    table = table.annotate_globals(columns=[hl.Struct(c=0), hl.Struct(c=1)])
    matrix = table._unlocalize_entries("entries", "columns", ["c"])
    with pytest.raises(Exception, match="entry array length differs from column count"):
        if action == "collect":
            matrix._mir.core.collect()
        else:
            matrix.count()
