"""Column occurrences and filtered entries through public Matrix operations."""
import pytest
import vail as hl


def assert_layout(matrix, original_columns):
    localized = matrix.localize_entries('entries', 'columns')
    assert [column.col_idx for column in hl.eval(localized.columns)] == original_columns
    for row in localized.collect():
        assert len(row.entries) == len(original_columns)
        for column, entry in zip(original_columns, row.entries):
            if (row.row_idx + column) % 3 == 0:
                assert entry is None
            else:
                assert entry.marker == row.row_idx * 100 + column


@pytest.mark.parametrize('partitions', [1, 3, 7])
def test_chained_column_masks_gathers_metadata_and_missing_slots(partitions, tmp_path):
    m = hl.range_matrix_table(6, 5, partitions)
    m = m.key_cols_by(sample=m.col_idx % 2)
    m = m.annotate_entries(marker=m.row_idx * 100 + m.col_idx)
    m = m.filter_entries((m.row_idx + m.col_idx) % 3 != 0)
    m = m.choose_cols([4, 1, 4, 0, 3])
    assert_layout(m, [4, 1, 4, 0, 3])
    annotated = m.annotate_cols(label=hl.str(m.col_idx))
    assert_layout(annotated, [4, 1, 4, 0, 3])
    target = hl.Table.parallelize([{'sample': 0}]).key_by('sample')
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
    path = str(tmp_path / 'matrix.mt')
    filtered.write(path)
    reread = hl.read_matrix_table(path)
    assert_layout(reread, [4, 4, 0])
    localized = reread.localize_entries('entries', 'columns')
    restored = localized.unlocalize_entries('entries', 'columns', ['sample'])
    assert_layout(restored, [4, 4, 0])


@pytest.mark.parametrize('action', ['count', 'entries'])
def test_unlocalize_rejects_misaligned_entry_lengths(action):
    table = hl.Table.parallelize([{'entries': [hl.Struct(x=1)]}])
    table = table.annotate_globals(columns=[hl.Struct(c=0), hl.Struct(c=1)])
    matrix = table.unlocalize_entries('entries', 'columns', ['c'])
    with pytest.raises(Exception, match='entry array length differs from column count'):
        matrix.count() if action == 'count' else matrix.entries().collect()


def test_exploded_columns_gather_parent_slots_in_order():
    m = hl.range_matrix_table(3, 3, 2)
    m = m.annotate_entries(marker=m.row_idx * 100 + m.col_idx)
    m = m.annotate_cols(tags=hl.range(m.col_idx)).explode_cols('tags')
    localized = m.localize_entries('entries', 'columns')
    assert [c.col_idx for c in hl.eval(localized.columns)] == [1, 2, 2]
    assert [[e.marker for e in row.entries] for row in localized.collect()] == [
        [r * 100 + c for c in [1, 2, 2]] for r in range(3)]


def test_column_name_matching_internal_position_name_is_supported():
    m = hl.range_matrix_table(2, 3)
    m = m.key_cols_by(__vail_matrix_position=m.col_idx)
    target = hl.Table.parallelize([{'__vail_matrix_position': 2}]).key_by('__vail_matrix_position')
    assert m.semi_join_cols(target).col_idx.collect() == [2]


def test_grouped_columns_preserve_occurrences_and_filtered_slots():
    m = hl.range_matrix_table(4, 5, 3)
    m = m.annotate_entries(marker=m.row_idx * 100 + m.col_idx).choose_cols([4, 1, 4, 0, 3])
    m = m.filter_entries(m.col_idx % 2 == 0)
    grouped = (m.group_cols_by(g=m.col_idx % 2)
               .aggregate_cols(first=hl.agg.take(m.col_idx, 1)[0])
               .aggregate(total=hl.agg.sum(m.marker), n=hl.agg.count()))
    localized = grouped.localize_entries('entries', 'columns')
    assert [(c.g, c.first) for c in hl.eval(localized.columns)] == [(0, 4), (1, 1)]
    for row in localized.collect():
        assert [e.total for e in row.entries] == [row.row_idx * 300 + 8, 0]
        assert [e.n for e in row.entries] == [3, 0]
    empty = m.choose_cols([])
    assert empty.group_cols_by(g=empty.col_idx % 2).aggregate(n=hl.agg.count()).count() == (4, 0)


def test_grouped_rows_keep_columns_without_entries():
    m = hl.range_matrix_table(4, 5, 3)
    m = m.annotate_entries(marker=m.row_idx * 100 + m.col_idx).choose_cols([4, 1, 4, 0, 3])
    m = m.filter_entries(m.col_idx % 2 == 0)
    grouped = m.group_rows_by(g=m.row_idx % 2).aggregate(total=hl.agg.sum(m.marker))
    localized = grouped.localize_entries('entries', 'columns')
    assert [c.col_idx for c in hl.eval(localized.columns)] == [4, 1, 4, 0, 3]
    assert [[e.total for e in row.entries] for row in localized.collect()] == [
        [208, 0, 208, 200, 0], [408, 0, 408, 400, 0]]


def test_sparse_coordinate_conversion_scatters_by_exact_column_positions():
    table = hl.Table.parallelize([{'r': 0, 'c': 2, 'v': 20}, {'r': 1, 'c': 1, 'v': 11},
                                 {'r': 0, 'c': 0, 'v': 0}, {'r': 0, 'c': 2, 'v': 21},
                                 {'r': 1, 'c': 2, 'v': 12}], n_partitions=3)
    matrix = table.to_matrix_table(['r'], ['c'])
    localized = matrix.localize_entries('entries', 'columns')
    assert [c.c for c in hl.eval(localized.columns)] == [0, 1, 2]
    assert [[None if e is None else e.v for e in row.entries] for row in localized.collect()] == [
        [0, None, 21], [None, 11, 12]]


def test_from_parts_infers_all_present_entry_values_and_promotes_numeric_types():
    m = hl.MatrixTable.from_parts(entries={'x': [[None, 1], [2.5, 3]]})
    assert m.x.dtype == hl.tfloat64
    assert [e.x for e in m.entries().collect()] == [None, 1., 2.5, 3.]
    with pytest.raises(ValueError, match='typed localized Table'):
        hl.MatrixTable.from_parts(entries={'x': [[None]]})


def test_from_parts_empty_entries_preserves_rows_columns_globals():
    m = hl.MatrixTable.from_parts(rows={'r': [4, 5]}, cols={'c': ['a', 'b', 'c']}, globals={'g': 7})
    assert m.count() == (2, 3)
    assert hl.eval(m.g) == 7
    assert [(e.row_idx, e.col_idx, e.r, e.c) for e in m.entries().collect()] == [
        (i, j, r, c) for i, r in enumerate([4, 5]) for j, c in enumerate(['a', 'b', 'c'])]
