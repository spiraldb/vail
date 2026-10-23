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


def test_localize_read_matrix_preserves_row_key_values_and_entry_alignment(tmp_path):
    matrix = hl.range_matrix_table(5, 4, 3)
    matrix = matrix.annotate_rows(
        nested=hl.struct(label=hl.str(matrix.row_idx), doubled=matrix.row_idx * 2)
    )
    matrix = matrix.key_rows_by(reverse=5 - matrix.row_idx, original=matrix.row_idx)
    matrix = matrix.annotate_entries(value=matrix.row_idx * 100 + matrix.col_idx)
    matrix = matrix.choose_cols([3, 1, 3, 0])
    matrix = matrix.filter_entries(matrix.col_idx != 0)
    matrix = matrix.annotate_globals(marker="matrix-global")
    path = str(tmp_path / "key-lineage.mt")
    matrix.write(path)

    localized = hl.read_matrix_table(path).localize_entries("entries", "columns")
    rows = localized.collect()
    assert [(row.reverse, row.original) for row in rows] == [
        (5 - index, index) for index in reversed(range(5))
    ]
    assert [(row.nested.label, row.nested.doubled) for row in rows] == [
        (str(index), index * 2) for index in reversed(range(5))
    ]
    assert [
        [None if entry is None else entry.value for entry in row.entries]
        for row in rows
    ] == [
        [index * 100 + column for column in (3, 1, 3)] + [None]
        for index in reversed(range(5))
    ]
    assert [column.col_idx for column in hl.eval(localized.columns)] == [3, 1, 3, 0]
    assert hl.eval(localized.globals.marker) == "matrix-global"


def test_localize_read_matrix_with_hidden_index_key_duplicates_and_missing_key(tmp_path):
    key_name = "__vail_matrix_row"
    rows = hl.Table.parallelize(
        [
            {key_name: None, "nested": {"value": "first-null"}},
            {key_name: None, "nested": {"value": "second-null"}},
            {key_name: 2, "nested": {"value": "two"}},
        ],
        key=key_name,
        globals={"source": "hidden-index"},
    )
    matrix = hl.MatrixTable.from_rows_table(rows)
    path = str(tmp_path / "hidden-key.mt")
    matrix.write(path)

    localized = hl.read_matrix_table(path).localize_entries("entries", "columns")
    result = localized.collect()
    observed = sorted(
        [(row[key_name], row.nested.value, row.entries) for row in result],
        key=lambda row: (row[0] is not None, row[0] or 0, row[1]),
    )
    assert observed == [
        (None, "first-null", []),
        (None, "second-null", []),
        (2, "two", []),
    ]
    assert hl.eval(localized.index_globals()["source"]) == "hidden-index"


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


def test_native_snapshot_preserves_filtered_missing_and_empty_entries(tmp_path):
    matrix = hl.range_matrix_table(3, 3, 2)
    matrix = matrix.annotate_entries(value=hl.or_missing(matrix.col_idx != 1, matrix.row_idx * 10 + matrix.col_idx))
    matrix = matrix.choose_cols([2, 0, 1])
    matrix = matrix.filter_entries(matrix.col_idx != 0)
    path = tmp_path / "filtered-missing.mt"
    matrix.write(path)
    restored = hl.read_matrix_table(path)
    assert [(entry.row_idx, entry.col_idx, entry.value) for entry in restored.entries().collect()] == [
        (row, col, row * 10 + col if col == 2 else None)
        for row in range(3) for col in (1, 2)
    ]
    localized = restored.localize_entries("entries", "columns")
    assert [column.col_idx for column in hl.eval(localized.columns)] == [2, 0, 1]

    empty = matrix.select_entries()
    empty_path = tmp_path / "empty-entries.mt"
    empty.write(empty_path)
    assert [(entry.row_idx, entry.col_idx) for entry in hl.read_matrix_table(empty_path).entries().collect()] == [
        (row, col) for row in range(3) for col in (1, 2)
    ]


@pytest.mark.parametrize('filter_slots', [False, True])
def test_entries_expansion_preserves_nested_child_offsets(filter_slots):
    matrix = hl.range_matrix_table(5, 7, 2)
    matrix = matrix.annotate_entries(
        payload=hl.or_missing(matrix.col_idx != 3, hl.range(matrix.row_idx + matrix.col_idx)),
        label=hl.str(matrix.row_idx) + ":" + hl.str(matrix.col_idx),
    )
    # Lose uniqueness metadata deliberately, so entries uses the duplicate-
    # capable streaming expansion rather than the unique-row fast path.
    matrix = matrix.key_rows_by(k=matrix.row_idx)
    if filter_slots:
        matrix = matrix.filter_entries((matrix.col_idx % 2) == 0)
    assert [(entry.k, entry.col_idx, entry.payload, entry.label) for entry in matrix.entries().collect()] == [
        (row, col, None if col == 3 else list(range(row + col)), f"{row}:{col}")
        for row in range(5) for col in range(7)
        if not filter_slots or col % 2 == 0
    ]


def test_matrix_slots_use_positions_without_repeated_column_ids(tmp_path):
    import json
    matrix = hl.range_matrix_table(2, 4)
    matrix = matrix.annotate_entries(value=matrix.row_idx * 10 + matrix.col_idx)
    matrix = matrix.filter_entries(matrix.col_idx != 1).choose_cols([3, 1, 3, 0])
    assert set(matrix._mir.core._schema['__vail_matrix_cells'].element_type) == {'value'}
    localized = matrix.localize_entries('entries', 'columns')
    assert [[None if entry is None else entry.value for entry in row.entries]
            for row in localized.collect()] == [[3, None, 3, 0], [13, None, 13, 10]]
    path = tmp_path / 'positional.mt'
    matrix.write(path)
    descriptor = json.loads((path / '_vail_matrix.json').read_text())
    assert descriptor['version'] == 4
    core = hl.read_table(path / descriptor['generation'] / 'core')
    assert set(core._schema['__vail_matrix_cells'].element_type) == {'value'}
    assert hl.read_matrix_table(path).localize_entries('entries', 'columns').collect() == localized.collect()


@pytest.mark.parametrize('empty_entry', [False, True])
def test_matrix_version_three_drops_repeated_ids_preserving_null_structs(tmp_path, empty_entry):
    import json
    matrix = hl.range_matrix_table(2, 3)
    matrix = matrix.annotate_entries(value=hl.or_missing(matrix.col_idx != 0, matrix.col_idx))
    matrix = matrix.filter_entries(matrix.col_idx != 1)
    if empty_entry:
        matrix = matrix.select_entries()
    expected = matrix.localize_entries('entries', 'columns').collect()
    path = tmp_path / 'version-three.mt'
    matrix.write(path)
    manifest = path / '_vail_matrix.json'
    descriptor = json.loads(manifest.read_text())
    core_path = path / descriptor['generation'] / 'core'
    core = hl.read_table(core_path)
    cells = core['__vail_matrix_cells']
    old_cells = hl.zip(cells, hl.range(hl.len(cells))).map(lambda pair: hl.struct(**{
        '__vail_matrix_col_id': hl.int64(pair[1]), 'value': pair[0].value,
    }))
    core.annotate(**{'__vail_matrix_cells': old_cells}).write(core_path, overwrite=True)
    descriptor['version'] = 3
    manifest.write_text(json.dumps(descriptor))
    restored = hl.read_matrix_table(path)
    assert set(restored._mir.core._schema['__vail_matrix_cells'].element_type) == {'value'}
    assert restored.localize_entries('entries', 'columns').collect() == expected
    assert restored.aggregate_entries(hl.agg.count()) == 4


@pytest.mark.parametrize('empty_entry', [False, True])
def test_forced_entry_count_matches_expansion_with_missing_and_duplicate_slots(empty_entry):
    matrix = hl.range_matrix_table(2, 3, 2)
    matrix = matrix.annotate_entries(value=hl.or_missing(matrix.col_idx != 0, matrix.col_idx))
    matrix = matrix.filter_entries(matrix.col_idx != 1).choose_cols([2, 1, 0, 2])
    if empty_entry:
        matrix = matrix.select_entries()
    entries = matrix.entries()
    collected = entries.collect()
    # Each row has a defined entry with all missing fields (or an empty
    # struct), one filtered slot, and two occurrences of column 2.
    assert len(collected) == 6
    assert entries._force_count() == len(collected)
    if not empty_entry:
        assert sum(entry.value is None for entry in collected) == 2


@pytest.mark.parametrize('stage', ['entry', 'expanded'])
def test_forced_entry_count_executes_computed_errors(stage):
    matrix = hl.range_matrix_table(2, 3, 2)
    if stage == 'entry':
        matrix = matrix.annotate_entries(value=hl.int32(hl.str(matrix.col_idx) + 'invalid'))
        entries = matrix.entries()
    else:
        entries = matrix.entries()
        entries = entries.annotate(value=hl.int32(hl.str(entries.col_idx) + 'invalid'))
    with pytest.raises(hl.utils.FatalError, match='(?i)invalid|parse|cast|digit'):
        entries._force_count()


@pytest.mark.parametrize('duplicates', [False, True])
@pytest.mark.parametrize('empty_entry', [False, True])
def test_native_entry_count_preserves_filtered_slots_and_key_occurrences(tmp_path, duplicates, empty_entry):
    matrix = hl.range_matrix_table(7, 5, 3)
    matrix = matrix.annotate_entries(value=hl.or_missing(matrix.col_idx != 0, matrix.row_idx))
    matrix = matrix.filter_entries((matrix.row_idx + matrix.col_idx) % 3 != 0)
    if duplicates:
        matrix = matrix.key_rows_by(k=matrix.row_idx // 2).choose_cols([4, 0, 4, 2, 1])
    if empty_entry:
        matrix = matrix.select_entries()
    expected = matrix.entries().collect()
    path = tmp_path / 'matrix.mt'
    matrix.write(path)
    restored = hl.read_matrix_table(path)
    assert restored.entries().collect() == expected
    assert restored.entries()._force_count() == len(expected)
    # A post-expansion filter/head is a cardinality barrier, not a validity count.
    assert restored.entries().head(3)._force_count() == min(3, len(expected))
    # Existing snapshots without the optional proofs still count correctly.
    import json
    manifest = path / '_vail_matrix.json'
    descriptor = json.loads(manifest.read_text())
    descriptor.pop('row_key_unique', None)
    descriptor.pop('column_ordering', None)
    manifest.write_text(json.dumps(descriptor))
    assert hl.read_matrix_table(path).entries()._force_count() == len(expected)


@pytest.mark.parametrize('dimensions', [(0, 3), (3, 0), (0, 0)])
def test_native_entry_count_empty_axes(tmp_path, dimensions):
    matrix = hl.range_matrix_table(*dimensions)
    path = tmp_path / 'empty.mt'
    matrix.write(path)
    assert hl.read_matrix_table(path).entries()._force_count() == 0


def test_forced_entry_count_keeps_column_expression_errors():
    matrix = hl.range_matrix_table(3, 4, 2)
    matrix = matrix.key_rows_by(k=matrix.row_idx // 2)
    matrix = matrix.annotate_cols(bad=hl.int32(hl.str(matrix.col_idx) + 'invalid'))
    with pytest.raises(hl.utils.FatalError, match='(?i)invalid|parse|cast|digit'):
        matrix.entries()._force_count()


@pytest.mark.parametrize('seed', [13, 27])
def test_forced_entry_count_preserves_seeded_entry_and_column_masks(seed):
    matrix = hl.range_matrix_table(12, 9, 3)
    matrix = matrix.key_rows_by(group=matrix.row_idx // 2)
    matrix = matrix.annotate_entries(score=hl.rand_int32(0, 11, seed=seed))
    matrix = matrix.filter_entries(matrix.score % 3 != 0)
    matrix = matrix.annotate_cols(keep=hl.rand_bool(0.6, seed=seed + 1))
    matrix = matrix.filter_cols(matrix.keep)
    entries = matrix.entries()
    expected = len(entries.collect())
    assert entries._force_count() == expected
    assert entries._force_count() == expected
