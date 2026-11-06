"""Row captures preserve list indexing, nulls and lazy branch semantics."""
from concurrent.futures import ThreadPoolExecutor
import pytest
import vail as hl


@pytest.fixture
def indexed_rows(monkeypatch):
    monkeypatch.setenv('VAIL_EXPERIMENT_INDEXED_BATCH', '1')
    monkeypatch.setenv('VAIL_EXPERIMENT_INVARIANT_MAP', '1')


@pytest.mark.parametrize('filtered', [False, True])
def test_constant_and_varying_rows_with_lazy_frames(indexed_rows, filtered):
    mt = hl.range_matrix_table(9, 7, n_partitions=3)
    mt = mt.annotate_rows(slot=hl.int32(1), constant=hl.int64(9), absent=hl.missing(hl.tint32),
                          absent64=hl.missing(hl.tint64),
                          changing=hl.int64(mt.row_idx),
                          nested=hl.or_missing(mt.row_idx > 0, hl.struct(x=hl.int64(7))),
                          rc=hl.call(1, 0, phased=True))
    mt = mt.annotate_entries(xs=hl.array([mt.row_idx + mt.col_idx, 50, 100]))
    if filtered:
        mt = mt.filter_rows(mt.row_idx > 1)
        mt = mt.filter_entries(mt.col_idx != 1)
    mt = mt.select_entries(value=mt.xs[mt.slot], negative=mt.xs[hl.int32(-1)],
                           absent_out=mt.xs[mt.absent], null64=mt.absent64, is_missing=hl.is_missing(mt.absent),
                           chosen=hl.if_else(mt.col_idx == 0, mt.constant, mt.changing),
                           nested_out=mt.nested.x, rc_out=mt.rc,
                           mapped=mt.xs.map(lambda x: x + mt.constant),
                           folded=mt.xs.fold(lambda s, x: s + x + mt.constant, hl.int64(0)))
    query = mt.localize_entries('entries', 'columns')
    for row in query.collect():
        for col, entry in enumerate(row.entries):
            if filtered and col == 1:
                assert entry is None
                continue
            assert entry.value == 50 and entry.negative == 100 and entry.absent_out is None
            assert entry.is_missing is True and entry.null64 is None
            assert entry.chosen == (9 if col == 0 else row.row_idx)
            assert entry.nested_out == (None if row.row_idx == 0 else 7)
            assert list(entry.rc_out.alleles) == [1, 0] and entry.rc_out.phased
            assert entry.mapped == [row.row_idx + col + 9, 59, 109]
            assert entry.folded == row.row_idx + col + 177
    expected = query.to_arrow()
    with ThreadPoolExecutor(max_workers=2) as pool:
        for result in pool.map(lambda _: query.to_arrow(), range(2)):
            assert result.equals(expected, check_metadata=True)


def test_guarded_and_demanded_invalid_selectors(indexed_rows):
    mt = hl.range_matrix_table(3, 2, n_partitions=2).annotate_entries(xs=[1, 2, 3])
    for index in [hl.int32(3), hl.int32(-4), hl.int32(-(2**31))]:
        guarded = mt.select_entries(value=hl.if_else(mt.col_idx < 0, mt.xs[index], 7))
        assert [[e.value for e in row.entries] for row in
                guarded.localize_entries('entries', 'columns').collect()] == [[7, 7]] * 3
        with pytest.raises(Exception, match='(?i)(bounds|index)'):
            mt.select_entries(value=mt.xs[index]).localize_entries('entries', 'columns').collect()


def test_empty_columns_keep_all_parent_rows(indexed_rows):
    mt = hl.range_matrix_table(4, 0).annotate_rows(constant=hl.int64(5))
    mt = mt.annotate_entries(xs=[1, 2, 3])
    mt = mt.select_entries(value=mt.xs[0] + mt.constant)
    assert [row.entries for row in mt.localize_entries('entries', 'columns').collect()] == [[], [], [], []]


def test_many_row_fields_preserve_values_when_some_work_is_declined(indexed_rows):
    mt = hl.range_matrix_table(8192, 1, n_partitions=1)
    mt = mt.annotate_rows(**{f'input_{i}': hl.int64(i) for i in range(32)})
    mt = mt.annotate_entries(xs=[11, 12])
    mt = mt.select_entries(**{f'output_{i}': mt[f'input_{i}'] for i in range(32)},
                           selected=mt.xs[0])
    result = mt.localize_entries('entries', 'columns').to_arrow()
    entries = result.column('entries').combine_chunks()
    assert len(entries) == 8192
    assert len(entries.values.type) == 33
    for i in range(32):
        assert entries.values.field(f'output_{i}').to_pylist() == [i] * 8192
    assert entries.values.field('selected').to_pylist() == [11] * 8192
