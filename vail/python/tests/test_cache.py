"""Public eager Arrow cache contracts used by import/export fixtures."""
import pytest
import vail as hl


def test_imported_table_cache_freezes_rows_globals_keys_and_partitions(tmp_path):
    path = tmp_path / 'rows.tsv'
    path.write_text('id\tvalue\nb\t2\na\t1\n')
    source = hl.import_table(str(path), types={'value': hl.tint32}, key='id', min_partitions=3)
    source = source.annotate_globals(total=source.aggregate(hl.agg.sum(source.value), _localize=False))
    expected = source.collect()
    cached = source.cache()
    assert cached.row.dtype == source.row.dtype
    assert cached.key.dtype == source.key.dtype
    assert cached.n_partitions() == source.n_partitions()
    path.write_text('id\tvalue\nz\t90\n')
    assert cached.collect() == expected
    assert hl.eval(cached.index_globals()) == hl.Struct(total=3)
    assert source.collect() == [hl.Struct(id='z', value=90)]
    assert hl.eval(source.index_globals()) == hl.Struct(total=90)
    assert cached.filter(cached.value > 1).collect() == [hl.Struct(id='b', value=2)]


def test_matrix_cache_preserves_column_order_filtered_slots_and_metadata(tmp_path):
    path = tmp_path / 'matrix.tsv'
    path.write_text('id\ts0\ts1\nr0\t1\t2\nr1\t3\t4\n')
    source = hl.import_matrix_table(str(path), row_fields={'id': hl.tstr}, row_key='id', entry_type=hl.tint32)
    source = source.filter_entries(source.x != 2).choose_cols([1, 0]).annotate_globals(label='snapshot')
    expected = source.localize_entries('cells', 'columns')
    cached = source.cache()
    actual = cached.localize_entries('cells', 'columns')
    assert actual.row.dtype == expected.row.dtype
    assert cached.row_key.dtype == source.row_key.dtype
    assert cached.col_key.dtype == source.col_key.dtype
    assert cached.n_partitions() == source.n_partitions()
    path.write_text('id\ts0\ts1\nr0\t10\t20\nr1\t30\t40\n')
    rows = actual.collect()
    assert rows == [hl.Struct(id='r0', cells=[None, hl.Struct(x=1)]),
                    hl.Struct(id='r1', cells=[hl.Struct(x=4), hl.Struct(x=3)])]
    assert [col.col_id for col in hl.eval(actual.index_globals()).columns] == ['s1', 's0']
    assert hl.eval(actual.index_globals()).label == 'snapshot'
    assert source.aggregate_entries(hl.agg.sum(source.x)) == 100
    assert cached.aggregate_entries(hl.agg.count()) == 3


def test_memory_only_persistence_and_empty_table_partitions():
    source = hl.range_table(0, n_partitions=3).annotate_globals(label='empty')
    cached = source.persist('MEMORY_ONLY')
    assert cached.collect() == []
    assert cached.n_partitions() == 3
    assert hl.eval(cached.index_globals()) == hl.Struct(label='empty')
    zero_columns = hl.range_table(4, n_partitions=3).key_by().select().cache()
    assert zero_columns.count() == 4
    assert zero_columns.row.dtype == hl.tstruct()
    assert zero_columns.n_partitions() == 3
    matrix = hl.utils.range_matrix_table(0, 2).persist('MEMORY_ONLY')
    assert matrix.count() == (0, 2)
    for dataset in (source, matrix):
        with pytest.raises(NotImplementedError, match='MEMORY_ONLY'):
            dataset.persist('MEMORY_AND_DISK')
