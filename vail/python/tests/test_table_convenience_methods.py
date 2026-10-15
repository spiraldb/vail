"""Hail's Table convenience methods delegate to aggregation and storage."""
import pytest
import vail as hl


def test_all_any_boolean_missing_empty_and_source_validation():
    table = hl.Table.parallelize(
        [{"p": True}, {"p": None}], schema=hl.tstruct(p=hl.tbool), n_partitions=2)
    assert table.all(table.p) is True
    assert table.any(table.p) is True
    false = hl.Table.parallelize([{"p": False}, {"p": None}], schema=table.row.dtype)
    assert false.all(false.p) is False
    assert false.any(false.p) is False
    missing = hl.Table.parallelize([{"p": None}], schema=table.row.dtype)
    empty = hl.Table.parallelize([], schema=table.row.dtype)
    for t in (missing, empty):
        assert t.all(t.p) is True
        assert t.any(t.p) is False
    for method in (table.all, table.any):
        with pytest.raises(TypeError, match='bool'):
            method(1)
        with pytest.raises(hl.ExpressionException, match='source mismatch'):
            method(false.p)


def test_collect_by_key_struct_values_missing_keys_globals_and_lazy_plan():
    table = hl.Table.parallelize(
        [{"k": 1, "v": "a"}, {"k": None, "v": "n"}, {"k": 1, "v": None}],
        schema=hl.tstruct(k=hl.tint32, v=hl.tstr), key='k', n_partitions=3,
    ).annotate_globals(g=7)
    collected = table.collect_by_key('rows')
    assert list(collected.key) == ['k']
    assert collected.rows.dtype == hl.tarray(hl.tstruct(v=hl.tstr))
    assert hl.eval(collected.g) == 7
    assert 'TableGroupedAggregate' in repr(collected._native.ir())
    assert 'AggregateExec' in collected._native.physical_plan()
    rows = collected.collect()
    assert [r.k for r in rows] == [1, None]
    assert sorted((r.v for r in rows[0].rows), key=lambda x: '' if x is None else x) == [None, 'a']
    assert rows[1].rows == [hl.Struct(v='n')]
    with pytest.raises(ValueError, match='keyed'):
        table.key_by().collect_by_key()
    with pytest.raises(ValueError, match='collides'):
        table.collect_by_key('k')


def test_checkpoint_replaces_lineage_preserves_types_globals_and_overwrites(tmp_path):
    table = hl.range_table(8, n_partitions=3).annotate_globals(g='yes')
    table = table.annotate(x=table.idx * 2)
    path = tmp_path / 'table'
    checkpoint = table.checkpoint(path)
    assert 'TableRead' in repr(checkpoint._native.ir())
    assert checkpoint._same(table)
    assert checkpoint.n_partitions() == table.n_partitions()
    replacement = table.filter(table.idx < 2)
    assert replacement.checkpoint(path, overwrite=True)._same(replacement)


@pytest.mark.parametrize('options', [
    {'stage_locally': True}, {'_codec_spec': 'custom'}, {'_read_if_exists': True},
    {'_intervals': []}, {'_filter_intervals': True},
])
def test_checkpoint_unsupported_flags_fail_before_writing(tmp_path, options):
    path = tmp_path / 'not-written'
    with pytest.raises(NotImplementedError):
        hl.range_table(1).checkpoint(path, **options)
    assert not path.exists()
