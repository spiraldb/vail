import pytest
import vail as hl


def table(rows, schema=None, key=None, n_partitions=1):
    return hl.Table.parallelize(rows, schema=schema, key=key, n_partitions=n_partitions)


def test_same_is_an_action_and_ignores_partition_boundaries(tmp_path):
    path = tmp_path / 'table'
    original = hl.range_table(10, n_partitions=3)
    original.write(path)
    read = hl.read_table(path)
    assert original._same(read)
    assert read._same(hl.range_table(10, n_partitions=1))
    assert original.annotate()._same(original)
    next(path.rglob('*.vortex')).unlink()
    with pytest.raises(FileNotFoundError):
        read._same(read)


def test_schema_and_keys_compare_without_executing(tmp_path, capsys):
    path = tmp_path / 'table'
    hl.range_table(1).write(path)
    t = hl.read_table(path)
    next(path.rglob('*.vortex')).unlink()
    assert not t._same(table([{'different': 1}]))
    assert not t._same(table([{'idx': 1}]))  # key mismatch
    assert not t._same(table([{'idx': 1}], schema=hl.tstruct(idx=hl.tint64), key='idx'))
    assert 'Table._same:' in capsys.readouterr().out


def test_top_level_reorder_and_nested_type_order():
    a = table([{'x': 1, 'y': 'a'}])
    b = table([{'y': 'a', 'x': 1}])
    assert not a._same(b)
    assert a._same(b, reorder_fields=True)
    assert a.key_by('x')._same(b.key_by('x'), reorder_fields=True)
    assert not a.key_by('x', 'y')._same(a.key_by('y', 'x'), reorder_fields=True)
    a = table([{'s': {'x': 1, 'y': 'a'}}])
    b = table([{'s': {'y': 'a', 'x': 1}}])
    assert not a._same(b, reorder_fields=True)  # Hail reorders only top-level fields


def test_duplicate_keys_preserve_multiplicity_and_sequence():
    a = table([{'k': 1, 'v': 10}, {'k': 1, 'v': 20}, {'k': None, 'v': 30}], key='k')
    b = table([{'k': 1, 'v': 10}, {'k': 1, 'v': 20}, {'k': None, 'v': 30}], key='k')
    assert a._same(b)
    assert not a._same(table([{'k': 1, 'v': 10}, {'k': None, 'v': 30}], key='k'))
    assert not a._same(table([{'k': 1, 'v': 20}, {'k': 1, 'v': 10}, {'k': None, 'v': 30}], key='k'))
    assert not a._same(table([{'k': 2, 'v': 10}, {'k': 2, 'v': 20}, {'k': None, 'v': 30}], key='k'), tolerance=100)
    assert not table([{'k': 1}], key='k')._same(table([{'k': 1}, {'k': 1}], key='k'))


def test_unkeyed_rows_and_nested_missing_values():
    schema = hl.tstruct(s=hl.tstruct(a=hl.tarray(hl.tfloat64), b=hl.tbool), label=hl.tstr)
    rows = [{'s': {'a': [1.0, None, float('nan')], 'b': True}, 'label': 'β'},
            {'s': None, 'label': None}]
    a = table(rows, schema, n_partitions=3)
    b = table(rows, schema, n_partitions=1)
    assert a._same(b)
    assert not a._same(table(list(reversed(rows)), schema))
    changed = [{'s': {'a': [1.0, 0.0, float('nan')], 'b': True}, 'label': 'β'}, rows[1]]
    assert not a._same(table(changed, schema))
    assert not table([{'a': None}], hl.tstruct(a=hl.tarray(hl.tint32)))._same(
        table([{'a': []}], hl.tstruct(a=hl.tarray(hl.tint32))))
    assert not table([{'s': None}], hl.tstruct(s=hl.tstruct(x=hl.tint32)))._same(
        table([{'s': {'x': None}}], hl.tstruct(s=hl.tstruct(x=hl.tint32))))


@pytest.mark.parametrize('dtype', [hl.tfloat32, hl.tfloat64])
def test_numeric_tolerance(dtype):
    def t(value):
        return table([{'x': value}], hl.tstruct(x=dtype))
    assert t(1_000_000.0)._same(t(1_000_000.5))
    assert not t(1_000_000.0)._same(t(1_000_000.5), absolute=True)
    assert t(1_000_000.0)._same(t(1_000_000.5), tolerance=0.5, absolute=True)
    assert not t(1.0)._same(t(1.01))
    assert t(float('nan'))._same(t(float('nan')), tolerance=0)
    assert not t(float('nan'))._same(t(1.0))
    assert t(-0.0)._same(t(0.0), tolerance=0)
    assert t(float('inf'))._same(t(float('inf')), tolerance=0)
    assert not t(float('inf'))._same(t(float('-inf')), absolute=True)
    # Hail D_== literally evaluates inf <= tolerance * inf in relative mode.
    assert t(float('inf'))._same(t(1.0))
    assert not t(float('inf'))._same(t(1.0), absolute=True)


def test_relative_absolute_floor_and_nested_tolerance():
    a = table([{'x': 0.0}])
    b = table([{'x': float.fromhex('0x1p-1022')}])
    assert a._same(b, tolerance=0)
    assert not a._same(b, tolerance=0, absolute=True)
    a = table([{'s': {'a': [1.0, 2.0]}}])
    b = table([{'s': {'a': [1.0 + 1e-7, 2.0]}}])
    assert a._same(b)
    assert not a._same(b, tolerance=0, absolute=True)


def test_empty_and_zero_column_tables():
    assert hl.range_table(0)._same(hl.range_table(0, n_partitions=5))
    a = hl.range_table(3).key_by().select()
    b = hl.range_table(3, n_partitions=2).key_by().select()
    assert a._same(b)
    assert not a._same(b.head(2))
    assert not a._same(b.head(0))
    assert a.head(0)._same(b.head(0))


def test_missing_tolerance_matches_hails_missing_predicate_rules():
    # A missing valuesSimilar result is not selected by agg.filter. Missing key
    # groups still differ; values and multiplicity of present groups do not.
    a = table([{'k': 1, 'v': 1}], key='k')
    assert a._same(table([{'k': 1, 'v': 7}, {'k': 1, 'v': 8}], key='k'), tolerance=None)
    assert not a._same(table([{'k': 2, 'v': 1}], key='k'), tolerance=None)
    assert not a._same(a.head(0), tolerance=None)
    assert table([{'x': 1}])._same(table([{'x': 7}, {'x': 8}]), tolerance=None)


@pytest.mark.parametrize('kwargs', [{'tolerance': 'small'}, {'absolute': 1}, {'reorder_fields': 1}])
def test_invalid_options(kwargs):
    t = hl.range_table(1)
    with pytest.raises(TypeError):
        t._same(t, **kwargs)
    with pytest.raises(TypeError):
        t._same([1])
