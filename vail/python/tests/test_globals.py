"""Global scope, singleton DataFusion plans, and immutable Arrow persistence."""
import math

import pytest
import vail as hl


def test_globals_are_a_typed_singleton_on_empty_tables():
    t = hl.Table.parallelize([], schema=hl.tstruct(x=hl.tint32), globals={
        'offset': 7, 'nested': hl.struct(a=[1, None, 3], missing=hl.missing(hl.tstr)),
        'wide': hl.int64(2**60 + 1), 'flag': True, 'text': 'β',
    })
    assert t.collect() == []
    assert hl.eval(t.globals) == hl.Struct(offset=7, nested=hl.Struct(a=[1, None, 3], missing=None),
                                         wide=2**60 + 1, flag=True, text='β')
    assert hl.eval(t.offset + 1) == 8
    assert t.globals.dtype['wide'] == hl.tint64
    assert hl.eval(t.select_globals().globals) == hl.Struct()
    assert hl.eval(hl.struct()) == hl.Struct()
    assert hl.eval(hl.literal({})) == hl.Struct()
    assert hl.eval(hl.literal(3) + 1) == 4


def test_row_expressions_bind_globals_through_datafusion():
    t = hl.range_table(6, n_partitions=3).annotate_globals(offset=2, values=[9, 12], s=hl.struct(x=4))
    mapped = t.annotate(y=t.idx + t.offset, z=t.values[t.idx % 2], w=t.s.x)
    assert [r.y for r in mapped.collect()] == list(range(2, 8))
    assert [r.z for r in mapped.collect()] == [9, 12] * 3
    filtered = mapped.filter(mapped.idx >= mapped.offset)
    assert [r.idx for r in filtered.collect()] == [2, 3, 4, 5]
    assert hl.eval(filtered.head(0).globals) == hl.eval(t.globals)
    assert hl.eval(filtered.tail(1).order_by('y').globals) == hl.eval(t.globals)


def test_global_updates_are_lazy_and_scoped_to_each_ir_node():
    t = hl.range_table(3).annotate_globals(g=1)
    first = t.annotate(a=t.g)
    changed = first.annotate_globals(g=first.g + 1)
    final = changed.annotate(b=changed.g)
    assert final.collect() == [hl.Struct(idx=i, a=1, b=2) for i in range(3)]
    assert hl.eval(t.g) == 1
    assert hl.eval(final.g) == 2
    invalid = t.annotate_globals(bad=hl.array([1])[10])
    assert invalid.globals.dtype['bad'] == hl.tint32
    used = invalid.annotate(x=invalid.bad)
    assert used.head(0).collect() == []
    assert used.tail(0).collect() == []
    with pytest.raises(RuntimeError, match='out of bounds'):
        hl.eval(invalid.bad)
    with pytest.raises(RuntimeError, match='out of bounds'):
        used.collect()


def test_select_transmute_and_field_order():
    t = hl.range_table(2).annotate_globals(a=5, b=10, nested=hl.struct(x=12, y=13))
    assert hl.eval(t.select_globals(t.nested.x, 'b', z=t.a + 1).globals) == hl.Struct(x=12, b=10, z=6)
    assert hl.eval(t.transmute_globals(c=t.a + t.nested.x).globals) == hl.Struct(b=10, c=17)
    assert list(t.annotate_globals(b=1, c=2).globals.dtype) == ['a', 'b', 'nested', 'c']
    assert list(t.transmute_globals(a=t.a + 1).globals.dtype) == ['a', 'b', 'nested']
    assert t.transmute(y=t.idx + t.a).globals.dtype == t.globals.dtype
    with pytest.raises(ValueError, match='duplicate'):
        t.select_globals('a', a=t.a)


@pytest.mark.parametrize('operation', [
    lambda t: t.annotate_globals(bad=t.idx),
    lambda t: t.select_globals('idx'),
    lambda t: t.transmute_globals(bad=hl.struct(x=t.idx)),
    lambda t: hl.eval(t.idx),
    lambda t: t.annotate_globals(idx=7),
    lambda t: t.annotate(g=1),
    lambda t: t.annotate_globals(bad=hl.range_table(1).idx),
    lambda t: t.annotate(bad=hl.range_table(1).annotate_globals(g=7).g),
])
def test_scope_and_name_collisions_are_checked_before_execution(operation):
    with pytest.raises(ValueError):
        operation(hl.range_table(3).annotate_globals(g=7))


@pytest.mark.parametrize('n', [0, 12])
def test_globals_roundtrip_with_empty_and_multifile_tables(tmp_path, n):
    path = tmp_path / 'table'
    t = hl.range_table(n, n_partitions=3).annotate_globals(
        g=hl.struct(values=[None, 2.5], text='β'), wide=hl.int64(2**60 + 1),
        nan=float('nan'), inf=float('inf'), missing=hl.missing(hl.tarray(hl.tint32)))
    t.write(path)
    read = hl.read_table(path)
    assert read.collect() == t.collect()
    assert read.globals.dtype == t.globals.dtype
    assert hl.eval(read.g) == hl.Struct(values=[None, 2.5], text='β')
    assert math.isnan(hl.eval(read.nan))
    assert hl.eval(read.wide) == 2**60 + 1
    assert hl.eval(read.missing) is None
    assert read.head(0).globals.dtype == t.globals.dtype
    bad_rows = read.annotate(unusable=hl.array([1])[read.idx + 10])
    assert hl.eval(bad_rows.g.text) == 'β'


def test_globals_reads_are_lazy_snapshot_pinned_and_replayed(tmp_path):
    path = tmp_path / 'table'
    moved = tmp_path / 'moved'
    hl.range_table(3).annotate_globals(g=[1, None]).write(path)
    old = hl.read_table(path)
    mapped = old.annotate(x=old.g[0])
    path.rename(moved)
    assert old.globals.dtype == hl.tstruct(g=hl.tarray(hl.tint32))
    assert mapped.head(0).collect() == []
    with pytest.raises(FileNotFoundError):
        hl.eval(old.g)
    moved.rename(path)
    assert hl.eval(old.g) == [1, None]
    hl.range_table(1).annotate_globals(g=[2]).write(path, overwrite=True)
    assert hl.eval(old.g) == [1, None]
    assert hl.eval(hl.read_table(path).g) == [2]
    path.rename(moved)
    with pytest.raises(FileNotFoundError):
        hl.eval(old.g)
    moved.rename(path)
    assert hl.eval(old.g) == [1, None]


def test_global_field_order_and_projection_types_are_preserved_without_rows():
    first = hl.range_table(0).annotate_globals(a=1.0, b=[2, None])
    second = hl.range_table(0).annotate_globals(b=[2, None], a=1.0)
    assert list(first.globals.dtype) == ['a', 'b']
    assert list(second.globals.dtype) == ['b', 'a']
    assert hl.eval(first.globals) == hl.eval(second.globals)
    assert hl.eval(first.annotate_globals(a=hl.int32(1)).a) == 1
    assert first.annotate_globals(a=hl.int32(1)).a.dtype == hl.tint32
    assert hl.eval(first.select_globals('a').globals) == hl.Struct(a=1.0)


def test_private_partition_names_do_not_leak_or_shadow_user_fields():
    t = hl.Table.parallelize([{'__vail_partition': 1}, {'__vail_partition': 2}], n_partitions=4)
    t = t.annotate_globals(__vail_partition_=5)
    mapped = t.annotate(x=t['__vail_partition'] + t['__vail_partition_'])
    assert [r.x for r in mapped.collect()] == [6, 7]


def test_literal_payloads_are_not_walked_as_expression_ir():
    t = hl.range_table(1).annotate_globals(a=5, payload={"op": "column", "name": "idx"})
    assert hl.eval(t.payload) == hl.Struct(op="column", name="idx")
    result = t.transmute_globals(c={"op": "global", "name": "a"})
    assert list(result.globals.dtype) == ["a", "payload", "c"]


def test_index_globals_binds_multiple_sources_without_name_collisions():
    left = hl.range_table(0).annotate_globals(g=2, a=[1, 4])
    right = hl.range_table(0).annotate_globals(g=7, nested=hl.struct(x=3))
    first, second = left.index_globals(), right.index_globals()
    table = hl.range_table(6, n_partitions=3).annotate_globals(g=10)
    result = table.annotate(x=table.idx + table.g + first.g + second.g,
                            y=first.a[table.idx % 2] + second.nested.x)
    assert [r.x for r in result.collect()] == list(range(19, 25))
    assert [r.y for r in result.collect()] == [4, 7] * 3
    assert hl.eval(first) == hl.Struct(g=2, a=[1, 4])
    assert hl.eval(first.g + second.g) == 9
    assert hl.eval(table.g) == 10
    assert (table.idx + first.g).collect() == list(range(2, 8))
    filtered = table.filter(table.idx < first.g)
    assert [r.idx for r in filtered.collect()] == [0, 1]


def test_index_globals_can_build_other_globals_and_constructor_globals(tmp_path):
    first = hl.range_table(2).annotate_globals(a=5, b=[1, None])
    indexed = first.index_globals()
    second = hl.Table.parallelize([{'x': 1}], globals=indexed)
    assert hl.eval(second.globals) == hl.eval(first.globals)
    second = second.transmute_globals(c=second.a + indexed.a)
    assert hl.eval(second.globals) == hl.Struct(b=[1, None], c=10)
    third = second.annotate_globals(d=hl.struct(v=indexed.b, e=second.index_globals()))
    assert hl.eval(third.d.e.c) == 10
    path = tmp_path / 'copy'
    third.write(path)
    read = hl.read_table(path)
    assert read.collect() == third.collect()
    assert hl.eval(read.globals) == hl.eval(third.globals)


def test_index_globals_keeps_lazy_snapshot_without_rows_or_python_table(tmp_path):
    import gc
    path = tmp_path / 'source'
    hl.range_table(9, n_partitions=3).annotate_globals(g=11).write(path)
    source = hl.read_table(path)
    indexed = source.index_globals()
    del source
    gc.collect()
    moved = tmp_path / 'moved'
    path.rename(moved)
    row = hl.range_table(2)
    mapped = row.annotate(x=indexed.g + row.idx)
    assert mapped.head(0).collect() == []
    with pytest.raises(FileNotFoundError):
        mapped.collect()
    moved.rename(path)
    hl.range_table(0).annotate_globals(g=99).write(path, overwrite=True)
    assert hl.eval(indexed.g) == 11
    assert [r.x for r in mapped.collect()] == [11, 12]
    assert hl.eval(hl.read_table(path).index_globals().g) == 99


def test_index_globals_survives_expression_composition_and_is_not_eager():
    source = hl.range_table(0).annotate_globals(a=[1, 2, 3], g=2)
    g = source.index_globals()
    expression = hl.struct(
        total=hl.sum(g.a[1:]) + hl.int64(g.g),
        choice=hl.if_else(g.g > 0, -g.g, g.g + 5),
        fields=hl.array([g.g, g.a[g.g - 1]]),
        prefix=g.a[:g.g],
    )
    assert hl.eval(expression) == hl.Struct(total=7, choice=-2, fields=[2, 2], prefix=[1, 2])
    bad = source.annotate_globals(bad=hl.array([1])[5]).index_globals()
    t = hl.range_table(3).annotate(value=bad.bad)
    assert t.head(0).collect() == []
    with pytest.raises(RuntimeError, match='out of bounds'):
        hl.eval(bad.bad)
    assert hl.eval(hl.range_table(0).index_globals()) == hl.Struct()




def test_discarded_invalid_globals_are_not_demanded():
    table = hl.range_table(0).annotate_globals(bad=hl.array([1])[5])
    discarded = table.select_globals()
    assert hl.eval(discarded.globals) == hl.Struct()
    assert hl.eval(discarded.index_globals()) == hl.Struct()
    copied = hl.Table.parallelize([{'x': 1}], globals=discarded.index_globals())
    assert copied.collect() == [hl.Struct(x=1)]
    with pytest.raises(RuntimeError, match='out of bounds'):
        hl.eval(table.bad)


def test_shared_global_expression_reuse_and_transmute_scope():
    source = hl.range_table(0).annotate_globals(g=3)
    value = source.index_globals().g
    for _ in range(8):
        value = value + value
    result = hl.range_table(0).annotate_globals(answer=value)
    assert hl.eval(result.answer) == 768
    table = hl.range_table(0).annotate_globals(g=3, keep=9)
    shared = table.g + table.g
    transmuted = table.transmute_globals(answer=shared + shared)
    assert hl.eval(transmuted.globals) == hl.Struct(keep=9, answer=12)


def test_global_conditionals_keep_lexical_callback_values_in_scope():
    table = hl.range_table(0).annotate_globals(offset=2)
    offset = table.index_globals().offset
    values = hl.range(4).map(lambda x: hl.if_else(x < offset, x + offset, x - offset))
    assert hl.eval(values) == [2, 3, 0, 1]
    nested = hl.range(2).map(lambda x: hl.range(3).map(
        lambda y: hl.if_else(y < offset, hl.int64(x + y + offset), hl.int64(x - y))))
    assert hl.eval(nested) == [[2, 3, -2], [3, 4, -1]]
