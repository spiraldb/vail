"""Global scope, singleton DataFusion plans, and immutable Arrow persistence."""
import json
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
    plan = mapped._native.physical_plan()
    assert 'ProjectionExec' in plan and 'CrossJoinExec' in plan and 'RangeExec' in plan
    # DataFusion may fuse the predicate into the singleton globals join.
    assert 'Filter:' in filtered._native.logical_plan() or 'filter=' in filtered._native.physical_plan()


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


def test_native_scope_checks_cannot_be_bypassed():
    t = hl.range_table(1).annotate_globals(g=1)
    with pytest.raises(ValueError):
        t._native.project_globals_json(json.dumps([['x', {'op': 'column', 'name': 'idx'}]]))
    with pytest.raises(ValueError):
        t._native.project_json(json.dumps([['idx', {'op': 'column', 'name': 'idx'}],
                                          ['g', {'op': 'global', 'name': 'g'}]]))


@pytest.mark.parametrize('n', [0, 12])
def test_globals_roundtrip_with_empty_and_multifile_tables(tmp_path, n):
    path = tmp_path / 'table'
    t = hl.range_table(n, n_partitions=3).annotate_globals(
        g=hl.struct(values=[None, 2.5], text='β'), wide=hl.int64(2**60 + 1),
        nan=float('nan'), inf=float('inf'), missing=hl.missing(hl.tarray(hl.tint32)))
    t.write(path)
    assert len(list(path.rglob('globals.arrow'))) == 1
    if n:
        assert len(list(path.rglob('*.vortex'))) >= 3
    read = hl.read_table(path)
    assert t._same(read)
    assert hl.eval(read.g) == hl.Struct(values=[None, 2.5], text='β')
    assert math.isnan(hl.eval(read.nan))
    assert hl.eval(read.wide) == 2**60 + 1
    assert hl.eval(read.missing) is None
    assert read.head(0).globals.dtype == t.globals.dtype
    for f in path.rglob('*.vortex'):
        f.unlink()
    assert hl.eval(read.g.text) == 'β'  # never scans table rows


def test_globals_reads_are_lazy_snapshot_pinned_and_checksummed(tmp_path):
    path = tmp_path / 'table'
    hl.range_table(3).annotate_globals(g=[1, None]).write(path)
    old = hl.read_table(path)
    old_file = next(path.rglob('globals.arrow'))
    data = old_file.read_bytes()
    old_file.unlink()
    read = hl.read_table(path)
    mapped = read.annotate(x=read.g[0])
    assert read.globals.dtype == hl.tstruct(g=hl.tarray(hl.tint32))
    assert mapped.head(0).collect() == []
    with pytest.raises(FileNotFoundError):
        hl.eval(read.g)
    old_file.write_bytes(data)
    assert hl.eval(read.g) == [1, None]
    hl.range_table(1).annotate_globals(g=[2]).write(path, overwrite=True)
    assert hl.eval(old.g) == [1, None]
    assert hl.eval(hl.read_table(path).g) == [2]
    old_file.write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='checksum'):
        hl.eval(old.g)


def test_same_compares_global_schema_values_and_tolerance_even_without_rows():
    a = hl.range_table(0).annotate_globals(a=1.0, b=[2, None])
    b = hl.range_table(0).annotate_globals(b=[2, None], a=1.0)
    assert not a._same(b)
    assert a._same(b, reorder_fields=True)
    assert a._same(a.annotate_globals(a=1.0 + 1e-7))
    assert not a._same(a.annotate_globals(a=1.1))
    assert not a._same(a.annotate_globals(a=hl.int32(1)))
    assert not a._same(a.select_globals('a'))
    assert a._same(a.annotate_globals(a=100.0), tolerance=None)


def test_private_partition_names_do_not_leak_or_shadow_user_fields():
    t = hl.Table.parallelize([{'__vail_partition': 1}, {'__vail_partition': 2}], n_partitions=4)
    t = t.annotate_globals(__vail_partition_=5)
    mapped = t.annotate(x=t['__vail_partition'] + t['__vail_partition_'])
    assert [r.x for r in mapped.collect()] == [6, 7]
    assert mapped.n_partitions() == 4
    assert mapped._same(mapped)


def test_same_and_sort_are_datafusion_plans():
    t = hl.range_table(6, n_partitions=3)
    plan = t._native.same_physical_plan(t._native)
    assert 'SortMergeJoinExec' in plan
    assert 'FilterExec' in plan and 'HailValuesDiffer' in plan
    assert 'OrdinalExec' in plan
    missing = t._native.same_physical_plan(t._native, tolerance=None)
    assert 'HashJoinExec' in missing and 'AggregateExec' in missing
    sorted_table = t.order_by(hl.desc(t.idx))
    assert 'SortExec' in sorted_table._native.physical_plan()
    assert [r.idx for r in sorted_table.collect()] == [5, 4, 3, 2, 1, 0]


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
    assert result._native.physical_plan().count('CrossJoinExec') >= 3
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
    assert third._same(hl.read_table(path))


def test_index_globals_keeps_lazy_snapshot_without_rows_or_python_table(tmp_path):
    import gc
    path = tmp_path / 'source'
    hl.range_table(9, n_partitions=3).annotate_globals(g=11).write(path)
    source = hl.read_table(path)
    indexed = source.index_globals()
    del source
    gc.collect()
    for file in path.rglob('*.vortex'):
        file.unlink()
    globals_file = next(path.rglob('globals.arrow'))
    saved = globals_file.read_bytes()
    globals_file.unlink()
    row = hl.range_table(2)
    mapped = row.annotate(x=indexed.g + row.idx)
    assert 'GlobalsScanExec' in mapped._native.physical_plan()
    assert 'VicebergScanExec' not in mapped._native.physical_plan()
    assert mapped.head(0).collect() == []
    with pytest.raises(FileNotFoundError):
        mapped.collect()
    globals_file.write_bytes(saved)
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


def test_index_globals_does_not_expose_mutable_bindings():
    source = hl.range_table(0).annotate_globals(g=5)
    indexed = source.index_globals()
    wire = indexed.to_ir()
    wire.references.clear()
    wire['reference'] = 'invalid'
    assert hl.eval(indexed.g) == 5
    with pytest.raises(TypeError):
        indexed._global_refs['new'] = source._native.globals_ref()


def test_native_rejects_missing_duplicate_and_mistyped_global_references():
    source = hl.range_table(0).annotate_globals(g=5)
    native = source._native
    expression = source.index_globals()
    wire = expression.to_ir()
    references = list(wire.references.items())
    with pytest.raises(ValueError, match='unbound'):
        native.eval_global_arrow(json.dumps(wire))
    with pytest.raises(ValueError, match='duplicate'):
        native.eval_global_arrow(json.dumps(wire), references + references)
    wire['dtype'] = 'int32'
    with pytest.raises(ValueError, match='type mismatch'):
        native.eval_global_arrow(json.dumps(wire), references)
