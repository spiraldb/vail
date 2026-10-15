import gc
import shutil

import pytest
import vail as hl


def test_parallelize_expression_rows_and_nested_expression_values():
    rows = [hl.struct(k=hl.int64(2), x=hl.int32('4')),
            {'k': hl.int64(1), 'x': hl.case().when(True, 3).default(9)}]
    table = hl.Table.parallelize(rows, n_partitions=3)
    assert table.row.dtype == hl.tstruct(k=hl.tint64, x=hl.tint32)
    assert table.n_partitions() == 3
    table = table.key_by('k')
    assert table.collect() == [hl.Struct(k=1, x=3), hl.Struct(k=2, x=4)]
    nested = hl.Table.parallelize([{'a': [hl.int32('2'), None],
                                  's': {'x': hl.int32(4)}, 't': (hl.int32(1), 'a')}])
    assert nested.collect() == [hl.Struct(a=[2, None], s=hl.Struct(x=4), t=(1, 'a'))]


def test_parallelize_array_expression_and_lazy_scalar_dependencies():
    source = hl.range_table(5, n_partitions=2).annotate_globals(g=7)
    rows = source.row.collect(_localize=False).map(lambda row: hl.struct(x=row.idx + 2))
    table = hl.Table.parallelize(rows, n_partitions=3, globals={'label': 'test'})
    assert table.collect() == [hl.Struct(x=i+2) for i in range(5)]
    assert hl.eval(table.label) == 'test'
    globals_table = hl.Table.parallelize([hl.struct(x=source.g), hl.struct(x=source.index_globals().g + 1)])
    assert globals_table.collect() == [hl.Struct(x=7), hl.Struct(x=8)]


def test_parallelize_expression_sources_remain_lazy_until_action(tmp_path):
    table = hl.Table.parallelize([hl.struct(x=hl.int32('invalid'))])
    with pytest.raises(RuntimeError):
        table.collect()
    path = tmp_path / 'source'
    hl.range_table(4, n_partitions=2).write(path)
    source = hl.read_table(path)
    shutil.rmtree(path)
    lazy = hl.Table.parallelize(source.collect(_localize=False), n_partitions=2)
    with pytest.raises((ValueError, RuntimeError, OSError)):
        lazy.collect()


def test_arrow_and_literal_inputs_outlive_python_producers():
    import pyarrow as pa
    values = pa.array([1, None, 3], type=pa.int32())
    producer = pa.record_batch([values], names=['x'])
    table = hl.Table.parallelize(producer, n_partitions=2)
    literal_values = [1, None, 3]
    expr = hl.literal(literal_values, hl.tarray(hl.tint32))
    del producer, values
    literal_values[:] = [99]
    del literal_values
    gc.collect()
    assert table.collect() == [hl.Struct(x=1), hl.Struct(x=None), hl.Struct(x=3)]
    assert hl.eval(expr) == [1, None, 3]
    annotated = table.annotate_globals(values=expr)
    assert hl.eval(annotated.values) == [1, None, 3]
    assert annotated.x.collect() == [1, None, 3]


def test_parallelize_expression_validation_and_empty_rows():
    source = hl.range_table(2)
    with pytest.raises(hl.ExpressionException, match='unindexed'):
        hl.Table.parallelize([hl.struct(x=source.idx)])
    with pytest.raises(hl.ExpressionException, match='aggregate'):
        hl.Table.parallelize([hl.struct(x=hl.agg.sum(source.idx))])
    with pytest.raises(TypeError, match='struct'):
        hl.Table.parallelize(hl.array([1, 2]))
    with pytest.raises(TypeError, match='schema'):
        hl.Table.parallelize(hl.array([hl.struct(x=1)]), schema=hl.tstruct(x=hl.tstr))
    empty = hl.Table.parallelize(hl.literal([], hl.tarray(hl.tstruct(x=hl.tint32))), n_partitions=3)
    assert empty.n_partitions() == 3
    assert empty.collect() == []
    assert hl.Table.parallelize(hl.array([hl.struct(), hl.struct()])).collect() == [hl.Struct(), hl.Struct()]


def test_expression_inputs_infer_across_missing_and_empty_values():
    table = hl.Table.parallelize([{'x': None, 'a': []}, {'x': hl.int32(3), 'a': [hl.int32(4)]}])
    assert table.collect() == [hl.Struct(x=None, a=[]), hl.Struct(x=3, a=[4])]


def test_expression_inputs_reject_missing_containers_at_action_time():
    missing_array = hl.Table.parallelize(hl.missing(hl.tarray(hl.tstruct(x=hl.tint32))))
    missing_row = hl.Table.parallelize(hl.array([hl.missing(hl.tstruct(x=hl.tint32))]))
    for table in (missing_array, missing_row):
        for action in (table.collect, table.count, lambda: table.aggregate(hl.agg.count())):
            with pytest.raises((ValueError, RuntimeError), match='missing'):
                action()


def test_dictionary_expression_keys_use_declared_type_and_remain_lazy():
    schema = hl.tstruct(d=hl.tdict(hl.tint32, hl.tstr))
    table = hl.Table.parallelize([{'d': {hl.int32('2'): 'value'}}], schema=schema)
    assert table.collect() == [hl.Struct(d={2: 'value'})]
    invalid = hl.Table.parallelize([{'d': {hl.int32('bad'): 'value'}}], schema=schema)
    with pytest.raises(RuntimeError, match='bad|parse|cast'):
        invalid.collect()
    source = hl.range_table(2).annotate_globals(k=7)
    globally_keyed = hl.Table.parallelize([{'d': {source.k: 'global'}}], schema=schema)
    assert globally_keyed.collect() == [hl.Struct(d={7: 'global'})]
    with pytest.raises(hl.ExpressionException, match='unindexed'):
        hl.Table.parallelize([{'d': {source.idx: 'row'}}], schema=schema)
    other = hl.range_table(1).annotate_globals(value='other')
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        hl.Table.parallelize([{'d': {source.k: other.value}}], schema=schema)


def test_mixed_input_literal_payloads_snapshot_and_survive_producer_lifetime():
    values = [1, None, 3]
    row = {'x': hl.int32('4'), 'values': values, 'empty': hl.Struct()}
    table = hl.Table.parallelize([row], schema=hl.tstruct(
        x=hl.tint32, values=hl.tarray(hl.tint32), empty=hl.tstruct()))
    values[:] = [99]
    del row, values
    gc.collect()
    assert table.collect() == [hl.Struct(x=4, values=[1, None, 3], empty=hl.Struct())]
    assert table.collect() == [hl.Struct(x=4, values=[1, None, 3], empty=hl.Struct())]


def test_input_binding_distinct_inline_siblings_and_shared_globals():
    source = hl.range_table(1).annotate_globals(a=2, b=5, c=9)
    shared = source.a + source.a
    for _ in range(4):
        shared = shared + shared
    rows = hl.array([hl.struct(x=hl.array([hl.struct(v=source.a)]), y=shared),
                     hl.struct(x=hl.array([hl.struct(v=source.b), hl.struct(v=source.c)]), y=shared)])
    table = hl.Table.parallelize(rows)
    del rows, shared, source
    gc.collect()
    assert table.collect() == [hl.Struct(x=[hl.Struct(v=2)], y=64),
                               hl.Struct(x=[hl.Struct(v=5), hl.Struct(v=9)], y=64)]


@pytest.mark.parametrize('depth', [0, 4, 8, 12])
def test_shared_expression_inputs_keep_values(depth):
    value = hl.int64(1)
    for _ in range(depth):
        value = value + value
    table = hl.Table.parallelize(hl.array([hl.struct(x=value, y=value)]))
    assert table.collect() == [hl.Struct(x=2**depth, y=2**depth)]


def test_input_binding_rejects_forbidden_nodes_in_later_inline_siblings():
    source = hl.range_table(2).annotate_globals(a=2)
    for bad, message in [(source.idx, 'unindexed'), (hl.agg.sum(source.idx), 'aggregate')]:
        rows = hl.array([hl.struct(x=hl.array([hl.struct(v=hl.int64(source.a))])),
                         hl.struct(x=hl.array([hl.struct(v=hl.int64(bad))]))])
        with pytest.raises(hl.ExpressionException, match=message):
            hl.Table.parallelize(rows)


def test_native_backend_local_filesystem_and_selection(tmp_path):
    from vail.backend import choose_backend
    backend = hl.current_backend()
    assert choose_backend() == choose_backend('vail') == 'vail'
    assert backend.supports('local_filesystem')
    assert not backend.supports('unavailable-capability')
    path = tmp_path / 'local values.txt'
    with backend.fs.open(path, 'w', encoding='utf-8') as output:
        output.write('α\n')
    with backend.fs.open(path.as_uri(), encoding='utf-8') as source:
        assert source.read() == 'α\n'
    with pytest.raises(ValueError, match='cannot select backend'):
        choose_backend('spark')


def test_input_globals_bind_across_switch_optional_slice_and_lambda_scopes():
    source = hl.range_table(1).annotate_globals(a=1, b=2, c=4, values=[10, 20, 30, 40])
    rows = hl.array([hl.struct(
        choice=hl.switch(source.a).when(source.b, source.c).when_missing(source.c).default(source.b),
        selected=source.values[source.a:source.c:source.b],
        mapped=source.values.map(lambda x: x + source.b),
    )])
    table = hl.Table.parallelize(rows)
    assert table.collect() == [hl.Struct(choice=2, selected=[20, 40], mapped=[12, 22, 32, 42])]
