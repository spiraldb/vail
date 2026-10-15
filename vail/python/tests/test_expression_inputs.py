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
