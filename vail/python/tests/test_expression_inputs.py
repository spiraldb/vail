import json
from unittest.mock import patch

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


def test_parallelize_expression_source_is_lazy_and_uses_arrow_literal_bindings(tmp_path):
    class Spy:
        def __init__(self, native):
            self.native = native
            self.wires = []
            self.literal_buffers = []
        def __getattr__(self, name):
            return getattr(self.native, name)
        def scalar_arrow(self, data):
            self.literal_buffers.append(data)
            return self.native.scalar_arrow(data)
        def parallelize_expression(self, expression, n_partitions, references):
            self.wires.append(json.loads(expression))
            return self.native.parallelize_expression(expression, n_partitions, references=references)
    spy = Spy(hl._native_session())
    with patch.object(hl, '_native_session', return_value=spy):
        table = hl.Table.parallelize([hl.struct(x=hl.int32('invalid'))])
    assert spy.literal_buffers and all(isinstance(data, bytes) for data in spy.literal_buffers)
    def no_data_literals(node):
        if isinstance(node, dict):
            assert node.get('op') != 'literal'
            for value in node.values(): no_data_literals(value)
        elif isinstance(node, list):
            for value in node: no_data_literals(value)
    for wire in spy.wires: no_data_literals(wire)
    with pytest.raises(RuntimeError):
        table.collect()
    path = tmp_path/'source'
    hl.range_table(4, n_partitions=2).write(path)
    source = hl.read_table(path)
    for file in path.rglob('*.vortex'):
        file.unlink()
    lazy = hl.Table.parallelize(source.collect(_localize=False), n_partitions=2)
    with pytest.raises(Exception):
        lazy.collect()


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
