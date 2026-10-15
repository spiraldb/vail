"""Ownership and representation contracts for the in-process native boundary."""
import gc
import json
from unittest.mock import patch

import pyarrow as pa
import pytest
import vail as hl
from vail._native import NativeExpr, NativeType
from vail.arrow import read_arrow


def test_expression_edges_share_native_nodes_and_keep_them_alive():
    source = hl.literal(17)
    source_node = source.to_ir()
    result = source + source
    assert result.to_ir()['left'].same_node_as(source_node)
    assert result.to_ir()['right'].same_node_as(source_node)
    del source, source_node
    gc.collect()
    assert hl.eval(result) == 34
    with pytest.raises(TypeError):
        result.to_ir()['left'] = hl.literal(0).to_ir()


def test_arrow_buffers_survive_both_owners_and_capsule_replays():
    array = pa.array([1, None, 3], type=pa.int32())
    address = array.buffers()[1].address
    table = hl.Table.parallelize(pa.table({'x': array}))
    del array
    gc.collect()
    owner = table._native.collect_arrow()
    first = pa.table(owner)
    second = pa.table(owner)
    assert first['x'].chunk(0).buffers()[1].address == address
    assert second['x'].chunk(0).buffers()[1].address == address
    del owner, table, first
    gc.collect()
    assert second.to_pylist() == [{'x': 1}, {'x': None}, {'x': 3}]


def test_api_construction_and_execution_do_not_use_json_codecs():
    def reject(*args, **kwargs):
        raise AssertionError('JSON is not an in-process transport')
    with (patch.object(json, 'dumps', reject), patch.object(json, 'loads', reject),
          patch.object(pa.ipc, 'open_stream', reject), patch.object(pa.ipc, 'new_stream', reject)):
        table = hl.Table.parallelize([{'a': [1, 2]}, {'a': [3]}],
                                     schema=hl.tstruct(a=hl.tarray(hl.tint32)))
        table = table.annotate(total=hl.sum(table.a))
        assert table.total.collect() == [3, 3]
        assert table.aggregate(hl.agg.sum(table.total)) == 6
        assert hl.eval(hl.bind(lambda x: x + x, hl.int32(4))) == 8
        assert hl.eval(hl.literal((1, {2, 3}), hl.ttuple(hl.tint32, hl.tset(hl.tint32)))) == (1, {2, 3})


def test_type_wrappers_only_own_native_type_structure():
    dtype = hl.tstruct(a=hl.tarray(hl.tint32), b=hl.tdict(hl.tstr, hl.tint64))
    assert isinstance(dtype._native_type, NativeType)
    assert '_fields' not in getattr(dtype, '__dict__', {})
    assert dtype['a'].element_type == hl.tint32
    assert dtype['b'].key_type == hl.tstr
    assert isinstance(hl.literal(1)._native_expr, NativeExpr)


def test_arrow_empty_struct_rows_keep_cardinality():
    original = pa.RecordBatch.from_struct_array(pa.array([{}, {}, {}], type=pa.struct([])))
    table = hl.Table.parallelize(original)
    assert table.count() == 3
    _, result = read_arrow(table._native.collect_arrow())
    assert result.num_rows == 3 and result.num_columns == 0


def test_repeated_subexpressions_remain_a_shared_graph():
    value = hl.int64(1)
    for _ in range(64):
        previous = value.to_ir()
        value = value + value
        node = value.to_ir()
        assert node['left'].same_node_as(previous)
        assert node['right'].same_node_as(previous)
    assert value.to_ir() == value.to_ir()
