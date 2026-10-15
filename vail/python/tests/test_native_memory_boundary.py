"""Ownership and representation contracts for the in-process native boundary."""
import gc

import pyarrow as pa
import pytest
import vail as hl
from vail.arrow import read_arrow


def test_expression_values_survive_source_lifetime_and_derived_operations():
    source = hl.literal(17)
    result = source + source
    derived = result + 1
    assert hl.eval(source) == 17
    assert hl.eval(result) == 34
    del source
    gc.collect()
    assert hl.eval(result) == 34
    assert hl.eval(derived) == 35


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


def test_nested_values_and_expressions_cross_the_native_boundary():
    table = hl.Table.parallelize([{'a': [1, 2]}, {'a': [3]}],
                                 schema=hl.tstruct(a=hl.tarray(hl.tint32)))
    table = table.annotate(total=hl.sum(table.a))
    assert table.total.collect() == [3, 3]
    assert table.aggregate(hl.agg.sum(table.total)) == 6
    assert hl.eval(hl.bind(lambda x: x + x, hl.int32(4))) == 8
    assert hl.eval(hl.literal((1, {2, 3}), hl.ttuple(hl.tint32, hl.tset(hl.tint32)))) == (1, {2, 3})


def test_nested_types_are_immutable_and_roundtrip_through_arrow():
    from vail.arrow import arrow_type, dtype_from_arrow
    dtype = hl.tstruct(a=hl.tarray(hl.tint32), b=hl.tdict(hl.tstr, hl.tint64))
    assert dtype['a'].element_type == hl.tint32
    assert dtype['b'].key_type == hl.tstr
    with pytest.raises(TypeError, match='immutable'):
        dtype.a = hl.tstr
    with pytest.raises(TypeError, match='immutable'):
        dtype['a'].element_type = hl.tstr
    assert dtype_from_arrow(arrow_type(dtype)) == dtype
    rows = [hl.Struct(a=[1, None], b={'x': 2})]
    table = hl.Table.parallelize(rows, schema=dtype)
    del dtype
    gc.collect()
    assert table.collect() == rows


def test_arrow_empty_struct_rows_keep_cardinality():
    original = pa.RecordBatch.from_struct_array(pa.array([{}, {}, {}], type=pa.struct([])))
    table = hl.Table.parallelize(original)
    assert table.count() == 3
    _, result = read_arrow(table._native.collect_arrow())
    assert result.num_rows == 3 and result.num_columns == 0


def test_repeated_subexpressions_preserve_earlier_expression_values():
    original = hl.int64(1)
    value = original
    for _ in range(12):
        value = value + value
    assert hl.eval(original) == 1
    assert hl.eval(value) == 4096
