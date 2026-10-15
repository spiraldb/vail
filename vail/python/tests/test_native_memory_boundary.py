"""Ownership and representation contracts for the in-process native boundary."""
import gc

import pyarrow as pa
import pytest
import vail as hl


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
    owner = table.to_arrow()
    first = pa.table(owner)
    second = table.to_arrow()
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
    result = table.to_arrow()
    assert result.num_rows == 3 and result.num_columns == 0


def test_repeated_subexpressions_preserve_earlier_expression_values():
    original = hl.int64(1)
    value = original
    for _ in range(12):
        value = value + value
    assert hl.eval(original) == 1
    assert hl.eval(value) == 4096


@pytest.mark.parametrize('partitions', [1, 2, 7])
def test_multibatch_import_preserves_rows_partitions_and_export_ownership(partitions):
    arrays=[pa.array([1,None],type=pa.int32()),pa.array([],type=pa.int32()),pa.array([3,4,5],type=pa.int32())]
    original=pa.Table.from_batches([pa.record_batch([array],names=['x']) for array in arrays])
    addresses={array.buffers()[1].address for array in arrays if len(array)}
    table=hl.Table.parallelize(original,n_partitions=partitions)
    del arrays,original
    gc.collect()
    assert table.n_partitions()==partitions
    first=table.to_arrow()
    assert first.to_pylist()==[{'x':1},{'x':None},{'x':3},{'x':4},{'x':5}]
    if partitions==1:
        assert {chunk.buffers()[1].address for chunk in first['x'].chunks}==addresses
    second=table.to_arrow()
    del first,table
    gc.collect()
    assert second.to_pylist()==[{'x':1},{'x':None},{'x':3},{'x':4},{'x':5}]


def test_public_backend_scalar_import_checks_stream_row_count():
    schema=pa.schema([('x',pa.int32())])
    def batch(values): return pa.record_batch([pa.array(values,type=pa.int32())],schema=schema)
    session=hl.current_backend().session
    for batches in [[],[batch([])],[batch([1,2])],[batch([1]),batch([]),batch([2])]]:
        reader=pa.RecordBatchReader.from_batches(schema,batches)
        with pytest.raises(ValueError,match='exactly one row'):
            session.scalar_arrow(reader)
        reader.close()
    reader=pa.RecordBatchReader.from_batches(schema,[batch([]),batch([7]),batch([])])
    scalar=session.scalar_arrow(reader)
    reader.close()
    del reader
    gc.collect()
    assert [name for name,_ in scalar.schema()]==['x']


def test_zero_column_multibatch_export_preserves_cardinality():
    batches=[pa.RecordBatch.from_struct_array(pa.array([{}]*rows,type=pa.struct([])))
             for rows in [2,0,3]]
    table=hl.Table.parallelize(pa.Table.from_batches(batches),n_partitions=7)
    exported=table.to_arrow()
    assert exported.num_rows==5 and exported.num_columns==0
    assert exported.to_pylist()==[{}]*5
    assert table.n_partitions()==7
