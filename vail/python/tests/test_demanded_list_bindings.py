"""Repeated list reads retain demand, nulls and callback coordinates."""
import pytest
import vail as hl




def test_list_result_and_conditional_consumers():
    table = hl.Table.parallelize(
        [{'id': 0, 'xs': [[1, 9], [4]], 'i': 0},
         {'id': 1, 'xs': [None, [5]], 'i': 0},
         {'id': 2, 'xs': [[None, 7]], 'i': 0},
         {'id': 3, 'xs': None, 'i': 0},
         {'id': 4, 'xs': [[2]], 'i': None}],
        schema=hl.tstruct(id=hl.tint32, xs=hl.tarray(hl.tarray(hl.tint32)), i=hl.tint32))
    selected = table.xs[table.i]
    result = table.select(selected=selected,
                          size=hl.if_else(hl.is_defined(selected), hl.len(selected), -1),
                          missing=hl.is_missing(selected))
    assert [(r.selected, r.size, r.missing) for r in result.collect()] == [
        ([1, 9], 2, False), (None, -1, True), ([None, 7], 2, False),
        (None, -1, True), (None, -1, True)]
    assert result.to_arrow().equals(result.to_arrow(), check_metadata=True)  # replay consistency


def test_projected_away_anchor_does_not_demand_bad_index():
    table = hl.Table.parallelize([{'xs': [], 'take': False}],
                                schema=hl.tstruct(xs=hl.tarray(hl.tint32), take=hl.tbool))
    read = table.xs[0]
    result = table.select(anchor=read, chosen=hl.if_else(table['take'], read, 17))
    # Projection removes the only unconditional consumer before execution.
    assert result.select(chosen=result.chosen).chosen.collect() == [17]


def test_outer_guard_and_empty_callback():
    table = hl.Table.parallelize([{'xs': []}, {'xs': [None]}, {'xs': [8]}, {'xs': None}],
                                schema=hl.tstruct(xs=hl.tarray(hl.tint32)))
    read = table.xs[0]
    result = hl.if_else(hl.len(table.xs) > 0,
                        hl.struct(value=read, fallback=hl.coalesce(read, 99)),
                        hl.struct(value=hl.int32(-1), fallback=hl.int32(-1)))
    rows = table.select(result=result).collect()
    assert [(None if r.result is None else (r.result.value, r.result.fallback)) for r in rows] == [
        (-1, -1), (None, 99), (8, 8), None]
    assert hl.eval(hl.empty_array(hl.tint32).map(lambda x: hl.struct(value=x, repeated=x))) == []


def test_scalar_and_distinct_reads():
    table = hl.Table.parallelize([{'xs': [3, 8], '__vail_demanded_list_0': 5}],
                                schema=hl.tstruct(xs=hl.tarray(hl.tint32), __vail_demanded_list_0=hl.tint32))
    first = table.xs[0]
    second = table.xs[1]
    row = table.select(first=first, again=hl.coalesce(first, -1), second=second,
                       constant=hl.array([10, 20])[1], reserved=table['__vail_demanded_list_0']).collect()[0]
    assert (row.first, row.again, row.second, row.constant, row.reserved) == (3, 3, 8, 20, 5)


def test_call_metadata_and_nested_callback_domains():
    table = hl.Table.parallelize([{'xs': [[0], [1]], 'calls': [hl.call(1, 0, phased=True)]}],
        schema=hl.tstruct(xs=hl.tarray(hl.tarray(hl.tint32)), calls=hl.tarray(hl.tcall)))
    call = table.calls[0]
    result = table.select(call=call, copy=hl.if_else(hl.is_defined(call), call, hl.missing(hl.tcall)),
                          nested=table.xs.map(lambda xs: hl.struct(value=xs[0], other=hl.coalesce(xs[0], -1))))
    row = result.collect()[0]
    assert row.call.phased and row.copy.phased
    assert list(row.call.alleles) == [1, 0] and list(row.copy.alleles) == [1, 0]
    assert [(x.value, x.other) for x in row.nested] == [(0, 0), (1, 1)]
    expected = hl.Table.parallelize([{'call': hl.Call((1, 0), True), 'copy': hl.Call((1, 0), True),
                                      'nested': [{'value': 0, 'other': 0}, {'value': 1, 'other': 1}]}],
        schema=hl.tstruct(call=hl.tcall, copy=hl.tcall, nested=hl.tarray(hl.tstruct(value=hl.tint32, other=hl.tint32))))
    assert result.to_arrow().equals(expected.to_arrow(), check_metadata=True)


def test_explicit_error_prefix_is_not_crossed():
    table = hl.Table.parallelize([{'xs': []}], schema=hl.tstruct(xs=hl.tarray(hl.tint32)))
    read = table.xs[0]
    stop = hl.case().when(False, hl.int32(1)).or_error('prefix stop')
    query = table.select(prefix=stop, value=read, repeated=hl.coalesce(read, -1))
    with pytest.raises(Exception, match='prefix stop'):
        query.collect()
