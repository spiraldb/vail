"""Public Call downcoding preserves alleles, ploidy, phase, and nulls."""
import pytest
import vail as hl


@pytest.mark.parametrize('alleles, phased, target, expected', [
    ((), False, 1, ()),
    ((), True, 1, ()),
    ((), False, None, None),
    ((), True, None, None),
    ((1,), False, 1, (1,)),
    ((2,), True, 1, (0,)),
    ((1,), True, None, None),
    ((0, 1), False, 1, (0, 1)),
    ((1, 2), False, 1, (0, 1)),
    ((1, 2), True, 1, (1, 0)),
    ((1, 2), True, 2, (0, 1)),
    ((1, 1), False, 1, (1, 1)),
    ((1, 2), False, -1, (0, 0)),
    ((1, 2), True, 65536, (0, 0)),
    ((65535, 65535), False, 65535, (1, 1)),
    ((1, 2), False, None, None),
])
def test_literal_downcode_preserves_ploidy_phase_and_nulls(alleles, phased, target, expected):
    index = hl.missing(hl.tint32) if target is None else hl.int32(target)
    result = hl.eval(hl.downcode(hl.call(*alleles, phased=phased), index))
    assert result == (None if expected is None else hl.Call(expected, phased=phased))


def test_missing_call_stays_missing():
    assert hl.eval(hl.downcode(hl.missing(hl.tcall), 1)) is None
    assert hl.eval(hl.downcode(hl.missing(hl.tcall), hl.missing(hl.tint32))) is None


def test_scalar_call_broadcasts_over_target_column():
    table = hl.Table.parallelize([{'i': i} for i in [1, 2, -1, 65536, None]],
                                schema=hl.tstruct(i=hl.tint32))
    output = table.annotate(result=hl.downcode(hl.call(1, 2, phased=True), table.i))
    assert [row.result for row in output.collect()] == [
        hl.Call((1, 0), True), hl.Call((0, 1), True),
        hl.Call((0, 0), True), hl.Call((0, 0), True), None]
    missing = table.annotate(result=hl.downcode(hl.missing(hl.tcall), table.i))
    assert [row.result for row in missing.collect()] == [None] * 5
    empty = table.filter(False)
    assert empty.annotate(result=hl.downcode(hl.call(1), empty.i)).collect() == []


def test_sliced_nullable_calls_keep_values_and_arrow_metadata():
    source = hl.Table.parallelize([
        {'c': hl.Call((9,)), 'i': 9},
        {'c': hl.Call((1, 2), True), 'i': 1},
        {'c': None, 'i': 1},
        {'c': hl.Call((1, 2)), 'i': None},
        {'c': hl.Call((), True), 'i': 1},
        {'c': hl.Call((65535,)), 'i': 65535},
    ], schema=hl.tstruct(c=hl.tcall, i=hl.tint32))
    table = hl.Table.parallelize(source.to_arrow().slice(1, 4))
    result = table.annotate(result=hl.downcode(table.c, table.i))
    arrow = result.to_arrow()
    del result, table, source
    restored = hl.Table.parallelize(arrow)
    assert restored.result.dtype == hl.tcall
    assert [row.result for row in restored.collect()] == [
        hl.Call((1, 0), True), None, None, hl.Call((), True)]


def test_call_column_and_nested_array_map_use_the_public_operation():
    calls = hl.literal([hl.Call((0, 1)), None, hl.Call((2,), True)], hl.tarray(hl.tcall))
    assert hl.eval(calls.map(lambda value: hl.downcode(value, 1))) == [
        hl.Call((0, 1)), None, hl.Call((0,), True)]
    table = hl.Table.parallelize([{'c': hl.Call((0, 1))}, {'c': None}],
                                schema=hl.tstruct(c=hl.tcall))
    output = table.annotate(result=hl.downcode(table.c, 1))
    assert [row.result for row in output.collect()] == [hl.Call((0, 1)), None]


def test_downcode_checks_public_argument_types():
    with pytest.raises(TypeError, match='Call and an int32'):
        hl.downcode(1, 1)
    with pytest.raises(TypeError, match='Call and an int32'):
        hl.downcode(hl.call(1), hl.int64(1))
