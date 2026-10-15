import pytest

import vail as hl


@pytest.mark.parametrize('alleles,phased,expected', [
    ([], False, []), ([2], True, [2]), ([1, 0], False, [0, 1]), ([1, 0], True, [1, 0]),
])
def test_call_public_list_and_immutable_hash_state(alleles, phased, expected):
    call = hl.Call(alleles=alleles, phased=phased)
    assert call.alleles == expected
    assert isinstance(call.alleles, list)
    assert call[:] == expected
    same = hl.Call(tuple(alleles), phased)
    assert call == same
    assert hash(call) == hash(same)
    values = {call: 'present'}
    alleles.append(99)
    exposed = call.alleles
    exposed.append(98)
    assert call.alleles == expected
    assert values[same] == 'present'
    assert hl.eval(hl.literal(call, hl.tcall)) == call
    assert hl.eval(hl.literal(call)).alleles == expected
    table = hl.Table.parallelize([{'gt': call}], schema=hl.tstruct(gt=hl.tcall))
    assert table.collect()[0].gt.alleles == expected


def test_call_equality_with_expressions_and_phase_order():
    assert hl.Call([1, 0]) == hl.Call([0, 1])
    assert hl.Call([1, 0], True) != hl.Call([0, 1], True)
    assert hl.Call([0, 1], True) != hl.Call([0, 1])
    value, expression = hl.Call([0, 0]), hl.call(0, 0)
    assert hl.eval(value == expression)
    assert hl.eval(expression == value)
