import pytest

import vail as hl
from vail.types import decode_value, encode_value


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
    assert decode_value(encode_value(call, hl.tcall), hl.tcall) == call
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
def test_call_predicates_use_packed_words_for_every_supported_ploidy():
    import vail as hl

    calls = [None]
    for phased in (False, True):
        calls.extend(hl.Call(alleles, phased=phased) for alleles in (
            [], [0], [2], [0, 0], [0, 1], [1, 0], [1, 1], [1, 2], [2, 1], [0, 30000],
        ))
    calls.append(hl.Call([30000, 30000]))
    table = hl.Table.parallelize([{"c": call} for call in calls], schema=hl.tstruct(c=hl.tcall))
    result = table.select(
        ploidy=table.c.ploidy, haploid=table.c.is_haploid(), diploid=table.c.is_diploid(),
        hom_ref=table.c.is_hom_ref(), hom_var=table.c.is_hom_var(), non_ref=table.c.is_non_ref(),
        het=table.c.is_het(), het_ref=table.c.is_het_ref(), het_non_ref=table.c.is_het_non_ref(),
        n_alt=table.c.n_alt_alleles(),
    ).collect()
    assert all(value is None for value in result[0].values())
    for call, row in zip(calls[1:], result[1:]):
        alleles = list(call.alleles)
        heterozygous = len(alleles) == 2 and alleles[0] != alleles[1]
        assert row.ploidy == len(alleles)
        assert row.haploid == (len(alleles) == 1)
        assert row.diploid == (len(alleles) == 2)
        assert row.hom_ref == (bool(alleles) and all(a == 0 for a in alleles))
        assert row.hom_var == (bool(alleles) and len(set(alleles)) == 1 and alleles[0] > 0)
        assert row.non_ref == any(a > 0 for a in alleles)
        assert row.het == heterozygous
        assert row.het_ref == (heterozygous and 0 in alleles)
        assert row.het_non_ref == (heterozygous and all(a > 0 for a in alleles))
        assert row.n_alt == sum(a > 0 for a in alleles)
