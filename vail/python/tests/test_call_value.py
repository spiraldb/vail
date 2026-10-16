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


@pytest.mark.parametrize('word', [6, 7, 8, 9, -1, -8, None])
@pytest.mark.parametrize('accessor', ['phase', 'alleles', 'ploidy', 'alternate_count', 'first', 'het', 'hom_var'])
def test_imported_malformed_call_is_rejected_by_executed_accessors(word, accessor):
    import pyarrow as pa
    from vail.arrow import arrow_type
    dtype = arrow_type(hl.tcall)
    calls = pa.StructArray.from_arrays([pa.array([word], type=pa.int32())], fields=list(dtype))
    table = hl.Table.parallelize(pa.table({'c': calls}))
    value = {'phase': lambda: table.c.phased, 'alleles': lambda: table.c.alleles,
             'ploidy': lambda: table.c.ploidy, 'alternate_count': lambda: table.c.n_alt_alleles(),
             'first': lambda: table.c[0], 'het': lambda: table.c.is_het(),
             'hom_var': lambda: table.c.is_hom_var()}[accessor]()
    with pytest.raises((ValueError, RuntimeError), match='Call|call'):
        table.select(value=value).collect()


def test_imported_parent_missing_call_ignores_invalid_hidden_word():
    import pyarrow as pa
    from vail.arrow import arrow_type
    dtype = arrow_type(hl.tcall)
    calls = pa.StructArray.from_arrays(
        [pa.array([8, 21], type=pa.int32())], fields=list(dtype), mask=pa.array([True, False]))
    table = hl.Table.parallelize(pa.table({'c': calls}))
    rows = table.select(alleles=table.c.alleles, phase=table.c.phased,
                        ploidy=table.c.ploidy, n=table.c.n_alt_alleles(), first=table.c[0]).collect()
    assert rows == [hl.Struct(alleles=None, phase=None, ploidy=None, n=None, first=None),
                    hl.Struct(alleles=[1, 0], phase=True, ploidy=2, n=1, first=1)]


@pytest.mark.parametrize('phased', [False, True])
def test_dynamic_call_construction_missing_values_and_boundaries(phased):
    pairs = [(0, 32767), (16383, 16384), (1, 0), (None, 1), (1, None)]
    table = hl.Table.parallelize([{'a': a, 'b': b} for a, b in pairs],
                                 schema=hl.tstruct(a=hl.tint32, b=hl.tint32))
    table = table.select(c=hl.call(table.a + 0, table.b * 1, phased=phased))
    rows = table.select(c=table.c, a=table.c.alleles, phase=table.c.phased, ploidy=table.c.ploidy).collect()
    expected = [hl.Call([a, b], phased) if a is not None and b is not None else None for a,b in pairs]
    assert [r.c for r in rows] == expected
    assert [r.a for r in rows] == [c.alleles if c else None for c in expected]
    assert [r.phase for r in rows] == [c.phased if c else None for c in expected]
    assert [r.ploidy for r in rows] == [c.ploidy if c else None for c in expected]
    for alleles in [(2**31-1,2**31-1),(-1,0),(32767,32767)]:
        bad = hl.Table.parallelize([{'a':alleles[0],'b':alleles[1]}])
        with pytest.raises((ValueError,RuntimeError), match='Call|allele'):
            bad.select(c=hl.call(bad.a,bad.b,phased=phased)).collect()


def test_call_adapters_keep_lazy_errors_haploid_and_empty_batches():
    table = hl.range_table(3)
    valid = hl.call(table.idx + 1, phased=True)
    invalid = hl.call(table.idx - 10)
    chosen = hl.if_else(table.idx >= 0, valid, invalid)
    rows = table.select(c=chosen, a=chosen.alleles, phase=chosen.phased, ploidy=chosen.ploidy).collect()
    assert rows == [hl.Struct(idx=i, c=hl.Call([i + 1], True), a=[i + 1], phase=True, ploidy=1) for i in range(3)]
    empty = hl.range_table(0)
    c = hl.call(empty.idx, 1)
    assert empty.select(c=c, a=c.alleles, phase=c.phased).collect() == []


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


@pytest.mark.parametrize("property", [
    "is_haploid", "is_diploid", "is_hom_ref", "is_non_ref", "is_het",
    "is_het_ref", "is_het_non_ref", "is_hom_var", "n_alt_alleles",
])
def test_packed_call_properties_preserve_missingness_of_sliced_parent(property):
    import pyarrow as pa
    from vail.arrow import arrow_type

    dtype = arrow_type(hl.tcall)
    # Invalid words beneath missing parents must never be validated or exposed.
    values = pa.StructArray.from_arrays(
        [pa.array([6, 21, 8, -6, 20], type=pa.int32())], fields=list(dtype),
        mask=pa.array([True, False, True, False, False]),
    ).slice(1, 3)
    table = hl.Table.parallelize(pa.table({"c": values}))
    result = table.select(value=getattr(table.c, property)()).collect()
    def expected_for(alleles):
        het = len(alleles) == 2 and alleles[0] != alleles[1]
        return {
            "is_haploid": len(alleles) == 1, "is_diploid": len(alleles) == 2,
            "is_hom_ref": all(a == 0 for a in alleles),
            "is_non_ref": any(a > 0 for a in alleles), "is_het": het,
            "is_het_ref": het and 0 in alleles,
            "is_het_non_ref": het and all(a > 0 for a in alleles),
            "is_hom_var": not het and all(a > 0 for a in alleles),
            "n_alt_alleles": sum(a > 0 for a in alleles),
        }[property]
    expected = [expected_for([1, 0]), None, expected_for([(1 << 29) - 1])]
    assert [row.value for row in result] == expected
