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


@pytest.mark.parametrize('fields,expected', [
    ((1, 0, 0, False), ([], 0, 1, False, False)),
    ((0, 1, 1, False), ([0], 1, 1, False, False)),
    ((2, 1, 2, False), ([2, 1], 2, 2, True, False)),
])
def test_imported_calls_use_stored_fields_without_canonical_validation(fields, expected):
    import pyarrow as pa
    from vail.arrow import arrow_type
    dtype = arrow_type(hl.tcall)
    calls = pa.StructArray.from_arrays(
        [pa.array([value], type=field.type) for value, field in zip(fields, dtype)],
        fields=list(dtype),
    )
    table = hl.Table.parallelize(pa.table({'c': calls}))
    row = table.select(alleles=table.c.alleles, ploidy=table.c.ploidy,
                       n=table.c.n_alt_alleles(), het=table.c.is_het(),
                       hom_var=table.c.is_hom_var()).collect()[0]
    assert (row.alleles, row.ploidy, row.n, row.het, row.hom_var) == expected


def test_imported_calls_do_not_scan_unreferenced_storage_invariants():
    import pyarrow as pa
    from vail.arrow import arrow_type
    dtype = arrow_type(hl.tcall)
    calls = pa.StructArray.from_arrays(
        [pa.array([value], type=field.type) for value, field in zip((None, 0, 3, False), dtype)],
        fields=list(dtype),
    )
    table = hl.Table.parallelize(pa.table({'c': calls}))
    assert table.select(n=table.c.ploidy, phase=table.c.phased).collect() == [hl.Struct(n=3, phase=False)]


def test_imported_parent_missing_call_ignores_invalid_hidden_word():
    import pyarrow as pa
    from vail.arrow import arrow_type
    dtype = arrow_type(hl.tcall)
    calls = pa.StructArray.from_arrays(
        [pa.array(v,type=f.type) for v,f in zip([[0,1],[0,0],[3,2],[False,True]],dtype)], fields=list(dtype), mask=pa.array([True, False]))
    table = hl.Table.parallelize(pa.table({'c': calls}))
    rows = table.select(alleles=table.c.alleles, phase=table.c.phased,
                        ploidy=table.c.ploidy, n=table.c.n_alt_alleles(), first=table.c[0]).collect()
    assert rows == [hl.Struct(alleles=None, phase=None, ploidy=None, n=None, first=None),
                    hl.Struct(alleles=[1, 0], phase=True, ploidy=2, n=1, first=1)]


@pytest.mark.parametrize('phased', [False, True])
def test_dynamic_call_construction_missing_values_and_boundaries(phased):
    pairs = [(0, 32767), (65535, 65535), (1, 0), (None, 1), (1, None)]
    table = hl.Table.parallelize([{'a': a, 'b': b} for a, b in pairs],
                                 schema=hl.tstruct(a=hl.tint32, b=hl.tint32))
    table = table.select(c=hl.call(table.a + 0, table.b * 1, phased=phased))
    rows = table.select(c=table.c, a=table.c.alleles, phase=table.c.phased, ploidy=table.c.ploidy).collect()
    expected = [hl.Call([a, b], phased) if a is not None and b is not None else None for a,b in pairs]
    assert [r.c for r in rows] == expected
    assert [r.a for r in rows] == [c.alleles if c else None for c in expected]
    assert [r.phase for r in rows] == [c.phased if c else None for c in expected]
    assert [r.ploidy for r in rows] == [c.ploidy if c else None for c in expected]
    for alleles in [(2**31-1,2**31-1),(-1,0),(65536,0)]:
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
        [pa.array(v,type=f.type) for v,f in zip([[0,1,0,65535,1],[0,0,0,0,1],[3,2,3,1,2],[False,True,False,False,False]],dtype)], fields=list(dtype),
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
    expected = [expected_for([1, 0]), None, expected_for([65535])]
    assert [row.value for row in result] == expected


def test_packed_call_accessors_after_storage_projection(tmp_path):
    calls = [None, hl.Call([]), hl.Call([0]), hl.Call([1]), hl.Call([0, 1]),
             hl.Call([1, 0], True), hl.Call([30000, 30000])]
    table = hl.Table.parallelize([{"c": call} for call in calls], schema=hl.tstruct(c=hl.tcall))
    table.write(str(tmp_path / "calls"))
    read = hl.read_table(str(tmp_path / "calls"))
    # Each projection is a direct read: no intermediate annotation can mask bad
    # storage hints by forcing the whole source column to be loaded.
    for name in ["ploidy", "phased", "alleles", "n_alt_alleles", "is_haploid", "is_diploid",
                 "is_hom_ref", "is_non_ref", "is_het", "is_het_ref", "is_het_non_ref", "is_hom_var"]:
        def select(t):
            value = getattr(t.c, name)
            return t.select(value=value() if callable(value) else value).collect()
        assert select(read) == select(table), name

    mt = hl.range_matrix_table(2, 3)
    mt = mt.annotate_entries(GT=hl.call(mt.row_idx, mt.col_idx))
    mt.write(str(tmp_path / "matrix"))
    stored = hl.read_matrix_table(str(tmp_path / "matrix"))
    def result(matrix):
        return matrix.annotate_rows(n=hl.agg.count_where(matrix.GT.is_het())).rows().collect()
    assert result(stored) == result(mt)


def test_stored_nested_call_accessors_and_literal_accessor_field_names(tmp_path):
    schema = hl.tstruct(xs=hl.tarray(hl.tstruct(GT=hl.tcall)),
                        s=hl.tstruct(__vail_call_is_het=hl.tbool))
    table = hl.Table.parallelize([
        {"xs": [{"GT": hl.Call([0, 1])}, None, {"GT": None}],
         "s": {"__vail_call_is_het": True}},
    ], schema=schema)
    table.write(str(tmp_path / "nested"))
    read = hl.read_table(str(tmp_path / "nested"))
    assert read.select(values=read.xs.map(lambda item: item.GT.is_het()),
                       real=read.s["__vail_call_is_het"]).collect() == [
        hl.Struct(values=[True, None, None], real=True)]


@pytest.mark.parametrize("alleles", [[65536], [0,65536], [-1], [65536,0]])
def test_uint16_call_limit_rejects_overflow(alleles):
    with pytest.raises(ValueError,match="Call|call|65535|nonnegative"):
        hl.Call(alleles)
    table=hl.range_table(1)
    with pytest.raises((ValueError,RuntimeError),match="Call|call|65535"):
        table.select(c=hl.call(*(table.idx+a for a in alleles))).collect()


def test_call_storage_uses_actual_uint16_fields():
    import pyarrow as pa
    from vail.arrow import arrow_type
    fields=arrow_type(hl.tcall)
    assert [f.type for f in fields] == [pa.uint16(),pa.uint16(),pa.uint8(),pa.bool_()]
    calls=[hl.Call([65535]),hl.Call([65535,65535]),hl.Call([65535,0],True)]
    table=hl.Table.parallelize([{'c':c} for c in calls])
    assert table.collect()==[hl.Struct(c=c) for c in calls]
    assert table.to_arrow().schema.field('c').type == fields
    assert table.to_arrow().column('c').to_pylist() == [
        {'__vail_call_allele0':65535,'__vail_call_allele1':0,'__vail_call_ploidy':1,'__vail_call_phased':False},
        {'__vail_call_allele0':65535,'__vail_call_allele1':65535,'__vail_call_ploidy':2,'__vail_call_phased':False},
        {'__vail_call_allele0':65535,'__vail_call_allele1':0,'__vail_call_ploidy':2,'__vail_call_phased':True},
    ]


def test_direct_call_order_grouping_and_joins():
    calls = [hl.Call([1, 1]), hl.Call([0, 2]), hl.Call([0, 1]), hl.Call([]),
             hl.Call([0]), hl.Call([0, 0]), hl.Call([65535]), hl.Call([1, 0], True)]
    table = hl.Table.parallelize([{'c':c} for c in calls], schema=hl.tstruct(c=hl.tcall)).key_by('c')
    expected = sorted(calls, key=lambda c: ((c.alleles + [0, 0])[:2], c.ploidy, c.phased))
    assert [r.c for r in table.collect()] == expected
    assert table.join(table).count() == len(calls)
    grouped = table.group_by(c=table.c).aggregate(n=hl.agg.count()).collect()
    assert {r.c:r.n for r in grouped} == {c:1 for c in calls}


def test_legacy_packed_call_metadata_is_rejected():
    import pyarrow as pa
    from vail.arrow import TYPE_DESCRIPTOR, TYPE_VERSION
    field=pa.field('__vail_call_word',pa.int32(),metadata={TYPE_VERSION:b'1',TYPE_DESCRIPTOR:b'"call"'})
    legacy=pa.StructArray.from_arrays([pa.array([12],type=pa.int32())],fields=[field])
    with pytest.raises((ValueError,RuntimeError,TypeError),match='metadata|layout|Call|call'):
        hl.Table.parallelize(pa.table({'c':legacy})).collect()
