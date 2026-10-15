"""Native operator policy and packed logical type regressions."""
import json
import pytest
import vail as hl
from vail.expressions import Expression


def test_packed_calls_roundtrip_order_and_lazy_construction(tmp_path):
    calls = [hl.Call((0, 2)), hl.Call((1, 1)), hl.Call((2, 0), True),
             hl.Call((7,)), hl.Call((), True), None]
    table = hl.Table.parallelize([{'c': c} for c in calls], schema=hl.tstruct(c=hl.tcall), n_partitions=2)
    assert table.collect() == [hl.Struct(c=c) for c in calls]
    ordered = table.key_by('c')
    assert [r.c for r in ordered.collect()] == [hl.Call((), True), hl.Call((1, 1)), hl.Call((0, 2)), hl.Call((2, 0), True), hl.Call((7,)), None]
    table.write(tmp_path / 'calls')
    loaded = hl.read_table(tmp_path / 'calls')
    assert loaded.c.dtype == hl.tcall
    assert loaded.collect() == table.collect()
    source = hl.range_table(4)
    constructed = source.annotate(c=hl.call(source.idx, 3, phased=False))
    assert [r.c for r in constructed.collect()] == [hl.Call((i, 3)) for i in range(4)]
    expanded = constructed.annotate(
        a=constructed.c.alleles,
        phase=constructed.c.phased,
    )
    assert [r.a for r in expanded.collect()] == [[i, 3] for i in range(4)]
    assert [r.phase for r in expanded.collect()] == [False] * 4
    with pytest.raises((ValueError, RuntimeError), match='Call|allele'):
        source.annotate(c=hl.call(source.idx - 1, 0)).collect()


@pytest.mark.parametrize('dtype,values', [
    (hl.tset(hl.tint32), [{1, 2}, {2}]),
    (hl.tdict(hl.tint32, hl.tint32), [{1: 2}, {2: 3}]),
    (hl.tarray(hl.tset(hl.tint32)), [[{1}], [{2}]]),
])
def test_unordered_types_reject_ordering_and_keys(dtype, values):
    table = hl.Table.parallelize([{'x': x} for x in values], schema=hl.tstruct(x=dtype))
    actions = [lambda: table.key_by('x'), lambda: table.order_by(table.x),
               lambda: table.group_by(x=table.x)]
    # Set < means proper subset in Hail; it is a collection operation, not
    # the total-order comparison prohibited by this policy.
    if not isinstance(dtype, hl.tset):
        actions.append(lambda: table.x < table.x)
    for action in actions:
        with pytest.raises(NotImplementedError, match='Set/Dictionary'):
            action()
    assert len(table.annotate(equal=table.x == table.x).collect()) == 2
    # The native entry point must reject the same key even without Python checks.
    with pytest.raises(NotImplementedError, match='Set/Dictionary'):
        table._native.key_by(['x'])


def test_locus_reference_parameter_rejected_and_roundtripped(tmp_path):
    a = hl.ReferenceGenome('native_type_policy_a', ['x'], {'x': 10})
    b = hl.ReferenceGenome('native_type_policy_b', ['x'], {'x': 10})
    left, right = hl.literal(hl.Locus('x', 1, a)), hl.literal(hl.Locus('x', 1, b))
    with pytest.raises(TypeError, match='comparison|incompatible'):
        left == right
    ta = hl.Table.parallelize([{'l': hl.Locus('x', 1, a)}], schema=hl.tstruct(l=hl.tlocus(a))).key_by('l')
    tb = hl.Table.parallelize([{'l': hl.Locus('x', 1, b)}], schema=hl.tstruct(l=hl.tlocus(b))).key_by('l')
    with pytest.raises((TypeError, ValueError), match='type|reference'):
        ta.join(tb)
    ta.write(tmp_path / 'loci')
    result = hl.read_table(tmp_path / 'loci')
    assert result.l.dtype == hl.tlocus(a)
    assert result.collect() == ta.collect()


def test_locus_arrow_metadata_cannot_change_reference_parameter():
    import pyarrow as pa
    from vail.arrow import arrow_type
    a = hl.ReferenceGenome('native_type_metadata_a', ['x'], {'x': 10})
    b = hl.ReferenceGenome('native_type_metadata_b', ['x'], {'x': 20})
    fields = list(arrow_type(hl.tlocus(a)))
    fields[1] = fields[1].with_metadata({'vail.reference': json.dumps(b.to_wire())})
    array = pa.array([{'__vail_locus_ordinal': 0, '__vail_locus_contig': 'x',
                       '__vail_locus_position': 1}], type=pa.struct(fields))
    with pytest.raises((TypeError, ValueError, RuntimeError), match='reference|metadata'):
        hl.Table.parallelize(pa.table({'l': array})).collect()


def test_native_join_keys_keep_all_matching_rows(tmp_path):
    table = hl.Table.parallelize([{'k': -0.0}, {'k': 0.0}],
                                schema=hl.tstruct(k=hl.tfloat64), n_partitions=2).key_by('k')
    assert table.join(table).count() == 4
    table.write(tmp_path / 'native-keys')
    stored = hl.read_table(tmp_path / 'native-keys')
    assert stored.join(stored).count() == 4


@pytest.mark.parametrize('dtype', [hl.tfloat32, hl.tfloat64])
def test_float_comparisons_and_grouping_use_datafusion_operators(dtype):
    table = hl.Table.parallelize([{'x': -0.0}, {'x': 0.0}, {'x': None}],
                                schema=hl.tstruct(x=dtype))
    expression = table.annotate(equal=table.x == table.x, greater=table.x > 0.0)
    plan = expression._native.physical_plan()
    assert 'NullableVailKey' not in plan
    assert 'vail_nullable_key' not in plan
    assert [r.equal for r in expression.collect()] == [True, True, None]
    grouped = table.group_by('x').aggregate(n=hl.agg.count())
    assert 'vail_key(' not in grouped._native.physical_plan()
    assert sorted(r.n for r in grouped.collect()) == [1, 2]


def test_packed_call_accessors_at_representation_boundaries():
    calls = [hl.Call((0, 32767)), hl.Call((16383, 16384), True),
             hl.Call(((1 << 29) - 1,), True), None]
    table = hl.Table.parallelize([{'c': c} for c in calls], schema=hl.tstruct(c=hl.tcall))
    rows = table.annotate(a=table.c.alleles,
                          phase=table.c.phased).collect()
    assert [r.a for r in rows] == [list(c.alleles) if c is not None else None for c in calls]
    assert [r.phase for r in rows] == [c.phased if c is not None else None for c in calls]


def test_array_keys_use_native_grouping_distinct_and_exact_table_comparison():
    schema = hl.tstruct(k=hl.tarray(hl.tint32), v=hl.tint32)
    table = hl.Table.parallelize(
        [dict(k=[1], v=2), dict(k=[1], v=3), dict(k=[1, 2], v=4), dict(k=None, v=5)],
        schema=schema,
        n_partitions=2,
    ).key_by('k')
    groups = table.group_by(k=table.k).aggregate(n=hl.agg.count()).collect()
    assert [(row.k, row.n) for row in groups] == [([1], 2), ([1, 2], 1), (None, 1)]
    assert table.distinct().count() == 3
    assert table._same(table)
    changed = table.annotate(v=table.v + 1)
    assert not table._same(changed)


def test_empty_struct_group_key_preserves_rows_and_missingness():
    table = hl.range_table(3).annotate(k=hl.struct())
    rows = table.group_by('k').aggregate(n=hl.agg.count()).collect()
    assert len(rows) == 1
    assert rows[0].k == hl.Struct()
    assert rows[0].n == 3

    table = table.annotate(k=hl.or_missing(table.idx != 0, table.k))
    rows = table.group_by('k').aggregate(n=hl.agg.count()).collect()
    assert [(row.k, row.n) for row in rows] == [(hl.Struct(), 2), (None, 1)]

    # Scoped aggregates use the physical aggregate path and collect nested
    # values into a Dict whose key itself is the zero-field struct.
    counts = table.aggregate(hl.agg.group_by(table.k, hl.agg.count()))
    assert counts == {hl.Struct(): 2, None: 1}

    arrays = table.aggregate(
        hl.agg.group_by(table.k, hl.agg.array_sum(hl.array([table.idx])))
    )
    assert arrays == {hl.Struct(): [3], None: [0]}
