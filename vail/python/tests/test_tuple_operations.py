import json
import pytest
import vail as hl


def test_tuple_construction_from_expressions_and_iterables():
    expr = hl.tuple([hl.int32(1), hl.str(2), hl.array([3, 4])])
    assert expr.dtype == hl.ttuple(hl.tint32, hl.tstr, hl.tarray(hl.tint32))
    assert expr.to_ir()['op'] == 'tuple'
    assert hl.eval(expr) == (1, '2', [3, 4])
    assert hl.eval(hl.tuple(x for x in [1, 'a'])) == (1, 'a')
    assert hl.eval(hl.as_expr((hl.int32(2), hl.str(3)))) == (2, '3')
    assert hl.eval(hl.tuple(expr)) == (1, '2', [3, 4])


@pytest.mark.parametrize('selection, expected', [
    (slice(None), (1, 'a', 2.5)), (slice(None, None, -1), (2.5, 'a', 1)),
    (slice(1, None), ('a', 2.5)), (slice(-2, -1), ('a',)),
    (slice(None, None, 2), (1, 2.5)), (slice(50, 60), ()),
    (slice(None, -50), ()), (slice(2, 0, -1), (2.5, 'a')),
])
def test_tuple_slices(selection, expected):
    expr = hl.tuple([1, 'a', 2.5])
    assert hl.eval(expr[selection]) == expected


def test_missing_tuple_slicing_preserves_parent_missingness():
    dtype = hl.ttuple(hl.tint32, hl.tstr)
    absent = hl.missing(dtype)
    present = hl.tuple([hl.missing(hl.tint32), hl.missing(hl.tstr)])
    assert hl.eval(absent[:]) is None
    assert hl.eval(absent[::-1]) is None
    assert hl.eval(absent[0:0]) is None
    assert hl.eval(present[:]) == (None, None)
    assert hl.eval(present[0:0]) == ()
    assert hl.eval(absent[0]) is None


def test_tuple_sequence_protocol():
    value = hl.tuple([1, 'a'])
    a, b = value
    assert hl.eval(a) == 1
    assert hl.eval(b) == 'a'
    assert len(value) == 2
    assert bool(value) is True
    assert len(hl.missing(value.dtype)) == 2
    assert bool(hl.missing(value.dtype)) is True
    assert hl.eval(hl.tuple([])) == ()
    assert len(hl.tuple([])) == 0
    assert bool(hl.tuple([])) is False
    assert list(hl.tuple([])) == []


def test_tuple_index_and_slice_errors_match_hail():
    value = hl.tuple([1, 2])
    for index in (-1, 2, 100):
        with pytest.raises(IndexError):
            value[index]
    with pytest.raises(TypeError, match='integer'):
        value[hl.int32(0)]
    with pytest.raises(ValueError, match='zero'):
        value[::0]
    with pytest.raises(TypeError):
        value[:hl.int32(1)]


def test_tuple_lazy_table_globals_lookup_and_roundtrip(tmp_path):
    target = hl.Table.parallelize([{'k': 1, 'v': 10}], key='k')
    source = hl.utils.range_table(3, n_partitions=2).annotate_globals(label='g')
    result = source.annotate(value=hl.tuple([source.idx, target[source.idx].v, source.label]))
    result = result.annotate(reverse=result.value[::-1])
    assert result.n_partitions() == 2
    assert [r.value for r in result.collect()] == [(0, None, 'g'), (1, 10, 'g'), (2, None, 'g')]
    assert [r.reverse for r in result.collect()] == [('g', None, 0), ('g', 10, 1), ('g', None, 2)]
    result.write(str(tmp_path / 'tuples'))
    restored = hl.read_table(str(tmp_path / 'tuples'))
    assert restored.value.dtype == result.value.dtype
    assert restored._same(result)


def test_tuple_in_lambdas_grouping_and_aggregations():
    expr = hl.array([1, 2]).map(lambda x: hl.tuple([x, x + 1])[::-1])
    assert hl.eval(expr) == [(2, 1), (3, 2)]
    source = hl.utils.range_table(3)
    assert source.aggregate(hl.agg.collect(hl.tuple([source.idx, source.idx + 1]))) == [(0, 1), (1, 2), (2, 3)]
    keyed = source.key_by(k=hl.tuple([source.idx % 2]))
    assert keyed.group_by(keyed.k).aggregate(n=hl.agg.count()).count() == 2


def test_tuple_constructor_rejects_mixed_sources():
    a, b = hl.utils.range_table(1), hl.utils.range_table(1)
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        hl.tuple([a.idx, b.idx])


def test_empty_tuple_in_table(tmp_path):
    table = hl.utils.range_table(2)
    result = table.annotate(empty=hl.tuple([]), empty_struct=hl.struct())
    assert [row.empty for row in result.collect()] == [(), ()]
    assert result.empty.dtype == hl.ttuple()
    assert result.empty_struct.dtype == hl.tstruct()
    assert [row.empty_struct for row in result.collect()] == [hl.Struct(), hl.Struct()]

    result.write(str(tmp_path / 'empty-tuples'))
    restored = hl.read_table(str(tmp_path / 'empty-tuples'))
    assert restored.empty.dtype == hl.ttuple()
    assert restored.empty_struct.dtype == hl.tstruct()
    assert [row.empty for row in restored.collect()] == [(), ()]
    assert restored._same(result)


def test_empty_tuple_literals_globals_and_keys():
    assert hl.eval(hl.literal(())) == ()
    table = hl.Table.parallelize([{'k': (), 'v': 1}, {'k': (), 'v': 2}],
                                schema=hl.tstruct(k=hl.ttuple(), v=hl.tint32), key='k')
    assert [row.k for row in table.collect()] == [(), ()]
    assert table.group_by(table.k).aggregate(n=hl.agg.count()).collect()[0].n == 2
    table = table.annotate_globals(e=hl.tuple([]))
    assert table.e.dtype == hl.ttuple()
    assert hl.eval(table.e) == ()
    assert hl.eval(hl.missing(hl.ttuple())[:]) is None
