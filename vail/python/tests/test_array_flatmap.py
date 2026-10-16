"""One-level flattening with Hail's missing-array semantics."""

import pytest
import vail as hl


def test_flatmap_missing_empty_and_missing_elements():
    t = hl.Table.parallelize([{'a': None}, {'a': []}, {'a': [1, None, 3]}],
                             schema=hl.tstruct(a=hl.tarray(hl.tint32)))
    assert t.a.flatmap(lambda x: [x, x + 1]).collect() == [None, [], [1, 2, None, None, 3, 4]]
    assert t.a.flatmap(lambda x: hl.if_else(hl.is_missing(x),
        hl.missing(hl.tarray(hl.tint32)), [x])).collect() == [None, [], [1, 3]]
    assert t.a.flatmap(lambda x: hl.missing(hl.tarray(hl.tint32))).collect() == [None, [], []]


def test_flatmap_flattens_exactly_one_level():
    expr = hl.array([1, 2]).flatmap(lambda x: [[x, x + 1], [x + 2]])
    assert expr.dtype == hl.tarray(hl.tarray(hl.tint32))
    assert hl.eval(expr) == [[1, 2], [3], [2, 3], [4]]
    assert hl.eval(hl.array([1]).flatmap(lambda x: [hl.struct(x=x), hl.struct(x=x+1)])) == [
        hl.Struct(x=1), hl.Struct(x=2)]


def test_nested_captures_and_globals():
    external = hl.range_table(0).annotate_globals(offset=10).index_globals()
    t = hl.range_table(3).annotate_globals(g=100)
    result = t.annotate(a=hl.array([1, 2]).flatmap(lambda x:
        hl.array([3, 4]).map(lambda y: x + y + t.idx + t.g + external.offset)))
    assert result.a.collect() == [[114+i, 115+i, 115+i, 116+i] for i in range(3)]
    g = t.annotate_globals(xs=hl.array([1, 2]).flatmap(lambda x: [x, x + t.g]))
    assert hl.eval(g.xs) == [1, 101, 2, 102]
    nested = hl.array([1, 2]).map(lambda x: hl.array([3, 4]).flatmap(lambda y: [x, y]))
    assert hl.eval(nested) == [[1, 3, 1, 4], [2, 3, 2, 4]]


def test_flatmap_lazy_body_and_replay():
    expr = hl.array([1,2]).flatmap(lambda x:[x])
    assert hl.eval(expr) == [1,2]
    assert hl.eval(expr) == [1,2]
    for array in [hl.literal([], hl.tarray(hl.tint32)), hl.missing(hl.tarray(hl.tint32))]:
        assert hl.eval(array.flatmap(lambda x: [hl.array([1])[10]])) == hl.eval(array)
    t = hl.range_table(1).annotate(a=hl.array([1]).flatmap(lambda x: [hl.array([1])[10]]))
    assert t.head(0).collect() == []
    with pytest.raises(RuntimeError, match='out of bounds'):
        t.collect()


def test_flatmap_type_and_source_validation():
    with pytest.raises(TypeError, match='callable'):
        hl.array([1]).flatmap(3)
    with pytest.raises(TypeError, match='return an array'):
        hl.array([1]).flatmap(lambda x: x)
    a, b = hl.range_table(1), hl.range_table(1)
    with pytest.raises(ValueError):
        hl.array([a.idx]).flatmap(lambda x: [x + b.idx])


def test_variable_lengths_slicing_and_storage(tmp_path):
    t = hl.Table.parallelize([{'a': [[1, None], None, [], [2]]}, {'a': None}, {'a': []},
                             {'a': [[3], [4, 5]]}], schema=hl.tstruct(a=hl.tarray(hl.tarray(hl.tint32))))
    result = t.annotate(b=t.a[::-1].flatmap(lambda x: x))
    expected = [[2, 1, None], None, [], [4, 5, 3]]
    assert result.b.collect() == expected
    path = str(tmp_path / 'flatmap')
    result.write(path)
    read = hl.read_table(path)
    assert read.b.collect() == expected
    assert read.a[::-1].flatmap(lambda x: x).collect() == expected
