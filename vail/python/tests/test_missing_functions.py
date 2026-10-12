import pytest

import vail as hl


def test_missing_predicates_preserve_container_validity_and_nan_values():
    t = hl.Table.parallelize([
        {'p': True, 'x': {'a': None}, 'a': [], 'f': float('nan')},
        {'p': False, 'x': None, 'a': None, 'f': None},
        {'p': None, 'x': {'a': 3}, 'a': [None], 'f': 0.},
    ], schema=hl.tstruct(p=hl.tbool, x=hl.tstruct(a=hl.tint32), a=hl.tarray(hl.tint32), f=hl.tfloat64),
        n_partitions=2)
    rows = t.select(x=hl.or_missing(t.p, t.x), d=hl.is_defined(t.x),
                    a=hl.is_defined(t.a), f=hl.is_defined(t.f)).collect()
    assert rows == [hl.Struct(x=hl.Struct(a=None), d=True, a=True, f=True),
                    hl.Struct(x=None, d=False, a=False, f=False),
                    hl.Struct(x=None, d=True, a=True, f=True)]
    assert hl.eval(hl.is_defined(hl.missing(hl.tbool))) is False
    assert hl.eval(hl.is_defined(False)) is True
    assert hl.eval(hl.or_missing(hl.missing(hl.tbool), 3)) is None


def test_or_missing_globals_aggregations_and_source_validation():
    t = hl.range_table(3).annotate_globals(g=7)
    assert hl.eval(hl.or_missing(True, t.g)) == 7
    assert t.aggregate(hl.agg.collect(hl.or_missing(t.idx > 0, t.idx))) == [None, 1, 2]
    assert t.aggregate(hl.agg.count_where(hl.is_defined(t.idx))) == 3
    with pytest.raises(TypeError, match='bool'):
        hl.or_missing(1, t.idx)
    other = hl.range_table(2)
    with pytest.raises(hl.ExpressionException, match='source'):
        hl.or_missing(other.idx > 0, t.idx)


def test_or_missing_does_not_evaluate_unused_value_branch():
    t = hl.Table.parallelize([{'p': False, 's': 'not an integer'}, {'p': True, 's': '4'}])
    assert hl.or_missing(t.p, hl.int32(t.s)).collect() == [None, 4]
