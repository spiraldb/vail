import pytest

import vail as hl


def test_case_branch_order_missingness_and_default():
    t = hl.Table.parallelize([{'x': 6}, {'x': 12}, {'x': 5}, {'x': -1}, {'x': 2}, {'x': None}],
                             schema=hl.tstruct(x=hl.tint32), n_partitions=3)
    expression = (hl.case().when(t.x == 6, 'A').when(t.x % 3 == 0, 'B')
                  .when(t.x == 5, 'C').when(t.x < 2, 'D').or_missing())
    assert expression.collect() == ['A', 'B', 'C', 'D', None, None]
    assert hl.eval(hl.case().when(hl.missing(hl.tbool), 1).default(2)) is None
    assert hl.eval(hl.case(missing_false=True).when(hl.missing(hl.tbool), 1).default(2)) == 2
    assert hl.eval(hl.case().default('fallback')) == 'fallback'


def test_case_types_validation_and_global_references():
    t = hl.range_table(2).annotate_globals(g=9)
    expression = hl.case().when(t.idx == 0, t.g).default(hl.int64(3))
    assert expression.dtype == hl.tint64
    assert expression.collect() == [9, 3]
    builder = hl.case().when(True, 1)
    assert builder.when(False, 2) is builder
    with pytest.raises(TypeError, match='same type'):
        builder.default('bad')
    with pytest.raises(TypeError, match='bool'):
        hl.case().when(1, 3)
    with pytest.raises(TypeError, match='bool'):
        hl.case(missing_false=1)
    for finish in (lambda: hl.case().or_missing(), lambda: hl.case().or_error('bad')):
        with pytest.raises(hl.ExpressionException, match="at least one 'when'"):
            finish()
    other = hl.range_table(2)
    with pytest.raises(hl.ExpressionException, match='source'):
        hl.case().when(t.idx == 0, 3).default(other.idx)


def test_case_short_circuits_value_and_error_branches():
    t = hl.Table.parallelize([{'p': True, 's': 'invalid'}, {'p': False, 's': '4'}])
    assert hl.case().when(t.p, 7).default(hl.int32(t.s)).collect() == [7, 4]
    assert hl.case().when(t.p, 7).when(~t.p, 4).or_error(t.s).collect() == [7, 4]
    assert hl.case().when(hl.missing(hl.tbool), 1).or_error('unused').collect() == [None]
    with pytest.raises(RuntimeError, match='failure message'):
        hl.eval(hl.case().when(False, hl.struct(x=1)).or_error('failure message'))
    with pytest.raises(RuntimeError, match='invalid'):
        hl.case().when(~t.p, 7).or_error(t.s).collect()
    assert hl.case().when(t.head(0).p, 7).or_error('unused').collect() == []


def test_case_matches_hail_numeric_branch_promotion_and_rejects_nested_coercion():
    expression = hl.case().when(True, hl.float32(2)).default(hl.int64(3))
    assert expression.dtype == hl.tfloat32
    assert hl.eval(expression) == 2.
    assert hl.eval(hl.case().when(True, True).default(3)) == 1
    with pytest.raises(TypeError, match='same type'):
        hl.case().when(True, hl.struct(x=1)).default(hl.struct(x=hl.int64(2)))


def test_missing_false_predicates_preserve_seeded_context_and_lazy_values():
    table=hl.range_table(64,n_partitions=3).annotate_globals(fallback=3)
    predicate=hl.rand_bool(.5,seed=99)
    actual=hl.if_else(predicate,True,False,missing_false=True)
    rows=table.select(actual=actual,expected=predicate).collect()
    assert [row.actual for row in rows]==[row.expected for row in rows]
    value=(hl.case(missing_false=True).when(hl.missing(hl.tbool),hl.int32('invalid'))
           .when(table.idx>=0,table.fallback).or_error('unused'))
    assert value.collect()==[3]*64
