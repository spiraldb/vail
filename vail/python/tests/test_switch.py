import pytest
import vail as hl


def test_switch_values_and_missingness():
    x = hl.literal('1')
    assert hl.eval(hl.switch(x).when('123', 5).when('1', 6).when('0', 2).or_missing()) == 6
    assert hl.eval(hl.switch(x).when('123', 5).when('0', 2).or_missing()) is None
    assert hl.eval(hl.switch(x).when('123', 5).when('0', 2).default(100)) == 100
    na = hl.missing(hl.tint32)
    assert hl.eval(hl.switch(na).when(5, 0).when(na, 3).default(4)) is None
    assert hl.eval(hl.switch(na).when(5, 0).when(na, 3).when_missing(-1).default(4)) == -1
    assert hl.eval(hl.switch(na).when_missing(7).default(9)) == 7
    assert hl.eval(hl.switch(0).when_missing(7).default(9)) == 9
    assert hl.eval(hl.switch(0).default(9)) == 9
    assert hl.eval(hl.switch(1).when(1, 3).when(1, 4).default(5)) == 3
    assert hl.eval(hl.switch(0).when(hl.missing(hl.tint32), 3).when(0, 4).default(5)) is None


def test_switch_types_and_validation():
    expr = hl.switch(0).when(0, hl.int64(3)).default(hl.float32(4))
    assert expr.dtype == hl.tfloat32
    assert hl.eval(expr) == 3.0
    with pytest.raises(TypeError, match='cannot compare'):
        hl.switch(1).when('1', 3)
    with pytest.raises(TypeError, match='same type'):
        hl.switch(1).when(1, [1]).default([1.0])
    with pytest.raises(hl.ExpressionException, match='only be called once'):
        hl.switch(1).when_missing(0).when_missing(1)
    for finish in ('or_missing', 'or_error'):
        with pytest.raises(hl.ExpressionException, match='at least one'):
            getattr(hl.switch(1).when_missing(0), finish)(*(['oops'] if finish == 'or_error' else []))
    with pytest.raises(TypeError, match='str type'):
        hl.switch(1).when(1, 2).or_error(3)


def test_switch_lazy_errors_and_indexed_source():
    assert hl.eval(hl.switch(1).when(1, 9).or_error('unmatched')) == 9
    with pytest.raises(RuntimeError, match='unmatched'):
        hl.eval(hl.switch(2).when(1, 9).or_error('unmatched'))
    table = hl.range_table(3)
    table = table.annotate(value=hl.switch(table.idx).when(0, 'a').when(1, 'b').default('c'))
    assert [row.value for row in table.collect()] == ['a', 'b', 'c']
    other = hl.range_table(3)
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        hl.switch(table.idx).when(1, other.idx).default(0)
