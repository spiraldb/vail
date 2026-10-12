import pytest
import vail as hl


@pytest.mark.parametrize('element_type', [
    hl.tint32, hl.tstr, hl.tstruct(), hl.tstruct(x=hl.tint32),
    hl.tarray(hl.tint64), hl.ttuple(hl.tint32, hl.tstr),
])
def test_empty_array_is_defined_and_typed(element_type):
    expression = hl.empty_array(element_type)
    assert expression.dtype == hl.tarray(element_type)
    assert hl.eval(expression) == []
    assert hl.eval(hl.is_defined(expression)) is True
    assert hl.eval(hl.len(expression)) == 0
    assert hl.eval(expression.map(lambda x: x)) == []
    table = hl.range_table(2).annotate(values=expression)
    assert [row['values'] for row in table.collect()] == [[], []]


def test_empty_array_accepts_type_string_and_composes():
    expression = hl.empty_array('int32')
    assert expression.dtype == hl.tarray(hl.tint32)
    assert hl.eval(expression.append(3)) == [3]


def test_empty_array_rejects_invalid_element_type():
    with pytest.raises(TypeError):
        hl.empty_array(123)
