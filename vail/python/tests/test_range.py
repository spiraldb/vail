import pytest

import vail as hl


def test_zeros_uses_int32_and_preserves_missingness():
    assert hl.zeros(3).dtype == hl.tarray(hl.tint32)
    assert hl.eval(hl.zeros(3)) == [0, 0, 0]
    assert hl.eval(hl.zeros(0)) == []
    assert hl.eval(hl.zeros(hl.missing(hl.tint32))) is None
    table = hl.range_table(4)
    assert [row.z for row in table.annotate(z=hl.zeros(table.idx)).collect()] == [
        [], [0], [0, 0], [0, 0, 0],
    ]


def test_zeros_rejects_negative_length():
    with pytest.raises(RuntimeError, match="nonnegative"):
        hl.eval(hl.zeros(-1))


@pytest.mark.parametrize("length", [1.5, "3", True, hl.int64(3)])
def test_zeros_requires_int32(length):
    with pytest.raises(TypeError, match="int32"):
        hl.zeros(length)


@pytest.mark.parametrize('args, expected', [
    ((5,), [0, 1, 2, 3, 4]),
    ((3, 8), [3, 4, 5, 6, 7]),
    ((0, 10, 3), [0, 3, 6, 9]),
    ((5, -2, -2), [5, 3, 1, -1]),
    ((0,), []),
    ((-3,), []),
    ((5, 1), []),
    ((1, 5, -1), []),
    ((2**31 - 2, 2**31 - 1), [2**31 - 2]),
    ((-2**31 + 1, -2**31, -1), [-2**31 + 1]),
])
def test_range_values(args, expected):
    expression = hl.range(*args)
    assert expression.dtype == hl.tarray(hl.tint32)
    assert hl.eval(expression) == expected


@pytest.mark.parametrize('index', [0, 1, 2])
def test_range_missing_arguments(index):
    arguments = [0, 5, 1]
    arguments[index] = hl.missing(hl.tint32)
    assert hl.eval(hl.range(*arguments)) is None


def test_range_expression_arguments_and_array_composition():
    table = hl.range_table(4).annotate_globals(step=2)
    result = table.annotate(values=hl.range(table.idx, table.idx + 5, table.step).map(lambda x: x * 2))
    assert [row["values"] for row in result.collect()] == [
        [0, 4, 8], [2, 6, 10], [4, 8, 12], [6, 10, 14],
    ]


def test_ndarray_arange_uses_lazy_native_range():
    assert hl.eval(hl.nd.arange(4)).tolist() == [0, 1, 2, 3]
    source = hl.range_table(4)
    table = source.annotate(values=hl.nd.arange(source.idx))
    assert [row["values"].tolist() for row in table.collect()] == [[], [0], [0, 1], [0, 1, 2]]


def test_range_zero_step_errors_during_execution():
    expression = hl.range(0, 5, step=0)
    with pytest.raises(RuntimeError, match='step'):
        hl.eval(expression)


@pytest.mark.parametrize('value', [1.5, '5', hl.int64(5), True])
def test_range_requires_int32_arguments(value):
    with pytest.raises(TypeError, match='int32'):
        hl.range(value)


def test_ndarray_arange_preserves_missingness_and_range_errors():
    assert hl.eval(hl.nd.arange(hl.missing(hl.tint32))) is None
    assert hl.eval(hl.nd.arange(5, 0, -2)).tolist() == [5, 3, 1]
    with pytest.raises(RuntimeError, match='step'):
        hl.eval(hl.nd.arange(0, 4, 0))
