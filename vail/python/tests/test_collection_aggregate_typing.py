"""Reducer arguments retain their native input type independently of results."""
import pytest
import vail as hl


@pytest.mark.parametrize('reducer,result_type,expected', [
    (lambda x: hl.agg.sum(x), hl.tint64, 6),
    (lambda x: hl.agg.collect(x), hl.tarray(hl.tint32), [1, 2, 3]),
    (lambda x: hl.agg.collect_as_set(x), hl.tset(hl.tint32), {1, 2, 3}),
    (lambda x: hl.agg.mean(x), hl.tfloat64, 2.0),
])
def test_collection_reducer_public_type_and_values(reducer, result_type, expected):
    expression = hl.array([1, 2, 3]).aggregate(reducer)
    assert expression.dtype == result_type
    assert hl.eval(expression) == expected


def test_collection_stats_type_and_nested_use():
    expression = hl.array([1, 2, 3]).aggregate(lambda x: hl.agg.stats(x))
    assert expression.dtype == hl.tstruct(
        mean=hl.tfloat64, stdev=hl.tfloat64, min=hl.tfloat64,
        max=hl.tfloat64, n=hl.tint64, sum=hl.tfloat64)
    result = hl.eval(expression)
    assert result.mean == 2.0
    assert result.stdev == pytest.approx((2 / 3) ** 0.5)
    assert (result.min, result.max, result.n, result.sum) == (1.0, 3.0, 3, 6.0)
    nested = hl.array([1, 2, 3]).aggregate(lambda x: hl.agg.collect(x)).map(
        lambda x: hl.array([x, x + 1]).aggregate(lambda y: hl.agg.sum(y)))
    assert nested.dtype == hl.tarray(hl.tint64)
    assert hl.eval(nested) == [3, 5, 7]


def test_collection_reducer_input_inference_retains_outer_bindings():
    expression = hl.bind(lambda offset: hl.range(4).aggregate(
        lambda x: hl.agg.collect(x + offset)), 7)
    assert expression.dtype == hl.tarray(hl.tint32)
    assert hl.eval(expression) == [7, 8, 9, 10]


def test_public_collection_unsupported_reducer_reports_capability():
    with pytest.raises(NotImplementedError, match="per-list hist reducer"):
        hl.array([1.0, 2.0]).aggregate(lambda x: hl.agg.hist(x, 0, 4, 2))
