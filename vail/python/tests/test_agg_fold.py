import pytest

from vail.agg_fold import fold
from vail.expressions import Expression, literal
from vail.types import tint32, tint64


def test_fold_builds_typed_sequence_and_combine_lambdas():
    result = fold(literal(0, tint32), lambda state: state + 1,
                  lambda left, right: left + right)
    ir = result.to_ir()
    assert result.dtype == tint32
    assert ir["op"] == "aggregate_fold"
    assert ir["seq"]["left"]["name"] == ir["state_parameter"]
    assert ir["combine"]["left"]["name"] == ir["left_parameter"]
    assert ir["combine"]["right"]["name"] == ir["right_parameter"]


def test_fold_rejects_unbound_initial_and_mismatched_result_types():
    with pytest.raises(TypeError, match="cannot depend on rows"):
        fold(Expression(tint32, None, {"op": "lambda_variable", "name": "initial"}),
             lambda state: state, lambda left, right: left)
    with pytest.raises(TypeError, match="incompatible types"):
        fold(literal(0, tint32), lambda state: literal("wrong"), lambda left, right: left)


def test_fold_requires_callable_operations():
    with pytest.raises(TypeError, match="must be callable"):
        fold(literal(0, tint32), None, None)


def test_fold_promotes_numeric_state():
    import vail as hl
    table = hl.range_table(3)
    expression = fold(0, lambda state: state + hl.int64(table.idx), lambda left, right: left + right)
    assert expression.dtype == tint64
    assert table.aggregate(expression) == 3
