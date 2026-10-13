"""Public expression namespace and dataset validation contracts."""
import pytest
import vail as hl
from vail.expressions import Expression, ExpressionException


def test_public_expression_names_resolve_to_the_actual_classes():
    from vail.expr.expressions import Expression as NestedExpression
    from vail.expr.expressions import ExpressionException as NestedException
    from vail.expr.expressions.typed_expressions import Float32Expression, Float64Expression
    assert hl.expr.Expression is NestedExpression is Expression
    assert hl.expr.ExpressionException is NestedException is ExpressionException
    assert type(hl.float32(1)) is hl.expr.Float32Expression is Float32Expression
    assert type(hl.float64(1)) is hl.expr.Float64Expression is Float64Expression


def test_matrix_indexing_without_keys_uses_expression_exception():
    matrix=hl.range_matrix_table(3,2)
    with pytest.raises(hl.expr.ExpressionException,match='MatrixTable row key: *<<<empty key>>>'):
        matrix.key_rows_by().index_rows(matrix.row_idx)
    with pytest.raises(hl.expr.ExpressionException,match='MatrixTable col key: *<<<empty key>>>'):
        matrix.key_cols_by().index_cols(matrix.col_idx)


def test_scope_and_source_failures_are_catchable_through_public_namespace():
    matrix=hl.range_matrix_table(3,2)
    other=hl.range_matrix_table(3,2)
    with pytest.raises(hl.expr.ExpressionException,match='scope violation'):
        matrix.aggregate_cols(hl.agg.sum(matrix.row_idx))
    with pytest.raises(hl.expr.ExpressionException,match='scope violation'):
        matrix.annotate_rows(value=matrix.col_idx)
    with pytest.raises(hl.expr.ExpressionException,match='Found fields from 2 objects:'):
        matrix.annotate_entries(value=matrix.row_idx*other.col_idx)
    with pytest.raises(hl.expr.ExpressionException,match='source mismatch'):
        matrix.annotate_rows(value=other.row_idx)
