import pytest

import vail as hl


@pytest.mark.parametrize(
    "expression,field",
    [
        (lambda m: hl.array([1, 2, 3])[: m.col_idx], "col_idx"),
        (lambda m: hl.array([1, 2, 3])[m.col_idx :], "col_idx"),
        (lambda m: hl.array([1, 2, 3])[:: m.col_idx], "col_idx"),
        (lambda m: m.b @ m.a, "a"),
        (lambda m: m.b[m.col_idx], "col_idx"),
        (lambda m: m.b[m.col_idx, :], "col_idx"),
        (lambda m: m.b[:, m.col_idx], "col_idx"),
        (lambda m: m.b[m.col_idx, None], "col_idx"),
        (lambda m: m.b[None, m.col_idx], "col_idx"),
        (lambda m: m.b.reshape((m.col_idx, 1)), "col_idx"),
        (lambda m: m.b.reshape((1, m.col_idx)), "col_idx"),
        (lambda m: m.b.reshape(m.shape), "shape"),
    ],
)
def test_matrix_scope_errors_name_invalid_ndarray_indices(expression, field):
    m = hl.range_matrix_table(1, 1)
    m = m.annotate_cols(a=hl.nd.array([0]), shape=hl.tuple((1, 1)))
    m = m.annotate_rows(b=hl.nd.array([[0]]))
    with pytest.raises(
        hl.ExpressionException,
        match=rf"'{field}' \(indices \['column'\]\)",
    ):
        m.annotate_rows(c=expression(m))


def test_unsupported_ndarray_operations_do_not_create_frontend_only_ir():
    value = hl.nd.array([1, 2, 3])
    with pytest.raises(NotImplementedError, match="ndarray indexing"):
        _ = value[0]
    with pytest.raises(NotImplementedError, match="ndarray reshape"):
        value.reshape((3, 1))
    with pytest.raises(NotImplementedError, match="ndarray matrix multiplication"):
        _ = value @ value
