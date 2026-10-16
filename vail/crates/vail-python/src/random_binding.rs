//! Python boundary adapters for the single execution-owned scoped DAG binder.
use pyo3::PyResult;
use std::sync::Arc;
use vail_execution::expressions::Expr;

pub(crate) fn rows_referenced(root: &Expr) -> bool {
    let mut rows = false;
    root.visit_unique(&mut |node| {
        rows |= matches!(node, Expr::Column { .. })
            || matches!(node, Expr::MatrixField { axis, .. } | Expr::MatrixScope { axis, .. } if axis != "global");
    });
    rows
}
pub(crate) fn bind_rows(root: &Expr, row: &Expr, mode: &str) -> PyResult<Arc<Expr>> {
    vail_execution::randomness_binding::bind_rows(root, Arc::new(row.clone()), mode)
        .map_err(crate::value_error)
}
pub(crate) fn context(root: &Expr, uid: &[Arc<Expr>]) -> PyResult<Arc<Expr>> {
    vail_execution::randomness_binding::bind_context(root, uid, None).map_err(crate::value_error)
}
pub(crate) fn replace_parameter(root: &Expr, name: &str, value: &Expr) -> PyResult<Arc<Expr>> {
    let value = Arc::new(value.clone());
    vail_execution::randomness_binding::bind_context(root, &[], Some((name, &value)))
        .map_err(crate::value_error)
}
