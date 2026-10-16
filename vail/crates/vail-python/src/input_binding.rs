//! Validate and bind scalar input DAGs without expanding shared subexpressions.
use crate::value_error;
use pyo3::PyResult;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use vail_execution::expressions::Expr;

pub(crate) fn global_names(root: &Expr) -> PyResult<Vec<String>> {
    fn visit(
        node: &Expr,
        seen: &mut HashSet<*const Expr>,
        names: &mut BTreeSet<String>,
    ) -> PyResult<()> {
        if !seen.insert(node as *const Expr) {
            return Ok(());
        }
        match node {
            Expr::Aggregate { .. }
            | Expr::AggregateFold { .. }
            | Expr::AggFilter { .. }
            | Expr::AggExplode { .. }
            | Expr::AggGroupBy { .. }
            | Expr::AggArray { .. } => {
                return Err(value_error(
                    "parallelize requires an evaluated or lazy scalar aggregate result",
                ));
            }
            Expr::Column { .. } => {
                return Err(value_error("parallelize requires unindexed expressions"));
            }
            Expr::Global { name } => {
                names.insert(name.clone());
            }
            _ => {}
        }
        let mut error = None;
        node.visit_children(&mut |child| {
            if error.is_none() {
                error = visit(child, seen, names).err();
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        Ok(())
    }
    let mut names = BTreeSet::new();
    visit(root, &mut HashSet::new(), &mut names)?;
    Ok(names.into_iter().collect())
}

pub(crate) fn bind(root: &Arc<Expr>, globals: &HashMap<String, Arc<Expr>>) -> PyResult<Arc<Expr>> {
    if globals.is_empty() {
        return Ok(root.clone());
    }
    fn rewrite(
        node: &Expr,
        globals: &HashMap<String, Arc<Expr>>,
        memo: &mut HashMap<*const Expr, Expr>,
    ) -> PyResult<Expr> {
        let key = node as *const Expr;
        if let Some(value) = memo.get(&key) {
            return Ok(value.clone());
        }
        let bound = match node {
            Expr::Global { name } => globals
                .get(name)
                .ok_or_else(|| value_error("unbound input global field"))?
                .as_ref()
                .clone(),
            _ => node.try_map_original_children(|child| rewrite(child, globals, memo))?,
        };
        memo.insert(key, bound.clone());
        Ok(bound)
    }
    Ok(Arc::new(rewrite(root, globals, &mut HashMap::new())?))
}
