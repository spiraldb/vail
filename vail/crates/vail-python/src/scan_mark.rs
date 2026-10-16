//! Mark aggregate nodes without expanding shared scalar argument DAGs.
use std::{collections::HashMap, sync::Arc};
use vail_execution::expressions::Expr;

pub(crate) fn mark(root: &Expr) -> Option<Expr> {
    fn visit(node: &Expr, memo: &mut HashMap<*const Expr, (Expr, bool)>) -> (Expr, bool) {
        let key = node as *const Expr;
        if let Some(value) = memo.get(&key) {
            return value.clone();
        }
        // A wrapped fold already contains the scan semantics for its callbacks.
        let result = if matches!(node, Expr::Aggregate { function, .. } if function == "__scan_fold")
        {
            (node.clone(), true)
        } else {
            let mut found = false;
            let rewritten = node
                .try_map_original_children(|child| {
                    let (value, child_found) = visit(child, memo);
                    found |= child_found;
                    Ok::<_, std::convert::Infallible>(value)
                })
                .unwrap();
            match rewritten {
                Expr::AggregateFold { .. } => (
                    Expr::Aggregate {
                        function: "__scan_fold".into(),
                        value: Some(Arc::new(rewritten)),
                        options: serde_json::json!({"__vail_scan": true}),
                        result_type: None,
                        init: Vec::new(),
                    },
                    true,
                ),
                Expr::Aggregate {
                    function,
                    value,
                    mut options,
                    result_type,
                    init,
                } => {
                    options["__vail_scan"] = serde_json::Value::Bool(true);
                    (
                        Expr::Aggregate {
                            function,
                            value,
                            options,
                            result_type,
                            init,
                        },
                        true,
                    )
                }
                value => (value, found),
            }
        };
        memo.insert(key, result.clone());
        result
    }
    let (result, found) = visit(root, &mut HashMap::new());
    found.then_some(result)
}
