//! Conservative nested source-column dependency analysis for MapRows.
//!
//! The only array projection recognized here is a direct `ArrayMap` over a
//! source column whose element is accessed through `GetField` paths. Any
//! opaque or whole-container use marks the source column as fully required.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use vail_execution::expressions::Expr;

#[derive(Clone)]
struct ArrayRoot {
    source: String,
}

#[derive(Default)]
struct Dependencies {
    paths: BTreeMap<String, BTreeSet<Vec<String>>>,
    full: BTreeSet<String>,
}

/// Return full-name paths for safely narrowed top-level source fields.
/// A path includes its row field, for example `[_CELLS, value, GT]`.
pub(crate) fn map_rows_paths(fields: &[(String, Expr)]) -> Vec<Vec<String>> {
    let mut deps = Dependencies::default();
    for (_, expression) in fields {
        walk(expression, &HashMap::new(), &mut deps);
    }
    deps.paths
        .into_iter()
        .filter(|(root, _)| !deps.full.contains(root))
        .flat_map(|(_, paths)| paths)
        .collect()
}

fn path_root<'a>(
    expression: &'a Expr,
    arrays: &HashMap<String, ArrayRoot>,
) -> Option<(String, Vec<String>)> {
    match expression {
        Expr::Column { name } => Some((name.clone(), Vec::new())),
        Expr::LambdaVariable { name } => arrays
            .get(name)
            .map(|root| (root.source.clone(), Vec::new())),
        Expr::GetField { value, name } => {
            let (root, mut path) = path_root(value, arrays)?;
            path.push(name.clone());
            Some((root, path))
        }
        _ => None,
    }
}

fn walk(expression: &Expr, arrays: &HashMap<String, ArrayRoot>, deps: &mut Dependencies) {
    // A GetField chain rooted in an input column or ArrayMap parameter is a
    // narrow dependency. Treat the chain as one node and do not visit its
    // children as whole-column uses.
    if let Expr::GetField { .. } = expression
        && let Some((root, path)) = path_root(expression, arrays)
    {
        if path.is_empty() {
            deps.full.insert(root);
        } else {
            let mut full_path = vec![root.clone()];
            full_path.extend(path);
            deps.paths.entry(root).or_default().insert(full_path);
        }
        return;
    }

    match expression {
        Expr::MatrixScope { .. }
        | Expr::MatrixField { .. }
        | Expr::MatrixNumCols { .. }
        | Expr::MatrixColumnPosition { .. }
        | Expr::TableLookup { .. } => walk_all_columns(expression, arrays, deps),

        Expr::Column { name } => {
            deps.full.insert(name.clone());
        }
        Expr::LambdaVariable { name } => {
            if let Some(root) = arrays.get(name) {
                deps.full.insert(root.source.clone());
            }
        }
        Expr::ArrayMap {
            value,
            parameter,
            body,
        } => {
            if let Expr::Column { name } = value.as_ref() {
                let mut nested = arrays.clone();
                nested.insert(
                    parameter.clone(),
                    ArrayRoot {
                        source: name.clone(),
                    },
                );
                walk(body, &nested, deps);
            } else {
                // Nested/list-derived arrays need a richer path model. Mark
                // every referenced source field full to avoid unsafe pruning.
                walk_all_columns(expression, arrays, deps);
            }
        }
        Expr::Struct { fields } => {
            for (_, value) in fields {
                walk(value, arrays, deps);
            }
        }
        Expr::Tuple { elements }
        | Expr::StringFunction { args: elements, .. }
        | Expr::NestedFunction { args: elements, .. }
        | Expr::Array { elements, .. } => {
            for value in elements {
                walk(value, arrays, deps);
            }
        }
        Expr::Binary { left, right, .. } | Expr::IntegerBinary { left, right, .. } => {
            walk(left, arrays, deps);
            walk(right, arrays, deps);
        }
        Expr::IfElse {
            condition,
            consequent,
            alternate,
        } => {
            walk(condition, arrays, deps);
            walk(consequent, arrays, deps);
            walk(alternate, arrays, deps);
        }
        Expr::Unary { value, .. }
        | Expr::Cast { value, .. }
        | Expr::CollectionCast { value, .. }
        | Expr::ToString { value }
        | Expr::Die { message: value, .. }
        | Expr::IsMissing { value }
        | Expr::ArrayFlatten { value }
        | Expr::ArrayLength { value }
        | Expr::ArrayReduce { value, .. }
        | Expr::LocusOp { value, .. }
        | Expr::ParseLocus { value, .. }
        | Expr::ParseCsvLine { value, .. } => walk(value, arrays, deps),
        Expr::Interval {
            start,
            end,
            includes_start,
            includes_end,
        } => {
            for value in [start, end, includes_start, includes_end] {
                walk(value, arrays, deps);
            }
        }
        Expr::IntervalContains { interval, point } => {
            walk(interval, arrays, deps);
            walk(point, arrays, deps);
        }
        Expr::IntervalOverlaps { left, right } => {
            walk(left, arrays, deps);
            walk(right, arrays, deps);
        }
        Expr::ArrayIndex { value, index } => {
            walk(value, arrays, deps);
            walk(index, arrays, deps);
        }
        Expr::Locus {
            contig, position, ..
        }
        | Expr::LocusValid {
            contig, position, ..
        } => {
            walk(contig, arrays, deps);
            walk(position, arrays, deps);
        }
        Expr::LocusFromGlobalPosition { position, .. } => walk(position, arrays, deps),
        Expr::ArraySlice {
            value,
            start,
            stop,
            step,
        } => {
            walk(value, arrays, deps);
            for value in [start, stop, step].into_iter().flatten() {
                walk(value, arrays, deps);
            }
        }
        // These binders and collection operators are not yet modeled with
        // scoped nested paths. Full-column fallback keeps their semantics.
        Expr::Aggregate { .. }
        | Expr::AggregateFold { .. }
        | Expr::AggGroupBy { .. }
        | Expr::AggArray { .. }
        | Expr::AggFilter { .. }
        | Expr::AggExplode { .. }
        | Expr::ArrayFold { .. }
        | Expr::ArrayPredicate { .. }
        | Expr::ArrayFilter { .. }
        | Expr::StreamRange { .. }
        | Expr::ArrayGrouped { .. } => walk_all_columns(expression, arrays, deps),
        Expr::ExternalGlobal { .. }
        | Expr::Global { .. }
        | Expr::Literal { .. }
        | Expr::RuntimeLiteral { .. } => {}
        Expr::GetField { value, .. } => walk(value, arrays, deps),
    }
}

fn walk_all_columns(
    expression: &Expr,
    arrays: &HashMap<String, ArrayRoot>,
    deps: &mut Dependencies,
) {
    // Opaque lambda/list use needs the complete list for each active source.
    for root in arrays.values() {
        deps.full.insert(root.source.clone());
    }
    expression.visit(&mut |node| {
        if let Expr::Column { name } = node {
            deps.full.insert(name.clone());
        }
        if let Expr::LambdaVariable { name } = node
            && let Some(root) = arrays.get(name)
        {
            deps.full.insert(root.source.clone());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn column(name: &str) -> Expr {
        Expr::Column { name: name.into() }
    }

    fn lambda(name: &str) -> Expr {
        Expr::LambdaVariable { name: name.into() }
    }

    fn field(value: Expr, name: &str) -> Expr {
        Expr::GetField {
            value: Arc::new(value),
            name: name.into(),
        }
    }

    #[test]
    fn map_paths_union_across_outputs_and_keep_field_names_atomic() {
        let mapped = |leaf: &str| Expr::ArrayMap {
            value: Arc::new(column("cells.with.dot")),
            parameter: "cell".into(),
            body: Arc::new(field(lambda("cell"), leaf)),
        };
        let paths = map_rows_paths(&[
            ("gt".into(), mapped("GT")),
            ("present".into(), mapped("present")),
        ]);
        assert_eq!(
            paths,
            vec![
                vec!["cells.with.dot".to_string(), "GT".to_string()],
                vec!["cells.with.dot".to_string(), "present".to_string()],
            ]
        );
    }

    #[test]
    fn bare_container_use_disables_its_nested_hints() {
        let mapped = Expr::ArrayMap {
            value: Arc::new(column("cells")),
            parameter: "cell".into(),
            body: Arc::new(field(lambda("cell"), "GT")),
        };
        assert_eq!(
            map_rows_paths(&[("gt".into(), mapped), ("raw".into(), column("cells"))]),
            Vec::<Vec<String>>::new()
        );
    }

    #[test]
    fn returning_whole_lambda_element_disables_nested_hints() {
        let mapped = Expr::ArrayMap {
            value: Arc::new(column("cells")),
            parameter: "cell".into(),
            body: Arc::new(lambda("cell")),
        };
        let gt = Expr::ArrayMap {
            value: Arc::new(column("cells")),
            parameter: "cell".into(),
            body: Arc::new(field(lambda("cell"), "GT")),
        };
        assert_eq!(
            map_rows_paths(&[("raw".into(), mapped), ("gt".into(), gt)]),
            Vec::<Vec<String>>::new()
        );
    }

    #[test]
    fn nested_array_map_falls_back_to_full_source_column() {
        let inner = Expr::ArrayMap {
            value: Arc::new(field(lambda("cell"), "nested")),
            parameter: "inner".into(),
            body: Arc::new(field(lambda("inner"), "GT")),
        };
        let outer = Expr::ArrayMap {
            value: Arc::new(column("cells")),
            parameter: "cell".into(),
            body: Arc::new(inner),
        };
        assert_eq!(
            map_rows_paths(&[("gt".into(), outer)]),
            Vec::<Vec<String>>::new()
        );
    }

    #[test]
    fn captured_columns_are_full_while_lambda_fields_are_narrow() {
        let mapped = Expr::ArrayMap {
            value: Arc::new(column("cells")),
            parameter: "cell".into(),
            body: Arc::new(Expr::Binary {
                operator: vail_execution::expressions::BinaryOperator::Add,
                left: Arc::new(field(lambda("cell"), "GT")),
                right: Arc::new(column("offset")),
            }),
        };
        assert_eq!(
            map_rows_paths(&[("mapped".into(), mapped)]),
            vec![vec!["cells".to_string(), "GT".to_string()]]
        );
    }

    #[test]
    fn nested_lambda_shadowing_keeps_the_correct_source_root() {
        let nested = Expr::ArrayMap {
            value: Arc::new(column("other_cells")),
            parameter: "cell".into(),
            body: Arc::new(field(lambda("cell"), "DP")),
        };
        let outer = Expr::ArrayMap {
            value: Arc::new(column("cells")),
            parameter: "cell".into(),
            body: Arc::new(Expr::Struct {
                fields: vec![
                    ("gt".into(), field(lambda("cell"), "GT")),
                    ("nested".into(), nested),
                ],
            }),
        };
        assert_eq!(
            map_rows_paths(&[("mapped".into(), outer)]),
            vec![
                vec!["cells".to_string(), "GT".to_string()],
                vec!["other_cells".to_string(), "DP".to_string()],
            ]
        );
    }
}
