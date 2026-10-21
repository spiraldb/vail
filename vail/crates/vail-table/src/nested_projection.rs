//! Conservative nested source-column dependency analysis for MapRows.
//!
//! The only array projection recognized here is a direct `ArrayMap` over a
//! source column whose element is accessed through `GetField` paths. Any
//! opaque or whole-container use marks the source column as fully required.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use vail_execution::expressions::Expr;

#[derive(Clone)]
struct ArrayRoot {
    source: String,
}

#[derive(Default, Clone)]
struct Dependencies {
    paths: BTreeMap<String, BTreeSet<Vec<String>>>,
    full: BTreeSet<String>,
}

/// Return full-name paths for safely narrowed top-level source fields.
/// A path includes its row field, for example `[_CELLS, value, GT]`.
pub(crate) fn map_rows_paths(fields: &[(String, Expr)]) -> Vec<Vec<String>> {
    let mut deps = Dependencies::default();
    let mut seen = HashSet::new();
    let mut next_context = 1;
    for (_, expression) in fields {
        walk(
            expression,
            &HashMap::new(),
            &mut deps,
            0,
            &mut seen,
            &mut next_context,
        );
    }
    deps.paths
        .into_iter()
        .filter(|(root, _)| !deps.full.contains(root))
        .flat_map(|(_, paths)| paths)
        .collect()
}

/// Indexed once per source root, so many projected siblings do not repeatedly
/// parse the same metadata or linearly scan the same struct fields.
#[derive(Default)]
struct StorageFields(HashMap<String, StorageFields>);
impl StorageFields {
    fn from_type(ty: vail_execution::expressions::Type) -> Self {
        use vail_execution::expressions::Type;
        match ty {
            Type::Array { array } => Self::from_type(*array),
            Type::Struct { r#struct: fields } => Self(
                fields
                    .into_iter()
                    .map(|(name, child)| (name, Self::from_type(child)))
                    .collect(),
            ),
            _ => Self::default(),
        }
    }
}

/// Logical accessors below an opaque value (Call, interval, tuple, etc.) are
/// operations, not physical storage field names. Preserve that whole value.
pub(crate) fn map_rows_storage_paths(
    fields: &[(String, Expr)],
    schema: &arrow_schema::Schema,
) -> Vec<Vec<String>> {
    use vail_execution::expressions::Type;
    let top_fields = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.as_ref()))
        .collect::<HashMap<_, _>>();
    let mut roots = HashMap::new();
    map_rows_paths(fields)
        .into_iter()
        .map(|path| {
            let mut node: &StorageFields = roots.entry(path[0].clone()).or_insert_with(|| {
                top_fields
                    .get(path[0].as_str())
                    .and_then(|field| Type::from_arrow(field.data_type()).ok())
                    .map(StorageFields::from_type)
                    .unwrap_or_default()
            });
            let mut prefix = vec![path[0].clone()];
            for name in &path[1..] {
                let Some(child) = node.0.get(name) else {
                    break;
                };
                prefix.push(name.clone());
                node = child;
            }
            prefix
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn path_root<'a>(
    expression: &'a Expr,
    arrays: &HashMap<String, ArrayRoot>,
) -> Option<(String, Vec<String>)> {
    match expression {
        Expr::Column { name } => Some((name.clone(), Vec::new())),
        Expr::LambdaVariable { name, .. } => arrays
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

fn walk(
    expression: &Expr,
    arrays: &HashMap<String, ArrayRoot>,
    deps: &mut Dependencies,
    context: usize,
    seen: &mut HashSet<(usize, usize)>,
    next_context: &mut usize,
) {
    if !seen.insert((expression as *const Expr as usize, context)) {
        return;
    }
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
        Expr::Switch { .. } => walk_all_columns(expression, arrays, deps),
        Expr::MatrixScope { .. }
        | Expr::MatrixField { .. }
        | Expr::MatrixNumCols { .. }
        | Expr::MatrixColumnPosition { .. }
        | Expr::TableLookup { .. }
        | Expr::AlignedMatrixMap { .. } => walk_all_columns(expression, arrays, deps),

        Expr::Column { name } => {
            deps.full.insert(name.clone());
        }
        Expr::LambdaVariable { name, .. } => {
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
                let nested_context = *next_context;
                *next_context += 1;
                walk(body, &nested, deps, nested_context, seen, next_context);
            } else {
                // Nested/list-derived arrays need a richer path model. Mark
                // every referenced source field full to avoid unsafe pruning.
                walk_all_columns(expression, arrays, deps);
            }
        }
        Expr::Struct { fields } => {
            for (_, value) in fields {
                walk(value, arrays, deps, context, seen, next_context);
            }
        }
        Expr::Tuple { elements }
        | Expr::StringFunction { args: elements, .. }
        | Expr::NestedFunction { args: elements, .. }
        | Expr::Array { elements, .. } => {
            for value in elements {
                walk(value, arrays, deps, context, seen, next_context);
            }
        }
        Expr::Binary { left, right, .. } | Expr::IntegerBinary { left, right, .. } => {
            walk(left, arrays, deps, context, seen, next_context);
            walk(right, arrays, deps, context, seen, next_context);
        }
        Expr::IfElse {
            condition,
            consequent,
            alternate,
        } => {
            walk(condition, arrays, deps, context, seen, next_context);
            walk(consequent, arrays, deps, context, seen, next_context);
            walk(alternate, arrays, deps, context, seen, next_context);
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
        | Expr::ParseCsvLine { value, .. } => {
            walk(value, arrays, deps, context, seen, next_context)
        }
        Expr::Interval {
            start,
            end,
            includes_start,
            includes_end,
        } => {
            for value in [start, end, includes_start, includes_end] {
                walk(value, arrays, deps, context, seen, next_context);
            }
        }
        Expr::IntervalContains { interval, point } => {
            walk(interval, arrays, deps, context, seen, next_context);
            walk(point, arrays, deps, context, seen, next_context);
        }
        Expr::IntervalOverlaps { left, right } => {
            walk(left, arrays, deps, context, seen, next_context);
            walk(right, arrays, deps, context, seen, next_context);
        }
        Expr::ArrayIndex { value, index } => {
            walk(value, arrays, deps, context, seen, next_context);
            walk(index, arrays, deps, context, seen, next_context);
        }
        Expr::Locus {
            contig, position, ..
        }
        | Expr::LocusValid {
            contig, position, ..
        } => {
            walk(contig, arrays, deps, context, seen, next_context);
            walk(position, arrays, deps, context, seen, next_context);
        }
        Expr::LocusFromGlobalPosition { position, .. } => {
            walk(position, arrays, deps, context, seen, next_context)
        }
        Expr::ArraySlice {
            value,
            start,
            stop,
            step,
        } => {
            walk(value, arrays, deps, context, seen, next_context);
            for value in [start, stop, step].into_iter().flatten() {
                walk(value, arrays, deps, context, seen, next_context);
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
        | Expr::ArrayAggregateScan { .. }
        | Expr::ArrayFold { .. }
        | Expr::ArrayPredicate { .. }
        | Expr::ArrayFilter { .. }
        | Expr::StreamRange { .. }
        | Expr::ArrayGrouped { .. } => walk_all_columns(expression, arrays, deps),
        Expr::ExternalGlobal { .. }
        | Expr::Global { .. }
        | Expr::AlignedMapInput { .. }
        | Expr::Literal { .. }
        | Expr::RuntimeLiteral { .. } => {}
        Expr::GetField { value, .. } => walk(value, arrays, deps, context, seen, next_context),
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
    expression.visit_unique(&mut |node| {
        if let Expr::Column { name } = node {
            deps.full.insert(name.clone());
        }
        if let Expr::LambdaVariable { name, .. } = node
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
        Expr::LambdaVariable {
            name: name.into(),
            dtype: None,
        }
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

// VCF-specific backward dependencies preserve lexical callback scopes and array
// shape. Planning runs once at an action boundary, memoized by original node,
// requested path, and an owning scope id. The source keeps its declared schema.
use vail_vcf::DecodePath;
#[derive(Default, Clone)]
struct VcfNeeds {
    full: BTreeSet<String>,
    paths: BTreeMap<String, BTreeSet<Vec<String>>>,
    shapes: BTreeMap<String, BTreeSet<Vec<String>>>,
    validities: BTreeMap<String, BTreeSet<Vec<String>>>,
}
struct Scope<'a> {
    name: String,
    value: &'a Expr,
    parent: usize,
}
#[derive(Default)]
struct VcfDependencies<'a> {
    scopes: Vec<Scope<'a>>,
    scope_ids: HashMap<(usize, usize), usize>,
    memo: HashMap<(usize, Vec<String>, usize, bool), VcfNeeds>,
    validity_memo: HashMap<(usize, Vec<String>, usize), VcfNeeds>,
    structs: HashMap<usize, HashMap<&'a str, &'a Expr>>,
}
fn merge_deps(into: &mut VcfNeeds, other: VcfNeeds) {
    into.full.extend(other.full);
    for (root, paths) in other.shapes {
        into.shapes.entry(root).or_default().extend(paths);
    }
    for (root, paths) in other.paths {
        into.paths.entry(root).or_default().extend(paths);
    }
    for (root, paths) in other.validities {
        into.validities.entry(root).or_default().extend(paths);
    }
}
impl<'a> VcfDependencies<'a> {
    fn scope(&mut self, node: &'a Expr, name: &str, value: &'a Expr, parent: usize) -> usize {
        let key = (node as *const Expr as usize, parent);
        if let Some(&id) = self.scope_ids.get(&key) {
            return id;
        }
        let id = self.scopes.len() + 1;
        self.scopes.push(Scope {
            name: name.into(),
            value,
            parent,
        });
        self.scope_ids.insert(key, id);
        id
    }
    fn collect(&mut self, expr: &'a Expr, tail: &[String], scope: usize) -> VcfNeeds {
        self.request(expr, tail, scope, false)
    }
    fn request_validity(&mut self, expr: &'a Expr, tail: &[String], scope: usize) -> VcfNeeds {
        let key = (expr as *const Expr as usize, tail.to_vec(), scope);
        if let Some(result) = self.validity_memo.get(&key) {
            return result.clone();
        }
        let mut result = VcfNeeds::default();
        match expr {
            Expr::MatrixField { axis, name, .. } if axis == "entry" => {
                let mut path = vec!["__vail_matrix_cells".into(), "value".into(), name.clone()];
                path.extend_from_slice(tail);
                result
                    .validities
                    .entry("__vail_matrix_cells".into())
                    .or_default()
                    .insert(path);
            }
            Expr::Column { name } => {
                let mut path = vec![name.clone()];
                path.extend_from_slice(tail);
                result
                    .validities
                    .entry(name.clone())
                    .or_default()
                    .insert(path);
            }
            Expr::GetField { value, name } => {
                let mut path = vec![name.clone()];
                path.extend_from_slice(tail);
                result = self.request_validity(value, &path, scope);
            }
            Expr::LambdaVariable { name, .. } => {
                let mut current = scope;
                while current > 0 {
                    let binding = &self.scopes[current - 1];
                    if &binding.name == name {
                        let value = binding.value;
                        let parent = binding.parent;
                        result = self.request_array_element_validity(value, tail, parent);
                        break;
                    }
                    current = binding.parent;
                }
            }
            Expr::Struct { fields } => {
                if let Some((field, rest)) = tail.split_first() {
                    if let Some((_, value)) = fields.iter().find(|(name, _)| name == field) {
                        result = self.request_validity(value, rest, scope);
                    }
                }
            }
            Expr::ArrayMap {
                value,
                parameter,
                body,
            } => {
                if tail.is_empty() {
                    // The mapped array itself is null exactly when its input is.
                    result = self.request_validity(value, &[], scope);
                } else {
                    // A projected element's validity depends on the callback
                    // result. Preserve the input offsets and trace that result
                    // through the callback's lexical binding.
                    merge_deps(&mut result, self.request(value, &[], scope, true));
                    let nested = self.scope(expr, parameter, value, scope);
                    merge_deps(&mut result, self.request_validity(body, tail, nested));
                }
            }
            Expr::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                merge_deps(&mut result, self.collect(condition, &[], scope));
                merge_deps(&mut result, self.request_validity(consequent, tail, scope));
                merge_deps(&mut result, self.request_validity(alternate, tail, scope));
            }
            Expr::Array { elements, .. } => {
                if let Some(index) = tail.first().and_then(|value| value.parse::<usize>().ok()) {
                    if let Some(element) = elements.get(index) {
                        result = self.request_validity(element, &tail[1..], scope);
                    }
                } else if !tail.is_empty() {
                    for element in elements {
                        merge_deps(&mut result, self.request_validity(element, tail, scope));
                    }
                }
            }
            // For operations whose nullness can depend on computed values,
            // retain their normal value dependencies.
            _ => merge_deps(&mut result, self.collect(expr, tail, scope)),
        }
        self.validity_memo.insert(key, result.clone());
        result
    }
    fn request_array_element_validity(
        &mut self,
        array: &'a Expr,
        tail: &[String],
        scope: usize,
    ) -> VcfNeeds {
        let mut result = VcfNeeds::default();
        match array {
            Expr::ArrayMap {
                value,
                parameter,
                body,
            } => {
                // Keep the container offsets/nullness, then project the
                // callback's element validity through its lexical origin.
                merge_deps(&mut result, self.request(value, &[], scope, true));
                let nested = self.scope(array, parameter, value, scope);
                merge_deps(&mut result, self.request_validity(body, tail, nested));
            }
            Expr::Array { elements, .. } => {
                for element in elements {
                    merge_deps(&mut result, self.request_validity(element, tail, scope));
                }
            }
            Expr::MatrixField { axis, .. } if axis == "entry" => {
                merge_deps(&mut result, self.request(array, &[], scope, true));
                merge_deps(&mut result, self.request_validity(array, tail, scope));
            }
            Expr::Column { .. } | Expr::GetField { .. } | Expr::LambdaVariable { .. } => {
                merge_deps(&mut result, self.request(array, &[], scope, true));
                merge_deps(&mut result, self.request_validity(array, tail, scope));
            }
            _ => merge_deps(&mut result, self.collect(array, &[], scope)),
        }
        result
    }
    fn request(&mut self, expr: &'a Expr, tail: &[String], scope: usize, shape: bool) -> VcfNeeds {
        let key = (expr as *const Expr as usize, tail.to_vec(), scope, shape);
        if let Some(result) = self.memo.get(&key) {
            return result.clone();
        }
        let mut result = VcfNeeds::default();
        match expr {
            Expr::MatrixField { axis, name, .. } if axis == "entry" => {
                let mut path = vec!["__vail_matrix_cells".into(), "value".into(), name.clone()];
                path.extend_from_slice(tail);
                if shape {
                    result
                        .shapes
                        .entry("__vail_matrix_cells".into())
                        .or_default()
                        .insert(vec!["__vail_matrix_cells".into()]);
                } else {
                    result
                        .paths
                        .entry("__vail_matrix_cells".into())
                        .or_default()
                        .insert(path);
                }
            }
            Expr::IsMissing { value } => {
                merge_deps(&mut result, self.request_validity(value, &[], scope));
            }
            Expr::Column { name } => {
                if shape {
                    let mut path = vec![name.clone()];
                    path.extend_from_slice(tail);
                    result.shapes.entry(name.clone()).or_default().insert(path);
                } else if tail.is_empty() {
                    result.full.insert(name.clone());
                } else {
                    let mut path = vec![name.clone()];
                    path.extend_from_slice(tail);
                    result.paths.entry(name.clone()).or_default().insert(path);
                }
            }
            Expr::LambdaVariable { name, .. } => {
                let mut current = scope;
                while current > 0 {
                    let binding = &self.scopes[current - 1];
                    if &binding.name == name {
                        let value = binding.value;
                        let parent = binding.parent;
                        result = self.request(value, tail, parent, shape);
                        break;
                    }
                    current = binding.parent;
                }
            }
            Expr::GetField { value, name } => {
                let mut path = vec![name.clone()];
                path.extend_from_slice(tail);
                result = self.request(value, &path, scope, shape);
            }
            Expr::Struct { fields } => {
                if tail.is_empty() && !shape {
                    for (_, value) in fields {
                        merge_deps(&mut result, self.collect(value, &[], scope));
                    }
                } else if !tail.is_empty() {
                    let fields = self
                        .structs
                        .entry(expr as *const Expr as usize)
                        .or_insert_with(|| {
                            fields
                                .iter()
                                .map(|(name, value)| (name.as_str(), value))
                                .collect()
                        });
                    if let Some(&value) = fields.get(tail[0].as_str()) {
                        result = self.request(value, &tail[1..], scope, shape);
                    }
                }
            }
            Expr::ArrayMap {
                value,
                parameter,
                body,
            } => {
                merge_deps(&mut result, self.request(value, &[], scope, true));
                if (!shape || !tail.is_empty()) {
                    let nested = self.scope(expr, parameter, value, scope);
                    merge_deps(&mut result, self.request(body, tail, nested, shape));
                }
            }
            Expr::ArrayFilter {
                value,
                parameter,
                predicate,
            } => {
                let nested = self.scope(expr, parameter, value, scope);
                merge_deps(&mut result, self.collect(predicate, &[], nested));
                merge_deps(&mut result, self.request(value, tail, scope, shape));
            }
            Expr::ArrayIndex { value, index } => {
                merge_deps(&mut result, self.collect(index, &[], scope));
                merge_deps(&mut result, self.request(value, &[], scope, true));
                merge_deps(&mut result, self.request(value, tail, scope, shape));
            }
            Expr::IfElse {
                condition,
                consequent,
                alternate,
            } => {
                merge_deps(&mut result, self.collect(condition, &[], scope));
                merge_deps(&mut result, self.request(consequent, tail, scope, shape));
                merge_deps(&mut result, self.request(alternate, tail, scope, shape));
            }
            Expr::Array { elements, .. } => {
                if (!shape || !tail.is_empty()) {
                    for element in elements {
                        merge_deps(&mut result, self.request(element, tail, scope, shape));
                    }
                }
            }
            Expr::NestedFunction { function, args, .. } if function == "arrays_zip" => {
                for arg in args {
                    merge_deps(&mut result, self.request(arg, &[], scope, true));
                }
                if (!shape || !tail.is_empty()) {
                    if let Some(index) = tail
                        .first()
                        .and_then(|v| v.parse::<usize>().ok())
                        .and_then(|v| v.checked_sub(1))
                        .filter(|&i| i < args.len())
                    {
                        merge_deps(
                            &mut result,
                            self.request(&args[index], &tail[1..], scope, shape),
                        );
                    } else {
                        for arg in args {
                            merge_deps(&mut result, self.collect(arg, &[], scope));
                        }
                    }
                }
            }
            Expr::Literal { .. }
            | Expr::RuntimeLiteral { .. }
            | Expr::ExternalGlobal { .. }
            | Expr::Global { .. } => {}
            _ => {
                // Unknown binders/operations need all their inputs. Visiting
                // original children avoids expanding shared expression DAGs.
                let mut children = Vec::new();
                expr.visit_children(&mut |child| children.push(child));
                for child in children {
                    merge_deps(&mut result, self.collect(child, &[], scope));
                }
            }
        }
        self.memo.insert(key, result.clone());
        result
    }
}
fn all_paths(deps: VcfNeeds) -> Vec<DecodePath> {
    let value_paths = deps.paths.clone();
    let mut result = deps
        .full
        .iter()
        .map(|root| DecodePath {
            fields: vec![root.clone()],
            shape: false,
            validity: false,
        })
        .collect::<Vec<_>>();
    for (root, paths) in deps.paths {
        if !deps.full.contains(&root) {
            result.extend(paths.into_iter().map(|fields| DecodePath {
                fields,
                shape: false,
                validity: false,
            }));
        }
    }
    for (root, paths) in deps.shapes {
        if !deps.full.contains(&root) {
            result.extend(paths.into_iter().map(|fields| DecodePath {
                fields,
                shape: true,
                validity: false,
            }));
        }
    }
    for (root, paths) in deps.validities {
        if !deps.full.contains(&root) {
            result.extend(paths.into_iter().filter_map(|fields| {
                (!value_paths
                    .get(&root)
                    .is_some_and(|values| values.contains(&fields)))
                .then_some(DecodePath {
                    fields,
                    shape: false,
                    validity: true,
                })
            }));
        }
    }
    result
}
fn vcf_source(table: &crate::TableRef) -> bool {
    use crate::TableNode;
    let mut source = table;
    loop {
        match source.node() {
            TableNode::MapRows { child, .. }
            | TableNode::KeyBy { child, .. }
            | TableNode::Filter { child, .. }
            | TableNode::MapGlobals { child }
            | TableNode::OrderBy { child, .. }
            | TableNode::Explode { child, .. }
            | TableNode::Head { child, .. }
            | TableNode::Tail { child, .. } => source = child,
            TableNode::TextRead { format, .. } => return format == "vcf",
            _ => return false,
        }
    }
}
pub(crate) fn prepare_vcf(table: &crate::TableRef) -> crate::TableRef {
    if !vcf_source(table) {
        return table.clone();
    }
    narrow_vcf_paths(
        table,
        table
            .schema()
            .fields()
            .iter()
            .map(|f| DecodePath {
                fields: vec![f.name().clone()],
                shape: false,
                validity: false,
            })
            .collect(),
        &mut VcfDependencies::default(),
    )
}
pub(crate) fn prepare_vcf_for<'a>(
    table: &'a crate::TableRef,
    expressions: impl Iterator<Item = &'a Expr>,
) -> crate::TableRef {
    if !vcf_source(table) {
        return table.clone();
    }
    let mut analyzer = VcfDependencies::default();
    let mut deps = VcfNeeds::default();
    for expression in expressions {
        merge_deps(&mut deps, analyzer.collect(expression, &[], 0));
    }
    narrow_vcf_paths(table, all_paths(deps), &mut analyzer)
}
fn narrow_vcf_paths<'a>(
    table: &'a crate::TableRef,
    mut paths: Vec<DecodePath>,
    analyzer: &mut VcfDependencies<'a>,
) -> crate::TableRef {
    use crate::TableNode;
    let node = match table.node() {
        TableNode::TextRead {
            paths: files,
            format,
            options_json,
        } if format == "vcf" => {
            let mut options = options_json.clone();
            options["decode_paths"] = serde_json::json!(paths);
            TableNode::TextRead {
                paths: files.clone(),
                format: format.clone(),
                options_json: options,
            }
        }
        TableNode::MapRows {
            child,
            fields,
            bindings,
        } => {
            let positions = fields
                .iter()
                .map(|(name, expr)| (name, expr))
                .collect::<HashMap<_, _>>();
            let mut deps = VcfNeeds::default();
            for path in paths {
                if let Some(expr) = path.fields.first().and_then(|name| positions.get(name)) {
                    merge_deps(
                        &mut deps,
                        if path.validity {
                            analyzer.request_validity(expr, &path.fields[1..], 0)
                        } else {
                            analyzer.request(expr, &path.fields[1..], 0, path.shape)
                        },
                    );
                }
            }
            TableNode::MapRows {
                child: narrow_vcf_paths(child, all_paths(deps), analyzer),
                fields: fields.clone(),
                bindings: bindings.clone(),
            }
        }
        TableNode::KeyBy { child, keys } => {
            paths.extend(keys.iter().map(|key| DecodePath {
                fields: vec![key.clone()],
                shape: false,
                validity: false,
            }));
            TableNode::KeyBy {
                child: narrow_vcf_paths(child, paths, analyzer),
                keys: keys.clone(),
            }
        }
        TableNode::OrderBy { child, keys } => {
            paths.extend(keys.iter().map(|(key, _)| DecodePath {
                fields: vec![key.clone()],
                shape: false,
                validity: false,
            }));
            TableNode::OrderBy {
                child: narrow_vcf_paths(child, paths, analyzer),
                keys: keys.clone(),
            }
        }
        TableNode::Filter {
            child,
            predicate,
            keep,
            bindings,
        } => {
            paths.extend(all_paths(analyzer.collect(predicate, &[], 0)));
            TableNode::Filter {
                child: narrow_vcf_paths(child, paths, analyzer),
                predicate: predicate.clone(),
                keep: *keep,
                bindings: bindings.clone(),
            }
        }
        TableNode::Explode { child, path } => {
            paths.push(DecodePath {
                fields: path.clone(),
                shape: true,
                validity: false,
            });
            TableNode::Explode {
                child: narrow_vcf_paths(child, paths, analyzer),
                path: path.clone(),
            }
        }
        TableNode::Head { child, n } => TableNode::Head {
            child: narrow_vcf_paths(child, paths, analyzer),
            n: *n,
        },
        TableNode::Tail { child, n } => TableNode::Tail {
            child: narrow_vcf_paths(child, paths, analyzer),
            n: *n,
        },
        TableNode::MapGlobals { child } => TableNode::MapGlobals {
            child: narrow_vcf_paths(child, paths, analyzer),
        },
        _ => return table.clone(),
    };
    table.replacing_node(node)
}
