//! Key-interval planning for independent Table join partitions.
//!
//! Bounds are metadata only. Certified Vortex fragment bounds are used for
//! direct reads; operators that may change keys fall back to a streaming pass
//! retaining just the first and last key of each prepared partition.
use crate::{
    Result, TableError, TableIR, TableJoinKind, TableNode, TableRef,
    partitioned::{OrderedConcatExec, PreparedTable},
    plan::{self, PhysicalPlan, column},
};
use arrow_array::{Array, ArrayRef, Int32Array};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::ScalarValue;
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, IsNullExpr, Literal, NotExpr};
use datafusion_physical_plan::filter::FilterExec;
use futures::TryStreamExt;
use std::cmp::Ordering;
use std::sync::Arc;
use vail_storage::fragment_index::compare_typed_key_components;

#[derive(Clone, Debug)]
struct Bound {
    first: Vec<ArrayRef>,
    last: Vec<ArrayRef>,
}

#[derive(Clone, Debug)]
enum Cell {
    Point(Vec<ArrayRef>),
    Between(Vec<ArrayRef>, Vec<ArrayRef>),
}

/// Prepare a normal join as disjoint key cells. Every cell executes a DataFusion
/// join over all original partitions whose certified/observed bounds overlap it.
pub(crate) async fn prepare(
    output: &TableRef,
    left: &TableRef,
    right: &TableRef,
    kind: TableJoinKind,
    join_key: usize,
) -> Result<PreparedTable> {
    let left_input = crate::partitioned::prepare_legacy(left.clone()).await?;
    let right_input = crate::partitioned::prepare_legacy(right.clone()).await?;
    let left_bounds = bounds(left, &left_input, join_key).await?;
    let right_bounds = bounds(right, &right_input, join_key).await?;
    let cells = cells(&left_bounds, &right_bounds)?;
    let mut parts = Vec::with_capacity(cells.len());
    for cell in cells {
        let left_parts = clipped_parts(&left_input.parts, &left_bounds, &cell, left, join_key)?;
        let right_parts = clipped_parts(&right_input.parts, &right_bounds, &cell, right, join_key)?;
        if left_parts.is_empty() && right_parts.is_empty() {
            continue;
        }
        let left_rows = concat_cell(left, left_parts)?;
        let right_rows = concat_cell(right, right_parts)?;
        let left_local = TableIR::prepared(left, left_rows, None, 1);
        let right_local = TableIR::prepared(right, right_rows, None, 1);
        parts.push(crate::joins::lower(output, &left_local, &right_local, kind, join_key)?.rows);
    }
    let schema = plan::physical_schema(output);
    if parts.is_empty() {
        parts.push(Arc::new(datafusion_physical_plan::empty::EmptyExec::new(
            schema.clone(),
        )) as PhysicalPlan);
    }
    Ok(PreparedTable {
        counts: vec![None; parts.len()],
        parts,
        schema,
    })
}

/// Keep each left reader partition independent for semi/anti joins. Prune and
/// clip the right side to that left partition's key interval before joining.
pub(crate) async fn prepare_membership(
    output: &TableRef,
    left: &TableRef,
    right: &TableRef,
    kind: TableJoinKind,
    join_key: usize,
) -> Result<PreparedTable> {
    let left_input = crate::partitioned::prepare_legacy(left.clone()).await?;
    let right_input = crate::partitioned::prepare_legacy(right.clone()).await?;
    let left_bounds = bounds(left, &left_input, join_key).await?;
    let right_bounds = bounds(right, &right_input, join_key).await?;
    let mut parts = Vec::with_capacity(left_input.parts.len());
    for (index, left_part) in left_input.parts.iter().enumerate() {
        let Some(bound) = left_bounds.get(index).and_then(Option::as_ref) else {
            let left_local =
                TableIR::prepared(left, reset(left_part.clone())?, left_input.counts[index], 1);
            let empty_right = empty_like(&right_input.schema)?;
            let right_local = TableIR::prepared(right, empty_right, Some(0), 1);
            parts
                .push(crate::joins::lower(output, &left_local, &right_local, kind, join_key)?.rows);
            continue;
        };
        let candidates = clipped_bounds_parts(
            &right_input.parts,
            &right_bounds,
            &bound.first,
            &bound.last,
            right,
            join_key,
        )?;
        let right_rows = if candidates.is_empty() {
            empty_like(&right_input.schema)?
        } else {
            concat_cell(right, candidates)?
        };
        let left_local =
            TableIR::prepared(left, reset(left_part.clone())?, left_input.counts[index], 1);
        let right_local = TableIR::prepared(right, right_rows, None, 1);
        parts.push(crate::joins::lower(output, &left_local, &right_local, kind, join_key)?.rows);
    }
    let schema = plan::physical_schema(output);
    Ok(PreparedTable {
        counts: vec![None; parts.len()],
        parts,
        schema,
    })
}

fn empty_like(schema: &SchemaRef) -> Result<PhysicalPlan> {
    Ok(Arc::new(datafusion_physical_plan::empty::EmptyExec::new(
        schema.clone(),
    )))
}

fn concat_cell(table: &TableRef, parts: Vec<PhysicalPlan>) -> Result<PhysicalPlan> {
    let schema = plan::physical_schema(table);
    Ok(Arc::new(OrderedConcatExec::new(parts, schema)))
}

/// Reuse the same key cells for multi-way joins, preserving one independently
/// executable output plan per interval.
pub(crate) async fn prepare_zip(
    output: &TableRef,
    tables: &[TableRef],
    data_field_name: &str,
) -> Result<PreparedTable> {
    let join_key = output.keys().len();
    let mut inputs = Vec::with_capacity(tables.len());
    let mut all_bounds = Vec::new();
    for table in tables {
        let prepared = crate::partitioned::prepare_legacy(table.clone()).await?;
        let bounds = bounds(table, &prepared, join_key).await?;
        all_bounds.extend(bounds.iter().cloned());
        inputs.push((table.clone(), prepared, bounds));
    }
    let cells = cells(&all_bounds, &[])?;
    let mut parts = Vec::with_capacity(cells.len());
    for cell in cells {
        let mut local_tables = Vec::with_capacity(inputs.len());
        let mut any_input = false;
        for (table, prepared, bounds) in &inputs {
            let selected = clipped_parts(&prepared.parts, bounds, &cell, table, join_key)?;
            any_input |= !selected.is_empty();
            let rows = if selected.is_empty() {
                empty_like(&prepared.schema)?
            } else {
                concat_cell(table, selected)?
            };
            local_tables.push(TableIR::prepared(table, rows, None, 1));
        }
        if any_input {
            parts.push(crate::zip_join::lower(&local_tables, output, data_field_name)?.rows);
        }
    }
    let schema = plan::physical_schema(output);
    if parts.is_empty() {
        parts.push(Arc::new(datafusion_physical_plan::empty::EmptyExec::new(
            schema.clone(),
        )) as PhysicalPlan);
    }
    Ok(PreparedTable {
        counts: vec![None; parts.len()],
        parts,
        schema,
    })
}

fn clipped_parts(
    parts: &[PhysicalPlan],
    bounds: &[Option<Bound>],
    cell: &Cell,
    table: &TableRef,
    join_key: usize,
) -> Result<Vec<PhysicalPlan>> {
    let mut selected = Vec::new();
    for (part, bound) in parts.iter().zip(bounds) {
        let Some(bound) = bound else { continue };
        if !overlaps(bound, cell)? {
            continue;
        }
        if join_key == 0 {
            selected.push(reset(part.clone())?);
            continue;
        }
        let key = key_expressions(part, &table.keys()[..join_key])?;
        let predicate = match cell {
            Cell::Point(value) => tuple_compare_expr(&key, value, Operator::Eq)?,
            Cell::Between(lower, upper) => Arc::new(BinaryExpr::new(
                tuple_compare_expr(&key, lower, Operator::Gt)?,
                Operator::And,
                tuple_compare_expr(&key, upper, Operator::Lt)?,
            )) as Arc<dyn PhysicalExpr>,
        };
        selected
            .push(Arc::new(FilterExec::try_new(predicate, reset(part.clone())?)?) as PhysicalPlan);
    }
    Ok(selected)
}

fn clipped_bounds_parts(
    parts: &[PhysicalPlan],
    bounds: &[Option<Bound>],
    lower: &[ArrayRef],
    upper: &[ArrayRef],
    table: &TableRef,
    join_key: usize,
) -> Result<Vec<PhysicalPlan>> {
    let mut selected = Vec::new();
    for (part, bound) in parts.iter().zip(bounds) {
        let Some(bound) = bound else { continue };
        if compare_keys(&bound.last, lower)? == Ordering::Less
            || compare_keys(&bound.first, upper)? == Ordering::Greater
        {
            continue;
        }
        if join_key == 0 {
            selected.push(reset(part.clone())?);
        } else {
            let key = key_expressions(part, &table.keys()[..join_key])?;
            let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                tuple_compare_expr(&key, lower, Operator::GtEq)?,
                Operator::And,
                tuple_compare_expr(&key, upper, Operator::LtEq)?,
            ));
            selected.push(
                Arc::new(FilterExec::try_new(predicate, reset(part.clone())?)?) as PhysicalPlan,
            );
        }
    }
    Ok(selected)
}

fn overlaps(bound: &Bound, cell: &Cell) -> Result<bool> {
    match cell {
        Cell::Point(key) => Ok(compare_keys(&bound.first, key)? != Ordering::Greater
            && compare_keys(key, &bound.last)? != Ordering::Greater),
        Cell::Between(lower, upper) => Ok(compare_keys(&bound.last, lower)? == Ordering::Greater
            && compare_keys(&bound.first, upper)? == Ordering::Less),
    }
}

fn cells(left: &[Option<Bound>], right: &[Option<Bound>]) -> Result<Vec<Cell>> {
    let mut boundaries = left
        .iter()
        .chain(right)
        .filter_map(Option::as_ref)
        .flat_map(|bound| [bound.first.clone(), bound.last.clone()])
        .collect::<Vec<_>>();
    for index in 1..boundaries.len() {
        let mut current = index;
        while current > 0
            && compare_keys(&boundaries[current - 1], &boundaries[current])? == Ordering::Greater
        {
            boundaries.swap(current - 1, current);
            current -= 1;
        }
    }
    let mut unique: Vec<Vec<ArrayRef>> = Vec::with_capacity(boundaries.len());
    for boundary in boundaries {
        let is_new = match unique.last() {
            Some(previous) => compare_keys(previous, &boundary)? != Ordering::Equal,
            None => true,
        };
        if is_new {
            unique.push(boundary);
        }
    }
    let mut result = Vec::with_capacity(unique.len().saturating_mul(2));
    for (index, boundary) in unique.iter().enumerate() {
        result.push(Cell::Point(boundary.clone()));
        if let Some(next) = unique.get(index + 1) {
            if compare_keys(boundary, next)? == Ordering::Less {
                result.push(Cell::Between(boundary.clone(), next.clone()));
            }
        }
    }
    Ok(result)
}

async fn bounds(
    table: &TableRef,
    prepared: &PreparedTable,
    join_key: usize,
) -> Result<Vec<Option<Bound>>> {
    if join_key == 0 {
        return Ok(vec![
            Some(Bound {
                first: vec![],
                last: vec![]
            });
            prepared.parts.len()
        ]);
    }
    if let Some(result) = inherited_bounds(table, join_key)? {
        if result.len() == prepared.parts.len() {
            return Ok(result);
        }
    }
    scan_bounds(prepared, &table.keys()[..join_key]).await
}

fn inherited_bounds(table: &TableRef, join_key: usize) -> Result<Option<Vec<Option<Bound>>>> {
    match table.node() {
        TableNode::Prepared { .. } => match &table.partition_metadata {
            Some(source) => inherited_bounds(source, join_key),
            None => Ok(None),
        },
        TableNode::Read { source }
            if table.keys().len() >= join_key && source.keys == table.keys() =>
        {
            let source_bounds = source.partition_typed_key_bounds()?;
            let mut result = Vec::with_capacity(source_bounds.len());
            for pair in source_bounds {
                result.push(
                    pair.map(|(first, last)| -> Result<Bound> {
                        if first.components.len() < join_key || last.components.len() < join_key {
                            return Err(TableError::Invalid(
                                "indexed key has fewer fields than join prefix".into(),
                            ));
                        }
                        Ok(Bound {
                            first: first.components[..join_key].to_vec(),
                            last: last.components[..join_key].to_vec(),
                        })
                    })
                    .transpose()?,
                );
            }
            Ok(Some(result))
        }
        // A range table's source partitions are exact, contiguous Int32 key
        // intervals. Reuse those known bounds instead of encoding every row
        // just to rediscover each partition's endpoints.
        TableNode::Range { n, n_partitions }
            if join_key == 1 && table.keys().first().is_some_and(|key| key == "idx") =>
        {
            let mut result = Vec::with_capacity(*n_partitions);
            for partition in 0..*n_partitions {
                let start = *n / *n_partitions * partition + partition.min(*n % *n_partitions);
                let end = start + *n / *n_partitions + usize::from(partition < *n % *n_partitions);
                result.push(if start == end {
                    None
                } else {
                    Some(Bound {
                        first: vec![Arc::new(Int32Array::from(vec![start as i32])) as ArrayRef],
                        last: vec![Arc::new(Int32Array::from(vec![(end - 1) as i32])) as ArrayRef],
                    })
                });
            }
            Ok(Some(result))
        }
        TableNode::MapGlobals { child }
        | TableNode::Filter { child, .. }
        | TableNode::Explode { child, .. }
            if child.keys() == table.keys() =>
        {
            inherited_bounds(child, join_key)
        }
        TableNode::MapRows { child, fields, .. }
            if child.keys() == table.keys() && fields.iter().all(|(name, expr)| {
                !table.keys()[..join_key].contains(name)
                    || matches!(expr, crate::ScalarIR::Column { name: source } if source == name)
            }) =>
        {
            inherited_bounds(child, join_key)
        }
        TableNode::FilterPartitions { child, parts, keep } if child.keys() == table.keys() => {
            let Some(bounds) = inherited_bounds(child, join_key)? else {
                return Ok(None);
            };
            let picked = bounds
                .into_iter()
                .enumerate()
                .filter(|(i, _)| parts.contains(i) == *keep)
                .map(|(_, b)| b)
                .collect();
            Ok(Some(picked))
        }
        _ => Ok(None),
    }
}

async fn scan_bounds(prepared: &PreparedTable, keys: &[String]) -> Result<Vec<Option<Bound>>> {
    let mut result = Vec::with_capacity(prepared.parts.len());
    for part in &prepared.parts {
        let indices = keys
            .iter()
            .map(|name| part.schema().index_of(name).map_err(TableError::from))
            .collect::<Result<Vec<_>>>()?;
        let mut stream = reset(part.clone())?.execute(0, Arc::new(TaskContext::default()))?;
        let mut first: Option<Vec<ArrayRef>> = None;
        let mut last: Option<Vec<ArrayRef>> = None;
        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            let row_key = |row: usize| {
                indices
                    .iter()
                    .map(|&i| batch.column(i).slice(row, 1))
                    .collect::<Vec<_>>()
            };
            if first.is_none() {
                first = Some(row_key(0));
            }
            last = Some(row_key(batch.num_rows() - 1));
        }
        result.push(first.zip(last).map(|(first, last)| Bound { first, last }));
    }
    Ok(result)
}

fn reset(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    Ok(crate::partitioned::reset_plan_states(plan)?)
}

fn key_sort_options(arity: usize) -> Vec<SortOptions> {
    vec![
        SortOptions {
            descending: false,
            nulls_first: false
        };
        arity
    ]
}

fn compare_keys(left: &[ArrayRef], right: &[ArrayRef]) -> Result<Ordering> {
    if left.len() != right.len() {
        return Err(TableError::Invalid("join key arity mismatch".into()));
    }
    let options = key_sort_options(left.len());
    for ((left, right), options) in left.iter().zip(right).zip(options) {
        let left_scalar = ScalarValue::try_from_array(left, 0)?;
        let right_scalar = ScalarValue::try_from_array(right, 0)?;
        // DataFusion equates signed zeros in join predicates even though
        // Arrow's total-order comparator places them at distinct sort points.
        // Cell boundaries must follow join equality or the same zero rows can
        // be selected into two adjacent cells and emit duplicate matches.
        let equal_native_float = match (&left_scalar, &right_scalar) {
            (ScalarValue::Float32(Some(left)), ScalarValue::Float32(Some(right))) => left == right,
            (ScalarValue::Float64(Some(left)), ScalarValue::Float64(Some(right))) => left == right,
            _ => false,
        };
        if equal_native_float {
            continue;
        }
        let ordering = compare_typed_key_components(
            std::slice::from_ref(left),
            std::slice::from_ref(right),
            &[options],
        )
        .map_err(|error| TableError::Invalid(error.to_string()))?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn key_expressions(input: &PhysicalPlan, keys: &[String]) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    keys.iter()
        .map(|name| {
            let index = input.schema().index_of(name)?;
            Ok(column(input, index))
        })
        .collect()
}

fn tuple_compare_expr(
    keys: &[Arc<dyn PhysicalExpr>],
    values: &[ArrayRef],
    op: Operator,
) -> Result<Arc<dyn PhysicalExpr>> {
    if keys.len() != values.len() {
        return Err(TableError::Invalid("join key arity mismatch".into()));
    }
    let mut equal_prefix: Option<Arc<dyn PhysicalExpr>> = None;
    let mut less_terms = Vec::with_capacity(keys.len());
    let mut greater_terms = Vec::with_capacity(keys.len());
    for (key, array) in keys.iter().zip(values) {
        let value = ScalarValue::try_from_array(array, 0)?;
        let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(value));
        let key_null: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(key.clone()));
        let literal_null: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(literal.clone()));
        let equal = or_expr(
            and_expr(key_null.clone(), literal_null.clone()),
            and_expr(
                not_expr(key_null.clone()),
                and_expr(
                    not_expr(literal_null.clone()),
                    binary(key.clone(), Operator::Eq, literal.clone()),
                ),
            ),
        );
        let less = or_expr(
            and_expr(not_expr(key_null.clone()), literal_null.clone()),
            and_expr(
                not_expr(key_null.clone()),
                and_expr(
                    not_expr(literal_null.clone()),
                    binary(key.clone(), Operator::Lt, literal.clone()),
                ),
            ),
        );
        let greater = or_expr(
            and_expr(key_null.clone(), not_expr(literal_null.clone())),
            and_expr(
                not_expr(key_null.clone()),
                and_expr(
                    not_expr(literal_null.clone()),
                    binary(key.clone(), Operator::Gt, literal.clone()),
                ),
            ),
        );
        less_terms.push(match &equal_prefix {
            Some(prefix) => and_expr(prefix.clone(), less),
            None => less,
        });
        greater_terms.push(match &equal_prefix {
            Some(prefix) => and_expr(prefix.clone(), greater),
            None => greater,
        });
        equal_prefix = Some(match equal_prefix {
            Some(prefix) => and_expr(prefix, equal),
            None => equal,
        });
    }
    let less = less_terms
        .into_iter()
        .reduce(or_expr)
        .unwrap_or_else(|| bool_literal(false));
    let greater = greater_terms
        .into_iter()
        .reduce(or_expr)
        .unwrap_or_else(|| bool_literal(false));
    let equal = equal_prefix.unwrap_or_else(|| bool_literal(true));
    Ok(match op {
        Operator::Eq => equal,
        Operator::Lt => less,
        Operator::LtEq => or_expr(less, equal),
        Operator::Gt => greater,
        Operator::GtEq => or_expr(greater, equal),
        _ => {
            return Err(TableError::Invalid(
                "unsupported key comparison operator".into(),
            ));
        }
    })
}

fn binary(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(left, op, right))
}
fn and_expr(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    binary(left, Operator::And, right)
}
fn or_expr(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    binary(left, Operator::Or, right)
}
fn not_expr(value: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(NotExpr::new(value))
}
fn bool_literal(value: bool) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Boolean(Some(value))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: i32) -> Vec<ArrayRef> {
        vec![Arc::new(Int32Array::from(vec![value])) as ArrayRef]
    }
    fn bound(first: i32, last: i32) -> Option<Bound> {
        Some(Bound {
            first: key(first),
            last: key(last),
        })
    }
    fn float_key(value: f64) -> Vec<ArrayRef> {
        vec![Arc::new(arrow_array::Float64Array::from(vec![value])) as ArrayRef]
    }
    fn point_values(cells: &[Cell]) -> Vec<i32> {
        cells
            .iter()
            .filter_map(|cell| match cell {
                Cell::Point(value) => match ScalarValue::try_from_array(&value[0], 0).unwrap() {
                    ScalarValue::Int32(Some(value)) => Some(value),
                    _ => panic!("expected Int32 key"),
                },
                _ => None,
            })
            .collect()
    }

    #[test]
    fn cells_are_disjoint_points_and_open_gaps() {
        let left = vec![bound(1, 5)];
        let right = vec![bound(5, 9)];
        let planned = cells(&left, &right).unwrap();
        assert_eq!(point_values(&planned), vec![1, 5, 9]);
        assert_eq!(
            planned
                .iter()
                .filter(|c| matches!(c, Cell::Between(_, _)))
                .count(),
            2
        );
        assert!(overlaps(&left[0].clone().unwrap(), &Cell::Point(key(5))).unwrap());
        assert!(!overlaps(&left[0].clone().unwrap(), &Cell::Between(key(5), key(9))).unwrap());
    }

    #[test]
    fn missing_bounds_and_duplicate_prefixes_keep_a_single_boundary_cell() {
        let left = vec![bound(1, 2), bound(2, 3)];
        let right = vec![None, bound(2, 4)];
        let planned = cells(&left, &right).unwrap();
        assert_eq!(
            point_values(&planned).iter().filter(|&&v| v == 2).count(),
            1
        );
        assert!(planned.iter().all(|c| match c {
            Cell::Between(a, b) => compare_keys(a, b).unwrap() == Ordering::Less,
            _ => true,
        }));
        assert!(cells(&[None], &[None]).unwrap().is_empty());
    }

    #[test]
    fn join_cells_merge_boundaries_equal_under_native_float_join_semantics() {
        let left = vec![Some(Bound {
            first: float_key(-0.0),
            last: float_key(-0.0),
        })];
        let right = vec![Some(Bound {
            first: float_key(0.0),
            last: float_key(0.0),
        })];
        let planned = cells(&left, &right).unwrap();
        assert_eq!(planned.len(), 1);
        assert!(matches!(planned[0], Cell::Point(_)));
        assert!(overlaps(&left[0].clone().unwrap(), &planned[0]).unwrap());
        assert!(overlaps(&right[0].clone().unwrap(), &planned[0]).unwrap());
    }

    #[test]
    fn range_table_bounds_are_derived_without_scanning_rows() {
        let table = TableIR::range(10, 3).unwrap();
        let bounds = inherited_bounds(&table, 1).unwrap().unwrap();
        let expected = [(0, 3), (4, 6), (7, 9)];
        assert_eq!(bounds.len(), expected.len());
        for (bound, (first, last)) in bounds.into_iter().zip(expected) {
            let bound = bound.unwrap();
            assert_eq!(
                ScalarValue::try_from_array(&bound.first[0], 0).unwrap(),
                ScalarValue::Int32(Some(first))
            );
            assert_eq!(
                ScalarValue::try_from_array(&bound.last[0], 0).unwrap(),
                ScalarValue::Int32(Some(last))
            );
        }

        let mostly_empty = TableIR::range(2, 4).unwrap();
        let bounds = inherited_bounds(&mostly_empty, 1).unwrap().unwrap();
        assert_eq!(bounds.len(), 2);
        assert!(bounds.iter().all(Option::is_some));
    }
}
