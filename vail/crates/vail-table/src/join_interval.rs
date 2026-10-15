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
use intervaltree::IntervalTree;
use std::cmp::Ordering;
use std::sync::Arc;
use vail_storage::fragment_index::{
    Endpoint, KeyValue, QueryInterval, compare_typed_key_components, encode_typed_key_component,
    make_typed_key_comparators, normalize_typed_key_component,
};
use vail_storage::local_table::LocalRead;

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
    let left_candidates = candidates_by_cell(&left_bounds, &cells)?;
    let right_candidates = candidates_by_cell(&right_bounds, &cells)?;
    let mut parts = Vec::with_capacity(cells.len());
    for (index, cell) in cells.iter().enumerate() {
        // Bounds certify that these cells cannot produce output. Avoid
        // executing the populated input merely to discover the empty join.
        let left_empty = left_candidates[index].is_empty();
        let right_empty = right_candidates[index].is_empty();
        if match kind {
            TableJoinKind::Inner | TableJoinKind::Semi => left_empty || right_empty,
            TableJoinKind::Left | TableJoinKind::Anti => left_empty,
            TableJoinKind::Right => right_empty,
            TableJoinKind::Outer => left_empty && right_empty,
        } {
            continue;
        }
        let left_parts = clipped_parts(
            &left_input.parts,
            &left_bounds,
            &left_candidates[index],
            cell,
            left,
            join_key,
        )?;
        let right_parts = clipped_parts(
            &right_input.parts,
            &right_bounds,
            &right_candidates[index],
            cell,
            right,
            join_key,
        )?;
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
    let right_index = BoundIndex::new(&right_bounds)?;
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
        let candidate_ids = right_index.query(&bound.first, &bound.last)?;
        let candidates = clipped_bounds_parts(
            &right_input.parts,
            &right_bounds,
            &candidate_ids,
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
    let candidates = inputs
        .iter()
        .map(|(_, _, bounds)| candidates_by_cell(bounds, &cells))
        .collect::<Result<Vec<_>>>()?;
    let mut parts = Vec::with_capacity(cells.len());
    for (cell_index, cell) in cells.iter().enumerate() {
        let mut local_tables = Vec::with_capacity(inputs.len());
        let mut any_input = false;
        for (input_index, (table, prepared, bounds)) in inputs.iter().enumerate() {
            let selected = clipped_parts(
                &prepared.parts,
                bounds,
                &candidates[input_index][cell_index],
                cell,
                table,
                join_key,
            )?;
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
    candidates: &[usize],
    cell: &Cell,
    table: &TableRef,
    join_key: usize,
) -> Result<Vec<PhysicalPlan>> {
    let mut selected = Vec::new();
    for &index in candidates {
        let Some(part) = parts.get(index) else {
            return Err(TableError::Invalid(
                "join partition index out of range".into(),
            ));
        };
        let Some(bound) = bounds.get(index).and_then(Option::as_ref) else {
            continue;
        };
        if !overlaps(bound, cell)? {
            continue;
        }
        if join_key == 0 {
            selected.push(reset(part.clone())?);
            continue;
        }
        if let TableNode::Read { source } = table.node()
            && let Some(query) = read_query_for_cell(table, source, cell, join_key)?
        {
            let rows = source.partition_rows();
            let start = rows[..index].iter().try_fold(0u64, |sum, &n| {
                sum.checked_add(n as u64)
                    .ok_or_else(|| TableError::Invalid("partition offset overflow".into()))
            })?;
            let end = start
                .checked_add(rows[index] as u64)
                .ok_or_else(|| TableError::Invalid("partition offset overflow".into()))?;
            let rows_plan = crate::interval_read::source_interval_in_rows(
                table,
                source.clone(),
                query,
                start..end,
            );
            let key = key_expressions(part, &table.keys()[..join_key])?;
            let predicate = match cell {
                Cell::Point(value) => tuple_compare_expr(&key, value, Operator::Eq)?,
                Cell::Between(lower, upper) => Arc::new(BinaryExpr::new(
                    tuple_compare_expr(&key, lower, Operator::Gt)?,
                    Operator::And,
                    tuple_compare_expr(&key, upper, Operator::Lt)?,
                )) as Arc<dyn PhysicalExpr>,
            };
            selected.push(Arc::new(FilterExec::try_new(predicate, rows_plan)?) as PhysicalPlan);
            continue;
        }
        if matches!(table.node(), TableNode::Range { .. })
            && join_key == 1
            && table.keys().first().is_some_and(|key| key == "idx")
            && let Some((lower, upper)) = integer_range_cell(cell)?
        {
            selected.push(plan::range_partition_clipped(table, index, lower, upper)?);
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

fn read_query_for_cell(
    table: &TableRef,
    source: &LocalRead,
    cell: &Cell,
    join_key: usize,
) -> Result<Option<QueryInterval>> {
    let Ok(binding) = source.index_binding() else {
        return Ok(None);
    };
    // The fragment index requires complete composite endpoints. Prefix joins
    // keep their existing partition scan plus residual filter semantics.
    if join_key == 0 || join_key != binding.key_fields.len() {
        return Ok(None);
    }
    // The interval endpoints are in TableIR key order. Only use the fragment
    // index when that order exactly matches the snapshot's field-ID order.
    if table.keys().len() != binding.key_fields.len()
        || binding.key_fields.iter().any(|key| key.descending)
        || table
            .keys()
            .iter()
            .zip(&binding.key_fields)
            .any(|(name, key)| {
                table
                    .schema()
                    .index_of(name)
                    .ok()
                    .and_then(|index| source.field_ids().get(index))
                    != Some(&key.field_id)
            })
    {
        return Ok(None);
    }
    let endpoint = |components: &[ArrayRef], inclusive: bool| -> Result<Option<Endpoint>> {
        if components.len() < join_key {
            return Ok(None);
        }
        let mut key = Vec::with_capacity(join_key);
        for (array, field) in components.iter().take(join_key).zip(&binding.key_fields) {
            let one = array.slice(0, 1);
            let Ok(normalized) = normalize_typed_key_component(&one, &field.physical_type) else {
                // Index pruning is an optimization. Keep the original scan
                // and residual filter for null or unsupported cell endpoints.
                return Ok(None);
            };
            let Ok(encoded) = encode_typed_key_component(&normalized) else {
                return Ok(None);
            };
            key.push(KeyValue::ArrowIpc(encoded));
        }
        Ok(Some(Endpoint { key, inclusive }))
    };
    let (lower, upper) = match cell {
        Cell::Point(point) => (endpoint(point, true)?, endpoint(point, true)?),
        Cell::Between(lower, upper) => (endpoint(lower, false)?, endpoint(upper, false)?),
    };
    Ok(Some(QueryInterval { lower, upper }))
}

/// Convert a join cell to the half-open range interval for RangeExec's
/// zero-based Int32 `idx` values. This lets the source avoid generating rows
/// outside the cell; the normal FilterExec remains for other source types.
fn integer_range_cell(cell: &Cell) -> Result<Option<(usize, usize)>> {
    let value = |key: &[ArrayRef]| -> Result<Option<i64>> {
        let Some(array) = key.first() else {
            return Ok(None);
        };
        let Some(array) = array.as_any().downcast_ref::<Int32Array>() else {
            return Ok(None);
        };
        if array.is_null(0) {
            Ok(None)
        } else {
            Ok(Some(i64::from(array.value(0))))
        }
    };
    let (start, end) = match cell {
        Cell::Point(point) => {
            let Some(value) = value(point)? else {
                return Ok(None);
            };
            (value, value + 1)
        }
        Cell::Between(lower, upper) => {
            let (Some(lower), Some(upper)) = (value(lower)?, value(upper)?) else {
                return Ok(None);
            };
            (lower + 1, upper)
        }
    };
    let start = start.max(0) as usize;
    let end = end.max(0) as usize;
    Ok(Some((start.min(end), end)))
}

fn clipped_bounds_parts(
    parts: &[PhysicalPlan],
    bounds: &[Option<Bound>],
    candidates: &[usize],
    lower: &[ArrayRef],
    upper: &[ArrayRef],
    table: &TableRef,
    join_key: usize,
) -> Result<Vec<PhysicalPlan>> {
    let mut selected = Vec::new();
    for &index in candidates {
        let Some(part) = parts.get(index) else {
            return Err(TableError::Invalid(
                "join partition index out of range".into(),
            ));
        };
        let Some(bound) = bounds.get(index).and_then(Option::as_ref) else {
            continue;
        };
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

/// Static interval tree for partition bounds used by membership joins. Arrow
/// keys are sorted once with native comparators, then mapped to ordinal ranges
/// for the generic interval tree.
struct BoundIndex {
    endpoints: Vec<Vec<ArrayRef>>,
    tree: IntervalTree<usize, usize>,
}

impl BoundIndex {
    fn new(bounds: &[Option<Bound>]) -> Result<Self> {
        let endpoints = bounds
            .iter()
            .filter_map(Option::as_ref)
            .flat_map(|bound| [bound.first.clone(), bound.last.clone()])
            .collect::<Vec<_>>();
        let endpoints = sort_keys(endpoints)?;
        let mut intervals = Vec::new();
        for (partition, bound) in bounds.iter().enumerate() {
            let Some(bound) = bound else { continue };
            if compare_keys(&bound.first, &bound.last)? == Ordering::Greater {
                continue;
            }
            let start = endpoint_lower_bound(&endpoints, &bound.first)?;
            let end = endpoint_upper_bound(&endpoints, &bound.last)?;
            if start < end {
                intervals.push((start..end, partition));
            }
        }
        Ok(Self {
            endpoints,
            tree: intervals.into_iter().collect(),
        })
    }

    fn query(&self, lower: &[ArrayRef], upper: &[ArrayRef]) -> Result<Vec<usize>> {
        if self.endpoints.is_empty() {
            return Ok(Vec::new());
        }
        let start = endpoint_lower_bound(&self.endpoints, lower)?;
        let mut end = endpoint_upper_bound(&self.endpoints, upper)?;
        if start >= self.endpoints.len() {
            return Ok(Vec::new());
        }
        if start >= end {
            end = start + 1;
        }
        let mut result = self
            .tree
            .query(start..end)
            .map(|entry| entry.value)
            .collect::<Vec<_>>();
        result.sort_unstable();
        Ok(result)
    }
}

fn endpoint_lower_bound(endpoints: &[Vec<ArrayRef>], key: &[ArrayRef]) -> Result<usize> {
    let mut lower = 0;
    let mut upper = endpoints.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if compare_keys(&endpoints[middle], key)? == Ordering::Less {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    Ok(lower)
}

fn endpoint_upper_bound(endpoints: &[Vec<ArrayRef>], key: &[ArrayRef]) -> Result<usize> {
    let mut lower = 0;
    let mut upper = endpoints.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if compare_keys(&endpoints[middle], key)? != Ordering::Greater {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    Ok(lower)
}

fn overlaps(bound: &Bound, cell: &Cell) -> Result<bool> {
    match cell {
        Cell::Point(key) => Ok(compare_keys(&bound.first, key)? != Ordering::Greater
            && compare_keys(key, &bound.last)? != Ordering::Greater),
        Cell::Between(lower, upper) => Ok(compare_keys(&bound.last, lower)? == Ordering::Greater
            && compare_keys(&bound.first, upper)? == Ordering::Less),
    }
}

/// Route each disjoint point/gap cell to only the partition bounds that cover
/// it. Bounds may overlap arbitrarily (for example, when several partitions
/// share a key prefix), so the active set retains every matching partition.
fn candidates_by_cell(bounds: &[Option<Bound>], cells: &[Cell]) -> Result<Vec<Vec<usize>>> {
    let point_cells = cells
        .iter()
        .enumerate()
        .filter_map(|(cell_index, cell)| match cell {
            Cell::Point(value) => Some((cell_index, value)),
            Cell::Between(_, _) => None,
        })
        .collect::<Vec<_>>();
    if point_cells.is_empty() {
        return Ok(vec![Vec::new(); cells.len()]);
    }
    if bounds.iter().all(Option::is_none) {
        return Ok(vec![Vec::new(); cells.len()]);
    }
    let boundary_keys = point_cells
        .iter()
        .map(|(_, key)| (*key).clone())
        .collect::<Vec<_>>();
    let mut lookup_keys = Vec::new();
    let mut lookup_rows = vec![None; bounds.len()];
    for (partition, bound) in bounds.iter().enumerate() {
        if let Some(bound) = bound {
            let first = lookup_keys.len();
            lookup_keys.push(bound.first.clone());
            let last = lookup_keys.len();
            lookup_keys.push(bound.last.clone());
            lookup_rows[partition] = Some((first, last));
        }
    }
    let arity = boundary_keys[0].len();
    let boundary_columns = concat_key_columns(&boundary_keys, arity)?;
    let lookup_columns = concat_key_columns(&lookup_keys, arity)?;
    let comparators =
        make_typed_key_comparators(&boundary_columns, &lookup_columns, &key_sort_options(arity))
            .map_err(|error| TableError::Invalid(error.to_string()))?;
    let lookup_comparators =
        make_typed_key_comparators(&lookup_columns, &lookup_columns, &key_sort_options(arity))
            .map_err(|error| TableError::Invalid(error.to_string()))?;
    let mut starts = vec![Vec::new(); point_cells.len()];
    let mut ends = vec![Vec::new(); point_cells.len()];
    for (partition, bound) in bounds.iter().enumerate() {
        if bound.is_none() {
            continue;
        }
        let (first_row, last_row) = lookup_rows[partition].expect("bound has lookup rows");
        if compare_indexed_keys(
            &lookup_columns,
            &lookup_columns,
            &lookup_comparators,
            first_row,
            last_row,
        )? == Ordering::Greater
        {
            continue;
        }
        let start = boundary_index(
            point_cells.len(),
            &boundary_columns,
            &lookup_columns,
            &comparators,
            first_row,
        )?;
        let end = boundary_index(
            point_cells.len(),
            &boundary_columns,
            &lookup_columns,
            &comparators,
            last_row,
        )?;
        let (Some(start), Some(end)) = (start, end) else {
            return Err(TableError::Invalid(
                "partition bound endpoint is absent from join cells".into(),
            ));
        };
        if start <= end {
            starts[start].push(partition);
            ends[end].push(partition);
        }
    }
    let mut candidates = vec![Vec::new(); cells.len()];
    let mut active = std::collections::BTreeSet::new();
    for point in 0..point_cells.len() {
        active.extend(starts[point].iter().copied());
        candidates[point_cells[point].0] = active.iter().copied().collect();
        for partition in &ends[point] {
            active.remove(partition);
        }
        if let Some((next_cell, _)) = point_cells.get(point + 1) {
            let gap_cell = *next_cell - 1;
            candidates[gap_cell] = active.iter().copied().collect();
        }
    }
    Ok(candidates)
}

fn boundary_index(
    boundary_count: usize,
    boundaries: &[ArrayRef],
    keys: &[ArrayRef],
    comparators: &[arrow_ord::ord::DynComparator],
    key_row: usize,
) -> Result<Option<usize>> {
    let mut lower = 0;
    let mut upper = boundary_count;
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        match compare_indexed_keys(boundaries, keys, comparators, middle, key_row)? {
            Ordering::Less => lower = middle + 1,
            Ordering::Greater => upper = middle,
            Ordering::Equal => return Ok(Some(middle)),
        }
    }
    Ok(None)
}

fn concat_key_columns(keys: &[Vec<ArrayRef>], arity: usize) -> Result<Vec<ArrayRef>> {
    (0..arity)
        .map(|column| {
            let arrays = keys
                .iter()
                .map(|key| key[column].as_ref())
                .collect::<Vec<_>>();
            arrow_select::concat::concat(&arrays).map_err(TableError::from)
        })
        .collect()
}

fn compare_indexed_keys(
    left: &[ArrayRef],
    right: &[ArrayRef],
    comparators: &[arrow_ord::ord::DynComparator],
    left_row: usize,
    right_row: usize,
) -> Result<Ordering> {
    for (column, comparator) in comparators.iter().enumerate() {
        let equal_native_float = match left[column].data_type() {
            arrow_schema::DataType::Float32 => {
                let left_values = left[column]
                    .as_any()
                    .downcast_ref::<arrow_array::Float32Array>()
                    .unwrap();
                let right_values = right[column]
                    .as_any()
                    .downcast_ref::<arrow_array::Float32Array>()
                    .unwrap();
                !left_values.is_null(left_row)
                    && !right_values.is_null(right_row)
                    && left_values.value(left_row) == right_values.value(right_row)
            }
            arrow_schema::DataType::Float64 => {
                let left_values = left[column]
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap();
                let right_values = right[column]
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap();
                !left_values.is_null(left_row)
                    && !right_values.is_null(right_row)
                    && left_values.value(left_row) == right_values.value(right_row)
            }
            _ => false,
        };
        if equal_native_float {
            continue;
        }
        let ordering = comparator(left_row, right_row);
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn cells(left: &[Option<Bound>], right: &[Option<Bound>]) -> Result<Vec<Cell>> {
    let mut boundaries = left
        .iter()
        .chain(right)
        .filter_map(Option::as_ref)
        .flat_map(|bound| [bound.first.clone(), bound.last.clone()])
        .collect::<Vec<_>>();
    boundaries = sort_keys(boundaries)?;
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

/// Sort endpoint metadata in O(P log P), building Arrow's typed comparators
/// once for the whole sort. The comparator operates on the concatenated
/// one-row endpoint arrays, so it does not rebuild Arrow comparison state for
/// every comparison made by the sorting algorithm.
fn sort_keys(keys: Vec<Vec<ArrayRef>>) -> Result<Vec<Vec<ArrayRef>>> {
    if keys.len() < 2 {
        return Ok(keys);
    }
    let arity = keys[0].len();
    if keys.iter().any(|key| key.len() != arity) {
        return Err(TableError::Invalid("join key arity mismatch".into()));
    }
    let columns = (0..arity)
        .map(|column| {
            let arrays = keys
                .iter()
                .map(|key| key[column].as_ref())
                .collect::<Vec<_>>();
            arrow_select::concat::concat(&arrays).map_err(TableError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let comparators = make_typed_key_comparators(&columns, &columns, &key_sort_options(arity))
        .map_err(|error| TableError::Invalid(error.to_string()))?;
    let mut order = (0..keys.len()).collect::<Vec<_>>();
    order.sort_by(|&left, &right| {
        for (column, comparator) in comparators.iter().enumerate() {
            let equal_native_float = match columns[column].data_type() {
                arrow_schema::DataType::Float32 => {
                    let values = columns[column]
                        .as_any()
                        .downcast_ref::<arrow_array::Float32Array>()
                        .unwrap();
                    !values.is_null(left)
                        && !values.is_null(right)
                        && values.value(left) == values.value(right)
                }
                arrow_schema::DataType::Float64 => {
                    let values = columns[column]
                        .as_any()
                        .downcast_ref::<arrow_array::Float64Array>()
                        .unwrap();
                    !values.is_null(left)
                        && !values.is_null(right)
                        && values.value(left) == values.value(right)
                }
                _ => false,
            };
            if equal_native_float {
                continue;
            }
            let ordering = comparator(left, right);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
    Ok(order.into_iter().map(|index| keys[index].clone()).collect())
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
    fn endpoint_sort_uses_arrow_order_and_preserves_native_float_equality() {
        let sorted = sort_keys(vec![key(7), key(-2), key(4), key(0)]).unwrap();
        let values = sorted
            .iter()
            .map(
                |key| match ScalarValue::try_from_array(&key[0], 0).unwrap() {
                    ScalarValue::Int32(Some(value)) => value,
                    _ => panic!("expected Int32 key"),
                },
            )
            .collect::<Vec<_>>();
        assert_eq!(values, vec![-2, 0, 4, 7]);

        let sorted = sort_keys(vec![float_key(0.0), float_key(-0.0), float_key(1.0)]).unwrap();
        assert_eq!(
            compare_keys(&sorted[0], &sorted[1]).unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn cell_sweep_routes_overlapping_bounds_and_keeps_partition_order() {
        let bounds = vec![bound(1, 5), bound(3, 8), None, bound(5, 9), bound(1, 5)];
        let planned = cells(&bounds, &[]).unwrap();
        let candidates = candidates_by_cell(&bounds, &planned).unwrap();
        for (cell, actual) in planned.iter().zip(&candidates) {
            let expected = bounds
                .iter()
                .enumerate()
                .filter_map(|(index, bound)| {
                    bound
                        .as_ref()
                        .and_then(|bound| overlaps(bound, cell).unwrap().then_some(index))
                })
                .collect::<Vec<_>>();
            assert_eq!(*actual, expected);
        }
        let five_cell = planned
            .iter()
            .position(|cell| matches!(cell, Cell::Point(value) if compare_keys(value, &key(5)).unwrap() == Ordering::Equal))
            .unwrap();
        assert_eq!(candidates[five_cell], vec![0, 1, 3, 4]);
    }

    #[test]
    fn membership_bound_index_matches_brute_force_for_overlapping_prefixes() {
        let bounds = vec![bound(1, 5), bound(3, 8), None, bound(5, 9), bound(1, 5)];
        let index = BoundIndex::new(&bounds).unwrap();
        for (lower, upper) in [(0, 1), (2, 4), (5, 5), (7, 10), (10, 12)] {
            let actual = index.query(&key(lower), &key(upper)).unwrap();
            let expected = bounds
                .iter()
                .enumerate()
                .filter_map(|(partition, bound)| {
                    bound.as_ref().and_then(|bound| {
                        (compare_keys(&bound.last, &key(lower)).unwrap() != Ordering::Less
                            && compare_keys(&bound.first, &key(upper)).unwrap()
                                != Ordering::Greater)
                            .then_some(partition)
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
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

    #[test]
    fn integer_join_cells_clip_range_before_row_generation() {
        assert_eq!(
            integer_range_cell(&Cell::Point(key(8))).unwrap(),
            Some((8, 9))
        );
        assert_eq!(
            integer_range_cell(&Cell::Between(key(2), key(7))).unwrap(),
            Some((3, 7))
        );
        let table = TableIR::range(10, 3).unwrap();
        let clipped = plan::range_partition_clipped(&table, 1, 5, 6).unwrap();
        let debug = format!("{clipped:?}");
        assert!(debug.contains("start: 5"), "{debug}");
        assert!(debug.contains("end: 6"), "{debug}");
    }
}
