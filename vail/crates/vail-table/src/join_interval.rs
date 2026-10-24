//! Key-interval planning for independent Table join partitions.
//!
//! Bounds are metadata only. Certified Vortex fragments and Hail partition
//! intervals support direct reads; operators that may change keys fall back to a streaming pass
//! retaining just the first and last key of each prepared partition.
use crate::{
    Result, TableError, TableIR, TableJoinKind, TableNode, TableRef,
    partitioned::{OrderedConcatExec, PreparedTable},
    plan::{self, PhysicalPlan, column},
};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_plan::filter::FilterExec;
use futures::TryStreamExt;
use intervaltree::IntervalTree;
use std::cmp::Ordering;
use std::sync::Arc;
use vail_storage::fragment_index::{
    Endpoint, KeyValue, QueryInterval, encode_typed_key_component, normalize_typed_key_component,
};
use vail_storage::local_table::LocalRead;

#[derive(Clone, Debug)]
pub(crate) struct Bound {
    pub(crate) first: Vec<ArrayRef>,
    pub(crate) last: Vec<ArrayRef>,
}

#[derive(Clone, Debug)]
enum Cell {
    Point(Vec<ArrayRef>),
    Between(Vec<ArrayRef>, Vec<ArrayRef>),
    Span(Vec<ArrayRef>, Vec<ArrayRef>),
}

// A forwarding proof permits moving key demand below value-only projections.
// Scan expressions change with their input scope, so they are never crossed.
pub(crate) fn forwards_keys(
    table: &TableRef,
    child: &TableRef,
    fields: &[(String, crate::ScalarIR)],
    arity: usize,
) -> bool {
    if table.keys().len() < arity || child.keys().len() < arity {
        return false;
    }
    table
        .keys()
        .iter()
        .zip(child.keys())
        .take(arity)
        .all(|(out, input)| {
            let Some((_, expr)) = fields.iter().find(|(name, _)| name == out) else {
                return false;
            };
            match expr {
                crate::ScalarIR::Column { name } => name == input,
                crate::ScalarIR::Cast { value, dtype } => {
                    matches!(value.as_ref(),crate::ScalarIR::Column{name} if name==input)
                        && child
                            .schema()
                            .field_with_name(input)
                            .ok()
                            .and_then(|field| {
                                vail_execution::expressions::Type::from_arrow(field.data_type())
                                    .ok()
                            })
                            .as_ref()
                            == Some(dtype)
                }
                _ => false,
            }
        })
}
fn context_free(fields: &[(String, crate::ScalarIR)]) -> bool {
    fn visit(
        node: &crate::ScalarIR,
        seen: &mut std::collections::HashSet<*const crate::ScalarIR>,
    ) -> bool {
        if !seen.insert(node as *const _) {
            return true;
        }
        if matches!(
            node,
            crate::ScalarIR::Aggregate { .. } | crate::ScalarIR::AggregateFold { .. }
        ) || matches!(node,crate::ScalarIR::NestedFunction{function,..} if function.starts_with("random_"))
        {
            return false;
        }
        let mut safe = true;
        node.visit_children(&mut |child| {
            if safe {
                safe = visit(child, seen);
            }
        });
        safe
    }
    let mut seen = std::collections::HashSet::new();
    fields.iter().all(|(_, node)| visit(node, &mut seen))
}
// Checked source row ranges are prepared once, not recomputed per key cell.
struct ClipSource {
    source: TableRef,
    forwarding: Vec<TableRef>,
    ranges: Option<Vec<std::ops::Range<u64>>>,
}
impl ClipSource {
    fn new(table: &TableRef, arity: usize, parts: usize) -> Result<Option<Self>> {
        let mut source = table.clone();
        let mut forwarding = Vec::new();
        loop {
            let child = match source.node() {
                TableNode::Prepared { .. } => source.partition_metadata.clone(),
                TableNode::MapGlobals { child } if source.keys() == child.keys() => {
                    Some(child.clone())
                }
                TableNode::MapRows { child, fields, .. }
                    if forwards_keys(&source, child, fields, arity) && context_free(fields) =>
                {
                    Some(child.clone())
                }
                _ => None,
            };
            match child {
                Some(child) => {
                    if !matches!(source.node(), TableNode::Prepared { .. }) {
                        forwarding.push(source);
                    }
                    source = child;
                }
                None => break,
            }
        }
        let ranges = match source.node() {
            TableNode::Read { source } => {
                let rows = source.partition_rows();
                if rows.len() != parts {
                    return Ok(None);
                }
                let mut offset = 0u64;
                let mut ranges = Vec::with_capacity(rows.len());
                for rows in rows {
                    let end = offset.checked_add(*rows as u64).ok_or_else(|| {
                        TableError::Invalid("partition row range overflow".into())
                    })?;
                    ranges.push(offset..end);
                    offset = end;
                }
                Some(ranges)
            }
            TableNode::TextRead {
                paths,
                format,
                legacy: Some(read),
                ..
            } if format == "legacy" && read.matrix.is_none() && paths.len() == parts => None,
            TableNode::Range { n_partitions, .. } if *n_partitions == parts => None,
            _ => return Ok(None),
        };
        Ok(Some(Self {
            source,
            forwarding,
            ranges,
        }))
    }
    // Integer range clipping changes the generator's bounds, so it produces
    // exactly the requested interval. Indexed reads retain their residual.
    fn exact_range(&self, arity: usize) -> bool {
        arity == 1
            && matches!(self.source.node(), TableNode::Range { .. })
            && self.source.keys().first().is_some_and(|key| key == "idx")
    }
    fn clip(&self, index: usize, cell: &Cell, arity: usize) -> Result<Option<PhysicalPlan>> {
        let (lower, upper, lower_inclusive, upper_inclusive) = match cell {
            Cell::Point(point) => (point.as_slice(), point.as_slice(), true, true),
            Cell::Between(lower, upper) => (lower.as_slice(), upper.as_slice(), false, false),
            Cell::Span(lower, upper) => (lower.as_slice(), upper.as_slice(), true, true),
        };
        self.clip_span(index, lower, upper, lower_inclusive, upper_inclusive, arity)
    }
    fn clip_span(
        &self,
        index: usize,
        lower: &[ArrayRef],
        upper: &[ArrayRef],
        lower_inclusive: bool,
        upper_inclusive: bool,
        arity: usize,
    ) -> Result<Option<PhysicalPlan>> {
        let rows = match self.source.node() {
            TableNode::Read { source } => {
                let Some(query) = read_query_for_span(
                    &self.source,
                    source,
                    lower,
                    upper,
                    lower_inclusive,
                    upper_inclusive,
                    arity,
                )?
                else {
                    return Ok(None);
                };
                let range = self
                    .ranges
                    .as_ref()
                    .and_then(|ranges| ranges.get(index))
                    .ok_or_else(|| {
                        TableError::Invalid(
                            "clipped source partition exceeds prepared row ranges".into(),
                        )
                    })?;
                crate::interval_read::source_interval_in_rows(
                    &self.source,
                    source.clone(),
                    query,
                    range.clone(),
                )
            }
            TableNode::TextRead {
                format,
                legacy: Some(read),
                ..
            } if format == "legacy" && read.matrix.is_none() => {
                // Build only this file's scan. Resetting a PartitionView of the
                // full prepared source clones every file's plan per join cell.
                let scan = plan::source_partition(&self.source, index)?;
                let keys = key_expressions(&scan, &self.source.keys()[..arity])?;
                let bound_filter = |values: &[ArrayRef], op| -> Result<Arc<dyn PhysicalExpr>> {
                    if arity == 1
                        && values.len() == 1
                        && values[0].is_valid(0)
                        && matches!(
                            values[0].data_type(),
                            arrow_schema::DataType::Int32 | arrow_schema::DataType::Int64
                        )
                    {
                        return Ok(Arc::new(BinaryExpr::new(
                            keys[0].clone(),
                            op,
                            Arc::new(datafusion_physical_expr::expressions::Literal::new(
                                datafusion_common::ScalarValue::try_from_array(&values[0], 0)?,
                            )),
                        )));
                    }
                    Ok(crate::join_key::predicate(
                        &keys,
                        values,
                        op,
                        scan.schema().as_ref(),
                    )?)
                };
                let filters = vec![
                    bound_filter(
                        lower,
                        if lower_inclusive {
                            Operator::GtEq
                        } else {
                            Operator::Gt
                        },
                    )?,
                    bound_filter(
                        upper,
                        if upper_inclusive {
                            Operator::LtEq
                        } else {
                            Operator::Lt
                        },
                    )?,
                ];
                // Keep the normal residual below; these predicates also enable
                // the legacy index's integer-key seek and block pruning.
                plan::legacy_source_partition(&self.source, index, None, filters)?
            }
            TableNode::Range { .. }
                if arity == 1 && self.source.keys().first().is_some_and(|key| key == "idx") =>
            {
                let Some((lower, upper)) =
                    integer_range_span(lower, upper, lower_inclusive, upper_inclusive)?
                else {
                    return Ok(None);
                };
                plan::range_partition_clipped(&self.source, index, lower, upper)?
            }
            _ => return Ok(None),
        };
        let mut rows = rows;
        let mut parent = self.source.clone();
        for table in self.forwarding.iter().rev() {
            let child = TableIR::prepared(&parent, rows, None, 1);
            let node = match table.node() {
                TableNode::MapRows {
                    fields, bindings, ..
                } => TableNode::MapRows {
                    child,
                    fields: fields.clone(),
                    bindings: bindings.clone(),
                },
                TableNode::MapGlobals { .. } => TableNode::MapGlobals { child },
                _ => unreachable!("checked forwarding chain"),
            };
            let operation = TableIR::new(
                table.schema().clone(),
                table.keys().to_vec(),
                table.globals().clone(),
                node,
            );
            rows = plan::lower_operation(&operation)?.rows;
            parent = table.clone();
        }
        Ok(Some(rows))
    }
}

// Bound the working set of each independently executable range join and expose
// parallel work even when a generated source has just one original partition.
const MAX_RANGE_JOIN_ROWS: usize = 8 * 1024 * 1024;

fn full_range_source(table: &TableRef) -> Option<&TableRef> {
    match table.node() {
        TableNode::Range { .. } => Some(table),
        TableNode::Prepared {
            count: Some(count), ..
        } => {
            let source = table.partition_metadata.as_ref()?;
            // Clipped action-local inputs deliberately have no count. Only
            // bypass a prepared plan certified to represent the full source.
            (source.known_count() == Some(*count))
                .then(|| full_range_source(source))
                .flatten()
        }
        _ => None,
    }
}

fn add_range_split_points(table: &TableRef, join_key: usize, bounds: &mut Vec<Option<Bound>>) {
    if join_key != 1 || table.keys() != ["idx"] {
        return;
    }
    if let Some(source) = full_range_source(table)
        && let TableNode::Range { n, .. } = source.node()
    {
        for split in (MAX_RANGE_JOIN_ROWS..*n).step_by(MAX_RANGE_JOIN_ROWS) {
            let key = vec![Arc::new(Int32Array::from(vec![split as i32])) as ArrayRef];
            bounds.push(Some(Bound {
                first: key.clone(),
                last: key,
            }));
        }
    }
}

fn needs_range_join_splitting(table: &TableRef, join_key: usize) -> bool {
    if join_key != 1 || table.keys() != ["idx"] {
        return false;
    }
    full_range_source(table)
        .and_then(|source| match source.node() {
            TableNode::Range { n, .. } => Some(*n > MAX_RANGE_JOIN_ROWS),
            _ => None,
        })
        .unwrap_or(false)
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
    let left_input = crate::partitioned::prepared_input(left)?;
    let right_input = crate::partitioned::prepared_input(right)?;
    let left_bounds = bounds(left, &left_input, join_key, true).await?;
    let right_bounds = bounds(right, &right_input, join_key, true).await?;
    if kind == TableJoinKind::Inner
        && join_key > 0
        && !needs_range_join_splitting(left, join_key)
        && !needs_range_join_splitting(right, join_key)
        && sorted_disjoint(&left_bounds)?
        && sorted_disjoint(&right_bounds)?
    {
        return prepare_disjoint_inner(
            output,
            left,
            right,
            join_key,
            &left_input,
            &right_input,
            &left_bounds,
            &right_bounds,
        );
    }
    let left_clip = ClipSource::new(left, join_key, left_input.parts.len())?;
    let right_clip = ClipSource::new(right, join_key, right_input.parts.len())?;
    let mut boundaries = left_bounds.clone();
    add_range_split_points(left, join_key, &mut boundaries);
    add_range_split_points(right, join_key, &mut boundaries);
    let cells = cells(&boundaries, &right_bounds)?;

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
            left_clip.as_ref(),
            true,
        )?;
        let right_parts = clipped_parts(
            &right_input.parts,
            &right_bounds,
            &right_candidates[index],
            cell,
            right,
            join_key,
            right_clip.as_ref(),
            true,
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

/// Use one join plan for each intersecting pair of certified disjoint input
/// partitions. The generic cell planner splits each pair at every endpoint,
/// creating several plans even when each key belongs to one partition on either side.
fn prepare_disjoint_inner(
    output: &TableRef,
    left: &TableRef,
    right: &TableRef,
    join_key: usize,
    left_input: &PreparedTable,
    right_input: &PreparedTable,
    left_bounds: &[Option<Bound>],
    right_bounds: &[Option<Bound>],
) -> Result<PreparedTable> {
    let left_clip = ClipSource::new(left, join_key, left_input.parts.len())?;
    let right_clip = ClipSource::new(right, join_key, right_input.parts.len())?;
    let mut pairs = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < left_bounds.len() && j < right_bounds.len() {
        let Some(left_bound) = left_bounds[i].as_ref() else {
            i += 1;
            continue;
        };
        let Some(right_bound) = right_bounds[j].as_ref() else {
            j += 1;
            continue;
        };
        let lower = if compare_keys(&left_bound.first, &right_bound.first)? == Ordering::Less {
            right_bound.first.clone()
        } else {
            left_bound.first.clone()
        };
        let upper = if compare_keys(&left_bound.last, &right_bound.last)? == Ordering::Greater {
            right_bound.last.clone()
        } else {
            left_bound.last.clone()
        };
        if compare_keys(&lower, &upper)? != Ordering::Greater {
            pairs.push((i, j, Cell::Span(lower, upper)));
        }
        match compare_keys(&left_bound.last, &right_bound.last)? {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }

    let mut parts = Vec::with_capacity(pairs.len());
    for (left_index, right_index, span) in pairs {
        let left_parts = clipped_parts(
            &left_input.parts,
            left_bounds,
            &[left_index],
            &span,
            left,
            join_key,
            left_clip.as_ref(),
            true,
        )?;
        let right_parts = clipped_parts(
            &right_input.parts,
            right_bounds,
            &[right_index],
            &span,
            right,
            join_key,
            right_clip.as_ref(),
            true,
        )?;
        if left_parts.is_empty() || right_parts.is_empty() {
            continue;
        }
        let left_rows = concat_cell(left, left_parts)?;
        let right_rows = concat_cell(right, right_parts)?;
        let left_local = TableIR::prepared(left, left_rows, None, 1);
        let right_local = TableIR::prepared(right, right_rows, None, 1);
        parts.push(
            crate::joins::lower(
                output,
                &left_local,
                &right_local,
                TableJoinKind::Inner,
                join_key,
            )?
            .rows,
        );
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

fn sorted_disjoint(bounds: &[Option<Bound>]) -> Result<bool> {
    let mut previous: Option<&Bound> = None;
    for bound in bounds.iter().flatten() {
        if let Some(previous) = previous {
            if compare_keys(&previous.last, &bound.first)? != Ordering::Less {
                return Ok(false);
            }
        }
        previous = Some(bound);
    }
    Ok(true)
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
    let left_input = crate::partitioned::prepared_input(left)?;
    let right_input = crate::partitioned::prepared_input(right)?;
    let left_bounds = bounds(left, &left_input, join_key, true).await?;
    let right_bounds = bounds(right, &right_input, join_key, true).await?;
    let right_clip = ClipSource::new(right, join_key, right_input.parts.len())?;
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
            right_clip.as_ref(),
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
    Ok(Arc::new(OrderedConcatExec::tagged(
        parts,
        schema.clone(),
        schema.fields().len() - 1,
    )))
}

/// Reuse the same key cells for multi-way joins, preserving one independently
/// executable output plan per interval.
struct ZipInput {
    table: TableRef,
    prepared: PreparedTable,
    bounds: Vec<Option<Bound>>,
    clip: Option<ClipSource>,
    owner: PhysicalPlan,
}
pub(crate) async fn prepare_zip(
    output: &TableRef,
    tables: &[TableRef],
    data_field_name: &str,
) -> Result<PreparedTable> {
    let join_key = output.keys().len();
    let budget = Arc::new(crate::zip_snapshot::Budget::default());
    let mut unique = std::collections::HashMap::<usize, Arc<ZipInput>>::new();
    let mut inputs = Vec::with_capacity(tables.len());
    let mut all_bounds = Vec::new();
    for table in tables {
        let id = Arc::as_ptr(table) as usize;
        let input = if let Some(input) = unique.get(&id) {
            input.clone()
        } else {
            let mut prepared = crate::partitioned::prepared_input(table)?;
            let clip = ClipSource::new(table, join_key, prepared.parts.len())?;
            let owner = crate::zip_snapshot::source(
                prepared.physical_plan(),
                (join_key == 0).then_some(1),
                budget.clone(),
            );
            prepared.parts = (0..prepared.parts.len())
                .map(|index| {
                    Arc::new(crate::partitioned::PartitionViewExec::new(
                        owner.clone(),
                        index,
                    )) as PhysicalPlan
                })
                .collect();
            // Endpoint discovery and payload branches consume the same action
            // snapshot. Resetting between them recursively replays nested zips.
            let bounds = bounds(table, &prepared, join_key, false).await?;
            let input = Arc::new(ZipInput {
                table: table.clone(),
                prepared,
                bounds,
                clip,
                owner,
            });
            all_bounds.extend(input.bounds.iter().cloned());
            unique.insert(id, input.clone());
            input
        };
        inputs.push(input);
    }
    let cells = cells(&all_bounds, &[])?;
    let candidates = inputs
        .iter()
        .map(|input| candidates_by_cell(&input.bounds, &cells))
        .collect::<Result<Vec<_>>>()?;
    let mut parts = Vec::with_capacity(cells.len());
    for (cell_index, cell) in cells.iter().enumerate() {
        let mut local = std::collections::HashMap::<usize, TableRef>::new();
        let mut local_tables = Vec::with_capacity(inputs.len());
        let mut any = false;
        for (index, input) in inputs.iter().enumerate() {
            let id = Arc::as_ptr(input) as usize;
            let table = if let Some(table) = local.get(&id) {
                table.clone()
            } else {
                let selected = clipped_parts(
                    &input.prepared.parts,
                    &input.bounds,
                    &candidates[index][cell_index],
                    cell,
                    &input.table,
                    join_key,
                    input.clip.as_ref(),
                    false,
                )?;
                any |= !selected.is_empty();
                let rows = if selected.is_empty() {
                    empty_like(&input.prepared.schema)?
                } else {
                    concat_cell(&input.table, selected)?
                };
                let rows: PhysicalPlan = if join_key == 0 {
                    Arc::new(datafusion_physical_plan::limit::GlobalLimitExec::new(
                        rows,
                        0,
                        Some(1),
                    ))
                } else {
                    rows
                };
                let rows = crate::zip_snapshot::cell(rows, input.owner.clone())?;
                let table = TableIR::prepared(&input.table, rows, None, 1);
                local.insert(id, table.clone());
                table
            };
            local_tables.push(table);
        }
        if any {
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
    clip: Option<&ClipSource>,
    fresh: bool,
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
            selected.push(if fresh {
                reset(part.clone())?
            } else {
                part.clone()
            });
            continue;
        }
        let clipped = clip
            .map(|clip| clip.clip(index, cell, join_key))
            .transpose()?
            .flatten();
        let exact = clipped.is_some() && clip.is_some_and(|clip| clip.exact_range(join_key));
        let rows = match clipped {
            Some(rows) => rows,
            None => {
                if fresh {
                    reset(part.clone())?
                } else {
                    part.clone()
                }
            }
        };
        // Bounds certify all rows of this partition. A cell containing that
        // entire interval needs no residual row-by-row key predicate.
        if exact || contains_bound(cell, bound)? {
            selected.push(rows);
            continue;
        }
        let key = key_expressions(&rows, &table.keys()[..join_key])?;

        let predicate = match cell {
            Cell::Point(value) => {
                crate::join_key::predicate(&key, value, Operator::Eq, rows.schema().as_ref())?
            }
            Cell::Between(lower, upper) => Arc::new(BinaryExpr::new(
                crate::join_key::predicate(&key, lower, Operator::Gt, rows.schema().as_ref())?,
                Operator::And,
                crate::join_key::predicate(&key, upper, Operator::Lt, rows.schema().as_ref())?,
            )) as Arc<dyn PhysicalExpr>,
            Cell::Span(lower, upper) => Arc::new(BinaryExpr::new(
                crate::join_key::predicate(&key, lower, Operator::GtEq, rows.schema().as_ref())?,
                Operator::And,
                crate::join_key::predicate(&key, upper, Operator::LtEq, rows.schema().as_ref())?,
            )) as Arc<dyn PhysicalExpr>,
        };
        selected.push(Arc::new(FilterExec::try_new(predicate, rows)?) as PhysicalPlan);
    }
    Ok(selected)
}

fn contains_bound(cell: &Cell, bound: &Bound) -> Result<bool> {
    Ok(match cell {
        Cell::Point(key) => {
            compare_keys(&bound.first, key)? == Ordering::Equal
                && compare_keys(&bound.last, key)? == Ordering::Equal
        }
        Cell::Between(lower, upper) => {
            compare_keys(&bound.first, lower)? == Ordering::Greater
                && compare_keys(&bound.last, upper)? == Ordering::Less
        }
        Cell::Span(lower, upper) => {
            compare_keys(&bound.first, lower)? != Ordering::Less
                && compare_keys(&bound.last, upper)? != Ordering::Greater
        }
    })
}

fn read_query_for_span(
    table: &TableRef,
    source: &LocalRead,
    lower: &[ArrayRef],
    upper: &[ArrayRef],
    lower_inclusive: bool,
    upper_inclusive: bool,
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
    let lower = endpoint(lower, lower_inclusive)?;
    let upper = endpoint(upper, upper_inclusive)?;
    Ok(Some(QueryInterval { lower, upper }))
}

/// Convert a join cell to the half-open range interval for RangeExec's
/// zero-based Int32 `idx` values. This lets the source avoid generating rows
/// outside the cell; the normal FilterExec remains for other source types.
fn integer_range_span(
    lower: &[ArrayRef],
    upper: &[ArrayRef],
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> Result<Option<(usize, usize)>> {
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
    let (Some(lower), Some(upper)) = (value(lower)?, value(upper)?) else {
        return Ok(None);
    };
    let start = lower + i64::from(!lower_inclusive);
    let end = upper + i64::from(upper_inclusive);
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
    clip: Option<&ClipSource>,
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
            let clipped = clip
                .map(|clip| clip.clip_span(index, lower, upper, true, true, join_key))
                .transpose()?
                .flatten();
            let exact = clipped.is_some() && clip.is_some_and(|clip| clip.exact_range(join_key));
            let rows = match clipped {
                Some(rows) => rows,
                None => reset(part.clone())?,
            };
            if exact
                || (compare_keys(&bound.first, lower)? != Ordering::Less
                    && compare_keys(&bound.last, upper)? != Ordering::Greater)
            {
                selected.push(rows);
                continue;
            }
            let key = key_expressions(&rows, &table.keys()[..join_key])?;
            let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                crate::join_key::predicate(&key, lower, Operator::GtEq, rows.schema().as_ref())?,
                Operator::And,
                crate::join_key::predicate(&key, upper, Operator::LtEq, rows.schema().as_ref())?,
            ));
            selected.push(Arc::new(FilterExec::try_new(predicate, rows)?) as PhysicalPlan);
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
        Cell::Span(lower, upper) => Ok(compare_keys(&bound.last, lower)? != Ordering::Less
            && compare_keys(&bound.first, upper)? != Ordering::Greater),
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
            Cell::Between(_, _) | Cell::Span(_, _) => None,
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
        crate::join_key::comparators(&boundary_columns, &lookup_columns, &key_sort_options(arity))
            .map_err(|error| TableError::Invalid(error.to_string()))?;
    let lookup_comparators =
        crate::join_key::comparators(&lookup_columns, &lookup_columns, &key_sort_options(arity))
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
    _left: &[ArrayRef],
    _right: &[ArrayRef],
    comparators: &[arrow_ord::ord::DynComparator],
    left_row: usize,
    right_row: usize,
) -> Result<Ordering> {
    for comparator in comparators {
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
    let comparators = crate::join_key::comparators(&columns, &columns, &key_sort_options(arity))
        .map_err(|error| TableError::Invalid(error.to_string()))?;
    let mut order = (0..keys.len()).collect::<Vec<_>>();
    order.sort_by(|&left, &right| {
        for comparator in &comparators {
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
    fresh: bool,
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
    scan_bounds(prepared, &table.keys()[..join_key], fresh).await
}

pub(crate) fn inherited_bounds(
    table: &TableRef,
    join_key: usize,
) -> Result<Option<Vec<Option<Bound>>>> {
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
        TableNode::TextRead {
            paths,
            format,
            legacy: Some(read),
            ..
        } if format == "legacy"
            && read.matrix.is_none()
            && join_key == 1
            && table.keys().len() == 1 =>
        {
            // Hail intervals may overestimate actual endpoint rows. Treat them
            // as conservative envelopes; clipping keeps its residual predicate.
            let component = &read.component;
            let Some(counts) = read.counts.as_ref() else {
                return Ok(None);
            };
            if component.int_key_bounds.len() != paths.len() || counts.len() != paths.len() {
                return Ok(None);
            }
            let name = &table.keys()[0];
            let dtype = table.schema().field_with_name(name)?.data_type();
            let endpoint = |value: i64| -> Option<ArrayRef> {
                match dtype {
                    arrow_schema::DataType::Int32 => {
                        Some(Arc::new(Int32Array::from(vec![i32::try_from(value).ok()?])))
                    }
                    arrow_schema::DataType::Int64 => Some(Arc::new(Int64Array::from(vec![value]))),
                    _ => None,
                }
            };
            let mut result = Vec::with_capacity(paths.len());
            for (bound, count) in component.int_key_bounds.iter().zip(counts) {
                if *count == 0 {
                    result.push(None);
                    continue;
                }
                // Missing bounds mean unknown, not an empty partition. Fall
                // back to endpoint discovery for null/unsupported keys.
                let Some(bound) = bound.as_ref().filter(|bound| &bound.key == name) else {
                    return Ok(None);
                };
                let (Some(first), Some(last)) = (endpoint(bound.min), endpoint(bound.max)) else {
                    return Ok(None);
                };
                result.push(Some(Bound {
                    first: vec![first],
                    last: vec![last],
                }));
            }
            Ok(Some(result))
        }
        // A range table's source partitions are exact, contiguous Int32 key
        // intervals. Reuse those known bounds instead of encoding every row
        // just to rediscover each partition's endpoints.
        TableNode::Range {
            n, n_partitions, ..
        } if join_key == 1 && table.keys().first().is_some_and(|key| key == "idx") => {
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
            if forwards_keys(table, child, fields, join_key) =>
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

async fn scan_bounds(
    prepared: &PreparedTable,
    keys: &[String],
    fresh: bool,
) -> Result<Vec<Option<Bound>>> {
    let action = if fresh {
        prepared.start_action()?
    } else {
        prepared.physical_plan()
    };
    let indices = keys
        .iter()
        .map(|name| action.schema().index_of(name).map_err(TableError::from))
        .collect::<Result<Vec<_>>>()?;
    let parts = (0..prepared.parts.len())
        .map(|index| {
            Arc::new(crate::partitioned::PartitionViewExec::new(
                action.clone(),
                index,
            )) as PhysicalPlan
        })
        .collect();
    let mut streams =
        crate::partitioned::ordered_partition_streams(parts, Arc::new(TaskContext::default()));
    let mut result = Vec::with_capacity(prepared.parts.len());
    use futures::StreamExt;
    while let Some((_, mut stream)) = streams.next().await {
        let mut first = None;
        let mut last = None;
        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            // Copy only endpoint key values; a one-row slice would retain the
            // entire reader batch and its nested children across partitions.
            let row_key = |row: usize| -> Result<Vec<ArrayRef>> {
                let row = arrow_array::UInt64Array::from(vec![row as u64]);
                indices
                    .iter()
                    .map(|index| {
                        Ok(arrow_select::take::take(
                            batch.column(*index).as_ref(),
                            &row,
                            None,
                        )?)
                    })
                    .collect()
            };
            if first.is_none() {
                first = Some(row_key(0)?);
            }
            last = Some(row_key(batch.num_rows() - 1)?);
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
    if left.len() != right.len() || left.iter().chain(right).any(|key| key.len() != 1) {
        return Err(TableError::Invalid(
            "join key arity or endpoint cardinality mismatch".into(),
        ));
    }
    let comparators = crate::join_key::comparators(left, right, &key_sort_options(left.len()))?;
    Ok(comparators
        .iter()
        .map(|compare| compare(0, 0))
        .find(|order| !order.is_eq())
        .unwrap_or(Ordering::Equal))
}

fn key_expressions(input: &PhysicalPlan, keys: &[String]) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    keys.iter()
        .map(|name| {
            let index = input.schema().index_of(name)?;
            Ok(column(input, index))
        })
        .collect()
}
