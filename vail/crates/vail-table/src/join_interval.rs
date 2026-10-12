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
use arrow_array::Array;
use arrow_schema::SchemaRef;
use datafusion_common::{ScalarValue, config::ConfigOptions};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::{
    PhysicalExpr, ScalarFunctionExpr,
    expressions::{BinaryExpr, Literal},
};
use datafusion_physical_plan::filter::FilterExec;
use futures::TryStreamExt;
use std::sync::Arc;
use vail_storage::fragment_index::{CompositeKey, KeyValue};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Bound {
    first: Vec<u8>,
    last: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Cell {
    Point(Vec<u8>),
    Between(Vec<u8>, Vec<u8>),
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
    let cells = cells(&left_bounds, &right_bounds);
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
    let cells = cells(&all_bounds, &[]);
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
        if !overlaps(bound, cell) {
            continue;
        }
        if join_key == 0 {
            selected.push(reset(part.clone())?);
            continue;
        }
        let key = key_expression(part, &table.keys()[..join_key])?;
        let predicate = match cell {
            Cell::Point(value) => compare(key, Operator::Eq, value.clone()),
            Cell::Between(lower, upper) => Arc::new(BinaryExpr::new(
                compare(key.clone(), Operator::Gt, lower.clone()),
                Operator::And,
                compare(key, Operator::Lt, upper.clone()),
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
    lower: &[u8],
    upper: &[u8],
    table: &TableRef,
    join_key: usize,
) -> Result<Vec<PhysicalPlan>> {
    let mut selected = Vec::new();
    for (part, bound) in parts.iter().zip(bounds) {
        let Some(bound) = bound else { continue };
        if bound.last.as_slice() < lower || bound.first.as_slice() > upper {
            continue;
        }
        if join_key == 0 {
            selected.push(reset(part.clone())?);
        } else {
            let key = key_expression(part, &table.keys()[..join_key])?;
            let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                compare(key.clone(), Operator::GtEq, lower.to_vec()),
                Operator::And,
                compare(key, Operator::LtEq, upper.to_vec()),
            ));
            selected.push(
                Arc::new(FilterExec::try_new(predicate, reset(part.clone())?)?) as PhysicalPlan,
            );
        }
    }
    Ok(selected)
}

fn compare(key: Arc<dyn PhysicalExpr>, op: Operator, value: Vec<u8>) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        key,
        op,
        Arc::new(Literal::new(ScalarValue::Binary(Some(value)))),
    ))
}

fn overlaps(bound: &Bound, cell: &Cell) -> bool {
    match cell {
        Cell::Point(key) => {
            bound.first.as_slice() <= key.as_slice() && key.as_slice() <= bound.last.as_slice()
        }
        Cell::Between(lower, upper) => {
            bound.last.as_slice() > lower.as_slice() && bound.first.as_slice() < upper.as_slice()
        }
    }
}

fn cells(left: &[Option<Bound>], right: &[Option<Bound>]) -> Vec<Cell> {
    let mut boundaries = left
        .iter()
        .chain(right)
        .filter_map(Option::as_ref)
        .flat_map(|bound| [bound.first.clone(), bound.last.clone()])
        .collect::<Vec<_>>();
    boundaries.sort();
    boundaries.dedup();
    let mut result = Vec::with_capacity(boundaries.len().saturating_mul(2));
    for (index, boundary) in boundaries.iter().enumerate() {
        result.push(Cell::Point(boundary.clone()));
        if let Some(next) = boundaries.get(index + 1) {
            if boundary < next {
                result.push(Cell::Between(boundary.clone(), next.clone()));
            }
        }
    }
    result
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
            let source_bounds = source.partition_key_bounds()?;
            let mut result = Vec::with_capacity(source_bounds.len());
            for pair in source_bounds {
                result.push(
                    pair.map(|(first, last)| -> Result<Bound> {
                        Ok(Bound {
                            first: encode_prefix(&first, join_key)?,
                            last: encode_prefix(&last, join_key)?,
                        })
                    })
                    .transpose()?,
                );
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
        let expr = key_expression(part, keys)?;
        let mut stream = reset(part.clone())?.execute(0, Arc::new(TaskContext::default()))?;
        let mut first: Option<Vec<u8>> = None;
        let mut last: Option<Vec<u8>> = None;
        while let Some(batch) = stream.try_next().await? {
            let encoded = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
            let encoded = encoded
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .ok_or_else(|| TableError::Invalid("join canonical key is not binary".into()))?;
            for row in 0..encoded.len() {
                let key = encoded.value(row).to_vec();
                if let Some(previous) = &last {
                    if previous > &key {
                        return Err(TableError::Invalid(
                            "join input partition is not sorted by its key".into(),
                        ));
                    }
                }
                first.get_or_insert_with(|| key.clone());
                last = Some(key);
            }
        }
        result.push(first.zip(last).map(|(first, last)| Bound { first, last }));
    }
    Ok(result)
}

fn reset(plan: PhysicalPlan) -> Result<PhysicalPlan> {
    Ok(datafusion_physical_plan::execution_plan::reset_plan_states(
        plan,
    )?)
}

fn key_expression(input: &PhysicalPlan, keys: &[String]) -> Result<Arc<dyn PhysicalExpr>> {
    let mut args = Vec::<Arc<dyn PhysicalExpr>>::new();
    for (index, name) in keys.iter().enumerate() {
        args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(format!(
            "__key_{index}"
        ))))));
        args.push(column(input, input.schema().index_of(name)?));
    }
    Ok(vail_execution::ordering::canonical_key(
        Arc::new(ScalarFunctionExpr::try_new(
            datafusion_functions::core::named_struct(),
            args,
            &input.schema(),
            Arc::new(ConfigOptions::default()),
        )?) as Arc<dyn PhysicalExpr>,
    ))
}

fn encode_prefix(key: &CompositeKey, length: usize) -> Result<Vec<u8>> {
    if key.len() < length {
        return Err(TableError::Invalid(
            "indexed key has fewer fields than join prefix".into(),
        ));
    }
    let mut encoded = vec![1];
    for value in &key[..length] {
        match value {
            KeyValue::Missing => encoded.push(2),
            KeyValue::Canonical(bytes) => encoded.extend_from_slice(bytes),
            KeyValue::Int32(value) => {
                encoded.extend_from_slice(&encode_scalar(ScalarValue::Int32(Some(*value)))?)
            }
            KeyValue::Int64(value) => {
                encoded.extend_from_slice(&encode_scalar(ScalarValue::Int64(Some(*value)))?)
            }
            KeyValue::Bool(value) => {
                encoded.extend_from_slice(&encode_scalar(ScalarValue::Boolean(Some(*value)))?)
            }
            KeyValue::AsciiString(value) => {
                encoded.extend_from_slice(&encode_scalar(ScalarValue::Utf8(Some(value.clone())))?)
            }
        }
    }
    Ok(encoded)
}

fn encode_scalar(value: ScalarValue) -> Result<Vec<u8>> {
    let array = value.to_array_of_size(1)?;
    Ok(vail_ordering::encode_typed_value(array.as_ref(), 0, None)
        .map_err(|e| TableError::Invalid(e.to_string()))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cells_are_disjoint_points_and_open_gaps() {
        let left = vec![Some(Bound {
            first: b"a".to_vec(),
            last: b"m".to_vec(),
        })];
        let right = vec![Some(Bound {
            first: b"m".to_vec(),
            last: b"z".to_vec(),
        })];
        assert_eq!(
            cells(&left, &right),
            vec![
                Cell::Point(b"a".to_vec()),
                Cell::Between(b"a".to_vec(), b"m".to_vec()),
                Cell::Point(b"m".to_vec()),
                Cell::Between(b"m".to_vec(), b"z".to_vec()),
                Cell::Point(b"z".to_vec()),
            ]
        );
        assert!(overlaps(
            &left[0].clone().unwrap(),
            &Cell::Point(b"m".to_vec())
        ));
        assert!(!overlaps(
            &left[0].clone().unwrap(),
            &Cell::Between(b"m".to_vec(), b"z".to_vec())
        ));
    }

    #[test]
    fn missing_bounds_and_duplicate_prefixes_keep_a_single_boundary_cell() {
        let left = vec![
            Some(Bound {
                first: vec![1, 1],
                last: vec![1, 2],
            }),
            Some(Bound {
                first: vec![1, 2],
                last: vec![1, 3],
            }),
        ];
        let right = vec![
            None,
            Some(Bound {
                first: vec![1, 2],
                last: vec![1, 4],
            }),
        ];
        let planned = cells(&left, &right);
        assert_eq!(
            planned
                .iter()
                .filter(|c| **c == Cell::Point(vec![1, 2]))
                .count(),
            1
        );
        assert!(planned.iter().all(|c| match c {
            Cell::Between(a, b) => a < b,
            _ => true,
        }));
        assert_eq!(cells(&[None], &[None]), Vec::<Cell>::new());
    }
}
