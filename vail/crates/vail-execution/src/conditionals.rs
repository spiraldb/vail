//! Flat, lazy switch evaluation with one selector value per input row.
use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, UInt64Array, new_empty_array,
    new_null_array,
};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Capture expressions are the only children exposed to an enclosing lambda's
/// column compaction. Callbacks use the reconstructed original frame plus its
/// private selector column, so their lexical slot indices cannot be rewritten
/// as if the selector were an outer variable.
#[derive(Debug)]
pub(crate) struct ScopedSwitch {
    pub inputs: Vec<Arc<dyn PhysicalExpr>>,
    pub value: Arc<dyn PhysicalExpr>,
    pub cases: Vec<(Callback, Callback)>,
    pub default: Callback,
    pub missing: Option<Callback>,
    pub input_schema: SchemaRef,
    pub bound_schema: SchemaRef,
    pub dtype: DataType,
}
impl PartialEq for ScopedSwitch {
    fn eq(&self, other: &Self) -> bool {
        self.inputs == other.inputs
            && self.value.eq(&other.value)
            && self.cases == other.cases
            && self.default.eq(&other.default)
            && self.missing == other.missing
            && self.input_schema == other.input_schema
            && self.bound_schema == other.bound_schema
            && self.dtype == other.dtype
    }
}
impl Eq for ScopedSwitch {}
impl Hash for ScopedSwitch {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inputs.hash(state);
        self.value.hash(state);
        self.cases.hash(state);
        self.default.hash(state);
        self.missing.hash(state);
        self.input_schema.hash(state);
        self.bound_schema.hash(state);
        self.dtype.hash(state);
    }
}
impl std::fmt::Display for ScopedSwitch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScopedSwitch")
    }
}

/// A callback's exact captured fields are known before row selection. Only
/// these columns are gathered; unused source text/nested payloads stay borrowed
/// in the switch frame instead of being copied for each comparison.
#[derive(Debug, Clone)]
pub(crate) struct Callback {
    pub expr: Arc<dyn PhysicalExpr>,
    pub indices: Vec<usize>,
    pub schema: SchemaRef,
}
impl PartialEq for Callback {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.indices == other.indices && self.schema == other.schema
    }
}
impl Eq for Callback {}
impl Hash for Callback {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.indices.hash(state);
        self.schema.hash(state);
    }
}
impl Callback {
    fn evaluate(&self, batch: &RecordBatch, rows: &[usize]) -> Result<ArrayRef> {
        // Selections retain original row order, so a full-length selection is
        // exactly the identity and shares all captured Arrow buffers.
        let indices = (rows.len() != batch.num_rows())
            .then(|| UInt64Array::from_iter_values(rows.iter().map(|&row| row as u64)));
        let columns = self
            .indices
            .iter()
            .map(|&index| {
                let column = batch.column(index);
                match &indices {
                    None => Ok(column.clone()),
                    Some(indices) => Ok(arrow_select::take::take(column.as_ref(), indices, None)?),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let input = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )?;
        let output = self.expr.evaluate(&input)?.into_array(rows.len())?;
        if output.len() != rows.len() {
            return exec_err!("switch callback output length differs from selected rows");
        }
        Ok(output)
    }
}

/// Each selected row is assigned once to an output piece. The mapping restores
/// original order in a single interleave; unassigned rows remain null.
fn evaluate_selected(
    expr: &Callback,
    rows: &[usize],
    batch: &RecordBatch,
    pieces: &mut Vec<ArrayRef>,
    mapping: &mut [Option<(usize, usize)>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let output = expr.evaluate(batch, rows)?;
    for (i, &row) in rows.iter().enumerate() {
        mapping[row] = Some((pieces.len(), i));
    }
    pieces.push(output);
    Ok(())
}
impl PhysicalExpr for ScopedSwitch {
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
    fn data_type(&self, _: &arrow_schema::Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, _: &arrow_schema::Schema) -> Result<bool> {
        Ok(true)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inputs.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.inputs.len() {
            return internal_err!("scoped switch capture count changed");
        }
        Ok(Arc::new(Self {
            inputs: children,
            value: self.value.clone(),
            cases: self.cases.clone(),
            default: self.default.clone(),
            missing: self.missing.clone(),
            input_schema: self.input_schema.clone(),
            bound_schema: self.bound_schema.clone(),
            dtype: self.dtype.clone(),
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let len = batch.num_rows();
        if len == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.dtype)));
        }
        let mut columns = self
            .inputs
            .iter()
            .map(|input| input.evaluate(batch)?.into_array(len))
            .collect::<Result<Vec<_>>>()?;
        let input = RecordBatch::try_new_with_options(
            self.input_schema.clone(),
            columns.clone(),
            &RecordBatchOptions::new().with_row_count(Some(len)),
        )?;
        let value = self.value.evaluate(&input)?.into_array(len)?;
        columns.push(value.clone());
        let bound = RecordBatch::try_new_with_options(
            self.bound_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(len)),
        )?;
        let mut pending = (0..len).collect::<Vec<_>>();
        let mut pieces = Vec::new();
        let mut mapping = vec![None; len];
        if let Some(missing) = &self.missing {
            let absent = pending
                .iter()
                .copied()
                .filter(|&row| value.is_null(row))
                .collect::<Vec<_>>();
            evaluate_selected(missing, &absent, &bound, &mut pieces, &mut mapping)?;
            pending.retain(|&row| !value.is_null(row));
        }
        for (predicate, result) in &self.cases {
            if pending.is_empty() {
                break;
            }
            let predicate = predicate.evaluate(&bound, &pending)?;
            let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
                return exec_err!("switch predicate must be Boolean");
            };
            let mut matched = Vec::new();
            let mut unresolved = Vec::new();
            for (i, &row) in pending.iter().enumerate() {
                // Missing comparisons stop at this case and produce a null;
                // only false comparisons proceed to a later case or default.
                if !predicate.is_null(i) {
                    if predicate.value(i) {
                        matched.push(row);
                    } else {
                        unresolved.push(row);
                    }
                }
            }
            evaluate_selected(result, &matched, &bound, &mut pieces, &mut mapping)?;
            pending = unresolved;
        }
        evaluate_selected(&self.default, &pending, &bound, &mut pieces, &mut mapping)?;
        let output = if pieces.is_empty() {
            new_null_array(&self.dtype, len)
        } else if pieces.len() == 1
            && pieces[0].len() == len
            && mapping
                .iter()
                .enumerate()
                .all(|(row, index)| *index == Some((0, row)))
        {
            // A uniformly selected compatible result already has correct order.
            pieces.pop().expect("one output piece")
        } else {
            let null_piece = pieces.len();
            let has_missing = mapping.iter().any(Option::is_none);
            if has_missing {
                pieces.push(new_null_array(&self.dtype, 1));
            }
            let indices = mapping
                .into_iter()
                .map(|index| index.unwrap_or((null_piece, 0)))
                .collect::<Vec<_>>();
            let refs = pieces
                .iter()
                .map(|piece| piece.as_ref())
                .collect::<Vec<_>>();
            arrow_select::interleave::interleave(&refs, &indices)?
        };
        Ok(ColumnarValue::Array(output))
    }
}
