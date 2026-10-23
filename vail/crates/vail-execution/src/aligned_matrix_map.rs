//! Physical execution for row-aligned MatrixTable maps.
//!
//! Each entry list borrows shared column metadata and scalar row captures.
//! The evaluation frame is prepared once per input batch; no metadata is
//! broadcast into per-entry arrays.

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, ListArray, RecordBatch, RecordBatchOptions, StructArray,
    make_array, new_empty_array,
};
use arrow_array::{Int32Array, RunArray, UInt32Array};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_cast::cast;
use arrow_schema::{DataType, FieldRef, Schema};
use arrow_select::{concat::concat, take::take};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;

pub(crate) const ENTRY_COLUMN: &str = "__vail_aligned_entry";
pub(crate) const COLUMN_COLUMN: &str = "__vail_aligned_column";
pub(crate) const ENTRY_PRESENT_COLUMN: &str = "__vail_aligned_entry_present";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct InputKey {
    axis: String,
    name: Option<String>,
}

/// A typed body placeholder. Bound instances carry either one scalar row
/// capture or a borrowed entry/column child array; they never own a repeated
/// array of row or column metadata.
#[derive(Debug)]
pub(crate) struct BoundInputExpr {
    key: InputKey,
    dtype: DataType,
    value: Option<ColumnarValue>,
}

impl BoundInputExpr {
    pub(crate) fn unbound(axis: String, name: Option<String>, dtype: DataType) -> Self {
        Self {
            key: InputKey { axis, name },
            dtype,
            value: None,
        }
    }
}

impl PartialEq for BoundInputExpr {
    fn eq(&self, other: &Self) -> bool {
        // Captured values are evaluation state, not part of the compiled body
        // expression's identity.
        self.key == other.key && self.dtype == other.dtype
    }
}
impl Eq for BoundInputExpr {}
impl Hash for BoundInputExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.dtype.hash(state);
    }
}
impl Display for BoundInputExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlignedInput({}:{:?})", self.key.axis, self.key.name)
    }
}
impl PhysicalExpr for BoundInputExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }

    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, _: &RecordBatch) -> Result<ColumnarValue> {
        let value = self
            .value
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("unbound aligned matrix map input".into()))?;
        Ok(value.clone())
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "aligned input cannot have children".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug)]
enum CaptureSource {
    Row(ArrayRef),
    Scalar(ScalarValue),
}
#[derive(Debug)]
struct RowCaptureExpr {
    row_index: Arc<AtomicUsize>,
    source: CaptureSource,
    slot: usize,
}
impl PartialEq for RowCaptureExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.row_index, &other.row_index) && self.slot == other.slot
    }
}
impl Eq for RowCaptureExpr {}
impl Hash for RowCaptureExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.row_index) as usize).hash(state);
        self.slot.hash(state);
    }
}
impl Display for RowCaptureExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlignedRowCapture({})", self.slot)
    }
}
impl PhysicalExpr for RowCaptureExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(match &self.source {
            CaptureSource::Row(a) => a.data_type().clone(),
            CaptureSource::Scalar(s) => s.data_type(),
        })
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn evaluate(&self, _: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(match &self.source {
            CaptureSource::Row(a) => {
                ScalarValue::try_from_array(a, self.row_index.load(Ordering::Relaxed))?
            }
            CaptureSource::Scalar(s) => s.clone(),
        }))
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        Vec::new()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "row capture has no children".into(),
            ));
        }
        Ok(self)
    }
}
fn prepare_row_captures(
    expression: &Arc<dyn PhysicalExpr>,
    captures: &HashMap<usize, Arc<dyn PhysicalExpr>>,
    memo: &mut HashMap<usize, Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let identity = Arc::as_ptr(expression) as *const () as usize;
    if let Some(result) = memo.get(&identity) {
        return Ok(Arc::clone(result));
    }
    let result = if let Some(column) = expression.downcast_ref::<Column>() {
        captures
            .get(&column.index())
            .cloned()
            .unwrap_or_else(|| Arc::clone(expression))
    } else {
        let children = expression
            .children()
            .into_iter()
            .map(|child| prepare_row_captures(child, captures, memo))
            .collect::<Result<Vec<_>>>()?;
        if children.is_empty() {
            Arc::clone(expression)
        } else {
            Arc::clone(expression).with_new_children(children)?
        }
    };
    memo.insert(identity, Arc::clone(&result));
    Ok(result)
}

#[derive(Debug)]
pub(crate) struct AlignedMatrixMapExpr {
    entries: Arc<dyn PhysicalExpr>,
    columns: Arc<dyn PhysicalExpr>,
    rows: Arc<dyn PhysicalExpr>,
    globals: Arc<dyn PhysicalExpr>,
    body: Arc<dyn PhysicalExpr>,
    all_present_body: Option<Arc<dyn PhysicalExpr>>,
    input_keys: HashSet<InputKey>,
    binding_order: Vec<InputKey>,
    column_dtype: Option<DataType>,
    dtype: DataType,
    body_dtype: DataType,
    preserve_cell: bool,
    scalar_rows: bool,
}

impl AlignedMatrixMapExpr {
    pub(crate) fn new(
        entries: Arc<dyn PhysicalExpr>,
        columns: Arc<dyn PhysicalExpr>,
        rows: Arc<dyn PhysicalExpr>,
        globals: Arc<dyn PhysicalExpr>,
        body: Arc<dyn PhysicalExpr>,
        all_present_body: Option<Arc<dyn PhysicalExpr>>,
        capture_keys: Vec<(String, Option<String>)>,
        column_dtype: Option<DataType>,
        dtype: DataType,
        body_dtype: DataType,
        preserve_cell: bool,
        scalar_rows: bool,
    ) -> Self {
        let binding_order = capture_keys
            .into_iter()
            .map(|(axis, name)| InputKey { axis, name })
            .collect::<Vec<_>>();
        let input_keys = binding_order.iter().cloned().collect();
        Self {
            entries,
            columns,
            rows,
            globals,
            body,
            all_present_body,
            input_keys,
            binding_order,
            column_dtype,
            dtype,
            body_dtype,
            preserve_cell,
            scalar_rows,
        }
    }
}

impl AlignedMatrixMapExpr {
    fn evaluate_flat(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let entries = self.entries.evaluate(batch)?;
        let entries = array_value(entries, "entry lists")?;
        let entries = entries
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("aligned matrix map entries must be ListArray".into())
            })?;
        let rows = self.rows.evaluate(batch)?;
        let rows = array_value(rows, "row records")?;
        let rows = rows.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
            DataFusionError::Execution("aligned matrix map rows must be StructArray".into())
        })?;
        let global_bindings = if entries.is_empty() {
            HashMap::new()
        } else {
            bind_globals(self.globals.evaluate(batch)?, &self.input_keys)?
        };
        let columns = self.columns.evaluate(batch)?;
        let columns = shared_list_values(columns)?;

        let collect_stats = std::env::var_os("VAIL_EXPERIMENT_REE_STATS").is_some();
        // Experimental batch implementation: genuine REE row/global metadata,
        // decoded once per binding for kernels that require logical arrays.
        // Each binding becomes a batch column so CASE's filtered evaluation
        // filters the captured metadata alongside the entry arrays.
        let offsets = entries.offsets();
        let base = offsets[0] as usize;
        let total = offsets[entries.len()] as usize - base;
        let entry_slots = entries.values().slice(base, total);
        let result_offsets = offsets
            .iter()
            .map(|offset| *offset - base as i32)
            .collect::<Vec<_>>();
        if total == 0 {
            let mapped = new_empty_array(&self.body_dtype);
            let output_values = if self.preserve_cell {
                preserve_cell_slots(entry_slots, mapped)?
            } else {
                inherit_parent_validity(mapped, entry_values(entry_slots.as_ref())?.as_ref())?
            };
            let DataType::List(field) = &self.dtype else {
                return Err(DataFusionError::Internal(
                    "aligned matrix map output must be List".into(),
                ));
            };
            return Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(result_offsets)),
                output_values,
                entries.nulls().cloned(),
            )?)));
        }
        let mut column_indices = self
            .column_dtype
            .as_ref()
            .map(|_| Vec::with_capacity(total));
        let mut run_ends = Vec::new();
        let mut row_indices = Vec::new();
        for row_index in 0..entries.len() {
            let count = (offsets[row_index + 1] - offsets[row_index]) as usize;
            if count > columns.len() {
                return Err(DataFusionError::Execution(
                    "aligned entry list exceeds column metadata length".into(),
                ));
            }
            if let Some(indices) = &mut column_indices {
                indices.extend((0..count).map(|index| index as u32));
            }
            if count > 0 {
                run_ends.push(offsets[row_index + 1] - base as i32);
                row_indices.push(row_index as u32);
            }
        }
        let repeated_columns: ArrayRef = if let Some(dtype) = &self.column_dtype {
            let projected = project_column_array(&columns, dtype)?;
            take(
                projected.as_ref(),
                &UInt32Array::from(column_indices.unwrap()),
                None,
            )?
        } else {
            // The compiled body cannot read this slot. An empty typed struct
            // carries its row count without allocating any payload children.
            Arc::new(StructArray::new_empty_fields(total, None))
        };
        let values = entry_values(entry_slots.as_ref())?;
        let entry_presence = values
            .nulls()
            .map(|validity| validity.inner().clone())
            .unwrap_or_else(|| BooleanBuffer::new_set(total));
        if collect_stats {
            eprintln!(
                "VAIL_REE_COLUMNS rows={} entries={} original_bytes={} flattened_bytes={}",
                entries.len(),
                total,
                columns.get_array_memory_size(),
                repeated_columns.get_array_memory_size()
            );
        }
        let mut body_arrays = vec![
            entry_slots.clone(),
            repeated_columns.clone(),
            Arc::new(BooleanArray::new(entry_presence, None)) as ArrayRef,
        ];
        let mut body_fields = vec![
            Arc::new(arrow_schema::Field::new(
                ENTRY_COLUMN,
                entry_slots.data_type().clone(),
                true,
            )),
            Arc::new(arrow_schema::Field::new(
                COLUMN_COLUMN,
                repeated_columns.data_type().clone(),
                true,
            )),
            Arc::new(arrow_schema::Field::new(
                ENTRY_PRESENT_COLUMN,
                DataType::Boolean,
                false,
            )),
        ];

        let row_indices = UInt32Array::from(row_indices);
        let row_run_ends = Int32Array::from(run_ends);
        let mut ree_bytes = 0;
        let mut dense_bytes = 0;
        for key in &self.binding_order {
            let (run_ends, run_values) = match key.axis.as_str() {
                "row" => {
                    let name = key.name.as_deref().ok_or_else(|| {
                        DataFusionError::Execution("row aligned input needs a field name".into())
                    })?;
                    let fields = rows
                        .column_by_name("__vail_aligned_row")
                        .and_then(|value| value.as_any().downcast_ref::<StructArray>())
                        .ok_or_else(|| {
                            DataFusionError::Execution("aligned row bundle is malformed".into())
                        })?;
                    let field = fields.column_by_name(name).ok_or_else(|| {
                        DataFusionError::Execution(format!("row field {name:?} is absent"))
                    })?;
                    let run_values = if row_indices.len() == field.len() {
                        field.clone()
                    } else {
                        take(field.as_ref(), &row_indices, None)?
                    };
                    (row_run_ends.clone(), run_values)
                }
                "global" | "num_cols" => {
                    let scalar = if key.axis == "num_cols" {
                        ScalarValue::Int32(Some(columns.len() as i32))
                    } else {
                        match global_bindings.get(key) {
                            Some(ColumnarValue::Scalar(value)) => value.clone(),
                            _ => {
                                return Err(DataFusionError::Execution(
                                    "missing scalar global binding".into(),
                                ));
                            }
                        }
                    };
                    if total == 0 {
                        (
                            Int32Array::from(Vec::<i32>::new()),
                            new_empty_array(&scalar.data_type()),
                        )
                    } else {
                        (Int32Array::from(vec![total as i32]), scalar.to_array()?)
                    }
                }
                _ => continue,
            };
            let logical_dtype = run_values.data_type().clone();
            let encoded = RunArray::<Int32Type>::try_new(&run_ends, run_values.as_ref())?;
            ree_bytes += encoded.get_array_memory_size();
            // Arrow's generic REE cast materializes with one take. Binding the
            // resulting column avoids decoding again at each consuming kernel.
            let decoded = if total == 0 {
                new_empty_array(&logical_dtype)
            } else {
                cast(&encoded, &logical_dtype)?
            };
            dense_bytes += decoded.get_array_memory_size();
            if collect_stats {
                let shape = match &logical_dtype {
                    DataType::Struct(fields) => format!("Struct({} fields)", fields.len()),
                    other => format!("{other:?}"),
                };
                eprintln!(
                    "VAIL_REE_BIND axis={} name={:?} runs={} ree_bytes={} decoded_bytes={} shape={}",
                    key.axis,
                    key.name,
                    run_ends.len(),
                    encoded.get_array_memory_size(),
                    decoded.get_array_memory_size(),
                    shape
                );
            }

            let index = body_arrays.len();
            let name = format!("__vail_aligned_bound_{index}");
            body_fields.push(Arc::new(arrow_schema::Field::new(
                &name,
                logical_dtype,
                true,
            )));
            body_arrays.push(decoded);
        }
        if collect_stats {
            eprintln!(
                "VAIL_REE_BATCH rows={} entries={} bindings={} ree_bytes={} decoded_bytes={}",
                entries.len(),
                total,
                self.binding_order.len(),
                ree_bytes,
                dense_bytes
            );
        }
        let mapped = if total == 0 {
            new_empty_array(&self.body_dtype)
        } else {
            let body_batch = RecordBatch::try_new_with_options(
                Arc::new(Schema::new(body_fields)),
                body_arrays,
                &RecordBatchOptions::new().with_row_count(Some(total)),
            )?;
            // The guarded and unguarded bodies are compiled once. Avoid CASE's
            // filter/take/scatter when this batch has no absent entries; retain
            // lazy evaluation for sparse batches so absent values cannot error.
            let body = if values.null_count() == 0 {
                self.all_present_body.as_ref().unwrap_or(&self.body)
            } else {
                &self.body
            };
            let value = body.evaluate(&body_batch)?.into_array(total)?;
            if value.len() != total {
                return Err(DataFusionError::Execution(format!(
                    "aligned batch body returned {} values for {total} entries",
                    value.len()
                )));
            }
            value
        };
        let output_values = if self.preserve_cell {
            preserve_cell_slots(entry_slots, mapped)?
        } else {
            inherit_parent_validity(mapped, values.as_ref())?
        };
        let DataType::List(field) = &self.dtype else {
            return Err(DataFusionError::Internal(
                "aligned matrix map output must be List".into(),
            ));
        };
        let output = ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(result_offsets)),
            output_values,
            entries.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

impl PartialEq for AlignedMatrixMapExpr {
    fn eq(&self, other: &Self) -> bool {
        self.entries.as_ref() == other.entries.as_ref()
            && self.columns.as_ref() == other.columns.as_ref()
            && self.rows.as_ref() == other.rows.as_ref()
            && self.globals.as_ref() == other.globals.as_ref()
            && self.body.as_ref() == other.body.as_ref()
            && self.binding_order == other.binding_order
            && self.column_dtype == other.column_dtype
            && self.dtype == other.dtype
            && self.body_dtype == other.body_dtype
            && self.preserve_cell == other.preserve_cell
            && self.scalar_rows == other.scalar_rows
    }
}
impl Eq for AlignedMatrixMapExpr {}
impl Hash for AlignedMatrixMapExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.entries.hash(state);
        self.columns.hash(state);
        self.rows.hash(state);
        self.globals.hash(state);
        self.body.hash(state);
        self.binding_order.hash(state);
        self.column_dtype.hash(state);
        self.dtype.hash(state);
        self.body_dtype.hash(state);
        self.preserve_cell.hash(state);
        self.scalar_rows.hash(state);
    }
}
impl Display for AlignedMatrixMapExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlignedMatrixMap({})", self.body)
    }
}

impl PhysicalExpr for AlignedMatrixMapExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }

    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![
            &self.entries,
            &self.columns,
            &self.rows,
            &self.globals,
            &self.body,
        ]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 5 {
            return Err(DataFusionError::Internal(
                "aligned matrix map child count mismatch".into(),
            ));
        }
        Ok(Arc::new(Self {
            entries: children[0].clone(),
            columns: children[1].clone(),
            rows: children[2].clone(),
            globals: children[3].clone(),
            // Only retain the prepared fast path if the body is unchanged.
            all_present_body: (children[4].as_ref() == self.body.as_ref())
                .then(|| self.all_present_body.clone())
                .flatten(),
            body: children[4].clone(),
            input_keys: self.input_keys.clone(),
            binding_order: self.binding_order.clone(),
            column_dtype: self.column_dtype.clone(),
            dtype: self.dtype.clone(),
            body_dtype: self.body_dtype.clone(),
            preserve_cell: self.preserve_cell,
            scalar_rows: self.scalar_rows && children[4].as_ref() == self.body.as_ref(),
        }))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if !self.scalar_rows {
            return self.evaluate_flat(batch);
        }
        let entries = self.entries.evaluate(batch)?;
        let entries = array_value(entries, "entry lists")?;
        let entries = entries
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("aligned matrix map entries must be ListArray".into())
            })?;
        let rows = self.rows.evaluate(batch)?;
        let rows = array_value(rows, "row records")?;
        let rows = rows.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
            DataFusionError::Execution("aligned matrix map rows must be StructArray".into())
        })?;
        let global_bindings = if entries.is_empty() {
            HashMap::new()
        } else {
            bind_globals(self.globals.evaluate(batch)?, &self.input_keys)?
        };
        let columns = self.columns.evaluate(batch)?;
        let columns = shared_list_values(columns)?;

        let offsets = entries.offsets();
        let base = offsets[0] as usize;
        let total = offsets[entries.len()] as usize - base;
        let entry_slots = entries.values().slice(base, total);
        let result_offsets = offsets
            .iter()
            .map(|offset| *offset - base as i32)
            .collect::<Vec<_>>();
        let values = entry_values(entry_slots.as_ref())?;
        let projected_columns = match &self.column_dtype {
            Some(dtype) => project_column_array(&columns, dtype)?,
            None => Arc::new(StructArray::new_empty_fields(columns.len(), None)) as ArrayRef,
        };
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new(ENTRY_COLUMN, entry_slots.data_type().clone(), true),
            arrow_schema::Field::new(COLUMN_COLUMN, projected_columns.data_type().clone(), true),
            arrow_schema::Field::new(ENTRY_PRESENT_COLUMN, DataType::Boolean, false),
        ]));
        // One evaluation-local frame owns captured arrays. Only the current row
        // changes during traversal; expression preparation never happens there.
        let row_index = Arc::new(AtomicUsize::new(0));
        let mut captures = HashMap::new();
        for (index, key) in self.binding_order.iter().enumerate() {
            let source = match key.axis.as_str() {
                "row" => {
                    let fields = rows
                        .column_by_name("__vail_aligned_row")
                        .and_then(|a| a.as_any().downcast_ref::<StructArray>())
                        .ok_or_else(|| {
                            DataFusionError::Execution("aligned row bundle is malformed".into())
                        })?;
                    let field = key
                        .name
                        .as_deref()
                        .and_then(|name| fields.column_by_name(name))
                        .ok_or_else(|| {
                            DataFusionError::Execution("aligned row capture is absent".into())
                        })?;
                    CaptureSource::Row(Arc::clone(field))
                }
                "num_cols" => CaptureSource::Scalar(ScalarValue::Int32(Some(columns.len() as i32))),
                "global" => match global_bindings.get(key) {
                    Some(ColumnarValue::Scalar(value)) => CaptureSource::Scalar(value.clone()),
                    _ => {
                        return Err(DataFusionError::Execution(
                            "missing scalar global binding".into(),
                        ));
                    }
                },
                _ => continue,
            };
            captures.insert(
                index + 3,
                Arc::new(RowCaptureExpr {
                    row_index: Arc::clone(&row_index),
                    source,
                    slot: index + 3,
                }) as Arc<dyn PhysicalExpr>,
            );
        }
        let body = if values.null_count() == 0 {
            self.all_present_body.as_ref().unwrap_or(&self.body)
        } else {
            &self.body
        };
        let body = prepare_row_captures(body, &captures, &mut HashMap::new())?;
        let mut mapped_rows = Vec::with_capacity(entries.len());
        for row in 0..entries.len() {
            let offset = offsets[row] as usize - base;
            let count = (offsets[row + 1] - offsets[row]) as usize;
            if count == 0 {
                continue;
            }
            if count > projected_columns.len() {
                return Err(DataFusionError::Execution(
                    "aligned entry list exceeds column metadata length".into(),
                ));
            }
            row_index.store(row, Ordering::Relaxed);
            let slots = entry_slots.slice(offset, count);
            let entry_values = entry_values(slots.as_ref())?;
            let presence = entry_values
                .nulls()
                .map(|n| n.inner().clone())
                .unwrap_or_else(|| BooleanBuffer::new_set(count));
            let body_batch = RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                vec![
                    slots,
                    projected_columns.slice(0, count),
                    Arc::new(BooleanArray::new(presence, None)),
                ],
                &RecordBatchOptions::new().with_row_count(Some(count)),
            )?;
            mapped_rows.push(body.evaluate(&body_batch)?.into_array(count)?);
        }
        let mapped = match mapped_rows.len() {
            0 => new_empty_array(&self.body_dtype),
            1 => mapped_rows.pop().unwrap(),
            _ => concat(&mapped_rows.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?,
        };
        let output_values = if self.preserve_cell {
            preserve_cell_slots(entry_slots, mapped)?
        } else {
            inherit_parent_validity(mapped, values.as_ref())?
        };
        let DataType::List(field) = &self.dtype else {
            return Err(DataFusionError::Internal(
                "aligned matrix map output must be List".into(),
            ));
        };
        let output = ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(result_offsets)),
            output_values,
            entries.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

/// Prune borrowed nested struct children before gathering repeated columns.
/// Parent null validity and field order are retained at every projected level.
fn project_column_array(array: &ArrayRef, dtype: &DataType) -> Result<ArrayRef> {
    if array.data_type() == dtype {
        return Ok(array.clone());
    }
    let DataType::Struct(fields) = dtype else {
        return Ok(cast(array.as_ref(), dtype)?);
    };
    let source = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("aligned column projection needs struct".into())
        })?;
    if fields.is_empty() {
        return Ok(Arc::new(StructArray::new_empty_fields(
            array.len(),
            source.nulls().cloned(),
        )));
    }
    let children = fields
        .iter()
        .map(|field| {
            let child = source.column_by_name(field.name()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "aligned column field {:?} absent",
                    field.name()
                ))
            })?;
            project_column_array(child, field.data_type())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        children,
        source.nulls().cloned(),
    )?))
}

fn array_value(value: ColumnarValue, what: &str) -> Result<ArrayRef> {
    match value {
        ColumnarValue::Array(array) => Ok(array),
        ColumnarValue::Scalar(value) => Ok(value.to_array()?),
    }
    .map_err(|error: DataFusionError| {
        DataFusionError::Execution(format!("cannot materialize aligned {what}: {error}"))
    })
}

fn shared_list_values(value: ColumnarValue) -> Result<ArrayRef> {
    let value = match value {
        ColumnarValue::Scalar(ScalarValue::List(values)) => values,
        ColumnarValue::Scalar(value) => value.to_array()?,
        ColumnarValue::Array(value) => value,
    };
    let lists = value.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Execution("aligned matrix columns must be a ListArray".into())
    })?;
    if lists.is_empty() {
        return Ok(lists.values().clone());
    }
    // External globals may arrive as a singleton scalar or as the singleton
    // global row joined to the outer rows. `value(0)` is a zero-copy child
    // slice; the resulting ArrayRef is reused for every map row.
    Ok(lists.value(0))
}

fn bind_globals(
    value: ColumnarValue,
    keys: &HashSet<InputKey>,
) -> Result<HashMap<InputKey, ColumnarValue>> {
    if !keys.iter().any(|key| key.axis == "global") {
        return Ok(HashMap::new());
    }
    let globals: ArrayRef = match value {
        ColumnarValue::Scalar(ScalarValue::Struct(value)) => value,
        ColumnarValue::Array(value) => value,
        ColumnarValue::Scalar(value) => value.to_array()?,
    };
    let globals = globals
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("aligned matrix map globals must be StructArray".into())
        })?;
    let mut result = HashMap::new();
    for key in keys.iter().filter(|key| key.axis == "global") {
        let name = key.name.as_deref().ok_or_else(|| {
            DataFusionError::Execution("global aligned input needs a field name".into())
        })?;
        let field = globals.column_by_name(name).ok_or_else(|| {
            DataFusionError::Execution(format!("global field {name:?} is absent"))
        })?;
        let scalar = ScalarValue::try_from_array(field, 0)?;
        result.insert(key.clone(), ColumnarValue::Scalar(scalar));
    }
    Ok(result)
}

fn preserve_cell_slots(entries: ArrayRef, mapped: ArrayRef) -> Result<ArrayRef> {
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("aligned entry slots must be StructArray".into())
        })?;
    let id = entries
        .column_by_name("__vail_matrix_col_id")
        .ok_or_else(|| DataFusionError::Execution("aligned entry slot lacks column id".into()))?;
    let input_values = entry_values(entries)?;
    let mapped = inherit_parent_validity(mapped, input_values.as_ref())?;
    let fields = match mapped.data_type() {
        DataType::Struct(_) => vec![
            Arc::new(arrow_schema::Field::new(
                "__vail_matrix_col_id",
                id.data_type().clone(),
                true,
            )),
            Arc::new(arrow_schema::Field::new(
                "value",
                mapped.data_type().clone(),
                true,
            )),
        ]
        .into_iter()
        .map(|field| field as FieldRef)
        .collect::<Vec<_>>(),
        _ => {
            return Err(DataFusionError::Execution(
                "aligned entry body must return a struct".into(),
            ));
        }
    };
    Ok(Arc::new(StructArray::try_new(
        fields.into(),
        vec![id.clone(), mapped],
        entries.nulls().cloned(),
    )?))
}

fn entry_values(entries: &dyn Array) -> Result<ArrayRef> {
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("aligned entry slots must be StructArray".into())
        })?;
    entries
        .column_by_name("value")
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("aligned entry slot lacks value".into()))
}

fn inherit_parent_validity(values: ArrayRef, parent: &dyn Array) -> Result<ArrayRef> {
    if parent.null_count() == 0 {
        return Ok(values);
    }
    let nulls = NullBuffer::union(values.nulls(), parent.nulls());
    let data = values.to_data().into_builder().nulls(nulls).build()?;
    Ok(make_array(data))
}

#[cfg(test)]
mod runend_experiment_tests {
    use super::*;
    #[test]
    fn native_numeric_kernel_does_not_accept_runend_input() {
        let values = Int32Array::from(vec![2, 7]);
        let run_ends = Int32Array::from(vec![3, 5]);
        let encoded = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let result = arrow_arith::numeric::add(&encoded, &encoded);
        assert!(
            result.is_err(),
            "kernel support changed; revisit decode fallback"
        );
        eprintln!("REE add probe: {}", result.unwrap_err());
        let decoded = cast(&encoded, &DataType::Int32).unwrap();
        assert_eq!(decoded.as_ref(), &Int32Array::from(vec![2, 2, 2, 7, 7]));
    }
    #[test]
    fn decode_once_supports_nested_structs_and_null_values() {
        let values = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new("x", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(2), None])) as ArrayRef,
        )]);
        let encoded =
            RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 5]), &values).unwrap();
        let decoded = cast(&encoded, values.data_type()).unwrap();
        let decoded = decoded.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(2), Some(2), None, None, None])
        );
    }
    #[test]
    fn flattened_map_preserves_sliced_offsets_short_empty_lists_and_null_slots() {
        use crate::expressions::{Expr, PrimitiveType, Type, lower};
        let entry_values = StructArray::new(
            vec![Arc::new(arrow_schema::Field::new(
                "x",
                DataType::Int32,
                true,
            ))]
            .into(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
            Some(NullBuffer::from(vec![true, false, true, true, true])),
        );
        let slots = StructArray::from(vec![
            (
                Arc::new(arrow_schema::Field::new(
                    "__vail_matrix_col_id",
                    DataType::Int32,
                    true,
                )),
                Arc::new(Int32Array::from(vec![0, 1, 0, 0, 1])) as ArrayRef,
            ),
            (
                Arc::new(arrow_schema::Field::new(
                    "value",
                    entry_values.data_type().clone(),
                    true,
                )),
                Arc::new(entry_values) as ArrayRef,
            ),
        ]);
        let entries = ListArray::new(
            Arc::new(arrow_schema::Field::new(
                "item",
                slots.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3, 5])),
            Arc::new(slots),
            None,
        );
        let row_fields = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new("x", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef,
        )]);
        let rows = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new(
                "__vail_aligned_row",
                row_fields.data_type().clone(),
                true,
            )),
            Arc::new(row_fields) as ArrayRef,
        )]);
        let cols = Int32Array::from(vec![0, 1]);
        let columns = ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(arrow_schema::Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            Arc::new(cols),
            None,
        )));
        let arrays = vec![Arc::new(entries) as ArrayRef, Arc::new(rows) as ArrayRef];
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("entries", arrays[0].data_type().clone(), true),
            arrow_schema::Field::new("rows", arrays[1].data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, arrays).unwrap();
        let expression = Expr::AlignedMatrixMap {
            entries: Arc::new(Expr::Column {
                name: "entries".into(),
            }),
            columns: Arc::new(Expr::RuntimeLiteral { value: columns }),
            rows: Arc::new(Expr::Column {
                name: "rows".into(),
            }),
            globals: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Int32(None),
            }),
            body: Arc::new(Expr::AlignedMapInput {
                axis: "row".into(),
                name: Some("x".into()),
                dtype: Type::Primitive(PrimitiveType::Int32),
            }),
            dtype: Type::Array {
                array: Box::new(Type::Primitive(PrimitiveType::Int32)),
            },
            preserve_cell: false,
        };
        let expr = lower(&expression, batch.schema().as_ref()).unwrap();
        let all = expr.evaluate(&batch).unwrap().into_array(4).unwrap();
        let all = all.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(all.value_offsets(), &[0, 2, 2, 3, 5]);
        assert_eq!(
            all.values().as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(10), None, Some(30), Some(40), Some(40)])
        );
        let sliced = expr
            .evaluate(&batch.slice(2, 2))
            .unwrap()
            .into_array(2)
            .unwrap();
        let sliced = sliced.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(sliced.value_offsets(), &[0, 1, 3]);
        assert_eq!(
            sliced
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![30, 40, 40])
        );
        let empty = expr
            .evaluate(&batch.slice(1, 1))
            .unwrap()
            .into_array(1)
            .unwrap();
        let empty = empty.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(empty.value_offsets(), &[0, 0]);
        assert!(empty.values().is_empty());
    }
    #[test]
    fn public_aligned_maps_keep_same_schema_generated_bodies_distinct() {
        use crate::expressions::{BinaryOperator, Expr, PrimitiveType, Type, lower};
        let int_type = Type::Primitive(PrimitiveType::Int32);
        let list_type = Type::Array {
            array: Box::new(int_type.clone()),
        };
        let values = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new(
                "value",
                DataType::Struct(Vec::<FieldRef>::new().into()),
                true,
            )),
            Arc::new(StructArray::new_empty_fields(2, None)) as ArrayRef,
        )]);
        let entries = ListArray::new(
            Arc::new(arrow_schema::Field::new(
                "item",
                values.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            Arc::new(values),
            None,
        );
        let row = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new("x", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![10])) as ArrayRef,
        )]);
        let rows = StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new(
                "__vail_aligned_row",
                row.data_type().clone(),
                true,
            )),
            Arc::new(row) as ArrayRef,
        )]);
        let arrays = vec![Arc::new(entries) as ArrayRef, Arc::new(rows) as ArrayRef];
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("entries", arrays[0].data_type().clone(), true),
            arrow_schema::Field::new("rows", arrays[1].data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, arrays).unwrap();
        let columns = ScalarValue::List(Arc::new(
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(0), Some(1)])]),
        ));
        let map = |amount: i32| Expr::AlignedMatrixMap {
            entries: Arc::new(Expr::Column {
                name: "entries".into(),
            }),
            columns: Arc::new(Expr::RuntimeLiteral {
                value: columns.clone(),
            }),
            rows: Arc::new(Expr::Column {
                name: "rows".into(),
            }),
            globals: Arc::new(Expr::RuntimeLiteral {
                value: ScalarValue::Int32(None),
            }),
            body: Arc::new(Expr::Binary {
                operator: BinaryOperator::Add,
                left: Arc::new(Expr::AlignedMapInput {
                    axis: "row".into(),
                    name: Some("x".into()),
                    dtype: int_type.clone(),
                }),
                right: Arc::new(Expr::RuntimeLiteral {
                    value: ScalarValue::Int32(Some(amount)),
                }),
            }),
            dtype: list_type.clone(),
            preserve_cell: false,
        };
        let expression = Expr::Struct {
            fields: (1..=8)
                .map(|amount| (format!("map_{amount}"), map(amount)))
                .collect(),
        };
        let result = lower(&expression, batch.schema().as_ref())
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(1)
            .unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(result.num_columns(), 8);
        for (index, column) in result.columns().iter().enumerate() {
            let list = column.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list.value_offsets(), &[0, 2]);
            let expected = 11 + index as i32;
            assert_eq!(
                list.values().as_any().downcast_ref::<Int32Array>().unwrap(),
                &Int32Array::from(vec![expected, expected])
            );
        }
    }
}
