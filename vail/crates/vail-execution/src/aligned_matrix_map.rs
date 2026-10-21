//! Physical execution for row-aligned MatrixTable maps.
//!
//! A row's entry slots are borrowed from the input ListArray. The shared
//! columns list is unpacked once per evaluate call and its child arrays are
//! reused for every row. The body is lowered once; only its immutable aligned
//! input leaves are rebound for each owning row.

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, ListArray, RecordBatch, RecordBatchOptions, StructArray, make_array,
    new_empty_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef, Schema};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;

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

    fn bind(&self, value: ColumnarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(Self {
            key: self.key.clone(),
            dtype: self.dtype.clone(),
            value: Some(value),
        })
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
pub(crate) struct AlignedMatrixMapExpr {
    entries: Arc<dyn PhysicalExpr>,
    columns: Arc<dyn PhysicalExpr>,
    rows: Arc<dyn PhysicalExpr>,
    body: Arc<dyn PhysicalExpr>,
    dtype: DataType,
    body_dtype: DataType,
    preserve_cell: bool,
}

impl AlignedMatrixMapExpr {
    pub(crate) fn new(
        entries: Arc<dyn PhysicalExpr>,
        columns: Arc<dyn PhysicalExpr>,
        rows: Arc<dyn PhysicalExpr>,
        body: Arc<dyn PhysicalExpr>,
        dtype: DataType,
        body_dtype: DataType,
        preserve_cell: bool,
    ) -> Self {
        Self {
            entries,
            columns,
            rows,
            body,
            dtype,
            body_dtype,
            preserve_cell,
        }
    }
}

impl PartialEq for AlignedMatrixMapExpr {
    fn eq(&self, other: &Self) -> bool {
        self.entries.as_ref() == other.entries.as_ref()
            && self.columns.as_ref() == other.columns.as_ref()
            && self.rows.as_ref() == other.rows.as_ref()
            && self.body.as_ref() == other.body.as_ref()
            && self.dtype == other.dtype
            && self.body_dtype == other.body_dtype
            && self.preserve_cell == other.preserve_cell
    }
}
impl Eq for AlignedMatrixMapExpr {}
impl Hash for AlignedMatrixMapExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.entries.hash(state);
        self.columns.hash(state);
        self.rows.hash(state);
        self.body.hash(state);
        self.dtype.hash(state);
        self.body_dtype.hash(state);
        self.preserve_cell.hash(state);
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
        vec![&self.entries, &self.columns, &self.rows, &self.body]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 4 {
            return Err(DataFusionError::Internal(
                "aligned matrix map child count mismatch".into(),
            ));
        }
        Ok(Arc::new(Self {
            entries: children[0].clone(),
            columns: children[1].clone(),
            rows: children[2].clone(),
            body: children[3].clone(),
            dtype: self.dtype.clone(),
            body_dtype: self.body_dtype.clone(),
            preserve_cell: self.preserve_cell,
        }))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
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
        let columns = self.columns.evaluate(batch)?;
        let columns = shared_list_values(columns)?;
        let mut input_keys = HashSet::new();
        collect_input_keys(&self.body, &mut input_keys);

        let entry_slots = entries.values();
        let mut mapped_rows = Vec::with_capacity(entries.len());
        let offsets = entries.offsets();
        let mut result_offsets = Vec::with_capacity(entries.len() + 1);
        result_offsets.push(0_i32);

        for row_index in 0..entries.len() {
            let start = offsets[row_index] as usize;
            let end = offsets[row_index + 1] as usize;
            let entry_count = end - start;
            let entry_slice = entry_slots.slice(start, entry_count);
            let row_values = bind_row_inputs(&input_keys, rows, row_index, columns.len())?;
            let mut bindings = row_values;
            for key in &input_keys {
                match key.axis.as_str() {
                    "entry" => {
                        bindings.insert(key.clone(), ColumnarValue::Array(entry_slice.clone()));
                    }
                    "col" => {
                        bindings.insert(key.clone(), ColumnarValue::Array(columns.clone()));
                    }
                    "row" | "global" | "num_cols" => {}
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "unknown aligned input axis {:?}",
                            key.axis
                        )));
                    }
                }
            }

            let mapped = if entry_count == 0 {
                new_empty_array(&self.body_dtype)
            } else {
                let bound_body = rebind_body(&self.body, &bindings)?;
                let empty_batch = RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    Vec::new(),
                    &RecordBatchOptions::new().with_row_count(Some(entry_count)),
                )?;
                let value = bound_body.evaluate(&empty_batch)?;
                let value = value.into_array(entry_count)?;
                if value.len() != entry_count {
                    return Err(DataFusionError::Execution(format!(
                        "aligned matrix map body returned {} values for {entry_count} entries",
                        value.len()
                    )));
                }
                value
            };

            let mapped = if self.preserve_cell {
                preserve_cell_slots(entry_slice, mapped)?
            } else {
                inherit_parent_validity(mapped, entry_slice.as_ref())?
            };
            result_offsets
                .push(result_offsets.last().copied().unwrap_or_default() + entry_count as i32);
            mapped_rows.push(mapped);
        }

        let output_values = if mapped_rows.is_empty() {
            let DataType::List(field) = &self.dtype else {
                return Err(DataFusionError::Internal(
                    "aligned matrix map output must be List".into(),
                ));
            };
            new_empty_array(field.data_type())
        } else {
            let arrays = mapped_rows
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>();
            concat(&arrays)?
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

fn collect_input_keys(expr: &Arc<dyn PhysicalExpr>, out: &mut HashSet<InputKey>) {
    if let Some(input) = expr.downcast_ref::<BoundInputExpr>() {
        out.insert(input.key.clone());
    }
    for child in expr.children() {
        collect_input_keys(child, out);
    }
}

fn bind_row_inputs(
    keys: &HashSet<InputKey>,
    rows: &StructArray,
    row_index: usize,
    num_columns: usize,
) -> Result<HashMap<InputKey, ColumnarValue>> {
    let mut result = HashMap::new();
    for key in keys {
        let value = match key.axis.as_str() {
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
                ColumnarValue::Scalar(ScalarValue::try_from_array(field, row_index)?)
            }
            "global" => {
                let name = key.name.as_deref().ok_or_else(|| {
                    DataFusionError::Execution("global aligned input needs a field name".into())
                })?;
                let fields = rows
                    .column_by_name("__vail_aligned_globals")
                    .and_then(|value| value.as_any().downcast_ref::<StructArray>())
                    .ok_or_else(|| {
                        DataFusionError::Execution("aligned global bundle is malformed".into())
                    })?;
                let field = fields.column_by_name(name).ok_or_else(|| {
                    DataFusionError::Execution(format!("global field {name:?} is absent"))
                })?;
                ColumnarValue::Scalar(ScalarValue::try_from_array(field, row_index)?)
            }
            "num_cols" => ColumnarValue::Scalar(ScalarValue::Int32(Some(num_columns as i32))),
            _ => continue,
        };
        result.insert(key.clone(), value);
    }
    Ok(result)
}

fn rebind_body(
    expr: &Arc<dyn PhysicalExpr>,
    bindings: &HashMap<InputKey, ColumnarValue>,
) -> Result<Arc<dyn PhysicalExpr>> {
    if let Some(input) = expr.downcast_ref::<BoundInputExpr>() {
        let value = bindings.get(&input.key).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "no aligned matrix input bound for {}:{:?}",
                input.key.axis, input.key.name
            ))
        })?;
        return Ok(input.bind(value));
    }
    let children = expr.children();
    if children.is_empty() {
        return Ok(expr.clone());
    }
    let rebound = children
        .into_iter()
        .map(|child| rebind_body(child, bindings))
        .collect::<Result<Vec<_>>>()?;
    expr.clone().with_new_children(rebound)
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

fn inherit_parent_validity(values: ArrayRef, parent: &dyn Array) -> Result<ArrayRef> {
    if parent.null_count() == 0 {
        return Ok(values);
    }
    let nulls = NullBuffer::union(values.nulls(), parent.nulls());
    let data = values.to_data().into_builder().nulls(nulls).build()?;
    Ok(make_array(data))
}
