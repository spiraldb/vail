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
    Array, ArrayRef, BooleanArray, ListArray, RecordBatch, RecordBatchOptions, StructArray,
    make_array, new_empty_array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef, Schema};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;

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
    globals: Arc<dyn PhysicalExpr>,
    body: Arc<dyn PhysicalExpr>,
    input_keys: HashSet<InputKey>,
    dtype: DataType,
    body_dtype: DataType,
    preserve_cell: bool,
}

impl AlignedMatrixMapExpr {
    pub(crate) fn new(
        entries: Arc<dyn PhysicalExpr>,
        columns: Arc<dyn PhysicalExpr>,
        rows: Arc<dyn PhysicalExpr>,
        globals: Arc<dyn PhysicalExpr>,
        body: Arc<dyn PhysicalExpr>,
        dtype: DataType,
        body_dtype: DataType,
        preserve_cell: bool,
    ) -> Self {
        let mut input_keys = HashSet::new();
        let mut visited = HashSet::new();
        collect_input_keys(&body, &mut input_keys, &mut visited);
        Self {
            entries,
            columns,
            rows,
            globals,
            body,
            input_keys,
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
            && self.globals.as_ref() == other.globals.as_ref()
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
        self.globals.hash(state);
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
            body: children[4].clone(),
            input_keys: self.input_keys.clone(),
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
        let global_bindings = if entries.is_empty() {
            HashMap::new()
        } else {
            bind_globals(self.globals.evaluate(batch)?, &self.input_keys)?
        };
        let columns = self.columns.evaluate(batch)?;
        let columns = shared_list_values(columns)?;

        let entry_slots = entries.values();
        let mut mapped_rows = Vec::with_capacity(entries.len());
        let offsets = entries.offsets();
        let mut result_offsets = Vec::with_capacity(entries.len() + 1);
        result_offsets.push(0_i32);
        let body_schema = Arc::new(Schema::new(vec![
            Arc::new(arrow_schema::Field::new(
                ENTRY_COLUMN,
                entry_slots.data_type().clone(),
                true,
            )),
            Arc::new(arrow_schema::Field::new(
                COLUMN_COLUMN,
                columns.data_type().clone(),
                true,
            )),
            Arc::new(arrow_schema::Field::new(
                ENTRY_PRESENT_COLUMN,
                DataType::Boolean,
                false,
            )),
        ]));

        for row_index in 0..entries.len() {
            let start = offsets[row_index] as usize;
            let end = offsets[row_index + 1] as usize;
            let entry_count = end - start;
            let entry_slice = entry_slots.slice(start, entry_count);
            let row_values = bind_row_inputs(
                &self.input_keys,
                rows,
                &global_bindings,
                row_index,
                columns.len(),
            )?;
            let bindings = row_values;

            let mapped = if entry_count == 0 {
                new_empty_array(&self.body_dtype)
            } else {
                let mut memo = HashMap::new();
                let bound_body = rebind_body(&self.body, &bindings, &mut memo)?;
                // CASE-based slot preservation lets DataFusion avoid evaluating
                // the body for null parent slots. Its filtered-batch path may
                // allocate subset metadata when only some slots are valid;
                // the all-valid path keeps the borrowed aligned arrays intact.
                let entry_presence = entry_slice
                    .nulls()
                    .map(|validity| validity.inner().clone())
                    .unwrap_or_else(|| BooleanBuffer::new_set(entry_count));
                let entry_present = Arc::new(BooleanArray::new(entry_presence, None)) as ArrayRef;
                let body_batch = RecordBatch::try_new_with_options(
                    body_schema.clone(),
                    vec![entry_slice.clone(), columns.clone(), entry_present],
                    &RecordBatchOptions::new().with_row_count(Some(entry_count)),
                )?;
                let value = bound_body.evaluate(&body_batch)?;
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
                let values = entry_values(entry_slice.as_ref())?;
                inherit_parent_validity(mapped, values.as_ref())?
            };
            let next_offset = result_offsets
                .last()
                .copied()
                .unwrap_or_default()
                .checked_add(i32::try_from(entry_count).map_err(|_| {
                    DataFusionError::Execution(
                        "aligned row exceeds ListArray offset capacity".into(),
                    )
                })?)
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "aligned output exceeds ListArray offset capacity".into(),
                    )
                })?;
            result_offsets.push(next_offset);
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

fn node_id(expr: &Arc<dyn PhysicalExpr>) -> usize {
    Arc::as_ptr(expr) as *const () as usize
}

fn collect_input_keys(
    expr: &Arc<dyn PhysicalExpr>,
    out: &mut HashSet<InputKey>,
    visited: &mut HashSet<usize>,
) {
    if !visited.insert(node_id(expr)) {
        return;
    }
    if let Some(input) = expr.downcast_ref::<BoundInputExpr>() {
        out.insert(input.key.clone());
    }
    for child in expr.children() {
        collect_input_keys(child, out, visited);
    }
}

fn bind_row_inputs(
    keys: &HashSet<InputKey>,
    rows: &StructArray,
    globals: &HashMap<InputKey, ColumnarValue>,
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
            "global" => globals.get(key).cloned().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "no aligned global input bound for {:?}",
                    key.name
                ))
            })?,
            "num_cols" => ColumnarValue::Scalar(ScalarValue::Int32(Some(num_columns as i32))),
            _ => continue,
        };
        result.insert(key.clone(), value);
    }
    Ok(result)
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

fn rebind_body(
    expr: &Arc<dyn PhysicalExpr>,
    bindings: &HashMap<InputKey, ColumnarValue>,
    memo: &mut HashMap<usize, Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let id = node_id(expr);
    if let Some(cached) = memo.get(&id) {
        return Ok(cached.clone());
    }
    if let Some(input) = expr.downcast_ref::<BoundInputExpr>() {
        let value = bindings.get(&input.key).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "no aligned matrix input bound for {}:{:?}",
                input.key.axis, input.key.name
            ))
        })?;
        let rebound = input.bind(value);
        memo.insert(id, rebound.clone());
        return Ok(rebound);
    }
    let children = expr.children();
    if children.is_empty() {
        memo.insert(id, expr.clone());
        return Ok(expr.clone());
    }
    let rebound = children
        .into_iter()
        .map(|child| rebind_body(child, bindings, memo))
        .collect::<Result<Vec<_>>>()?;
    let rebound = expr.clone().with_new_children(rebound)?;
    memo.insert(id, rebound.clone());
    Ok(rebound)
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
