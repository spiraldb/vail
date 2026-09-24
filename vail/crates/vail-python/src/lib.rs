//! Initial in-process Table bridge. The supported scalar subset is deliberately explicit.
use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    ListBuilder, StringBuilder, StructBuilder, make_builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, RecordBatchOptions, StringArray, StringViewArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};
use vail_execution::expressions::{
    CompiledProjection, Expr, PrimitiveType, Type, lower, validate_projection,
};
use vail_execution::{OrderKey, OrderRequirement, OrderedSession};

fn value_error(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyclass]
struct NativeSession {
    runtime: Arc<Runtime>,
}

#[pymethods]
impl NativeSession {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    fn parallelize(
        &self,
        schema_json: &str,
        rows_json: &str,
        n_partitions: usize,
    ) -> PyResult<NativeTable> {
        if n_partitions == 0 {
            return Err(value_error("n_partitions must be positive"));
        }
        let fields: Vec<(String, Type)> = serde_json::from_str(schema_json).map_err(value_error)?;
        let mut names = HashSet::new();
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dtype)| {
                    if !names.insert(name.clone()) {
                        return Err(value_error(format!("duplicate field {name:?}")));
                    }
                    let dtype = dtype.to_arrow().map_err(value_error)?;
                    Ok(Field::new(name, dtype, true))
                })
                .collect::<PyResult<Vec<_>>>()?,
        ));
        let rows: Vec<Vec<Value>> = serde_json::from_str(rows_json).map_err(value_error)?;
        if rows.iter().any(|row| row.len() != fields.len()) {
            return Err(value_error("row width does not match schema"));
        }
        let mut partitions = Vec::with_capacity(n_partitions);
        for partition in 0..n_partitions {
            let start = rows.len() * partition / n_partitions;
            let end = rows.len() * (partition + 1) / n_partitions;
            let part = &rows[start..end];
            let columns = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| build_column(field.data_type(), part, index))
                .collect::<PyResult<Vec<_>>>()?;
            let options = RecordBatchOptions::new().with_row_count(Some(part.len()));
            let batch = RecordBatch::try_new_with_options(schema.clone(), columns, &options)
                .map_err(value_error)?;
            partitions.push(vec![batch]);
        }
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            source_schema: schema.clone(),
            schema,
            partitions: Arc::new(partitions),
            operations: Vec::new(),
            row_count: Some(rows.len()),
        })
    }

    fn range_table(&self, n: usize, n_partitions: usize) -> PyResult<NativeTable> {
        if n_partitions == 0 {
            return Err(value_error("n_partitions must be positive"));
        }
        if n > i32::MAX as usize {
            return Err(value_error("range_table supports n up to 2**31 - 1"));
        }
        let effective = n.min(n_partitions).max(1);
        let schema = Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)]));
        let mut partitions = Vec::with_capacity(effective);
        let mut start = 0;
        for partition in 0..effective {
            let length = n / effective + usize::from(partition < n % effective);
            let end = start + length;
            let values = Int32Array::from_iter_values((start..end).map(|idx| idx as i32));
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)])
                .map_err(value_error)?;
            partitions.push(vec![batch]);
            start = end;
        }
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            source_schema: schema.clone(),
            schema,
            partitions: Arc::new(partitions),
            operations: Vec::new(),
            row_count: Some(n),
        })
    }
}

fn build_column(dtype: &DataType, rows: &[Vec<Value>], index: usize) -> PyResult<ArrayRef> {
    let mut builder = make_builder(dtype, rows.len());
    for row in rows {
        append_value(builder.as_mut(), dtype, &row[index])?;
    }
    Ok(builder.finish())
}

fn float_value(value: &Value) -> PyResult<f64> {
    if let Some(number) = value.as_f64() {
        return Ok(number);
    }
    match value.get("$float").and_then(Value::as_str) {
        Some("nan") => Ok(f64::NAN),
        Some("inf") => Ok(f64::INFINITY),
        Some("-inf") => Ok(f64::NEG_INFINITY),
        _ => Err(value_error("invalid floating-point value")),
    }
}

fn append_value(builder: &mut dyn ArrayBuilder, dtype: &DataType, value: &Value) -> PyResult<()> {
    let invalid = || value_error(format!("invalid value for {dtype}"));
    macro_rules! primitive {
        ($builder:ty, $value:expr) => {{
            let builder = builder
                .as_any_mut()
                .downcast_mut::<$builder>()
                .ok_or_else(invalid)?;
            if value.is_null() {
                builder.append_null();
            } else {
                builder.append_value($value);
            }
        }};
    }
    match dtype {
        DataType::Int32 => primitive!(
            Int32Builder,
            i32::try_from(value.as_i64().ok_or_else(invalid)?).map_err(|_| invalid())?
        ),
        DataType::Int64 => primitive!(Int64Builder, value.as_i64().ok_or_else(invalid)?),
        DataType::Float32 => primitive!(Float32Builder, float_value(value)? as f32),
        DataType::Float64 => primitive!(Float64Builder, float_value(value)?),
        DataType::Boolean => primitive!(BooleanBuilder, value.as_bool().ok_or_else(invalid)?),
        DataType::Utf8 => primitive!(StringBuilder, value.as_str().ok_or_else(invalid)?),
        DataType::Struct(fields) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .ok_or_else(invalid)?;
            let object = if value.is_null() {
                None
            } else {
                Some(value.as_object().ok_or_else(invalid)?)
            };
            if let Some(object) = object {
                if object.len() != fields.len()
                    || fields
                        .iter()
                        .any(|field| !object.contains_key(field.name()))
                {
                    return Err(value_error("nested struct fields do not match schema"));
                }
            }
            for (field, child) in fields.iter().zip(builder.field_builders_mut()) {
                let value = object
                    .map(|object| &object[field.name()])
                    .unwrap_or(&Value::Null);
                append_value(child.as_mut(), field.data_type(), value)?;
            }
            builder.append(object.is_some());
        }
        DataType::List(field) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(invalid)?;
            if value.is_null() {
                builder.append(false);
            } else {
                for element in value.as_array().ok_or_else(invalid)? {
                    append_value(builder.values().as_mut(), field.data_type(), element)?;
                }
                builder.append(true);
            }
        }
        _ => return Err(value_error(format!("unsupported bridge type {dtype}"))),
    }
    Ok(())
}

#[derive(Clone)]
enum Operation {
    Order(Vec<(String, bool)>),
    Head(usize),
    Tail(usize),
    Filter(Expr, bool),
    Project(Vec<(String, Expr)>),
}

#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct NativeTable {
    runtime: Arc<Runtime>,
    schema: SchemaRef,
    source_schema: SchemaRef,
    partitions: Arc<Vec<Vec<RecordBatch>>>,
    operations: Vec<Operation>,
    row_count: Option<usize>,
}

#[pymethods]
impl NativeTable {
    fn filter_json(&self, expression_json: &str, keep: bool) -> PyResult<Self> {
        let expression: Expr = serde_json::from_str(expression_json).map_err(value_error)?;
        let dtype = expression
            .dtype(self.schema.as_ref())
            .map_err(value_error)?;
        if dtype != Type::Primitive(PrimitiveType::Bool) {
            return Err(value_error(format!(
                "filter predicate must be bool, got {dtype:?}"
            )));
        }
        let mut next = self.clone();
        next.operations.push(Operation::Filter(expression, keep));
        next.row_count = None;
        Ok(next)
    }

    fn project_json(&self, projections_json: &str) -> PyResult<Self> {
        let projections: Vec<(String, Expr)> =
            serde_json::from_str(projections_json).map_err(value_error)?;
        let schema =
            validate_projection(self.schema.as_ref(), &projections).map_err(value_error)?;
        let mut next = self.clone();
        next.schema = schema;
        next.operations.push(Operation::Project(projections));
        Ok(next)
    }

    fn schema_json(&self) -> PyResult<String> {
        let fields = self
            .schema
            .fields()
            .iter()
            .map(|field| Type::from_arrow(field.data_type()).map(|dtype| (field.name(), dtype)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(value_error)?;
        serde_json::to_string(&fields).map_err(value_error)
    }

    fn order_by(&self, keys: Vec<(String, bool)>) -> PyResult<Self> {
        for (name, _) in &keys {
            self.schema.index_of(name).map_err(value_error)?;
        }
        let mut next = self.clone();
        if !keys.is_empty() {
            next.operations.push(Operation::Order(keys));
        }
        Ok(next)
    }

    fn head(&self, n: usize) -> Self {
        let mut next = self.clone();
        next.operations.push(Operation::Head(n));
        next.row_count = next.row_count.map(|count| count.min(n));
        next
    }

    fn tail(&self, n: usize) -> Self {
        let mut next = self.clone();
        next.operations.push(Operation::Tail(n));
        next.row_count = next.row_count.map(|count| count.min(n));
        next
    }

    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        match self.row_count {
            Some(count) => Ok(count),
            None => self.force_count(py),
        }
    }

    fn force_count(&self, py: Python<'_>) -> PyResult<usize> {
        let table = self.clone();
        py.detach(move || table.runtime.block_on(table.evaluate_partitions()))
            .map(|parts| parts.iter().flatten().map(RecordBatch::num_rows).sum())
            .map_err(PyRuntimeError::new_err)
    }

    fn n_partitions(&self, py: Python<'_>) -> PyResult<usize> {
        let table = self.clone();
        py.detach(move || table.runtime.block_on(table.evaluate_partitions()))
            .map(|parts| parts.len())
            .map_err(PyRuntimeError::new_err)
    }

    fn collect_json(&self, py: Python<'_>) -> PyResult<String> {
        let table = self.clone();
        py.detach(move || table.runtime.block_on(table.evaluate()))
            .map_err(PyRuntimeError::new_err)
    }

    fn collect_field_json(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        let index = self.schema.index_of(name).map_err(value_error)?;
        let table = self.clone();
        py.detach(move || table.runtime.block_on(table.evaluate_field(index)))
            .map_err(PyRuntimeError::new_err)
    }
}

impl NativeTable {
    async fn evaluate_partitions(&self) -> Result<Vec<Vec<RecordBatch>>, String> {
        let mut partitions = self.partitions.as_ref().clone();
        let mut schema = self.source_schema.clone();
        let session = OrderedSession::default();
        for operation in &self.operations {
            match operation {
                Operation::Order(keys) => {
                    let requirement = OrderRequirement::new(
                        keys.iter()
                            .map(|(name, descending)| {
                                OrderKey::new(name.clone(), *descending, true)
                            })
                            .collect(),
                    )
                    .map_err(|e| e.to_string())?;
                    let batches = session
                        .collect(&partitions, schema.clone(), &requirement, None)
                        .await
                        .map_err(|e| e.to_string())?;
                    partitions = vec![batches];
                }
                Operation::Head(n) => {
                    let mut remaining = *n;
                    let mut prefix = Vec::new();
                    for partition in partitions {
                        if remaining == 0 {
                            break;
                        }
                        let mut retained = Vec::new();
                        for batch in partition {
                            let length = remaining.min(batch.num_rows());
                            if length > 0 {
                                retained.push(batch.slice(0, length));
                                remaining -= length;
                            }
                        }
                        if !retained.is_empty() {
                            prefix.push(retained);
                        }
                    }
                    partitions = prefix;
                }
                Operation::Tail(n) => {
                    let mut remaining = *n;
                    let mut suffix = Vec::new();
                    for partition in partitions.into_iter().rev() {
                        if remaining == 0 {
                            break;
                        }
                        let mut retained = Vec::new();
                        for batch in partition.into_iter().rev() {
                            let length = remaining.min(batch.num_rows());
                            if length > 0 {
                                retained.push(batch.slice(batch.num_rows() - length, length));
                                remaining -= length;
                            }
                        }
                        if !retained.is_empty() {
                            retained.reverse();
                            suffix.push(retained);
                        }
                    }
                    suffix.reverse();
                    partitions = suffix;
                }
                Operation::Filter(expression, keep) => {
                    let compiled = lower(expression, schema.as_ref()).map_err(|e| e.to_string())?;
                    partitions = partitions
                        .iter()
                        .map(|partition| {
                            partition
                                .iter()
                                .map(|batch| {
                                    let result = compiled
                                        .evaluate(batch)
                                        .and_then(|value| value.into_array(batch.num_rows()))
                                        .map_err(|e| e.to_string())?;
                                    let values = result
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .ok_or_else(|| {
                                            "compiled filter expression did not produce bool"
                                                .to_string()
                                        })?;
                                    if *keep {
                                        filter_record_batch(batch, values)
                                            .map_err(|e| e.to_string())
                                    } else {
                                        let mask =
                                            BooleanArray::from_iter((0..values.len()).map(|row| {
                                                values.is_valid(row).then_some(!values.value(row))
                                            }));
                                        filter_record_batch(batch, &mask).map_err(|e| e.to_string())
                                    }
                                })
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                }
                Operation::Project(projections) => {
                    let compiled = CompiledProjection::new(schema.as_ref(), projections)
                        .map_err(|e| e.to_string())?;
                    let output_schema = compiled.schema().clone();
                    partitions = partitions
                        .iter()
                        .map(|partition| {
                            partition
                                .iter()
                                .map(|batch| {
                                    compiled.project_batch(batch).map_err(|e| e.to_string())
                                })
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    schema = output_schema;
                }
            }
        }
        Ok(partitions)
    }

    async fn evaluate(&self) -> Result<String, String> {
        let partitions = self.evaluate_partitions().await?;
        let mut rows = Vec::new();
        for batch in partitions.iter().flatten() {
            for row in 0..batch.num_rows() {
                let values = batch
                    .columns()
                    .iter()
                    .map(|array| array_value(array, row))
                    .collect::<Result<Vec<_>, String>>()?;
                rows.push(values);
            }
        }
        serde_json::to_string(&rows).map_err(|e| e.to_string())
    }

    async fn evaluate_field(&self, index: usize) -> Result<String, String> {
        let partitions = self.evaluate_partitions().await?;
        let mut values = Vec::new();
        for batch in partitions.iter().flatten() {
            let array = batch.column(index);
            for row in 0..batch.num_rows() {
                values.push(array_value(array, row)?);
            }
        }
        serde_json::to_string(&values).map_err(|e| e.to_string())
    }
}

fn array_value(array: &ArrayRef, row: usize) -> Result<Value, String> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    match array.data_type() {
        DataType::Int32 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Int64 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Float32 => Ok(encode_float(f64::from(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row),
        ))),
        DataType::Float64 => Ok(encode_float(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Boolean => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        )),
        DataType::Utf8 => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row),
        )),
        DataType::Utf8View => Ok(Value::from(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row),
        )),
        DataType::Struct(fields) => {
            let values = array.as_any().downcast_ref::<StructArray>().unwrap();
            let object = fields
                .iter()
                .zip(values.columns())
                .map(|(field, column)| {
                    array_value(column, row).map(|value| (field.name().clone(), value))
                })
                .collect::<Result<serde_json::Map<_, _>, _>>()?;
            Ok(Value::Object(object))
        }
        DataType::List(_) => {
            let values = array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row);
            Ok(Value::Array(
                (0..values.len())
                    .map(|i| array_value(&values, i))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        other => Err(format!("unsupported output type {other}")),
    }
}

fn encode_float(value: f64) -> Value {
    if value.is_finite() {
        Value::from(value)
    } else {
        serde_json::json!({"$float": if value.is_nan() { "nan" } else if value.is_sign_positive() { "inf" } else { "-inf" }})
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeSession>()?;
    module.add_class::<NativeTable>()?;
    Ok(())
}
