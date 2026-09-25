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
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};
use vail_execution::expressions::{Expr, Type};
use vail_table::{TableError, TableIR, TableRef};

fn value_error(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn table_error(error: TableError) -> PyErr {
    use pyo3::exceptions::PyNotImplementedError;
    use vail_storage::StorageError;
    let mut current: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(table) = current.downcast_ref::<TableError>() {
            match table {
                TableError::Storage(storage) => {
                    current = storage;
                    continue;
                }
                TableError::Invalid(_) | TableError::Arrow(_) => return value_error(table),
                TableError::Expression(expr) => {
                    return match expr {
                        vail_execution::expressions::ExprError::DataFusion(_) => {
                            PyRuntimeError::new_err(expr.to_string())
                        }
                        _ => value_error(expr),
                    };
                }
                _ => (),
            }
        }
        if let Some(storage) = current.downcast_ref::<StorageError>() {
            match storage {
                StorageError::Io(io) => {
                    return std::io::Error::new(io.kind(), io.to_string()).into();
                }
                StorageError::Invalid(_) | StorageError::Json(_) => return value_error(storage),
                StorageError::Unsupported(_) => {
                    return PyNotImplementedError::new_err(storage.to_string());
                }
                _ => (),
            }
        }
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            return std::io::Error::new(io.kind(), io.to_string()).into();
        }
        match current.source() {
            Some(source) => current = source,
            None => return PyRuntimeError::new_err(error.to_string()),
        }
    }
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

    fn read_table(&self, py: Python<'_>, path: String) -> PyResult<(NativeTable, Vec<String>)> {
        let ir = py
            .detach(|| self.runtime.block_on(TableIR::read(&path)))
            .map_err(table_error)?;
        let keys = ir.keys().to_vec();
        Ok((
            NativeTable {
                runtime: self.runtime.clone(),
                ir,
            },
            keys,
        ))
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
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| build_column(field.data_type(), &rows, index))
            .collect::<PyResult<Vec<_>>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        let batch =
            RecordBatch::try_new_with_options(schema, columns, &options).map_err(value_error)?;
        let ir = TableIR::parallelize(batch, n_partitions).map_err(table_error)?;
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            ir,
        })
    }

    fn range_table(&self, n: usize, n_partitions: usize) -> PyResult<NativeTable> {
        let ir = TableIR::range(n, n_partitions).map_err(table_error)?;
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            ir,
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
                let canonical;
                let elements = if let Type::Set { set } =
                    Type::from_arrow(dtype).map_err(value_error)?
                {
                    canonical = vail_execution::expressions::canonical_set_elements(value, &set)
                        .map_err(value_error)?;
                    &canonical
                } else {
                    value.as_array().ok_or_else(invalid)?
                };
                for element in elements {
                    append_value(builder.values().as_mut(), field.data_type(), element)?;
                }
                builder.append(true);
            }
        }
        _ => return Err(value_error(format!("unsupported bridge type {dtype}"))),
    }
    Ok(())
}

/// A native, immutable reference to a singleton IR. It owns no table row source.
#[pyclass(frozen)]
struct NativeGlobals {
    ir: vail_table::globals::GlobalsRef,
}

fn global_bindings(
    references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
) -> PyResult<vail_table::globals::GlobalBindings> {
    let mut bindings = vail_table::globals::GlobalBindings::new();
    for (name, target) in references.unwrap_or_default() {
        if bindings.insert(name, target.ir.clone()).is_some() {
            return Err(value_error("duplicate external global binding"));
        }
    }
    Ok(bindings)
}

#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct NativeTable {
    runtime: Arc<Runtime>,
    ir: TableRef,
}

#[pymethods]
impl NativeTable {
    #[pyo3(signature = (other, tolerance, absolute, reorder_fields))]
    fn same(
        &self,
        py: Python<'_>,
        other: &NativeTable,
        tolerance: Option<f64>,
        absolute: bool,
        reorder_fields: bool,
    ) -> PyResult<(bool, Option<String>)> {
        let result = py
            .detach(|| {
                self.runtime.block_on(self.ir.same(
                    &other.ir,
                    vail_table::comparison::SameOptions {
                        tolerance,
                        absolute,
                        reorder_fields,
                    },
                ))
            })
            .map_err(table_error)?;
        Ok((result.is_same, result.diagnostic))
    }
    fn write(&self, py: Python<'_>, path: String, overwrite: bool) -> PyResult<()> {
        py.detach(|| self.runtime.block_on(self.ir.write(&path, overwrite)))
            .map_err(table_error)
    }
    #[pyo3(signature = (expression_json, keep, references=None))]
    fn filter_json(
        &self,
        expression_json: &str,
        keep: bool,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let expression: Expr = serde_json::from_str(expression_json).map_err(value_error)?;
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .filter_bound(expression, keep, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    #[pyo3(signature = (projections_json, references=None))]
    fn project_json(
        &self,
        projections_json: &str,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let projections: Vec<(String, Expr)> =
            serde_json::from_str(projections_json).map_err(value_error)?;
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .map_rows_bound(projections, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    fn global_schema_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.ir.globals().logical_type().map_err(table_error)?)
            .map_err(value_error)
    }
    fn globals_ref(&self) -> NativeGlobals {
        NativeGlobals {
            ir: self.ir.globals().clone(),
        }
    }
    #[pyo3(signature = (fields, references=None))]
    fn project_globals_json(
        &self,
        fields: &str,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let fields = serde_json::from_str(fields).map_err(value_error)?;
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .map_globals_bound(fields, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    #[pyo3(signature = (expression, references=None))]
    fn eval_global_json(
        &self,
        py: Python<'_>,
        expression: &str,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<String> {
        let expression = serde_json::from_str(expression).map_err(value_error)?;
        let globals = self
            .ir
            .globals()
            .map_bound(
                vec![("value".into(), expression)],
                global_bindings(references)?,
            )
            .map_err(table_error)?;
        let batch = py
            .detach(|| self.runtime.block_on(globals.evaluate()))
            .map_err(table_error)?;
        serde_json::to_string(&array_value(batch.column(0), 0).map_err(value_error)?)
            .map_err(value_error)
    }
    fn schema_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.ir.row_type().map_err(table_error)?).map_err(value_error)
    }
    #[pyo3(signature = (other, tolerance=Some(1e-6), absolute=false, reorder_fields=false))]
    fn same_physical_plan(
        &self,
        other: &Self,
        tolerance: Option<f64>,
        absolute: bool,
        reorder_fields: bool,
    ) -> PyResult<String> {
        self.ir
            .same_physical_plan(
                &other.ir,
                vail_table::comparison::SameOptions {
                    tolerance,
                    absolute,
                    reorder_fields,
                },
            )
            .map_err(table_error)
    }
    fn physical_plan(&self) -> PyResult<String> {
        self.ir.physical_plan().map_err(table_error)
    }
    fn ir_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.ir.explain()).map_err(value_error)
    }
    fn key_by(&self, keys: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self.ir.key_by(keys).map_err(table_error)?,
        })
    }
    fn order_by(&self, keys: Vec<(String, bool)>) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self.ir.order_by(keys).map_err(table_error)?,
        })
    }
    fn head(&self, n: usize) -> Self {
        Self {
            runtime: self.runtime.clone(),
            ir: self.ir.head(n),
        }
    }
    fn tail(&self, n: usize) -> Self {
        Self {
            runtime: self.runtime.clone(),
            ir: self.ir.tail(n),
        }
    }
    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        match self.ir.known_count() {
            Some(count) => Ok(count),
            None => self.force_count(py),
        }
    }
    fn force_count(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| self.runtime.block_on(self.ir.count()))
            .map_err(table_error)
    }
    fn n_partitions(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| self.runtime.block_on(self.ir.n_partitions()))
            .map_err(table_error)
    }
    fn collect_json(&self, py: Python<'_>) -> PyResult<String> {
        py.detach(|| self.runtime.block_on(self.evaluate()))
    }
    fn collect_field_json(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        let index = self.ir.schema().index_of(name).map_err(value_error)?;
        py.detach(|| self.runtime.block_on(self.evaluate_field(index)))
    }
}

impl NativeTable {
    async fn evaluate(&self) -> PyResult<String> {
        let mut input = self.ir.execute();
        let mut rows = Vec::new();
        while let Some(event) = input.try_next().await.map_err(table_error)? {
            if let vail_table::PartitionEvent::Batch(batch) = event {
                for row in 0..batch.num_rows() {
                    let values = batch
                        .columns()
                        .iter()
                        .map(|array| array_value(array, row))
                        .collect::<Result<Vec<_>, String>>()
                        .map_err(PyRuntimeError::new_err)?;
                    rows.push(values);
                }
            }
        }
        serde_json::to_string(&rows).map_err(value_error)
    }

    async fn evaluate_field(&self, index: usize) -> PyResult<String> {
        let mut input = self.ir.execute();
        let mut values = Vec::new();
        while let Some(event) = input.try_next().await.map_err(table_error)? {
            if let vail_table::PartitionEvent::Batch(batch) = event {
                for row in 0..batch.num_rows() {
                    values.push(
                        array_value(batch.column(index), row).map_err(PyRuntimeError::new_err)?,
                    );
                }
            }
        }
        serde_json::to_string(&values).map_err(value_error)
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
    module.add_class::<NativeGlobals>()?;
    Ok(())
}
