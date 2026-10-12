//! Initial in-process Table bridge. The supported scalar subset is deliberately explicit.
use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    StringArray, StringViewArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use futures::TryStreamExt;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};
use vail_execution::expressions::{Expr, Type};
use vail_table::{RepartitionStrategy, TableError, TableIR, TableJoinKind, TableRef};

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

    /// Retain one Arrow row as native scalar globals. Values cross the Python
    /// boundary only as Arrow IPC, never through JSON.
    fn scalar_arrow(&self, data: &[u8]) -> PyResult<NativeGlobals> {
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)
            .map_err(value_error)?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(value_error)?;
        let batch = arrow_select::concat::concat_batches(&schema, &batches).map_err(value_error)?;
        if batch.num_rows() != 1 {
            return Err(value_error(
                "scalar Arrow input must contain exactly one row",
            ));
        }
        Ok(NativeGlobals {
            ir: vail_table::globals::GlobalsIR::arrow_literal(batch).map_err(table_error)?,
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

    /// Build a lazy DataFusion CSV/JSON scan. `schema_json` is an explicit
    /// list of `(field_name, wire_type)` pairs; input rows are never decoded in
    /// Python and no data batches are read during construction.
    fn read_text(
        &self,
        schema_json: &str,
        paths: Vec<String>,
        format: &str,
        options_json: &str,
    ) -> PyResult<NativeTable> {
        let fields: Vec<(String, Type)> = serde_json::from_str(schema_json).map_err(value_error)?;
        let schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dtype)| {
                    Ok(Field::new(
                        name,
                        dtype.to_arrow().map_err(value_error)?,
                        true,
                    ))
                })
                .collect::<PyResult<Vec<_>>>()?,
        ));
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            ir: TableIR::read_text(schema, paths, format, options_json).map_err(table_error)?,
        })
    }

    fn infer_text_schema(
        &self,
        py: Python<'_>,
        paths: Vec<String>,
        format: &str,
        options_json: &str,
    ) -> PyResult<String> {
        let schema = py
            .detach(|| vail_table::text_schema::infer(&paths, format, options_json))
            .map_err(table_error)?;
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                Ok((
                    field.name().clone(),
                    Type::from_arrow(field.data_type()).map_err(value_error)?,
                ))
            })
            .collect::<PyResult<Vec<_>>>()?;
        serde_json::to_string(&fields).map_err(value_error)
    }

    /// Arrow IPC is the sole row-data transport for in-memory table sources.
    fn parallelize_arrow(&self, data: &[u8], n_partitions: usize) -> PyResult<NativeTable> {
        if n_partitions == 0 {
            return Err(value_error("n_partitions must be positive"));
        }
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)
            .map_err(value_error)?;
        let schema = reader.schema();
        let mut names = HashSet::new();
        for field in schema.fields() {
            if !names.insert(field.name()) {
                return Err(value_error(format!("duplicate field {:?}", field.name())));
            }
            Type::from_arrow(field.data_type()).map_err(value_error)?;
        }
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(value_error)?;
        let batch = arrow_select::concat::concat_batches(&schema, &batches).map_err(value_error)?;
        let ir = TableIR::parallelize(batch, n_partitions).map_err(table_error)?;
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            ir,
        })
    }

    #[pyo3(signature = (expression_json, n_partitions, references=None))]
    fn parallelize_expression(
        &self,
        expression_json: &str,
        n_partitions: usize,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<NativeTable> {
        let expression: Expr = serde_json::from_str(expression_json).map_err(value_error)?;
        let ir =
            TableIR::parallelize_expression(expression, global_bindings(references)?, n_partitions)
                .map_err(table_error)?;
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

    fn multi_way_zip_join(
        &self,
        tables: Vec<PyRef<'_, NativeTable>>,
        data_field_name: String,
        global_field_name: String,
    ) -> PyResult<NativeTable> {
        let tables = tables.iter().map(|table| table.ir.clone()).collect();
        let ir = TableIR::multi_way_zip_join(tables, data_field_name, global_field_name)
            .map_err(table_error)?;
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            ir,
        })
    }
}

#[cfg(test)]
fn float_value(value: &Value) -> PyResult<f64> {
    if let Some(number) = value.as_f64() {
        return Ok(number);
    }
    if let Some(object) = value.as_object() {
        match (object.len(), object.get("$float").and_then(Value::as_str)) {
            (1, Some("nan")) => return Ok(f64::NAN),
            (1, Some("inf")) => return Ok(f64::INFINITY),
            (1, Some("-inf")) => return Ok(f64::NEG_INFINITY),
            (2, Some("nan")) => {
                let bits = float_nan_bits(object.get("bits"), 16)?;
                if bits & 0x7ff0000000000000 != 0x7ff0000000000000 || bits & 0x000fffffffffffff == 0
                {
                    return Err(value_error("NaN bit pattern does not encode a NaN"));
                }
                return Ok(f64::from_bits(bits));
            }
            _ => (),
        }
    }
    Err(value_error("invalid floating-point value"))
}

#[cfg(test)]
fn float32_value(value: &Value) -> PyResult<f32> {
    if let Some(number) = value.as_f64() {
        return Ok(number as f32);
    }
    if let Some(object) = value.as_object() {
        match (object.len(), object.get("$float").and_then(Value::as_str)) {
            (1, Some("nan")) => return Ok(f32::NAN),
            (1, Some("inf")) => return Ok(f32::INFINITY),
            (1, Some("-inf")) => return Ok(f32::NEG_INFINITY),
            (2, Some("nan")) => {
                let bits = float_nan_bits(object.get("bits"), 8)? as u32;
                if bits & 0x7f800000 != 0x7f800000 || bits & 0x007fffff == 0 {
                    return Err(value_error("NaN bit pattern does not encode a NaN"));
                }
                return Ok(f32::from_bits(bits));
            }
            _ => (),
        }
    }
    Err(value_error("invalid floating-point value"))
}

#[cfg(test)]
fn float_nan_bits(value: Option<&Value>, digits: usize) -> PyResult<u64> {
    let text = value
        .and_then(Value::as_str)
        .ok_or_else(|| value_error("NaN bit pattern must be fixed-width hexadecimal"))?;
    if text.len() != digits || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(value_error(
            "NaN bit pattern must be fixed-width hexadecimal",
        ));
    }
    u64::from_str_radix(text, 16).map_err(value_error)
}

#[pyclass(frozen)]
struct NativeGlobals {
    ir: vail_table::globals::GlobalsRef,
}

#[pymethods]
impl NativeGlobals {
    fn schema_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.ir.logical_type().map_err(table_error)?).map_err(value_error)
    }
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
    fn key_names(&self) -> Vec<String> {
        self.ir.keys().to_vec()
    }

    #[pyo3(signature = (other, keys_json, field, all_matches=false, references=None, query_interval=false))]
    fn lookup_json(
        &self,
        other: &NativeTable,
        keys_json: &str,
        field: String,
        all_matches: bool,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
        query_interval: bool,
    ) -> PyResult<Self> {
        let keys: Vec<Expr> = serde_json::from_str(keys_json).map_err(value_error)?;
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .lookup(
                    other.ir.clone(),
                    keys,
                    field,
                    global_bindings(references)?,
                    all_matches,
                    query_interval,
                )
                .map_err(table_error)?,
        })
    }

    fn join(&self, other: &NativeTable, how: &str, join_key: usize) -> PyResult<Self> {
        let kind = match how {
            "inner" => TableJoinKind::Inner,
            "left" => TableJoinKind::Left,
            "right" => TableJoinKind::Right,
            "outer" => TableJoinKind::Outer,
            "semi" => TableJoinKind::Semi,
            "anti" => TableJoinKind::Anti,
            _ => return Err(value_error(format!("unknown join type {how:?}"))),
        };
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .join(other.ir.clone(), kind, join_key)
                .map_err(table_error)?,
        })
    }

    fn explode(&self, path: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self.ir.explode(path).map_err(table_error)?,
        })
    }
    fn repartition(&self, n: usize, strategy: &str) -> PyResult<Self> {
        let strategy = match strategy {
            "shuffle" => RepartitionStrategy::Shuffle,
            "coalesce" => RepartitionStrategy::Coalesce,
            "naive_coalesce" => RepartitionStrategy::NaiveCoalesce,
            _ => {
                return Err(value_error(format!(
                    "unknown repartition strategy {strategy:?}"
                )));
            }
        };
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self.ir.repartition(n, strategy).map_err(table_error)?,
        })
    }
    fn filter_partitions(&self, parts: Vec<usize>, keep: bool) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .filter_partitions(parts, keep)
                .map_err(table_error)?,
        })
    }
    fn group_within_partitions(&self, name: String, n: usize) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .group_within_partitions(name, n)
                .map_err(table_error)?,
        })
    }
    #[pyo3(signature = (rows_name, body_json, references=None))]
    fn map_partitions_json(
        &self,
        rows_name: &str,
        body_json: &str,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let body: Expr = serde_json::from_str(body_json).map_err(value_error)?;
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .map_partitions(rows_name.to_owned(), body, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    fn calculate_new_partitions_json(&self, py: Python<'_>, n: usize) -> PyResult<String> {
        if n == 0 {
            return Ok("[]".to_owned());
        }
        let calculated = py
            .detach(|| self.runtime.block_on(self.ir.calculate_new_partitions(n)))
            .map_err(table_error)?;
        let keys = self.ir.keys();
        let mut encoded = Vec::with_capacity(calculated.len());
        for (start, end, includes_start, includes_end) in calculated {
            encoded.push(serde_json::json!({
                "start": scalar_key_json(&start, keys, self.ir.schema())?,
                "end": scalar_key_json(&end, keys, self.ir.schema())?,
                "includes_start": includes_start,
                "includes_end": includes_end,
            }));
        }
        serde_json::to_string(&encoded).map_err(value_error)
    }
    fn is_read_source(&self) -> bool {
        matches!(self.ir.node(), vail_table::TableNode::Read { .. })
    }

    fn filter_intervals_arrow(&self, data: &[u8], filter: bool) -> PyResult<Self> {
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)
            .map_err(value_error)?;
        if reader.schema().fields().len() != 1 {
            return Err(value_error("interval input must have exactly one column"));
        }
        let Type::Interval { interval } =
            Type::from_arrow(reader.schema().field(0).data_type()).map_err(value_error)?
        else {
            return Err(value_error("interval input must contain interval values"));
        };
        let mut literals = Vec::new();
        for batch in reader {
            let batch = batch.map_err(value_error)?;
            for index in 0..batch.num_rows() {
                if batch.column(0).is_null(index) {
                    return Err(value_error(
                        "interval input cannot contain missing intervals",
                    ));
                }
                literals.push(Expr::RuntimeLiteral {
                    value: ScalarValue::try_from_array(batch.column(0), index)
                        .map_err(value_error)?,
                });
            }
        }
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .filter_intervals(literals, *interval, filter)
                .map_err(table_error)?,
        })
    }
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
    fn export_text(
        &self,
        py: Python<'_>,
        path: String,
        format: &str,
        options_json: &str,
    ) -> PyResult<()> {
        py.detach(|| {
            self.runtime
                .block_on(self.ir.export_text(&path, format, options_json))
        })
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
    #[pyo3(signature = (keys_json, fields_json, references=None))]
    fn grouped_aggregate_json(
        &self,
        keys_json: &str,
        fields_json: &str,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let keys = serde_json::from_str(keys_json).map_err(value_error)?;
        let fields = serde_json::from_str(fields_json).map_err(value_error)?;
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .grouped_aggregate_bound(keys, fields, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    fn global_schema_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.ir.globals().logical_type().map_err(table_error)?)
            .map_err(value_error)
    }
    #[pyo3(signature = (expression_json, references=None))]
    fn aggregate_globals_json(
        &self,
        expression_json: &str,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<NativeGlobals> {
        let expression = serde_json::from_str(expression_json).map_err(value_error)?;
        let ir = vail_table::globals::GlobalsIR::aggregate(
            self.ir.clone(),
            expression,
            global_bindings(references)?,
        )
        .map_err(table_error)?;
        Ok(NativeGlobals { ir })
    }
    #[pyo3(signature = (expression_json, references=None))]
    fn aggregate_physical_plan(
        &self,
        expression_json: &str,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<String> {
        let expression = serde_json::from_str(expression_json).map_err(value_error)?;
        let ir = vail_table::globals::GlobalsIR::aggregate(
            self.ir.clone(),
            expression,
            global_bindings(references)?,
        )
        .map_err(table_error)?;
        ir.physical_plan().map_err(table_error)
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
    fn eval_global_arrow<'py>(
        &self,
        py: Python<'py>,
        expression: &str,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
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
        let mut bytes = Vec::new();
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())
            .map_err(value_error)?;
        writer.write(&batch).map_err(value_error)?;
        writer.finish().map_err(value_error)?;
        drop(writer);
        Ok(pyo3::types::PyBytes::new(py, &bytes))
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
    fn logical_plan(&self) -> PyResult<String> {
        self.ir.logical_plan().map_err(table_error)
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
    #[pyo3(signature = (other, unify=false))]
    fn union(&self, other: &NativeTable, unify: bool) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: TableIR::union(vec![self.ir.clone(), other.ir.clone()], unify)
                .map_err(table_error)?,
        })
    }
    fn distinct(&self) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self.ir.distinct().map_err(table_error)?,
        })
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
    fn collect_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let bytes = py.detach(|| {
            self.runtime.block_on(async {
                let schema = self.ir.schema().clone();
                let mut bytes = Vec::new();
                let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema)
                    .map_err(value_error)?;
                let mut input = self.ir.execute();
                while let Some(event) = input.try_next().await.map_err(table_error)? {
                    if let vail_table::PartitionEvent::Batch(batch) = event {
                        let batch = vail_storage::local_table::normalize(&batch, schema.clone())
                            .map_err(value_error)?;
                        writer.write(&batch).map_err(value_error)?;
                    }
                }
                writer.finish().map_err(value_error)?;
                drop(writer);
                Ok::<_, PyErr>(bytes)
            })
        })?;
        Ok(pyo3::types::PyBytes::new(py, &bytes))
    }

    /// Bounded metadata sampling. Unlike partition-preserving Table.head, this
    /// action does not count unknown source partitions before reading a prefix.
    fn sample_arrow<'py>(
        &self,
        py: Python<'py>,
        limit: usize,
    ) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let bytes = py.detach(|| {
            self.runtime.block_on(async {
                let batches = self.ir.sample(limit).await.map_err(table_error)?;
                let mut bytes = Vec::new();
                let mut writer =
                    arrow_ipc::writer::StreamWriter::try_new(&mut bytes, self.ir.schema())
                        .map_err(value_error)?;
                for batch in batches {
                    let batch =
                        vail_storage::local_table::normalize(&batch, self.ir.schema().clone())
                            .map_err(value_error)?;
                    writer.write(&batch).map_err(value_error)?;
                }
                writer.finish().map_err(value_error)?;
                drop(writer);
                Ok::<_, PyErr>(bytes)
            })
        })?;
        Ok(pyo3::types::PyBytes::new(py, &bytes))
    }
}

fn scalar_key_json(values: &[ScalarValue], keys: &[String], schema: &Schema) -> PyResult<Value> {
    if values.len() != keys.len() {
        return Err(value_error("partition key width does not match table key"));
    }
    let mut object = serde_json::Map::new();
    for (key, scalar) in keys.iter().zip(values) {
        let index = schema.index_of(key).map_err(value_error)?;
        let field = schema.field(index);
        let array = scalar.to_array_of_size(1).map_err(value_error)?;
        if array.data_type() != field.data_type() {
            return Err(value_error(format!(
                "partition key value for {key:?} has type {}, expected {}",
                array.data_type(),
                field.data_type()
            )));
        }
        object.insert(key.clone(), array_value(&array, 0).map_err(value_error)?);
    }
    Ok(Value::Object(object))
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
        DataType::Float32 => Ok(encode_float32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row),
        )),
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
            let special_names: Option<Vec<&str>> =
                if fields.len() == 4 && fields[0].name() == "__vail_interval_start" {
                    Some(vec!["start", "includes_start", "end", "includes_end"])
                } else {
                    None
                };
            let object = fields
                .iter()
                .zip(values.columns())
                .enumerate()
                .map(|(i, (field, column))| {
                    array_value(column, row).map(|mut value| {
                        if special_names.is_some()
                            && i == 1
                            && let Some(flag) = value.as_bool()
                        {
                            value = Value::Bool(!flag);
                        }
                        (
                            special_names
                                .as_ref()
                                .map_or_else(|| field.name().clone(), |n| n[i].to_owned()),
                            value,
                        )
                    })
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
    } else if value.is_nan() {
        serde_json::json!({"$float": "nan", "bits": format!("{:016x}", value.to_bits())})
    } else {
        serde_json::json!({"$float": if value.is_sign_positive() { "inf" } else { "-inf" }})
    }
}

fn encode_float32(value: f32) -> Value {
    if value.is_finite() {
        Value::from(f64::from(value))
    } else if value.is_nan() {
        serde_json::json!({"$float": "nan", "bits": format!("{:08x}", value.to_bits())})
    } else {
        serde_json::json!({"$float": if value.is_sign_positive() { "inf" } else { "-inf" }})
    }
}

#[cfg(test)]
mod float_wire_tests {
    use super::{encode_float, encode_float32, float_value, float32_value};
    use serde_json::json;

    #[test]
    fn float_nan_wire_preserves_payload_sign_and_signaling_bit() {
        for bits in [0x7f80_0001, 0x7fc0_1234, 0xffc0_1234] {
            let value = f32::from_bits(bits);
            let encoded = encode_float32(value);
            assert_eq!(
                encoded,
                json!({"$float":"nan", "bits":format!("{bits:08x}")})
            );
            assert_eq!(float32_value(&encoded).unwrap().to_bits(), bits);
        }
        for bits in [
            0x7ff0_0000_0000_0001,
            0x7ff8_0000_0000_1234,
            0xfff8_0000_0000_1234,
        ] {
            let value = f64::from_bits(bits);
            let encoded = encode_float(value);
            assert_eq!(
                encoded,
                json!({"$float":"nan", "bits":format!("{bits:016x}")})
            );
            assert_eq!(float_value(&encoded).unwrap().to_bits(), bits);
        }
    }

    #[test]
    fn float_nan_wire_rejects_malformed_tags_and_keeps_legacy_specials() {
        for value in [
            json!({"$float":"nan", "bits":"7ff"}),
            json!({"$float":"nan", "bits":"7ff0000000000000"}),
            json!({"$float":"nan", "bits":"7ff8000000000001", "extra":true}),
        ] {
            assert!(float_value(&value).is_err());
        }
        assert!(float_value(&json!({"$float":"nan"})).unwrap().is_nan());
        assert_eq!(
            float_value(&json!({"$float":"-inf"})).unwrap(),
            f64::NEG_INFINITY
        );
        assert_eq!(
            float32_value(&json!({"$float":"inf"})).unwrap(),
            f32::INFINITY
        );
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeSession>()?;
    module.add_class::<NativeTable>()?;
    module.add_class::<NativeGlobals>()?;
    Ok(())
}
