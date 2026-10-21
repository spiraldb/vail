mod collection_aggregate;
mod input_binding;
mod ir_view;
mod native_ir;
mod random_binding;
mod scan_mark;
use native_ir::{NativeExpr, NativeType};
mod arrow_memory;
use arrow_array::RecordBatchReader;
use arrow_memory::NativeArrow;
// In-process native Table bridge.
use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, StructArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use futures::TryStreamExt;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};
use vail_execution::expressions::{Expr, Type};
use vail_table::{RepartitionStrategy, TableError, TableIR, TableJoinKind, TableRef};

fn value_error(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Query execution failures share the public FatalError category. It remains a
/// RuntimeError subclass so callers using the native exception contract work too.
fn execution_error(error: impl std::fmt::Display) -> PyErr {
    let message = error.to_string();
    Python::attach(|py| {
        py.import("vail.utils")
            .and_then(|module| module.getattr("FatalError"))
            .and_then(|class| class.call1((&message,)))
            .map(PyErr::from_value)
            .unwrap_or_else(|_| PyRuntimeError::new_err(message))
    })
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
                        vail_execution::expressions::ExprError::Unsupported(_) => {
                            PyNotImplementedError::new_err(expr.to_string())
                        }
                        vail_execution::expressions::ExprError::DataFusion(_) => {
                            execution_error(expr)
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
            None => return execution_error(error),
        }
    }
}

#[pyclass]
struct NativeSession {
    runtime: Arc<Runtime>,
}

#[pymethods]
impl NativeSession {
    fn vcf_metadata<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let header = py
            .detach(|| vail_vcf::read_header(&path, &vail_vcf::Options::default()))
            .map_err(value_error)?;
        Ok(header.metadata.into_pyobject(py)?.into_any())
    }
    /// Read only headers now; retain a bounded lazy native VCF source for rows.
    fn read_vcf<'py>(
        &self,
        py: Python<'py>,
        paths: Vec<String>,
        header_file: Option<String>,
        options: &Bound<'_, PyAny>,
    ) -> PyResult<(NativeTable, Vec<String>, Bound<'py, PyAny>)> {
        let reference = options.get_item("reference")?;
        let indexed_region: Option<(String, Vec<String>, i32, i32)> = options
            .get_item("indexed_region")
            .ok()
            .filter(|value| !value.is_none())
            .map(|value| value.extract())
            .transpose()?;
        let options = vail_vcf::Options {
            reference: if reference.is_none() {
                None
            } else {
                Some(native_ir::reference(&reference)?)
            },
            contig_recoding: options.get_item("contig_recoding")?.extract()?,
            call_fields: options
                .get_item("call_fields")?
                .extract::<Vec<String>>()?
                .into_iter()
                .collect(),
            entry_float32: options.get_item("entry_float32")?.extract()?,
            array_elements_required: options.get_item("array_elements_required")?.extract()?,
            skip_invalid_loci: options.get_item("skip_invalid_loci")?.extract()?,
            drop_samples: options.get_item("drop_samples")?.extract()?,
            filter: options.get_item("filter")?.extract()?,
            find_replace: options.get_item("find_replace")?.extract()?,
        };
        let (header, options) = py
            .detach(|| -> Result<_, String> {
                let first = header_file
                    .as_ref()
                    .or(paths.first())
                    .ok_or("VCF input requires at least one path")?;
                let header = vail_vcf::read_header(first, &options)?;
                if header_file.is_none() {
                    for path in paths.iter().skip(1) {
                        header.validate_other(&vail_vcf::read_header(path, &options)?)?;
                    }
                }
                Ok((header, options))
            })
            .map_err(value_error)?;
        let schema = header.core_schema(&options).map_err(value_error)?;
        let samples = if options.drop_samples {
            Vec::new()
        } else {
            header.samples.clone()
        };
        let metadata = header.metadata.clone().into_pyobject(py)?.into_any();
        let indexed_region = indexed_region.map(|(executable, contigs, start, end)| {
            serde_json::json!({"executable": executable, "contigs": contigs, "start": start, "end": end})
        });
        let config = serde_json::json!({"header": header, "options": options, "indexed_region": indexed_region});
        let ir = TableIR::read_text(schema, paths, "vcf", &config).map_err(table_error)?;
        Ok((
            NativeTable {
                runtime: self.runtime.clone(),
                ir,
            },
            samples,
            metadata,
        ))
    }
    /// Read only the object-container header, never Python data rows.
    fn avro_schema(&self, py: Python<'_>, path: String) -> PyResult<String> {
        py.detach(|| vail_table::avro_source::read_schema(&path))
            .map_err(value_error)
    }
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(vail_table::EXECUTION_CONCURRENCY)
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    /// Retain separate lazy scalar dependencies behind a conditional boundary.
    #[pyo3(signature = (condition, consequent, alternate, references=None))]
    fn conditional_globals(
        &self,
        condition: &NativeExpr,
        consequent: &NativeExpr,
        alternate: &NativeExpr,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<NativeGlobals> {
        if ![condition, consequent, alternate]
            .iter()
            .all(|value| value.inner.is_closed_scalar())
        {
            return Err(value_error(
                "conditional globals require closed scalar expressions",
            ));
        }
        let bindings = global_bindings(references)?;
        let base = vail_table::globals::GlobalsIR::empty();
        let make = |value: &NativeExpr| {
            base.map_bound(
                vec![("value".into(), value.inner.as_ref().clone())],
                bindings.clone(),
            )
            .map_err(table_error)
        };
        Ok(NativeGlobals {
            ir: vail_table::globals::GlobalsIR::conditional(
                make(condition)?,
                make(consequent)?,
                make(alternate)?,
            )
            .map_err(table_error)?,
        })
    }
    /// Retain one Arrow row as native scalar globals using the C Stream protocol.
    fn scalar_arrow(&self, data: &Bound<'_, PyAny>) -> PyResult<NativeGlobals> {
        let batch = arrow_memory::scalar(data)?;
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

    /// Build a lazy DataFusion CSV/JSON scan. `schema` is an explicit
    /// list of `(field_name, NativeType)` pairs; input rows are never decoded in
    /// Python and no data batches are read during construction.
    fn read_text(
        &self,
        schema: Vec<(String, PyRef<'_, NativeType>)>,
        paths: Vec<String>,
        format: &str,
        options: &Bound<'_, PyAny>,
    ) -> PyResult<NativeTable> {
        let fields = schema.into_iter().map(|(n, t)| (n, (*t.inner).clone()));
        let schema = Arc::new(Schema::new(
            fields
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
            ir: TableIR::read_text(schema, paths, format, &native_ir::option_value(options)?)
                .map_err(table_error)?,
        })
    }

    fn infer_text_schema(
        &self,
        py: Python<'_>,
        paths: Vec<String>,
        format: &str,
        options: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<(String, NativeType)>> {
        let options = native_ir::option_value(options)?;
        let schema = py
            .detach(|| vail_table::text_schema::infer(&paths, format, &options))
            .map_err(table_error)?;
        native_schema(&schema)
    }

    /// Import an Arrow C Stream, retaining the producer's data buffers.
    fn parallelize_arrow(
        &self,
        data: &Bound<'_, PyAny>,
        n_partitions: usize,
    ) -> PyResult<NativeTable> {
        if n_partitions == 0 {
            return Err(value_error("n_partitions must be positive"));
        }
        let reader = arrow_memory::import(data)?;
        let schema = reader.schema();
        let mut names = HashSet::new();
        for field in schema.fields() {
            if !names.insert(field.name()) {
                return Err(value_error(format!("duplicate field {:?}", field.name())));
            }
            Type::from_arrow(field.data_type()).map_err(value_error)?;
        }
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(value_error)?;
        let ir =
            TableIR::parallelize_batches(schema, batches, n_partitions).map_err(table_error)?;
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            ir,
        })
    }

    #[pyo3(signature = (expression, n_partitions, references=None))]
    fn parallelize_expression(
        &self,
        expression: &NativeExpr,
        n_partitions: usize,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<NativeTable> {
        let expression = (*expression.inner).clone();
        let ir =
            TableIR::parallelize_expression(expression, global_bindings(references)?, n_partitions)
                .map_err(table_error)?;
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            ir,
        })
    }

    #[pyo3(signature = (n, n_partitions, batch_size=65536))]
    fn range_table(
        &self,
        n: usize,
        n_partitions: usize,
        batch_size: usize,
    ) -> PyResult<NativeTable> {
        let ir = TableIR::range_batched(n, n_partitions, batch_size).map_err(table_error)?;
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

#[pyclass(frozen)]
struct NativeGlobals {
    ir: vail_table::globals::GlobalsRef,
}

#[pymethods]
impl NativeGlobals {
    fn schema(&self) -> PyResult<Vec<(String, NativeType)>> {
        native_schema(self.ir.schema())
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

    fn query_prefix(&self, count: usize, key: String, payload: String) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .query_prefix(count, key, payload)
                .map_err(table_error)?,
        })
    }
    #[pyo3(signature = (other, keys, field, all_matches=false, references=None, query_interval=false))]
    fn lookup(
        &self,
        other: &NativeTable,
        keys: Vec<PyRef<'_, NativeExpr>>,
        field: String,
        all_matches: bool,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
        query_interval: bool,
    ) -> PyResult<Self> {
        let keys = keys.iter().map(|v| (*v.inner).clone()).collect();
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

    fn rename_fields(&self, mapping: std::collections::BTreeMap<String, String>) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self.ir.rename_fields(&mapping).map_err(table_error)?,
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
    #[pyo3(signature = (rows_name, body, references=None))]
    fn map_partitions(
        &self,
        rows_name: &str,
        body: &NativeExpr,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let body = (*body.inner).clone();
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .map_partitions(rows_name.to_owned(), body, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    fn calculate_new_partitions(&self, py: Python<'_>, n: usize) -> PyResult<NativeArrow> {
        let calculated = if n == 0 {
            Vec::new()
        } else {
            py.detach(|| self.runtime.block_on(self.ir.calculate_new_partitions(n)))
                .map_err(table_error)?
        };
        let key_fields = self
            .ir
            .keys()
            .iter()
            .map(|key| {
                self.ir
                    .schema()
                    .field_with_name(key)
                    .map(|f| Arc::new(f.clone().with_nullable(true)))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(value_error)?;
        let point_type = DataType::Struct(key_fields.clone().into());
        let interval_type = Type::Interval {
            interval: Box::new(Type::from_arrow(&point_type).map_err(value_error)?),
        }
        .to_arrow()
        .map_err(value_error)?;
        let DataType::Struct(interval_fields) = &interval_type else {
            unreachable!()
        };
        let mut values = Vec::new();
        for (start, end, includes_start, includes_end) in calculated {
            let point = |components: Vec<ScalarValue>| -> PyResult<ArrayRef> {
                let columns = components
                    .into_iter()
                    .map(|v| v.to_array_of_size(1))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(value_error)?;
                Ok(Arc::new(
                    StructArray::try_new(key_fields.clone().into(), columns, None)
                        .map_err(value_error)?,
                ))
            };
            values.push(Arc::new(
                StructArray::try_new(
                    interval_fields.clone(),
                    vec![
                        point(start)?,
                        Arc::new(BooleanArray::from(vec![!includes_start])),
                        point(end)?,
                        Arc::new(BooleanArray::from(vec![includes_end])),
                    ],
                    None,
                )
                .map_err(value_error)?,
            ) as ArrayRef);
        }
        let array = if values.is_empty() {
            arrow_array::new_empty_array(&interval_type)
        } else {
            arrow_select::concat::concat(&values.iter().map(|v| v.as_ref()).collect::<Vec<_>>())
                .map_err(value_error)?
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "interval",
            interval_type,
            true,
        )]));
        Ok(NativeArrow::single(
            arrow_array::RecordBatch::try_new(schema, vec![array]).map_err(value_error)?,
        ))
    }
    fn is_read_source(&self) -> bool {
        self.ir.can_pushdown_interval_read()
    }

    fn filter_intervals_arrow(&self, data: &Bound<'_, PyAny>, filter: bool) -> PyResult<Self> {
        let reader = arrow_memory::import(data)?;
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
    fn bgen_select(&self, variants: &NativeTable) -> PyResult<NativeTable> {
        Ok(NativeTable {
            runtime: self.runtime.clone(),
            ir: self.ir.bgen_select(variants.ir.clone()).map_err(table_error)?,
        })
    }
    fn export_bgen(
        &self, py: Python<'_>, path: String, samples: Vec<String>,
        parallel: Option<String>, compression_codec: String,
    ) -> PyResult<()> {
        py.detach(|| self.runtime.block_on(vail_table::bgen_export::export(
            self.ir.clone(), &path, &samples, parallel.as_deref(), &compression_codec,
        ))).map_err(table_error)
    }
    fn export_vcf(
        &self,
        py: Python<'_>,
        path: String,
        header: Vec<String>,
        entries_name: String,
        entry_names: Vec<String>,
        sample_count: usize,
    ) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(vail_table::vcf_export::export(
                self.ir.clone(),
                &path,
                &header,
                &entries_name,
                &entry_names,
                sample_count,
            ))
        })
        .map_err(table_error)
    }
    fn export_plink(&self, py: Python<'_>, path: String, sample_count: usize) -> PyResult<()> {
        py.detach(|| self.runtime.block_on(vail_table::plink_export::export(
            self.ir.clone(), &path, sample_count,
        ))).map_err(table_error)
    }
    fn export_text(
        &self,
        py: Python<'_>,
        path: String,
        format: &str,
        options: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let options = native_ir::option_value(options)?;
        py.detach(|| {
            self.runtime
                .block_on(self.ir.export_text(&path, format, &options))
        })
        .map_err(table_error)
    }
    #[pyo3(signature = (expression, keep, references=None))]
    fn filter(
        &self,
        expression: &NativeExpr,
        keep: bool,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let expression = (*expression.inner).clone();
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .filter_bound(expression, keep, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    #[pyo3(signature = (projections, references=None))]
    fn project(
        &self,
        projections: Vec<(String, PyRef<'_, NativeExpr>)>,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let projections = projections
            .into_iter()
            .map(|(n, v)| (n, (*v.inner).clone()))
            .collect();
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .map_rows_bound(projections, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    #[pyo3(signature = (keys, fields, references=None))]
    fn grouped_aggregate(
        &self,
        keys: Vec<(String, PyRef<'_, NativeExpr>)>,
        fields: Vec<(String, PyRef<'_, NativeExpr>)>,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let keys = keys
            .into_iter()
            .map(|(n, v)| (n, (*v.inner).clone()))
            .collect();
        let fields = fields
            .into_iter()
            .map(|(n, v)| (n, (*v.inner).clone()))
            .collect();
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self
                .ir
                .grouped_aggregate_bound(keys, fields, global_bindings(references)?)
                .map_err(table_error)?,
        })
    }
    fn global_schema(&self) -> PyResult<Vec<(String, NativeType)>> {
        native_schema(self.ir.globals().schema())
    }
    #[pyo3(signature = (expression, references=None))]
    fn aggregate_globals(
        &self,
        expression: &NativeExpr,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<NativeGlobals> {
        let expression = (*expression.inner).clone();
        let ir = vail_table::globals::GlobalsIR::aggregate(
            self.ir.clone(),
            expression,
            global_bindings(references)?,
        )
        .map_err(table_error)?;
        Ok(NativeGlobals { ir })
    }
    #[pyo3(signature = (expression, references=None))]
    fn aggregate_physical_plan(
        &self,
        expression: &NativeExpr,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<String> {
        let expression = (*expression.inner).clone();
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
    fn project_globals(
        &self,
        fields: Vec<(String, PyRef<'_, NativeExpr>)>,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<Self> {
        let fields = fields
            .into_iter()
            .map(|(n, v)| (n, (*v.inner).clone()))
            .collect();
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
        expression: &NativeExpr,
        references: Option<Vec<(String, PyRef<'_, NativeGlobals>)>>,
    ) -> PyResult<NativeArrow> {
        let expression = (*expression.inner).clone();
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
        Ok(NativeArrow::single(batch))
    }

    fn schema(&self) -> PyResult<Vec<(String, NativeType)>> {
        native_schema(self.ir.schema())
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
    fn ir<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(ir_view::table(py, &self.ir)?.into_any())
    }
    fn key_by(&self, keys: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self.ir.key_by(keys).map_err(table_error)?,
        })
    }
    fn with_keys(&self, keys: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            runtime: self.runtime.clone(),
            ir: self.ir.with_keys(keys).map_err(table_error)?,
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
            None => py
                .detach(|| self.runtime.block_on(self.ir.count()))
                .map_err(table_error),
        }
    }
    fn force_count(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| self.runtime.block_on(self.ir.force_count()))
            .map_err(table_error)
    }
    fn n_partitions(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| self.runtime.block_on(self.ir.n_partitions()))
            .map_err(table_error)
    }
    fn cache(&self, py: Python<'_>) -> PyResult<Self> {
        let ir = py.detach(|| self.runtime.block_on(self.ir.cache())).map_err(table_error)?;
        Ok(Self { runtime: self.runtime.clone(), ir })
    }
    fn collect_arrow<'py>(&self, py: Python<'py>) -> PyResult<NativeArrow> {
        let batches = py.detach(|| {
            self.runtime.block_on(async {
                let schema = self.ir.schema().clone();
                let mut batches = Vec::new();
                let mut input = self.ir.execute();
                while let Some(event) = input.try_next().await.map_err(table_error)? {
                    if let vail_table::PartitionEvent::Batch(batch) = event {
                        batches.push(
                            vail_storage::local_table::normalize(&batch, schema.clone())
                                .map_err(value_error)?,
                        );
                    }
                }
                Ok::<_, PyErr>(batches)
            })
        })?;
        Ok(NativeArrow {
            schema: self.ir.schema().clone(),
            batches,
        })
    }

    fn sample_arrow(&self, py: Python<'_>, limit: usize) -> PyResult<NativeArrow> {
        let batches = py
            .detach(|| self.runtime.block_on(self.ir.sample(limit)))
            .map_err(table_error)?;
        Ok(NativeArrow {
            schema: self.ir.schema().clone(),
            batches,
        })
    }
}

#[pyfunction]
fn supports_list_reducer(function: &str) -> bool {
    vail_execution::supports_list_reducer(function)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(supports_list_reducer, module)?)?;
    module.add_class::<NativeSession>()?;
    module.add_class::<NativeArrow>()?;
    module.add_class::<NativeExpr>()?;
    module.add_class::<NativeType>()?;
    module.add_class::<NativeTable>()?;
    module.add_class::<NativeGlobals>()?;
    Ok(())
}

fn native_schema(schema: &arrow_schema::Schema) -> PyResult<Vec<(String, NativeType)>> {
    schema
        .fields()
        .iter()
        .map(|f| {
            Ok((
                f.name().clone(),
                NativeType::new_type(Type::from_arrow(f.data_type()).map_err(value_error)?),
            ))
        })
        .collect()
}

fn vail_scalar_at(array: &ArrayRef, row: usize) -> PyResult<ScalarValue> {
    vail_execution::expressions::scalar_at(array, row).map_err(value_error)
}
