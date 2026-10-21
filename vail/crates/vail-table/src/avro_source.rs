//! Native Avro object-container scan with bounded batch backpressure.
use apache_avro::{Reader, types::Value};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::{
    PartitionedFile, TableSchema,
    file::FileSource,
    file_scan_config::FileScanConfig,
    file_stream::{FileOpenFuture, FileOpener},
};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;
use std::{fmt, sync::Arc};

pub fn read_schema(path: &str) -> std::result::Result<String, String> {
    let reader = Reader::new(std::fs::File::open(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    serde_json::to_string(reader.writer_schema()).map_err(|e| e.to_string())
}
#[derive(Clone, Debug)]
pub struct AvroSource {
    schema: TableSchema,
    batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
    projection: Option<datafusion_physical_expr::projection::ProjectionExprs>,
}
impl AvroSource {
    pub fn new(schema: SchemaRef, _value: &serde_json::Value) -> Result<Self> {
        Ok(Self {
            schema: TableSchema::from(schema),
            batch_size: 8192,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: None,
        })
    }
}
impl FileSource for AvroSource {
    fn create_file_opener(
        &self,
        _store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(AvroOpener {
            source: self.clone(),
        }))
    }
    fn table_schema(&self) -> &TableSchema {
        &self.schema
    }
    fn projection(&self) -> Option<&datafusion_physical_expr::projection::ProjectionExprs> {
        self.projection.as_ref()
    }
    fn try_pushdown_projection(
        &self,
        projection: &datafusion_physical_expr::projection::ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        if !projection.expr_iter().all(|expr| {
            expr.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                .is_some()
        }) {
            return Ok(None);
        }
        let mut source = self.clone();
        source.projection = Some(projection.clone());
        Ok(Some(Arc::new(source)))
    }
    fn with_batch_size(&self, size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = size.max(1);
        Arc::new(source)
    }
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }
    fn file_type(&self) -> &str {
        "avro"
    }
    fn supports_repartitioning(&self) -> bool {
        false
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_plan::PhysicalExpr>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        datafusion_physical_plan::apply_expression_roots(
            std::iter::empty::<Arc<dyn datafusion_physical_plan::PhysicalExpr>>(),
            f,
        )
    }
    fn fmt_extra(
        &self,
        _t: datafusion_physical_plan::display::DisplayFormatType,
        _f: &mut fmt::Formatter,
    ) -> fmt::Result {
        Ok(())
    }
}
struct AvroOpener {
    source: AvroSource,
}
impl FileOpener for AvroOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "BGEN byte-range partitions are not implemented".into(),
            ));
        }
        let source = self.source.clone();
        let path = format!("/{}", file.object_meta.location);
        Ok(Box::pin(async move {
            let (sender, receiver) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(2);
            tokio::task::spawn_blocking(move || {
                let run = || -> std::result::Result<(), String> {
                    scan(&path, &source, &sender)?;
                    Ok(())
                };
                if let Err(error) = run() {
                    let _ = sender.blocking_send(Err(DataFusionError::Execution(error)));
                }
            });
            let stream = futures::stream::unfold(receiver, |mut receiver| async move {
                receiver.recv().await.map(|batch| (batch, receiver))
            });
            Ok(Box::pin(stream) as futures::stream::BoxStream<'static, Result<RecordBatch>>)
        }))
    }
}

fn ununion(v: &Value) -> &Value {
    match v {
        Value::Union(_, v) => ununion(v),
        _ => v,
    }
}
fn column(ty: &DataType, values: &[&Value]) -> std::result::Result<ArrayRef, String> {
    use arrow_array::*;
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    let values: Vec<_> = values.iter().map(|v| ununion(v)).collect();
    macro_rules! primitive { ($array:ty, $($pat:pat => $expr:expr),+) => {{
        let data = values.iter().map(|v| match v { Value::Null => Ok(None), $($pat => Ok(Some($expr)),)+ _ => Err(format!("Avro value does not match {ty}")) }).collect::<std::result::Result<Vec<_>, String>>()?;
        Arc::new(<$array>::from(data)) as ArrayRef
    }}; }
    Ok(match ty {
        DataType::Boolean => primitive!(BooleanArray, Value::Boolean(x) => *x),
        DataType::Int32 => {
            primitive!(Int32Array, Value::Int(x) | Value::Date(x) | Value::TimeMillis(x) => *x)
        }
        DataType::Int64 => {
            primitive!(Int64Array, Value::Long(x) | Value::TimeMicros(x) | Value::TimestampMillis(x) | Value::TimestampMicros(x) | Value::TimestampNanos(x) | Value::LocalTimestampMillis(x) | Value::LocalTimestampMicros(x) | Value::LocalTimestampNanos(x) => *x)
        }
        DataType::Float32 => primitive!(Float32Array, Value::Float(x) => *x),
        DataType::Float64 => primitive!(Float64Array, Value::Double(x) => *x),
        DataType::Utf8 => {
            let mut out = builder::StringBuilder::new();
            for v in &values {
                match v {
                    Value::Null => out.append_null(),
                    Value::String(s) | Value::Enum(_, s) => out.append_value(s),
                    Value::Uuid(u) => out.append_value(u.to_string()),
                    _ => return Err("Avro value is not a string".into()),
                }
            }
            Arc::new(out.finish())
        }
        DataType::Struct(fields) => {
            let mut arrays = Vec::new();
            for (index, field) in fields.iter().enumerate() {
                let children = values
                    .iter()
                    .map(|v| match v {
                        Value::Null => Ok(&Value::Null),
                        Value::Record(record) => record
                            .get(index)
                            .filter(|(name, _)| name == field.name())
                            .map(|(_, v)| v)
                            .ok_or("Avro record fields do not match schema"),
                        _ => Err("Avro value is not a record"),
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                arrays.push(column(field.data_type(), &children)?);
            }
            Arc::new(
                StructArray::try_new_with_length(
                    fields.clone(),
                    arrays,
                    Some(NullBuffer::from(
                        values
                            .iter()
                            .map(|v| !matches!(v, Value::Null))
                            .collect::<Vec<_>>(),
                    )),
                    values.len(),
                )
                .map_err(|e| e.to_string())?,
            )
        }
        DataType::List(field) => {
            let mut offsets = vec![0i32];
            let mut items = Vec::new();
            let mut keys = Vec::new();
            let mut map = false;
            for v in &values {
                match v {
                    Value::Null => {}
                    Value::Array(a) => items.extend(a.iter()),
                    Value::Map(m) => {
                        map = true;
                        for (k, v) in m {
                            keys.push(k.as_str());
                            items.push(v);
                        }
                    }
                    _ => return Err("Avro value is not an array or map".into()),
                }
                offsets.push(
                    i32::try_from(items.len())
                        .map_err(|_| "Avro batch exceeds Arrow list offset limit")?,
                );
            }
            let child = if map {
                let DataType::Struct(fields) = field.data_type() else {
                    return Err("Avro map requires dictionary schema".into());
                };
                let key_array: ArrayRef = Arc::new(StringArray::from(keys));
                let value_array = column(fields[1].data_type(), &items)?;
                Arc::new(
                    StructArray::try_new(fields.clone(), vec![key_array, value_array], None)
                        .map_err(|e| e.to_string())?,
                ) as ArrayRef
            } else {
                column(field.data_type(), &items)?
            };
            Arc::new(
                ListArray::try_new(
                    field.clone(),
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    child,
                    Some(NullBuffer::from(
                        values
                            .iter()
                            .map(|v| !matches!(v, Value::Null))
                            .collect::<Vec<_>>(),
                    )),
                )
                .map_err(|e| e.to_string())?,
            )
        }
        _ => return Err(format!("unsupported Avro Arrow type {ty}")),
    })
}
fn scan(
    path: &str,
    source: &AvroSource,
    sender: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> std::result::Result<(), String> {
    let reader = Reader::new(std::fs::File::open(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let mut reader = reader.into_iter();
    let schema = source.schema.table_schema();
    let indices = source.projection.as_ref().map(|p| {
        p.expr_iter()
            .map(|e| {
                e.downcast_ref::<datafusion_physical_expr::expressions::Column>()
                    .unwrap()
                    .index()
            })
            .collect::<Vec<_>>()
    });
    let indices = indices.unwrap_or_else(|| (0..schema.fields().len()).collect());
    let projected = Arc::new(schema.project(&indices).map_err(|e| e.to_string())?);
    loop {
        let rows = reader
            .by_ref()
            .take(source.batch_size)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        if rows.is_empty() {
            break;
        }
        let mut arrays = Vec::with_capacity(indices.len());
        for &index in &indices {
            let field = &schema.fields()[index];
            let values = rows
                .iter()
                .map(|row| match row {
                    Value::Record(record) => record
                        .get(index)
                        .filter(|(name, _)| name == field.name())
                        .map(|(_, v)| v)
                        .ok_or("Avro record fields do not match schema"),
                    _ => Err("Avro root must be a record"),
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            arrays.push(column(field.data_type(), &values)?);
        }
        let batch = RecordBatch::try_new_with_options(
            projected.clone(),
            arrays,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(|e| e.to_string())?;
        if sender.blocking_send(Ok(batch)).is_err() {
            break;
        }
    }
    Ok(())
}
