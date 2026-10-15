use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use datafusion_common::{
    config::{CsvOptions, JsonOptions},
    file_options::csv_writer::CsvWriterOptions,
    file_options::json_writer::JsonWriterOptions,
    parsers::CsvQuoteStyle,
};
use datafusion_datasource::{
    ListingTableUrl,
    file_groups::FileGroup,
    file_sink_config::{FileOutputMode, FileSinkConfig},
    sink::{DataSink, DataSinkExec},
};
use datafusion_datasource_csv::file_format::CsvSink;
use datafusion_datasource_json::file_format::JsonSink;
use datafusion_execution::{TaskContext, object_store::ObjectStoreUrl};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_plan::{ExecutionPlan, projection::ProjectionExec};
use futures::TryStreamExt;
use serde_json::Value;

use crate::{Result, TableError, TableRef};

fn sink_config(path: &str, extension: &str, schema: SchemaRef) -> Result<FileSinkConfig> {
    let table_path = ListingTableUrl::parse(path)?;
    Ok(FileSinkConfig {
        original_url: path.to_owned(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::new(vec![]),
        table_paths: vec![table_path],
        output_schema: schema,
        table_partition_cols: vec![],
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: false,
        file_extension: extension.to_owned(),
        file_output_mode: FileOutputMode::SingleFile,
    })
}

pub async fn export(table: TableRef, path: &str, format: &str, options_json: &Value) -> Result<()> {
    if path.contains("://") && !path.starts_with("file://") {
        return Err(TableError::Invalid(
            "text export supports local paths only".into(),
        ));
    }
    if [".gz", ".bgz", ".bz2", ".xz", ".zst"]
        .iter()
        .any(|suffix| path.ends_with(suffix))
    {
        return Err(TableError::Invalid(
            "compressed text export is not implemented".into(),
        ));
    }
    if !matches!(format, "csv" | "tsv" | "text" | "json" | "ndjson") {
        return Err(TableError::Invalid(format!(
            "unsupported export format {format:?}"
        )));
    }
    let options: Value = serde_json::from_value(options_json.clone())
        .map_err(|e| TableError::Invalid(e.to_string()))?;
    let map = options
        .as_object()
        .ok_or_else(|| TableError::Invalid("text export options must be a JSON object".into()))?;
    let mut plan = crate::partitioned::prepare(table.clone()).await?.ordered();
    let schema = table.schema().clone();
    if format == "text"
        && schema.fields().iter().any(|field| {
            !matches!(
                field.data_type(),
                DataType::Boolean
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
            )
        })
    {
        return Err(TableError::Invalid(
            "text export does not support floating point or nested columns".into(),
        ));
    }
    let projections = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            (
                Arc::new(datafusion_physical_expr::expressions::Column::new(
                    field.name(),
                    i,
                )) as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
                field.name().clone(),
            )
        })
        .collect::<Vec<_>>();
    plan = Arc::new(ProjectionExec::try_new(projections, plan)?);
    let context = Arc::new(TaskContext::default());
    let sink: Arc<dyn DataSink> = if format == "json" || format == "ndjson" {
        let mut opts = JsonOptions::default();
        for (key, value) in map {
            let value = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            datafusion_common::config::ConfigField::set(&mut opts, key, &value)?;
        }
        if !opts.newline_delimited {
            return Err(TableError::Invalid(
                "JSON export supports newline-delimited objects only".into(),
            ));
        }
        let sink = JsonSink::new(
            sink_config(path, "json", schema)?,
            JsonWriterOptions::try_from(&opts)?,
        );
        Arc::new(sink)
    } else {
        let default_delimiter = if format == "tsv" || format == "text" {
            b'\t'
        } else {
            b','
        };
        let mut opts = CsvOptions::default()
            .with_delimiter(default_delimiter)
            .with_has_header(true);
        if format == "text" || format == "tsv" {
            opts.null_value = Some("NA".into());
        }
        for (key, value) in map {
            let key = match key.as_str() {
                "header" => "has_header",
                "missing" => "null_value",
                other => other,
            };
            let value = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            datafusion_common::config::ConfigField::set(&mut opts, key, &value)?;
        }
        if format == "text" {
            opts = opts.with_quote_style(CsvQuoteStyle::Never);
        }
        let extension = if format == "tsv" || format == "text" {
            "tsv"
        } else {
            "csv"
        };
        let sink = CsvSink::new(
            sink_config(path, extension, schema)?,
            CsvWriterOptions::try_from(&opts)?,
        );
        Arc::new(sink)
    };
    let exec = DataSinkExec::new(plan, sink, None);
    let mut stream = exec.execute(0, context)?;
    while stream.try_next().await?.is_some() {}
    Ok(())
}
