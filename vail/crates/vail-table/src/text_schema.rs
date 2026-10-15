//! Schema inference for local text imports, using Arrow's CSV and JSON readers.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow_csv::reader::Format as CsvFormat;
use arrow_json::reader::{ValueIter, infer_json_schema_from_iterator};
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::config::{ConfigField, CsvOptions, JsonOptions};
use serde_json::{Map, Value};

use crate::{Result, TableError};

const DEFAULT_INFER_RECORDS: usize = 1000;

/// Infer and merge the schemas of local CSV or JSON text files.
///
/// `options_json` uses the same DataFusion CSV/JSON option names as table
/// reads. Each file is sampled independently up to the configured inference
/// limit, and Arrow merges their fields into one schema.
pub fn infer(paths: &[String], format: &str, options_json: &Value) -> Result<SchemaRef> {
    if paths.is_empty() {
        return Err(TableError::Invalid(
            "schema inference requires at least one input path".into(),
        ));
    }
    let options: Map<String, Value> = serde_json::from_value(options_json.clone())
        .map_err(|error| TableError::Invalid(format!("invalid text options: {error}")))?;

    let schemas = match format {
        "csv" => {
            let csv = csv_options(options)?;
            let max_records = csv.schema_infer_max_rec.unwrap_or(DEFAULT_INFER_RECORDS);
            let mut csv_format = CsvFormat::default()
                .with_header(csv.has_header().unwrap_or(true))
                .with_delimiter(csv.delimiter())
                .with_quote(csv.quote());
            if let Some(escape) = csv.escape() {
                csv_format = csv_format.with_escape(escape);
            }
            if let Some(terminator) = csv.terminator() {
                csv_format = csv_format.with_terminator(terminator);
            }
            if let Some(comment) = csv.comment {
                csv_format = csv_format.with_comment(comment);
            }
            if let Some(allow) = csv.truncated_rows {
                csv_format = csv_format.with_truncated_rows(allow);
            }
            let schemas = paths
                .iter()
                .map(|path| {
                    let file = File::open(path).map_err(|error| {
                        TableError::Invalid(format!("cannot open text input {path:?}: {error}"))
                    })?;
                    let (schema, _) =
                        csv_format.infer_schema(BufReader::new(file), Some(max_records))?;
                    Ok(schema)
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(first) = schemas.first() {
                for (path, schema) in paths.iter().zip(&schemas).skip(1) {
                    let same_columns = first.fields().len() == schema.fields().len()
                        && first
                            .fields()
                            .iter()
                            .zip(schema.fields())
                            .all(|(left, right)| left.name() == right.name());
                    if !same_columns {
                        return Err(TableError::Invalid(format!(
                            "CSV input {path:?} has column names or order that differ from the first file"
                        )));
                    }
                }
            }
            schemas
        }
        "json" | "ndjson" => {
            let json = json_options(options)?;
            if !json.newline_delimited {
                return Err(TableError::Invalid(
                    "schema inference supports newline-delimited JSON only".into(),
                ));
            }
            let max_records = json.schema_infer_max_rec.unwrap_or(DEFAULT_INFER_RECORDS);
            paths
                .iter()
                .map(|path| {
                    let file = File::open(path).map_err(|error| {
                        TableError::Invalid(format!("cannot open text input {path:?}: {error}"))
                    })?;
                    let reader = BufReader::new(file);
                    let values = ValueIter::new(reader, None).take(max_records);
                    Ok(infer_json_schema_from_iterator(values)?)
                })
                .collect::<Result<Vec<_>>>()?
        }
        _ => {
            return Err(TableError::Invalid(format!(
                "schema inference does not support text format {format:?}"
            )));
        }
    };

    Ok(Arc::new(Schema::try_merge(schemas)?))
}

pub(crate) fn csv_options(options: Map<String, Value>) -> Result<CsvOptions> {
    let mut csv = CsvOptions::default().with_has_header(true);
    for (key, value) in options {
        if !matches!(
            key.as_str(),
            "delimiter"
                | "quote"
                | "escape"
                | "terminator"
                | "comment"
                | "has_header"
                | "newlines_in_values"
                | "truncated_rows"
                | "schema_infer_max_rec"
        ) {
            return Err(TableError::Invalid(format!(
                "unsupported CSV read option {key:?}"
            )));
        }
        let value = value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string());
        ConfigField::set(&mut csv, &key, &value)?;
    }
    for (name, byte) in [("delimiter", csv.delimiter), ("quote", csv.quote)] {
        if !byte.is_ascii() || matches!(byte, b'\r' | b'\n' | 0) {
            return Err(TableError::Invalid(format!(
                "CSV {name} must be one non-newline ASCII byte"
            )));
        }
    }
    if csv.delimiter == csv.quote {
        return Err(TableError::Invalid(
            "CSV quote and delimiter must differ".into(),
        ));
    }
    Ok(csv)
}

pub(crate) fn json_options(options: Map<String, Value>) -> Result<JsonOptions> {
    let mut json = JsonOptions::default();
    for (key, value) in options {
        if !matches!(key.as_str(), "newline_delimited" | "schema_infer_max_rec") {
            return Err(TableError::Invalid(format!(
                "unsupported JSON read option {key:?}"
            )));
        }
        let value = value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string());
        ConfigField::set(&mut json, &key, &value)?;
    }
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use std::fs;

    fn write(dir: &tempfile::TempDir, name: &str, content: &str) -> String {
        let path = dir.path().join(name);
        fs::write(&path, content).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn csv_inference_merges_typed_columns_across_files() {
        let dir = tempfile::tempdir().unwrap();
        let first = write(&dir, "a.csv", "id,count\na,1\nb,2\n");
        let second = write(&dir, "b.csv", "id,count\nc,3\n");
        let schema = infer(&[first, second], "csv", &serde_json::json!({})).unwrap();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("count").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn csv_inference_rejects_different_column_names_or_order() {
        let dir = tempfile::tempdir().unwrap();
        let first = write(&dir, "a.csv", "id,count\na,1\n");
        let second = write(&dir, "b.csv", "count,id\n2,b\n");
        assert!(infer(&[first, second], "csv", &serde_json::json!({})).is_err());
    }

    #[test]
    fn json_inference_handles_nested_values_and_merges_files() {
        let dir = tempfile::tempdir().unwrap();
        let first = write(&dir, "a.json", "{\"id\":1,\"nested\":{\"ok\":true}}\n");
        let second = write(&dir, "b.json", "{\"name\":\"sample\",\"id\":2}\n");
        let schema = infer(&[first, second], "ndjson", &serde_json::json!({})).unwrap();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(schema.field_with_name("nested").is_ok());
        assert!(schema.field_with_name("name").is_ok());
    }

    #[test]
    fn schema_inference_rejects_unbounded_json_array_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, "rows.json", r#"[{"first":1},{"second":"later"}]"#);
        let result = infer(
            &[path.clone()],
            "json",
            &serde_json::json!({"newline_delimited":false,"schema_infer_max_rec":1}),
        );
        assert!(result.is_err());
    }

    #[test]
    fn unknown_options_and_empty_paths_are_rejected() {
        assert!(infer(&[], "csv", &serde_json::json!({})).is_err());
        let dir = tempfile::tempdir().unwrap();
        let path = write(&dir, "a.csv", "a\n1\n");
        assert!(infer(&[path], "csv", &serde_json::json!({"typo":true})).is_err());
        // These are DataFusion configuration keys but this scanner does not
        // implement them; accepting them would silently change import semantics.
        assert!(csv_options(serde_json::from_str(r#"{"null_regex":"NA"}"#).unwrap()).is_err());
        assert!(csv_options(serde_json::from_str(r#"{"compression":"gzip"}"#).unwrap()).is_err());
        assert!(json_options(serde_json::from_str(r#"{"compression":"gzip"}"#).unwrap()).is_err());
        assert!(csv_options(serde_json::from_str(r#"{"terminator":10}"#).unwrap()).is_ok());
    }
}
