//! CSV-backed parsing for the `parse_csv_line` execution primitive.

use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, expr_fn::create_udf};

fn csv_delimiter(value: &str) -> Result<u8> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 || !bytes[0].is_ascii() || matches!(bytes[0], b'\r' | b'\n' | 0) {
        return Err(DataFusionError::Execution(
            "CSV delimiter must be one ASCII byte".into(),
        ));
    }
    Ok(bytes[0])
}

fn csv_quote(value: Option<&str>) -> Result<Option<u8>> {
    value
        .map(|value| {
            let bytes = value.as_bytes();
            if bytes.len() != 1 || !bytes[0].is_ascii() || matches!(bytes[0], b'\r' | b'\n' | 0) {
                return Err(DataFusionError::Execution(
                    "CSV quote must be one ASCII byte".into(),
                ));
            }
            Ok(bytes[0])
        })
        .transpose()
}

fn parse_one(
    line: &str,
    delimiter: u8,
    quote: Option<u8>,
    quote_grammar: Option<&regex::Regex>,
    missing: &[String],
    expected_fields: usize,
) -> Result<Vec<Option<String>>> {
    if line.is_empty() || line == "\n" || line == "\r\n" {
        if expected_fields == 0 {
            return Ok(vec![if missing.iter().any(String::is_empty) {
                None
            } else {
                Some(String::new())
            }]);
        }
        return Err(DataFusionError::Execution("empty CSV line".into()));
    }
    if quote_grammar.is_some_and(|grammar| !grammar.is_match(line)) {
        return Err(DataFusionError::Execution("invalid CSV quoting".into()));
    }

    let mut builder = csv::ReaderBuilder::new();
    builder
        .has_headers(false)
        .flexible(true)
        .delimiter(delimiter);
    match quote {
        Some(quote) => {
            builder.quote(quote).double_quote(false);
        }
        None => {
            builder.quoting(false);
        }
    }
    let mut reader = builder.from_reader(line.as_bytes());
    let mut records = reader.records();
    let record = records
        .next()
        .ok_or_else(|| DataFusionError::Execution("empty CSV line".into()))?
        .map_err(|e| DataFusionError::Execution(format!("invalid CSV line: {e}")))?;
    if let Some(extra) = records.next() {
        extra.map_err(|e| DataFusionError::Execution(format!("invalid CSV line: {e}")))?;
        return Err(DataFusionError::Execution(
            "expected exactly one CSV record".into(),
        ));
    }
    if expected_fields != 0 && record.len() != expected_fields {
        return Err(DataFusionError::Execution(format!(
            "CSV field count mismatch: expected {expected_fields}, got {}",
            record.len()
        )));
    }
    Ok(record
        .iter()
        .map(|field| {
            if missing.iter().any(|missing| missing == field) {
                None
            } else {
                Some(field.to_owned())
            }
        })
        .collect())
}

fn append_row(builder: &mut ListBuilder<StringBuilder>, row: Option<Vec<Option<String>>>) {
    if let Some(row) = row {
        for field in row {
            builder.values().append_option(field.as_deref());
        }
        builder.append(true);
    } else {
        builder.append(false);
    }
}

fn finish_array(mut builder: ListBuilder<StringBuilder>) -> ArrayRef {
    Arc::new(builder.finish())
}

fn build_udf(
    delimiter: String,
    quote: Option<String>,
    missing: Vec<String>,
    expected_fields: usize,
) -> Result<ScalarUDF> {
    let delimiter = csv_delimiter(&delimiter)?;
    let quote = csv_quote(quote.as_deref())?;
    if quote == Some(delimiter) {
        return Err(DataFusionError::Execution(
            "CSV quote and delimiter must differ".into(),
        ));
    }
    let mut hasher = DefaultHasher::new();
    (delimiter, quote, &missing, expected_fields).hash(&mut hasher);
    let udf_name = format!("parse_csv_line_{:016x}", hasher.finish());
    let quote_grammar = quote.map(|quote| {
        let delimiter = regex::escape(&(delimiter as char).to_string());
        let quote = regex::escape(&(quote as char).to_string());
        let pattern = format!(
            r#"\A(?:[^{delimiter}{quote}]*|{quote}[^{quote}]*{quote})(?:{delimiter}(?:[^{delimiter}{quote}]*|{quote}[^{quote}]*{quote}))*\z"#
        );
        regex::Regex::new(&pattern).map_err(|error| {
            DataFusionError::Execution(format!("invalid CSV quote configuration: {error}"))
        })
    }).transpose()?;
    let element = Arc::new(Field::new("item", DataType::Utf8, true));
    let output = DataType::List(element);
    Ok(create_udf(
        &udf_name,
        vec![DataType::Utf8],
        output,
        Volatility::Immutable,
        Arc::new(move |args: &[ColumnarValue]| {
            let Some(input) = args.first() else {
                return Err(DataFusionError::Internal(
                    "parse_csv_line requires one argument".into(),
                ));
            };
            match input {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(line))) => {
                    let row = parse_one(
                        line,
                        delimiter,
                        quote,
                        quote_grammar.as_ref(),
                        &missing,
                        expected_fields,
                    )?;
                    let mut builder = ListBuilder::new(StringBuilder::new());
                    append_row(&mut builder, Some(row));
                    let array = finish_array(builder);
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        array.as_ref(),
                        0,
                    )?))
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(None))
                | ColumnarValue::Scalar(ScalarValue::Null) => {
                    let mut builder = ListBuilder::new(StringBuilder::new());
                    append_row(&mut builder, None);
                    let array = finish_array(builder);
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        array.as_ref(),
                        0,
                    )?))
                }
                ColumnarValue::Array(input) => {
                    let strings =
                        input
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "parse_csv_line requires UTF-8 input".into(),
                                )
                            })?;
                    let mut builder =
                        ListBuilder::with_capacity(StringBuilder::new(), strings.len());
                    for i in 0..strings.len() {
                        if strings.is_null(i) {
                            append_row(&mut builder, None);
                        } else {
                            let row = parse_one(
                                strings.value(i),
                                delimiter,
                                quote,
                                quote_grammar.as_ref(),
                                &missing,
                                expected_fields,
                            )?;
                            append_row(&mut builder, Some(row));
                        }
                    }
                    Ok(ColumnarValue::Array(finish_array(builder)))
                }
                _ => Err(DataFusionError::Execution(
                    "parse_csv_line requires UTF-8 input".into(),
                )),
            }
        }),
    ))
}

pub(crate) fn parse_csv_line_udf(
    delimiter: String,
    quote: Option<String>,
    missing: Vec<String>,
    expected_fields: usize,
) -> Result<ScalarUDF> {
    build_udf(delimiter, quote, missing, expected_fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_reader_handles_quoted_and_trailing_empty_fields() {
        let grammar =
            regex::Regex::new(r#"\A(?:[^,"]*|"[^"]*")(?:,(?:[^,"]*|"[^"]*"))*\z"#).unwrap();
        let result = parse_one("a,\"b,c\",", b',', Some(b'\"'), Some(&grammar), &[], 3).unwrap();
        assert_eq!(
            result,
            vec![Some("a".into()), Some("b,c".into()), Some("".into())]
        );
        let result = parse_one("a,\"b\"", b',', None, None, &[], 2).unwrap();
        assert_eq!(result, vec![Some("a".into()), Some("\"b\"".into())]);
    }

    #[test]
    fn csv_reader_reports_malformed_lines_and_field_count() {
        // A structurally valid CSV record with the wrong number of columns is
        // an error at this execution boundary, rather than a short list.
        let grammar =
            regex::Regex::new(r#"\A(?:[^,"]*|"[^"]*")(?:,(?:[^,"]*|"[^"]*"))*\z"#).unwrap();
        assert!(parse_one("a,b", b',', Some(b'\"'), Some(&grammar), &[], 3).is_err());
        assert!(parse_one("a,\"b", b',', Some(b'\"'), Some(&grammar), &[], 0).is_err());
        assert!(parse_one("a,\"b\"\"c\"", b',', Some(b'\"'), Some(&grammar), &[], 0).is_err());
        assert!(parse_one("a\"b,c", b',', Some(b'\"'), Some(&grammar), &[], 0).is_err());
    }

    #[test]
    fn missing_markers_include_empty_when_configured() {
        let grammar =
            regex::Regex::new(r#"\A(?:[^,"]*|"[^"]*")(?:,(?:[^,"]*|"[^"]*"))*\z"#).unwrap();
        let result = parse_one(
            "a,,NA",
            b',',
            Some(b'\"'),
            Some(&grammar),
            &["".into(), "NA".into()],
            3,
        )
        .unwrap();
        assert_eq!(result, vec![Some("a".into()), None, None]);
    }

    #[test]
    fn empty_header_line_is_one_empty_field_but_data_rows_fail() {
        assert_eq!(
            parse_one("", b',', Some(b'\"'), None, &[], 0).unwrap(),
            vec![Some(String::new())]
        );
        assert_eq!(
            parse_one("", b',', Some(b'\"'), None, &[String::new()], 0).unwrap(),
            vec![None]
        );
        assert!(parse_one("", b',', Some(b'\"'), None, &[], 1).is_err());
    }
}
