//! Prepared CSV parsing for the public ParseCsvLine expression.
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, expr_fn::create_udf};
use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

fn csv_byte(value: &str, name: &str) -> Result<u8> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 || !bytes[0].is_ascii() || matches!(bytes[0], b'\r' | b'\n' | 0) {
        return Err(DataFusionError::Execution(format!(
            "CSV {name} must be one ASCII byte"
        )));
    }
    Ok(bytes[0])
}

enum FieldCount {
    Unrestricted,
    Exact(NonZeroUsize),
}
enum Quoting {
    Disabled,
    Enabled(regex::Regex),
}

/// Validated byte configuration, a grammar for every enabled quote mode, and
/// prepared marker lookup. The reader builder is configured once per kernel;
/// mutable record scratch is local to each evaluated batch.
struct PreparedCsv {
    reader: csv::ReaderBuilder,
    quoting: Quoting,
    missing: HashSet<String>,
    fields: FieldCount,
}
impl PreparedCsv {
    fn new(
        delimiter: &str,
        quote: Option<&str>,
        missing: Vec<String>,
        expected_fields: usize,
    ) -> Result<Self> {
        let delimiter = csv_byte(delimiter, "delimiter")?;
        let quote = quote.map(|q| csv_byte(q, "quote")).transpose()?;
        if quote == Some(delimiter) {
            return Err(DataFusionError::Execution(
                "CSV quote and delimiter must differ".into(),
            ));
        }
        let mut reader = csv::ReaderBuilder::new();
        reader
            .has_headers(false)
            .flexible(true)
            .delimiter(delimiter);
        let quoting = match quote {
            Some(quote) => {
                reader.quote(quote).double_quote(false);
                let delimiter = regex::escape(&(delimiter as char).to_string());
                let quote = regex::escape(&(quote as char).to_string());
                let pattern = format!(
                    r#"\A(?:[^{delimiter}{quote}]*|{quote}[^{quote}]*{quote})(?:{delimiter}(?:[^{delimiter}{quote}]*|{quote}[^{quote}]*{quote}))*\z"#
                );
                Quoting::Enabled(regex::Regex::new(&pattern).map_err(|error| {
                    DataFusionError::Execution(format!("invalid CSV quote configuration: {error}"))
                })?)
            }
            None => {
                reader.quoting(false);
                Quoting::Disabled
            }
        };
        let fields =
            NonZeroUsize::new(expected_fields).map_or(FieldCount::Unrestricted, FieldCount::Exact);
        Ok(Self {
            reader,
            quoting,
            missing: missing.into_iter().collect(),
            fields,
        })
    }
    fn append(
        &self,
        line: &str,
        record: &mut csv::StringRecord,
        out: &mut ListBuilder<StringBuilder>,
    ) -> Result<()> {
        if line.is_empty() || line == "\n" || line == "\r\n" {
            match self.fields {
                FieldCount::Unrestricted => {
                    out.values()
                        .append_option((!self.missing.contains("")).then_some(""));
                    out.append(true);
                    return Ok(());
                }
                FieldCount::Exact(_) => {
                    return Err(DataFusionError::Execution("empty CSV line".into()));
                }
            }
        }
        if let Quoting::Enabled(grammar) = &self.quoting {
            if !grammar.is_match(line) {
                return Err(DataFusionError::Execution("invalid CSV quoting".into()));
            }
        }
        let invalid = |error| DataFusionError::Execution(format!("invalid CSV line: {error}"));
        let mut reader = self.reader.from_reader(line.as_bytes());
        if !reader.read_record(record).map_err(invalid)? {
            return Err(DataFusionError::Execution("empty CSV line".into()));
        }
        if let FieldCount::Exact(expected) = self.fields {
            if record.len() != expected.get() {
                return Err(DataFusionError::Execution(format!(
                    "CSV field count mismatch: expected {expected}, got {}",
                    record.len()
                )));
            }
        }
        // Borrow parsed fields directly into Arrow output, then reuse the same
        // record's storage to check for an extra record and for the next row.
        for field in record.iter() {
            out.values()
                .append_option((!self.missing.contains(field)).then_some(field));
        }
        if reader.read_record(record).map_err(invalid)? {
            return Err(DataFusionError::Execution(
                "expected exactly one CSV record".into(),
            ));
        }
        out.append(true);
        Ok(())
    }
    fn evaluate(&self, input: &ColumnarValue) -> Result<ColumnarValue> {
        let mut out = ListBuilder::new(StringBuilder::new());
        let mut record = csv::StringRecord::new();
        match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
                match value {
                    Some(line) => self.append(line, &mut record, &mut out)?,
                    None => out.append(false),
                }
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &out.finish(),
                    0,
                )?))
            }
            ColumnarValue::Scalar(ScalarValue::Null) => {
                out.append(false);
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &out.finish(),
                    0,
                )?))
            }
            ColumnarValue::Array(input) => {
                let strings = input
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("parse_csv_line requires UTF-8 input".into())
                    })?;
                for row in 0..strings.len() {
                    if strings.is_null(row) {
                        out.append(false);
                    } else {
                        self.append(strings.value(row), &mut record, &mut out)?;
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
            }
            _ => Err(DataFusionError::Execution(
                "parse_csv_line requires UTF-8 input".into(),
            )),
        }
    }
}

pub(crate) fn parse_csv_line_udf(
    delimiter: String,
    quote: Option<String>,
    missing: Vec<String>,
    expected_fields: usize,
) -> Result<ScalarUDF> {
    let mut hasher = DefaultHasher::new();
    (&delimiter, &quote, &missing, expected_fields).hash(&mut hasher);
    let name = format!("parse_csv_line_{:016x}", hasher.finish());
    let prepared = PreparedCsv::new(&delimiter, quote.as_deref(), missing, expected_fields)?;
    Ok(create_udf(
        &name,
        vec![DataType::Utf8],
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        Volatility::Immutable,
        Arc::new(move |args| {
            let [input] = args else {
                return Err(DataFusionError::Internal(
                    "parse_csv_line requires one argument".into(),
                ));
            };
            prepared.evaluate(input)
        }),
    ))
}
