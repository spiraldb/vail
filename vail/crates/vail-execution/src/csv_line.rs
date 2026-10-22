//! Prepared CSV parsing for the public ParseCsvLine expression.
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility};
use std::collections::HashSet;
use std::io::{Cursor, SeekFrom};
use std::num::NonZeroUsize;
use std::sync::Arc;

type RecordReader<'a> = csv::Reader<Cursor<&'a [u8]>>;

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
    delimiter: u8,
    separator: Option<regex::Regex>,
    quote: Option<u8>,
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
        let separator = if delimiter.len() > 1 {
            Some(regex::Regex::new(delimiter).map_err(|error| {
                DataFusionError::Execution(format!("invalid delimiter regex: {error}"))
            })?)
        } else {
            None
        };
        let delimiter = if separator.is_some() {
            b'\t'
        } else {
            csv_byte(delimiter, "delimiter")?
        };
        let quote = quote.map(|q| csv_byte(q, "quote")).transpose()?;
        if separator.is_none() && quote == Some(delimiter) {
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
            delimiter,
            separator,
            quote,
            quoting,
            missing: missing.into_iter().collect(),
            fields,
        })
    }
    fn append<'a>(
        &self,
        line: &'a str,
        record: &mut csv::StringRecord,
        reader: &mut Option<RecordReader<'a>>,
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
        if let Some(separator) = &self.separator {
            let mut count = 0;
            let mut start = 0;
            loop {
                let (field, end) = if self
                    .quote
                    .is_some_and(|quote| line.as_bytes().get(start) == Some(&quote))
                {
                    let quote = self.quote.unwrap() as char;
                    let close = line[start + 1..]
                        .find(quote)
                        .map(|i| start + 1 + i)
                        .ok_or_else(|| DataFusionError::Execution("invalid CSV quoting".into()))?;
                    (&line[start + 1..close], close + 1)
                } else {
                    let end = separator
                        .find_at(line, start)
                        .map_or(line.len(), |m| m.start());
                    let field = &line[start..end];
                    if self
                        .quote
                        .is_some_and(|quote| field.contains(quote as char))
                    {
                        return Err(DataFusionError::Execution("invalid CSV quoting".into()));
                    }
                    (field, end)
                };
                out.values()
                    .append_option((!self.missing.contains(field)).then_some(field));
                count += 1;
                if end == line.len() {
                    break;
                }
                let matched = separator
                    .find_at(line, end)
                    .filter(|m| m.start() == end && m.end() > end)
                    .ok_or_else(|| {
                        DataFusionError::Execution("invalid CSV separator after field".into())
                    })?;
                start = matched.end();
            }
            if let FieldCount::Exact(expected) = self.fields {
                if count != expected.get() {
                    return Err(DataFusionError::Execution(format!(
                        "Expected {expected} fields, found {} fields",
                        count
                    )));
                }
            }
            out.append(true);
            return Ok(());
        }
        if let Quoting::Enabled(grammar) = &self.quoting {
            if !grammar.is_match(line) {
                return Err(DataFusionError::Execution("invalid CSV quoting".into()));
            }
        }
        if matches!(self.quoting, Quoting::Disabled) {
            // The default Hail import mode has no quoting or escaping. Split
            // borrowed fields directly rather than building a CSV DFA, reader,
            // and IO buffer for every physical line.
            let line = line.trim_start_matches(['\r', '\n']);
            if line.is_empty() {
                return Err(DataFusionError::Execution("empty CSV line".into()));
            }
            let (line, remaining) = line.split_once(['\r', '\n']).unwrap_or((line, ""));
            let fields = line.split(self.delimiter as char);
            if let FieldCount::Exact(expected) = self.fields {
                let count = fields.clone().count();
                if count != expected.get() {
                    return Err(DataFusionError::Execution(format!(
                        "Expected {expected} fields, found {count} field(s)"
                    )));
                }
            }
            for field in fields {
                out.values()
                    .append_option((!self.missing.contains(field)).then_some(field));
            }
            // csv::Reader skips blank records around the one requested record.
            if remaining.bytes().any(|byte| !matches!(byte, b'\r' | b'\n')) {
                return Err(DataFusionError::Execution(
                    "expected exactly one CSV record".into(),
                ));
            }
            out.append(true);
            return Ok(());
        }
        let invalid = |error| DataFusionError::Execution(format!("invalid CSV line: {error}"));
        // Reuse the batch's configured CSV state machine and IO buffer. Only
        // the input slice and parser position change between records.
        let reader = reader.as_mut().expect("quoted CSV batch reader");
        *reader.get_mut().get_mut() = line.as_bytes();
        reader
            .seek_raw(SeekFrom::Start(0), csv::Position::new())
            .map_err(invalid)?;
        if !reader.read_record(record).map_err(invalid)? {
            return Err(DataFusionError::Execution("empty CSV line".into()));
        }
        if let FieldCount::Exact(expected) = self.fields {
            if record.len() != expected.get() {
                return Err(DataFusionError::Execution(format!(
                    "Expected {expected} fields, found {} field(s)",
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
        // Parser configuration and allocation are batch work, never row work.
        // Unquoted and regex-separated inputs do not need a CSV reader at all.
        let mut reader = (self.separator.is_none() && matches!(self.quoting, Quoting::Enabled(_)))
            .then(|| self.reader.from_reader(Cursor::new(&[][..])));
        match input {
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
                match value {
                    Some(line) => self.append(line, &mut record, &mut reader, &mut out)?,
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
                        self.append(strings.value(row), &mut record, &mut reader, &mut out)?;
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
    let identity = (
        delimiter.clone(),
        quote.clone(),
        missing.clone(),
        expected_fields,
    );
    let prepared = PreparedCsv::new(&delimiter, quote.as_deref(), missing, expected_fields)?;
    Ok(crate::kernel_identity::scalar(
        "parse_csv_line",
        identity,
        Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
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
