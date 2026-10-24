//! Bounded local VCF serialization. Bind schema positions once, Arrow views once
//! per batch, and reuse a single record buffer. Python owns header validation.
use crate::{Result, TableError, TableRef};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, LargeStringArray,
    ListArray, StringArray, StringViewArray, StructArray,
};
use arrow_schema::{DataType, Schema};
use futures::{StreamExt, TryStreamExt};
use std::{
    fmt::Write as _,
    io::{BufWriter, Write},
    sync::Arc,
};
use vail_execution::expressions::{PrimitiveType as P, Type};

fn bad(message: impl Into<String>) -> TableError {
    TableError::Invalid(message.into())
}
fn down<T: Array + 'static>(a: &dyn Array) -> Result<&T> {
    a.as_any()
        .downcast_ref()
        .ok_or_else(|| bad("VCF Arrow storage disagrees with its type"))
}

enum Values<'a> {
    Text(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
    I32(&'a Int32Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Bool(&'a BooleanArray),
    List(&'a ListArray, Box<Values<'a>>),
    Call(&'a StructArray, vail_execution::calls::CallRows<'a>),
}
impl<'a> Values<'a> {
    fn bind(dtype: &Type, a: &'a dyn Array) -> Result<Self> {
        Ok(match dtype {
            Type::Primitive(P::Str) => match a.data_type() {
                DataType::Utf8 => Self::Text(down(a)?),
                DataType::LargeUtf8 => Self::Large(down(a)?),
                DataType::Utf8View => Self::View(down(a)?),
                _ => return Err(bad("VCF string storage")),
            },
            Type::Primitive(P::Int32) => Self::I32(down(a)?),
            Type::Primitive(P::Float32) => Self::F32(down(a)?),
            Type::Primitive(P::Float64) => Self::F64(down(a)?),
            Type::Primitive(P::Bool) => Self::Bool(down(a)?),
            Type::Primitive(P::Call) => {
                let a: &StructArray = down(a)?;
                Self::Call(
                    a,
                    vail_execution::calls::CallRows::from_struct(a)
                        .map_err(|e| bad(&e.to_string()))?,
                )
            }
            Type::Array { array } | Type::Set { set: array } => {
                let list: &ListArray = down(a)?;
                Self::List(list, Box::new(Self::bind(array, list.values().as_ref())?))
            }
            _ => return Err(bad("unsupported VCF field type")),
        })
    }
    fn array(&self) -> &dyn Array {
        match self {
            Self::Text(a) => *a,
            Self::Large(a) => *a,
            Self::View(a) => *a,
            Self::I32(a) => *a,
            Self::F32(a) => *a,
            Self::F64(a) => *a,
            Self::Bool(a) => *a,
            Self::List(a, _) => *a,
            Self::Call(a, _) => *a,
        }
    }
    fn append(&self, row: usize, separator: char, out: &mut String) -> Result<()> {
        if self.array().is_null(row) {
            out.push_str(if matches!(self, Self::Call(..)) {
                "./."
            } else {
                "."
            });
            return Ok(());
        }
        match self {
            Self::Text(a) => text(a.value(row), separator, out)?,
            Self::Large(a) => text(a.value(row), separator, out)?,
            Self::View(a) => text(a.value(row), separator, out)?,
            Self::I32(a) => {
                write!(out, "{:?}", a.value(row)).unwrap();
            }
            Self::F32(a) => {
                write!(out, "{:?}", a.value(row)).unwrap();
            }
            Self::F64(a) => {
                write!(out, "{:?}", a.value(row)).unwrap();
            }
            Self::Bool(a) => out.push_str(if a.value(row) { "true" } else { "false" }),
            Self::Call(_, rows) => {
                let call = rows
                    .get(row)
                    .map_err(|e| bad(&e.to_string()))?
                    .ok_or_else(|| bad("missing VCF Call"))?;
                let alleles = call.alleles();
                let phased = call.phased();
                if alleles.is_empty() {
                    out.push('.');
                }
                for (i, a) in alleles.iter().enumerate() {
                    if i > 0 {
                        out.push(if phased { '|' } else { '/' });
                    }
                    write!(out, "{a}").unwrap();
                }
            }
            Self::List(a, values) => {
                let start = a.value_offsets()[row] as usize;
                let end = a.value_offsets()[row + 1] as usize;
                if start == end {
                    out.push('.');
                }
                for i in start..end {
                    if i > start {
                        out.push(',');
                    }
                    values.append(i, separator, out)?;
                }
            }
        }
        Ok(())
    }
}
fn text(value: &str, separator: char, out: &mut String) -> Result<()> {
    if value.contains(['\t', '\r', '\n']) {
        return Err(bad("VCF values cannot contain tabs or newlines"));
    }
    if value.is_empty()
        || (separator != '\0' && value.contains(separator))
        || (separator != '\0' && value.contains(','))
    {
        return Err(bad(
            "VCF values contain an empty string or reserved delimiter",
        ));
    }
    out.push_str(value);
    Ok(())
}
#[derive(Clone)]
struct Field {
    name: String,
    index: usize,
    dtype: Type,
}
impl Field {
    fn new(schema: &Schema, name: &str) -> Result<Self> {
        Self::at(schema, schema.index_of(name)?)
    }
    fn at(schema: &Schema, index: usize) -> Result<Self> {
        Ok(Self {
            name: schema.field(index).name().clone(),
            index,
            dtype: Type::from_arrow(schema.field(index).data_type())?,
        })
    }
    fn bind<'a>(&self, columns: &'a [ArrayRef]) -> Result<Values<'a>> {
        Values::bind(&self.dtype, columns[self.index].as_ref())
    }
}

// Finish compression before atomically publishing the temporary destination.
enum Output<W: Write> {
    Plain(W),
    Gzip(flate2::write::GzEncoder<W>),
}
impl<W: Write> Output<W> {
    fn finish(self) -> std::io::Result<()> {
        match self {
            Self::Plain(mut w) => w.flush(),
            Self::Gzip(w) => w.finish()?.flush(),
        }
    }
}
impl<W: Write> Write for Output<W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(w) => w.write(data),
            Self::Gzip(w) => w.write(data),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(w) => w.flush(),
            Self::Gzip(w) => w.flush(),
        }
    }
}

pub async fn export(
    table: TableRef,
    path: &str,
    headers: &[String],
    entries_name: &str,
    entry_names: &[String],
    sample_count: usize,
) -> Result<()> {
    let schema = table.schema();
    let allele = Field::new(schema, "alleles")?;
    let entries = Field::new(schema, entries_name)?;
    let locus_index = schema.index_of("locus")?;
    let DataType::Struct(locus_fields) = schema.field(locus_index).data_type() else {
        return Err(bad("VCF requires locus struct"));
    };
    let locus_schema = Schema::new(locus_fields.clone());
    let tagged = locus_schema.index_of("__vail_locus_contig").is_ok();
    let contig = Field::new(
        &locus_schema,
        if tagged {
            "__vail_locus_contig"
        } else {
            "contig"
        },
    )?;
    let position = Field::new(
        &locus_schema,
        if tagged {
            "__vail_locus_position"
        } else {
            "position"
        },
    )?;
    let optional = |name: &str| {
        if schema.index_of(name).is_ok() {
            Field::new(schema, name).map(Some)
        } else {
            Ok(None)
        }
    };
    let (rsid, qual, filters) = (optional("rsid")?, optional("qual")?, optional("filters")?);
    let info_index = schema.index_of("info").ok();
    let info_fields = if let Some(index) = info_index {
        let DataType::Struct(fields) = schema.field(index).data_type() else {
            return Err(bad("VCF info must be struct"));
        };
        let s = Schema::new(fields.clone());
        fields
            .iter()
            .enumerate()
            .map(|(index, _)| Field::at(&s, index))
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    let DataType::List(entry_field) = schema.field(entries.index).data_type() else {
        return Err(bad("VCF entries must be array"));
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        return Err(bad("VCF entries must be structs"));
    };
    let entry_schema = Schema::new(entry_fields.clone());
    let entry_positions = entry_fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect::<std::collections::HashMap<_, _>>();
    let fields = entry_names
        .iter()
        .map(|name| {
            let name = name.as_str();
            Field::at(
                &entry_schema,
                *entry_positions
                    .get(name)
                    .ok_or_else(|| bad("unknown VCF entry field"))?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let format = fields
        .iter()
        .map(|f| f.name.as_str())
        .collect::<Vec<_>>()
        .join(":");
    let plan = crate::partitioned::prepare(table.clone()).await?.ordered();
    let mut batches = plan.execute(0, Arc::new(datafusion_execution::TaskContext::default()))?;
    let destination = std::path::Path::new(path);
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut output = tempfile::NamedTempFile::new_in(parent).map_err(|e| bad(e.to_string()))?;
    let buffered = BufWriter::new(output.as_file_mut());
    let mut writer = if path.ends_with(".bgz") {
        Output::Plain(buffered)
    } else if path.ends_with(".gz") {
        Output::Gzip(flate2::write::GzEncoder::new(
            buffered,
            flate2::Compression::default(),
        ))
    } else {
        Output::Plain(buffered)
    };
    let bgzf = path.ends_with(".bgz");
    let mut header_bytes = Vec::new();
    for header in headers {
        writeln!(header_bytes, "{}", header).map_err(|e| bad(e.to_string()))?;
    }
    if bgzf {
        let mut compressor = crate::compressed_text::BgzfWriter::new(&mut writer);
        compressor
            .write_all(&header_bytes)
            .map_err(|e| bad(e.to_string()))?;
        compressor
            .finish_members()
            .map_err(|e| bad(e.to_string()))?;
    } else {
        writer
            .write_all(&header_bytes)
            .map_err(|e| bad(e.to_string()))?;
    }
    let formatter = Arc::new(Formatter {
        allele,
        entries,
        locus_index,
        contig,
        position,
        rsid,
        qual,
        filters,
        info_index,
        info_fields,
        fields,
        format,
        sample_count,
    });
    // Slice before queuing: at most 32K entries (or one wide row) per job.
    // Arrow slices borrow buffers; only the bounded worker window owns formatted output.
    let rows_per_job = (32_768 / sample_count.max(1)).clamp(1, 4096);
    let chunks: futures::stream::BoxStream<'_, Result<arrow_array::RecordBatch>> =
        async_stream::try_stream! {
            while let Some(batch) = batches.try_next().await? {
                for start in (0..batch.num_rows()).step_by(rows_per_job) {
                    yield batch.slice(start, rows_per_job.min(batch.num_rows() - start));
                }
            }
        }
        .boxed();
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(crate::EXECUTION_CONCURRENCY);
    futures::pin_mut!(chunks);
    let mut jobs = futures::stream::FuturesOrdered::new();
    let mut input_done = false;
    let mut failure = None;
    loop {
        while !input_done && failure.is_none() && jobs.len() < workers {
            match chunks.try_next().await {
                Ok(Some(batch)) => {
                    let formatter = formatter.clone();
                    jobs.push_back(tokio::task::spawn_blocking(move || {
                        formatter.batch(batch, bgzf)
                    }));
                }
                Ok(None) => input_done = true,
                Err(error) => failure = Some(error.into()),
            }
        }
        let Some(result) = jobs.next().await else {
            break;
        };
        match result
            .map_err(|e| bad(e.to_string()))
            .and_then(|result| result)
        {
            Ok(bytes) if failure.is_none() => {
                if let Err(error) = writer.write_all(&bytes) {
                    failure = Some(bad(error.to_string()));
                }
            }
            Err(error) if failure.is_none() => failure = Some(error),
            _ => {}
        }
        // On failure stop producing input and join only the bounded existing jobs.
        // Workers own private buffers, never the temporary file or destination.
    }
    if let Some(error) = failure {
        return Err(error);
    }
    if bgzf {
        crate::compressed_text::BgzfWriter::new(&mut writer)
            .finish()
            .map_err(|e| bad(e.to_string()))?;
    }
    writer.finish().map_err(|e| bad(e.to_string()))?;
    output
        .persist(destination)
        .map_err(|e| bad(e.error.to_string()))?;
    Ok(())
}

struct Formatter {
    allele: Field,
    entries: Field,
    locus_index: usize,
    contig: Field,
    position: Field,
    rsid: Option<Field>,
    qual: Option<Field>,
    filters: Option<Field>,
    info_index: Option<usize>,
    info_fields: Vec<Field>,
    fields: Vec<Field>,
    format: String,
    sample_count: usize,
}
impl Formatter {
    fn batch(&self, batch: arrow_array::RecordBatch, bgzf: bool) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        if bgzf {
            let mut compressor = crate::compressed_text::BgzfWriter::new(&mut encoded);
            self.write_batch(&batch, &mut compressor)?;
            compressor
                .finish_members()
                .map_err(|e| bad(e.to_string()))?;
        } else {
            self.write_batch(&batch, &mut encoded)?;
        }
        Ok(encoded)
    }
    fn write_batch(&self, batch: &arrow_array::RecordBatch, writer: &mut impl Write) -> Result<()> {
        let Self {
            allele,
            entries,
            locus_index,
            contig,
            position,
            rsid,
            qual,
            filters,
            info_index,
            info_fields,
            fields,
            format,
            sample_count,
        } = self;
        let locus_index = *locus_index;
        let sample_count = *sample_count;
        let mut line = String::new();

        let loci: &StructArray = down(batch.column(locus_index).as_ref())?;
        let contigs = contig.bind(loci.columns())?;
        let positions = position.bind(loci.columns())?;
        let alleles = allele.bind(batch.columns())?;
        let Values::List(allele_lists, allele_values) = &alleles else {
            return Err(bad("VCF alleles must be array"));
        };
        let entries_array: &ListArray = down(batch.column(entries.index).as_ref())?;
        let entry_values: &StructArray = down(entries_array.values().as_ref())?;
        let entry_views = fields
            .iter()
            .map(|f| f.bind(entry_values.columns()))
            .collect::<Result<Vec<_>>>()?;
        let info_array = info_index
            .map(|i| down::<StructArray>(batch.column(i).as_ref()))
            .transpose()?;
        let info_views = info_fields
            .iter()
            .map(|f| f.bind(info_array.unwrap().columns()))
            .collect::<Result<Vec<_>>>()?;
        let rsids = rsid.as_ref().map(|f| f.bind(batch.columns())).transpose()?;
        let quals = qual.as_ref().map(|f| f.bind(batch.columns())).transpose()?;
        let filter_views = filters
            .as_ref()
            .map(|f| f.bind(batch.columns()))
            .transpose()?;
        for row in 0..batch.num_rows() {
            line.clear();
            if loci.is_null(row)
                || contigs.array().is_null(row)
                || positions.array().is_null(row)
                || allele_lists.is_null(row)
                || allele_lists.value_length(row) < 1
            {
                return Err(bad(
                    "VCF requires a defined locus and nonempty allele array",
                ));
            }
            contigs.append(row, '\0', &mut line)?;
            line.push('\t');
            positions.append(row, '\0', &mut line)?;
            line.push('\t');
            if let Some(rsids) = &rsids {
                rsids.append(row, '\0', &mut line)?;
            } else {
                line.push('.');
            }
            line.push('\t');
            let start = allele_lists.value_offsets()[row] as usize;
            let end = allele_lists.value_offsets()[row + 1] as usize;
            for a in start..end {
                if allele_values.array().is_null(a) {
                    return Err(bad("VCF alleles must be defined"));
                }
            }
            allele_values.append(start, ',', &mut line)?;
            line.push('\t');
            if end == start + 1 {
                line.push('.');
            }
            for a in start + 1..end {
                if a > start + 1 {
                    line.push(',');
                }
                allele_values.append(a, ',', &mut line)?;
            }
            line.push('\t');
            if let Some(quals) = &quals {
                quals.append(row, '\0', &mut line)?;
            } else {
                line.push('.');
            }
            line.push('\t');
            match &filter_views {
                Some(Values::List(a, child)) if !a.is_null(row) => {
                    let start = a.value_offsets()[row] as usize;
                    let end = a.value_offsets()[row + 1] as usize;
                    if start == end {
                        line.push_str("PASS");
                    }
                    // FILTER is a set. Canonical order keeps output stable
                    // regardless of its physical Arrow element order.
                    let mut names = Vec::with_capacity(end - start);
                    for i in start..end {
                        let mut name = String::new();
                        child.append(i, ';', &mut name)?;
                        names.push(name);
                    }
                    names.sort_unstable();
                    for (i, name) in names.iter().enumerate() {
                        if i > 0 {
                            line.push(';');
                        }
                        line.push_str(name);
                    }
                }
                _ => line.push('.'),
            }
            line.push('\t');
            let info_start = line.len();
            if info_array.is_some_and(|a| !a.is_null(row)) {
                for (field, view) in info_fields.iter().zip(&info_views) {
                    if view.array().is_null(row) {
                        continue;
                    }
                    if let Values::Bool(flags) = view {
                        if !flags.value(row) {
                            continue;
                        }
                    }
                    if line.len() > info_start {
                        line.push(';');
                    }
                    line.push_str(&field.name);
                    if !matches!(view, Values::Bool(_)) {
                        line.push('=');
                        view.append(row, ';', &mut line)?;
                    }
                }
            }
            if line.len() == info_start {
                line.push('.');
            }
            if sample_count > 0 {
                if entries_array.is_null(row)
                    || entries_array.value_length(row) as usize != sample_count
                {
                    return Err(bad("VCF entry count differs from sample header"));
                }
                line.push('\t');
                line.push_str(if format.is_empty() { "." } else { &format });
                let start = entries_array.value_offsets()[row] as usize;
                for cell in start..start + sample_count {
                    line.push('\t');
                    if fields.is_empty() {
                        line.push('.');
                        continue;
                    }
                    // VCF permits omitted trailing missing FORMAT values. Keep
                    // the first field (especially GT) even if all are missing.
                    let retained = entry_views
                        .iter()
                        .rposition(|view| {
                            !entry_values.is_null(cell) && !view.array().is_null(cell)
                        })
                        .map_or(1, |i| i + 1);
                    for (i, view) in entry_views.iter().take(retained).enumerate() {
                        if i > 0 {
                            line.push(':');
                        }
                        if entry_values.is_null(cell) {
                            line.push_str(if matches!(view, Values::Call(..)) {
                                "./."
                            } else {
                                "."
                            });
                        } else {
                            view.append(cell, ':', &mut line)?;
                        }
                    }
                }
            }
            line.push('\n');
            writer
                .write_all(line.as_bytes())
                .map_err(|e| bad(e.to_string()))?;
        }
        Ok(())
    }
}
