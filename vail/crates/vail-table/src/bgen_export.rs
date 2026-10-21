//! BGEN layout-2 writer. The input table contains typed identifying columns and
//! an ordered array of nullable GP arrays. One variant and one compressed block
//! are retained at a time; native Arrow streams supply bounded batches.
use crate::{Result, TableError, TableRef};
use arrow_array::{
    Array, Float64Array, Int32Array, LargeStringArray, ListArray, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Schema};
use futures::TryStreamExt;
use std::{
    io::{BufWriter, Seek, SeekFrom, Write},
    sync::Arc,
};

fn bad(value: impl Into<String>) -> TableError {
    TableError::Invalid(value.into())
}
fn down<T: Array + 'static>(value: &dyn Array) -> Result<&T> {
    value
        .as_any()
        .downcast_ref()
        .ok_or_else(|| bad("BGEN Arrow storage disagrees with its planned schema"))
}
enum Text<'a> {
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}
impl<'a> Text<'a> {
    fn bind(value: &'a dyn Array) -> Result<Self> {
        Ok(match value.data_type() {
            DataType::Utf8 => Self::Utf8(down(value)?),
            DataType::LargeUtf8 => Self::Large(down(value)?),
            DataType::Utf8View => Self::View(down(value)?),
            _ => return Err(bad("BGEN text field must have string storage")),
        })
    }
    fn get(&self, row: usize) -> Result<&str> {
        let (null, value) = match self {
            Self::Utf8(a) => (a.is_null(row), a.value(row)),
            Self::Large(a) => (a.is_null(row), a.value(row)),
            Self::View(a) => (a.is_null(row), a.value(row)),
        };
        if null {
            Err(bad("BGEN identifying strings cannot be missing"))
        } else {
            Ok(value)
        }
    }
}
fn short(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u16::try_from(value.len()).map_err(|_| bad("BGEN identifier exceeds 65535 bytes"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
fn wide(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| bad("BGEN allele exceeds uint32 length"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
#[derive(Clone, Copy)]
enum Codec {
    Zlib,
    Zstd,
}
impl Codec {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "zlib" => Ok(Self::Zlib),
            "zstd" => Ok(Self::Zstd),
            _ => Err(bad("BGEN compression_codec must be zlib or zstd")),
        }
    }
    fn flag(self) -> u32 {
        match self {
            Self::Zlib => 1,
            Self::Zstd => 2,
        }
    }
    fn compress(self, data: &[u8], out: &mut Vec<u8>) -> Result<()> {
        out.clear();
        match self {
            Self::Zlib => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(out, flate2::Compression::default());
                encoder.write_all(data).map_err(|e| bad(e.to_string()))?;
                encoder.finish().map_err(|e| bad(e.to_string()))?;
            }
            Self::Zstd => {
                let mut encoder =
                    zstd::stream::write::Encoder::new(out, 0).map_err(|e| bad(e.to_string()))?;
                encoder.write_all(data).map_err(|e| bad(e.to_string()))?;
                encoder.finish().map_err(|e| bad(e.to_string()))?;
            }
        }
        Ok(())
    }
}
/// Bind all schema positions before execution; invalid plans cannot begin output.
struct Layout {
    contig: usize,
    position: usize,
    alleles: usize,
    varid: usize,
    rsid: usize,
    gp: usize,
}
impl Layout {
    fn new(schema: &Schema) -> Result<Self> {
        let position = |name: &str| schema.index_of(name).map_err(|e| bad(e.to_string()));
        let value = Self {
            contig: position("contig")?,
            position: position("position")?,
            alleles: position("alleles")?,
            varid: position("varid")?,
            rsid: position("rsid")?,
            gp: position("gp")?,
        };
        if schema.field(value.position).data_type() != &DataType::Int32 {
            return Err(bad("BGEN position must be int32"));
        }
        for index in [value.contig, value.varid, value.rsid] {
            if !matches!(
                schema.field(index).data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Err(bad("BGEN identifying fields must be strings"));
            }
        }
        let DataType::List(allele) = schema.field(value.alleles).data_type() else {
            return Err(bad("BGEN alleles must be array<str>"));
        };
        if !matches!(
            allele.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(bad("BGEN alleles must be array<str>"));
        }
        let DataType::List(samples) = schema.field(value.gp).data_type() else {
            return Err(bad("BGEN gp must be array<array<float64>>"));
        };
        let DataType::List(probability) = samples.data_type() else {
            return Err(bad("BGEN gp must be array<array<float64>>"));
        };
        if probability.data_type() != &DataType::Float64 {
            return Err(bad("BGEN gp must be array<array<float64>>"));
        }
        Ok(value)
    }
}
fn header(samples: &[String], codec: Codec, variants: u32) -> Result<Vec<u8>> {
    let n = u32::try_from(samples.len()).map_err(|_| bad("BGEN sample count exceeds uint32"))?;
    let mut block = Vec::new();
    block.extend_from_slice(&0u32.to_le_bytes());
    block.extend_from_slice(&n.to_le_bytes());
    for sample in samples {
        short(&mut block, sample)?;
    }
    let len = u32::try_from(block.len()).map_err(|_| bad("BGEN sample block exceeds uint32"))?;
    block[..4].copy_from_slice(&len.to_le_bytes());
    let offset = len
        .checked_add(20)
        .ok_or_else(|| bad("BGEN sample block offset overflow"))?;
    let mut result = Vec::with_capacity(block.len() + 24);
    result.extend_from_slice(&offset.to_le_bytes());
    result.extend_from_slice(&20u32.to_le_bytes());
    result.extend_from_slice(&variants.to_le_bytes());
    result.extend_from_slice(&n.to_le_bytes());
    result.extend_from_slice(b"bgen");
    result.extend_from_slice(&(codec.flag() | (2 << 2) | (1 << 31)).to_le_bytes());
    result.extend_from_slice(&block);
    Ok(result)
}
/// Constant-sum rounding from the BGEN specification: floor scaled values and
/// round the largest fractional parts upward until their total is exactly 255.
fn quantize(
    values: &Float64Array,
    start: usize,
    end: usize,
    scaled: &mut Vec<u16>,
    order: &mut Vec<(f64, usize)>,
) -> Result<bool> {
    scaled.clear();
    order.clear();
    let mut sum = 0.0;
    for index in start..end {
        if values.is_null(index) {
            return Ok(false);
        }
        let value = values.value(index);
        if value < 0.0 {
            return Err(bad("found GP value less than 0"));
        }
        if !value.is_finite() {
            return Ok(false);
        }
        sum += value;
    }
    if !(0.999..=1.001).contains(&sum) {
        return Ok(false);
    }
    let mut floor_sum = 0usize;
    for (index, offset) in (start..end).enumerate() {
        let value = values.value(offset) * 255.0;
        let floor = value.floor() as u16;
        floor_sum += usize::from(floor);
        scaled.push(floor);
        order.push((value - f64::from(floor), index));
    }
    let deficit = 255usize
        .checked_sub(floor_sum)
        .ok_or_else(|| bad("BGEN GP values cannot be rounded to unit sum"))?;
    if deficit > scaled.len() {
        return Err(bad("BGEN GP values cannot be rounded to unit sum"));
    }
    order.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    for &(_, index) in &order[..deficit] {
        scaled[index] += 1;
    }
    if scaled.iter().any(|v| *v > 255) {
        return Err(bad("BGEN rounded GP exceeds one"));
    }
    Ok(true)
}

pub async fn export(
    table: TableRef,
    path: &str,
    samples: &[String],
    parallel: Option<&str>,
    codec: &str,
) -> Result<()> {
    let codec = Codec::parse(codec)?;
    if !matches!(
        parallel,
        None | Some("header_per_shard") | Some("separate_header")
    ) {
        return Err(bad("invalid BGEN parallel export mode"));
    }
    let layout = Layout::new(table.schema())?;
    let prepared = crate::partitioned::prepare(table.clone()).await?;
    let plans = if parallel.is_none() {
        vec![prepared.ordered()]
    } else {
        prepared.parts.clone()
    };
    let destination = std::path::Path::new(path);
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent).map_err(|e| bad(e.to_string()))?;
    if parallel.is_some() {
        std::fs::create_dir_all(destination).map_err(|e| bad(e.to_string()))?;
    }
    let mut manifest = Vec::new();
    let mut total = 0u32;
    let mut record = Vec::new();
    let mut data = Vec::new();
    let mut compressed = Vec::new();
    let mut scaled = Vec::new();
    let mut order = Vec::new();
    for (part, plan) in plans.into_iter().enumerate() {
        let name = format!("part-{part:05}");
        let target = if parallel.is_some() {
            manifest.push(name.clone());
            destination.join(&name)
        } else {
            destination.to_path_buf()
        };
        let mut output = tempfile::NamedTempFile::new_in(if parallel.is_some() {
            destination
        } else {
            parent
        })
        .map_err(|e| bad(e.to_string()))?;
        let mut writer = BufWriter::new(output.as_file_mut());
        let own_header = parallel != Some("separate_header");
        if own_header {
            writer
                .write_all(&header(samples, codec, 0)?)
                .map_err(|e| bad(e.to_string()))?;
        }
        let mut variants = 0u32;
        let mut stream = plan.execute(0, Arc::new(datafusion_execution::TaskContext::default()))?;
        while let Some(batch) = stream.try_next().await? {
            let contigs = Text::bind(batch.column(layout.contig).as_ref())?;
            let positions: &Int32Array = down(batch.column(layout.position).as_ref())?;
            let varids = Text::bind(batch.column(layout.varid).as_ref())?;
            let rsids = Text::bind(batch.column(layout.rsid).as_ref())?;
            let allele_lists: &ListArray = down(batch.column(layout.alleles).as_ref())?;
            let allele_text = Text::bind(allele_lists.values().as_ref())?;
            let gps: &ListArray = down(batch.column(layout.gp).as_ref())?;
            let genotype_lists: &ListArray = down(gps.values().as_ref())?;
            let probabilities: &Float64Array = down(genotype_lists.values().as_ref())?;
            for row in 0..batch.num_rows() {
                if positions.is_null(row)
                    || positions.value(row) < 0
                    || allele_lists.is_null(row)
                    || gps.is_null(row)
                {
                    return Err(bad(
                        "BGEN requires defined locus, alleles, and sample array",
                    ));
                }
                let astart = allele_lists.value_offsets()[row] as usize;
                let aend = allele_lists.value_offsets()[row + 1] as usize;
                let k = u16::try_from(aend - astart)
                    .map_err(|_| bad("BGEN allele count exceeds uint16"))?;
                if k < 2 {
                    return Err(bad("BGEN requires at least two alleles"));
                }
                let genotypes = usize::from(k)
                    .checked_mul(usize::from(k) + 1)
                    .and_then(|n| n.checked_div(2))
                    .ok_or_else(|| bad("BGEN genotype dimension overflow"))?;
                let start = gps.value_offsets()[row] as usize;
                let end = gps.value_offsets()[row + 1] as usize;
                if end - start != samples.len() {
                    return Err(bad("BGEN entry count disagrees with sample metadata"));
                }
                record.clear();
                short(&mut record, varids.get(row)?)?;
                short(&mut record, rsids.get(row)?)?;
                short(&mut record, contigs.get(row)?)?;
                record.extend_from_slice(&(positions.value(row) as u32).to_le_bytes());
                record.extend_from_slice(&k.to_le_bytes());
                for allele in astart..aend {
                    wide(&mut record, allele_text.get(allele)?)?;
                }
                data.clear();
                data.extend_from_slice(&(samples.len() as u32).to_le_bytes());
                data.extend_from_slice(&k.to_le_bytes());
                data.extend_from_slice(&[2, 2]);
                data.resize(8 + samples.len(), 0x82);
                data.extend_from_slice(&[0, 8]);
                for (sample, cell) in (start..end).enumerate() {
                    let first = genotype_lists.value_offsets()[cell] as usize;
                    let last = genotype_lists.value_offsets()[cell + 1] as usize;
                    let present = if genotype_lists.is_null(cell) {
                        false
                    } else {
                        if last - first != genotypes {
                            return Err(bad("BGEN GP array length disagrees with allele count"));
                        }
                        quantize(probabilities, first, last, &mut scaled, &mut order)?
                    };
                    if present {
                        data[8 + sample] = 2;
                        data.extend(scaled[..genotypes - 1].iter().map(|v| *v as u8));
                    } else {
                        data.resize(data.len() + genotypes - 1, 0);
                    }
                }
                codec.compress(&data, &mut compressed)?;
                let clen = u32::try_from(compressed.len())
                    .map_err(|_| bad("BGEN compressed variant block exceeds uint32"))?
                    .checked_add(4)
                    .ok_or_else(|| bad("BGEN compressed block length overflow"))?;
                let dlen = u32::try_from(data.len())
                    .map_err(|_| bad("BGEN probability block exceeds uint32"))?;
                record.extend_from_slice(&clen.to_le_bytes());
                record.extend_from_slice(&dlen.to_le_bytes());
                record.extend_from_slice(&compressed);
                writer.write_all(&record).map_err(|e| bad(e.to_string()))?;
                variants = variants
                    .checked_add(1)
                    .ok_or_else(|| bad("BGEN variant count exceeds uint32"))?;
            }
        }
        if own_header {
            writer
                .seek(SeekFrom::Start(8))
                .map_err(|e| bad(e.to_string()))?;
            writer
                .write_all(&variants.to_le_bytes())
                .map_err(|e| bad(e.to_string()))?;
        }
        writer.flush().map_err(|e| bad(e.to_string()))?;
        drop(writer);
        output
            .persist(&target)
            .map_err(|e| bad(e.error.to_string()))?;
        total = total
            .checked_add(variants)
            .ok_or_else(|| bad("BGEN variant count exceeds uint32"))?;
    }
    if parallel == Some("separate_header") {
        std::fs::write(destination.join("header"), header(samples, codec, total)?)
            .map_err(|e| bad(e.to_string()))?;
        manifest.insert(0, "header".into());
    }
    if parallel.is_some() {
        std::fs::write(
            destination.join("shard-manifest.txt"),
            manifest
                .iter()
                .map(|s| format!("{s}\n"))
                .collect::<String>(),
        )
        .map_err(|e| bad(e.to_string()))?;
    }
    Ok(())
}
