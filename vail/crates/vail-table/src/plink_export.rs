//! Stream localized Arrow variants directly to SNP-major BED and BIM files.
use crate::{Result, TableError, TableRef};
use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, LargeStringArray, ListArray, StringArray,
    StringViewArray, StructArray, UInt8Array, UInt16Array,
};
use futures::TryStreamExt;
use std::{
    io::{BufWriter, Write},
    sync::Arc,
};
fn bad(s: impl Into<String>) -> TableError {
    TableError::Invalid(s.into())
}
fn down<T: Array + 'static>(a: &dyn Array) -> Result<&T> {
    a.as_any()
        .downcast_ref()
        .ok_or_else(|| bad("PLINK Arrow type mismatch"))
}
enum Text<'a> {
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}
impl<'a> Text<'a> {
    fn bind(a: &'a dyn Array) -> Result<Self> {
        use arrow_schema::DataType;
        match a.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(down(a)?)),
            DataType::LargeUtf8 => Ok(Self::Large(down(a)?)),
            DataType::Utf8View => Ok(Self::View(down(a)?)),
            _ => Err(bad("PLINK text field must be a string")),
        }
    }
    fn value(&self, i: usize) -> Result<&str> {
        let (missing, s) = match self {
            Self::Utf8(a) => (a.is_null(i), a.value(i)),
            Self::Large(a) => (a.is_null(i), a.value(i)),
            Self::View(a) => (a.is_null(i), a.value(i)),
        };
        if missing || s.chars().any(char::is_whitespace) {
            Err(bad("no white space allowed: PLINK BIM field"))
        } else {
            Ok(s)
        }
    }
}
pub async fn export(table: TableRef, path: &str, sample_count: usize) -> Result<()> {
    let schema = table.schema();
    let index = |name: &str| schema.index_of(name).map_err(|e| bad(e.to_string()));
    let contig = index("contig")?;
    let varid = index("varid")?;
    let cm = index("cm")?;
    let pos = index("position")?;
    let a1 = index("a1")?;
    let a2 = index("a2")?;
    let entries = index("entries")?;
    let plan = crate::partitioned::prepare(table.clone()).await?.ordered();
    let mut batches = plan.execute(0, Arc::new(datafusion_execution::TaskContext::default()))?;
    let mut bed = BufWriter::new(
        std::fs::File::create(format!("{path}.bed")).map_err(|e| bad(e.to_string()))?,
    );
    let mut bim = BufWriter::new(
        std::fs::File::create(format!("{path}.bim")).map_err(|e| bad(e.to_string()))?,
    );
    bed.write_all(&[0x6c, 0x1b, 1])
        .map_err(|e| bad(e.to_string()))?;
    let mut packed = vec![0u8; sample_count.div_ceil(4)];
    while let Some(batch) = batches.try_next().await? {
        // Resolve array storage and Call field positions once per batch.
        let contigs = Text::bind(batch.column(contig).as_ref())?;
        let ids = Text::bind(batch.column(varid).as_ref())?;
        let allele1 = Text::bind(batch.column(a1).as_ref())?;
        let allele2 = Text::bind(batch.column(a2).as_ref())?;
        let cms: &Float64Array = down(batch.column(cm).as_ref())?;
        let positions: &Int32Array = down(batch.column(pos).as_ref())?;
        let lists: &ListArray = down(batch.column(entries).as_ref())?;
        let entry: &StructArray = down(lists.values().as_ref())?;
        let calls: &StructArray = down(
            entry
                .column_by_name("_plink_gt")
                .ok_or_else(|| bad("PLINK GT absent"))?
                .as_ref(),
        )?;
        let field = |name: &str| {
            calls
                .column_by_name(name)
                .map(|a| a.as_ref())
                .ok_or_else(|| bad("PLINK Call field absent"))
        };
        let first: &UInt16Array = down(field(vail_execution::calls::FIRST)?)?;
        let second: &UInt16Array = down(field(vail_execution::calls::SECOND)?)?;
        let ploidy: &UInt8Array = down(field(vail_execution::calls::PLOIDY)?)?;
        let phased: &BooleanArray = down(field(vail_execution::calls::PHASED)?)?;
        for row in 0..batch.num_rows() {
            if lists.is_null(row) || cms.is_null(row) || positions.is_null(row) {
                return Err(bad("missing PLINK row metadata"));
            }
            let start = lists.value_offsets()[row] as usize;
            let end = lists.value_offsets()[row + 1] as usize;
            if end - start != sample_count {
                return Err(bad("PLINK entry count mismatch"));
            }
            packed.fill(0);
            for (i, j) in (start..end).enumerate() {
                let code = if entry.is_null(j) || calls.is_null(j) {
                    1
                } else {
                    if ploidy.value(j) != 2
                        || phased.value(j)
                        || first.value(j) > 1
                        || second.value(j) > 1
                    {
                        return Err(bad("PLINK requires unphased diploid biallelic calls"));
                    }
                    match (first.value(j), second.value(j)) {
                        (0, 0) => 3,
                        (1, 1) => 0,
                        _ => 2,
                    }
                };
                packed[i / 4] |= code << (2 * (i % 4));
            }
            writeln!(
                bim,
                "{}\t{}\t{:?}\t{}\t{}\t{}",
                contigs.value(row)?,
                ids.value(row)?,
                cms.value(row),
                positions.value(row),
                allele1.value(row)?,
                allele2.value(row)?
            )
            .map_err(|e| bad(e.to_string()))?;
            bed.write_all(&packed).map_err(|e| bad(e.to_string()))?;
        }
    }
    bed.flush().map_err(|e| bad(e.to_string()))?;
    bim.flush().map_err(|e| bad(e.to_string()))?;
    Ok(())
}
