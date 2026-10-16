//! Bounded VCF sink: rows stay in Arrow and are consumed once, batch by batch.
use crate::{PartitionEvent, Result, TableError, TableRef};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, ListArray, StringArray,
    StructArray,
};
use futures::TryStreamExt;
use std::io::{BufWriter, Write};
use vail_execution::expressions::{PrimitiveType, Type};

fn invalid(message: impl Into<String>) -> TableError {
    TableError::Invalid(message.into())
}
fn io(error: std::io::Error) -> TableError {
    TableError::Storage(error.into())
}
fn checked(value: &str) -> Result<&str> {
    if value.contains(['\t', '\r', '\n']) {
        return Err(invalid("VCF values must not contain tabs or newlines"));
    }
    Ok(value)
}
fn structure(array: &ArrayRef) -> Result<&StructArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| invalid("VCF expected struct storage"))
}
fn list(array: &ArrayRef) -> Result<&ListArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| invalid("VCF expected array storage"))
}
fn field<'a>(array: &'a StructArray, name: &str) -> Result<&'a ArrayRef> {
    array
        .column_by_name(name)
        .ok_or_else(|| invalid(format!("VCF field {name:?} is missing")))
}
fn text(array: &ArrayRef, row: usize, dtype: &Type) -> Result<String> {
    if array.is_null(row) {
        return Ok(if *dtype == Type::Primitive(PrimitiveType::Call) {
            "./."
        } else {
            "."
        }
        .into());
    }
    let result = match dtype {
        Type::Array { array: element } => {
            let values = list(array)?.value(row);
            let pieces = (0..values.len())
                .map(|i| text(&values, i, element))
                .collect::<Result<Vec<_>>>()?;
            if pieces.is_empty() {
                ".".into()
            } else {
                pieces.join(",")
            }
        }
        Type::Primitive(PrimitiveType::Call) => {
            let word = field(structure(array)?, "__vail_call_word")?
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| invalid("invalid Call storage"))?
                .value(row);
            let (alleles, phased) = vail_execution::calls::unpack(word).map_err(invalid)?;
            if alleles.is_empty() {
                ".".into()
            } else {
                alleles
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(if phased { "|" } else { "/" })
            }
        }
        Type::Primitive(PrimitiveType::Str) => array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("invalid string storage"))?
            .value(row)
            .to_owned(),
        Type::Primitive(PrimitiveType::Int32) => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .to_string(),
        Type::Primitive(PrimitiveType::Float32) => float_text(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row) as f64,
        ),
        Type::Primitive(PrimitiveType::Float64) => float_text(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        _ => return Err(invalid(format!("unsupported VCF value type {dtype:?}"))),
    };
    checked(&result)?;
    Ok(result)
}
fn float_text(value: f64) -> String {
    if value.is_nan() {
        "nan".into()
    } else if value == f64::INFINITY {
        "inf".into()
    } else if value == f64::NEG_INFINITY {
        "-inf".into()
    } else {
        let value = value.to_string();
        if value.contains(['.', 'e', 'E']) {
            value
        } else {
            format!("{value}.0")
        }
    }
}

/// Publish only a completely written file. A failed upstream batch or formatter
/// leaves an existing destination untouched and removes the temporary sibling.
pub async fn export(
    table: TableRef,
    path: &str,
    header: &[String],
    entries_name: &str,
    entry_names: &[String],
    sample_count: usize,
) -> Result<()> {
    let destination = std::path::Path::new(path);
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut output = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
    {
        let mut writer = BufWriter::new(output.as_file_mut());
        for line in header {
            writeln!(writer, "{line}").map_err(io)?;
        }
        write_batches(
            &mut writer,
            table.execute(),
            entries_name,
            entry_names,
            sample_count,
        )
        .await?;
        writer.flush().map_err(io)?;
    }
    output.persist(destination).map_err(|e| io(e.error))?;
    Ok(())
}

async fn write_batches(
    writer: &mut impl Write,
    mut batches: crate::BatchStream,
    entries_name: &str,
    entry_names: &[String],
    sample_count: usize,
) -> Result<()> {
    let format_text = if entry_names.is_empty() {
        ".".to_owned()
    } else {
        entry_names.join(":")
    };
    while let Some(event) = batches.try_next().await? {
        let PartitionEvent::Batch(batch) = event else {
            continue;
        };
        let rows = StructArray::from(batch);
        let loci = structure(field(&rows, "locus")?)?;
        let contigs = loci
            .column_by_name("__vail_locus_contig")
            .or_else(|| loci.column_by_name("contig"))
            .ok_or_else(|| invalid("invalid locus contig storage"))?;
        let positions = loci
            .column_by_name("__vail_locus_position")
            .or_else(|| loci.column_by_name("position"))
            .ok_or_else(|| invalid("invalid locus position storage"))?;
        let alleles = list(field(&rows, "alleles")?)?;
        let entries = list(field(&rows, entries_name)?)?;
        let arrow_schema::DataType::Struct(entry_fields) = entries.value_type() else {
            return Err(invalid("VCF entries must be structs"));
        };
        let cells = structure(entries.values())?;
        let entry_indices: std::collections::HashMap<_, _> = entry_fields
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name().as_str(), index))
            .collect();
        let entry_types = entry_names
            .iter()
            .map(|name| {
                let index = *entry_indices
                    .get(name.as_str())
                    .ok_or_else(|| invalid(format!("missing FORMAT field {name}")))?;
                Ok((cells.column(index), Type::from_arrow(entry_fields[index].data_type())?))
            })
            .collect::<Result<Vec<_>>>()?;
        let infos = rows.column_by_name("info").map(structure).transpose()?;
        let info_types = infos
            .map(|info| {
                info.fields()
                    .iter().zip(info.columns())
                    .map(|(f, array)| Ok((f.name(), array, Type::from_arrow(f.data_type())?)))
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let optional = |name: &str| -> Result<Option<(&ArrayRef, Type)>> {
            rows.column_by_name(name)
                .map(|array| Ok((array, Type::from_arrow(array.data_type())?)))
                .transpose()
        };
        let rsids = optional("rsid")?;
        let qualities = optional("qual")?;
        let filters = rows.column_by_name("filters").map(list).transpose()?;
        for row in 0..rows.len() {
            if loci.is_null(row) || alleles.is_null(row) {
                return Err(invalid("VCF locus and alleles must be defined"));
            }
            let allele_values = alleles.value(row);
            if allele_values.is_empty() {
                return Err(invalid("VCF requires at least a reference allele"));
            }
            let strings = allele_values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("VCF alleles must be strings"))?;
            let mut fixed = vec![
                text(contigs, row, &Type::Primitive(PrimitiveType::Str))?,
                text(positions, row, &Type::Primitive(PrimitiveType::Int32))?,
            ];
            let optional = |binding: &Option<(&ArrayRef, Type)>| -> Result<String> {
                binding.as_ref()
                    .map(|(array, dtype)| text(array, row, dtype))
                    .unwrap_or_else(|| Ok(".".into()))
            };
            let rsid = optional(&rsids)?;
            fixed.push(if rsid.is_empty() { ".".into() } else { rsid });
            fixed.push(checked(strings.value(0))?.into());
            fixed.push(if strings.len() == 1 {
                ".".into()
            } else {
                (1..strings.len())
                    .map(|i| checked(strings.value(i)))
                    .collect::<Result<Vec<_>>>()?
                    .join(",")
            });
            fixed.push(optional(&qualities)?);
            let rendered_filters = match filters {
                None => ".".into(),
                Some(a) if a.is_null(row) => ".".into(),
                Some(a) => {
                    let values = a.value(row);
                    let values = values
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| invalid("VCF filters must be strings"))?;
                    let mut names = (0..values.len())
                        .map(|i| checked(values.value(i)))
                        .collect::<Result<Vec<_>>>()?;
                    names.sort();
                    if names.is_empty() {
                        "PASS".into()
                    } else {
                        names.join(";")
                    }
                }
            };
            fixed.push(rendered_filters);
            let mut info_values = Vec::new();
            if let Some(info) = infos {
                if !info.is_null(row) {
                    for (name, values, dtype) in &info_types {
                        if values.is_null(row) {
                            continue;
                        }
                        if *dtype == Type::Primitive(PrimitiveType::Bool) {
                            if values
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .value(row)
                            {
                                info_values.push((*name).clone());
                            }
                        } else {
                            info_values.push(format!("{name}={}", text(values, row, dtype)?));
                        }
                    }
                }
            }
            fixed.push(if info_values.is_empty() {
                ".".into()
            } else {
                info_values.join(";")
            });
            if sample_count != 0 {
                let start = entries.value_offsets()[row] as usize;
                let end = entries.value_offsets()[row + 1] as usize;
                if end - start != sample_count {
                    return Err(invalid("VCF entry count does not match sample count"));
                }
                fixed.push(format_text.clone());
                for sample in start..end {
                    let values = entry_types
                        .iter()
                        .map(|(values, dtype)| {
                            if cells.is_null(sample) {
                                Ok(if *dtype == Type::Primitive(PrimitiveType::Call) {
                                    "./."
                                } else {
                                    "."
                                }
                                .into())
                            } else {
                                text(values, sample, dtype)
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    fixed.push(if values.is_empty() {
                        ".".into()
                    } else {
                        values.join(":")
                    });
                }
            }
            writeln!(writer, "{}", fixed.join("\t")).map_err(io)?;
        }
    }
    Ok(())
}
