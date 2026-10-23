//! Hail native storage descriptors and streaming decoders. Opening a dataset
//! reads metadata only; every part is an independently schedulable scan task.
mod decode;
mod encoded_int;
pub mod index;
mod input;
pub mod layout;
pub mod matrix_layout;
mod packed_call;
mod schema;
use arrow_schema::{Schema, SchemaRef};
pub use schema::{Encoding, Parser, Relation};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use vail_execution::{expressions::Type, reference::ReferenceDefinition};
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub fn metadata(path: &Path) -> Result<Value> {
    Ok(serde_json::from_reader(flate2::read::GzDecoder::new(
        std::fs::File::open(path.join("metadata.json.gz"))?,
    ))?)
}
fn reference(value: Value) -> Result<ReferenceDefinition> {
    if value.get("lengths").is_some() {
        return Ok(serde_json::from_value(value)?);
    }
    let contigs = value["contigs"]
        .as_array()
        .ok_or("invalid Hail reference contigs")?;
    let definition = ReferenceDefinition {
        name: value["name"]
            .as_str()
            .ok_or("missing reference name")?
            .into(),
        contigs: contigs
            .iter()
            .map(|c| c["name"].as_str().unwrap_or_default().into())
            .collect(),
        lengths: contigs
            .iter()
            .map(|c| {
                Ok((
                    c["name"].as_str().ok_or("invalid contig")?.into(),
                    i32::try_from(c["length"].as_i64().ok_or("invalid contig length")?)?,
                ))
            })
            .collect::<Result<_>>()?,
        x_contigs: serde_json::from_value(
            value
                .get("xContigs")
                .cloned()
                .unwrap_or(serde_json::json!([])),
        )?,
        y_contigs: serde_json::from_value(
            value
                .get("yContigs")
                .cloned()
                .unwrap_or(serde_json::json!([])),
        )?,
        mt_contigs: serde_json::from_value(
            value
                .get("mtContigs")
                .cloned()
                .unwrap_or(serde_json::json!([])),
        )?,
        par: value["par"]
            .as_array()
            .map(|ps| {
                ps.iter()
                    .map(|p| {
                        if p["start"]["contig"] != p["end"]["contig"] {
                            return Err("PAR interval endpoints must be on the same contig".into());
                        }
                        Ok(vail_execution::reference::ParInterval {
                            contig: p["start"]["contig"]
                                .as_str()
                                .ok_or("invalid PAR contig")?
                                .into(),
                            start: i32::try_from(
                                p["start"]["position"].as_i64().ok_or("invalid PAR start")?,
                            )?,
                            end: i32::try_from(
                                p["end"]["position"].as_i64().ok_or("invalid PAR end")?,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default(),
    };
    definition.validate()?;
    Ok(definition)
}
fn local_path(path: &str) -> Result<PathBuf> {
    let path = if let Some(uri) = path.strip_prefix("file:") {
        let uri = if let Some(authority) = uri.strip_prefix("//") {
            if authority.starts_with('/') {
                authority
            } else {
                authority
                    .strip_prefix("localhost/")
                    .map(|_| &authority["localhost".len()..])
                    .ok_or("legacy input supports only local file URIs")?
            }
        } else {
            uri
        };
        let uri = uri.split(['?', '#']).next().unwrap();
        let mut bytes = Vec::with_capacity(uri.len());
        let raw = uri.as_bytes();
        let mut i = 0;
        while i < raw.len() {
            if raw[i] == b'%' {
                let encoded = uri.get(i + 1..i + 3).ok_or("invalid file URI escape")?;
                bytes.push(u8::from_str_radix(encoded, 16)?);
                i += 3;
            } else {
                bytes.push(raw[i]);
                i += 1;
            }
        }
        PathBuf::from(String::from_utf8(bytes)?)
    } else {
        if path.contains("://") {
            return Err("legacy input supports only local paths".into());
        }
        PathBuf::from(path)
    };
    Ok(std::path::absolute(path)?)
}
#[derive(Debug, Clone)]
pub struct Dataset {
    pub root: PathBuf,
    pub metadata: Value,
    pub references: HashMap<String, ReferenceDefinition>,
}
impl Dataset {
    pub fn open(path: &str, expected: &str) -> Result<Self> {
        let root = local_path(path)?;
        let metadata = metadata(&root)?;
        if metadata["name"] != expected {
            return Err(format!("expected {expected}, found {}", metadata["name"]).into());
        }
        if !metadata["file_version"]
            .as_u64()
            .is_some_and(|v| (65536..=67584).contains(&v))
        {
            return Err(format!(
                "unsupported Hail native file version {}",
                metadata["file_version"]
            )
            .into());
        }
        let mut references = HashMap::new();
        for data in [
            include_str!("../../../../python/vail/reference_data/grch37.json"),
            include_str!("../../../../python/vail/reference_data/grch38.json"),
        ] {
            let r = reference(serde_json::from_str(data)?)?;
            references.insert(r.name.clone(), r);
        }
        let dir = root.join(
            metadata["references_rel_path"]
                .as_str()
                .unwrap_or("references"),
        );
        if dir.exists() {
            for entry in std::fs::read_dir(dir)? {
                let path = entry?.path();
                let name = path.to_string_lossy();
                let r = if name.ends_with(".json.gz") {
                    Some(reference(serde_json::from_reader(
                        flate2::read::GzDecoder::new(std::fs::File::open(path)?),
                    )?)?)
                } else if name.ends_with(".json") {
                    Some(reference(serde_json::from_reader(std::fs::File::open(
                        path,
                    )?)?)?)
                } else {
                    None
                };
                if let Some(r) = r {
                    references.insert(r.name.clone(), r);
                }
            }
        }
        Ok(Self {
            root,
            metadata,
            references,
        })
    }
    pub fn relation(&self) -> Result<Relation> {
        let name = if self.metadata["name"] == "TableSpec" {
            "table_type"
        } else {
            "matrix_type"
        };
        let mut parser = Parser::new(
            self.metadata[name]
                .as_str()
                .ok_or("missing Hail relational type")?,
            &self.references,
        );
        let r = parser.relation()?;
        parser.finish()?;
        Ok(r)
    }
    pub fn component(&self, name: &str) -> Result<Component> {
        let c = &self.metadata["components"][name];
        if c["name"] != "RVDComponentSpec" {
            return Err(format!("invalid Hail {name} component").into());
        }
        Component::open(
            &self.root.join(
                c["rel_path"]
                    .as_str()
                    .ok_or("invalid Hail component path")?,
            ),
            &self.references,
        )
    }
    pub fn counts(&self) -> Result<Option<Vec<u64>>> {
        self.metadata["components"]["partition_counts"]["counts"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|v| {
                        v.as_u64()
                            .ok_or_else(|| "invalid Hail partition counts".into())
                    })
                    .collect()
            })
            .transpose()
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Component {
    pub schema: SchemaRef,
    pub dtype: Type,
    pub encoding: Encoding,
    pub buffer: Value,
    pub paths: Vec<PathBuf>,
    pub int_key_bounds: Vec<Option<IntKeyBounds>>,
    pub index: Option<Arc<index::IndexSpec>>,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IntKeyBounds {
    pub key: String,
    pub min: i64,
    pub max: i64,
}
impl Component {
    pub fn open(path: &Path, refs: &HashMap<String, ReferenceDefinition>) -> Result<Self> {
        let spec = metadata(path)?;
        let (dtype, encoding, buffer, parts) = match spec["name"].as_str().unwrap_or("") {
            "OrderedRVDSpec2" | "IndexedRVDSpec2" => {
                let c = &spec["_codecSpec"];
                if c["name"] != "TypedCodecSpec" {
                    return Err("unsupported Hail typed codec".into());
                }
                let mut p = Parser::new(
                    c["_vType"].as_str().ok_or("missing Hail virtual type")?,
                    refs,
                );
                let (t, _) = p.virtual_type()?;
                p.finish()?;
                let mut p = Parser::new(
                    c["_eType"].as_str().ok_or("missing Hail encoded type")?,
                    refs,
                );
                let e = p.encoded()?;
                p.finish()?;
                (t, e, c["_bufferSpec"].clone(), &spec["_partFiles"])
            }
            "OrderedRVDSpec" | "IndexedRVDSpec" | "UnpartitionedRVDSpec" => {
                let c = &spec["codecSpec"];
                if c["name"] != "PackCodecSpec" {
                    return Err("unsupported Hail packed codec".into());
                }
                let (t, mut e) = if spec["name"] == "UnpartitionedRVDSpec" {
                    let mut p = Parser::new(
                        spec["rowType"].as_str().ok_or("missing Hail row type")?,
                        refs,
                    );
                    let t = p.virtual_type()?;
                    p.finish()?;
                    t
                } else {
                    let mut p = Parser::new(
                        spec.get("rvdType")
                            .or(spec.get("orvdType"))
                            .and_then(Value::as_str)
                            .ok_or("missing Hail RVD type")?,
                        refs,
                    );
                    let mut r = p.relation()?;
                    p.finish()?;
                    r.types.remove("row").ok_or("missing Hail row type")?
                };
                e.required = true;
                (t, e, c["child"].clone(), &spec["partFiles"])
            }
            name => return Err(format!("unsupported Hail RVD {name}").into()),
        };
        let arrow = dtype.to_arrow()?;
        let arrow_schema::DataType::Struct(fields) = arrow else {
            return Err("Hail rows must be structs".into());
        };
        let schema = Arc::new(Schema::new(fields));
        let paths = parts
            .as_array()
            .ok_or("missing Hail part files")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|v| path.join("parts").join(v))
                    .ok_or_else(|| "invalid Hail part filename".into())
            })
            .collect::<Result<Vec<_>>>()?;
        let int_key_bounds = integer_key_bounds(&spec, &dtype);
        let index = index::IndexSpec::from_rvd(path, &spec, refs)?.map(Arc::new);
        decode::check(&encoding, &dtype)?;
        input::validate(&buffer)?;
        Ok(Self {
            schema,
            dtype,
            encoding,
            buffer,
            paths,
            int_key_bounds,
            index,
        })
    }
    pub fn reader(&self, part: usize, projection: &[usize]) -> Result<decode::PartReader> {
        decode::PartReader::new(self, part, projection)
    }
    /// An independent bounded row slice. The index seek avoids scanning the prefix.
    pub fn reader_slice(
        &self,
        part: usize,
        projection: &[usize],
        range: std::ops::Range<u64>,
    ) -> Result<decode::PartReader> {
        let spec = self
            .index
            .clone()
            .ok_or("legacy part does not have a seek index")?;
        let path = self
            .paths
            .get(part)
            .ok_or("legacy part index out of bounds")?;
        let mut index = index::IndexReader::open(spec, path)?;
        if range.start > range.end || range.end > index.rows {
            return Err("legacy row slice out of bounds".into());
        }
        let mut reader = self.reader(part, projection)?;
        if range.start < index.rows {
            let offset = index.row_offset(range.start)?;
            reader.seek(self, range.start, offset)?;
        } else {
            reader.rows = range.start;
        }
        Ok(reader.with_end_row(range.end))
    }
    pub fn collect_small(&self) -> Result<arrow_array::RecordBatch> {
        let mut batches = vec![];
        let projection = (0..self.schema.fields().len()).collect::<Vec<_>>();
        for p in 0..self.paths.len() {
            let mut reader = self.reader(p, &projection)?;
            while let Some(batch) = reader.next_batch(8192)? {
                batches.push(batch)
            }
        }
        Ok(arrow_select::concat::concat_batches(
            &self.schema,
            batches.iter(),
        )?)
    }
}

fn integer_key_bounds(spec: &Value, dtype: &Type) -> Vec<Option<IntKeyBounds>> {
    use vail_execution::expressions::PrimitiveType;
    let Some(keys) = spec
        .get("_key")
        .and_then(Value::as_array)
        .filter(|k| k.len() == 1)
    else {
        return vec![];
    };
    let Some(key) = keys[0].as_str() else {
        return vec![];
    };
    let Type::Struct { r#struct: fields } = dtype else {
        return vec![];
    };
    if !fields.iter().any(|(name, ty)| {
        name == key
            && matches!(
                ty,
                Type::Primitive(PrimitiveType::Int32 | PrimitiveType::Int64)
            )
    }) {
        return vec![];
    }
    let Some(bounds) = spec.get("_jRangeBounds").and_then(Value::as_array) else {
        return vec![];
    };
    bounds
        .iter()
        .map(|b| {
            let mut min = b.get("start")?.get(key)?.as_i64()?;
            let mut max = b.get("end")?.get(key)?.as_i64()?;
            if !b.get("includeStart")?.as_bool()? {
                min = min.checked_add(1)?;
            }
            if !b.get("includeEnd")?.as_bool()? {
                max = max.checked_sub(1)?;
            }
            (min <= max).then(|| IntKeyBounds {
                key: key.into(),
                min,
                max,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "requires the pinned Hail compatibility corpus; set VAIL_HAIL_RESOURCES"]
    fn indexed_row_slices_match_full_reads() {
        use arrow_array::Array;
        let root = PathBuf::from(std::env::var("VAIL_HAIL_RESOURCES").unwrap())
            .join("backward_compatability");
        let mut checked = 0;
        for version in std::fs::read_dir(root).unwrap() {
            let dir = version.unwrap().path().join("table");
            if !dir.is_dir() {
                continue;
            }
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if !path.is_dir() {
                    continue;
                }
                let dataset = Dataset::open(path.to_str().unwrap(), "TableSpec").unwrap();
                let c = dataset.component("rows").unwrap();
                if c.index.is_none() {
                    continue;
                }
                for part in 0..c.paths.len() {
                    let mut reader = c.reader(part, &[0]).unwrap();
                    let mut full = vec![];
                    while let Some(batch) = reader.next_batch(8192).unwrap() {
                        full.push(batch)
                    }
                    let schema = Arc::new(c.schema.project(&[0]).unwrap());
                    let full = arrow_select::concat::concat_batches(&schema, full.iter()).unwrap();
                    let start = full.num_rows() / 2;
                    let end = (start + 2).min(full.num_rows());
                    let mut slice = c
                        .reader_slice(part, &[0], start as u64..end as u64)
                        .unwrap_or_else(|e| panic!("{}: {e}", c.paths[part].display()));
                    if start < end {
                        let actual = slice.next_batch(8192).unwrap().unwrap();
                        assert_eq!(actual.num_rows(), end - start);
                        assert_eq!(
                            actual.column(0).to_data(),
                            full.column(0).slice(start, end - start).to_data()
                        );
                    }
                    assert!(slice.next_batch(8192).unwrap().is_none());
                    checked += 1;
                }
            }
        }
        assert!(checked > 0);
    }
    #[test]
    #[ignore = "requires the pinned Hail compatibility corpus; set VAIL_HAIL_RESOURCES"]
    fn hail_compatibility_corpus_decodes_in_native_columns() {
        let root = PathBuf::from(
            std::env::var("VAIL_HAIL_RESOURCES")
                .expect("set VAIL_HAIL_RESOURCES to the Hail test resources"),
        );
        let mut tables = 0;
        let mut matrices = 0;
        for version in std::fs::read_dir(root.join("backward_compatability")).unwrap() {
            let version = version.unwrap().path();
            for (kind, expected) in [("table", "TableSpec"), ("matrix_table", "MatrixTableSpec")] {
                let dir = version.join(kind);
                if !dir.exists() {
                    continue;
                }
                for entry in std::fs::read_dir(dir).unwrap() {
                    let path = entry.unwrap().path();
                    if !path.is_dir() {
                        continue;
                    }
                    let dataset = Dataset::open(path.to_str().unwrap(), expected).unwrap();
                    let relation = dataset.relation().unwrap();
                    for component in if kind == "table" {
                        vec!["rows", "globals"]
                    } else {
                        vec!["rows", "cols", "entries", "globals"]
                    } {
                        let c = dataset.component(component).unwrap();
                        if component != "entries" {
                            assert_eq!(
                                c.dtype,
                                relation.types[if component == "rows" {
                                    "row"
                                } else if component == "cols" {
                                    "col"
                                } else {
                                    "global"
                                }]
                                .0
                            )
                        }
                        let batch = c
                            .collect_small()
                            .unwrap_or_else(|e| panic!("{} {component}: {e}", path.display()));
                        assert_eq!(
                            batch.num_rows(),
                            if component == "globals" {
                                1
                            } else if kind == "table" {
                                5
                            } else if component == "cols" {
                                2
                            } else {
                                3
                            }
                        );
                    }
                    if kind == "table" {
                        tables += 1
                    } else {
                        matrices += 1
                    }
                }
            }
        }
        assert_eq!(tables, 56);
        assert_eq!(matrices, 60);
    }
}
