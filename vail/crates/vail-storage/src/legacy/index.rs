//! Hail's independent B-tree indexes, including directory metadata and newer
//! inline footers. Node codecs are the same native codecs as data records.
use super::*;
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, StructArray};
use std::io::{Read, Seek, SeekFrom};
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Codec {
    ty: Type,
    encoding: Encoding,
    buffer: Value,
}
impl Codec {
    fn parse(value: &Value, refs: &HashMap<String, ReferenceDefinition>) -> Result<Self> {
        if value["name"] != "TypedCodecSpec" {
            return Err("unsupported Hail index codec".into());
        }
        let mut p = Parser::new(
            value["_vType"]
                .as_str()
                .ok_or("missing index virtual type")?,
            refs,
        );
        let (ty, _) = p.virtual_type()?;
        p.finish()?;
        let mut p = Parser::new(
            value["_eType"].as_str().ok_or("missing index encoding")?,
            refs,
        );
        let encoding = p.encoded()?;
        p.finish()?;
        decode::check(&encoding, &ty)?;
        input::validate(&value["_bufferSpec"])?;
        Ok(Self {
            ty,
            encoding,
            buffer: value["_bufferSpec"].clone(),
        })
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexSpec {
    root: PathBuf,
    leaf: Codec,
    internal: Codec,
    offset_field: Option<String>,
}
impl IndexSpec {
    pub fn from_rvd(
        path: &Path,
        rvd: &Value,
        refs: &HashMap<String, ReferenceDefinition>,
    ) -> Result<Option<Self>> {
        let Some(spec) = rvd.get("_indexSpec").filter(|s| s["name"] == "IndexSpec2") else {
            return Ok(None);
        };
        Ok(Some(Self {
            root: path.join(spec["_relPath"].as_str().ok_or("missing Hail index path")?),
            leaf: Codec::parse(&spec["_leafCodec"], refs)?,
            internal: Codec::parse(&spec["_internalNodeCodec"], refs)?,
            offset_field: spec
                .get("_offsetField")
                .and_then(Value::as_str)
                .map(str::to_owned),
        }))
    }
}
// Construction proves defined, nonnegative, sorted internal child ordinals.
struct SortedOrdinals(Vec<u64>);
impl SortedOrdinals {
    fn new(array: &ArrayRef) -> Result<Self> {
        if array
            .logical_nulls()
            .is_some_and(|nulls| nulls.null_count() != 0)
        {
            return Err("missing Hail index integer".into());
        }
        let values = if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            values
                .values()
                .iter()
                .map(|&value| Ok(u64::try_from(value)?))
                .collect::<Result<Vec<_>>>()?
        } else if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
            values
                .values()
                .iter()
                .map(|&value| Ok(u64::try_from(value)?))
                .collect::<Result<Vec<_>>>()?
        } else {
            return Err("invalid Hail index integer type".into());
        };
        if values.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err("unsorted Hail internal ordinals".into());
        }
        Ok(Self(values))
    }
    fn child(&self, index: u64) -> Result<usize> {
        self.0
            .partition_point(|&first| first <= index)
            .checked_sub(1)
            .ok_or_else(|| "Hail internal node does not cover row".into())
    }
}
#[derive(Clone)]
struct Node {
    array: Arc<StructArray>,
    leaf: bool,
    first: u64,
    ordinals: Option<Arc<SortedOrdinals>>,
}
pub struct IndexReader {
    spec: Arc<IndexSpec>,
    path: PathBuf,
    root: u64,
    height: u64,
    pub rows: u64,
    cache: std::collections::VecDeque<(u64, Node)>,
}
fn structure(array: &ArrayRef) -> Result<&StructArray> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| "invalid Hail index struct".into())
}
fn field<'a>(array: &'a StructArray, name: &str) -> Result<&'a ArrayRef> {
    array
        .column_by_name(name)
        .ok_or_else(|| format!("missing Hail index field {name}").into())
}
fn number(array: &ArrayRef, row: usize) -> Result<i64> {
    if array
        .logical_nulls()
        .is_some_and(|nulls| !nulls.is_valid(row))
    {
        return Err("missing Hail index integer".into());
    }
    number_defined(array, row)
}
// Caller has already checked the prepared logical mask for this position.
fn number_defined(array: &ArrayRef, row: usize) -> Result<i64> {
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        Ok(a.value(row))
    } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        Ok(a.value(row) as i64)
    } else {
        Err("invalid Hail index integer type".into())
    }
}
fn unsigned(array: &ArrayRef, row: usize) -> Result<u64> {
    Ok(u64::try_from(number(array, row)?)?)
}
impl IndexReader {
    pub fn open(spec: Arc<IndexSpec>, part: &Path) -> Result<Self> {
        let index = spec.root.join(format!(
            "{}.idx",
            part.file_name()
                .ok_or("missing Hail part name")?
                .to_string_lossy()
        ));
        let (path, metadata) = if index.is_dir() {
            let md = super::metadata(&index)?;
            let file = index.join(
                md["indexPath"]
                    .as_str()
                    .ok_or("missing legacy index filename")?,
            );
            (file, md)
        } else {
            let mut file = std::fs::File::open(&index)?;
            let len = file.metadata()?.len();
            if len < 8 {
                return Err("truncated Hail index footer".into());
            }
            file.seek(SeekFrom::End(-8))?;
            let mut b = [0; 8];
            file.read_exact(&mut b)?;
            let offset = u64::try_from(i64::from_le_bytes(b))?;
            if offset > len - 8 {
                return Err("invalid Hail index footer offset".into());
            }
            if len - 8 - offset > 16 * 1024 * 1024 {
                return Err("Hail index metadata exceeds 16 MiB".into());
            }
            file.seek(SeekFrom::Start(offset))?;
            let metadata = serde_json::from_reader(file.take(len - 8 - offset))?;
            (index, metadata)
        };
        let root = metadata["rootOffset"]
            .as_u64()
            .ok_or("missing Hail index root offset")?;
        let height = metadata["height"]
            .as_u64()
            .filter(|n| *n > 0 && *n < 64)
            .ok_or("invalid Hail index height")?;
        let rows = metadata["nKeys"]
            .as_u64()
            .ok_or("invalid Hail index row count")?;
        Ok(Self {
            spec,
            path,
            root,
            height,
            rows,
            cache: Default::default(),
        })
    }
    fn node(&mut self, offset: u64, leaf: bool) -> Result<Node> {
        if let Some((_, node)) = self.cache.iter().find(|(p, _)| *p == offset) {
            if node.leaf != leaf {
                return Err("inconsistent Hail index node kind".into());
            }
            return Ok(node.clone());
        }
        let codec = if leaf {
            &self.spec.leaf
        } else {
            &self.spec.internal
        };
        let mut input = input::Input::open_raw(&self.path, &codec.buffer, offset)?;
        if input.byte()? != if leaf { 0 } else { 1 } {
            return Err("invalid Hail index node marker".into());
        }
        let array = decode::record(&mut input, &codec.encoding, &codec.ty)?;
        let record = structure(&array)?;
        let list = field(record, if leaf { "keys" } else { "children" })?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("invalid Hail index child array")?;
        if list.len() != 1 || list.logical_nulls().is_some_and(|nulls| !nulls.is_valid(0)) {
            return Err("invalid Hail index child array".into());
        }
        let children = list.value(0);
        let array = Arc::new(structure(&children)?.clone());
        // Keep leaf first_idx alongside its child records for ordinal queries.
        let ordinals = if leaf {
            None
        } else {
            Some(Arc::new(SortedOrdinals::new(field(&array, "first_idx")?)?))
        };
        let node = Node {
            array,
            ordinals,
            leaf,
            first: if leaf {
                unsigned(field(record, "first_idx")?, 0)?
            } else {
                0
            },
        };
        if self.cache.len() == 8 {
            self.cache.pop_front();
        }
        self.cache.push_back((offset, node.clone()));

        Ok(node)
    }
    pub fn row_offset(&mut self, index: u64) -> Result<u64> {
        if index >= self.rows {
            return Err("Hail index row is out of bounds".into());
        }
        let mut offset = self.root;
        for level in (0..self.height).rev() {
            let node = self.node(offset, level == 0)?;
            if level == 0 {
                let first = node.first;
                let row = usize::try_from(
                    index
                        .checked_sub(first)
                        .ok_or("invalid Hail leaf ordinal")?,
                )?;
                if row >= node.array.len() {
                    return Err("Hail leaf ordinal exceeds node size".into());
                }
                return self.offset(&node.array, row);
            }
            let i = node
                .ordinals
                .as_ref()
                .ok_or("missing Hail internal ordinals")?
                .child(index)?;
            offset = unsigned(field(&node.array, "index_file_offset")?, i)?;
        }
        Err("invalid Hail index traversal".into())
    }
    fn offset(&self, array: &StructArray, row: usize) -> Result<u64> {
        if let Some(name) = &self.spec.offset_field {
            unsigned(field(structure(field(array, "annotation")?)?, name)?, row)
        } else {
            unsigned(field(array, "offset")?, row)
        }
    }
    fn int_bound(&mut self, key: &str, value: i64, upper: bool) -> Result<u64> {
        if self.rows == 0 {
            return Ok(0);
        }
        let mut offset = self.root;
        for level in (0..self.height).rev() {
            let leaf = level == 0;
            let node = self.node(offset, leaf)?;
            let keys = field(&node.array, if leaf { "key" } else { "first_key" })?;
            let key_struct = structure(keys)?;
            let values = field(key_struct, key)?;
            let key_nulls = keys.logical_nulls();
            let value_nulls = values.logical_nulls();
            let before = |i: usize| -> Result<bool> {
                if key_nulls.as_ref().is_some_and(|nulls| !nulls.is_valid(i))
                    || value_nulls.as_ref().is_some_and(|nulls| !nulls.is_valid(i))
                {
                    return Ok(false);
                }
                let actual = number_defined(values, i)?;
                Ok(if upper {
                    actual <= value
                } else {
                    actual < value
                })
            };
            let mut lo = 0;
            let mut hi = node.array.len();
            while lo < hi {
                let mid = (lo + hi) / 2;
                if before(mid)? { lo = mid + 1 } else { hi = mid }
            }
            if leaf {
                return Ok(node.first + lo as u64);
            }
            let i = lo.saturating_sub(1);
            if node.array.is_empty() {
                return Err("empty Hail internal index node".into());
            }
            offset = unsigned(field(&node.array, "index_file_offset")?, i)?;
        }
        Err("invalid Hail key index traversal".into())
    }
    pub fn integer_rows(&mut self, bounds: &IntKeyBounds) -> Result<std::ops::Range<u64>> {
        let start = self.int_bound(&bounds.key, bounds.min, false)?;
        let end = self.int_bound(&bounds.key, bounds.max, true)?;
        if start > end || end > self.rows {
            return Err("invalid Hail indexed row bounds".into());
        }
        Ok(start..end)
    }
}
