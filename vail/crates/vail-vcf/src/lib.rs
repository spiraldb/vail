//! VCF text decoding directly into Arrow child buffers. No per-cell objects.
//! FORMAT plans are cached; delimiter discovery is shared across all fields and
//! field dispatch happens outside each contiguous sample loop.
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StructArray, builder::*};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader, Read},
    sync::Arc,
};
use vail_execution::{
    expressions::{PrimitiveType as P, Type},
    reference::ReferenceDefinition,
};

pub type Result<T> = std::result::Result<T, String>;
fn primitive(p: P) -> Type {
    Type::Primitive(p)
}
fn structure(fields: Vec<(String, Type)>) -> Type {
    Type::Struct { r#struct: fields }
}
fn array(t: Type) -> Type {
    Type::Array { array: Box::new(t) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldSpec {
    pub name: String,
    pub number: String,
    pub kind: String,
    pub dtype: Type,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Header {
    pub samples: Vec<String>,
    pub info: Vec<FieldSpec>,
    pub format: Vec<FieldSpec>,
    pub metadata: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Options {
    pub reference: Option<ReferenceDefinition>,
    pub contig_recoding: HashMap<String, String>,
    pub call_fields: HashSet<String>,
    pub entry_float32: bool,
    pub array_elements_required: bool,
    pub skip_invalid_loci: bool,
    pub drop_samples: bool,
    pub filter: Option<String>,
    pub find_replace: Option<(String, String)>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            reference: None,
            contig_recoding: HashMap::new(),
            call_fields: ["GT".into(), "PGT".into()].into(),
            entry_float32: false,
            array_elements_required: true,
            skip_invalid_loci: false,
            drop_samples: false,
            filter: None,
            find_replace: None,
        }
    }
}

pub fn open(path: &str) -> Result<Box<dyn BufRead + Send>> {
    let mut file = File::open(path).map_err(|e| format!("{path}: {e}"))?;
    let mut magic = [0u8; 2];
    let n = file.read(&mut magic).map_err(|e| e.to_string())?;
    let file = File::open(path).map_err(|e| e.to_string())?;
    if n == 2 && magic == [31, 139] {
        Ok(Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(
            file,
        ))))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

/// Quoted header values may contain commas, escaped quotes, and backslashes.
fn attributes(text: &str) -> Result<BTreeMap<String, String>> {
    let mut pieces = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (i, b) in text.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if b == b'\\' && quoted {
            escaped = true;
            continue;
        }
        if b == b'"' {
            quoted = !quoted;
        }
        if b == b',' && !quoted {
            pieces.push(&text[start..i]);
            start = i + 1;
        }
    }
    if quoted {
        return Err("unterminated quoted VCF header value".into());
    }
    pieces.push(&text[start..]);
    pieces
        .into_iter()
        .map(|piece| {
            let (key, value) = piece
                .split_once('=')
                .ok_or_else(|| format!("invalid VCF header attribute {piece:?}"))?;
            let value =
                if let Some(value) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
                    let mut out = String::new();
                    let mut chars = value.chars();
                    while let Some(c) = chars.next() {
                        if c == '\\' {
                            out.push(chars.next().ok_or("invalid header escape")?);
                        } else {
                            out.push(c);
                        }
                    }
                    out
                } else {
                    value.to_string()
                };
            Ok((key.to_string(), value))
        })
        .collect()
}

pub fn read_header(path: &str, options: &Options) -> Result<Header> {
    let mut reader = open(path)?;
    let mut line = String::new();
    let mut header = Header {
        samples: Vec::new(),
        info: Vec::new(),
        format: Vec::new(),
        metadata: ["info", "format", "filter"]
            .into_iter()
            .map(|s| (s.into(), BTreeMap::new()))
            .collect(),
    };
    let mut first = true;
    let mut field_names = HashSet::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
            return Err(format!("{path}: missing #CHROM header"));
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if first {
            first = false;
            if !line.starts_with("##fileformat=VCFv4.") {
                return Err(format!("{path}: invalid VCF fileformat header"));
            }
        }
        if line.starts_with("##") {
            for (prefix, group) in [
                ("##INFO=<", "info"),
                ("##FORMAT=<", "format"),
                ("##FILTER=<", "filter"),
            ] {
                if let Some(text) = line.strip_prefix(prefix) {
                    let mut values = attributes(
                        text.strip_suffix('>')
                            .ok_or("invalid VCF header, missing >")?,
                    )?;
                    let name = values.remove("ID").ok_or("VCF header field missing ID")?;
                    if group != "filter" {
                        let number = values
                            .get("Number")
                            .ok_or("VCF field missing Number")?
                            .clone();
                        let kind = values.get("Type").ok_or("VCF field missing Type")?.clone();
                        let call = group == "format"
                            && (name == "GT" || options.call_fields.contains(&name));
                        let scalar = match kind.as_str() {
                            "Integer" => primitive(P::Int32),
                            "Float" => primitive(if group == "format" && options.entry_float32 {
                                P::Float32
                            } else {
                                P::Float64
                            }),
                            "String" | "Character" => primitive(P::Str),
                            "Flag" if group == "info" => primitive(P::Bool),
                            _ => return Err(format!("unsupported VCF {group} Type={kind}")),
                        };
                        let dtype = if call {
                            primitive(P::Call)
                        } else if number == "1" || kind == "Flag" {
                            scalar
                        } else {
                            array(scalar)
                        };
                        let spec = FieldSpec {
                            name: name.clone(),
                            number,
                            kind,
                            dtype,
                        };
                        let fields = if group == "info" {
                            &mut header.info
                        } else {
                            &mut header.format
                        };
                        if !field_names.insert((group.to_owned(), name.clone())) {
                            return Err(format!("duplicate VCF {group} field {name}"));
                        }
                        fields.push(spec);
                    }
                    header.metadata.get_mut(group).unwrap().insert(name, values);
                }
            }
        } else if line.starts_with("#CHROM\t") {
            let columns: Vec<_> = line.split('\t').collect();
            if columns.len() < 8
                || columns[..8]
                    != [
                        "#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO",
                    ]
            {
                return Err("invalid VCF column header".into());
            }
            if columns.len() > 8 && columns[8] != "FORMAT" {
                return Err("invalid VCF FORMAT column header".into());
            }
            header.samples = columns.iter().skip(9).map(|s| s.to_string()).collect();
            if header.samples.iter().collect::<HashSet<_>>().len() != header.samples.len() {
                return Err("invalid sample IDs: duplicate IDs".into());
            }
            return Ok(header);
        } else {
            return Err(format!("{path}: missing #CHROM header before records"));
        }
    }
}
impl Header {
    pub fn validate_other(&self, other: &Header) -> Result<()> {
        if self.samples != other.samples {
            return Err("invalid sample IDs: VCF headers differ".into());
        }
        if self
            .info
            .iter()
            .map(|f| (&f.name, &f.dtype))
            .collect::<Vec<_>>()
            != other
                .info
                .iter()
                .map(|f| (&f.name, &f.dtype))
                .collect::<Vec<_>>()
        {
            return Err("Check that all files have same INFO fields".into());
        }
        if self
            .format
            .iter()
            .map(|f| (&f.name, &f.dtype))
            .collect::<Vec<_>>()
            != other
                .format
                .iter()
                .map(|f| (&f.name, &f.dtype))
                .collect::<Vec<_>>()
        {
            return Err("Check that all files have same FORMAT fields".into());
        }
        Ok(())
    }
    pub fn row_type(&self, options: &Options) -> Type {
        structure(vec![
            (
                "locus".into(),
                options
                    .reference
                    .as_ref()
                    .map(|r| Type::Locus { locus: r.clone() })
                    .unwrap_or_else(|| {
                        structure(vec![
                            ("contig".into(), primitive(P::Str)),
                            ("position".into(), primitive(P::Int32)),
                        ])
                    }),
            ),
            ("alleles".into(), array(primitive(P::Str))),
            ("rsid".into(), primitive(P::Str)),
            ("qual".into(), primitive(P::Float64)),
            (
                "filters".into(),
                Type::Set {
                    set: Box::new(primitive(P::Str)),
                },
            ),
            (
                "info".into(),
                structure(
                    self.info
                        .iter()
                        .map(|f| (f.name.clone(), f.dtype.clone()))
                        .collect(),
                ),
            ),
        ])
    }
    pub fn entry_type(&self) -> Type {
        structure(
            self.format
                .iter()
                .map(|f| (f.name.clone(), f.dtype.clone()))
                .collect(),
        )
    }
    pub fn core_schema(&self, options: &Options) -> Result<SchemaRef> {
        let cell = structure(vec![
            ("__vail_matrix_col_id".into(), primitive(P::Int64)),
            ("value".into(), self.entry_type()),
        ]);
        Ok(Arc::new(Schema::new(vec![
            Field::new(
                "__vail_matrix_row",
                self.row_type(options)
                    .to_arrow()
                    .map_err(|e| e.to_string())?,
                true,
            ),
            Field::new(
                "__vail_matrix_cells",
                array(cell).to_arrow().map_err(|e| e.to_string())?,
                true,
            ),
        ])))
    }
}

struct Sink {
    dtype: Type,
    builder: Box<dyn ArrayBuilder>,
    skipped: usize,
}
impl Sink {
    fn new(dtype: Type) -> Result<Self> {
        Ok(Self {
            builder: make_builder(&dtype.to_arrow().map_err(|e| e.to_string())?, 1024),
            dtype,
            skipped: 0,
        })
    }
    /// Dispatch once per FORMAT field, then parse the whole sample stripe.
    fn append_stripe<'a>(
        &mut self,
        tokens: impl Iterator<Item = &'a str> + Clone,
        required: bool,
        context: &str,
    ) -> Result<()> {
        macro_rules! primitive_loop {
            ($builder:ty, $ty:ty) => {{
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<$builder>()
                    .unwrap();
                for token in tokens {
                    if token == "." {
                        builder.append_null();
                    } else {
                        builder.append_value(parse_number::<$ty>(token)?);
                    }
                }
            }};
        }
        match &self.dtype {
            Type::Primitive(P::Int32) => primitive_loop!(Int32Builder, i32),
            Type::Primitive(P::Float32) => primitive_loop!(Float32Builder, f32),
            Type::Primitive(P::Float64) => primitive_loop!(Float64Builder, f64),
            Type::Primitive(P::Str) => {
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap();
                for token in tokens {
                    if token == "." {
                        builder.append_null();
                    } else {
                        builder.append_value(token);
                    }
                }
            }
            Type::Primitive(P::Bool) => {
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap();
                for token in tokens {
                    builder.append_value(token == "true");
                }
            }
            Type::Primitive(P::Call) => {
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .unwrap();
                for token in tokens {
                    append_call(builder, token)?;
                }
            }
            Type::Array { array } => {
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                    .unwrap();
                macro_rules! list_loop {
                    ($builder:ty, $ty:ty) => {{
                        for token in tokens {
                            if token == "." {
                                builder.append(false);
                                continue;
                            }
                            let values = builder
                                .values()
                                .as_any_mut()
                                .downcast_mut::<$builder>()
                                .unwrap();
                            for value in token.split(',') {
                                if value == "." {
                                    missing_element(required, context)?;
                                    values.append_null();
                                } else {
                                    values.append_value(parse_number::<$ty>(value)?);
                                }
                            }
                            builder.append(true);
                        }
                    }};
                }
                match array.as_ref() {
                    Type::Primitive(P::Int32) => list_loop!(Int32Builder, i32),
                    Type::Primitive(P::Float32) => list_loop!(Float32Builder, f32),
                    Type::Primitive(P::Float64) => list_loop!(Float64Builder, f64),
                    Type::Primitive(P::Str) => {
                        for token in tokens {
                            if token == "." {
                                builder.append(false);
                                continue;
                            }
                            let values = builder
                                .values()
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .unwrap();
                            for value in token.split(',') {
                                if value == "." {
                                    missing_element(required, context)?;
                                    values.append_null();
                                } else {
                                    values.append_value(value);
                                }
                            }
                            builder.append(true);
                        }
                    }
                    _ => return Err("unsupported VCF array type".into()),
                }
            }
            _ => return Err("unsupported VCF field type".into()),
        }
        Ok(())
    }
    /// Append only each sample's value-validity bit. FORMAT field paths used
    /// by missingness checks must not parse otherwise-unused scalar payloads.
    fn append_validity_stripe<'a>(&mut self, tokens: impl Iterator<Item = &'a str>) -> Result<()> {
        macro_rules! primitive_validity_loop {
            ($builder:ty, $value:expr) => {{
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<$builder>()
                    .unwrap();
                for token in tokens {
                    if token == "." {
                        builder.append_null();
                    } else {
                        builder.append_value($value);
                    }
                }
            }};
        }
        match &self.dtype {
            Type::Primitive(P::Int32) => primitive_validity_loop!(Int32Builder, 0),
            Type::Primitive(P::Float32) => primitive_validity_loop!(Float32Builder, 0.0),
            Type::Primitive(P::Float64) => primitive_validity_loop!(Float64Builder, 0.0),
            Type::Primitive(P::Str) => {
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap();
                for token in tokens {
                    if token == "." {
                        builder.append_null();
                    } else {
                        builder.append_value("");
                    }
                }
            }
            Type::Primitive(P::Bool) => {
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap();
                for token in tokens {
                    if token == "." {
                        builder.append_null();
                    } else {
                        builder.append_value(false);
                    }
                }
            }
            Type::Primitive(P::Call) => {
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .unwrap();
                for token in tokens {
                    if token.split(['/', '|']).any(|allele| allele == ".") {
                        append_call(builder, ".")?;
                    } else {
                        append_call(builder, "0")?;
                    }
                }
            }
            Type::Array { .. } => {
                let builder = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                    .unwrap();
                for token in tokens {
                    builder.append(token != ".");
                }
            }
            _ => return Err("unsupported VCF field type".into()),
        }
        Ok(())
    }
    /// Preserve list shape and child validity while leaving non-null payloads
    /// as placeholders. This lets array callbacks inspect cardinality or child
    /// missingness without parsing unused numeric values.
    fn append_shape_stripe<'a>(&mut self, tokens: impl Iterator<Item = &'a str>) -> Result<()> {
        let Type::Array { array } = &self.dtype else {
            return self.append_validity_stripe(tokens);
        };
        let builder = self
            .builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .unwrap();
        macro_rules! shape_loop {
            ($builder:ty, $placeholder:expr) => {{
                for token in tokens {
                    if token == "." {
                        builder.append(false);
                        continue;
                    }
                    let values = builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<$builder>()
                        .unwrap();
                    for value in token.split(',') {
                        if value == "." {
                            values.append_null();
                        } else {
                            values.append_value($placeholder);
                        }
                    }
                    builder.append(true);
                }
            }};
        }
        match array.as_ref() {
            Type::Primitive(P::Int32) => shape_loop!(Int32Builder, 0),
            Type::Primitive(P::Float32) => shape_loop!(Float32Builder, 0.0),
            Type::Primitive(P::Float64) => shape_loop!(Float64Builder, 0.0),
            Type::Primitive(P::Str) => shape_loop!(StringBuilder, ""),
            _ => return Err("unsupported VCF array element type".into()),
        }
        Ok(())
    }
    fn skip(&mut self, n: usize) {
        self.skipped += n;
    }
    fn finish(&mut self) -> ArrayRef {
        if self.skipped > 0 {
            let n = std::mem::take(&mut self.skipped);
            arrow_array::new_null_array(&self.dtype.to_arrow().expect("validated sink type"), n)
        } else {
            self.builder.finish()
        }
    }
}
fn missing_element(required: bool, context: &str) -> Result<()> {
    if required {
        Err(format!(
            "Missing value in {context} array. Use 'hl.import_vcf(..., array_elements_required=False)'."
        ))
    } else {
        Ok(())
    }
}
fn parse_number<T: std::str::FromStr>(value: &str) -> Result<T> {
    value
        .parse()
        .map_err(|_| format!("invalid character in numeric literal {value:?}"))
}
fn append_call(builder: &mut StructBuilder, token: &str) -> Result<()> {
    let mut missing = false;
    let mut alleles = [0i32; 2];
    let mut ploidy = 0;
    for value in token.split(['/', '|']) {
        if ploidy == 2 {
            return Err("VCF calls with ploidy > 2 are not supported".into());
        }
        if value == "." {
            missing = true;
        } else {
            let allele = parse_number::<i32>(value)?;
            if allele < 0 {
                return Err("negative genotype allele".into());
            }
            let allele = u16::try_from(allele)
                .map_err(|_| "Call allele index must be between 0 and 65535")?;
            alleles[ploidy] = i32::from(allele);
        }
        ploidy += 1;
    }
    if token.contains('/') && token.contains('|') {
        return Err("invalid mixed genotype separators".into());
    }
    if !token.contains('|') && ploidy == 2 && alleles[0] > alleles[1] {
        alleles.swap(0, 1);
    }
    let call = vail_execution::calls::Call::new(
        if missing { &[] } else { &alleles[..ploidy] },
        token.contains('|'),
    )?;
    builder
        .field_builder::<UInt16Builder>(0)
        .unwrap()
        .append_value(call.first());
    builder
        .field_builder::<UInt16Builder>(1)
        .unwrap()
        .append_value(call.second());
    builder
        .field_builder::<UInt8Builder>(2)
        .unwrap()
        .append_value(call.ploidy());
    builder
        .field_builder::<BooleanBuilder>(3)
        .unwrap()
        .append_value(call.phased());
    builder.append(!missing);
    Ok(())
}

#[derive(Clone, Debug)]
pub struct FormatPlan {
    pub width: usize,
    pub fields: Vec<Option<usize>>,
}
impl FormatPlan {
    pub fn new(format: &str, header: &Header) -> Result<Self> {
        let keys: Vec<_> = if format == "." {
            Vec::new()
        } else {
            format.split(':').collect()
        };
        if keys.iter().collect::<HashSet<_>>().len() != keys.len() {
            return Err("duplicate FORMAT field".into());
        }
        let positions: HashMap<_, _> = keys.iter().enumerate().map(|(i, k)| (*k, i)).collect();
        Ok(Self {
            width: keys.len(),
            fields: header
                .format
                .iter()
                .map(|f| positions.get(f.name.as_str()).copied())
                .collect(),
        })
    }
}

/// A value-field path, or an independent request for its shape/nullness.
/// Shape is separate from field names, so every legal user name is representable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecodePath {
    pub fields: Vec<String>,
    pub shape: bool,
    #[serde(default)]
    pub validity: bool,
}
struct DecodeProjection {
    rsid: bool,
    qual: bool,
    filters: bool,
    info: Vec<bool>,
    format: Vec<bool>,
    format_validity: Vec<bool>,
    format_shape: Vec<bool>,
    samples: bool,
}
impl DecodeProjection {
    fn all(header: &Header) -> Self {
        Self {
            rsid: true,
            qual: true,
            filters: true,
            info: vec![true; header.info.len()],
            format: vec![true; header.format.len()],
            format_validity: vec![false; header.format.len()],
            format_shape: vec![false; header.format.len()],
            samples: true,
        }
    }
    fn from_paths(header: &Header, paths: &[DecodePath]) -> Self {
        let (mut row_all, mut info_all, mut format_all, mut samples) = (false, false, false, false);
        let (mut fixed, mut info, mut format, mut format_validity, mut format_shape) = (
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
        );
        for requested in paths {
            let path = &requested.fields;
            match path.first().map(String::as_str) {
                Some("__vail_matrix_row") => {
                    if path.len() == 1 {
                        row_all |= !requested.shape;
                    } else {
                        fixed.insert(path[1].as_str());
                        if path[1] == "info" {
                            if path.len() == 2 {
                                info_all |= !requested.shape;
                            } else {
                                info.insert(path[2].as_str());
                            }
                        }
                    }
                }
                Some("__vail_matrix_cells") => {
                    samples = true;
                    if !requested.shape
                        && !requested.validity
                        && (path.len() == 1
                            || (path.get(1).is_some_and(|v| v == "value") && path.len() == 2))
                    {
                        format_all = true;
                    } else if path.get(1).is_some_and(|v| v == "value") {
                        if let Some(name) = path.get(2) {
                            if requested.shape {
                                format_shape.insert(name.as_str());
                            } else if requested.validity {
                                format_validity.insert(name.as_str());
                            } else {
                                format.insert(name.as_str());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Self {
            rsid: row_all || fixed.contains("rsid"),
            qual: row_all || fixed.contains("qual"),
            filters: row_all || fixed.contains("filters"),
            info: header
                .info
                .iter()
                .map(|f| row_all || info_all || info.contains(f.name.as_str()))
                .collect(),
            format: header
                .format
                .iter()
                .map(|f| format_all || format.contains(f.name.as_str()))
                .collect(),
            format_validity: header
                .format
                .iter()
                .map(|f| {
                    !format_all
                        && !format.contains(f.name.as_str())
                        && format_validity.contains(f.name.as_str())
                })
                .collect(),
            format_shape: header
                .format
                .iter()
                .map(|f| {
                    !format_all
                        && !format.contains(f.name.as_str())
                        && format_shape.contains(f.name.as_str())
                })
                .collect(),
            samples,
        }
    }
}
pub struct Parser {
    header: Header,
    options: Options,
    schema: SchemaRef,
    plans: HashMap<String, Arc<FormatPlan>>,
    last_format: String,
    last_plan: Option<Arc<FormatPlan>>,
    filter: Option<regex::Regex>,
    replace: Option<(regex::Regex, String)>,
    ordinal: HashMap<String, i32>,
    tokens: Vec<(usize, usize)>,
    projection: DecodeProjection,
}
impl Parser {
    /// COUNT needs only record validity and locus inclusion, not INFO/FORMAT.
    pub fn count_record(&self, original: &str) -> Result<bool> {
        if original.is_empty()
            || original.starts_with('#')
            || self.filter.as_ref().is_some_and(|r| r.is_match(original))
        {
            return Ok(false);
        }
        let replaced;
        let line = if let Some((pattern, replacement)) = &self.replace {
            replaced = pattern.replace_all(original, replacement);
            replaced.as_ref()
        } else {
            original
        };
        let fields: Vec<_> = line.splitn(9, '\t').collect();
        if fields.len() < 8 {
            return Err("VCF record has fewer than 8 columns".into());
        }
        let contig = self
            .options
            .contig_recoding
            .get(fields[0])
            .map(String::as_str)
            .unwrap_or(fields[0]);
        let position = parse_number::<i32>(fields[1])?;
        if let Some(reference) = &self.options.reference {
            if position <= 0 || position > *reference.lengths.get(contig).unwrap_or(&0) {
                if self.options.skip_invalid_loci {
                    return Ok(false);
                }
                return Err(format!(
                    "Invalid locus {contig}:{position} for reference {}",
                    reference.name
                ));
            }
        }
        Ok(!fields[3].is_empty()
            && fields[3]
                .bytes()
                .all(|b| matches!(b.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T' | b'N')))
    }
    pub fn new(header: Header, options: Options) -> Result<Self> {
        let schema = header.core_schema(&options)?;
        let filter = options
            .filter
            .as_ref()
            .map(|s| regex::Regex::new(s).map_err(|e| e.to_string()))
            .transpose()?;
        let replace = options
            .find_replace
            .as_ref()
            .map(|(pattern, replacement)| {
                Ok::<_, String>((
                    regex::Regex::new(pattern).map_err(|e| e.to_string())?,
                    replacement.clone(),
                ))
            })
            .transpose()?;
        let ordinal = options
            .reference
            .as_ref()
            .map(|r| {
                r.contigs
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.clone(), i as i32))
                    .collect()
            })
            .unwrap_or_default();
        let projection = DecodeProjection::all(&header);
        Ok(Self {
            header,
            options,
            schema,
            plans: HashMap::new(),
            last_format: String::new(),
            last_plan: None,
            filter,
            replace,
            ordinal,
            tokens: Vec::new(),
            projection,
        })
    }
    /// Decoding keeps the declared schema; unrequested values are typed nulls.
    /// Paths start at `__vail_matrix_row` or `__vail_matrix_cells`; nested
    /// `info` and `value` paths name requested INFO/FORMAT fields. The
    /// independent `shape` flag needs array cardinality/nullness only. Configure
    /// projection before creating batches: every Batch belongs to this Parser
    /// configuration and must only be appended/finished with that configuration.
    pub fn with_projection(mut self, paths: Vec<DecodePath>) -> Self {
        self.projection = DecodeProjection::from_paths(&self.header, &paths);
        self
    }
    pub fn reads_samples(&self) -> bool {
        !self.options.drop_samples && self.projection.samples
    }
    pub fn batch(&self) -> Result<Batch> {
        Batch::new(self)
    }
    pub fn append(&mut self, batch: &mut Batch, original: &str) -> Result<bool> {
        if original.is_empty() || original.starts_with('#') {
            return Ok(false);
        }
        if self.filter.as_ref().is_some_and(|r| r.is_match(original)) {
            return Ok(false);
        }
        let replaced;
        let line = if let Some((pattern, replacement)) = &self.replace {
            replaced = pattern.replace_all(original, replacement);
            replaced.as_ref()
        } else {
            original
        };
        let mut fields = line.splitn(10, '\t');
        let mut fixed = Vec::with_capacity(9);
        for _ in 0..9 {
            if let Some(value) = fields.next() {
                fixed.push(value);
            } else {
                break;
            }
        }
        if fixed.len() < 8 {
            return Err("VCF record has fewer than 8 columns".into());
        }
        let contig = self
            .options
            .contig_recoding
            .get(fixed[0])
            .map(String::as_str)
            .unwrap_or(fixed[0]);
        let position = parse_number::<i32>(fixed[1])?;
        let ordinal = self.ordinal.get(contig).copied();
        if let Some(reference) = &self.options.reference {
            let valid = ordinal.is_some()
                && position > 0
                && position <= *reference.lengths.get(contig).unwrap_or(&0);
            if !valid {
                if self.options.skip_invalid_loci {
                    return Ok(false);
                }
                return Err(format!(
                    "Invalid locus {contig}:{position} for reference {}",
                    reference.name
                ));
            }
        }
        // Hail excludes records whose reference allele is not a sequence of bases.
        if fixed[3].is_empty()
            || !fixed[3]
                .bytes()
                .all(|b| matches!(b.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T' | b'N'))
        {
            return Ok(false);
        }
        batch.contigs.append_value(contig);
        batch.positions.append_value(position);
        if self.options.reference.is_some() {
            batch.ordinals.append_value(ordinal.unwrap());
        }
        batch.alleles.values().append_value(fixed[3]);
        if fixed[4] != "." {
            for allele in fixed[4].split(',') {
                batch.alleles.values().append_value(allele);
            }
        }
        batch.alleles.append(true);
        if !self.projection.rsid || fixed[2] == "." {
            batch.rsids.append_null();
        } else {
            batch.rsids.append_value(fixed[2]);
        }
        if !self.projection.qual || fixed[5] == "." {
            batch.quals.append_null();
        } else {
            batch.quals.append_value(parse_number::<f64>(fixed[5])?);
        }
        if !self.projection.filters || fixed[6] == "." {
            batch.filters.append(false);
        } else {
            if fixed[6] != "PASS" {
                let mut seen = HashSet::new();
                for filter in fixed[6].split(';') {
                    if seen.insert(filter) {
                        batch.filters.values().append_value(filter);
                    }
                }
            }
            batch.filters.append(true);
        }
        let info_text = if self.projection.info.iter().any(|&v| v) {
            fixed[7]
        } else {
            "."
        };
        let info: HashMap<_, _> = info_text
            .split(';')
            .filter(|s| *s != ".")
            .map(|item| {
                item.split_once('=')
                    .map(|(k, v)| (k, Some(v)))
                    .unwrap_or((item, None))
            })
            .collect();
        for ((spec, sink), &wanted) in self
            .header
            .info
            .iter()
            .zip(&mut batch.info)
            .zip(&self.projection.info)
        {
            if !wanted {
                sink.skip(1);
                continue;
            }
            let token = if spec.kind == "Flag" {
                if info
                    .get(spec.name.as_str())
                    .is_some_and(|value| *value != Some("0"))
                {
                    "true"
                } else {
                    "false"
                }
            } else {
                info.get(spec.name.as_str())
                    .copied()
                    .flatten()
                    .unwrap_or(".")
            };
            sink.append_stripe(
                std::iter::once(token),
                self.options.array_elements_required,
                "INFO",
            )?;
        }
        if self.reads_samples() {
            let samples = self.header.samples.len();
            if samples > 0 {
                if fixed.len() != 9 {
                    return Err("missing FORMAT column".into());
                }
                let format = fixed[8];
                if self.last_plan.is_none() || self.last_format != format {
                    let plan = if let Some(plan) = self.plans.get(format) {
                        plan.clone()
                    } else {
                        let plan = Arc::new(FormatPlan::new(format, &self.header)?);
                        // Bound retained layouts even for adversarial files. The
                        // current plan is independently shared, so eviction is safe.
                        if self.plans.len() >= 128 {
                            self.plans.clear();
                        }
                        self.plans.insert(format.to_string(), plan.clone());
                        plan
                    };
                    self.last_format.clear();
                    self.last_format.push_str(format);
                    self.last_plan = Some(plan);
                }
                let plan = self.last_plan.as_ref().unwrap();
                let tail = fields.next().ok_or("missing sample columns")?;
                self.tokens.clear();
                self.tokens.resize(samples * plan.width, (0, 0));
                let mut count = 0;
                let mut sample_start = 0;
                let mut excess_samples = false;
                for sample_end in
                    memchr::memchr_iter(b'\t', tail.as_bytes()).chain(std::iter::once(tail.len()))
                {
                    if count >= samples {
                        excess_samples = true;
                        break;
                    }
                    let sample = &tail[sample_start..sample_end];
                    let mut field = 0;
                    let mut start = sample_start;
                    for end in memchr::memchr_iter(b':', sample.as_bytes())
                        .map(|i| i + sample_start)
                        .chain(std::iter::once(sample_end))
                    {
                        if field >= plan.width {
                            if plan.width == 0 && sample == "." {
                                break;
                            }
                            return Err("too many FORMAT fields in sample".into());
                        }
                        if end == start {
                            return Err("empty FORMAT value".into());
                        }
                        self.tokens[count * plan.width + field] = (start, end);
                        field += 1;
                        start = end + 1;
                    }
                    count += 1;
                    sample_start = sample_end + 1;
                }
                if count != samples {
                    return Err(format!("expected {samples} samples, found {count}"));
                }
                for ((((sink, position), &wanted), &validity_only), &shape_only) in batch
                    .entries
                    .iter_mut()
                    .zip(&plan.fields)
                    .zip(&self.projection.format)
                    .zip(&self.projection.format_validity)
                    .zip(&self.projection.format_shape)
                {
                    if !wanted && !validity_only && !shape_only {
                        sink.skip(samples);
                        continue;
                    }
                    let tokens = (0..samples).map(|sample| {
                        let Some(field) = position else {
                            return ".";
                        };
                        let (start, end) = self.tokens[sample * plan.width + field];
                        if start == end { "." } else { &tail[start..end] }
                    });
                    if wanted {
                        sink.append_stripe(tokens, self.options.array_elements_required, "FORMAT")?;
                    } else if shape_only {
                        sink.append_shape_stripe(tokens)?;
                    } else {
                        sink.append_validity_stripe(tokens)?;
                    }
                }
                if excess_samples {
                    return Err("too many sample columns".into());
                }
            } else if fields.next().is_some() {
                return Err("sample data without sample header".into());
            }
        }
        batch.rows += 1;
        Ok(true)
    }
    pub fn header(&self) -> &Header {
        &self.header
    }
    pub fn options(&self) -> &Options {
        &self.options
    }
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

pub struct Batch {
    pub rows: usize,
    schema: SchemaRef,
    samples: usize,
    ordinals: Int32Builder,
    contigs: StringBuilder,
    positions: Int32Builder,
    alleles: ListBuilder<StringBuilder>,
    rsids: StringBuilder,
    quals: Float64Builder,
    filters: ListBuilder<StringBuilder>,
    info: Vec<Sink>,
    entries: Vec<Sink>,
}
impl Batch {
    fn new(parser: &Parser) -> Result<Self> {
        Ok(Self {
            rows: 0,
            schema: parser.schema.clone(),
            samples: if !parser.reads_samples() {
                0
            } else {
                parser.header.samples.len()
            },
            ordinals: Int32Builder::new(),
            contigs: StringBuilder::new(),
            positions: Int32Builder::new(),
            alleles: ListBuilder::new(StringBuilder::new()),
            rsids: StringBuilder::new(),
            quals: Float64Builder::new(),
            filters: ListBuilder::new(StringBuilder::new()),
            info: parser
                .header
                .info
                .iter()
                .map(|f| Sink::new(f.dtype.clone()))
                .collect::<Result<_>>()?,
            entries: parser
                .header
                .format
                .iter()
                .map(|f| Sink::new(f.dtype.clone()))
                .collect::<Result<_>>()?,
        })
    }
    pub fn finish(&mut self) -> Result<RecordBatch> {
        let DataType::Struct(row_fields) = self.schema.field(0).data_type() else {
            unreachable!()
        };
        let DataType::Struct(locus_fields) = row_fields[0].data_type() else {
            unreachable!()
        };
        let mut locus: Vec<ArrayRef> = Vec::new();
        if locus_fields.len() == 3 {
            locus.push(Arc::new(self.ordinals.finish()));
        }
        locus.push(Arc::new(self.contigs.finish()));
        locus.push(Arc::new(self.positions.finish()));
        let locus: ArrayRef = Arc::new(
            StructArray::try_new(locus_fields.clone(), locus, None).map_err(|e| e.to_string())?,
        );
        let DataType::Struct(info_fields) = row_fields[5].data_type() else {
            unreachable!()
        };
        let info = struct_array(
            info_fields.clone(),
            self.info.iter_mut().map(Sink::finish).collect(),
            self.rows,
        )?;
        let filters = self.filters.finish();
        let DataType::List(filter_field) = row_fields[4].data_type() else {
            unreachable!()
        };
        let filters: ArrayRef = Arc::new(
            ListArray::try_new(
                filter_field.clone(),
                filters.offsets().clone(),
                filters.values().clone(),
                filters.nulls().cloned(),
            )
            .map_err(|e| e.to_string())?,
        );
        let row: ArrayRef = Arc::new(
            StructArray::try_new(
                row_fields.clone(),
                vec![
                    locus,
                    Arc::new(self.alleles.finish()),
                    Arc::new(self.rsids.finish()),
                    Arc::new(self.quals.finish()),
                    filters,
                    info,
                ],
                None,
            )
            .map_err(|e| e.to_string())?,
        );
        let DataType::List(cell_field) = self.schema.field(1).data_type() else {
            unreachable!()
        };
        let DataType::Struct(cell_fields) = cell_field.data_type() else {
            unreachable!()
        };
        let DataType::Struct(entry_fields) = cell_fields[1].data_type() else {
            unreachable!()
        };
        let n = self.rows * self.samples;
        let entries = struct_array(
            entry_fields.clone(),
            self.entries.iter_mut().map(Sink::finish).collect(),
            n,
        )?;
        let cells = struct_array(
            cell_fields.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..n).map(|i| (i % self.samples) as i64),
                )),
                entries,
            ],
            n,
        )?;
        let offsets = OffsetBuffer::new(ScalarBuffer::from(
            (0..=self.rows)
                .map(|i| {
                    i32::try_from(i * self.samples)
                        .map_err(|_| "VCF batch exceeds Arrow list offset capacity".to_string())
                })
                .collect::<Result<Vec<_>>>()?,
        ));
        let cells: ArrayRef = Arc::new(
            ListArray::try_new(cell_field.clone(), offsets, cells, None)
                .map_err(|e| e.to_string())?,
        );
        self.rows = 0;
        RecordBatch::try_new(self.schema.clone(), vec![row, cells]).map_err(|e| e.to_string())
    }
}
fn struct_array(
    fields: arrow_schema::Fields,
    arrays: Vec<ArrayRef>,
    len: usize,
) -> Result<ArrayRef> {
    Ok(Arc::new(
        StructArray::try_new_with_length(fields, arrays, None, len).map_err(|e| e.to_string())?,
    ))
}
