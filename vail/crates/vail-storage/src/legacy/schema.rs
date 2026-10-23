use super::*;
use vail_execution::expressions::{PrimitiveType as P, Type};
use vail_execution::reference::ReferenceDefinition;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Encoding {
    pub kind: String,
    pub required: bool,
    pub fields: Vec<(String, Encoding)>,
    pub rank: usize,
}
impl Encoding {
    fn new(kind: &str, required: bool) -> Self {
        Self {
            kind: kind.into(),
            required,
            fields: vec![],
            rank: 0,
        }
    }
}
pub struct Parser<'a> {
    text: &'a str,
    pos: usize,
    refs: &'a HashMap<String, ReferenceDefinition>,
}
impl<'a> Parser<'a> {
    pub fn new(text: &'a str, refs: &'a HashMap<String, ReferenceDefinition>) -> Self {
        Self { text, pos: 0, refs }
    }
    fn space(&mut self) {
        while self
            .text
            .as_bytes()
            .get(self.pos)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.pos += 1;
        }
    }
    fn accept(&mut self, c: char) -> bool {
        self.space();
        if self.text[self.pos..].starts_with(c) {
            self.pos += c.len_utf8();
            true
        } else {
            false
        }
    }
    fn expect(&mut self, c: char) -> Result<()> {
        if self.accept(c) {
            Ok(())
        } else {
            Err(format!("expected {c:?} at {} in Hail type", self.pos).into())
        }
    }
    fn ident(&mut self) -> Result<String> {
        self.space();
        if self.accept('`') {
            let mut out = String::new();
            loop {
                let c = self.text[self.pos..]
                    .chars()
                    .next()
                    .ok_or("unterminated Hail identifier")?;
                self.pos += c.len_utf8();
                if c == '`' {
                    break;
                }
                if c == '\\' {
                    let escaped = self.text[self.pos..]
                        .chars()
                        .next()
                        .ok_or("unterminated Hail escape")?;
                    self.pos += escaped.len_utf8();
                    match escaped {
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'b' => out.push('\u{0008}'),
                        'f' => out.push('\u{000c}'),
                        'u' | 'U' | 'x' => {
                            let n = if escaped == 'u' {
                                4
                            } else if escaped == 'U' {
                                8
                            } else {
                                2
                            };
                            let text = self
                                .text
                                .get(self.pos..self.pos + n)
                                .ok_or("truncated Hail identifier escape")?;
                            let code = u32::from_str_radix(text, 16)?;
                            self.pos += n;
                            out.push(
                                char::from_u32(code)
                                    .ok_or("invalid Hail identifier Unicode escape")?,
                            );
                        }
                        other => out.push(other),
                    }
                } else {
                    out.push(c)
                }
            }
            return Ok(out);
        }
        let start = self.pos;
        while let Some(c) = self.text[self.pos..].chars().next() {
            if !c.is_alphanumeric() && c != '_' {
                break;
            }
            self.pos += c.len_utf8();
        }
        if start == self.pos {
            return Err(format!("expected Hail identifier at {}", self.pos).into());
        }
        Ok(self.text[start..self.pos].into())
    }
    fn fields<T>(
        &mut self,
        mut parse: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Vec<(String, T)>> {
        self.expect('{')?;
        let mut fields = vec![];
        if !self.accept('}') {
            loop {
                let name = self.ident()?;
                self.expect(':')?;
                if fields.iter().any(|(n, _)| n == &name) {
                    return Err("duplicate Hail field".into());
                }
                fields.push((name, parse(self)?));
                if !self.accept(',') {
                    self.expect('}')?;
                    break;
                }
            }
        }
        Ok(fields)
    }
    pub fn encoded(&mut self) -> Result<Encoding> {
        let req = self.accept('+');
        let kind = self.ident()?;
        let mut e = Encoding::new(&kind, req);
        match kind.as_str() {
            "EBaseStruct" => e.fields = self.fields(Self::encoded)?,
            "EArray" | "EArray2" | "EArrayLegacyFullWidthIntegerLength" => {
                self.expect('[')?;
                e.fields.push(("item".into(), self.encoded()?));
                self.expect(']')?;
            }
            "ENDArrayColumnMajor" => {
                self.expect('[')?;
                e.fields.push(("item".into(), self.encoded()?));
                self.expect(',')?;
                e.rank = self.ident()?.parse()?;
                self.expect(']')?;
            }
            "EBoolean"
            | "EInt32"
            | "EInt64"
            | "EFloat32"
            | "EFloat64"
            | "EVarint"
            | "EBinary"
            | "EBinary2"
            | "EBinaryLegacyFullWidthIntegerLength" => {}
            _ => return Err(format!("unsupported Hail encoding {kind}").into()),
        }
        Ok(e)
    }
    pub fn virtual_type(&mut self) -> Result<(Type, Encoding)> {
        let req = self.accept('+');
        let kind = self.ident()?;
        let primitive = match kind.as_str() {
            "Boolean" => Some((P::Bool, "EBoolean")),
            "Int" | "Int32" => Some((P::Int32, "EInt32")),
            "Int64" => Some((P::Int64, "EInt64")),
            "Float32" => Some((P::Float32, "EFloat32")),
            "Float64" => Some((P::Float64, "EFloat64")),
            "String" => Some((P::Str, "EBinary")),
            "Call" => Some((P::Call, "EInt32")),
            _ => None,
        };
        if let Some((p, e)) = primitive {
            return Ok((Type::Primitive(p), Encoding::new(e, req)));
        }
        if kind == "Struct" {
            let fields = self.fields(Self::virtual_type)?;
            return Ok((
                Type::Struct {
                    r#struct: fields
                        .iter()
                        .map(|(n, (t, _))| (n.clone(), t.clone()))
                        .collect(),
                },
                Encoding {
                    kind: "EBaseStruct".into(),
                    required: req,
                    fields: fields.into_iter().map(|(n, (_, e))| (n, e)).collect(),
                    rank: 0,
                },
            ));
        }
        if kind == "Locus" {
            self.expect('(')?;
            let name = self.ident()?;
            self.expect(')')?;
            let locus = self
                .refs
                .get(&name)
                .ok_or_else(|| format!("unknown Hail reference {name}"))?
                .clone();
            return Ok((
                Type::Locus { locus },
                Encoding {
                    kind: "EBaseStruct".into(),
                    required: req,
                    fields: vec![
                        ("contig".into(), Encoding::new("EBinary", true)),
                        ("position".into(), Encoding::new("EInt32", true)),
                    ],
                    rank: 0,
                },
            ));
        }
        self.expect('[')?;
        let mut args = vec![];
        let mut rank = 0;
        if !self.accept(']') {
            loop {
                if kind == "NDArray" && !args.is_empty() {
                    rank = self.ident()?.parse()?;
                } else {
                    args.push(self.virtual_type()?);
                }
                if !self.accept(',') {
                    self.expect(']')?;
                    break;
                }
            }
        }
        if kind == "Tuple" {
            return Ok((
                Type::Tuple {
                    tuple: args.iter().map(|(t, _)| t.clone()).collect(),
                },
                Encoding {
                    kind: "EBaseStruct".into(),
                    required: req,
                    fields: args
                        .into_iter()
                        .enumerate()
                        .map(|(i, (_, e))| (i.to_string(), e))
                        .collect(),
                    rank: 0,
                },
            ));
        }
        if args.is_empty() {
            return Err("missing Hail type argument".into());
        }
        let (t, e) = args.remove(0);
        let mut encoding = Encoding {
            kind: "EArray".into(),
            required: req,
            fields: vec![("item".into(), e.clone())],
            rank: 0,
        };
        let ty = match kind.as_str() {
            "Array" => Type::Array { array: Box::new(t) },
            "Set" => Type::Set { set: Box::new(t) },
            "Dict" => {
                if args.len() != 1 {
                    return Err("invalid Hail dictionary".into());
                }
                let (t2, e2) = args.remove(0);
                encoding.fields[0].1 = Encoding {
                    kind: "EBaseStruct".into(),
                    required: true,
                    fields: vec![("key".into(), e), ("value".into(), e2)],
                    rank: 0,
                };
                Type::Dict {
                    dict: Box::new((t, t2)),
                }
            }
            "Interval" => {
                encoding.kind = "EBaseStruct".into();
                encoding.fields = vec![
                    ("start".into(), e.clone()),
                    ("end".into(), e),
                    ("includesStart".into(), Encoding::new("EBoolean", true)),
                    ("includesEnd".into(), Encoding::new("EBoolean", true)),
                ];
                Type::Interval {
                    interval: Box::new(t),
                }
            }
            "NDArray" => {
                encoding.kind = "ENDArrayColumnMajor".into();
                encoding.rank = rank;
                Type::Ndarray {
                    ndarray: Box::new((t, rank)),
                }
            }
            _ => return Err(format!("unsupported Hail type {kind}").into()),
        };
        Ok((ty, encoding))
    }
    fn keys(&mut self) -> Result<Vec<String>> {
        self.expect('[')?;
        let mut out = vec![];
        if !self.accept(']') {
            loop {
                self.space();
                if self.text[self.pos..].starts_with('[') {
                    out.extend(self.keys()?)
                } else {
                    out.push(self.ident()?)
                }
                if !self.accept(',') {
                    self.expect(']')?;
                    break;
                }
            }
        }
        Ok(out)
    }
    pub fn relation(&mut self) -> Result<Relation> {
        let kind = self.ident()?;
        if !["Table", "Matrix", "RVDType", "OrderedRVDType"].contains(&kind.as_str()) {
            return Err("expected Hail relational type".into());
        }
        let mut out = Relation::default();
        self.expect('{')?;
        while !self.accept('}') {
            let name = self.ident()?;
            self.expect(':')?;
            if ["key", "row_key", "col_key"].contains(&name.as_str()) {
                out.keys.insert(name, self.keys()?);
            } else {
                out.types.insert(name, self.virtual_type()?);
            }
            if !self.accept(',') {
                self.expect('}')?;
                break;
            }
        }
        Ok(out)
    }
    pub fn finish(&mut self) -> Result<()> {
        self.space();
        if self.pos != self.text.len() {
            return Err("trailing Hail type input".into());
        }
        Ok(())
    }
}
#[derive(Default)]
pub struct Relation {
    pub types: HashMap<String, (Type, Encoding)>,
    pub keys: HashMap<String, Vec<String>>,
}
