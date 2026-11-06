//! Resolve maximal ordinary reads without cloning a wide typed lambda root for
//! every field. Original-node and shared-prefix identities are preparation-only.
use crate::{
    expressions::{Expr, PrimitiveType, Type},
    indexed_batch::{COLUMN_INDEX, ENTRY_INDEX, INPUT, IS_NULL, Input, RootDescriptor, ROW_INDEX},
};
use std::{collections::HashMap,sync::Arc};

pub(crate) fn capture_key(expr: &Expr) -> Option<(u8, String)> {
    Some(match expr {
        Expr::Column { name } => (0, name.clone()),
        Expr::Global { name } => (1, name.clone()),
        Expr::ExternalGlobal { reference, .. } => (2, reference.clone()),
        _ => return None,
    })
}
struct Node<'a> {
    root: usize,
    parent: Option<usize>,
    field: Option<(usize, &'a str)>,
    ty: &'a Type,
}
struct Paths<'a> {
    outer: &'a str,
    inner: &'a str,
    types: Vec<&'a Type>,
    captures: &'a HashMap<(u8, String), usize>,
    nodes: Vec<Node<'a>>,
    roots: HashMap<usize, usize>,
    children: HashMap<(usize, usize), usize>,
    seen: HashMap<usize, Option<usize>>,
    fields: HashMap<usize, HashMap<&'a str, usize>>,
}
impl<'a> Paths<'a> {
    fn resolve(&mut self, e: &Expr) -> Option<usize> {
        let address = e as *const Expr as usize;
        if let Some(id) = self.seen.get(&address) {
            return *id;
        }
        let root = match e {
            Expr::LambdaVariable { name, .. } if name == self.inner => Some(0),
            Expr::LambdaVariable { name, .. } if name == self.outer => Some(1),
            _ => capture_key(e).and_then(|k| self.captures.get(&k).map(|i| i + 2)),
        };
        let found = if let Some(root) = root {
            Some(*self.roots.entry(root).or_insert_with(|| {
                let id = self.nodes.len();
                self.nodes.push(Node {
                    root,
                    parent: None,
                    field: None,
                    ty: self.types[root],
                });
                id
            }))
        } else if let Expr::GetField { value, name } = e {
            let Some(parent) = self.resolve(value) else {self.seen.insert(address,None);return None;};
            let ty = self.nodes[parent].ty;
            if let Type::Struct { r#struct: fields } = ty {
                let indices = self
                    .fields
                    .entry(ty as *const Type as usize)
                    .or_insert_with(|| {
                        fields
                            .iter()
                            .enumerate()
                            .map(|(i, (n, _))| (n.as_str(), i))
                            .collect()
                    });
                if let Some(&index) = indices.get(name.as_str()) {
                    let root = self.nodes[parent].root;
                    Some(*self.children.entry((parent, index)).or_insert_with(|| {
                        let id = self.nodes.len();
                        self.nodes.push(Node {
                            root,
                            parent: Some(parent),
                            field: Some((index, fields[index].0.as_str())),
                            ty: &fields[index].1,
                        });
                        id
                    }))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        self.seen.insert(address, found);
        found
    }
}
pub(crate) fn rewrite(
    body: &Expr,
    outer: &str,
    outer_type: &Type,
    inner: &str,
    inner_type: &Type,
    capture_slots: &HashMap<(u8, String), usize>,
    capture_types: &[Type],
) -> Option<(Expr, Vec<Input>)> {
    let mut types = vec![inner_type, outer_type];
    types.extend(capture_types.iter());
    let mut paths = Paths {
        outer,
        inner,
        types,
        captures: capture_slots,
        nodes: vec![],
        roots: HashMap::new(),
        children: HashMap::new(),
        seen: HashMap::new(),
        fields: HashMap::new(),
    };
    fn walk(
        e: &Expr,
        shared: bool,
        paths: &mut Paths<'_>,
        memo: &mut HashMap<usize, Arc<Expr>>,
        slots: &mut HashMap<usize, usize>,
        inputs: &mut Vec<Input>,
        root_types: &mut HashMap<usize, Arc<RootDescriptor>>,
    ) -> Option<Arc<Expr>> {
        let address = e as *const Expr as usize;
        if shared { if let Some(found) = memo.get(&address) {
            return Some(found.clone());
        }}
        let validity = matches!(e, Expr::IsMissing { .. });
        let observed = if let Expr::IsMissing { value } = e {
            value.as_ref()
        } else {
            e
        };
        let result = if let Some(node) = paths.resolve(observed) {
            let ty = paths.nodes[node].ty;
            if !validity && matches!(ty, Type::Struct { .. }) {
                return None;
            }
            let slot = if let Some(slot) = slots.get(&node) {
                *slot
            } else {
                let root = paths.nodes[node].root;
                let root_type = if let Some(t) = root_types.get(&root) {
                    t.clone()
                } else {
                    let ty = RootDescriptor::new(paths.types[root].to_arrow().ok()?);
                    root_types.insert(root, ty.clone());
                    ty
                };
                let mut path = vec![];
                let mut current = node;
                while let Some(parent) = paths.nodes[current].parent {
                    let (index, name) = paths.nodes[current].field?;
                    path.push((index, name.to_owned()));
                    current = parent;
                }
                path.reverse();
                let (axis, name) = match root {
                    0 => ("entry", None),
                    1 => ("row", None),
                    i => ("col", Some((i - 2).to_string())),
                };
                let slot = inputs.len();
                inputs.push(Input {
                    axis: axis.into(),
                    name,
                    path,
                    root_type: Some(root_type),
                    dtype: ty.to_arrow().ok()?,
                });
                slots.insert(node, slot);
                slot
            };
            let column = match paths.nodes[node].root {
                0 => ENTRY_INDEX,
                1 => ROW_INDEX,
                _ => COLUMN_INDEX,
            };
            Expr::NestedFunction {
                function: if validity { IS_NULL } else { INPUT }.into(),
                args: vec![
                    Expr::Column {
                        name: column.into(),
                    },
                    Expr::Literal {
                        dtype: Type::Primitive(PrimitiveType::Int64),
                        value: serde_json::json!(slot),
                    },
                    Expr::Literal {
                        dtype: Type::Primitive(PrimitiveType::Str),
                        value: serde_json::json!(format!("product-input-{slot}")),
                    },
                ],
                dtype: if validity {
                    Type::Primitive(PrimitiveType::Bool)
                } else {
                    ty.clone()
                },
            }
        } else {
            let mut children = vec![];
            e.for_each_child(&mut |c| children.push(c));
            let mut children = children.into_iter();
            e.try_map_children_shared(|_,shared| {
                let old=children.next().unwrap();
                walk(old,shared,paths,memo,slots,inputs,root_types).ok_or(())
            }).ok()?
        };
        let result=Arc::new(result);
        if shared {memo.insert(address, result.clone());}
        Some(result)
    }
    let mut inputs = vec![];
    let body = walk(
        body,
        false,
        &mut paths,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut inputs,
        &mut HashMap::new(),
    )?;
    Some((Arc::unwrap_or_clone(body), inputs))
}
