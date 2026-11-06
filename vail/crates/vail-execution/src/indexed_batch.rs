//! Evaluation-local indexed inputs for a flat aligned-map body.
use crate::expressions::{Expr, ExprError, PrimitiveType, RebuiltExpr, Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StructArray,
    UInt32Array, builder::BooleanBuilder, make_array,
};
use arrow_buffer::{BooleanBuffer,NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, Schema};
use arrow_select::take::{TakeOptions, take};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

pub(crate) const SELECTED_INDEX: &str = "__vail_selected_entry";
pub(crate) const ENTRY_INDEX: &str = "__vail_indexed_entry";
pub(crate) const COLUMN_INDEX: &str = "__vail_indexed_column";
pub(crate) const ROW_INDEX: &str = "__vail_indexed_row";
pub(crate) const PRESENCE: &str = "__vail_indexed_present";
pub(crate) const INPUT: &str = "__vail_indexed_input";
pub(crate) const IS_NULL: &str = "__vail_indexed_is_null";

/// A full root descriptor is retained and hashed once, then shared by leaves.
#[derive(Debug)]
pub(crate) struct RootDescriptor {
    dtype: DataType,
    fingerprint: u64,
}
impl RootDescriptor {
    pub fn dtype(&self)->&DataType {&self.dtype}
    pub fn new(dtype: DataType) -> Arc<Self> {
        let mut hasher=std::collections::hash_map::DefaultHasher::new();
        dtype.hash(&mut hasher);
        Arc::new(Self {dtype,fingerprint:hasher.finish()})
    }
}
impl PartialEq for RootDescriptor {
    fn eq(&self,other:&Self)->bool {
        std::ptr::eq(self,other) || (self.fingerprint==other.fingerprint && self.dtype==other.dtype)
    }
}
impl Eq for RootDescriptor {}
impl Hash for RootDescriptor {
    fn hash<H:Hasher>(&self,hasher:&mut H) {self.fingerprint.hash(hasher);}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Input {
    pub axis: String,
    pub name: Option<String>,
    pub path: Vec<(usize, String)>,
    pub root_type: Option<Arc<RootDescriptor>>,
    pub dtype: DataType,
}

impl Input {
    pub fn prepare(&mut self, root: Arc<RootDescriptor>) -> bool {
        let mut dtype = root.dtype();
        for (index, name) in &mut self.path {
            let DataType::Struct(fields) = dtype else {
                return false;
            };
            let found = if fields.get(*index).is_some_and(|field|field.name()==name) {
                *index
            } else {
                let Some(found)=fields.iter().position(|field|field.name()==name) else {return false;};
                found
            };
            *index = found;
            dtype = fields[found].data_type();
        }
        if dtype != &self.dtype {
            return false;
        }
        self.root_type = Some(root);
        true
    }
}

// Shared retained prefixes are resolved once; leaf descriptors borrow logical
// types while discovery runs and materialize only their own ordinary path.
struct PathNode<'a> {
    parent: Option<usize>,
    field: Option<(usize, &'a str)>,
    dtype: &'a Type,
    root: Option<(&'a str, &'a Option<String>)>,
}
#[derive(Default)]
struct Paths<'a> {
    nodes: Vec<PathNode<'a>>,
    memo: HashMap<usize, Option<usize>>,
    fields: HashMap<usize, HashMap<&'a str, usize>>,
}
impl<'a> Paths<'a> {
    fn resolve(&mut self, expr: &'a Expr) -> Option<usize> {
        let address = expr as *const Expr as usize;
        if let Some(found) = self.memo.get(&address) {
            return *found;
        }
        let found = match expr {
            Expr::AlignedMapInput { axis, name, dtype } if axis != "entry_present" => {
                let id = self.nodes.len();
                self.nodes.push(PathNode {
                    parent: None,
                    field: None,
                    dtype,
                    root: Some((axis, name)),
                });
                Some(id)
            }
            Expr::GetField { value, name } => {
                let parent = self.resolve(value)?;
                let dtype = self.nodes[parent].dtype;
                let Type::Struct { r#struct: fields } = dtype else {
                    self.memo.insert(address, None);
                    return None;
                };
                let indices = self
                    .fields
                    .entry(dtype as *const Type as usize)
                    .or_insert_with(|| {
                        fields
                            .iter()
                            .enumerate()
                            .map(|(i, (name, _))| (name.as_str(), i))
                            .collect()
                    });
                let index = *indices.get(name.as_str())?;
                let id = self.nodes.len();
                self.nodes.push(PathNode {
                    parent: Some(parent),
                    field: Some((index, name)),
                    dtype: &fields[index].1,
                    root: None,
                });
                Some(id)
            }
            _ => None,
        };
        self.memo.insert(address, found);
        found
    }
    fn input(&self, id: usize) -> Option<Input> {
        let dtype = self.nodes[id].dtype.to_arrow().ok()?;
        let mut current = id;
        let mut path = vec![];
        while let Some(parent) = self.nodes[current].parent {
            let (index, name) = self.nodes[current].field?;
            path.push((index, name.to_owned()));
            current = parent;
        }
        path.reverse();
        let (axis, name) = self.nodes[current].root?;
        Some(Input {
            axis: axis.to_owned(),
            name: name.clone(),
            path,
            root_type: if axis == "selected_entry" { Some(RootDescriptor::new(self.nodes[current].dtype.to_arrow().ok()?)) } else { None },
            dtype,
        })
    }
}
pub(crate) fn ordinary_entry_reads<'a>(expressions: impl IntoIterator<Item = &'a Expr>) -> bool {
    let mut paths = Paths::default();
    for expression in expressions {
        if !expression.is_pure() {
            return false;
        }
        let Some(mut id) = paths.resolve(expression) else {
            return false;
        };
        while let Some(parent) = paths.nodes[id].parent {
            id = parent;
        }
        if paths.nodes[id].root.is_none_or(|(axis, _)| axis != "entry") {
            return false;
        }
    }
    true
}

type InputKey = (String, Option<String>, Vec<String>);

pub(crate) fn rewrite(body: &Expr) -> Option<(Expr, Vec<Input>)> {
    fn walk<'a>(
        expr: &'a Expr,
        shared: bool,
        inputs: &mut Vec<Input>,
        slots: &mut HashMap<InputKey, usize>,
        memo: &mut HashMap<usize, Arc<Expr>>,
        paths: &mut Paths<'a>,
    ) -> Option<RebuiltExpr> {
        if matches!(
            expr,
            Expr::AlignedMatrixMap { .. }
                | Expr::ArrayAggregateScan { .. }
                | Expr::Column { .. }
                | Expr::Global { .. }
                | Expr::ExternalGlobal { .. }
                | Expr::TableLookup { .. }
        ) {
            return None;
        }
        let address = expr as *const Expr as usize;
        if shared {if let Some(value)=memo.get(&address){return Some(RebuiltExpr::Shared(value.clone()));}}
        let validity = matches!(expr, Expr::IsMissing { .. });
        let observed = if let Expr::IsMissing { value } = expr {
            value.as_ref()
        } else {
            expr
        };
        let output = if let Some(path) = paths.resolve(observed) {
            let dtype = paths.nodes[path].dtype;
            let input = paths.input(path)?;
            let names = input
                .path
                .iter()
                .map(|(_, name)| name.clone())
                .collect::<Vec<_>>();
            let key = (input.axis.clone(), input.name.clone(), names);
            if !validity && matches!(dtype, Type::Struct { .. }) {
                return None;
            }
            let slot = *slots.entry(key.clone()).or_insert_with(|| {
                let slot = inputs.len();
                inputs.push(input.clone());
                slot
            });
            let column = match input.axis.as_str() {
                "entry" => ENTRY_INDEX,
                "selected_entry" => SELECTED_INDEX,
                "col" => COLUMN_INDEX,
                "row" | "row_invariant" | "global" | "num_cols" => ROW_INDEX,
                _ => return None,
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
                        value: serde_json::json!(serde_json::to_string(&key).ok()?),
                    },
                ],
                dtype: if validity {
                    Type::Primitive(PrimitiveType::Bool)
                } else {
                    dtype.clone()
                },
            }
        } else if matches!(expr, Expr::AlignedMapInput { axis, .. } if axis == "entry_present") {
            Expr::Column {
                name: PRESENCE.into(),
            }
        } else {
            let mut children: Vec<&'a Expr> = vec![];
            expr.for_each_child(&mut |child| children.push(child));
            let mut children=children.into_iter();
            expr.try_map_children_rebuilt(|_,shared| {
                let child=children.next().unwrap();
                walk(child,shared,inputs,slots,memo,paths).ok_or(())
            }).ok()?

        };
        if shared {let output=Arc::new(output);memo.insert(address,output.clone());Some(RebuiltExpr::Shared(output))}
        else {Some(RebuiltExpr::Owned(output))}
    }
    let mut inputs = vec![];
    let output = walk(
        body,
        false,
        &mut inputs,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut Paths::default(),
    )?;
    Some((output.into_owned(), inputs))
}

/// Proof generated while constructing this invocation's coordinate frame.
/// A different filtered/gathered index array cannot use this borrowing path.
#[derive(Clone,Debug)]
pub(crate) struct ContiguousDomain {pub indices:ArrayRef,pub start:usize}
#[derive(Clone, Debug)]
pub(crate) struct Source {
    pub values: ArrayRef,
    pub ancestors: Vec<NullBuffer>,
    all_ancestors_valid: bool,
    pub scalar: bool,
    binding: Arc<()>,
    pub contiguous:Option<Arc<ContiguousDomain>>,
}
impl Source {
    pub fn bind(root: ArrayRef, input: &Input, scalar: bool) -> Result<Self> {
        Self::validate_root(&root,input)?;
        Self::bind_validated(root,input,scalar)
    }
    pub fn validate_root(root: &ArrayRef,input: &Input)->Result<()> {
        if input.root_type.as_ref().is_some_and(|descriptor| root.data_type()!=descriptor.dtype()) {
            return Err(DataFusionError::Execution(
                "indexed root schema changed".into(),
            ));
        }
        Ok(())
    }
    /// The caller has checked this exact owner's full root descriptor.
    pub fn bind_validated(root:ArrayRef,input:&Input,scalar:bool)->Result<Self> {
        let mut values = root;
        let mut ancestors = vec![];
        for (index, name) in &input.path {
            if let Some(nulls) = values.nulls() {
                ancestors.push(nulls.clone());
            }
            let parent = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("indexed source Struct layout changed".into())
                })?;
            if parent
                .fields()
                .get(*index)
                .is_none_or(|field| field.name() != name)
            {
                return Err(DataFusionError::Execution(
                    "indexed source field descriptor changed".into(),
                ));
            }
            values = parent.columns().get(*index).cloned().ok_or_else(|| {
                DataFusionError::Execution("indexed source field is absent".into())
            })?;
        }
        if values.data_type() != &input.dtype {
            return Err(DataFusionError::Execution(
                "indexed source physical type changed".into(),
            ));
        }
        let all_ancestors_valid = ancestors.iter().all(|nulls| nulls.null_count() == 0);
        Ok(Self {
            values,
            ancestors,
            all_ancestors_valid,
            scalar,
            binding: Arc::new(()),
            contiguous:None,
        })
    }
    fn valid(&self, row: usize) -> bool {
        self.ancestors.iter().all(|nulls| nulls.is_valid(row)) && self.values.is_valid(row)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedInput {
    slot: usize,
    key: String,
    dtype: DataType,
    indices: Arc<dyn PhysicalExpr>,
    selector: Option<Arc<dyn PhysicalExpr>>,
    validity: bool,
    borrow_list_children: bool,
    trace_list_borrow: bool,
    source: Option<Source>,
}
impl IndexedInput {
    pub fn new(
        slot: usize,
        key: String,
        dtype: DataType,
        indices: Arc<dyn PhysicalExpr>,
        selector: Option<Arc<dyn PhysicalExpr>>,
        validity: bool,
        borrow_list_children: bool,
        trace_list_borrow: bool,
    ) -> Self {
        Self {
            slot,
            key,
            dtype,
            indices,
            selector,
            validity,
            borrow_list_children,
            trace_list_borrow,
            source: None,
        }
    }
}
impl PartialEq for IndexedInput {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot
            && self.key == other.key
            && self.dtype == other.dtype
            && self.validity == other.validity
            && self.borrow_list_children == other.borrow_list_children
            && self.indices.eq(&other.indices)
            && self.selector == other.selector
            && match (&self.source, &other.source) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(&a.binding, &b.binding),
                _ => false,
            }
    }
}
impl Eq for IndexedInput {}
impl Hash for IndexedInput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.slot.hash(state);
        self.key.hash(state);
        self.dtype.hash(state);
        self.validity.hash(state);
        self.borrow_list_children.hash(state);
        self.indices.hash(state);
        self.selector.hash(state);
        self.source
            .as_ref()
            .map(|s| Arc::as_ptr(&s.binding) as usize)
            .hash(state);
    }
}
impl fmt::Display for IndexedInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexedInput({})", self.key)
    }
}
impl PhysicalExpr for IndexedInput {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(!self.validity)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![&self.indices];
        children.extend(self.selector.iter());
        children
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 + usize::from(self.selector.is_some()) {
            return Err(DataFusionError::Internal(
                "indexed input children changed".into(),
            ));
        }
        Ok(Arc::new(Self {
            indices: children[0].clone(),
            selector: children.get(1).cloned(),
            ..self.as_ref().clone()
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.evaluate_value(batch)
    }
}
impl IndexedInput {
    fn evaluate_value(&self,batch:&RecordBatch)->Result<ColumnarValue>{
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("indexed input owner is unbound".into()))?;
        if source.scalar && self.selector.is_none() {
            let value = if self.validity {
                datafusion_common::ScalarValue::Boolean(Some(!source.valid(0)))
            } else if source.valid(0) {
                datafusion_common::ScalarValue::try_from_array(&source.values, 0)?
            } else {
                datafusion_common::ScalarValue::try_from(&self.dtype)?
            };
            return Ok(ColumnarValue::Scalar(value));
        }
        let indices = self.indices.evaluate(batch)?.into_array(batch.num_rows())?;
        if self.selector.is_none() && source.all_ancestors_valid {
            if let Some(domain)=&source.contiguous {
                // Whole logical owners only: a tiny contiguous selection must
                // retain the existing compacting gather rather than a view.
                // Broader backing-buffer retention is still experimental.
                let plain=source.values.data_type().is_primitive()
                    || matches!(source.values.data_type(),DataType::Boolean|DataType::List(_)|DataType::Struct(_));
                if plain && domain.start==0 && indices.len()==source.values.len()
                    && Arc::ptr_eq(&domain.indices,&indices) {
                    let end=domain.start.checked_add(indices.len()).ok_or_else(||DataFusionError::Execution("contiguous input span overflow".into()))?;
                    if end>source.values.len(){return Err(DataFusionError::Execution("contiguous input outside source".into()));}
                    let view=source.values.slice(domain.start,indices.len());
                    if self.validity {
                        let missing=match view.nulls(){Some(nulls)=>!nulls.inner(),None=>BooleanBuffer::new_unset(view.len())};
                        return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(missing,None))));
                    }
                    return Ok(ColumnarValue::Array(view));
                }
            }
        }
        let indices = indices
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| DataFusionError::Execution("indexed lanes must be int32".into()))?;
        if self.selector.is_none()
            && !matches!(source.values.data_type(), DataType::List(_))
            && !self.validity
            && source.all_ancestors_valid
        {
            return Ok(ColumnarValue::Array(take(
                source.values.as_ref(),
                indices,
                Some(TakeOptions { check_bounds: true }),
            )?));
        }
        let selector = self
            .selector
            .as_ref()
            .map(|selector| selector.evaluate(batch)?.into_array(batch.num_rows()))
            .transpose()?;
        if self.validity {
            let mut output = BooleanBuilder::with_capacity(indices.len());
            for lane in 0..indices.len() {
                if indices.is_null(lane) {
                    output.append_value(true);
                    continue;
                }
                let row = indices.value(lane) as usize;
                if row >= source.values.len() {
                    return Err(DataFusionError::Execution(
                        "indexed lane outside source".into(),
                    ));
                }
                output.append_value(!source.valid(row));
            }
            return Ok(ColumnarValue::Array(Arc::new(output.finish())));
        }
        enum Selector<'a> {
            I32(&'a Int32Array),
            I64(&'a Int64Array),
        }
        let selector_values = selector
            .as_ref()
            .map(|array| {
                if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
                    Ok(Selector::I32(values))
                } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
                    Ok(Selector::I64(values))
                } else {
                    Err(DataFusionError::Execution(
                        "array index must be integer".into(),
                    ))
                }
            })
            .transpose()?;
        let mut selected = Vec::with_capacity(indices.len());
        let mut selected_validity = NullBufferBuilder::new(indices.len());
        let lists = if selector.is_some() {
            Some(
                source
                    .values
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("indexed list source changed".into())
                    })?,
            )
        } else {
            None
        };
        for lane in 0..indices.len() {
            if indices.is_null(lane) {
                selected.push(0);
                selected_validity.append_null();
                continue;
            }
            let row = if source.scalar {
                0
            } else {
                indices.value(lane) as usize
            };
            if row >= source.values.len() {
                return Err(DataFusionError::Execution(
                    "indexed lane outside source".into(),
                ));
            }
            let present = indices.is_valid(lane) && source.valid(row);
            if !present {
                selected.push(0);
                selected_validity.append_null();
                continue;
            }
            let index = if let Some(selector) = &selector {
                if selector.is_null(lane) {
                    selected.push(0);
                    selected_validity.append_null();
                    continue;
                }
                let value = match selector_values.as_ref().unwrap() {
                    Selector::I32(values) => i64::from(values.value(lane)),
                    Selector::I64(values) => values.value(lane),
                };
                let list = lists.unwrap();
                let offsets = list.value_offsets();
                let length = i64::from(offsets[row + 1] - offsets[row]);
                let index = if value < 0 {
                    value.checked_add(length)
                } else {
                    Some(value)
                };
                let Some(index) = index.filter(|&index| index >= 0 && index < length) else {
                    return Err(DataFusionError::Execution(format!(
                        "array index out of bounds: index {value}, length {length}"
                    )));
                };
                u32::try_from(i64::from(offsets[row]) + index).map_err(|_| {
                    DataFusionError::Execution("indexed child offset overflow".into())
                })?
            } else {
                row as u32
            };
            selected.push(index);
            selected_validity.append_non_null();
        }
        let owner = lists.map_or(&source.values, |lists| lists.values());
        let selected = UInt32Array::new(ScalarBuffer::from(selected), selected_validity.finish());
        let output = if let Some(lists) = owner
            .as_any()
            .downcast_ref::<ListArray>()
            .filter(|lists| lists.values().data_type().is_primitive())
        {
            if self.borrow_list_children {gather_primitive_lists::<true>(lists,&selected,self.trace_list_borrow)?}
            else {gather_primitive_lists::<false>(lists,&selected,self.trace_list_borrow)?}
        } else {
            take(
                owner.as_ref(),
                &selected,
                Some(TakeOptions { check_bounds: true }),
            )?
        };
        Ok(ColumnarValue::Array(output))
    }
}

pub(crate) fn bind(
    expression: &Arc<dyn PhysicalExpr>,
    sources: &[Source],
    memo: &mut HashMap<usize, Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let key = Arc::as_ptr(expression) as *const () as usize;
    if let Some(bound) = memo.get(&key) {
        return Ok(bound.clone());
    }
    if let Some(bound) = crate::expressions::bind_indexed_wrapper(expression, sources, memo)? {
        memo.insert(key, bound.clone());
        return Ok(bound);
    }
    if let Some(coalesce)=expression.downcast_ref::<crate::coalesce_once::CoalesceOnce>() {
        let first=bind(&coalesce.first,sources,memo)?;
        let fallback=bind(&coalesce.fallback,sources,memo)?;
        let selected=bind(&coalesce.selected_fallback.expr,sources,memo)?;
        let bound=coalesce.bound(first,fallback,selected);memo.insert(key,bound.clone());return Ok(bound);
    }
    if let Some(switch) = expression.downcast_ref::<crate::conditionals::ScopedSwitch>() {
        fn callback(
            value: &crate::conditionals::Callback,
            sources: &[Source],
            memo: &mut HashMap<usize, Arc<dyn PhysicalExpr>>,
        ) -> Result<crate::conditionals::Callback> {
            Ok(crate::conditionals::Callback {
                expr: bind(&value.expr, sources, memo)?,
                indices: value.indices.clone(),
                schema: value.schema.clone(),
            })
        }
        let bound: Arc<dyn PhysicalExpr> = Arc::new(crate::conditionals::ScopedSwitch {
            inputs: switch
                .inputs
                .iter()
                .map(|input| bind(input, sources, memo))
                .collect::<Result<Vec<_>>>()?,
            value: bind(&switch.value, sources, memo)?,
            cases: switch
                .cases
                .iter()
                .map(|(condition, value)| {
                    Ok((
                        callback(condition, sources, memo)?,
                        callback(value, sources, memo)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            default: callback(&switch.default, sources, memo)?,
            missing: switch
                .missing
                .as_ref()
                .map(|value| callback(value, sources, memo))
                .transpose()?,
            input_schema: switch.input_schema.clone(),
            bound_schema: switch.bound_schema.clone(),
            dtype: switch.dtype.clone(),
        });
        memo.insert(key, bound.clone());
        return Ok(bound);
    }
    let children = expression
        .children()
        .into_iter()
        .map(|child| bind(child, sources, memo))
        .collect::<Result<Vec<_>>>()?;
    let unchanged = children
        .iter()
        .zip(expression.children())
        .all(|(new, old)| Arc::ptr_eq(new, old));
    let rebuilt = if unchanged {
        expression.clone()
    } else {
        expression.clone().with_new_children(children)?
    };
    let result: Arc<dyn PhysicalExpr> = if let Some(input) = rebuilt.downcast_ref::<IndexedInput>()
    {
        let source = sources
            .get(input.slot)
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("indexed source slot absent".into()))?;
        Arc::new(IndexedInput {
            source: Some(source),
            ..input.clone()
        })
    } else {
        rebuilt
    };
    memo.insert(key, result.clone());
    Ok(result)
}

// The selected output size comes from selected list lengths, not the owner's
// average list length, which can include large spans hidden below null lists.
// Whole logical coverage does not by itself bound retained backing storage.
// Only ordinary owned, unsliced primitive allocations with modest spare
// capacity may outlive this invocation through a borrowed output child.
fn bounded_primitive_child(values:&ArrayRef)->bool {
    let Some(width)=values.data_type().primitive_width() else{return false;};
    let Some(bytes)=values.len().checked_mul(width) else{return false;};
    if bytes==0{return false;}
    let bounded=|buffer:&arrow_buffer::Buffer,bytes:usize| {
        let Some(limit)=bytes.checked_mul(2).and_then(|x|x.checked_add(64)) else{return false;};
        buffer.ptr_offset()==0&&buffer.capacity()>0&&buffer.capacity()<=limit
    };
    let data=values.to_data();
    if data.offset()!=0||!data.child_data().is_empty()||data.buffers().len()!=1||!bounded(&data.buffers()[0],bytes){return false;}
    if let Some(nulls)=values.nulls() {
        let Some(bytes)=values.len().checked_add(7).map(|n|n/8) else{return false;};
        if nulls.offset()!=0||!bounded(nulls.inner().inner(),bytes){return false;}
    }
    true
}

pub(crate) fn take_primitive_lists(values:&ListArray,indices:&UInt32Array)->Result<ArrayRef>{
    gather_primitive_lists::<false>(values,indices,false)
}
fn gather_primitive_lists<const BORROW:bool>(values:&ListArray,indices:&UInt32Array,trace:bool)->Result<ArrayRef>{
    let mut complete=BORROW&&bounded_primitive_child(values.values());
    let mut previous_end=0usize;
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    offsets.push(0i32);
    let mut nulls = NullBufferBuilder::new(indices.len());
    let mut ranges = Vec::with_capacity(indices.len());
    for index in indices.iter() {
        let range = match index {
            Some(index) if (index as usize) < values.len() => {
                let index = index as usize;
                values.is_valid(index).then(|| {
                    let offsets = values.value_offsets();
                    (offsets[index] as usize, offsets[index + 1] as usize)
                })
            }
            Some(_) => {
                return Err(DataFusionError::Execution(
                    "indexed list outside source".into(),
                ));
            }
            None => None,
        };
        if let Some((start, end)) = range {
            if complete {complete=start==previous_end;previous_end=end;}
            let length = i32::try_from(end - start)
                .map_err(|_| DataFusionError::Execution("selected list length overflow".into()))?;
            offsets.push(offsets.last().unwrap().checked_add(length).ok_or_else(|| {
                DataFusionError::Execution("selected list offset overflow".into())
            })?);
            ranges.push((start, end));
            nulls.append_non_null();
        } else {
            offsets.push(*offsets.last().unwrap());
            nulls.append_null();
        }
    }
    fn copy<T: arrow_array::types::ArrowPrimitiveType>(values:&arrow_array::PrimitiveArray<T>,ranges:&[(usize,usize)],len:usize)->ArrayRef {
        let mut out=Vec::with_capacity(len);let mut valid=NullBufferBuilder::new(len);
        if values.null_count()==0 {
            for &(start,end) in ranges {out.extend_from_slice(&values.values()[start..end]);}
            valid.append_n_non_nulls(len);
        }else{
            let nulls=values.nulls().unwrap();
            for &(start,end) in ranges {out.extend_from_slice(&values.values()[start..end]);valid.append_buffer(&nulls.slice(start,end-start));}
        }
        Arc::new(arrow_array::PrimitiveArray::<T>::new(ScalarBuffer::from(out),valid.finish()).with_data_type(values.data_type().clone()))
    }
    let len=*offsets.last().unwrap() as usize;
    let borrowed=complete&&previous_end==values.values().len();
    let mut child=borrowed.then(||values.values().clone());
    macro_rules! typed {($($ty:ty),*)=>{$(if let Some(values)=values.values().as_any().downcast_ref::<$ty>(){child=Some(copy(values,&ranges,len));})*};}
    if child.is_none() {
    typed!(arrow_array::Int8Array,arrow_array::Int16Array,Int32Array,Int64Array,arrow_array::UInt8Array,arrow_array::UInt16Array,UInt32Array,arrow_array::UInt64Array,arrow_array::Float32Array,arrow_array::Float64Array);
    }
    let child=if let Some(child)=child{child}else{
        let data=values.values().to_data();let mut out=MutableArrayData::new(vec![&data],data.null_count()>0,len);
        for (start,end) in ranges {out.try_extend(0,start,end)?;}make_array(out.freeze())
    };
    if trace {eprintln!("VAIL_LIST_CHILD rows={} child_values={} borrowed={borrowed}",indices.len(),len);}
    Ok(Arc::new(ListArray::try_new(
        match values.data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        nulls.finish(),
    )?))
}
