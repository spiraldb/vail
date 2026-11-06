//! Prepare pure row-domain scalar values before expanding aligned entry lanes.
use crate::{expressions::{Expr,ExprError,Type,PreparationOptions,lower_with_options},list_dependencies::Dependencies};
use arrow_array::{Array,ArrayRef,RecordBatch,StructArray,new_empty_array};
use arrow_schema::{DataType,Field,Schema};
use datafusion_common::{DataFusionError,Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::{collections::{HashMap,HashSet},fmt,hash::{Hash,Hasher},sync::Arc};

pub(crate) fn prepare(body:&Expr,rows:Arc<dyn PhysicalExpr>,schema:&Schema,options:&PreparationOptions,types:&mut dyn FnMut(&Expr)->std::result::Result<Type,ExprError>)->std::result::Result<Option<(Arc<Expr>,Arc<dyn PhysicalExpr>)>,ExprError> {
    let mut nested=false;
    body.visit_unique(&mut |node|nested|=matches!(node,Expr::AlignedMatrixMap{..}|Expr::Column{..}|Expr::Global{..}|Expr::ExternalGlobal{..}));
    if nested {return Ok(None);}
    let Some(mut dependencies)=Dependencies::new(std::iter::empty(),"__vail_entry_domain") else {return Ok(None);};
    for axis in ["row","entry","col","entry_present","global","num_cols"] {
        if dependencies.add_intrinsic(axis,axis=="row").is_none(){return Ok(None);}
    }
    let context=dependencies.callback_context();
    if dependencies.analyze(body,context).is_none(){return Ok(None);}
    let mut names=HashSet::new();
    body.visit_unique(&mut |node|if let Expr::AlignedMapInput{axis,name:Some(name),..}=node {if axis=="row"{names.insert(name.clone());}});
    let original=rows.data_type(schema)?;
    let DataType::Struct(outer_fields)=&original else {return Ok(None);};
    let Some(index)=outer_fields.iter().position(|field|field.name()=="__vail_aligned_row") else {return Ok(None);};
    let DataType::Struct(inner_fields)=outer_fields[index].data_type() else {return Ok(None);};
    names.extend(inner_fields.iter().map(|field|field.name().clone()));
    let mut prefix="__vail_row_invariant_".to_owned();
    while names.iter().any(|name|name.starts_with(&prefix)){prefix.push('_');}
    fn computed(e:&Expr,memo:&mut HashMap<usize,bool>)->bool {
        let key=e as *const Expr as usize;
        if let Some(value)=memo.get(&key){return *value;}
        let value=match e {Expr::AlignedMapInput{..}|Expr::Column{..}|Expr::LambdaVariable{..}|Expr::Literal{..}|Expr::RuntimeLiteral{..}=>false,Expr::GetField{value,..}=>computed(value,memo),_=>true};
        memo.insert(key,value);value
    }
    fn rewrite(e:&Expr,context:usize,shared:bool,dependencies:&Dependencies,prefix:&str,captures:&mut Vec<(Arc<Expr>,Type)>,slots:&mut HashMap<(usize,usize),usize>,memo:&mut HashMap<(usize,usize),Arc<Expr>>,computed_memo:&mut HashMap<usize,bool>,types:&mut dyn FnMut(&Expr)->std::result::Result<Type,ExprError>)->std::result::Result<Arc<Expr>,ExprError> {
        let key=(e as *const Expr as usize,context);
        if shared {if let Some(value)=memo.get(&key){return Ok(value.clone());}}
        let ty=if computed(e,computed_memo)&&dependencies.invariant(e,context) {types(e).ok().filter(|ty|ty.to_arrow().is_ok_and(|dt|dt.is_primitive()||dt==DataType::Boolean))}else{None};
        let out=if let Some(ty)=ty {
            let slot=*slots.entry(key).or_insert_with(||{let slot=captures.len();captures.push((Arc::new(e.clone()),ty.clone()));slot});
            Expr::AlignedMapInput{axis:"row_invariant".into(),name:Some(format!("{prefix}{slot}")),dtype:ty}
        }else {
            let mut position=0;
            e.try_map_children_shared(|child,shared|{let child_context=dependencies.child_context(e,context,position);position+=1;rewrite(child,child_context,shared,dependencies,prefix,captures,slots,memo,computed_memo,types)})?
        };
        let out=Arc::new(out);if shared{memo.insert(key,out.clone());}Ok(out)
    }
    let mut captures=vec![];
    let rewritten=rewrite(body,context,false,&dependencies,&prefix,&mut captures,&mut HashMap::new(),&mut HashMap::new(),&mut HashMap::new(),types)?;
    if captures.is_empty(){return Ok(None);}
    let frame=Arc::new(Schema::new(vec![Field::new("__vail_original_rows",original.clone(),true)]));
    fn bind(e:&Expr,shared:bool,memo:&mut HashMap<usize,Arc<Expr>>)->std::result::Result<Arc<Expr>,ExprError> {
        let key=e as *const Expr as usize;
        if shared {if let Some(value)=memo.get(&key){return Ok(value.clone());}}
        let result=if let Expr::AlignedMapInput{axis,name:Some(name),..}=e {
            if axis!="row" {return Err(ExprError::Invalid("non-row input in row invariant".into()));}
            Expr::GetField{value:Arc::new(Expr::GetField{value:Arc::new(Expr::Column{name:"__vail_original_rows".into()}),name:"__vail_aligned_row".into()}),name:name.clone()}
        }else {e.try_map_children_shared(|child,shared|bind(child,shared,memo))?};
        let result=Arc::new(result);if shared{memo.insert(key,result.clone());}Ok(result)
    }
    let mut bound=HashMap::new();
    let capture_expression=Expr::Struct {fields:captures.iter().enumerate().map(|(slot,(expr,_))|Ok((format!("{prefix}{slot}"),Arc::unwrap_or_clone(bind(expr,false,&mut bound)?)))).collect::<std::result::Result<Vec<_>,ExprError>>()?};
    let program=lower_with_options(&capture_expression,&frame,options)?;
    let mut fields=vec![];
    for (slot,(_,ty)) in captures.iter().enumerate(){fields.push(Arc::new(Field::new(format!("{prefix}{slot}"),ty.to_arrow()?,true)));}
    let dtype=DataType::Struct(vec![
        Arc::new(Field::new("__vail_invariant_source",original,true)),
        Arc::new(Field::new("__vail_invariant_values",DataType::Struct(fields.into()),false)),
    ].into());
    Ok(Some((rewritten,Arc::new(RowCaptures{input:rows,program,frame,dtype}))))
}
#[derive(Clone,Debug)]
struct RowCaptures {input:Arc<dyn PhysicalExpr>,program:Arc<dyn PhysicalExpr>,frame:Arc<Schema>,dtype:DataType}
impl PartialEq for RowCaptures {fn eq(&self,o:&Self)->bool{self.input.eq(&o.input)&&self.program.eq(&o.program)&&self.frame==o.frame&&self.dtype==o.dtype}}
impl Eq for RowCaptures {}
impl Hash for RowCaptures {fn hash<H:Hasher>(&self,h:&mut H){self.input.hash(h);self.program.hash(h);self.frame.hash(h);self.dtype.hash(h);}}
impl fmt::Display for RowCaptures {fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{write!(f,"AlignedRowInvariants")}}
impl PhysicalExpr for RowCaptures {
 fn fmt_sql(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{fmt::Display::fmt(self,f)}
 fn data_type(&self,_:&Schema)->Result<DataType>{Ok(self.dtype.clone())}
 fn nullable(&self,_:&Schema)->Result<bool>{Ok(true)}
 fn children(&self)->Vec<&Arc<dyn PhysicalExpr>>{vec![&self.input]}
 fn with_new_children(self:Arc<Self>,children:Vec<Arc<dyn PhysicalExpr>>)->Result<Arc<dyn PhysicalExpr>>{if children.len()!=1{return Err(DataFusionError::Internal("row invariant input arity changed".into()));}Ok(Arc::new(Self{input:children[0].clone(),..self.as_ref().clone()}))}
 fn evaluate(&self,batch:&RecordBatch)->Result<ColumnarValue>{
    self.evaluate_value(self.input.evaluate(batch)?,batch.num_rows(),true)
 }
}
impl RowCaptures {
 fn evaluate_value(&self,value:ColumnarValue,rows:usize,demanded:bool)->Result<ColumnarValue> {
    if rows==0{return Ok(ColumnarValue::Array(new_empty_array(&self.dtype)));}
    let scalar=matches!(&value,ColumnarValue::Scalar(_));
    let len=if scalar {1}else{rows};
    let root=value.into_array(len)?;
    let frame=RecordBatch::try_new(self.frame.clone(),vec![root.clone()])?;
    let DataType::Struct(fields)=&self.dtype else {unreachable!()};
    let derived=if demanded {self.program.evaluate(&frame)?.into_array(len)?}
        else {arrow_array::new_null_array(fields[1].data_type(),len)};
    let derived=derived.as_any().downcast_ref::<StructArray>().ok_or_else(||DataFusionError::Execution("row invariant values must be Struct".into()))?;
    let DataType::Struct(fields)=&self.dtype else {unreachable!()};
    let DataType::Struct(capture_fields)=fields[1].data_type() else {unreachable!()};
    let derived=Arc::new(StructArray::try_new(capture_fields.clone(),derived.columns().to_vec(),None)?) as ArrayRef;
    let result=Arc::new(StructArray::try_new(fields.clone(),vec![root,derived],None)?) as ArrayRef;
    if scalar {Ok(ColumnarValue::Scalar(datafusion_common::ScalarValue::try_from_array(&result,0)?))}
    else {Ok(ColumnarValue::Array(result))}
 }
}

/// Apply the same prepared row program to externally supplied selected rows.
pub(crate) fn selected_rows(program:&Arc<dyn PhysicalExpr>,value:ColumnarValue,rows:usize,demanded:bool)->Result<ColumnarValue> {
    if let Some(program)=program.downcast_ref::<RowCaptures>() {program.evaluate_value(value,rows,demanded)} else {Ok(value)}
}

pub(crate) fn evaluate_rows(program:&Arc<dyn PhysicalExpr>,batch:&RecordBatch,demanded:bool)->Result<ColumnarValue> {
    if let Some(program)=program.downcast_ref::<RowCaptures>() {program.evaluate_value(program.input.evaluate(batch)?,batch.num_rows(),demanded)}else{program.evaluate(batch)}
}
