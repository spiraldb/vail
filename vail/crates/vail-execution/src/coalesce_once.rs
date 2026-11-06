//! Reuse an exactly matched null-tested value without a lexical binding frame.
//! Numeric/Boolean outputs only: physical validity is logical missingness.
use crate::{conditionals::Callback, expressions::{Expr, UnaryOperator}};
use arrow_array::{Array, BooleanArray, RecordBatch, RecordBatchOptions, new_empty_array};
use arrow_schema::{DataType, Schema};
use arrow_select::zip::zip;
use datafusion_common::{Result, exec_err};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{PhysicalExpr, expressions::{CaseExpr, IsNotNullExpr}};
use std::{collections::HashMap, fmt, hash::{Hash, Hasher}, sync::Arc};

pub(crate) fn recognize<'a>(expr:&'a Expr,context:usize,memo:&mut HashMap<(usize,usize),bool>,budget:&mut usize)->Option<(&'a Arc<Expr>, &'a Arc<Expr>)> {
    let Expr::IfElse{condition,consequent,alternate}=expr else{return None;};
    let (test,defined)=match condition.as_ref(){Expr::Unary{operator:UnaryOperator::Not,value}=>(value,true),_=>(condition,false)};
    let Expr::IsMissing{value}=test.as_ref() else{return None;};
    let (returned,fallback)=if defined{(consequent,alternate)}else{(alternate,consequent)};
    if !Arc::ptr_eq(value,returned){
        if std::env::var_os("VAIL_TRACE_COALESCE_ONCE").is_some(){eprintln!("VAIL_COALESCE_CANDIDATE same_node=false");}
        return None;
    }
    fn eligible(expr:&Expr,context:usize,memo:&mut HashMap<(usize,usize),bool>,budget:&mut usize)->bool {
        let key=(expr as *const Expr as usize,context);
        if let Some(result)=memo.get(&key){return *result;}
        if *budget==0{return false;}*budget-=1;
        let mut result=crate::expression_effects::can_block_list_node(expr)
            || matches!(expr,Expr::NestedFunction{function,..}
                if function==crate::indexed_batch::INPUT || function==crate::indexed_batch::IS_NULL);
        if result{expr.for_each_child(&mut |child|result &= eligible(child,context,memo,budget));}
        memo.insert(key,result);result
    }
    let eligible=eligible(value,context,memo,budget);
    if std::env::var_os("VAIL_TRACE_COALESCE_ONCE").is_some(){eprintln!("VAIL_COALESCE_CANDIDATE same_node=true eligible={eligible}");}
    eligible.then_some((value,fallback))
}

#[derive(Debug)]
pub(crate) struct CoalesceOnce { pub first:Arc<dyn PhysicalExpr>, pub fallback:Arc<dyn PhysicalExpr>, dtype:DataType, trace:bool, pub selected_fallback:Callback }
impl CoalesceOnce {
    pub fn new(first:Arc<dyn PhysicalExpr>,fallback:Arc<dyn PhysicalExpr>,dtype:DataType,selected_fallback:Callback)->Self{Self{first,fallback,dtype,selected_fallback,trace:std::env::var_os("VAIL_TRACE_COALESCE_ONCE").is_some()}}
    // Only the source binder uses this: it substitutes validated owners without
    // changing operators, input types, or coordinate expressions. Arbitrary
    // physical child rewrites instead fall back to ordinary CASE below.
    pub fn bound(&self,first:Arc<dyn PhysicalExpr>,fallback:Arc<dyn PhysicalExpr>,selected:Arc<dyn PhysicalExpr>)->Arc<dyn PhysicalExpr>{Arc::new(Self{first,fallback,dtype:self.dtype.clone(),trace:self.trace,selected_fallback:Callback{expr:selected,indices:self.selected_fallback.indices.clone(),schema:self.selected_fallback.schema.clone()}})}
}
impl PartialEq for CoalesceOnce {fn eq(&self,other:&Self)->bool{self.first.eq(&other.first)&&self.fallback.eq(&other.fallback)&&self.dtype==other.dtype&&self.selected_fallback==other.selected_fallback}}
impl Eq for CoalesceOnce {}
impl Hash for CoalesceOnce {fn hash<H:Hasher>(&self,state:&mut H){self.first.hash(state);self.fallback.hash(state);self.dtype.hash(state);self.selected_fallback.hash(state);}}
impl fmt::Display for CoalesceOnce {fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{write!(f,"CoalesceOnce")}}
impl PhysicalExpr for CoalesceOnce {
    fn fmt_sql(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{write!(f,"CoalesceOnce")}
    fn data_type(&self,_:&Schema)->Result<DataType>{Ok(self.dtype.clone())}
    fn nullable(&self,schema:&Schema)->Result<bool>{Ok(self.first.nullable(schema)?&&self.fallback.nullable(schema)?)}
    fn children(&self)->Vec<&Arc<dyn PhysicalExpr>>{vec![&self.first,&self.fallback]}
    fn with_new_children(self:Arc<Self>,children:Vec<Arc<dyn PhysicalExpr>>)->Result<Arc<dyn PhysicalExpr>>{
        if children.len()!=2{return exec_err!("coalesce child count changed");}
        if Arc::ptr_eq(&children[0],&self.first)&&Arc::ptr_eq(&children[1],&self.fallback){return Ok(self);}
        // No purity or same-frame certificate survives arbitrary replacement.
        let condition:Arc<dyn PhysicalExpr>=Arc::new(IsNotNullExpr::new(children[0].clone()));
        Ok(Arc::new(CaseExpr::try_new(None,vec![(condition,children[0].clone())],Some(children[1].clone()))?))
    }
    fn evaluate(&self,batch:&RecordBatch)->Result<ColumnarValue>{
        let len=batch.num_rows();
        if self.trace{eprintln!("VAIL_COALESCE_EVAL rows={len}");}
        if len==0{return Ok(ColumnarValue::Array(new_empty_array(&self.dtype)));}
        match self.first.evaluate(batch)? {
            ColumnarValue::Scalar(value)=>if value.is_null(){self.fallback.evaluate(batch)}else{Ok(ColumnarValue::Scalar(value))},
            ColumnarValue::Array(first)=>{
                if first.len()!=len{return exec_err!("coalesce input cardinality differs");}
                // Admitted layouts have one validity bitmap and no dictionary
                // or nested logical-null representation to reinterpret.
                let Some(nulls)=first.nulls() else{return Ok(ColumnarValue::Array(first));};
                if nulls.null_count()==0{return Ok(ColumnarValue::Array(first));}
                if nulls.null_count()==len{return self.fallback.evaluate(batch);}
                let missing=BooleanArray::new(!nulls.inner(),None);
                // Dependency positions and their compact frame were prepared
                // once, using the same callback contract as ScopedSwitch.
                let selected=&self.selected_fallback;
                let columns=selected.indices.iter().map(|&index|batch.column(index).clone()).collect();
                let input=RecordBatch::try_new_with_options(selected.schema.clone(),columns,
                    &RecordBatchOptions::new().with_row_count(Some(len)))?;
                let fallback=selected.expr.evaluate_selection(&input,&missing)?.into_array(len)?;
                Ok(ColumnarValue::Array(zip(&missing,&fallback,&first)?))
            }
        }
    }
}
