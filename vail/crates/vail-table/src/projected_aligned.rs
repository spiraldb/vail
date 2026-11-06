//! Produce a selected aligned entry struct without an intermediate full list.
use arrow_array::{Array,Int32Array,RecordBatch};
use arrow_schema::{DataType,Schema};
use datafusion_common::{DataFusionError,Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{PhysicalExpr,ScalarFunctionExpr,expressions::Column};
use std::{fmt,hash::{Hash,Hasher},sync::Arc};
use vail_execution::logical_expr::{AlignedSelection,SelectedAlignedProgram,restore_arrow_array_type,recognize_selected_entry_projection};

pub(crate) fn prepare(template:Arc<dyn PhysicalExpr>)->Option<Arc<dyn PhysicalExpr>> {
    let outer_template=crate::projection_broadcast::count_inner(&template);
    let outer=outer_template.downcast_ref::<ScalarFunctionExpr>()?;
    if outer.args().len()!=1{return None;}
    let inner_template=crate::projection_broadcast::count_inner(&outer.args()[0]);
    let inner=inner_template.downcast_ref::<ScalarFunctionExpr>()?;
    let proof=recognize_selected_entry_projection(inner.fun(),outer.fun())?;
    let mut arguments=inner.args().to_vec();
    let entry=crate::projection_broadcast::count_inner(arguments.get(proof.entry_argument())?);
    if !entry.is::<Column>(){return None;}
    let program=proof.prepare()?;
    arguments[program.entry_argument]=entry;
    Some(Arc::new(ProjectedProducer {template:outer_template,inner_template,arguments,program}))
}
#[derive(Clone,Debug)]
struct ProjectedProducer {
    template:Arc<dyn PhysicalExpr>,
    inner_template:Arc<dyn PhysicalExpr>,
    arguments:Vec<Arc<dyn PhysicalExpr>>,
    program:SelectedAlignedProgram,
}
impl PartialEq for ProjectedProducer {fn eq(&self,other:&Self)->bool{self.template.eq(&other.template)&&self.inner_template.eq(&other.inner_template)&&self.arguments==other.arguments}}
impl Eq for ProjectedProducer {}
impl Hash for ProjectedProducer {fn hash<H:Hasher>(&self,h:&mut H){self.template.hash(h);self.inner_template.hash(h);self.arguments.hash(h);}}
impl fmt::Display for ProjectedProducer {fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{write!(f,"ProjectedAlignedProducer")}}
impl PhysicalExpr for ProjectedProducer {
 fn fmt_sql(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{fmt::Display::fmt(self,f)}
 fn data_type(&self,_:&Schema)->Result<DataType>{Ok(self.program.output_type().clone())}
 fn nullable(&self,schema:&Schema)->Result<bool>{self.template.nullable(schema)}
 fn return_field(&self,schema:&Schema)->Result<Arc<arrow_schema::Field>>{self.template.return_field(schema)}
 fn children(&self)->Vec<&Arc<dyn PhysicalExpr>>{self.arguments.iter().collect()}
 fn with_new_children(self:Arc<Self>,children:Vec<Arc<dyn PhysicalExpr>>)->Result<Arc<dyn PhysicalExpr>> {
    if children.len()!=self.arguments.len(){return Err(DataFusionError::Internal("projected aligned argument arity changed".into()));}
    let inner=self.inner_template.clone().with_new_children(children.clone())?;
    let template=self.template.clone().with_new_children(vec![inner.clone()])?;
    if !children[self.program.entry_argument].is::<Column>() {return Ok(template);}
    Ok(Arc::new(Self {template,inner_template:inner,arguments:children,program:self.program.clone()}))
 }
 fn evaluate(&self,batch:&RecordBatch)->Result<ColumnarValue> {
    let arguments=self.arguments.iter().map(|argument|argument.evaluate(batch)).collect::<Result<Vec<_>>>()?;
    let ColumnarValue::Array(entries)=&arguments[self.program.entry_argument] else {
        return Err(DataFusionError::Internal("certified entry column returned a scalar".into()));
    };
    if entries.len()!=batch.num_rows(){return Err(DataFusionError::Execution("projected aligned entry cardinality differs".into()));}
    let count=i32::try_from(batch.num_rows()).map_err(|_|DataFusionError::Execution("projected aligned parent count exceeds int32".into()))?;
    let entries=restore_arrow_array_type(entries,self.program.entry_type())?;
    self.program.invoke(&arguments,AlignedSelection {entries,parents:Int32Array::from_iter_values(0..count)})
 }
}

pub(crate) fn is_projected(expression:&Arc<dyn PhysicalExpr>)->bool {expression.is::<ProjectedProducer>()}
