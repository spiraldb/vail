//! Parent-domain captures for a batched list map. The callback sees computed
//! invariant columns rather than replicating their original wide owners.
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, new_empty_array};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Schema};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};
#[derive(Clone, Debug)]
pub(crate) struct MapCaptures {
    pub input: Arc<dyn PhysicalExpr>,
    pub captures: Vec<Arc<dyn PhysicalExpr>>,
    pub zero: Option<Arc<dyn PhysicalExpr>>,
    pub schema: Arc<Schema>,
    pub program: Arc<dyn PhysicalExpr>,
    pub dtype: DataType,
    pub empty_list_result: bool,
}
impl PartialEq for MapCaptures {
    fn eq(&self, o: &Self) -> bool {
        self.input.eq(&o.input)
            && self.captures == o.captures
            && self.zero == o.zero
            && self.schema == o.schema
            && self.program.eq(&o.program)
            && self.dtype == o.dtype
            && self.empty_list_result == o.empty_list_result
    }
}
impl Eq for MapCaptures {}
impl Hash for MapCaptures {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.input.hash(h);
        self.captures.hash(h);
        self.zero.hash(h);
        self.schema.hash(h);
        self.program.hash(h);
        self.dtype.hash(h);
        self.empty_list_result.hash(h);
    }
}
impl fmt::Display for MapCaptures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvariantListMap")
    }
}
impl PhysicalExpr for MapCaptures {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut out = vec![&self.input];
        out.extend(self.captures.iter());
        out.extend(self.zero.iter());
        out
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 + self.captures.len() + usize::from(self.zero.is_some()) {
            return Err(DataFusionError::Internal(
                "list capture arity changed".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children[0].clone(),
            captures: children[1..1+self.captures.len()].to_vec(),
            zero: self.zero.as_ref().map(|_| children.last().unwrap().clone()),
            ..self.as_ref().clone()
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.zero.is_some() && batch.num_rows()==0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.dtype)));
        }
        let mut arrays: Vec<ArrayRef> =
            vec![self.input.evaluate(batch)?.into_array(batch.num_rows())?];
        if let Some(list) = arrays[0].as_any().downcast_ref::<ListArray>() {
            if self.empty_list_result && (0..list.len())
                .all(|i| list.is_null(i) || list.value_offsets()[i] == list.value_offsets()[i + 1])
            {
                if let DataType::List(item) = &self.dtype {
                    return Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
                        item.clone(),
                        OffsetBuffer::new(ScalarBuffer::from(vec![0; list.len() + 1])),
                        new_empty_array(item.data_type()),
                        list.nulls().cloned(),
                    )?)));
                }
            }
        }
        for capture in &self.captures {
            arrays.push(capture.evaluate(batch)?.into_array(batch.num_rows())?);
        }
        if let Some(zero) = &self.zero {
            arrays.push(zero.evaluate(batch)?.into_array(batch.num_rows())?);
        }
        let frame = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.program.evaluate(&frame)
    }
}
