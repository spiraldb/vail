//! Sequential array folds. Each callback runs against the owning row plus
//! accumulator and element bindings, without exposing those bindings as rows.
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, ListArray, RecordBatch, RecordBatchOptions, new_empty_array, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Schema, SchemaRef};
use arrow_select::concat::concat;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;

#[derive(Debug)]
pub(crate) struct ArrayFoldExpr {
    args: Vec<Arc<dyn PhysicalExpr>>,
    callback_schema: SchemaRef,
    dtype: DataType,
    scan: bool,
}

impl ArrayFoldExpr {
    pub(crate) fn new(
        value: Arc<dyn PhysicalExpr>,
        zero: Arc<dyn PhysicalExpr>,
        body: Arc<dyn PhysicalExpr>,
        callback_schema: SchemaRef,
        dtype: DataType,
        scan: bool,
    ) -> Self {
        Self {
            args: vec![value, zero, body],
            callback_schema,
            dtype,
            scan,
        }
    }
}

impl PartialEq for ArrayFoldExpr {
    fn eq(&self, other: &Self) -> bool {
        self.args == other.args
            && self.callback_schema == other.callback_schema
            && self.dtype == other.dtype
            && self.scan == other.scan
    }
}
impl Eq for ArrayFoldExpr {}
impl Hash for ArrayFoldExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dtype.hash(state);
        self.scan.hash(state);
        self.callback_schema.hash(state);
        for arg in &self.args {
            arg.hash(state);
        }
    }
}
impl std::fmt::Display for ArrayFoldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Array{}", if self.scan { "Scan" } else { "Fold" })
    }
}

impl PhysicalExpr for ArrayFoldExpr {
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        args: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if args.len() != 3 {
            return Err(DataFusionError::Internal(
                "array fold child count mismatch".into(),
            ));
        }
        Ok(Arc::new(Self {
            args,
            callback_schema: self.callback_schema.clone(),
            dtype: self.dtype.clone(),
            scan: self.scan,
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let len = batch.num_rows();
        let input = self.args[0].evaluate(batch)?.into_array(len)?;
        let input = input
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("array fold input is not a list".into()))?;
        let zero = self.args[1].evaluate(batch)?.into_array(len)?;
        let mut values: Vec<ArrayRef> = Vec::new();
        let mut offsets = vec![0_i32];
        let mut valid = Vec::with_capacity(len);
        for row in 0..len {
            if input.is_null(row) {
                if !self.scan {
                    values.push(new_null_array(&self.dtype, 1));
                }
                offsets.push(*offsets.last().unwrap());
                valid.push(false);
                continue;
            }
            valid.push(true);
            let elements = input.value(row);
            let mut accumulator = zero.slice(row, 1);
            if self.scan {
                values.push(accumulator.clone());
            }
            for element in 0..elements.len() {
                let mut columns = batch
                    .columns()
                    .iter()
                    .map(|a| a.slice(row, 1))
                    .collect::<Vec<_>>();
                columns.push(accumulator);
                columns.push(elements.slice(element, 1));
                let nested = RecordBatch::try_new_with_options(
                    self.callback_schema.clone(),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(1)),
                )?;
                accumulator = self.args[2].evaluate(&nested)?.into_array(1)?;
                if self.scan {
                    values.push(accumulator.clone());
                }
            }
            if self.scan {
                let length = i32::try_from(elements.len() + 1).map_err(|_| {
                    DataFusionError::Execution(
                        "array scan output exceeds Arrow list capacity".into(),
                    )
                })?;
                offsets.push(offsets.last().unwrap().checked_add(length).ok_or_else(|| {
                    DataFusionError::Execution(
                        "array scan output exceeds Arrow list capacity".into(),
                    )
                })?);
            } else {
                values.push(accumulator);
            }
        }
        let element_type = if self.scan {
            let DataType::List(field) = &self.dtype else {
                unreachable!()
            };
            field.data_type()
        } else {
            &self.dtype
        };
        let flat = if values.is_empty() {
            new_empty_array(element_type)
        } else {
            concat(&values.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
        };
        if !self.scan {
            return Ok(ColumnarValue::Array(flat));
        }
        let DataType::List(field) = &self.dtype else {
            unreachable!()
        };
        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(offsets.into()),
            flat,
            Some(NullBuffer::from(valid)),
        )?)))
    }
}
