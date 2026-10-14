//! Sequential array folds. Each callback runs against the owning row plus
//! accumulator and element bindings, without exposing those bindings as rows.
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    Array, ListArray, RecordBatch, RecordBatchOptions, UInt32Array, new_empty_array, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Schema, SchemaRef};
use arrow_select::{concat::concat, take::take};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;

#[derive(Debug)]
pub(crate) struct ArrayFoldExpr {
    args: Vec<Arc<dyn PhysicalExpr>>,
    callback_schema: SchemaRef,
    dtype: DataType,
    scan: bool,
    used_columns: Vec<bool>,
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
        let outer_count = callback_schema.fields().len() - 2;
        let mut used_columns = vec![false; outer_count];
        for column in datafusion_physical_expr::utils::collect_columns(&body) {
            if column.index() < outer_count {
                used_columns[column.index()] = true;
            }
        }
        fn captured_variables(expr: &Arc<dyn PhysicalExpr>, used: &mut [bool]) {
            if let Some(variable) =
                expr.downcast_ref::<datafusion_physical_expr::expressions::LambdaVariable>()
            {
                if variable.index() < used.len() {
                    used[variable.index()] = true;
                }
            }
            for child in expr.children() {
                captured_variables(child, used);
            }
        }
        captured_variables(&body, &mut used_columns);
        // Unused placeholders have no value buffers. Relax their nullability
        // while preserving names/types and all callback column indices.
        let fields = callback_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if index < outer_count && !used_columns[index] {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                } else {
                    field.clone()
                }
            })
            .collect::<Vec<_>>();
        let callback_schema = Arc::new(Schema::new_with_metadata(
            fields,
            callback_schema.metadata().clone(),
        ));
        Self {
            args: vec![value, zero, body],
            callback_schema,
            dtype,
            scan,
            used_columns,
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
        Ok(Arc::new(Self::new(
            args[0].clone(),
            args[1].clone(),
            args[2].clone(),
            self.callback_schema.clone(),
            self.dtype.clone(),
            self.scan,
        )))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if batch.num_columns() != self.used_columns.len() {
            return Err(DataFusionError::Execution(
                "array fold callback input schema mismatch".into(),
            ));
        }
        let len = batch.num_rows();
        let input = self.args[0].evaluate(batch)?.into_array(len)?;
        let input = input
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("array fold input is not a list".into()))?;
        let zero = self.args[1].evaluate(batch)?.into_array(len)?;
        // Each callback batch contains the rows whose arrays still have an
        // element at this position. Arrow gathers supply both captured columns
        // and elements; DataFusion evaluates the callback once for all lanes.
        let mut active = (0..len)
            .filter(|&row| input.is_valid(row) && input.value_length(row) > 0)
            .collect::<Vec<_>>();
        let indices = |rows: &[usize]| -> Result<UInt32Array> {
            rows.iter()
                .map(|&row| {
                    u32::try_from(row).map_err(|_| {
                        DataFusionError::Execution("array fold exceeds Arrow index capacity".into())
                    })
                })
                .collect::<Result<Vec<_>>>()
                .map(UInt32Array::from)
        };
        let mut accumulator = take(zero.as_ref(), &indices(&active)?, None)?;
        let mut final_values = (0..len)
            .map(|row| {
                if input.is_null(row) {
                    new_null_array(zero.data_type(), 1)
                } else {
                    zero.slice(row, 1)
                }
            })
            .collect::<Vec<_>>();
        let mut histories = if self.scan {
            (0..len)
                .map(|row| {
                    if input.is_valid(row) {
                        vec![zero.slice(row, 1)]
                    } else {
                        Vec::new()
                    }
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let mut step = 0usize;
        while !active.is_empty() {
            let owners = indices(&active)?;
            let elements = active
                .iter()
                .map(|&row| usize::try_from(input.value_offsets()[row]).unwrap() + step)
                .collect::<Vec<_>>();
            let mut columns = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    if self.used_columns[index] {
                        take(column.as_ref(), &owners, None)
                    } else {
                        Ok(new_null_array(column.data_type(), active.len()))
                    }
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            columns.push(accumulator);
            columns.push(take(input.values().as_ref(), &indices(&elements)?, None)?);
            let nested = RecordBatch::try_new_with_options(
                self.callback_schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(active.len())),
            )?;
            let updated = self.args[2].evaluate(&nested)?.into_array(active.len())?;
            let mut next_rows = Vec::new();
            let mut next_lanes = Vec::new();
            for (lane, &row) in active.iter().enumerate() {
                if self.scan {
                    histories[row].push(updated.slice(lane, 1));
                }
                if step + 1 < input.value_length(row) as usize {
                    next_rows.push(row);
                    next_lanes.push(lane);
                } else if !self.scan {
                    final_values[row] = updated.slice(lane, 1);
                }
            }
            accumulator = take(updated.as_ref(), &indices(&next_lanes)?, None)?;
            active = next_rows;
            step += 1;
        }
        let valid = (0..len).map(|row| input.is_valid(row)).collect::<Vec<_>>();
        let mut offsets = vec![0_i32];
        let values = if self.scan {
            for history in &histories {
                let length = i32::try_from(history.len()).map_err(|_| {
                    DataFusionError::Execution(
                        "array scan output exceeds Arrow list capacity".into(),
                    )
                })?;
                offsets.push(offsets.last().unwrap().checked_add(length).ok_or_else(|| {
                    DataFusionError::Execution(
                        "array scan output exceeds Arrow list capacity".into(),
                    )
                })?);
            }
            histories.into_iter().flatten().collect::<Vec<_>>()
        } else {
            final_values
        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::{Column, Literal};

    #[test]
    fn callback_projection_does_not_copy_unreferenced_nested_columns() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let schema = Arc::new(Schema::new(vec![
            Field::new("source", list_type, false),
            Field::new("capture", DataType::Int32, false),
            Field::new("acc", DataType::Int32, true),
            Field::new("item", DataType::Int32, true),
        ]));
        let expression = ArrayFoldExpr::new(
            Arc::new(Column::new("source", 0)),
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
            Arc::new(Column::new("capture", 1)),
            schema,
            DataType::Int32,
            false,
        );
        assert_eq!(expression.used_columns, vec![false, true]);
        assert!(expression.callback_schema.field(0).is_nullable());
        assert!(!expression.callback_schema.field(1).is_nullable());
    }
}
