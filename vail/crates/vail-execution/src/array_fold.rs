//! Sequential array folds. Each callback runs against the owning row plus
//! accumulator and element bindings, without exposing those bindings as rows.
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, ListArray, RecordBatch, RecordBatchOptions, UInt32Array, new_empty_array,
    new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Schema, SchemaRef};
use arrow_select::{concat::concat, take::take};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;

/// An immutable, validated ordered subset of a fixed row/lane domain.
/// Contiguous selections retain nested buffers; sparse selections gather once.
enum RowSelection {
    Span {
        source_len: usize,
        start: usize,
        len: usize,
    },
    Sparse {
        source_len: usize,
        indices: UInt32Array,
    },
}
impl RowSelection {
    fn checked(rows: &[usize], source_len: usize) -> Result<Self> {
        if rows.iter().any(|&row| row >= source_len)
            || rows.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(DataFusionError::Internal(
                "fold row selection is not ordered/bounded".into(),
            ));
        }
        if rows
            .windows(2)
            .all(|pair| pair[0].checked_add(1) == Some(pair[1]))
        {
            return Ok(Self::Span {
                source_len,
                start: rows.first().copied().unwrap_or(0),
                len: rows.len(),
            });
        }
        let indices = rows
            .iter()
            .map(|&row| {
                u32::try_from(row).map_err(|_| {
                    DataFusionError::Execution("fold row index exceeds Arrow capacity".into())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::Sparse {
            source_len,
            indices: UInt32Array::from(indices),
        })
    }
    fn select(&self, array: &ArrayRef) -> Result<ArrayRef> {
        let source_len = match self {
            Self::Span { source_len, .. } | Self::Sparse { source_len, .. } => *source_len,
        };
        if array.len() != source_len {
            return Err(DataFusionError::Internal(
                "fold selection/input cardinality mismatch".into(),
            ));
        }
        match self {
            Self::Span { start, len, .. } => {
                if *len == 0 {
                    Ok(new_empty_array(array.data_type()))
                } else if *start == 0 && *len == source_len {
                    Ok(array.clone())
                } else {
                    Ok(array.slice(*start, *len))
                }
            }
            Self::Sparse { indices, .. } => Ok(take(array.as_ref(), indices, None)?),
        }
    }
}

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
        fn captured_fields(
            expr: &Arc<dyn PhysicalExpr>,
            used: &mut [bool],
            seen: &mut std::collections::HashSet<usize>,
        ) {
            if !seen.insert(Arc::as_ptr(expr) as *const () as usize) {
                return;
            }
            let index = if let Some(column) =
                expr.downcast_ref::<datafusion_physical_expr::expressions::Column>()
            {
                Some(column.index())
            } else {
                expr.downcast_ref::<datafusion_physical_expr::expressions::LambdaVariable>()
                    .map(|variable| variable.index())
            };
            if let Some(index) = index.filter(|&index| index < used.len()) {
                used[index] = true;
            }
            for child in expr.children() {
                captured_fields(child, used, seen);
            }
        }
        captured_fields(&body, &mut used_columns, &mut Default::default());
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
        if len == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.dtype)));
        }
        let value = self.args[0].evaluate(batch)?;
        let scalar_input = matches!(value, ColumnarValue::Scalar(_));
        let input = match value {
            ColumnarValue::Scalar(datafusion_common::ScalarValue::List(array)) => array as ArrayRef,
            other => other.into_array(if scalar_input { 1 } else { len })?,
        };
        let state_type = self
            .callback_schema
            .field(self.used_columns.len())
            .data_type();
        let element_type = self
            .callback_schema
            .field(self.used_columns.len() + 1)
            .data_type();
        let input_type = DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            element_type.clone(),
            true,
        )));
        let normalize = |array: &ArrayRef, dtype: &DataType| -> Result<ArrayRef> {
            if array.data_type() == dtype {
                return Ok(array.clone());
            }
            // Cast offset/string representations before metadata restoration;
            // descriptor retagging alone cannot change physical offset widths.
            if vail_ordering::same_layout(array.data_type(), dtype) {
                return crate::logical_expr::restore_arrow_array_type(array, dtype);
            }
            let cast = arrow_cast::cast(array.as_ref(), dtype)?;
            crate::logical_expr::restore_arrow_array_type(&cast, dtype)
        };
        let input = normalize(&input, &input_type)?;
        let input = input
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("array fold input is not a list".into()))?;
        // Every span is bounded by the canonical child array. Scalars share
        // one span across lanes rather than expanding their element buffers.
        #[derive(Clone, Copy)]
        struct Span {
            start: usize,
            len: usize,
            valid: bool,
        }
        let span = |row| Span {
            start: input.value_offsets()[row] as usize,
            len: input.value_length(row) as usize,
            valid: input.is_valid(row),
        };
        let rows = if scalar_input {
            vec![span(0); len]
        } else {
            (0..len).map(span).collect::<Vec<_>>()
        };
        let zero = normalize(&self.args[1].evaluate(batch)?.into_array(len)?, state_type)?;
        let captures = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if self.used_columns[index] {
                    normalize(column, self.callback_schema.field(index).data_type())
                } else {
                    Ok(new_null_array(
                        self.callback_schema.field(index).data_type(),
                        len,
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        // Each callback batch contains the rows whose arrays still have an
        // element at this position. Arrow gathers supply both captured columns
        // and elements; DataFusion evaluates the callback once for all lanes.
        let mut active = (0..len)
            .filter(|&row| rows[row].valid && rows[row].len > 0)
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
        let selected = RowSelection::checked(&active, len)?;
        let mut accumulator = selected.select(&zero)?;
        let mut cached_captures = captures
            .iter()
            .map(|column| selected.select(column))
            .collect::<Result<Vec<_>>>()?;
        let mut final_values = (0..len)
            .map(|row| {
                if !rows[row].valid {
                    new_null_array(zero.data_type(), 1)
                } else {
                    zero.slice(row, 1)
                }
            })
            .collect::<Vec<_>>();
        let mut histories = if self.scan {
            (0..len)
                .map(|row| {
                    if rows[row].valid {
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
            let elements = active
                .iter()
                .map(|&row| rows[row].start + step)
                .collect::<Vec<_>>();
            let mut columns = cached_captures.clone();
            columns.push(accumulator);
            columns.push(take(input.values().as_ref(), &indices(&elements)?, None)?);
            let nested = RecordBatch::try_new_with_options(
                self.callback_schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(active.len())),
            )?;
            let updated = normalize(
                &self.args[2].evaluate(&nested)?.into_array(active.len())?,
                state_type,
            )?;
            let mut next_rows = Vec::new();
            let mut next_lanes = Vec::new();
            for (lane, &row) in active.iter().enumerate() {
                if self.scan {
                    histories[row].push(updated.slice(lane, 1));
                }
                if step + 1 < rows[row].len {
                    next_rows.push(row);
                    next_lanes.push(lane);
                } else if !self.scan {
                    final_values[row] = updated.slice(lane, 1);
                }
            }
            if next_lanes.len() == active.len() {
                accumulator = updated;
            } else {
                let survivors = RowSelection::checked(&next_lanes, active.len())?;
                accumulator = survivors.select(&updated)?;
                cached_captures = cached_captures
                    .iter()
                    .map(|column| survivors.select(column))
                    .collect::<Result<Vec<_>>>()?;
            }
            active = next_rows;
            step += 1;
        }
        let valid = (0..len).map(|row| rows[row].valid).collect::<Vec<_>>();
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
