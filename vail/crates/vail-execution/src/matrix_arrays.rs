//! Matrix layout checks and batched list gathering using Arrow kernels.
//! DataFusion has scalar array extraction, but no gather of an index list per
//! row. Only offsets and indices are constructed here; Arrow copies the values.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, StructArray, UInt32Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use arrow_select::take::take;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

const CID: &str = "__vail_matrix_col_id";

#[derive(Debug, PartialEq, Eq, Hash)]
enum Operation {
    IndexValues,
    Take,
    TakeGroups,
    DenseCells,
    Columns,
    Aligned,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MatrixArray {
    operation: Operation,
    signature: Signature,
}

pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    let operation = match name {
        "__vail_index_values" => Operation::IndexValues,
        "__vail_list_take" => Operation::Take,
        "__vail_list_take_groups" => Operation::TakeGroups,
        "__vail_dense_cells" => Operation::DenseCells,
        "__vail_matrix_columns" => Operation::Columns,
        "__vail_matrix_aligned" => Operation::Aligned,
        _ => return None,
    };
    let arity = if matches!(operation, Operation::Columns | Operation::IndexValues) {
        1
    } else {
        2
    };
    Some(Arc::new(ScalarUDF::from(MatrixArray {
        operation,
        signature: Signature::any(arity, Volatility::Immutable),
    })))
}

fn list(array: &ArrayRef) -> Result<&ListArray> {
    array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        datafusion_common::exec_datafusion_err!("matrix operation requires an Arrow List")
    })
}

fn ids(array: &ListArray) -> Result<&Int64Array> {
    let values = array
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("matrix records must be structs"))?;
    values
        .column_by_name(CID)
        .and_then(|v| v.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("matrix column identity must be int64")
        })
}

fn check_columns(columns: &ListArray) -> Result<()> {
    let ids = ids(columns)?;
    if columns.null_count() != 0 {
        return exec_err!("matrix columns and identities must not be missing");
    }
    for row in 0..columns.len() {
        let start = columns.value_offsets()[row] as usize;
        let end = columns.value_offsets()[row + 1] as usize;
        let records = columns
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        if (start..end).any(|i| ids.is_null(i) || records.is_null(i)) {
            return exec_err!("matrix columns and identities must not be missing");
        }
        let unique: HashSet<_> = ids.values()[start..end].iter().collect();
        if unique.len() != end - start {
            return exec_err!("matrix column identities must be unique per occurrence");
        }
    }
    Ok(())
}

/// A validated batch view; scalar lists retain their single physical owner.
struct ListRows<'a> {
    values: &'a ListArray,
    scalar: bool,
    rows: usize,
}
impl<'a> ListRows<'a> {
    fn new(values: &'a ListArray, scalar: bool, rows: usize) -> Result<Self> {
        if values.len() != if scalar { 1 } else { rows } {
            return exec_err!("matrix gather row counts differ");
        }
        if values.null_count() != 0 {
            return exec_err!("matrix gather requires defined arrays and positions");
        }
        Ok(Self {
            values,
            scalar,
            rows,
        })
    }
    fn span(&self, row: usize) -> std::ops::Range<usize> {
        let owner = if self.scalar { 0 } else { row };
        self.values.value_offsets()[owner] as usize..self.values.value_offsets()[owner + 1] as usize
    }
}

fn gather(values: ListRows<'_>, positions: ListRows<'_>, grouped: bool) -> Result<ArrayRef> {
    let indices = if grouped {
        list(positions.values.values())?
    } else {
        positions.values
    };
    let indices_values = indices
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("list positions must be int32"))?;
    let mut selection = Vec::new();
    let mut owner_offsets = vec![0i32];
    let mut group_offsets = vec![0i32];
    for row in 0..values.rows {
        let source = values.span(row);
        let owners = positions.span(row);
        let append = |range: std::ops::Range<usize>, selection: &mut Vec<u32>| -> Result<()> {
            for index in range {
                if indices_values.is_null(index) {
                    return exec_err!("matrix positions must not be missing");
                }
                let position = indices_values.value(index);
                if position < 0 || position as usize >= source.len() {
                    return exec_err!(
                        "matrix column index {position} is out of bounds for {} columns",
                        source.len()
                    );
                }
                selection.push((source.start + position as usize) as u32);
            }
            Ok(())
        };
        if grouped {
            for group in owners {
                if indices.is_null(group) {
                    return exec_err!("grouped gather positions must not be missing");
                }
                append(
                    indices.value_offsets()[group] as usize
                        ..indices.value_offsets()[group + 1] as usize,
                    &mut selection,
                )?;
                group_offsets.push(i32::try_from(selection.len()).map_err(|_| {
                    datafusion_common::exec_datafusion_err!(
                        "matrix gather exceeds Arrow list capacity"
                    )
                })?);
            }
            owner_offsets.push(i32::try_from(group_offsets.len() - 1).map_err(|_| {
                datafusion_common::exec_datafusion_err!("matrix gather exceeds Arrow list capacity")
            })?);
        } else {
            append(owners, &mut selection)?;
            owner_offsets.push(i32::try_from(selection.len()).map_err(|_| {
                datafusion_common::exec_datafusion_err!("matrix gather exceeds Arrow list capacity")
            })?);
        }
    }
    let selected = take(
        values.values.values().as_ref(),
        &UInt32Array::from(selection),
        None,
    )?;
    let DataType::List(field) = values.values.data_type() else {
        unreachable!()
    };
    if grouped {
        let inner: ArrayRef = Arc::new(ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(group_offsets.into()),
            selected,
            None,
        )?);
        Ok(Arc::new(ListArray::try_new(
            Arc::new(arrow_schema::Field::new(
                "item",
                inner.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(owner_offsets.into()),
            inner,
            None,
        )?))
    } else {
        Ok(Arc::new(ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(owner_offsets.into()),
            selected,
            None,
        )?))
    }
}

fn dense_type(value_field: &arrow_schema::Field) -> DataType {
    use arrow_schema::Field;
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(
            vec![
                Arc::new(Field::new(CID, DataType::Int64, true)),
                Arc::new(Field::new("present", DataType::Boolean, true)),
                Arc::new(value_field.clone().with_name("value").with_nullable(true)),
            ]
            .into(),
        ),
        true,
    )))
}

/// Scatter known sparse column positions into a dense Arrow take index. No
/// search or sort is required; Arrow supplies values and missing output slots.
fn dense_cells(entries: &ListArray, widths: &Int32Array) -> Result<ArrayRef> {
    if entries.len() != widths.len() || entries.null_count() != 0 || widths.null_count() != 0 {
        return exec_err!("dense cells require defined records and matching column counts");
    }
    let lengths = widths
        .values()
        .iter()
        .map(|&n| {
            usize::try_from(n).map_err(|_| {
                datafusion_common::exec_datafusion_err!("column count must not be negative")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let total = lengths
        .iter()
        .try_fold(0usize, |n, &width| n.checked_add(width))
        .filter(|&n| n <= i32::MAX as usize)
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("dense cell output exceeds Arrow list capacity")
        })?;
    let identities = ids(entries)?;
    let records = entries
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let values = records
        .column_by_name("value")
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("sparse entries require values"))?;
    let mut indices = vec![None; total];
    let mut output_ids = Vec::with_capacity(total);
    let mut offset = 0;
    for (row, &width) in lengths.iter().enumerate() {
        let start = entries.value_offsets()[row] as usize;
        let end = entries.value_offsets()[row + 1] as usize;
        for entry in start..end {
            let id = identities.value(entry);
            if records.is_null(entry) || identities.is_null(entry) || id < 0 || id >= width as i64 {
                return exec_err!("sparse entry positions must be defined and within column count");
            }
            let target = offset + id as usize;
            if indices[target].replace(entry as u32).is_some() {
                return exec_err!("sparse entry positions must be unique");
            }
        }
        output_ids.extend((0..width).map(|column| column as i64));
        offset += width;
    }
    let presence = indices.iter().map(Option::is_some).collect::<Vec<_>>();
    let selected = take(values.as_ref(), &UInt32Array::from(indices), None)?;
    let DataType::List(field) = dense_type(
        records
            .fields()
            .iter()
            .find(|field| field.name() == "value")
            .unwrap(),
    ) else {
        unreachable!()
    };
    let DataType::Struct(fields) = field.data_type() else {
        unreachable!()
    };
    let records = Arc::new(StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(Int64Array::from(output_ids)),
            Arc::new(BooleanArray::from(presence)),
            selected,
        ],
        None,
    )?);
    Ok(Arc::new(ListArray::try_new(
        field,
        OffsetBuffer::from_lengths(lengths),
        records,
        None,
    )?))
}

fn aligned(cells: &ListArray, columns: &ListArray, scalar_columns: bool) -> Result<ArrayRef> {
    if columns.len() != if scalar_columns { 1 } else { cells.len() } {
        return exec_err!("matrix alignment row counts differ");
    }
    if cells.null_count() != 0 {
        return exec_err!("matrix entry arrays must not be missing");
    }
    let cell_ids = ids(cells)?;
    let column_ids = ids(columns)?;
    let records = cells
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let presence = records
        .column_by_name("present")
        .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("matrix entry presence must be boolean")
        })?;
    // The usual matrix representation has no validity buffers on its lists,
    // entry structs, presence bits, or identity columns. In that case Arrow's
    // primitive value slices can check each row's complete identity span with
    // a single slice comparison instead of repeated per-value null checks.
    // Keep the element-wise path for nullable arrays so malformed nulls retain
    // their existing errors.
    let all_valid = cells.nulls().is_none()
        && columns.nulls().is_none()
        && records.nulls().is_none()
        && presence.nulls().is_none()
        && cell_ids.nulls().is_none()
        && column_ids.nulls().is_none();
    for row in 0..cells.len() {
        let column_row = if scalar_columns { 0 } else { row };
        let a = cells.value_offsets()[row] as usize;
        let b = columns.value_offsets()[column_row] as usize;
        let count = cells.value_length(row) as usize;
        if (!all_valid && columns.is_null(column_row))
            || count != columns.value_length(column_row) as usize
        {
            return exec_err!(
                "matrix alignment violation: entry array length differs from column count"
            );
        }
        if all_valid {
            if cell_ids.values()[a..a + count] != column_ids.values()[b..b + count] {
                return exec_err!(
                    "matrix alignment violation: entry column identity differs from column at the same position"
                );
            }
            continue;
        }
        for position in 0..count {
            let left = a + position;
            let right = b + position;
            if records.is_null(left) {
                return exec_err!("matrix entry records must not be missing");
            }
            if presence.is_null(left) {
                return exec_err!("matrix entry presence must not be missing");
            }
            if cell_ids.is_null(left)
                || column_ids.is_null(right)
                || cell_ids.value(left) != column_ids.value(right)
            {
                return exec_err!(
                    "matrix alignment violation: entry column identity differs from column at the same position"
                );
            }
        }
    }
    Ok(Arc::new(BooleanArray::from(vec![true; cells.len()])))
}

fn index_values(values: &ListArray, dtype: &DataType) -> Result<ArrayRef> {
    let DataType::List(item) = dtype else {
        return exec_err!("indexed values require array output");
    };
    let DataType::Struct(fields) = item.data_type() else {
        return exec_err!("indexed values require tuple output");
    };
    let first = values.value_offsets()[0] as usize;
    let last = values.value_offsets()[values.len()] as usize;
    let child = values.values().slice(first, last - first);
    let mut indices = vec![0; last - first];
    for row in 0..values.len() {
        let start = values.value_offsets()[row] as usize - first;
        let end = values.value_offsets()[row + 1] as usize - first;
        for (index, slot) in indices[start..end].iter_mut().enumerate() {
            *slot = i32::try_from(index).map_err(|_| {
                datafusion_common::exec_datafusion_err!("collection exceeds int32 index capacity")
            })?;
        }
    }
    let pairs = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(indices)), child],
        None,
    )?;
    Ok(Arc::new(ListArray::try_new(
        item.clone(),
        OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(
            values
                .value_offsets()
                .iter()
                .map(|v| *v - first as i32)
                .collect::<Vec<_>>(),
        )),
        Arc::new(pairs),
        values.nulls().cloned(),
    )?))
}

impl ScalarUDFImpl for MatrixArray {
    fn name(&self) -> &str {
        match self.operation {
            Operation::IndexValues => "__vail_index_values",
            Operation::Take => "__vail_list_take",
            Operation::TakeGroups => "__vail_list_take_groups",
            Operation::DenseCells => "__vail_dense_cells",
            Operation::Columns => "__vail_matrix_columns",
            Operation::Aligned => "__vail_matrix_aligned",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !matches!(args.first(), Some(DataType::List(_))) {
            return plan_err!("{} requires lists", self.name());
        }
        match self.operation {
            Operation::IndexValues => {
                let DataType::List(item) = &args[0] else {
                    unreachable!()
                };
                let pair = crate::expressions::Type::Tuple {
                    tuple: vec![
                        crate::expressions::Type::Primitive(
                            crate::expressions::PrimitiveType::Int32,
                        ),
                        crate::expressions::Type::from_arrow(item.data_type())
                            .map_err(|e| datafusion_common::DataFusionError::Plan(e.to_string()))?,
                    ],
                };
                Ok(DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    vail_ordering::tag_type(
                        DataType::Struct(
                            vec![
                                Arc::new(arrow_schema::Field::new(
                                    "__vail_tuple_0",
                                    DataType::Int32,
                                    true,
                                )),
                                Arc::new(item.as_ref().clone().with_name("__vail_tuple_1")),
                            ]
                            .into(),
                        ),
                        &serde_json::to_value(pair)
                            .map_err(|e| datafusion_common::DataFusionError::Plan(e.to_string()))?,
                    )
                    .map_err(|e| datafusion_common::DataFusionError::Plan(e.to_string()))?,
                    true,
                ))))
            }
            Operation::Take => {
                if !matches!(args.get(1), Some(DataType::List(field)) if field.data_type()==&DataType::Int32)
                {
                    return plan_err!("matrix gather requires a list of int32 positions");
                }
                Ok(args[0].clone())
            }
            Operation::TakeGroups => {
                if !matches!(args.get(1), Some(DataType::List(group)) if
                    matches!(group.data_type(), DataType::List(index) if index.data_type() == &DataType::Int32))
                {
                    return plan_err!("grouped gather requires lists of int32 position lists");
                }
                Ok(DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    args[0].clone(),
                    true,
                ))))
            }
            Operation::DenseCells => {
                if args.get(1) != Some(&DataType::Int32) {
                    return plan_err!("dense cells require an int32 column count");
                }
                let DataType::List(field) = &args[0] else {
                    unreachable!()
                };
                let DataType::Struct(fields) = field.data_type() else {
                    return plan_err!("dense cells require sparse entry records");
                };
                let value = fields.iter().find(|f| f.name() == "value").ok_or_else(|| {
                    datafusion_common::plan_datafusion_err!("sparse entries require a value field")
                })?;
                Ok(dense_type(value))
            }
            Operation::Columns => Ok(args[0].clone()),
            Operation::Aligned => {
                if !matches!(args.get(1), Some(DataType::List(_))) {
                    return plan_err!("matrix alignment requires column records");
                }
                Ok(DataType::Boolean)
            }
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)));
        let count = if scalar { 1 } else { args.number_rows };
        // No owner exists in an empty array batch. Preserve the established
        // empty-output policy without inspecting unused scalar list values.
        if count == 0 {
            return Ok(ColumnarValue::Array(arrow_array::new_empty_array(
                args.return_type(),
            )));
        }
        // Shared global columns stay scalar: validation borrows one descriptor
        // and compares each owning cell span directly, without broadcast/takes.
        if self.operation == Operation::Aligned {
            let cells = args.args[0].clone().into_array(count)?;
            let scalar_columns = matches!(&args.args[1], ColumnarValue::Scalar(_));
            let columns =
                args.args[1]
                    .clone()
                    .into_array(if scalar_columns { 1 } else { count })?;
            let result = aligned(list(&cells)?, list(&columns)?, scalar_columns)?;
            return if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            };
        }
        if matches!(self.operation, Operation::Take | Operation::TakeGroups) {
            let first_scalar = matches!(&args.args[0], ColumnarValue::Scalar(_));
            let second_scalar = matches!(&args.args[1], ColumnarValue::Scalar(_));
            let first = args.args[0]
                .clone()
                .into_array(if first_scalar { 1 } else { count })?;
            let second = args.args[1]
                .clone()
                .into_array(if second_scalar { 1 } else { count })?;
            let result = gather(
                ListRows::new(list(&first)?, first_scalar, count)?,
                ListRows::new(list(&second)?, second_scalar, count)?,
                self.operation == Operation::TakeGroups,
            )?;
            return if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            } else {
                Ok(ColumnarValue::Array(result))
            };
        }
        let arrays = args
            .args
            .iter()
            .map(|v| v.clone().into_array(count))
            .collect::<Result<Vec<_>>>()?;
        let first = list(&arrays[0])?;
        let result = match self.operation {
            Operation::IndexValues => index_values(first, &args.return_type())?,
            Operation::Take => unreachable!("handled without broadcasting above"),
            Operation::TakeGroups => unreachable!("handled without broadcasting above"),
            Operation::DenseCells => dense_cells(
                first,
                arrays[1]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!("dense cell widths must be int32")
                    })?,
            )?,
            Operation::Columns => {
                check_columns(first)?;
                arrays[0].clone()
            }
            Operation::Aligned => unreachable!("handled without broadcasting above"),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(
                crate::expressions::scalar_at(&result, 0).map_err(|error| {
                    datafusion_common::DataFusionError::Execution(error.to_string())
                })?,
            ))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}
