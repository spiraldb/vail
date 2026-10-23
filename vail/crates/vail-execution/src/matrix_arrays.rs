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
    let column_nulls = columns.logical_nulls();
    let identity_nulls = ids.logical_nulls();
    let records = columns
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let record_nulls = records.logical_nulls();
    if column_nulls
        .as_ref()
        .is_some_and(|nulls| nulls.null_count() != 0)
    {
        return exec_err!("matrix columns and identities must not be missing");
    }
    for row in 0..columns.len() {
        let start = columns.value_offsets()[row] as usize;
        let end = columns.value_offsets()[row + 1] as usize;
        if (start..end).any(|i| {
            identity_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(i))
                || record_nulls.as_ref().is_some_and(|nulls| nulls.is_null(i))
        }) {
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
        let nulls = values.logical_nulls();
        if nulls.as_ref().is_some_and(|nulls| nulls.null_count() != 0) {
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

fn take_offset(length: usize) -> Result<i32> {
    i32::try_from(length).map_err(|_| {
        datafusion_common::exec_datafusion_err!("matrix gather exceeds Arrow list capacity")
    })
}
/// Visit the same validated selection for payload gathers and validity counts.
fn visit_take(
    values: &ListRows<'_>,
    positions: &ListRows<'_>,
    mut selected: impl FnMut(usize),
) -> Result<Vec<i32>> {
    let indices = positions
        .values
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("list positions must be int32"))?;
    let index_nulls = indices.logical_nulls();
    let mut offsets = vec![0];
    let mut length = 0usize;
    for row in 0..values.rows {
        let source = values.span(row);
        let owner = positions.span(row);
        let width = owner.len();
        for index in owner {
            if index_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(index))
            {
                return exec_err!("matrix positions must not be missing");
            }
            let position = indices.value(index);
            if position < 0 || position as usize >= source.len() {
                return exec_err!(
                    "matrix column index {position} is out of bounds for {} columns",
                    source.len()
                );
            }
            selected(source.start + position as usize);
        }
        length = length.checked_add(width).ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("matrix gather exceeds Arrow list capacity")
        })?;
        offsets.push(take_offset(length)?);
    }
    Ok(offsets)
}
/// Count a positional gather's selected validity without taking payload arrays.
/// Owning values and position expressions have already executed in normal order.
pub(crate) fn count_take(
    values: ColumnarValue,
    positions: ColumnarValue,
    rows: usize,
    fields: &[String],
) -> Result<usize> {
    let first_scalar = matches!(&values, ColumnarValue::Scalar(_));
    let second_scalar = matches!(&positions, ColumnarValue::Scalar(_));
    let scalar = first_scalar && second_scalar;
    let count = if scalar { 1 } else { rows };
    if count == 0 {
        return Ok(0);
    }
    let first = values.into_array(if first_scalar { 1 } else { count })?;
    let second = positions.into_array(if second_scalar { 1 } else { count })?;
    let values = ListRows::new(list(&first)?, first_scalar, count)?;
    let positions = ListRows::new(list(&second)?, second_scalar, count)?;
    let mut levels = vec![values.values.values().clone()];
    for name in fields {
        let parent = levels
            .last()
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::exec_datafusion_err!("gather count field requires struct")
            })?;
        let child = parent
            .column_by_name(name)
            .ok_or_else(|| datafusion_common::exec_datafusion_err!("gather count field absent"))?
            .clone();
        levels.push(child);
    }
    let nullable = levels
        .iter()
        .filter_map(|array| array.logical_nulls())
        .filter(|nulls| nulls.null_count() != 0)
        .collect::<Vec<_>>();
    let mut present = 0usize;
    visit_take(&values, &positions, |index| {
        present += usize::from(nullable.iter().all(|nulls| nulls.is_valid(index)));
    })?;
    present
        .checked_mul(if scalar { rows } else { 1 })
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("gather count exceeds usize capacity")
        })
}

/// A shared identity permutation is common after sorting already-ordered
/// column metadata. Check the small index owner once, and retain the original
/// Arrow list buffers instead of copying every entry payload.
fn identity_gather(values: &ListRows<'_>, positions: &ListRows<'_>) -> Result<Option<ArrayRef>> {
    if values.scalar && values.rows != 1 {
        return Ok(None);
    }
    if values.rows == 0 {
        return Ok(Some(Arc::new(values.values.clone())));
    }
    let indices = positions
        .values
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("list positions must be int32"))?;
    let nulls = indices.logical_nulls();
    let owners = if positions.scalar { 1 } else { values.rows };
    for row in 0..owners {
        let owner = positions.span(row);
        for (position, index) in owner.enumerate() {
            if nulls.as_ref().is_some_and(|nulls| !nulls.is_valid(index))
                || indices.value(index) as i64 != position as i64
            {
                return Ok(None);
            }
        }
    }
    for row in 0..values.rows {
        if values.span(row).len() != positions.span(row).len() {
            return Ok(None);
        }
    }
    Ok(Some(Arc::new(values.values.clone())))
}
fn gather(values: ListRows<'_>, positions: ListRows<'_>, grouped: bool) -> Result<ArrayRef> {
    if !grouped {
        if let Some(identity) = identity_gather(&values, &positions)? {
            return Ok(identity);
        }
        let mut selection = Vec::new();
        let offsets = visit_take(&values, &positions, |index| selection.push(index as u32))?;
        let selected = take(
            values.values.values().as_ref(),
            &UInt32Array::from(selection),
            None,
        )?;
        let DataType::List(field) = values.values.data_type() else {
            unreachable!()
        };
        return Ok(Arc::new(ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(offsets.into()),
            selected,
            None,
        )?));
    }
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
    let index_nulls = indices_values.logical_nulls();
    let group_nulls = indices.logical_nulls();
    let mut selection = Vec::new();
    let mut owner_offsets = vec![0i32];
    let mut group_offsets = vec![0i32];
    for row in 0..values.rows {
        let source = values.span(row);
        let owners = positions.span(row);
        let append = |range: std::ops::Range<usize>, selection: &mut Vec<u32>| -> Result<()> {
            for index in range {
                if index_nulls
                    .as_ref()
                    .is_some_and(|nulls| nulls.is_null(index))
                {
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
                if group_nulls
                    .as_ref()
                    .is_some_and(|nulls| nulls.is_null(group))
                {
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
            vec![Arc::new(
                value_field.clone().with_name("value").with_nullable(true),
            )]
            .into(),
        ),
        true,
    )))
}

/// Scatter known sparse column positions into a dense Arrow take index. No
/// search or sort is required; Arrow supplies values and missing output slots.
fn dense_cells(entries: &ListArray, widths: &Int32Array) -> Result<ArrayRef> {
    let entry_nulls = entries.logical_nulls();
    let width_nulls = widths.logical_nulls();
    if entries.len() != widths.len()
        || entry_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.null_count() != 0)
        || width_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.null_count() != 0)
    {
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
    let record_nulls = records.logical_nulls();
    let identity_nulls = identities.logical_nulls();
    let mut indices = vec![None; total];
    let mut offset = 0;
    for (row, &width) in lengths.iter().enumerate() {
        let start = entries.value_offsets()[row] as usize;
        let end = entries.value_offsets()[row + 1] as usize;
        for entry in start..end {
            let id = identities.value(entry);
            if record_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(entry))
                || identity_nulls
                    .as_ref()
                    .is_some_and(|nulls| nulls.is_null(entry))
                || id < 0
                || id >= width as i64
            {
                return exec_err!("sparse entry positions must be defined and within column count");
            }
            let target = offset + id as usize;
            if indices[target].replace(entry as u32).is_some() {
                return exec_err!("sparse entry positions must be unique");
            }
        }
        offset += width;
    }
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
    let records = Arc::new(StructArray::try_new(fields.clone(), vec![selected], None)?);
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
    let cell_nulls = cells.logical_nulls();
    if cell_nulls
        .as_ref()
        .is_some_and(|nulls| nulls.null_count() != 0)
    {
        return exec_err!("matrix entry arrays must not be missing");
    }
    // Column identity belongs to the shared descriptor. Dense slots are
    // aligned by position and never repeat a column ID for each entry.
    check_columns(columns)?;
    let records = cells
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("matrix entry slots must be structs")
        })?;
    if records.column_by_name("value").is_none() {
        return exec_err!("matrix entry slots require a value field");
    }
    let record_nulls = records.logical_nulls();
    let all_valid = record_nulls
        .as_ref()
        .is_none_or(|nulls| nulls.null_count() == 0);
    for row in 0..cells.len() {
        let column_row = if scalar_columns { 0 } else { row };
        let count = cells.value_length(row) as usize;
        if count != columns.value_length(column_row) as usize {
            return exec_err!(
                "matrix alignment violation: entry array length differs from column count"
            );
        }
        if !all_valid {
            let start = cells.value_offsets()[row] as usize;
            if (start..start + count).any(|position| {
                record_nulls
                    .as_ref()
                    .is_some_and(|nulls| nulls.is_null(position))
            }) {
                return exec_err!("matrix entry records must not be missing");
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
        // The indexed list preserves the input's semantic missingness.
        values.logical_nulls(),
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
