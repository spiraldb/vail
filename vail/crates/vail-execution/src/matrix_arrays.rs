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
    Take,
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
        "__vail_list_take" => Operation::Take,
        "__vail_matrix_columns" => Operation::Columns,
        "__vail_matrix_aligned" => Operation::Aligned,
        _ => return None,
    };
    let arity = if operation == Operation::Columns {
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

fn gather(values: &ListArray, positions: &ListArray) -> Result<ArrayRef> {
    if values.len() != positions.len() {
        return exec_err!("matrix gather row counts differ");
    }
    let indices = positions
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("list positions must be int32"))?;
    if values.null_count() != 0 || positions.null_count() != 0 {
        return exec_err!("matrix gather requires defined arrays and positions");
    }
    let mut lengths = Vec::with_capacity(values.len());
    let mut offsets = Vec::new();
    for row in 0..values.len() {
        let start = values.value_offsets()[row];
        let size = values.value_offsets()[row + 1] - start;
        let lo = positions.value_offsets()[row] as usize;
        let hi = positions.value_offsets()[row + 1] as usize;
        if (lo..hi).any(|i| indices.is_null(i)) {
            return exec_err!("matrix positions must not be missing");
        }
        lengths.push(hi - lo);
        for &position in &indices.values()[lo..hi] {
            if position < 0 || position >= size {
                return exec_err!(
                    "matrix column index {position} is out of bounds for {size} columns"
                );
            }
            offsets.push((start + position) as u32);
        }
    }
    let selected = take(values.values().as_ref(), &UInt32Array::from(offsets), None)?;
    let DataType::List(field) = values.data_type() else {
        unreachable!()
    };
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        OffsetBuffer::from_lengths(lengths),
        selected,
        None,
    )?))
}

fn aligned(cells: &ListArray, columns: &ListArray) -> Result<ArrayRef> {
    if cells.len() != columns.len() {
        return exec_err!("matrix alignment row counts differ");
    }
    if cells.null_count() != 0 {
        return exec_err!("matrix entry arrays must not be missing");
    }
    let cell_ids = ids(cells)?;
    let column_ids = ids(columns)?;
    let mut left = Vec::new();
    let mut right = Vec::new();
    for row in 0..cells.len() {
        let a = cells.value_offsets()[row] as usize;
        let b = columns.value_offsets()[row] as usize;
        let count = cells.value_length(row) as usize;
        if columns.is_null(row) || count != columns.value_length(row) as usize {
            return exec_err!(
                "matrix alignment violation: entry array length differs from column count"
            );
        }
        left.extend((a..a + count).map(|v| v as u32));
        right.extend((b..b + count).map(|v| v as u32));
    }
    let left = UInt32Array::from(left);
    let records = cells
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let validity = arrow_arith::boolean::is_not_null(records)?;
    let selected_validity = take(&validity, &left, None)?;
    let selected_validity = selected_validity
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    if selected_validity.true_count() != selected_validity.len() {
        return exec_err!("matrix entry records must not be missing");
    }
    let presence = records
        .column_by_name("present")
        .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("matrix entry presence must be boolean")
        })?;
    if take(presence, &left, None)?.null_count() != 0 {
        return exec_err!("matrix entry presence must not be missing");
    }
    let a = take(cell_ids, &left, None)?;
    let b = take(column_ids, &UInt32Array::from(right), None)?;
    let equal = arrow_ord::cmp::eq(&a, &b)?;
    if equal.true_count() != equal.len() {
        return exec_err!(
            "matrix alignment violation: entry column identity differs from column at the same position"
        );
    }
    Ok(Arc::new(BooleanArray::from(vec![true; cells.len()])))
}

impl ScalarUDFImpl for MatrixArray {
    fn name(&self) -> &str {
        match self.operation {
            Operation::Take => "__vail_list_take",
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
            Operation::Take => {
                if !matches!(args.get(1), Some(DataType::List(field)) if field.data_type()==&DataType::Int32)
                {
                    return plan_err!("matrix gather requires a list of int32 positions");
                }
                Ok(args[0].clone())
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
        let arrays = args
            .args
            .iter()
            .map(|v| v.clone().into_array(count))
            .collect::<Result<Vec<_>>>()?;
        let first = list(&arrays[0])?;
        let result = match self.operation {
            Operation::Take => gather(first, list(&arrays[1])?)?,
            Operation::Columns => {
                check_columns(first)?;
                arrays[0].clone()
            }
            Operation::Aligned => aligned(first, list(&arrays[1])?)?,
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    fn records(rows: &[&[Option<i64>]], cells: bool) -> ListArray {
        let values = Int64Array::from(
            rows.iter()
                .flat_map(|r| r.iter().copied())
                .collect::<Vec<_>>(),
        );
        let len = values.len();
        let mut fields = vec![Arc::new(Field::new(CID, DataType::Int64, true))];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(values)];
        if cells {
            fields.push(Arc::new(Field::new("present", DataType::Boolean, true)));
            arrays.push(Arc::new(BooleanArray::from(vec![true; len])));
        }
        let empty = Arc::new(StructArray::new_empty_fields(len, None));
        fields.push(Arc::new(Field::new(
            if cells { "value" } else { "__vail_matrix_col" },
            empty.data_type().clone(),
            true,
        )));
        arrays.push(empty);
        let values = Arc::new(StructArray::try_new(fields.into(), arrays, None).unwrap());
        ListArray::try_new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::from_lengths(rows.iter().map(|r| r.len())),
            values,
            None,
        )
        .unwrap()
    }

    fn positions(rows: &[&[i32]]) -> ListArray {
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::from_lengths(rows.iter().map(|r| r.len())),
            Arc::new(Int32Array::from(
                rows.iter()
                    .flat_map(|r| r.iter().copied())
                    .collect::<Vec<_>>(),
            )),
            None,
        )
        .unwrap()
    }

    #[test]
    fn gathers_preserve_repetition_order_empty_structs_and_sliced_offsets() {
        let values = records(&[&[Some(99)], &[Some(7), Some(2), Some(8)], &[]], true).slice(1, 2);
        let selected = positions(&[&[99], &[2, 0, 2], &[]]).slice(1, 2);
        let out = gather(&values, &selected).unwrap();
        let out = list(&out).unwrap();
        assert_eq!(out.value_offsets(), &[0, 3, 3]);
        assert_eq!(ids(out).unwrap().values(), &[8, 7, 8]);
        let values = out.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(values.column_by_name("value").unwrap().len(), 3);
    }

    #[test]
    fn gathers_reject_invalid_positions_and_row_counts() {
        let cells = records(&[&[Some(8)]], true);
        for index in [-1, 1] {
            assert!(gather(&cells, &positions(&[&[index]])).is_err());
        }
        assert!(gather(&cells, &positions(&[])).is_err());
    }

    #[test]
    fn alignment_is_positional_and_checks_lengths_and_missing_identities() {
        let cells = records(&[&[Some(7), Some(2)]], true);
        assert!(aligned(&cells, &records(&[&[Some(7), Some(2)]], false)).is_ok());
        assert!(aligned(&cells, &records(&[&[Some(2), Some(7)]], false)).is_err());
        assert!(aligned(&cells, &records(&[&[Some(7)]], false)).is_err());
        assert!(aligned(&records(&[&[None]], true), &records(&[&[Some(7)]], false)).is_err());
        // Unused backing values outside a slice must not invalidate selected rows.
        let sliced = records(&[&[None], &[Some(7), Some(2)]], true).slice(1, 1);
        assert!(aligned(&sliced, &records(&[&[Some(7), Some(2)]], false)).is_ok());
    }

    #[test]
    fn metadata_rejects_duplicate_and_missing_occurrence_ids() {
        assert!(check_columns(&records(&[&[Some(2), Some(2)]], false)).is_err());
        assert!(check_columns(&records(&[&[None]], false)).is_err());
        assert!(check_columns(&records(&[&[Some(7), Some(2)]], false)).is_ok());
        assert!(check_columns(&records(&[&[]], false)).is_ok());
    }
}
