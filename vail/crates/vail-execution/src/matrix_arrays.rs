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

/// Gather all groups in one Arrow take, without repeating the source list for
/// each group. The outer offsets identify owners; inner offsets identify groups.
fn gather_groups(values: &ListArray, groups: &ListArray) -> Result<ArrayRef> {
    if values.len() != groups.len() || groups.null_count() != 0 {
        return exec_err!("grouped gather requires defined groups with matching row counts");
    }
    let positions = list(groups.values())?;
    let DataType::List(index_field) = positions.data_type() else {
        unreachable!()
    };
    let mut flat_offsets = Vec::with_capacity(groups.len() + 1);
    let mut group_lengths = Vec::new();
    let mut owner_lengths = Vec::with_capacity(groups.len());
    for row in 0..groups.len() {
        let start = groups.value_offsets()[row] as usize;
        let end = groups.value_offsets()[row + 1] as usize;
        flat_offsets.push(positions.value_offsets()[start]);
        owner_lengths.push(end - start);
        for group in start..end {
            if positions.is_null(group) {
                return exec_err!("grouped gather positions must not be missing");
            }
            group_lengths.push(positions.value_length(group) as usize);
        }
    }
    let end = *groups.value_offsets().last().unwrap() as usize;
    flat_offsets.push(positions.value_offsets()[end]);
    let flat = ListArray::try_new(
        index_field.clone(),
        OffsetBuffer::new(flat_offsets.into()),
        positions.values().clone(),
        None,
    )?;
    let selected = gather(values, &flat)?;
    let selected = list(&selected)?;
    let DataType::List(value_field) = values.data_type() else {
        unreachable!()
    };
    let inner = Arc::new(ListArray::try_new(
        value_field.clone(),
        OffsetBuffer::from_lengths(group_lengths),
        selected.values().clone(),
        None,
    )?);
    Ok(Arc::new(ListArray::try_new(
        Arc::new(arrow_schema::Field::new(
            "item",
            inner.data_type().clone(),
            true,
        )),
        OffsetBuffer::from_lengths(owner_lengths),
        inner,
        None,
    )?))
}

fn dense_type(value_type: &DataType) -> DataType {
    use arrow_schema::Field;
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(
            vec![
                Arc::new(Field::new(CID, DataType::Int64, true)),
                Arc::new(Field::new("present", DataType::Boolean, true)),
                Arc::new(Field::new("value", value_type.clone(), true)),
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
    let DataType::List(field) = dense_type(values.data_type()) else {
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
                Ok(dense_type(value.data_type()))
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
            Operation::IndexValues => index_values(first, &args.return_type())?,
            Operation::Take => gather(first, list(&arrays[1])?)?,
            Operation::TakeGroups => gather_groups(first, list(&arrays[1])?)?,
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
    fn grouped_gathers_preserve_empty_groups_repetition_and_unused_prefixes() {
        let values = records(&[&[Some(7), Some(2), Some(8)], &[Some(4)], &[]], true);
        let positions = positions(&[&[99], &[2, 0], &[], &[1, 2], &[0], &[]]);
        let groups = ListArray::try_new(
            Arc::new(Field::new("item", positions.data_type().clone(), true)),
            OffsetBuffer::new(vec![1, 4, 5, 6].into()),
            Arc::new(positions),
            None,
        )
        .unwrap();
        let out = gather_groups(&values, &groups).unwrap();
        let outer = list(&out).unwrap();
        assert_eq!(outer.value_offsets(), &[0, 3, 4, 5]);
        let inner = list(outer.values()).unwrap();
        assert_eq!(inner.value_offsets(), &[0, 2, 2, 4, 5, 5]);
        assert_eq!(ids(inner).unwrap().values(), &[8, 7, 2, 8, 4]);
        let sliced = gather_groups(&values.slice(1, 2), &groups.slice(1, 2)).unwrap();
        let inner = list(list(&sliced).unwrap().values()).unwrap();
        assert_eq!(ids(inner).unwrap().values(), &[4]);
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
    fn dense_scatter_preserves_unsorted_values_and_missing_slots() {
        let sparse = records(&[&[Some(2), Some(0)], &[Some(1)], &[]], true);
        let old = sparse
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let payload = StructArray::try_new(
            vec![Arc::new(Field::new("marker", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![102, 100, 201]))],
            None,
        )
        .unwrap();
        let records = Arc::new(
            StructArray::try_new(
                vec![
                    old.fields()[0].clone(),
                    Arc::new(Field::new("value", payload.data_type().clone(), true)),
                ]
                .into(),
                vec![old.column(0).clone(), Arc::new(payload)],
                None,
            )
            .unwrap(),
        );
        let sparse = ListArray::try_new(
            Arc::new(Field::new("item", records.data_type().clone(), true)),
            sparse.offsets().clone(),
            records,
            None,
        )
        .unwrap();
        let out = dense_cells(&sparse, &Int32Array::from(vec![3, 2, 0])).unwrap();
        let out = list(&out).unwrap();
        assert_eq!(out.value_offsets(), &[0, 3, 5, 5]);
        let values = out.values().as_any().downcast_ref::<StructArray>().unwrap();
        let present = values
            .column_by_name("present")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(
            present.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(true), Some(false), Some(true)]
        );
        let payload = values
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(payload.is_null(1) && payload.is_null(3));
        let markers = payload
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (markers.value(0), markers.value(2), markers.value(4)),
            (100, 102, 201)
        );
    }

    #[test]
    fn dense_scatter_rejects_duplicate_and_invalid_positions() {
        for row in [
            &[Some(0), Some(0)][..],
            &[Some(-1)][..],
            &[Some(2)][..],
            &[None][..],
        ] {
            assert!(dense_cells(&records(&[row], true), &Int32Array::from(vec![2])).is_err());
        }
        assert!(dense_cells(&records(&[&[]], true), &Int32Array::from(vec![-1])).is_err());
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
