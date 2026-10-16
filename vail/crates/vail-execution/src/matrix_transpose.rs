//! Batched transposition for per-row arrays used by matrix agg.array_agg.
//!
//! The adapter only constructs list offsets and Arrow take indices. Arrow
//! performs the context and value copying, so each source value array is
//! traversed once and each transposed item is emitted once.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, StructArray, UInt32Array};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use arrow_select::take::take;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

const CONTEXT: &str = "context";
const VALUE: &str = "value";

#[derive(Debug, PartialEq, Eq, Hash)]
struct MatrixTranspose {
    signature: Signature,
}

pub(crate) fn udf(name: &str) -> Option<Arc<ScalarUDF>> {
    (name == "__vail_matrix_transpose").then(|| {
        Arc::new(ScalarUDF::from(MatrixTranspose {
            signature: Signature::any(1, Volatility::Immutable),
        }))
    })
}

fn list(array: &ArrayRef) -> Result<&ListArray> {
    array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        datafusion_common::exec_datafusion_err!("matrix transpose requires Arrow lists")
    })
}

fn output_type(input: &DataType) -> Result<DataType> {
    let DataType::List(records_field) = input else {
        return plan_err!("matrix transpose requires a list of records");
    };
    let DataType::Struct(fields) = records_field.data_type() else {
        return plan_err!("matrix transpose requires records with context and value fields");
    };
    let context = fields
        .iter()
        .find(|field| field.name() == CONTEXT)
        .ok_or_else(|| {
            datafusion_common::plan_datafusion_err!("matrix transpose is missing context")
        })?;
    if !matches!(context.data_type(), DataType::Struct(_)) {
        return plan_err!("matrix transpose context must be a struct");
    }
    let value = fields
        .iter()
        .find(|field| field.name() == VALUE)
        .ok_or_else(|| {
            datafusion_common::plan_datafusion_err!("matrix transpose is missing values")
        })?;
    let DataType::List(item_field) = value.data_type() else {
        return plan_err!("matrix transpose values must be lists");
    };
    let transposed_item = DataType::Struct(
        vec![
            Arc::new(Field::new(CONTEXT, context.data_type().clone(), true)),
            Arc::new(Field::new(VALUE, item_field.data_type().clone(), true)),
        ]
        .into(),
    );
    let positions = DataType::List(Arc::new(Field::new("item", transposed_item, true)));
    Ok(DataType::List(Arc::new(Field::new(
        "position", positions, true,
    ))))
}

fn transpose(input: &ListArray) -> Result<ArrayRef> {
    let DataType::List(records_field) = input.data_type() else {
        unreachable!()
    };
    let DataType::Struct(record_fields) = records_field.data_type() else {
        unreachable!()
    };
    let records = input
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("matrix transpose records must be structs")
        })?;
    let context = records.column_by_name(CONTEXT).ok_or_else(|| {
        datafusion_common::exec_datafusion_err!("matrix transpose is missing context")
    })?;
    let arrays = list(records.column_by_name(VALUE).ok_or_else(|| {
        datafusion_common::exec_datafusion_err!("matrix transpose is missing values")
    })?)?;
    let context_field = record_fields
        .iter()
        .find(|field| field.name() == CONTEXT)
        .ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("matrix transpose is missing context")
        })?;
    let item_field = match arrays.data_type() {
        DataType::List(field) => field,
        _ => unreachable!(),
    };

    let mut outer_offsets = vec![0_i32];
    let mut inner_offsets = vec![0_i32];
    let mut context_indices = Vec::<u32>::new();
    let mut value_indices = Vec::<u32>::new();
    let mut outer_validity = Vec::with_capacity(input.len());

    for row in 0..input.len() {
        if input.is_null(row) {
            outer_validity.push(false);
            outer_offsets.push(*outer_offsets.last().unwrap());
            continue;
        }
        outer_validity.push(true);
        let row_start = input.value_offsets()[row] as usize;
        let row_end = input.value_offsets()[row + 1] as usize;
        let mut width = None;
        let mut defined = Vec::new();
        for record in row_start..row_end {
            if records.is_null(record) {
                return exec_err!("matrix transpose records must not be missing");
            }
            if arrays.is_null(record) {
                continue;
            }
            defined.push(record);
            let len = arrays.value_length(record) as usize;
            if let Some(previous) = width {
                if previous != len {
                    return exec_err!(
                        "array_agg requires equal array lengths, found {previous} and {len}"
                    );
                }
            } else {
                width = Some(len);
            }
        }
        let width = width.unwrap_or(0);
        if outer_offsets.last().copied().unwrap() as usize + width > i32::MAX as usize {
            return exec_err!("matrix transpose output exceeds Arrow list capacity");
        }
        for position in 0..width {
            for &record in &defined {
                let value_index = arrays.value_offsets()[record] as usize + position;
                context_indices.push(u32::try_from(record).map_err(|_| {
                    datafusion_common::exec_datafusion_err!(
                        "matrix transpose context index exceeds Arrow take capacity"
                    )
                })?);
                value_indices.push(u32::try_from(value_index).map_err(|_| {
                    datafusion_common::exec_datafusion_err!(
                        "matrix transpose value index exceeds Arrow take capacity"
                    )
                })?);
            }
            let next = i32::try_from(context_indices.len()).map_err(|_| {
                datafusion_common::exec_datafusion_err!(
                    "matrix transpose output exceeds Arrow list capacity"
                )
            })?;
            inner_offsets.push(next);
        }
        outer_offsets.push(outer_offsets.last().unwrap() + width as i32);
    }

    let contexts = take(context.as_ref(), &UInt32Array::from(context_indices), None)?;
    let values = take(
        arrays.values().as_ref(),
        &UInt32Array::from(value_indices),
        None,
    )?;
    let item_values = Arc::new(StructArray::try_new(
        vec![
            Arc::new(Field::new(CONTEXT, context_field.data_type().clone(), true)),
            Arc::new(Field::new(VALUE, item_field.data_type().clone(), true)),
        ]
        .into(),
        vec![contexts, values],
        None,
    )?);
    let positions = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", item_values.data_type().clone(), true)),
        OffsetBuffer::new(inner_offsets.into()),
        item_values,
        None,
    )?);
    let nulls = outer_validity
        .iter()
        .any(|valid| !valid)
        .then(|| NullBuffer::from(outer_validity));
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("position", positions.data_type().clone(), true)),
        OffsetBuffer::new(outer_offsets.into()),
        positions,
        nulls,
    )?))
}

impl ScalarUDFImpl for MatrixTranspose {
    fn name(&self) -> &str {
        "__vail_matrix_transpose"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        output_type(args.first().ok_or_else(|| {
            datafusion_common::plan_datafusion_err!("matrix transpose requires one list")
        })?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let count = if scalar { 1 } else { args.number_rows };
        let input = args.args[0].clone().into_array(count)?;
        let result = transpose(list(&input)?)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}
