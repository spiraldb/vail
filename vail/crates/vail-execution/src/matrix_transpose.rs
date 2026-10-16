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
use datafusion_common::{Result, exec_err, plan_err};
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
            Arc::new(
                context
                    .as_ref()
                    .clone()
                    .with_name(CONTEXT)
                    .with_nullable(true),
            ),
            Arc::new(
                item_field
                    .as_ref()
                    .clone()
                    .with_name(VALUE)
                    .with_nullable(true),
            ),
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
        for record in row_start..row_end {
            if records.is_null(record) {
                return exec_err!("matrix transpose records must not be missing");
            }
            if arrays.is_null(record) {
                continue;
            }
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
            for record in row_start..row_end {
                if arrays.is_null(record) {
                    continue;
                }
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
            Arc::new(
                context_field
                    .as_ref()
                    .clone()
                    .with_name(CONTEXT)
                    .with_nullable(true),
            ),
            Arc::new(
                item_field
                    .as_ref()
                    .clone()
                    .with_name(VALUE)
                    .with_nullable(true),
            ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};

    fn inputs(
        context: ArrayRef,
        context_field: Arc<Field>,
        value_offsets: Vec<i32>,
        value_values: ArrayRef,
        value_nulls: Option<NullBuffer>,
        row_offsets: Vec<i32>,
        row_nulls: Option<NullBuffer>,
    ) -> ListArray {
        let values = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", value_values.data_type().clone(), true)),
                OffsetBuffer::new(value_offsets.into()),
                value_values,
                value_nulls,
            )
            .unwrap(),
        );
        let records = Arc::new(
            StructArray::try_new(
                vec![
                    Arc::new(
                        context_field
                            .as_ref()
                            .clone()
                            .with_name(CONTEXT)
                            .with_nullable(true),
                    ),
                    Arc::new(Field::new(VALUE, values.data_type().clone(), true)),
                ]
                .into(),
                vec![context, values],
                None,
            )
            .unwrap(),
        );
        ListArray::try_new(
            Arc::new(Field::new("item", records.data_type().clone(), true)),
            OffsetBuffer::new(row_offsets.into()),
            records,
            row_nulls,
        )
        .unwrap()
    }

    #[test]
    fn transposed_schema_retains_context_and_item_field_metadata() {
        let metadata = std::collections::HashMap::from([("logical".to_owned(), "set".to_owned())]);
        let context = Field::new(CONTEXT, DataType::Struct(Vec::<Field>::new().into()), true)
            .with_metadata(metadata.clone());
        let item = Field::new("item", DataType::Int32, true).with_metadata(metadata.clone());
        let input = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Arc::new(context),
                    Arc::new(Field::new(VALUE, DataType::List(Arc::new(item)), true)),
                ]
                .into(),
            ),
            true,
        )));
        let DataType::List(positions) = output_type(&input).unwrap() else {
            unreachable!()
        };
        let DataType::List(records) = positions.data_type() else {
            unreachable!()
        };
        let DataType::Struct(fields) = records.data_type() else {
            unreachable!()
        };
        assert_eq!(fields[0].metadata(), &metadata);
        assert_eq!(fields[1].metadata(), &metadata);
    }

    #[test]
    fn transposes_values_and_contexts_once_per_position_with_sliced_offsets() {
        let context = Arc::new(
            StructArray::try_new(
                vec![Arc::new(Field::new("marker", DataType::Int32, true))].into(),
                vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
                None,
            )
            .unwrap(),
        );
        let source = inputs(
            context,
            Arc::new(Field::new(
                "context",
                DataType::Struct(
                    vec![Arc::new(Field::new("marker", DataType::Int32, true))].into(),
                ),
                true,
            )),
            vec![0, 2, 4, 6],
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            None,
            vec![0, 2, 3],
            None,
        );
        let out = transpose(&source.slice(1, 1)).unwrap();
        let positions = list(&out).unwrap();
        assert_eq!(positions.value_offsets(), &[0, 2]);
        let items = list(positions.values()).unwrap();
        assert_eq!(items.value_offsets(), &[0, 1, 2]);
        let records = items
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            records
                .column_by_name(VALUE)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[5, 6]
        );
        assert_eq!(
            records
                .column_by_name(CONTEXT)
                .unwrap()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column_by_name("marker")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[30, 30]
        );
    }

    #[test]
    fn skips_missing_arrays_and_supports_empty_context_structs() {
        let context = Arc::new(StructArray::new_empty_fields(2, None));
        let source = inputs(
            context,
            Arc::new(Field::new(
                "context",
                DataType::Struct(Vec::<Field>::new().into()),
                true,
            )),
            vec![0, 2, 2],
            Arc::new(Int32Array::from(vec![7, 8])),
            Some(NullBuffer::from(vec![true, false])),
            vec![0, 2],
            None,
        );
        let out = transpose(&source).unwrap();
        let positions = list(&out).unwrap();
        assert_eq!(positions.value_offsets(), &[0, 2]);
        let items = list(positions.values()).unwrap();
        assert_eq!(items.value_offsets(), &[0, 1, 2]);
        assert_eq!(items.values().len(), 2);

        let all_missing = inputs(
            Arc::new(StructArray::new_empty_fields(1, None)),
            Arc::new(Field::new(
                "context",
                DataType::Struct(Vec::<Field>::new().into()),
                true,
            )),
            vec![0, 0],
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Some(NullBuffer::from(vec![false])),
            vec![0, 1],
            None,
        );
        assert_eq!(
            list(&transpose(&all_missing).unwrap())
                .unwrap()
                .value_offsets(),
            &[0, 0]
        );
    }

    #[test]
    fn rejects_ragged_arrays_with_existing_error_shape() {
        let context = Arc::new(
            StructArray::try_new(
                vec![Arc::new(Field::new("marker", DataType::Utf8, true))].into(),
                vec![Arc::new(StringArray::from(vec!["a", "b"]))],
                None,
            )
            .unwrap(),
        );
        let source = inputs(
            context,
            Arc::new(Field::new(
                "context",
                DataType::Struct(vec![Arc::new(Field::new("marker", DataType::Utf8, true))].into()),
                true,
            )),
            vec![0, 2, 2],
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
            vec![0, 2],
            None,
        );
        assert!(
            transpose(&source)
                .unwrap_err()
                .to_string()
                .contains("array_agg requires equal array lengths, found 2 and 0")
        );
    }
}
