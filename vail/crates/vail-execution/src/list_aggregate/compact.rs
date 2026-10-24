//! Establish the outer-list ownership boundary before a batch library kernel.
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray};
use arrow_buffer::OffsetBuffer;
use arrow_data::transform::MutableArrayData;
use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// A ListArray whose null parents own no child values. Its first-to-last offset
/// envelope therefore contains only defined outer-list payload. Nested child
/// ownership is unchanged; Arrow's existing nested row encoding still applies.
struct CompactDefinedLists(ArrayRef);

impl CompactDefinedLists {
    fn try_new(input: ArrayRef) -> Result<Self> {
        let lists = input
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| exec_datafusion_err!("list ownership guard requires List input"))?;
        let offsets = lists.value_offsets();
        // Actual masks/offsets are batch properties. Schema/type eligibility was
        // selected by the caller at query preparation, not inside these loops.
        if lists.null_count() == 0
            || (0..lists.len()).all(|row| !lists.is_null(row) || offsets[row] == offsets[row + 1])
        {
            return Ok(Self(input));
        }
        let mut compact_offsets = Vec::with_capacity(lists.len() + 1);
        compact_offsets.push(0i32);
        let mut selected = 0usize;
        for row in 0..lists.len() {
            let len = if lists.is_null(row) {
                0
            } else {
                (offsets[row + 1] - offsets[row]) as usize
            };
            selected = selected
                .checked_add(len)
                .ok_or_else(|| exec_datafusion_err!("list ownership child count overflow"))?;
            compact_offsets.push(
                i32::try_from(selected)
                    .map_err(|_| exec_datafusion_err!("list ownership offset exceeds int32"))?,
            );
        }
        let data = lists.values().to_data();
        let mut output = MutableArrayData::new(vec![&data], false, selected);
        let mut run: Option<(usize, usize)> = None;
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            match run {
                Some((first, last)) if last == start => run = Some((first, end)),
                Some((first, last)) => {
                    output.extend(0, first, last);
                    run = Some((start, end));
                }
                None => run = Some((start, end)),
            }
        }
        if let Some((first, last)) = run {
            output.extend(0, first, last);
        }
        let DataType::List(field) = lists.data_type() else {
            unreachable!()
        };
        let compact = ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(compact_offsets.into()),
            arrow_array::make_array(output.freeze()),
            lists.nulls().cloned(),
        )?;
        Ok(Self(Arc::new(compact)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CompactDefinedListsUdf {
    signature: Signature,
    dtype: DataType,
}

/// Both logical and physical set lowering use this same prepared guard before
/// ordinary DataFusion array_distinct, which otherwise encodes hidden payload.
pub(crate) fn udf(dtype: DataType) -> Result<Arc<ScalarUDF>> {
    if !matches!(dtype, DataType::List(_)) {
        return Err(plan_datafusion_err!(
            "list ownership guard requires List input"
        ));
    }
    Ok(Arc::new(ScalarUDF::from(CompactDefinedListsUdf {
        signature: Signature::exact(vec![dtype.clone()], Volatility::Immutable),
        dtype,
    })))
}

impl ScalarUDFImpl for CompactDefinedListsUdf {
    fn name(&self) -> &str {
        "__vail_compact_defined_lists"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.dtype.clone())
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args.args.remove(0);
        match input {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(CompactDefinedLists::try_new(array)?.0))
            }
            ColumnarValue::Scalar(ScalarValue::List(array)) => {
                let compact = CompactDefinedLists::try_new(array)?;
                let lists = compact.0.as_any().downcast_ref::<ListArray>().unwrap();
                Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                    lists.clone(),
                ))))
            }
            _ => Err(exec_datafusion_err!(
                "list ownership guard requires List input"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{Expr, Type};
    use arrow_array::{Int32Array, RecordBatch, StructArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{Field, Schema};
    use std::collections::BTreeSet;

    fn evaluate(input: ListArray, logical: bool, scalar: bool) -> ArrayRef {
        let dtype = Type::Set {
            set: Box::new(Type::from_arrow(&input.value_type()).unwrap()),
        };
        let schema = Schema::new(vec![Field::new("items", input.data_type().clone(), true)]);
        let rows = input.len();
        let argument = if scalar {
            Expr::RuntimeLiteral {
                value: ScalarValue::List(Arc::new(input.clone())),
            }
        } else {
            Expr::Column {
                name: "items".into(),
            }
        };
        let expression = Expr::NestedFunction {
            function: "__vail_list_reduce_collect_as_set".into(),
            args: vec![argument],
            dtype,
        };
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)]).unwrap();
        let physical = if logical {
            let df_schema = datafusion_common::DFSchema::try_from(schema.clone()).unwrap();
            let expression = crate::logical_expr::lower_logical(&expression, &df_schema).unwrap();
            datafusion_physical_expr::planner::create_physical_expr(
                &expression,
                &df_schema,
                &datafusion_physical_expr::execution_props::ExecutionProps::new(),
                &datafusion_expr::physical_planning_context::PhysicalPlanningContext::default(),
            )
            .unwrap()
        } else {
            crate::expressions::lower(&expression, &schema).unwrap()
        };
        physical.evaluate(&batch).unwrap().into_array(rows).unwrap()
    }

    #[test]
    fn public_set_lowering_preserves_fixed_visible_values_with_growing_hidden_payload() {
        for logical in [false, true] {
            for hidden in [0usize, 1, 63, 64, 65, 1_000_000] {
                let mut values = vec![7; hidden + 4];
                values[hidden + 2] = 9;
                values[hidden + 3] = 9;
                let input = ListArray::new(
                    Arc::new(Field::new_list_field(DataType::Int32, true)),
                    OffsetBuffer::new(vec![0, 2, (hidden + 2) as i32, (hidden + 4) as i32].into()),
                    Arc::new(Int32Array::from(values)),
                    Some(NullBuffer::from(vec![true, false, true])),
                );
                let output = evaluate(input, logical, false);
                let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(lists.len(), 3);
                assert!(lists.is_null(1));
                for (row, expected) in [(0, BTreeSet::from([7])), (2, BTreeSet::from([9]))] {
                    let value = lists.value(row);
                    let value = value.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(
                        value.iter().collect::<BTreeSet<_>>(),
                        expected.into_iter().map(Some).collect()
                    );
                    assert_eq!(value.len(), 1);
                }
            }
        }
    }

    #[test]
    fn public_set_lowering_preserves_nested_nulls_slice_empty_and_scalar() {
        for logical in [false, true] {
            let child: ArrayRef = Arc::new(StructArray::new(
                vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
                vec![Arc::new(Int32Array::from(vec![
                    Some(88),
                    Some(7),
                    None,
                    Some(99),
                    Some(9),
                    Some(77),
                ]))],
                Some(NullBuffer::from(vec![true, true, false, true, true, true])),
            ));
            let input = ListArray::new(
                Arc::new(Field::new_list_field(child.data_type().clone(), true)),
                OffsetBuffer::new(vec![0, 1, 3, 4, 4, 5, 6].into()),
                child,
                Some(NullBuffer::from(vec![true, true, false, true, true, true])),
            )
            .slice(1, 4);
            let output = evaluate(input.clone(), logical, false);
            let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(lists.value_length(0), 2);
            let first = lists.value(0);
            let first = first.as_any().downcast_ref::<StructArray>().unwrap();
            let actual = (0..first.len())
                .map(|i| {
                    if first.is_null(i) {
                        None
                    } else {
                        let value = first
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        Some(value.value(i))
                    }
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, BTreeSet::from([None, Some(7)]));
            assert!(lists.is_null(1));
            assert!(!lists.is_null(2));
            assert_eq!(lists.value_length(2), 0);
            assert_eq!(lists.value_length(3), 1);
            let scalar = evaluate(input.slice(1, 1), logical, true);
            assert!(scalar.is_null(0));
            let empty = evaluate(input.slice(0, 0), logical, false);
            assert_eq!(empty.len(), 0);
        }
    }
}
