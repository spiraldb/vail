//! Lift a DataFusion aggregate over array coordinates without unnesting rows.
//! Arithmetic, null handling, partial states and merges remain owned by the
//! underlying aggregate. A native GroupsAccumulator processes all coordinates
//! in one batch; other UDAFs receive one Arrow batch per coordinate.
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, ListArray, UInt32Array, new_empty_array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::{concat::concat, take::take};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, EmitTo, GroupsAccumulator, Volatility, expr_fn::create_udaf,
};
use datafusion_physical_expr::{
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
    expressions::Column,
};

fn list_type(dtype: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(dtype, true)))
}
fn list_scalar(values: ArrayRef) -> ScalarValue {
    ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::from_lengths([values.len()]),
        values,
        None,
    )))
}
fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

/// The adapter accepts any unary UDAF with ordinary mergeable Arrow state.
/// `empty` supplies the caller's language-level identity, e.g. Hail sum's zero.
pub fn coordinate_udaf(
    inner: Arc<AggregateUDF>,
    input: DataType,
    empty: ScalarValue,
) -> Result<Arc<AggregateUDF>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "element",
        input.clone(),
        true,
    )]));
    let aggregate = Arc::new(
        AggregateExprBuilder::new(inner, vec![Arc::new(Column::new("element", 0))])
            .schema(schema)
            .alias("coordinate")
            .build()?,
    );
    let output = aggregate.field().data_type().clone();
    let state_types = aggregate
        .state_fields()?
        .iter()
        .map(|f| list_type(f.data_type().clone()))
        .chain(std::iter::once(DataType::Int64))
        .collect::<Vec<_>>();
    let name = format!("vail_coordinate_{}", aggregate.fun().name());
    Ok(Arc::new(create_udaf(
        &name,
        vec![list_type(input)],
        Arc::new(list_type(output)),
        Volatility::Immutable,
        Arc::new(move |_| {
            Ok(Box::new(Coordinates::new(
                aggregate.clone(),
                empty.clone(),
            )?))
        }),
        Arc::new(state_types),
    )))
}

enum Kernels {
    Groups(std::sync::Mutex<Box<dyn GroupsAccumulator>>),
    Coordinates(Vec<Box<dyn Accumulator>>),
}
impl std::fmt::Debug for Kernels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Groups(_) => f.write_str("DataFusionGroupsAccumulator"),
            Self::Coordinates(kernels) => f
                .debug_tuple("CoordinateAccumulators")
                .field(kernels)
                .finish(),
        }
    }
}
#[derive(Debug)]
struct Coordinates {
    aggregate: Arc<AggregateFunctionExpr>,
    kernels: Kernels,
    width: Option<usize>,
    empty: ScalarValue,
    group_indices: Vec<usize>,
}
impl Coordinates {
    fn new(aggregate: Arc<AggregateFunctionExpr>, empty: ScalarValue) -> Result<Self> {
        let kernels = if aggregate.groups_accumulator_supported() {
            Kernels::Groups(std::sync::Mutex::new(
                aggregate.create_groups_accumulator()?,
            ))
        } else {
            Kernels::Coordinates(Vec::new())
        };
        Ok(Self {
            aggregate,
            kernels,
            width: None,
            empty,
            group_indices: Vec::new(),
        })
    }
    fn set_width(&mut self, width: usize) -> Result<()> {
        if let Some(old) = self.width {
            if old != width {
                return Err(error(&format!(
                    "array_agg requires equal array lengths, found {old} and {width}"
                )));
            }
        } else {
            self.width = Some(width);
            if let Kernels::Coordinates(kernels) = &mut self.kernels {
                *kernels = (0..width)
                    .map(|_| self.aggregate.create_accumulator())
                    .collect::<Result<_>>()?;
            }
        }
        Ok(())
    }
    fn states(&mut self) -> Result<Vec<ArrayRef>> {
        let width = self.width.unwrap_or(0);
        match &mut self.kernels {
            Kernels::Groups(kernel) => {
                let kernel = kernel
                    .get_mut()
                    .map_err(|_| error("coordinate accumulator state poisoned"))?;
                let state = kernel.state(EmitTo::All)?;
                // GroupsAccumulator emits state destructively, whereas the
                // scalar Accumulator interface requires a repeatable snapshot.
                if width > 0 {
                    kernel.merge_batch(&state, &(0..width).collect::<Vec<_>>(), width)?;
                }
                Ok(state)
            }
            Kernels::Coordinates(kernels) => {
                let states = kernels
                    .iter_mut()
                    .map(|k| k.state())
                    .collect::<Result<Vec<_>>>()?;
                self.aggregate
                    .state_fields()?
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        if states.is_empty() {
                            Ok(new_empty_array(f.data_type()))
                        } else {
                            ScalarValue::iter_to_array(states.iter().map(|s| s[i].clone()))
                        }
                    })
                    .collect()
            }
        }
    }
}
impl Accumulator for Coordinates {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arrays = values
            .first()
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| error("coordinate aggregate requires a list"))?;
        let mut chunks = Vec::new();
        for row in 0..arrays.len() {
            if arrays.is_valid(row) {
                self.set_width(arrays.value_length(row) as usize)?;
            }
        }
        let Some(width) = self.width else {
            return Ok(());
        };
        if width == 0 {
            return Ok(());
        }
        // For ordinary non-null arrays the child values are already contiguous
        // Arrow storage. Missing lists may hide arbitrary children: omit them.
        let flat = if arrays.null_count() == 0 {
            let offsets = arrays.value_offsets();
            arrays.values().slice(
                offsets[0] as usize,
                (offsets[arrays.len()] - offsets[0]) as usize,
            )
        } else {
            for row in 0..arrays.len() {
                if arrays.is_valid(row) {
                    chunks.push(arrays.value(row));
                }
            }
            if chunks.is_empty() {
                return Ok(());
            }
            concat(&chunks.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
        };
        if matches!(self.kernels, Kernels::Groups(_)) && self.group_indices.len() < flat.len() {
            self.group_indices
                .extend((self.group_indices.len()..flat.len()).map(|i| i % width));
        }
        match &mut self.kernels {
            Kernels::Groups(kernel) => {
                let kernel = kernel
                    .get_mut()
                    .map_err(|_| error("coordinate accumulator state poisoned"))?;
                let groups = &self.group_indices[..flat.len()];
                kernel.update_batch(&[flat], groups, None, width)
            }
            Kernels::Coordinates(kernels) => {
                for (coordinate, kernel) in kernels.iter_mut().enumerate() {
                    let indices = UInt32Array::from_iter_values(
                        (coordinate..flat.len()).step_by(width).map(|i| i as u32),
                    );
                    kernel.update_batch(&[take(flat.as_ref(), &indices, None)?])?;
                }
                Ok(())
            }
        }
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(width) = self.width else {
            return ScalarValue::try_from(&list_type(self.aggregate.field().data_type().clone()));
        };
        if width == 0 {
            return Ok(list_scalar(new_empty_array(
                self.aggregate.field().data_type(),
            )));
        }
        let values = match &mut self.kernels {
            Kernels::Groups(_) => {
                let states = self.states()?;
                let mut copy = self.aggregate.create_groups_accumulator()?;
                copy.merge_batch(&states, &(0..width).collect::<Vec<_>>(), width)?;
                copy.evaluate(EmitTo::All)?
            }
            Kernels::Coordinates(kernels) => ScalarValue::iter_to_array(
                kernels
                    .iter_mut()
                    .map(|k| k.evaluate())
                    .collect::<Result<Vec<_>>>()?,
            )?,
        };
        // Only language-level empty identities are added; reductions themselves
        // use the underlying library's vectorized implementation.
        let values = if values.null_count() > 0 && !self.empty.is_null() {
            let mask = arrow_arith::boolean::is_null(values.as_ref())?;
            let fill = self.empty.to_scalar()?;
            arrow_select::zip::zip(&mask, &fill, &values)?
        } else {
            values
        };
        Ok(list_scalar(values))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.group_indices.capacity() * std::mem::size_of::<usize>()
            + match &self.kernels {
                Kernels::Groups(k) => k.lock().map(|k| k.size()).unwrap_or_default(),
                Kernels::Coordinates(ks) => ks.iter().map(|k| k.size()).sum(),
            }
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut states = self
            .states()?
            .into_iter()
            .map(list_scalar)
            .collect::<Vec<_>>();
        states.push(ScalarValue::Int64(self.width.map(|n| n as i64)));
        Ok(states)
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let widths = states
            .last()
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| error("coordinate aggregate has invalid width state"))?;
        let lists = states[..states.len() - 1]
            .iter()
            .map(|a| {
                a.as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| error("coordinate aggregate has invalid partial state"))
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..widths.len() {
            if widths.is_null(row) {
                continue;
            }
            let width =
                usize::try_from(widths.value(row)).map_err(|_| error("invalid array width"))?;
            self.set_width(width)?;
            if width == 0 {
                continue;
            }
            let values = lists.iter().map(|a| a.value(row)).collect::<Vec<_>>();
            if values.iter().any(|a| a.len() != width) {
                return Err(error("coordinate state length mismatch"));
            }
            match &mut self.kernels {
                Kernels::Groups(kernel) => {
                    let kernel = kernel
                        .get_mut()
                        .map_err(|_| error("coordinate accumulator state poisoned"))?;
                    kernel.merge_batch(&values, &(0..width).collect::<Vec<_>>(), width)?
                }
                Kernels::Coordinates(kernels) => {
                    for (i, kernel) in kernels.iter_mut().enumerate() {
                        kernel.merge_batch(
                            &values.iter().map(|a| a.slice(i, 1)).collect::<Vec<_>>(),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::{Float64Type, Int64Type};

    fn coordinate_accumulator(
        inner: Arc<AggregateUDF>,
        input: DataType,
        empty: ScalarValue,
    ) -> Box<dyn Accumulator> {
        let list_input = list_type(input.clone());
        let udf = coordinate_udaf(inner, input, empty).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("arrays", list_input, true)]));
        AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("arrays", 0))])
            .schema(schema)
            .alias("coordinate_aggregate")
            .build()
            .unwrap()
            .create_accumulator()
            .unwrap()
    }

    fn accumulator() -> Box<dyn Accumulator> {
        let udf = coordinate_udaf(
            datafusion_functions_aggregate::sum::sum_udaf(),
            DataType::Int64,
            ScalarValue::Int64(Some(0)),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "arrays",
            list_type(DataType::Int64),
            true,
        )]));
        AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("arrays", 0))])
            .schema(schema)
            .alias("array_sum")
            .build()
            .unwrap()
            .create_accumulator()
            .unwrap()
    }
    fn arrays(values: Vec<Option<Vec<Option<i64>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(values))
    }
    fn float_arrays(values: Vec<Option<Vec<Option<f64>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(values))
    }
    fn result(value: ScalarValue) -> Vec<Option<i64>> {
        let ScalarValue::List(value) = value else {
            panic!("expected list")
        };
        value
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }
    fn float_result(value: ScalarValue) -> Vec<Option<f64>> {
        let ScalarValue::List(value) = value else {
            panic!("expected list")
        };
        value
            .value(0)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }
    #[test]
    fn native_groups_preserve_missing_elements_and_partial_merge() {
        let mut left = accumulator();
        left.update_batch(&[arrays(vec![
            Some(vec![Some(1), None]),
            None,
            Some(vec![Some(3), None]),
        ])])
        .unwrap();
        assert_eq!(result(left.evaluate().unwrap()), vec![Some(4), Some(0)]);
        let state = left.state().unwrap();
        // Capturing partial state must not consume the running accumulator.
        assert_eq!(result(left.evaluate().unwrap()), vec![Some(4), Some(0)]);
        let mut merged = accumulator();
        merged
            .merge_batch(
                &state
                    .into_iter()
                    .map(|s| s.to_array())
                    .collect::<Result<Vec<_>>>()
                    .unwrap(),
            )
            .unwrap();
        merged
            .update_batch(&[arrays(vec![Some(vec![Some(2), Some(7)])])])
            .unwrap();
        assert_eq!(result(merged.evaluate().unwrap()), vec![Some(6), Some(7)]);
    }
    #[test]
    fn missing_empty_sliced_and_inconsistent_arrays() {
        let mut a = accumulator();
        assert!(a.evaluate().unwrap().is_null());
        a.update_batch(&[arrays(vec![None, None])]).unwrap();
        assert!(a.evaluate().unwrap().is_null());
        a.update_batch(&[arrays(vec![Some(vec![])])]).unwrap();
        assert_eq!(result(a.evaluate().unwrap()), vec![]);
        assert!(
            a.update_batch(&[arrays(vec![Some(vec![Some(1)])])])
                .is_err()
        );
        let mut a = accumulator();
        let input = arrays(vec![
            Some(vec![Some(99), Some(99)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
        ]);
        a.update_batch(&[input.slice(1, 2)]).unwrap();
        assert_eq!(result(a.evaluate().unwrap()), vec![Some(4), Some(6)]);
        assert!(
            a.update_batch(&[arrays(vec![Some(vec![Some(1)])])])
                .is_err()
        );
    }

    #[test]
    fn float_sum_keeps_fractional_values_across_partial_state() {
        let mut left = coordinate_accumulator(
            datafusion_functions_aggregate::sum::sum_udaf(),
            DataType::Float64,
            ScalarValue::Float64(Some(0.0)),
        );
        left.update_batch(&[float_arrays(vec![
            Some(vec![Some(1.25), None]),
            Some(vec![Some(2.5), Some(4.75)]),
        ])])
        .unwrap();
        let state = left.state().unwrap();
        assert_eq!(
            float_result(left.evaluate().unwrap()),
            vec![Some(3.75), Some(4.75)]
        );

        let mut merged = coordinate_accumulator(
            datafusion_functions_aggregate::sum::sum_udaf(),
            DataType::Float64,
            ScalarValue::Float64(Some(0.0)),
        );
        merged
            .merge_batch(
                &state
                    .iter()
                    .map(|state| state.to_array())
                    .collect::<Result<Vec<_>>>()
                    .unwrap(),
            )
            .unwrap();
        merged
            .update_batch(&[float_arrays(vec![Some(vec![Some(0.5), Some(0.25)])])])
            .unwrap();
        assert_eq!(
            float_result(merged.evaluate().unwrap()),
            vec![Some(4.25), Some(5.0)]
        );
    }

    #[test]
    fn count_ignores_missing_coordinate_values_but_counts_defined_values() {
        let mut count = coordinate_accumulator(
            datafusion_functions_aggregate::count::count_udaf(),
            DataType::Float64,
            ScalarValue::Int64(Some(0)),
        );
        count
            .update_batch(&[float_arrays(vec![
                Some(vec![Some(1.0), None]),
                None,
                Some(vec![None, Some(3.0)]),
            ])])
            .unwrap();
        let ScalarValue::List(value) = count.evaluate().unwrap() else {
            panic!("expected list")
        };
        assert_eq!(
            value
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(1)]
        );
    }

    #[test]
    fn stddev_fallback_state_is_repeatable_and_merges_partial_widths() {
        let stddev =
            || crate::aggregates::aggregate_udf("stddev_pop", Some(&DataType::Float64)).unwrap();
        let inner_schema = Arc::new(Schema::new(vec![Field::new(
            "element",
            DataType::Float64,
            true,
        )]));
        let inner = Arc::new(
            AggregateExprBuilder::new(stddev(), vec![Arc::new(Column::new("element", 0))])
                .schema(inner_schema)
                .alias("stddev_pop")
                .build()
                .unwrap(),
        );
        assert!(!inner.groups_accumulator_supported());
        let fallback = Coordinates::new(inner, ScalarValue::Float64(Some(f64::NAN))).unwrap();
        assert!(matches!(fallback.kernels, Kernels::Coordinates(_)));
        let create = || {
            coordinate_accumulator(
                stddev(),
                DataType::Float64,
                ScalarValue::Float64(Some(f64::NAN)),
            )
        };
        let mut left = create();
        left.update_batch(&[float_arrays(vec![
            Some(vec![Some(1.0), None]),
            Some(vec![Some(3.0), Some(5.0)]),
        ])])
        .unwrap();
        let state = left.state().unwrap();
        let left_value = float_result(left.evaluate().unwrap());
        assert!((left_value[0].unwrap() - 1.0).abs() < 1e-12);
        assert_eq!(left_value[1], Some(0.0));
        let repeated = float_result(left.evaluate().unwrap());
        assert!((repeated[0].unwrap() - 1.0).abs() < 1e-12);
        assert_eq!(repeated[1], Some(0.0));

        let mut merged = create();
        merged
            .merge_batch(
                &state
                    .iter()
                    .map(|state| state.to_array())
                    .collect::<Result<Vec<_>>>()
                    .unwrap(),
            )
            .unwrap();
        merged
            .update_batch(&[float_arrays(vec![Some(vec![Some(5.0), Some(7.0)])])])
            .unwrap();
        let value = float_result(merged.evaluate().unwrap());
        assert!((value[0].unwrap() - (8.0f64 / 3.0).sqrt()).abs() < 1e-12);
        assert!((value[1].unwrap() - 1.0).abs() < 1e-12);

        // A partition with no defined array must not lock the global width.
        let mut missing_partition = create();
        missing_partition
            .update_batch(&[float_arrays(vec![None, None])])
            .unwrap();
        let state = missing_partition.state().unwrap();
        let mut width_known_later = create();
        width_known_later
            .merge_batch(
                &state
                    .iter()
                    .map(|state| state.to_array())
                    .collect::<Result<Vec<_>>>()
                    .unwrap(),
            )
            .unwrap();
        width_known_later
            .update_batch(&[float_arrays(vec![Some(vec![Some(2.0)])])])
            .unwrap();
        assert_eq!(float_result(width_known_later.evaluate().unwrap()).len(), 1);

        let mut zero_width = create();
        zero_width
            .update_batch(&[float_arrays(vec![Some(vec![])])])
            .unwrap();
        let state = zero_width.state().unwrap();
        let mut nonzero_width = create();
        nonzero_width
            .update_batch(&[float_arrays(vec![Some(vec![Some(2.0)])])])
            .unwrap();
        assert!(
            nonzero_width
                .merge_batch(
                    &state
                        .iter()
                        .map(|state| state.to_array())
                        .collect::<Result<Vec<_>>>()
                        .unwrap(),
                )
                .is_err()
        );
    }
}
