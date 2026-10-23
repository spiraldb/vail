//! Hail's Boolean fraction aggregate, delegated to native DataFusion COUNTs.
//!
//! The denominator counts every row. The numerator counts non-null true
//! predicates. Both use native scalar/group accumulators and merge state; the
//! only input adaptation is Arrow bitmap views for COUNT's null semantics.

use std::sync::Arc;

use arrow_array::builder::Float64Builder;
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, EmitTo, GroupsAccumulator};
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};

fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn count_value(value: ScalarValue) -> Result<i64> {
    match value {
        ScalarValue::Int64(Some(value)) => Ok(value),
        _ => Err(error("fraction count must be a defined int64")),
    }
}

/// Two zero-copy payload views tell native COUNT which rows belong to each
/// side of the fraction. Denominator has no nulls; numerator validity is
/// `predicate AND original validity`, so a missing predicate is false.
fn count_inputs(values: &[ArrayRef]) -> Result<(ArrayRef, ArrayRef)> {
    let input = values
        .first()
        .ok_or_else(|| error("fraction requires input"))?;
    let input = input
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| error("fraction requires Boolean input"))?;
    let payload = input.values().clone();
    let denominator = Arc::new(BooleanArray::new(payload.clone(), None)) as ArrayRef;
    let numerator_validity = match input.nulls() {
        Some(nulls) => &payload & nulls.inner(),
        None => payload.clone(),
    };
    let numerator = Arc::new(BooleanArray::new(
        payload,
        Some(NullBuffer::new(numerator_validity)),
    )) as ArrayRef;
    Ok((denominator, numerator))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FractionIdentity;

pub(crate) fn coordinate_fraction(udf: &AggregateUDF) -> bool {
    crate::kernel_identity::aggregate_key::<FractionIdentity>(udf).is_some()
}

pub(crate) fn udf(
    input: Option<&DataType>,
) -> std::result::Result<Arc<AggregateUDF>, crate::expressions::ExprError> {
    if input.is_some_and(|ty| ty != &DataType::Boolean) {
        return Err(crate::expressions::ExprError::Invalid(
            "fraction requires Boolean input".into(),
        ));
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Boolean,
        true,
    )]));
    let count = Arc::new(
        AggregateExprBuilder::new(
            datafusion_functions_aggregate::count::count_udaf(),
            vec![Arc::new(Column::new("value", 0))],
        )
        .schema(schema)
        .alias("fraction_count")
        .build()
        .map_err(|error| crate::expressions::ExprError::Invalid(error.to_string()))?,
    );
    Ok(crate::kernel_identity::aggregate_named_with_groups(
        "fraction",
        FractionIdentity,
        vec![DataType::Boolean],
        DataType::Float64,
        vec![DataType::Int64, DataType::Int64],
        Arc::new({
            let count = Arc::clone(&count);
            move |_| {
                Ok(Box::new(FractionAccumulator {
                    total: count.create_accumulator()?,
                    true_count: count.create_accumulator()?,
                }))
            }
        }),
        Some(Arc::new({
            let count = Arc::clone(&count);
            move || {
                Ok(Box::new(FractionGroups {
                    total: count.create_groups_accumulator()?,
                    true_count: count.create_groups_accumulator()?,
                }))
            }
        })),
        Some(Arc::new(
            |args: &datafusion_expr::function::AccumulatorArgs<'_>| {
                !args.is_distinct
                    && args.order_bys.is_empty()
                    && !args.is_reversed
                    && args.expr_fields.len() == 1
                    && args.expr_fields[0].data_type() == &DataType::Boolean
            },
        )),
    ))
}

#[derive(Debug)]
struct FractionAccumulator {
    total: Box<dyn Accumulator>,
    true_count: Box<dyn Accumulator>,
}

impl Accumulator for FractionAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let (total, true_count) = count_inputs(values)?;
        self.total.update_batch(&[total])?;
        self.true_count.update_batch(&[true_count])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let total = count_value(self.total.evaluate()?)?;
        let true_count = count_value(self.true_count.evaluate()?)?;
        Ok(ScalarValue::Float64(
            (total != 0).then(|| true_count as f64 / total as f64),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.total.size() + self.true_count.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut total = self.total.state()?;
        let mut true_count = self.true_count.state()?;
        if total.len() != 1 || true_count.len() != 1 {
            return Err(error("fraction count state must have one field"));
        }
        Ok(vec![total.remove(0), true_count.remove(0)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [total, true_count] = states else {
            return Err(error("fraction state requires total and true counts"));
        };
        if total.len() != true_count.len()
            || total.as_any().downcast_ref::<Int64Array>().is_none()
            || true_count.as_any().downcast_ref::<Int64Array>().is_none()
        {
            return Err(error("fraction states must be matching int64 arrays"));
        }
        self.total.merge_batch(std::slice::from_ref(total))?;
        self.true_count
            .merge_batch(std::slice::from_ref(true_count))
    }
}

struct FractionGroups {
    total: Box<dyn GroupsAccumulator>,
    true_count: Box<dyn GroupsAccumulator>,
}

impl GroupsAccumulator for FractionGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let input_len = values
            .first()
            .map(|values| values.len())
            .ok_or_else(|| error("fraction requires input"))?;
        if input_len != group_indices.len()
            || opt_filter.is_some_and(|filter| filter.len() != input_len)
            || group_indices.iter().any(|group| *group >= total_num_groups)
        {
            return Err(error("fraction grouping shape mismatch"));
        }
        let (total, true_count) = count_inputs(values)?;
        self.total
            .update_batch(&[total], group_indices, opt_filter, total_num_groups)?;
        self.true_count
            .update_batch(&[true_count], group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let total = self
            .total
            .evaluate(emit_to)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| error("fraction total count must be int64"))?
            .clone();
        let true_count = self
            .true_count
            .evaluate(emit_to)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| error("fraction true count must be int64"))?
            .clone();
        if total.len() != true_count.len() {
            return Err(error("fraction count state lengths differ"));
        }
        let mut values = Float64Builder::with_capacity(total.len());
        for (total, true_count) in total.iter().zip(true_count.iter()) {
            if let Some(total) = total.filter(|count| *count != 0) {
                if let Some(true_count) = true_count {
                    values.append_value(true_count as f64 / total as f64);
                } else {
                    values.append_null();
                }
            } else {
                values.append_null();
            }
        }
        Ok(Arc::new(values.finish()))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut total = self.total.state(emit_to)?;
        let mut true_count = self.true_count.state(emit_to)?;
        if total.len() != 1 || true_count.len() != 1 {
            return Err(error("fraction count state must have one field"));
        }
        Ok(vec![total.remove(0), true_count.remove(0)])
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        let [total, true_count] = states else {
            return Err(error("fraction state requires total and true counts"));
        };
        if total.len() != group_indices.len()
            || true_count.len() != group_indices.len()
            || total.as_any().downcast_ref::<Int64Array>().is_none()
            || true_count.as_any().downcast_ref::<Int64Array>().is_none()
            || group_indices.iter().any(|group| *group >= total_num_groups)
        {
            return Err(error("fraction grouped states have invalid shape/type"));
        }
        self.total
            .merge_batch(std::slice::from_ref(total), group_indices, total_num_groups)?;
        self.true_count.merge_batch(
            std::slice::from_ref(true_count),
            group_indices,
            total_num_groups,
        )
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let input_len = values
            .first()
            .map(|values| values.len())
            .ok_or_else(|| error("fraction requires input"))?;
        if opt_filter.is_some_and(|filter| filter.len() != input_len) {
            return Err(error("fraction converter filter length mismatch"));
        }
        let (total, true_count) = count_inputs(values)?;
        Ok(vec![
            self.total.convert_to_state(&[total], opt_filter)?.remove(0),
            self.true_count
                .convert_to_state(&[true_count], opt_filter)?
                .remove(0),
        ])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.total.size() + self.true_count.size()
    }
}
