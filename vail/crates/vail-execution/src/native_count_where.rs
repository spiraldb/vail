//! Compatibility factory for Boolean true counts using native DataFusion COUNT.
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{Accumulator, AggregateUDF, EmitTo, GroupsAccumulator};
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};

fn true_values(values: &[ArrayRef]) -> Result<ArrayRef> {
    let input = values
        .first()
        .ok_or_else(|| exec_datafusion_err!("count_where requires input"))?;
    let input = input
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| exec_datafusion_err!("count_where requires Boolean input"))?;
    let validity = match input.nulls() {
        Some(nulls) => input.values() & nulls.inner(),
        None => input.values().clone(),
    };
    Ok(Arc::new(BooleanArray::new(
        input.values().clone(),
        Some(NullBuffer::new(validity)),
    )))
}

pub(crate) fn udf(
    input: Option<&DataType>,
) -> std::result::Result<Arc<AggregateUDF>, crate::expressions::ExprError> {
    if input.is_some_and(|t| t != &DataType::Boolean) {
        return Err(crate::expressions::ExprError::Invalid(
            "count_where requires Boolean input".into(),
        ));
    }
    let native = Arc::new(
        AggregateExprBuilder::new(
            datafusion_functions_aggregate::count::count_udaf(),
            vec![Arc::new(Column::new("value", 0))],
        )
        .schema(Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Boolean,
            true,
        )])))
        .alias("count_where")
        .build()
        .map_err(|e| crate::expressions::ExprError::Invalid(e.to_string()))?,
    );
    Ok(crate::kernel_identity::aggregate_named_with_groups(
        "count_where",
        ("count_where", DataType::Boolean),
        vec![DataType::Boolean],
        DataType::Int64,
        vec![DataType::Int64],
        Arc::new({
            let native = native.clone();
            move |_| {
                Ok(Box::new(TrueCount {
                    inner: native.create_accumulator()?,
                }))
            }
        }),
        Some(Arc::new(move || {
            Ok(Box::new(TrueCountGroups {
                inner: native.create_groups_accumulator()?,
            }))
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
struct TrueCount {
    inner: Box<dyn Accumulator>,
}
impl Accumulator for TrueCount {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(&[true_values(values)?])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.inner.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(&[true_values(values)?])
    }
}

struct TrueCountGroups {
    inner: Box<dyn GroupsAccumulator>,
}
impl GroupsAccumulator for TrueCountGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        filter: Option<&BooleanArray>,
        n: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(&[true_values(values)?], groups, filter, n)
    }
    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        self.inner.evaluate(emit)
    }
    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit)
    }
    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], n: usize) -> Result<()> {
        self.inner.merge_batch(states, groups, n)
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.inner.size()
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(&[true_values(values)?], filter)
    }
}
