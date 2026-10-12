//! Stateful Hail-style fold implemented with compiled DataFusion expressions.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, Volatility, expr_fn::create_udaf};
use datafusion_physical_expr::PhysicalExpr;

#[derive(Debug)]
struct FoldAccumulator {
    state: ScalarValue,
    input_schema: SchemaRef,
    seq_schema: SchemaRef,
    combine_schema: SchemaRef,
    seq: Arc<dyn PhysicalExpr>,
    combine: Arc<dyn PhysicalExpr>,
}

impl FoldAccumulator {
    fn evaluate_one(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> DfResult<ScalarValue> {
        let value = expr.evaluate(batch)?.into_array(1)?;
        ScalarValue::try_from_array(&value, 0)
    }

    fn sequence(&mut self, row_columns: &[ArrayRef]) -> DfResult<()> {
        let mut columns = Vec::with_capacity(row_columns.len() + 1);
        columns.push(ScalarValue::iter_to_array(vec![self.state.clone()])?);
        columns.extend(row_columns.iter().map(|column| column.slice(0, 1)));
        let batch = RecordBatch::try_new(Arc::clone(&self.seq_schema), columns)?;
        self.state = Self::evaluate_one(&self.seq, &batch)?;
        Ok(())
    }

    fn combine_states(&self, left: ScalarValue, right: ScalarValue) -> DfResult<ScalarValue> {
        let columns = vec![
            ScalarValue::iter_to_array(vec![left])?,
            ScalarValue::iter_to_array(vec![right])?,
        ];
        let batch = RecordBatch::try_new(Arc::clone(&self.combine_schema), columns)?;
        Self::evaluate_one(&self.combine, &batch)
    }

    fn update_packed(&mut self, values: &ArrayRef) -> DfResult<()> {
        let rows = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("fold input must be a packed struct".into())
            })?;
        if rows.num_columns() != self.input_schema.fields().len() {
            return Err(DataFusionError::Execution(format!(
                "fold packed struct has {} fields; expected {}",
                rows.num_columns(),
                self.input_schema.fields().len()
            )));
        }
        for row in 0..rows.len() {
            let row_columns = rows
                .columns()
                .iter()
                .map(|column| column.slice(row, 1))
                .collect::<Vec<_>>();
            self.sequence(&row_columns)?;
        }
        Ok(())
    }
}

impl Accumulator for FoldAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let [values] = values else {
            return Err(DataFusionError::Execution(
                "fold expects one packed struct input".into(),
            ));
        };
        self.update_packed(values)
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(self.state.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.state.size()
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [states] = states else {
            return Err(DataFusionError::Execution(
                "fold state must contain one value".into(),
            ));
        };
        for row in 0..states.len() {
            let partial = ScalarValue::try_from_array(states, row)?;
            self.state = self.combine_states(self.state.clone(), partial)?;
        }
        Ok(())
    }
}

/// Construct a fold UDAF. `input_schema` describes the fields of the packed
/// input struct. `seq_expr` reads state at field 0 followed by those inputs;
/// `comb_expr` reads left state at field 0 and right state at field 1.
pub fn fold_udaf(
    input_schema: SchemaRef,
    initial: ScalarValue,
    state_type: DataType,
    seq_expr: Arc<dyn PhysicalExpr>,
    comb_expr: Arc<dyn PhysicalExpr>,
) -> AggregateUDF {
    let mut seq_fields = vec![Arc::new(Field::new("state", state_type.clone(), true))];
    seq_fields.extend(input_schema.fields().iter().cloned());
    let seq_schema = Arc::new(Schema::new(seq_fields));
    let combine_schema = Arc::new(Schema::new(vec![
        Field::new("left", state_type.clone(), true),
        Field::new("right", state_type.clone(), true),
    ]));
    let packed_type = DataType::Struct(input_schema.fields().clone());
    let factory_initial = initial.clone();
    let factory_input_schema = Arc::clone(&input_schema);
    let factory_seq_schema = Arc::clone(&seq_schema);
    let factory_combine_schema = Arc::clone(&combine_schema);
    let factory_seq = Arc::clone(&seq_expr);
    let factory_combine = Arc::clone(&comb_expr);
    let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
        Ok(Box::new(FoldAccumulator {
            state: factory_initial.clone(),
            input_schema: Arc::clone(&factory_input_schema),
            seq_schema: Arc::clone(&factory_seq_schema),
            combine_schema: Arc::clone(&factory_combine_schema),
            seq: Arc::clone(&factory_seq),
            combine: Arc::clone(&factory_combine),
        }))
    });
    create_udaf(
        "fold",
        vec![packed_type],
        Arc::new(state_type.clone()),
        Volatility::Immutable,
        factory,
        Arc::new(vec![state_type]),
    )
}
