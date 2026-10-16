//! Stateful Hail-style fold implemented with compiled DataFusion expressions.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, ColumnarValue};
use datafusion_physical_expr::PhysicalExpr;

fn one_row(value: ColumnarValue) -> DfResult<ArrayRef> {
    let array: ArrayRef = match value {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(ScalarValue::List(array)) => array,
        ColumnarValue::Scalar(ScalarValue::LargeList(array)) => array,
        ColumnarValue::Scalar(ScalarValue::FixedSizeList(array)) => array,
        ColumnarValue::Scalar(ScalarValue::Struct(array)) => array,
        value => value.into_array(1)?,
    };
    if array.len() != 1 {
        return Err(DataFusionError::Execution(
            "fold state must contain one row".into(),
        ));
    }
    Ok(array)
}
fn normalize(array: &ArrayRef, dtype: &DataType) -> DfResult<ArrayRef> {
    if vail_ordering::same_layout(array.data_type(), dtype) {
        crate::logical_expr::restore_arrow_array_type(array, dtype)
    } else {
        let cast = arrow_cast::cast(array, dtype)?;
        crate::logical_expr::restore_arrow_array_type(&cast, dtype)
    }
}

#[derive(Debug)]
struct FoldAccumulator {
    state: ArrayRef,
    has_state: bool,
    input_schema: SchemaRef,
    seq_schema: SchemaRef,
    combine_schema: SchemaRef,
    seq: Arc<dyn PhysicalExpr>,
    combine: Arc<dyn PhysicalExpr>,
}

impl FoldAccumulator {
    fn evaluate_one(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
    ) -> DfResult<ArrayRef> {
        let array = one_row(expr.evaluate(batch)?)?;
        normalize(&array, self.state.data_type())
    }

    fn sequence(&mut self, rows: &StructArray, row: usize) -> DfResult<()> {
        let mut columns = Vec::with_capacity(rows.num_columns() + 1);
        columns.push(self.state.clone());
        columns.extend(rows.columns().iter().map(|column| column.slice(row, 1)));
        let batch = RecordBatch::try_new(Arc::clone(&self.seq_schema), columns)?;
        self.state = self.evaluate_one(&self.seq, &batch)?;
        self.has_state = true;
        Ok(())
    }

    fn combine_states(&self, left: ArrayRef, right: ArrayRef) -> DfResult<ArrayRef> {
        let batch = RecordBatch::try_new(Arc::clone(&self.combine_schema), vec![left, right])?;
        self.evaluate_one(&self.combine, &batch)
    }

    fn update_packed(&mut self, values: &ArrayRef) -> DfResult<()> {
        let rows = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("fold input must be a packed struct".into())
            })?;
        if rows.null_count() != 0 {
            return Err(DataFusionError::Execution(
                "fold packed records must be defined".into(),
            ));
        }
        if rows.fields() != self.input_schema.fields() {
            return Err(DataFusionError::Execution(
                "fold packed struct schema mismatch".into(),
            ));
        }
        for row in 0..rows.len() {
            self.sequence(rows, row)?;
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
        crate::expressions::scalar_at(&self.state, 0)
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.state.get_array_memory_size()
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [states] = states else {
            return Err(DataFusionError::Execution(
                "fold state must contain one value".into(),
            ));
        };
        for row in 0..states.len() {
            let partial = normalize(&states.slice(row, 1), self.state.data_type())?;
            // A final accumulator starts with the same initializer as a
            // partial one. Adopt the first partial rather than adding that
            // initializer a second time (it need not be a combine identity).
            self.state = if self.has_state {
                self.combine_states(self.state.clone(), partial)?
            } else {
                partial
            };
            self.has_state = true;
        }
        Ok(())
    }
}

/// A compiled fold body retains its typed source and binding context for native
/// semantic identity. Compiled closure addresses never identify the program.
#[derive(Clone)]
pub struct FoldProgram {
    identity: ProgramIdentity,
    physical: Arc<dyn PhysicalExpr>,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProgramIdentity {
    expression: crate::expression_identity::Identity,
    schema: SchemaRef,
    globals: SchemaRef,
    bindings: std::collections::BTreeMap<String, usize>,
    storage: Option<DataType>,
}
impl FoldProgram {
    pub fn lower(
        expression: &crate::expressions::Expr,
        schema: &Schema,
        globals: &Schema,
        bindings: &std::collections::BTreeMap<String, usize>,
    ) -> Result<Self, crate::expressions::ExprError> {
        Ok(Self {
            identity: ProgramIdentity {
                expression: crate::expression_identity::Identity::new(expression.clone()),
                schema: Arc::new(schema.clone()),
                globals: Arc::new(globals.clone()),
                bindings: bindings.clone(),
                storage: None,
            },
            physical: crate::expressions::lower_with_bindings(
                expression, schema, globals, bindings,
            )?,
        })
    }
    pub fn lower_with_state_type(
        expression: &crate::expressions::Expr,
        schema: &Schema,
        globals: &Schema,
        bindings: &std::collections::BTreeMap<String, usize>,
        storage: &DataType,
    ) -> Result<Self, crate::expressions::ExprError> {
        let mut program = Self::lower(expression, schema, globals, bindings)?;
        program.physical =
            crate::expressions::cast_physical_storage(program.physical, schema, storage.clone())?;
        program.identity.storage = Some(storage.clone());
        Ok(program)
    }
    pub fn output_type(&self) -> DfResult<DataType> {
        self.physical.data_type(&self.identity.schema)
    }
}

/// Construct a fold UDAF. `input_schema` describes the fields of the packed
/// input struct. `seq_expr` reads state at field 0 followed by those inputs;
/// `comb_expr` reads left state at field 0 and right state at field 1.
pub fn fold_udaf(
    input_schema: SchemaRef,
    initial: ScalarValue,
    state_type: DataType,
    sequence: FoldProgram,
    combine: FoldProgram,
) -> DfResult<AggregateUDF> {
    let mut seq_fields = vec![Arc::new(Field::new("state", state_type.clone(), true))];
    seq_fields.extend(input_schema.fields().iter().cloned());
    let seq_schema = Arc::new(Schema::new(seq_fields));
    let combine_schema = Arc::new(Schema::new(vec![
        Field::new("left", state_type.clone(), true),
        Field::new("right", state_type.clone(), true),
    ]));
    if initial.data_type() != state_type
        || sequence.output_type()? != state_type
        || combine.output_type()? != state_type
    {
        return Err(DataFusionError::Plan(
            "fold initial/state/sequence/combine types must agree".into(),
        ));
    }
    let packed_type = DataType::Struct(input_schema.fields().clone());
    let factory_initial = one_row(ColumnarValue::Scalar(initial.clone()))?;
    let factory_input_schema = Arc::clone(&input_schema);
    let factory_seq_schema = Arc::clone(&seq_schema);
    let factory_combine_schema = Arc::clone(&combine_schema);
    let identity = (initial, input_schema, sequence.identity, combine.identity);
    let factory_seq = sequence.physical;
    let factory_combine = combine.physical;
    let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
        Ok(Box::new(FoldAccumulator {
            state: factory_initial.clone(),
            has_state: false,
            input_schema: Arc::clone(&factory_input_schema),
            seq_schema: Arc::clone(&factory_seq_schema),
            combine_schema: Arc::clone(&factory_combine_schema),
            seq: Arc::clone(&factory_seq),
            combine: Arc::clone(&factory_combine),
        }))
    });
    Ok(Arc::unwrap_or_clone(
        crate::kernel_identity::aggregate_named(
            "fold",
            identity,
            vec![packed_type],
            state_type.clone(),
            vec![state_type],
            factory,
        ),
    ))
}
