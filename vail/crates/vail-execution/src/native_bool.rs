//! Total Boolean adapters over DataFusion's native `bool_or` and `bool_and`.
//!
//! Hail treats missing values as ignored and gives empty/all-missing inputs a
//! Boolean identity. DataFusion's native kernels and grouped accumulators
//! already implement the Boolean reduction and nullable partial state; this
//! adapter changes only finalized output values.
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF,
    Signature, Volatility,
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::AggregateOrderSensitivity,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Operation {
    Any,
    All,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::All => "all",
        }
    }

    fn identity(self) -> bool {
        matches!(self, Self::All)
    }

    fn native(self) -> Arc<AggregateUDF> {
        match self {
            Self::Any => datafusion_functions_aggregate::bool_and_or::bool_or_udaf(),
            Self::All => datafusion_functions_aggregate::bool_and_or::bool_and_udaf(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TotalBoolUdf {
    operation: Operation,
    signature: Signature,
}

pub(crate) fn total_bool_udaf(name: &str) -> Arc<AggregateUDF> {
    let operation = match name {
        "any" => Operation::Any,
        "all" => Operation::All,
        _ => unreachable!("total Boolean aggregate name was validated by its caller"),
    };
    Arc::new(AggregateUDF::from(TotalBoolUdf {
        operation,
        signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
    }))
}

impl AggregateUDFImpl for TotalBoolUdf {
    fn name(&self) -> &str {
        self.operation.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.operation.native().return_type(args)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn default_value(&self, dtype: &DataType) -> Result<ScalarValue> {
        if dtype == &DataType::Boolean {
            Ok(ScalarValue::Boolean(Some(self.operation.identity())))
        } else {
            self.operation.native().default_value(dtype)
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(TotalBoolAccumulator {
            inner: self.operation.native().inner().accumulator(args)?,
            identity: self.operation.identity(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.operation.native().inner().state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.operation
            .native()
            .inner()
            .groups_accumulator_supported(args)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.operation.native().order_sensitivity()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.operation.native().reverse_udf()
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(TotalBoolGroupsAccumulator {
            inner: self
                .operation
                .native()
                .inner()
                .create_groups_accumulator(args)?,
        }))
    }
}

#[derive(Debug)]
struct TotalBoolAccumulator {
    inner: Box<dyn Accumulator>,
    identity: bool,
}

impl Accumulator for TotalBoolAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.inner.evaluate()? {
            ScalarValue::Boolean(None) => Ok(ScalarValue::Boolean(Some(self.identity))),
            result => Ok(result),
        }
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
}

struct TotalBoolGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
}

impl std::fmt::Debug for TotalBoolGroupsAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TotalBoolGroupsAccumulator")
            .finish_non_exhaustive()
    }
}

impl GroupsAccumulator for TotalBoolGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = self.inner.evaluate(emit_to)?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "native Boolean aggregate returned non-Boolean groups".into(),
                )
            })?;
        // Native AND/OR group accumulators store their operation identity under
        // null validity bits for groups with no non-null input. Making those
        // values valid implements Hail's total-result identity.
        Ok(Arc::new(BooleanArray::new(values.values().clone(), None)))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(values, group_indices, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, opt_filter)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}
