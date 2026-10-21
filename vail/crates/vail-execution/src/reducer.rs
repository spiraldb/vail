//! One registry for Hail reducers, shared by relational, list and scan adapters.
//!
//! Adapters choose the layout and lifetime of an accumulator. Aggregate
//! algorithms, empty identities and merge states are defined only by the UDAF.
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_expr::AggregateUDF;

use crate::expressions::ExprError;

fn configured_udaf_impl(
    function: &str,
    dtype: Option<&DataType>,
    options: &serde_json::Value,
) -> Result<Arc<AggregateUDF>, ExprError> {
    if let Some(dtype) = dtype {
        if let Some(udf) =
            crate::advanced_collection_aggregates::make_udaf(function, dtype, options)?
        {
            return Ok(udf);
        }
        if let Some(udf) = crate::advanced_numeric_aggregates::make_udaf(function, dtype, options)?
        {
            return Ok(udf);
        }
        if let Some(udf) = crate::advanced_genomic_aggregates::make_udaf(function, dtype, options)?
        {
            return Ok(udf);
        }
    }
    crate::aggregates::aggregate_udf(function, dtype)
}

/// Native identity is independent of the accumulator factory closure address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReducerIdentity {
    function: String,
    input: Option<DataType>,
    options: serde_json::Value,
    output: DataType,
    beneficial_ordering: Option<bool>,
    reversed: bool,
}
#[derive(Debug, Clone)]
struct IdentifiedReducer {
    identity: ReducerIdentity,
    inner: Arc<AggregateUDF>,
}
impl PartialEq for IdentifiedReducer {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}
impl Eq for IdentifiedReducer {}
impl std::hash::Hash for IdentifiedReducer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}
pub fn configured_udaf(
    function: &str,
    dtype: Option<&DataType>,
    options: &serde_json::Value,
) -> Result<Arc<AggregateUDF>, ExprError> {
    // DataFusion's array_agg owns the optimized grouped accumulator. Returning
    // it directly preserves that specialization instead of wrapping it in an
    // identity UDAF.
    if function == "collect" {
        dtype.ok_or_else(|| ExprError::Invalid("collect requires input".into()))?;
        return Ok(datafusion_functions_aggregate::array_agg::array_agg_udaf());
    }
    let inner = configured_udaf_impl(function, dtype, options)?;
    let output = inner.return_type(&dtype.cloned().into_iter().collect::<Vec<_>>())?;
    Ok(Arc::new(AggregateUDF::from(IdentifiedReducer {
        identity: ReducerIdentity {
            function: function.to_owned(),
            input: dtype.cloned(),
            options: options.clone(),
            output,
            beneficial_ordering: None,
            reversed: false,
        },
        inner,
    })))
}
use arrow_schema::FieldRef;
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Expr, GroupsAccumulator, Operator, ReversedUDAF,
    SetMonotonicity, Signature, StatisticsArgs,
    expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams},
    function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
    utils::AggregateOrderSensitivity,
};
impl AggregateUDFImpl for IdentifiedReducer {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn schema_name(&self, params: &AggregateFunctionParams) -> DfResult<String> {
        self.inner.schema_name(params)
    }
    fn human_display(&self, params: &AggregateFunctionParams) -> DfResult<String> {
        self.inner.human_display(params)
    }
    fn window_function_schema_name(&self, params: &WindowFunctionParams) -> DfResult<String> {
        self.inner.window_function_schema_name(params)
    }
    fn display_name(&self, params: &AggregateFunctionParams) -> DfResult<String> {
        self.inner.display_name(params)
    }
    fn window_function_display_name(&self, params: &WindowFunctionParams) -> DfResult<String> {
        self.inner.window_function_display_name(params)
    }
    fn return_type(&self, args: &[DataType]) -> DfResult<DataType> {
        self.inner.return_type(args)
    }
    fn return_field(&self, args: &[FieldRef]) -> DfResult<FieldRef> {
        self.inner.return_field(args)
    }
    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }
    fn accumulator(&self, args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        self.inner.accumulator(args)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DfResult<Box<dyn GroupsAccumulator>> {
        self.inner.create_groups_accumulator(args)
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        self.inner.create_sliding_accumulator(args)
    }
    fn coerce_types(&self, args: &[DataType]) -> DfResult<Vec<DataType>> {
        self.inner.coerce_types(args)
    }
    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial: bool,
    ) -> DfResult<Option<Arc<dyn AggregateUDFImpl>>> {
        self.inner
            .inner()
            .clone()
            .with_beneficial_ordering(beneficial)
            .map(|updated| {
                updated.map(|inner| {
                    let mut identity = self.identity.clone();
                    identity.beneficial_ordering = Some(beneficial);
                    Arc::new(Self {
                        identity,
                        inner: Arc::new(AggregateUDF::new_from_shared_impl(inner)),
                    }) as Arc<dyn AggregateUDFImpl>
                })
            })
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }
    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify()
    }
    fn simplify_expr_op_literal(
        &self,
        agg: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        lit: &Expr,
        left: bool,
    ) -> DfResult<Option<Expr>> {
        self.inner.simplify_expr_op_literal(agg, arg, op, lit, left)
    }
    fn reverse_expr(&self) -> ReversedUDAF {
        match self.inner.reverse_udf() {
            ReversedUDAF::Reversed(inner) => {
                let mut identity = self.identity.clone();
                identity.reversed = !identity.reversed;
                ReversedUDAF::Reversed(Arc::new(AggregateUDF::from(Self { identity, inner })))
            }
            unchanged => unchanged,
        }
    }
    fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
    }
    fn value_from_stats(&self, args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(args)
    }
    fn default_value(&self, dtype: &DataType) -> DfResult<ScalarValue> {
        self.inner.default_value(dtype)
    }
    fn supports_null_handling_clause(&self) -> bool {
        self.inner.supports_null_handling_clause()
    }
    fn supports_within_group_clause(&self) -> bool {
        self.inner.supports_within_group_clause()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
    fn set_monotonicity(&self, dtype: &DataType) -> SetMonotonicity {
        self.inner.inner().set_monotonicity(dtype)
    }
}
