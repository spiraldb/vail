//! Keep large constants out of optimizer expression identity and formatting.
//!
//! Existing Arrow nested values become opaque, shared scalar parameters. Large pure,
//! closed repeat/range generators are evaluated lazily once per lowered plan,
//! with Arrow buffers shared by its consumers. This is a targeted generation
//! policy for unary expression nodes, not a general constant-evaluation cost model.
//! Multi-input expressions (notably side-specific join keys) remain untouched.
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, new_empty_array};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, DataFusionError, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{
    ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_expr::{
    PhysicalExpr, create_physical_expr, execution_props::ExecutionProps,
};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};

const LARGE_VALUE_BYTES: usize = 64 * 1024;
const LARGE_REPEAT_COUNT: i64 = 8192;
static NEXT_PARAMETER: AtomicU64 = AtomicU64::new(1);

/// Keep large constants opaque to expression optimization, using a raw Arrow
/// input column as the non-foldable dependency. No subquery execution context
/// is needed when a prepared relation is embedded under a join or replayed.
pub(crate) fn parameterize(
    plan: LogicalPlan,
    execution_props: &ExecutionProps,
) -> datafusion_common::Result<LogicalPlan> {
    Ok(plan
        .transform_up_with_subqueries(|plan| {
            if plan.inputs().len() != 1 || plan.expressions().is_empty() {
                return Ok(Transformed::no(plan));
            }
            // Unaffected nodes need no seed relation or schema reconstruction.
            let mut candidate = false;
            for expression in plan.expressions() {
                if expression.exists(|expr| {
                    Ok(match expr {
                        Expr::ScalarFunction(function) => {
                            parameterized_generator(function).is_some()
                        }
                        Expr::Literal(value, _) => is_large_nested_value(value),
                        _ => false,
                    })
                })? {
                    candidate = true;
                    break;
                }
            }
            if !candidate {
                return Ok(Transformed::no(plan));
            }
            let original = plan;
            let output_columns = original.schema().columns();
            // A column produced by a projection may be substituted with a
            // literal during optimization. Bind to a raw Arrow scan instead,
            // so immutable constants remain deferred after projection merging.
            let (seed, column) = seed_scan()?;
            let mut inputs = original.inputs().into_iter().cloned().collect::<Vec<_>>();
            // DataFusion buffers the left side of CrossJoinExec. Keep the
            // singleton there; stream the actual relation on its right side.
            inputs[0] = LogicalPlanBuilder::from(seed)
                .cross_join(inputs[0].clone())?
                .build()?;
            let plan = original.with_new_exprs(original.expressions(), inputs)?;
            let dependency = Expr::Column(column);
            let rewritten = plan.map_expressions(|expr| {
                expr.transform_down(|expr| match expr {
                    Expr::ScalarFunction(function) => {
                        let large = parameterized_generator(&function).is_some();
                        let expr = Expr::ScalarFunction(function);
                        if large && is_closed_pure(&expr)? {
                            Ok(Transformed::yes(defer_constant(
                                expr,
                                execution_props,
                                dependency.clone(),
                            )?))
                        } else {
                            Ok(Transformed::no(expr))
                        }
                    }
                    Expr::Literal(value, metadata) if is_large_nested_value(&value) => {
                        let _ = scalar_array(&value)?; // Validate singleton cardinality without copying buffers.
                        Ok(Transformed::yes(defer_constant(
                            Expr::Literal(value, metadata),
                            execution_props,
                            dependency.clone(),
                        )?))
                    }
                    expr => Ok(Transformed::no(expr)),
                })
            })?;
            if !rewritten.transformed {
                return Ok(Transformed::no(original));
            }
            let plan = {
                // The seed is an input dependency, never an observable output field.
                LogicalPlanBuilder::from(rewritten.data)
                    .project(
                        output_columns
                            .into_iter()
                            .map(Expr::Column)
                            .collect::<Vec<_>>(),
                    )?
                    .build()?
            };
            Ok(Transformed::yes(plan))
        })?
        .data)
}

/// A one-row Arrow input supplies cardinality when the original input has zero
/// columns. Cross joining it preserves zero, one or many input rows.
fn seed_scan() -> datafusion_common::Result<(LogicalPlan, Column)> {
    let id = NEXT_PARAMETER.fetch_add(1, Ordering::Relaxed);
    let field = Field::new(format!("__vail_constant_seed_{id}"), DataType::UInt8, false);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![ScalarValue::UInt8(Some(0)).to_array_of_size(1)?],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    let scan = LogicalPlanBuilder::scan(
        format!("__vail_constant_parameters_{id}"),
        provider_as_source(Arc::new(provider)),
        None,
    )?
    .build()?;
    let column = scan.schema().columns().remove(0);
    Ok((scan, column))
}

/// Only closed immutable scalar trees are eligible. In particular, a large
/// repeat count does not make a column-dependent repeat a constant.
fn is_closed_pure(expr: &Expr) -> datafusion_common::Result<bool> {
    use datafusion_common::tree_node::TreeNodeRecursion;
    let mut pure = true;
    expr.apply(|node| {
        let allowed = match node {
            Expr::Literal(..)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::BinaryExpr(_)
            | Expr::Negative(_)
            | Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Case(_)
            | Expr::Alias(_) => true,
            Expr::ScalarFunction(call) => call.func.signature().volatility == Volatility::Immutable,
            _ => false,
        };
        if !allowed {
            pure = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(pure)
}

/// One state object is owned by a lowered plan, shared by its partitions.
/// The parameter argument prevents plan-time folding without misrepresenting
/// the captured expression's purity. Only a selected nonempty batch computes
/// the value; its Arrow buffers (or deterministic error) are then shared.
/// Explicit replay may reuse the value with that plan. Fresh lowering creates
/// fresh state; the source IR and global process state never own cached data.
struct DeferredConstant {
    id: u64,
    signature: Signature,
    dtype: DataType,
    field: FieldRef,
    expression: Arc<dyn PhysicalExpr>,
    input: RecordBatch,
    value: OnceLock<std::result::Result<ScalarValue, Arc<DataFusionError>>>,
    initialization: Mutex<()>,
}

impl DeferredConstant {
    fn get(&self) -> datafusion_common::Result<ScalarValue> {
        if self.value.get().is_none() {
            let _guard = self.initialization.lock().map_err(|_| {
                datafusion_common::internal_datafusion_err!(
                    "deferred constant initialization panicked"
                )
            })?;
            if self.value.get().is_none() {
                let result =
                    self.expression
                        .evaluate(&self.input)
                        .and_then(|result| match result {
                            ColumnarValue::Scalar(value) => Ok(value),
                            ColumnarValue::Array(array) if array.len() == 1 => {
                                ScalarValue::try_from_array(array.as_ref(), 0)
                            }
                            ColumnarValue::Array(array) => datafusion_common::exec_err!(
                                "closed scalar expression produced {} rows",
                                array.len()
                            ),
                        });
                if let Err(error) = &result {
                    if !deterministic_failure(error) {
                        return result;
                    }
                }
                // Only this initializer can set the slot while holding the lock.
                let _ = self.value.set(result.map_err(Arc::new));
            }
        }
        match self
            .value
            .get()
            .expect("initializer filled the constant slot")
        {
            Ok(value) => Ok(value.clone()),
            Err(error) => Err(DataFusionError::Shared(error.clone())),
        }
    }
}

/// Cache expression failures, not cancellation, I/O or resource exhaustion.
fn deterministic_failure(error: &DataFusionError) -> bool {
    use arrow_schema::ArrowError;
    match error {
        DataFusionError::Execution(_) => true,
        DataFusionError::ArrowError(error, _) => matches!(
            error.as_ref(),
            ArrowError::CastError(_)
                | ArrowError::ParseError(_)
                | ArrowError::ComputeError(_)
                | ArrowError::DivideByZero
                | ArrowError::ArithmeticOverflow(_)
                | ArrowError::InvalidArgumentError(_)
                | ArrowError::OffsetOverflowError(_)
        ),
        DataFusionError::Context(_, error) | DataFusionError::Diagnostic(_, error) => {
            deterministic_failure(error)
        }
        DataFusionError::Shared(error) => deterministic_failure(error),
        _ => false,
    }
}

impl std::fmt::Debug for DeferredConstant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredConstant")
            .field("id", &self.id)
            .field("dtype", &self.dtype)
            .finish_non_exhaustive()
    }
}
impl PartialEq for DeferredConstant {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for DeferredConstant {}
impl std::hash::Hash for DeferredConstant {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for DeferredConstant {
    fn name(&self) -> &str {
        "vail_deferred_constant"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn schema_name(&self, _: &[Expr]) -> datafusion_common::Result<String> {
        Ok(format!("vail_deferred_constant_{}", self.id))
    }
    fn return_type(&self, args: &[DataType]) -> datafusion_common::Result<DataType> {
        if args.len() != 1 {
            return datafusion_common::plan_err!("deferred constant requires its Arrow parameter");
        }
        Ok(self.dtype.clone())
    }
    fn return_field_from_args(&self, _: ReturnFieldArgs) -> datafusion_common::Result<FieldRef> {
        Ok(self.field.clone())
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.dtype)));
        }
        Ok(ColumnarValue::Scalar(self.get()?))
    }
}

fn defer_constant(
    expr: Expr,
    execution_props: &ExecutionProps,
    dependency: Expr,
) -> datafusion_common::Result<Expr> {
    let expression = create_physical_expr(
        &expr,
        &DFSchema::empty(),
        execution_props,
        &PhysicalPlanningContext::default(),
    )?;
    let schema = Arc::new(Schema::empty());
    let id = NEXT_PARAMETER.fetch_add(1, Ordering::Relaxed);
    let dtype = expression.data_type(&schema)?;
    let field = Arc::new(
        expression
            .return_field(&schema)?
            .as_ref()
            .clone()
            .with_name(format!("vail_deferred_constant_{id}")),
    );
    let input = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    let kernel = DeferredConstant {
        id,
        signature: Signature::any(1, Volatility::Immutable),
        dtype,
        field,
        expression,
        input,
        value: OnceLock::new(),
        initialization: Mutex::new(()),
    };
    Ok(ScalarUDF::from(kernel).call(vec![dependency]))
}

fn should_parameterize_repeat(function: &ScalarFunction) -> bool {
    if function.func.name() != "array_repeat" || function.args.len() != 2 {
        return false;
    }
    let Some(count) = integer_literal(&function.args[1]) else {
        return false;
    };
    if count >= LARGE_REPEAT_COUNT {
        return true;
    }
    if count <= 0 {
        return false;
    }
    literal_value(&function.args[0])
        .map(estimated_scalar_value_bytes)
        .is_some_and(|bytes| bytes.saturating_mul(count as usize) >= LARGE_VALUE_BYTES)
}

#[derive(Clone, Copy)]
enum ParameterizedGenerator {
    Repeat,
    Range,
}

fn parameterized_generator(function: &ScalarFunction) -> Option<ParameterizedGenerator> {
    if should_parameterize_repeat(function) {
        Some(ParameterizedGenerator::Repeat)
    } else if should_parameterize_range(function) {
        Some(ParameterizedGenerator::Range)
    } else {
        None
    }
}

fn should_parameterize_range(function: &ScalarFunction) -> bool {
    if function.func.name() != "vail_range_int32" || function.args.len() != 3 {
        return false;
    }
    estimated_range_length(&function.args)
        .is_some_and(|length| length >= i128::from(LARGE_REPEAT_COUNT))
}

fn estimated_range_length(arguments: &[Expr]) -> Option<i128> {
    let [start, stop, step] = arguments else {
        return None;
    };
    let start = i128::from(integer_literal(start)?);
    let stop = i128::from(integer_literal(stop)?);
    let step = i128::from(integer_literal(step)?);
    if step == 0 {
        return None;
    }
    let distance = if step > 0 { stop - start } else { start - stop };
    if distance <= 0 {
        return Some(0);
    }
    let step_magnitude = step.abs();
    Some((distance + step_magnitude - 1) / step_magnitude)
}

fn estimated_scalar_value_bytes(value: &ScalarValue) -> usize {
    value
        .data_type()
        .primitive_width()
        .unwrap_or_else(|| value.size())
}

fn integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(value, _) => integer_value(value),
        Expr::Cast(cast) => integer_literal(&cast.expr),
        Expr::TryCast(cast) => integer_literal(&cast.expr),
        _ => None,
    }
}

fn integer_value(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn literal_value(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value),
        Expr::Cast(cast) => literal_value(&cast.expr),
        Expr::TryCast(cast) => literal_value(&cast.expr),
        _ => None,
    }
}

fn is_large_nested_value(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::List(_)
            | ScalarValue::LargeList(_)
            | ScalarValue::FixedSizeList(_)
            | ScalarValue::ListView(_)
            | ScalarValue::LargeListView(_)
            | ScalarValue::Struct(_)
            | ScalarValue::Map(_)
    ) && value.size() >= LARGE_VALUE_BYTES
}

fn scalar_array(value: &ScalarValue) -> datafusion_common::Result<ArrayRef> {
    let array: Option<ArrayRef> = match value {
        ScalarValue::List(array) => Some(array.clone()),
        ScalarValue::LargeList(array) => Some(array.clone()),
        ScalarValue::FixedSizeList(array) => Some(array.clone()),
        ScalarValue::ListView(array) => Some(array.clone()),
        ScalarValue::LargeListView(array) => Some(array.clone()),
        ScalarValue::Struct(array) => Some(array.clone()),
        ScalarValue::Map(array) => Some(array.clone()),
        _ => None,
    };
    let array = match array {
        Some(array) => array,
        None => value.to_array_of_size(1)?,
    };
    if array.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "scalar parameter must contain one value, got {}",
            array.len()
        )));
    }
    Ok(array)
}
