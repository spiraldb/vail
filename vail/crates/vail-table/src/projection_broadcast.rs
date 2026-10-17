//! Reuse materialized nested constants at the projection boundary.
//!
//! DataFusion correctly represents closed expressions as scalars, then expands
//! them to the input batch length in every projection batch. For a large nested
//! value, that expansion copies the value's child buffers repeatedly. This
//! wrapper is applied only to eligible top-level projection expressions, after
//! expression evaluation, so nested UDF arguments retain their normal scalar
//! dispatch behavior.

use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, RecordBatch, new_empty_array};
use arrow_schema::{DataType, Schema};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr, expressions::Literal};
use datafusion_physical_plan::{
    ExecutionPlan,
    projection::{ProjectionExec, ProjectionExpr},
};

use crate::{Result as TableResult, plan::PhysicalPlan};

const MAX_RETAINED_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
struct BroadcastState {
    /// The single retained non-empty batch size and its fully materialized array.
    cached: Option<(usize, ArrayRef)>,
}

#[derive(Debug)]
struct ProjectionBroadcastExpr {
    inner: Arc<dyn PhysicalExpr>,
    state: Mutex<BroadcastState>,
}

impl ProjectionBroadcastExpr {
    fn new(inner: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            inner,
            state: Mutex::new(BroadcastState { cached: None }),
        }
    }
}

impl PartialEq for ProjectionBroadcastExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}
impl Eq for ProjectionBroadcastExpr {}
impl Hash for ProjectionBroadcastExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}
impl std::fmt::Display for ProjectionBroadcastExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProjectionBroadcast({})", self.inner)
    }
}

impl PhysicalExpr for ProjectionBroadcastExpr {
    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        self.inner.data_type(schema)
    }

    fn nullable(&self, schema: &Schema) -> Result<bool> {
        self.inner.nullable(schema)
    }

    fn return_field(&self, schema: &Schema) -> Result<Arc<arrow_schema::Field>> {
        self.inner.return_field(schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rows = batch.num_rows();
        let dtype = self.inner.data_type(batch.schema_ref())?;
        if rows == 0 {
            // Match the normal scalar expansion without evaluating a potentially
            // failing immutable expression on an empty input.
            return Ok(ColumnarValue::Array(new_empty_array(&dtype)));
        }

        {
            let state = self.state.lock().map_err(|_| {
                DataFusionError::Internal("projection broadcast cache poisoned".into())
            })?;
            if let Some((cached_rows, cached)) = &state.cached {
                if *cached_rows == rows {
                    return Ok(ColumnarValue::Array(cached.clone()));
                }
            }
        }

        // Preserve normal evaluation semantics. In particular, an immutable
        // nullary UDF is allowed to return a batch-sized Array; only Scalar
        // results need DataFusion's nested scalar expansion.
        let value = self.inner.evaluate(batch)?;
        let ColumnarValue::Scalar(scalar) = value else {
            return Ok(value);
        };
        if scalar.data_type() != dtype {
            return Err(DataFusionError::Internal(format!(
                "closed nested projection returned {:?}, expected {dtype:?}",
                scalar.data_type()
            )));
        }
        let array = scalar.to_array_of_size(rows)?;
        if array.len() != rows {
            return Err(DataFusionError::Internal(format!(
                "nested projection broadcast returned {} rows, expected {rows}",
                array.len()
            )));
        }
        if array.get_array_memory_size() <= MAX_RETAINED_BYTES {
            let mut state = self.state.lock().map_err(|_| {
                DataFusionError::Internal("projection broadcast cache poisoned".into())
            })?;
            if let Some((cached_rows, cached)) = &state.cached {
                if *cached_rows == rows {
                    return Ok(ColumnarValue::Array(cached.clone()));
                }
            } else {
                state.cached = Some((rows, array.clone()));
            }
        }
        Ok(ColumnarValue::Array(array))
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &arrow_array::BooleanArray,
    ) -> Result<ColumnarValue> {
        if selection.len() != batch.num_rows() {
            return Err(DataFusionError::Execution(format!(
                "Selection array length does not match batch row count: {} != {}",
                selection.len(),
                batch.num_rows()
            )));
        }
        if selection.null_count() == 0 && !selection.has_false() {
            return self.evaluate(batch);
        }
        // In particular, retain Scalar results for partial selections: the
        // default DataFusion selection semantics broadcast non-boolean scalars
        // across the original batch rather than scattering them as arrays.
        self.inner.evaluate_selection(batch, selection)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // Rewrites start fresh; retained immutable arrays may safely outlive an action.
        Ok(Arc::new(Self::new(
            self.inner.clone().with_new_children(children)?,
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }

    fn evaluate_bounds(
        &self,
        children: &[&datafusion_expr::interval_arithmetic::Interval],
    ) -> Result<datafusion_expr::interval_arithmetic::Interval> {
        self.inner.evaluate_bounds(children)
    }

    fn propagate_constraints(
        &self,
        interval: &datafusion_expr::interval_arithmetic::Interval,
        children: &[&datafusion_expr::interval_arithmetic::Interval],
    ) -> Result<Option<Vec<datafusion_expr::interval_arithmetic::Interval>>> {
        self.inner.propagate_constraints(interval, children)
    }

    fn get_properties(
        &self,
        children: &[datafusion_expr::sort_properties::ExprProperties],
    ) -> Result<datafusion_expr::sort_properties::ExprProperties> {
        self.inner.get_properties(children)
    }

    fn is_volatile_node(&self) -> bool {
        self.inner.is_volatile_node()
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        Ok(self
            .inner
            .snapshot()?
            .map(|expr| Arc::new(Self::new(expr)) as Arc<dyn PhysicalExpr>))
    }

    fn snapshot_generation(&self) -> u64 {
        self.inner.snapshot_generation()
    }

    fn placement(&self) -> datafusion_expr::ExpressionPlacement {
        self.inner.placement()
    }

    fn expression_id(&self) -> Option<u64> {
        self.inner.expression_id()
    }
}

fn is_nested(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    ) && !vail_ordering::has_type_metadata(dtype)
}

fn eligible(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    let Ok(dtype) = expr.data_type(schema) else {
        return false;
    };
    if !is_nested(&dtype) {
        return false;
    }
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        return literal.value().data_type() == dtype;
    }
    expr.downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|function| {
            function.args().is_empty()
                && (function.fun().signature().volatility == Volatility::Immutable
                    || vail_execution::logical_expr::is_cacheable_closed_semantic_kernel(
                        function.fun(),
                    ))
        })
}

fn wrap_projection(projection: &ProjectionExec) -> Result<Option<ProjectionExec>> {
    let input = projection.input().clone();
    let schema = input.schema();
    let mut changed = false;
    let expressions = projection
        .expr()
        .iter()
        .map(|projection_expr| {
            let expr = if eligible(&projection_expr.expr, &schema) {
                changed = true;
                Arc::new(ProjectionBroadcastExpr::new(projection_expr.expr.clone()))
                    as Arc<dyn PhysicalExpr>
            } else {
                projection_expr.expr.clone()
            };
            ProjectionExpr {
                expr,
                alias: projection_expr.alias.clone(),
            }
        })
        .collect::<Vec<_>>();
    if !changed {
        return Ok(None);
    }
    Ok(Some(ProjectionExec::try_new_with_schema_metadata(
        expressions,
        input,
        projection.schema().as_ref(),
    )?))
}

/// Insert bounded value caches only around closed nested values at ProjectionExec
/// output boundaries. The graph rewrite memoizes by physical node identity so a
/// shared plan subtree remains shared and each separately compiled plan gets
/// independent cache state.
pub(crate) fn cache_nested_projection_broadcasts(plan: PhysicalPlan) -> TableResult<PhysicalPlan> {
    fn rewrite(
        plan: PhysicalPlan,
        memo: &mut std::collections::HashMap<usize, PhysicalPlan>,
    ) -> datafusion_common::Result<PhysicalPlan> {
        let identity = Arc::as_ptr(&plan) as *const () as usize;
        if let Some(rewritten) = memo.get(&identity) {
            return Ok(rewritten.clone());
        }
        let mut children = Vec::with_capacity(plan.children().len());
        let mut changed = false;
        for child in plan.children() {
            let rewritten = rewrite(child.clone(), memo)?;
            changed |= !Arc::ptr_eq(child, &rewritten);
            children.push(rewritten);
        }
        let mut rewritten = if changed {
            plan.clone().with_new_children(children)?
        } else {
            plan
        };
        if let Some(projection) = rewritten.as_ref().downcast_ref::<ProjectionExec>()
            && let Some(cached) = wrap_projection(projection)?
        {
            rewritten = Arc::new(cached);
        }
        memo.insert(identity, rewritten.clone());
        Ok(rewritten)
    }

    rewrite(plan, &mut std::collections::HashMap::new()).map_err(Into::into)
}
