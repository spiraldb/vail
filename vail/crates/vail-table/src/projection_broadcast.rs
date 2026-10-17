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
        // State must not cross a physical-plan rewrite/action boundary.
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
                && function.fun().signature().volatility == Volatility::Immutable
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{Int32Array, ListArray, RecordBatchOptions};
    use arrow_schema::Field;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature};
    use datafusion_physical_expr::expressions::{Column, Literal};

    #[derive(Debug)]
    struct NestedProbe {
        signature: Signature,
        calls: Arc<AtomicUsize>,
        error: bool,
        array_result: bool,
    }

    impl PartialEq for NestedProbe {
        fn eq(&self, other: &Self) -> bool {
            self.signature == other.signature
                && self.error == other.error
                && self.array_result == other.array_result
        }
    }
    impl Eq for NestedProbe {}
    impl Hash for NestedProbe {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.signature.hash(state);
            self.error.hash(state);
            self.array_result.hash(state);
        }
    }

    impl ScalarUDFImpl for NestedProbe {
        fn name(&self) -> &str {
            "nested_projection_probe"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(list_type())
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.error {
                return Err(DataFusionError::Execution("probe failure".into()));
            }
            let scalar = ScalarValue::new_list(
                &[
                    ScalarValue::Int32(Some(11)),
                    ScalarValue::Int32(None),
                    ScalarValue::Int32(Some(13)),
                ],
                &DataType::Int32,
                true,
            );
            let value = ScalarValue::List(scalar);
            if self.array_result {
                Ok(ColumnarValue::Array(
                    value.to_array_of_size(args.number_rows)?,
                ))
            } else {
                Ok(ColumnarValue::Scalar(value))
            }
        }
    }

    fn list_type() -> DataType {
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
    }

    fn empty_batch(rows: usize) -> RecordBatch {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap()
    }

    fn probe(
        volatility: Volatility,
        error: bool,
        array_result: bool,
    ) -> (Arc<dyn PhysicalExpr>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let udf = Arc::new(ScalarUDF::from(NestedProbe {
            signature: Signature::exact(vec![], volatility),
            calls: calls.clone(),
            error,
            array_result,
        }));
        let field = Arc::new(Field::new("value", list_type(), true));
        let expr = ScalarFunctionExpr::new(
            "nested_projection_probe",
            udf,
            vec![],
            field,
            Arc::new(ConfigOptions::default()),
        );
        (Arc::new(expr), calls)
    }

    #[test]
    fn scalar_nested_projection_reuses_same_size_broadcast_and_preserves_validity() {
        let (expr, calls) = probe(Volatility::Immutable, false, false);
        assert!(eligible(&expr, &Schema::empty()));
        let wrapped = Arc::new(ProjectionBroadcastExpr::new(expr));
        let first = wrapped
            .evaluate(&empty_batch(4))
            .unwrap()
            .into_array(4)
            .unwrap();
        let second = wrapped
            .evaluate(&empty_batch(4))
            .unwrap()
            .into_array(4)
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let lists = first.as_any().downcast_ref::<ListArray>().unwrap();
        for row in 0..4 {
            assert!(!lists.is_null(row));
            let values = lists.value(row);
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(values.value(0), 11);
            assert!(values.is_null(1));
            assert_eq!(values.value(2), 13);
        }

        // A different batch length is materialized transiently and does not
        // evict the retained common-size value.
        let other = wrapped
            .evaluate(&empty_batch(2))
            .unwrap()
            .into_array(2)
            .unwrap();
        assert_eq!(other.len(), 2);
        let again = wrapped
            .evaluate(&empty_batch(4))
            .unwrap()
            .into_array(4)
            .unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn empty_and_failed_projection_evaluations_are_lazy_and_errors_are_not_cached() {
        let (expr, calls) = probe(Volatility::Immutable, true, false);
        let wrapped = ProjectionBroadcastExpr::new(expr);
        let empty = wrapped
            .evaluate(&empty_batch(0))
            .unwrap()
            .into_array(0)
            .unwrap();
        assert_eq!(empty.len(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(wrapped.evaluate(&empty_batch(3)).is_err());
        assert!(wrapped.evaluate(&empty_batch(3)).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn partial_selection_preserves_inner_scalar_result_semantics() {
        let (expr, _) = probe(Volatility::Immutable, false, false);
        let wrapped = ProjectionBroadcastExpr::new(expr.clone());
        for selection in [
            arrow_array::BooleanArray::from(vec![Some(true), Some(false), Some(true)]),
            arrow_array::BooleanArray::from(vec![Some(true), None, Some(false)]),
        ] {
            let expected = expr
                .evaluate_selection(&empty_batch(3), &selection)
                .unwrap();
            let actual = wrapped
                .evaluate_selection(&empty_batch(3), &selection)
                .unwrap();
            match (expected, actual) {
                (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
                    assert_eq!(left, right)
                }
                (left, right) => panic!("selection results differ: {left:?} vs {right:?}"),
            }
        }
    }

    #[test]
    fn array_returning_immutable_udf_is_returned_unchanged_and_not_cached() {
        let (expr, calls) = probe(Volatility::Immutable, false, true);
        let wrapped = ProjectionBroadcastExpr::new(expr);
        let first = wrapped
            .evaluate(&empty_batch(3))
            .unwrap()
            .into_array(3)
            .unwrap();
        let second = wrapped
            .evaluate(&empty_batch(3))
            .unwrap()
            .into_array(3)
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(first.len(), 3);
    }

    #[test]
    fn concurrent_same_size_projection_calls_share_one_materialization() {
        let (expr, calls) = probe(Volatility::Immutable, false, false);
        let wrapped = Arc::new(ProjectionBroadcastExpr::new(expr));
        let workers = (0..8)
            .map(|_| {
                let wrapped = wrapped.clone();
                std::thread::spawn(move || {
                    wrapped
                        .evaluate(&empty_batch(7))
                        .unwrap()
                        .into_array(7)
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert!(
            results[1..]
                .iter()
                .all(|array| Arc::ptr_eq(&results[0], array))
        );
        assert!((1..=8).contains(&calls.load(Ordering::SeqCst)));
    }

    #[test]
    fn only_closed_immutable_nested_projection_roots_are_eligible() {
        let (immutable, _) = probe(Volatility::Immutable, false, false);
        assert!(eligible(&immutable, &Schema::empty()));
        let (volatile, _) = probe(Volatility::Volatile, false, false);
        assert!(!eligible(&volatile, &Schema::empty()));

        let schema = Schema::new(vec![Field::new("items", list_type(), true)]);
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("items", 0));
        assert!(!eligible(&column, &schema));

        let list = ScalarValue::new_list_nullable(
            &[ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(2))],
            &DataType::Int32,
        );
        let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::List(list)));
        assert!(eligible(&literal, &Schema::empty()));

        let properties = literal.get_properties(&[]).unwrap();
        let wrapped = ProjectionBroadcastExpr::new(literal);
        let wrapped_properties = wrapped.get_properties(&[]).unwrap();
        assert_eq!(format!("{wrapped_properties:?}"), format!("{properties:?}"));
    }

    #[test]
    fn rewrite_wraps_only_top_level_projection_expression_and_preserves_schema() {
        let list = ScalarValue::new_list_nullable(
            &[ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(2))],
            &DataType::Int32,
        );
        let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::List(list)));
        let source_schema = Arc::new(Schema::empty());
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion_physical_plan::empty::EmptyExec::new(source_schema),
        );
        let projection = ProjectionExec::try_new(
            [ProjectionExpr {
                expr: literal,
                alias: "nested".into(),
            }],
            input,
        )
        .unwrap();
        let original_schema = projection.schema();
        let rewritten = wrap_projection(&projection).unwrap().unwrap();
        assert_eq!(rewritten.schema(), original_schema);
        assert!(
            rewritten.expr()[0]
                .expr
                .downcast_ref::<ProjectionBroadcastExpr>()
                .is_some()
        );
    }
}
