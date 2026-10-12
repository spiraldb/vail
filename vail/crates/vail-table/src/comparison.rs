//! Hail Table._same lowered to DataFusion joins and similarity expressions.
use crate::plan::{PhysicalPlan, column, ordinal};
use crate::{Result, TableError, TableIR};
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, StructArray,
};
use arrow_schema::DataType;
use futures::TryStreamExt;
use std::sync::Arc;
use vail_execution::expressions::Type;

#[derive(Clone, Copy, Debug)]
pub struct SameOptions {
    pub tolerance: Option<f64>,
    pub absolute: bool,
    pub reorder_fields: bool,
}
impl Default for SameOptions {
    fn default() -> Self {
        Self {
            tolerance: Some(1e-6),
            absolute: false,
            reorder_fields: false,
        }
    }
}

#[derive(Debug)]
pub struct SameResult {
    pub is_same: bool,
    pub diagnostic: Option<String>,
}
impl SameResult {
    fn equal() -> Self {
        Self {
            is_same: true,
            diagnostic: None,
        }
    }
    fn different(reason: impl Into<String>) -> Self {
        Self {
            is_same: false,
            diagnostic: Some(format!("Table._same: {}", reason.into())),
        }
    }
}

impl TableIR {
    /// Compare logical schemas/keys first, then execute both lazy streams.
    /// Diagnostics report the first difference, without collecting whole groups.
    pub async fn same(
        self: &Arc<Self>,
        other: &Arc<Self>,
        options: SameOptions,
    ) -> Result<SameResult> {
        let left_fields = self.schema().fields();
        let right_fields = other.schema().fields();
        if left_fields.len() != right_fields.len() {
            return Ok(SameResult::different("different row fields"));
        }
        let mut pairs = Vec::new();
        for (i, field) in left_fields.iter().enumerate() {
            let j = if options.reorder_fields {
                match other.schema().index_of(field.name()) {
                    Ok(index) => index,
                    Err(_) => return Ok(SameResult::different("different row fields")),
                }
            } else {
                if field.name() != right_fields[i].name() {
                    return Ok(SameResult::different("different row fields or field order"));
                }
                i
            };
            // Arrow requiredness/field-ID metadata are physical distinctions,
            // whereas Hail's row type includes only names and logical types.
            if Type::from_arrow(field.data_type())?
                != Type::from_arrow(right_fields[j].data_type())?
            {
                return Ok(SameResult::different(format!(
                    "types differ at field {:?}",
                    field.name()
                )));
            }
            pairs.push((i, j));
        }
        if self.keys() != other.keys() {
            return Ok(SameResult::different("keys differ"));
        }
        let left_globals = self.globals().schema();
        let right_globals = other.globals().schema();
        if left_globals.fields().len() != right_globals.fields().len() {
            return Ok(SameResult::different("different global fields"));
        }
        let mut global_pairs = Vec::new();
        for (i, field) in left_globals.fields().iter().enumerate() {
            let j = if options.reorder_fields {
                match right_globals.index_of(field.name()) {
                    Ok(j) => j,
                    Err(_) => return Ok(SameResult::different("different global fields")),
                }
            } else {
                i
            };
            let right = &right_globals.fields()[j];
            if field.name() != right.name()
                || Type::from_arrow(field.data_type())? != Type::from_arrow(right.data_type())?
            {
                return Ok(SameResult::different(
                    "different global types or field order",
                ));
            }
            global_pairs.push((i, j));
        }
        if options.tolerance.is_some() {
            let left = crate::logical::physical_globals(self.globals()).await?;
            let right = crate::logical::physical_globals(other.globals()).await?;
            let offset = left.schema().fields().len();
            let joined: PhysicalPlan = crate::plan::cross_join(left, right)?;
            let fields = global_pairs
                .iter()
                .map(|(l, r)| (*l, offset + r))
                .collect::<Vec<_>>();
            if has_difference(joined, &[], &fields, None, options).await? {
                return Ok(SameResult::different("globals differ"));
            }
        }
        let plan = self.same_row_plan(other, options, &pairs).await?;
        if has_rows(plan).await? {
            Ok(SameResult::different(
                "row values, keys, or row counts differ",
            ))
        } else {
            Ok(SameResult::equal())
        }
    }

    async fn same_row_plan(
        self: &Arc<Self>,
        other: &Arc<Self>,
        options: SameOptions,
        pairs: &[(usize, usize)],
    ) -> Result<PhysicalPlan> {
        crate::logical_same::plan(self, other, options, pairs).await
    }

    pub(crate) fn same_from_inputs(
        self: &Arc<Self>,
        other: &Arc<Self>,
        options: SameOptions,
        pairs: &[(usize, usize)],
        mut left: PhysicalPlan,
        mut right: PhysicalPlan,
    ) -> Result<PhysicalPlan> {
        if options.tolerance.is_none() {
            // Only key-group presence matters for Hail's missing tolerance.
            // A constant GROUP BY key represents the sole unkeyed group and
            // produces no group for an empty input (unlike a global aggregate).
            left = distinct_keys(left, self.keys())?;
            right = distinct_keys(right, other.keys())?;
            let count = left.schema().fields().len();
            left = ordinal(left);
            right = ordinal(right);
            let on = (0..count)
                .map(|i| (column(&left, i), column(&right, i)))
                .collect();
            let offset = left.schema().fields().len();
            let joined: PhysicalPlan = Arc::new(HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Full,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNull,
                false,
            )?);
            return difference_plan(joined, &[], &[], Some((count, offset + count)), options);
        }
        left = ordinal(left);
        right = ordinal(right);
        let li = left.schema().fields().len() - 1;
        let ri = right.schema().fields().len() - 1;
        let offset = left.schema().fields().len();
        let on = vec![(column(&left, li), column(&right, ri))];
        // Ordinals align duplicate values without a many-to-many join. The
        // existing key order makes this equivalent to comparing collected key
        // groups, while a streaming merge join avoids collecting either table.
        let joined: PhysicalPlan = Arc::new(SortMergeJoinExec::try_new(
            left,
            right,
            on,
            None,
            JoinType::Full,
            vec![SortOptions::default()],
            NullEquality::NullEqualsNull,
        )?);
        let keys = self
            .keys()
            .iter()
            .map(|name| {
                Ok((
                    self.schema().index_of(name)?,
                    offset + other.schema().index_of(name)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let fields = pairs
            .iter()
            .map(|(l, r)| (*l, offset + r))
            .collect::<Vec<_>>();
        difference_plan(joined, &keys, &fields, Some((li, offset + ri)), options)
    }

    pub fn same_physical_plan(
        self: &Arc<Self>,
        other: &Arc<Self>,
        options: SameOptions,
    ) -> Result<String> {
        let pairs = self
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| Ok((i, other.schema().index_of(f.name())?)))
            .collect::<Result<Vec<_>>>()?;
        let left = self.clone();
        let right = other.clone();
        let plan = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| TableError::Execution(e.to_string()))?;
            runtime.block_on(left.same_row_plan(&right, options, &pairs))
        })
        .join()
        .map_err(|_| TableError::Execution("same plan diagnostic panicked".into()))??;
        Ok(datafusion_physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string())
    }
}

use arrow_schema::{Schema, SortOptions};
use datafusion_common::{DataFusionError, NullEquality, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_expr::{ColumnarValue, JoinType};
use datafusion_physical_expr::{PhysicalExpr, expressions::Literal};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    filter::FilterExec,
    joins::{HashJoinExec, PartitionMode, SortMergeJoinExec},
    limit::GlobalLimitExec,
};
use std::hash::{Hash, Hasher};

fn distinct_keys(input: PhysicalPlan, keys: &[String]) -> Result<PhysicalPlan> {
    let schema = input.schema();
    let fields = if keys.is_empty() {
        vec![(
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))) as Arc<dyn PhysicalExpr>,
            "__group".to_owned(),
        )]
    } else {
        keys.iter()
            .map(|name| {
                Ok((
                    vail_execution::ordering::canonical_key(column(&input, schema.index_of(name)?)),
                    name.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?
    };
    Ok(Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(fields),
        vec![],
        vec![],
        input,
        schema,
    )?))
}
async fn has_rows(plan: PhysicalPlan) -> Result<bool> {
    let mut output = plan.execute(0, Arc::new(TaskContext::default()))?;
    while let Some(batch) = output.try_next().await? {
        if batch.num_rows() > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}
async fn has_difference(
    plan: PhysicalPlan,
    keys: &[(usize, usize)],
    fields: &[(usize, usize)],
    presence: Option<(usize, usize)>,
    options: SameOptions,
) -> Result<bool> {
    has_rows(difference_plan(plan, keys, fields, presence, options)?).await
}
fn difference_plan(
    input: PhysicalPlan,
    keys: &[(usize, usize)],
    fields: &[(usize, usize)],
    presence: Option<(usize, usize)>,
    options: SameOptions,
) -> Result<PhysicalPlan> {
    let mut args = Vec::new();
    for (l, r) in presence
        .into_iter()
        .chain(keys.iter().copied())
        .chain(fields.iter().copied())
    {
        args.push(column(&input, l));
        args.push(column(&input, r));
    }
    let predicate = Arc::new(DifferentExpr {
        args,
        presence: presence.is_some(),
        exact_pairs: keys.len(),
        tolerance: options.tolerance.unwrap_or(0.0),
        absolute: options.absolute,
    });
    let filtered = Arc::new(FilterExec::try_new(predicate, input)?);
    Ok(Arc::new(GlobalLimitExec::new(filtered, 0, Some(1))))
}

/// Hail's recursive valuesSimilar semantics as a DataFusion scalar expression.
/// A true value denotes a mismatch, including an absent side of a full join.
#[derive(Debug)]
struct DifferentExpr {
    args: Vec<Arc<dyn PhysicalExpr>>,
    presence: bool,
    exact_pairs: usize,
    tolerance: f64,
    absolute: bool,
}
impl PartialEq for DifferentExpr {
    fn eq(&self, other: &Self) -> bool {
        self.args == other.args
            && self.presence == other.presence
            && self.exact_pairs == other.exact_pairs
            && self.tolerance.to_bits() == other.tolerance.to_bits()
            && self.absolute == other.absolute
    }
}
impl Eq for DifferentExpr {}
impl Hash for DifferentExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.args.hash(state);
        self.presence.hash(state);
        self.exact_pairs.hash(state);
        self.tolerance.to_bits().hash(state);
        self.absolute.hash(state);
    }
}
impl std::fmt::Display for DifferentExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HailValuesDiffer")
    }
}
impl PhysicalExpr for DifferentExpr {
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
    fn data_type(&self, _: &Schema) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(false)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        args: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        if args.len() != self.args.len() {
            return Err(DataFusionError::Plan(
                "valuesSimilar child count mismatch".into(),
            ));
        }
        Ok(Arc::new(Self {
            args,
            presence: self.presence,
            exact_pairs: self.exact_pairs,
            tolerance: self.tolerance,
            absolute: self.absolute,
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arrays = self
            .args
            .iter()
            .map(|arg| arg.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let mut result = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut different = self.presence && (arrays[0].is_null(row) || arrays[1].is_null(row));
            let start = if self.presence { 2 } else { 0 };
            if !different {
                for (i, pair) in arrays[start..].chunks_exact(2).enumerate() {
                    let exact = i < self.exact_pairs;
                    let same = if exact {
                        let left = vail_ordering::encode_value(pair[0].as_ref(), row)
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                        let right = vail_ordering::encode_value(pair[1].as_ref(), row)
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                        left == right
                    } else {
                        values_similar(
                            pair[0].as_ref(),
                            row,
                            pair[1].as_ref(),
                            row,
                            self.tolerance,
                            self.absolute,
                        )
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?
                    };
                    if !same {
                        different = true;
                        break;
                    }
                }
            }
            result.push(different);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(result))))
    }
}

fn float_similar(a: f64, b: f64, difference: f64, tolerance: f64, absolute: bool) -> bool {
    // Port of TFloat{32,64}.valuesSimilar and utils.D_==, including its absolute
    // floor and IEEE infinity behavior. Do not substitute Python math.isclose.
    a == b
        || (a.is_nan() && b.is_nan())
        || difference
            <= if absolute {
                tolerance
            } else {
                f64::MIN_POSITIVE + tolerance * a.abs().max(b.abs())
            }
}

fn values_similar(
    left: &dyn Array,
    li: usize,
    right: &dyn Array,
    ri: usize,
    tolerance: f64,
    absolute: bool,
) -> Result<bool> {
    if left.is_null(li) || right.is_null(ri) {
        return Ok(left.is_null(li) == right.is_null(ri));
    }
    macro_rules! values {
        ($array:ty) => {{
            let l = left
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| TableError::Invalid("comparison array type mismatch".into()))?;
            let r = right
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| TableError::Invalid("comparison array type mismatch".into()))?;
            (l.value(li), r.value(ri))
        }};
    }
    Ok(match left.data_type() {
        DataType::Int32 => {
            let (a, b) = values!(Int32Array);
            a == b
        }
        DataType::Int64 => {
            let (a, b) = values!(Int64Array);
            a == b
        }
        DataType::Boolean => {
            let (a, b) = values!(BooleanArray);
            a == b
        }
        DataType::Utf8 => {
            let (a, b) = values!(StringArray);
            a == b
        }
        DataType::Float32 => {
            let (a, b) = values!(Float32Array);
            let difference = if absolute {
                (a - b).abs() as f64
            } else {
                (a as f64 - b as f64).abs()
            };
            float_similar(a as f64, b as f64, difference, tolerance, absolute)
        }
        DataType::Float64 => {
            let (a, b) = values!(Float64Array);
            float_similar(a, b, (a - b).abs(), tolerance, absolute)
        }
        DataType::List(_) => {
            let (a, b) = values!(ListArray);
            if a.len() != b.len() {
                return Ok(false);
            }
            for i in 0..a.len() {
                if !values_similar(a.as_ref(), i, b.as_ref(), i, tolerance, absolute)? {
                    return Ok(false);
                }
            }
            true
        }
        DataType::Struct(_) => {
            let a = left
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| TableError::Invalid("comparison struct type mismatch".into()))?;
            let b = right
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| TableError::Invalid("comparison struct type mismatch".into()))?;
            for (a, b) in a.columns().iter().zip(b.columns()) {
                if !values_similar(a.as_ref(), li, b.as_ref(), ri, tolerance, absolute)? {
                    return Ok(false);
                }
            }
            true
        }
        dtype => {
            return Err(TableError::Invalid(format!(
                "unsupported comparison type {dtype}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hail_relative_tolerance_and_ieee_rules() {
        assert!(float_similar(
            0.0,
            f64::MIN_POSITIVE,
            f64::MIN_POSITIVE,
            0.0,
            false
        ));
        assert!(!float_similar(
            0.0,
            f64::MIN_POSITIVE,
            f64::MIN_POSITIVE,
            0.0,
            true
        ));
        assert!(float_similar(
            1.0,
            f64::INFINITY,
            f64::INFINITY,
            1e-6,
            false
        ));
        assert!(!float_similar(
            1.0,
            f64::INFINITY,
            f64::INFINITY,
            1e-6,
            true
        ));
        assert!(float_similar(f64::NAN, f64::NAN, f64::NAN, 0.0, false));
    }
}
