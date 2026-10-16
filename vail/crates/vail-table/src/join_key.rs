//! Checked, total key predicates for partition clipping, including nested nulls.
use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, RecordBatch, builder::BooleanBuilder,
};
use arrow_schema::{DataType, Schema, SortOptions};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr::PhysicalExpr;
use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};
use vail_storage::fragment_index::{encode_typed_key_component, make_typed_key_comparators};

pub(crate) fn comparators(
    left: &[ArrayRef],
    right: &[ArrayRef],
    options: &[SortOptions],
) -> Result<Vec<arrow_ord::ord::DynComparator>> {
    if left.len() != right.len()
        || left
            .iter()
            .zip(right)
            .any(|(a, b)| a.data_type() != b.data_type())
    {
        return Err(DataFusionError::Execution(
            "key comparator storage mismatch".into(),
        ));
    }
    let native = make_typed_key_comparators(left, right, options)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(native
        .into_iter()
        .enumerate()
        .map(|(i, compare)| {
            // Ordinary top-level float joins equate signed zeros. All nested
            // comparisons retain the native typed comparator's bit policy.
            match left[i].data_type() {
                DataType::Float32 => {
                    let a = left[i]
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap()
                        .clone();
                    let b = right[i]
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap()
                        .clone();
                    Box::new(move |l, r| {
                        if a.is_valid(l) && b.is_valid(r) && a.value(l) == b.value(r) {
                            Ordering::Equal
                        } else {
                            compare(l, r)
                        }
                    }) as arrow_ord::ord::DynComparator
                }
                DataType::Float64 => {
                    let a = left[i]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .clone();
                    let b = right[i]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .clone();
                    Box::new(move |l, r| {
                        if a.is_valid(l) && b.is_valid(r) && a.value(l) == b.value(r) {
                            Ordering::Equal
                        } else {
                            compare(l, r)
                        }
                    }) as arrow_ord::ord::DynComparator
                }
                _ => compare,
            }
        })
        .collect())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Relation {
    Equal,
    Less,
    AtMost,
    Greater,
    AtLeast,
}
#[derive(Debug)]
struct KeyPredicate {
    args: Vec<Arc<dyn PhysicalExpr>>,
    values: Vec<ArrayRef>,
    encoded: Vec<Vec<u8>>,
    relation: Relation,
}
impl PartialEq for KeyPredicate {
    fn eq(&self, other: &Self) -> bool {
        self.args == other.args && self.encoded == other.encoded && self.relation == other.relation
    }
}
impl Eq for KeyPredicate {}
impl Hash for KeyPredicate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.args.hash(state);
        self.encoded.hash(state);
        self.relation.hash(state);
    }
}
impl fmt::Display for KeyPredicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VailKeyPredicate({:?})", self.relation)
    }
}
pub(crate) fn predicate(
    args: &[Arc<dyn PhysicalExpr>],
    values: &[ArrayRef],
    op: Operator,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    if args.len() != values.len() || values.iter().any(|value| value.len() != 1) {
        return Err(DataFusionError::Plan(
            "key predicate requires matching single-row endpoints".into(),
        ));
    }
    let values = args
        .iter()
        .zip(values)
        .map(|(arg, value)| {
            let dtype = arg.data_type(schema)?;
            let logical = vail_execution::expressions::Type::from_arrow(&dtype)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            if vail_execution::expressions::Type::from_arrow(value.data_type())
                .map_err(|e| DataFusionError::Plan(e.to_string()))?
                != logical
            {
                return Err(DataFusionError::Plan(
                    "key endpoint logical type mismatch".into(),
                ));
            }
            Ok(arrow_cast::cast(value, &dtype)?)
        })
        .collect::<Result<Vec<_>>>()?;
    let encoded = values
        .iter()
        .map(|value| {
            encode_typed_key_component(value).map_err(|e| DataFusionError::Plan(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    let relation = match op {
        Operator::Eq => Relation::Equal,
        Operator::Lt => Relation::Less,
        Operator::LtEq => Relation::AtMost,
        Operator::Gt => Relation::Greater,
        Operator::GtEq => Relation::AtLeast,
        _ => return Err(DataFusionError::Plan("unsupported key comparison".into())),
    };
    Ok(Arc::new(KeyPredicate {
        args: args.to_vec(),
        values,
        encoded,
        relation,
    }))
}
impl PhysicalExpr for KeyPredicate {
    fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self}")
    }
    fn data_type(&self, schema: &Schema) -> Result<DataType> {
        for (arg, value) in self.args.iter().zip(&self.values) {
            if arg.data_type(schema)? != *value.data_type() {
                return Err(DataFusionError::Plan(
                    "key predicate input storage changed".into(),
                ));
            }
        }
        Ok(DataType::Boolean)
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(false)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        args: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if args.len() != self.args.len() {
            return Err(DataFusionError::Plan(
                "key predicate child count changed".into(),
            ));
        }
        Ok(Arc::new(Self {
            args,
            values: self.values.clone(),
            encoded: self.encoded.clone(),
            relation: self.relation,
        }))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arrays = self
            .args
            .iter()
            .map(|arg| arg.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        if arrays.iter().zip(&self.values).any(|(array, value)| {
            array.len() != batch.num_rows() || array.data_type() != value.data_type()
        }) {
            return Err(DataFusionError::Execution(
                "key predicate input schema or cardinality changed".into(),
            ));
        }
        let options = vec![
            SortOptions {
                descending: false,
                nulls_first: false
            };
            arrays.len()
        ];
        let compare = comparators(&arrays, &self.values, &options)?;
        let accept: fn(Ordering) -> bool = match self.relation {
            Relation::Equal => |x| x.is_eq(),
            Relation::Less => |x| x.is_lt(),
            Relation::AtMost => |x| !x.is_gt(),
            Relation::Greater => |x| x.is_gt(),
            Relation::AtLeast => |x| !x.is_lt(),
        };
        let mut output = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let ordering = compare
                .iter()
                .map(|compare| compare(row, 0))
                .find(|order| !order.is_eq())
                .unwrap_or(Ordering::Equal);
            output.append_value(accept(ordering));
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}
