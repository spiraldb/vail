//! Hail-compatible numeric and Boolean DataFusion aggregate UDAFs.
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, Volatility, expr_fn::create_udaf};

use crate::expressions::ExprError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateKind {
    Count,
    Sum,
    Product,
    Min,
    Max,
    Mean,
    CountWhere,
    Fraction,
    Any,
    All,
    Stddev,
}
#[derive(Debug)]
struct HailAccumulator {
    kind: AggregateKind,
    input_type: DataType,
    count: i64,
    integer_value: i64,
    float_value: f64,
    sum_squares: f64,
    seen: bool,
}
impl HailAccumulator {
    fn new(kind: AggregateKind, ty: DataType) -> Self {
        Self {
            kind,
            input_type: ty,
            count: 0,
            integer_value: 0,
            float_value: 0.,
            sum_squares: 0.,
            seen: false,
        }
    }
}

impl Accumulator for HailAccumulator {
    fn update_batch(&mut self, vals: &[ArrayRef]) -> DfResult<()> {
        if vals.is_empty() {
            return Err(DataFusionError::Execution(
                "aggregate expects one input".into(),
            ));
        }
        // DataFusion appends ORDER BY expression arrays to aggregate inputs.
        // These reducers are order-insensitive, so consume only the value arg.
        let a = &vals[0];
        for row in 0..a.len() {
            if self.kind == AggregateKind::Fraction {
                self.count = self.count.wrapping_add(1);
            }
            if a.is_null(row) {
                continue;
            }
            let v = ScalarValue::try_from_array(a, row)?;
            match self.kind {
                AggregateKind::Count => self.count = self.count.wrapping_add(1),
                AggregateKind::CountWhere => {
                    if matches!(v, ScalarValue::Boolean(Some(true))) {
                        self.count = self.count.wrapping_add(1)
                    }
                }
                AggregateKind::Fraction => {
                    if matches!(v, ScalarValue::Boolean(Some(true))) {
                        self.float_value += 1.
                    }
                }
                AggregateKind::Any => {
                    if matches!(v, ScalarValue::Boolean(Some(true))) {
                        self.seen = true
                    }
                }
                AggregateKind::All => {
                    if matches!(v, ScalarValue::Boolean(Some(false))) {
                        self.seen = true
                    }
                }
                AggregateKind::Min | AggregateKind::Max => {
                    if !self.seen {
                        self.float_value = num(&v)?;
                        self.integer_value = as_i64(&v).unwrap_or(0);
                        self.seen = true
                    } else if replace_extreme(
                        &v,
                        self.kind,
                        &self.input_type,
                        self.integer_value,
                        self.float_value,
                    )? {
                        self.float_value = num(&v)?;
                        self.integer_value = as_i64(&v).unwrap_or(0);
                    }
                }
                AggregateKind::Sum | AggregateKind::Product => {
                    if int_type(&self.input_type) {
                        let z = as_i64(&v)?;
                        self.integer_value = if self.count == 0 {
                            z
                        } else if self.kind == AggregateKind::Sum {
                            self.integer_value.wrapping_add(z)
                        } else {
                            self.integer_value.wrapping_mul(z)
                        };
                    } else {
                        let z = num(&v)?;
                        self.float_value = if self.kind == AggregateKind::Sum {
                            self.float_value + z
                        } else if self.count == 0 {
                            z
                        } else {
                            self.float_value * z
                        };
                    }
                    self.count = self.count.wrapping_add(1);
                }
                AggregateKind::Mean | AggregateKind::Stddev => {
                    let z = num(&v)?;
                    self.float_value += z;
                    self.sum_squares += z * z;
                    self.count = self.count.wrapping_add(1);
                }
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(match self.kind {
            AggregateKind::Count | AggregateKind::CountWhere => {
                ScalarValue::Int64(Some(self.count))
            }
            AggregateKind::Fraction => ScalarValue::Float64(
                (self.count != 0).then(|| self.float_value / self.count as f64),
            ),
            AggregateKind::Any => ScalarValue::Boolean(Some(self.seen)),
            AggregateKind::All => ScalarValue::Boolean(Some(!self.seen)),
            AggregateKind::Min | AggregateKind::Max => {
                if self.seen {
                    if self.input_type == DataType::Int32 {
                        ScalarValue::Int32(Some(self.integer_value as i32))
                    } else if self.input_type == DataType::Int64 {
                        ScalarValue::Int64(Some(self.integer_value))
                    } else if self.input_type == DataType::Float32 {
                        ScalarValue::Float32(Some(self.float_value as f32))
                    } else {
                        ScalarValue::Float64(Some(self.float_value))
                    }
                } else {
                    ScalarValue::try_from(&self.input_type).unwrap_or(ScalarValue::Null)
                }
            }
            AggregateKind::Sum => {
                if int_type(&self.input_type) {
                    ScalarValue::Int64(Some(self.integer_value))
                } else {
                    ScalarValue::Float64(Some(self.float_value))
                }
            }
            AggregateKind::Product => {
                if int_type(&self.input_type) {
                    ScalarValue::Int64(Some(if self.count == 0 {
                        1
                    } else {
                        self.integer_value
                    }))
                } else {
                    ScalarValue::Float64(Some(if self.count == 0 {
                        1.
                    } else {
                        self.float_value
                    }))
                }
            }
            AggregateKind::Mean => ScalarValue::Float64(Some(self.float_value / self.count as f64)),
            AggregateKind::Stddev => {
                let m = self.float_value / self.count as f64;
                ScalarValue::Float64(Some(
                    ((self.sum_squares - m * self.float_value) / self.count as f64).sqrt(),
                ))
            }
        })
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(match self.kind {
            AggregateKind::Count | AggregateKind::CountWhere => {
                vec![ScalarValue::Int64(Some(self.count))]
            }
            AggregateKind::Fraction => vec![
                ScalarValue::Int64(Some(self.count)),
                ScalarValue::Float64(Some(self.float_value)),
            ],
            AggregateKind::Any | AggregateKind::All => vec![ScalarValue::Boolean(Some(self.seen))],
            AggregateKind::Min | AggregateKind::Max => vec![if self.seen {
                if self.input_type == DataType::Int32 {
                    ScalarValue::Int32(Some(self.integer_value as i32))
                } else if self.input_type == DataType::Int64 {
                    ScalarValue::Int64(Some(self.integer_value))
                } else if self.input_type == DataType::Float32 {
                    ScalarValue::Float32(Some(self.float_value as f32))
                } else {
                    ScalarValue::Float64(Some(self.float_value))
                }
            } else {
                ScalarValue::try_from(&self.input_type).unwrap_or(ScalarValue::Null)
            }],
            AggregateKind::Sum | AggregateKind::Product => vec![
                ScalarValue::Int64(Some(self.count)),
                if int_type(&self.input_type) {
                    ScalarValue::Int64(Some(self.integer_value))
                } else {
                    ScalarValue::Float64(Some(self.float_value))
                },
            ],
            AggregateKind::Mean => vec![
                ScalarValue::Float64(Some(self.float_value)),
                ScalarValue::Int64(Some(self.count)),
            ],
            AggregateKind::Stddev => vec![
                ScalarValue::Float64(Some(self.float_value)),
                ScalarValue::Float64(Some(self.sum_squares)),
                ScalarValue::Int64(Some(self.count)),
            ],
        })
    }
    fn merge_batch(&mut self, s: &[ArrayRef]) -> DfResult<()> {
        let expected = match self.kind {
            AggregateKind::Count | AggregateKind::CountWhere => 1,
            AggregateKind::Fraction
            | AggregateKind::Sum
            | AggregateKind::Product
            | AggregateKind::Mean => 2,
            AggregateKind::Any | AggregateKind::All | AggregateKind::Min | AggregateKind::Max => 1,
            AggregateKind::Stddev => 3,
        };
        if s.len() != expected {
            return Err(DataFusionError::Execution(
                "invalid aggregate state arity".into(),
            ));
        }
        for row in 0..s.first().map_or(0, |a| a.len()) {
            let get = |idx: usize| ScalarValue::try_from_array(&s[idx], row);
            match self.kind {
                AggregateKind::Count | AggregateKind::CountWhere => {
                    self.count = self.count.wrapping_add(as_i64(&get(0)?).unwrap_or(0))
                }
                AggregateKind::Fraction => {
                    self.count = self.count.wrapping_add(as_i64(&get(0)?).unwrap_or(0));
                    self.float_value += num(&get(1)?).unwrap_or(0.)
                }
                AggregateKind::Any | AggregateKind::All => {
                    self.seen |= matches!(get(0)?, ScalarValue::Boolean(Some(true)))
                }
                AggregateKind::Min | AggregateKind::Max => {
                    let v = get(0)?;
                    if !v.is_null() {
                        if !self.seen {
                            self.float_value = num(&v)?;
                            self.integer_value = as_i64(&v).unwrap_or(0);
                            self.seen = true
                        } else if replace_extreme(
                            &v,
                            self.kind,
                            &self.input_type,
                            self.integer_value,
                            self.float_value,
                        )? {
                            self.float_value = num(&v)?;
                            self.integer_value = as_i64(&v).unwrap_or(0)
                        }
                    }
                }
                AggregateKind::Sum | AggregateKind::Product => {
                    let n = as_i64(&get(0)?).unwrap_or(0);
                    let z = get(1)?;
                    if n == 0 {
                        continue;
                    }
                    let old = self.count;
                    self.count = self.count.wrapping_add(n);
                    if int_type(&self.input_type) {
                        let q = as_i64(&z).unwrap_or(0);
                        self.integer_value = if old == 0 {
                            q
                        } else if self.kind == AggregateKind::Sum {
                            self.integer_value.wrapping_add(q)
                        } else {
                            self.integer_value.wrapping_mul(q)
                        }
                    } else {
                        let q = num(&z).unwrap_or(0.);
                        self.float_value = if old == 0 {
                            q
                        } else if self.kind == AggregateKind::Sum {
                            self.float_value + q
                        } else {
                            self.float_value * q
                        }
                    }
                }
                AggregateKind::Mean => {
                    self.float_value += num(&get(0)?).unwrap_or(0.);
                    self.count = self.count.wrapping_add(as_i64(&get(1)?).unwrap_or(0));
                }
                AggregateKind::Stddev => {
                    self.float_value += num(&get(0)?).unwrap_or(0.);
                    self.sum_squares += num(&get(1)?).unwrap_or(0.);
                    self.count = self.count.wrapping_add(as_i64(&get(2)?).unwrap_or(0));
                }
            }
        }
        Ok(())
    }
}
fn int_type(t: &DataType) -> bool {
    matches!(t, DataType::Int32 | DataType::Int64)
}
fn as_i64(v: &ScalarValue) -> DfResult<i64> {
    match v {
        ScalarValue::Int32(Some(x)) => Ok(*x as i64),
        ScalarValue::Int64(Some(x)) => Ok(*x),
        _ => Err(DataFusionError::Execution("expected integer".into())),
    }
}
fn num(v: &ScalarValue) -> DfResult<f64> {
    match v {
        ScalarValue::Int32(Some(x)) => Ok(*x as f64),
        ScalarValue::Int64(Some(x)) => Ok(*x as f64),
        ScalarValue::Float32(Some(x)) => Ok(*x as f64),
        ScalarValue::Float64(Some(x)) => Ok(*x),
        _ => Err(DataFusionError::Execution("expected numeric".into())),
    }
}
fn replace_extreme(
    v: &ScalarValue,
    k: AggregateKind,
    t: &DataType,
    old_i: i64,
    old_f: f64,
) -> DfResult<bool> {
    if int_type(t) {
        let x = as_i64(v)?;
        return Ok(if k == AggregateKind::Min {
            x < old_i
        } else {
            x > old_i
        });
    }
    let x = num(v)?;
    if x.is_nan() {
        return Ok(false);
    }
    if old_f.is_nan() {
        return Ok(true);
    }
    if x == 0.0 && old_f == 0.0 {
        return Ok(if k == AggregateKind::Min {
            x.is_sign_negative() && !old_f.is_sign_negative()
        } else {
            !x.is_sign_negative() && old_f.is_sign_negative()
        });
    }
    Ok(if k == AggregateKind::Min {
        x < old_f
    } else {
        x > old_f
    })
}

pub fn aggregate_udf(
    function: &str,
    input_type: Option<&DataType>,
) -> Result<Arc<AggregateUDF>, ExprError> {
    let (kind, input, ret, state) = match function {
        "count" => (
            AggregateKind::Count,
            Some(DataType::Int64),
            DataType::Int64,
            vec![DataType::Int64],
        ),
        "count_where" | "fraction" | "any" | "all" => {
            if let Some(t) = input_type {
                if t != &DataType::Boolean {
                    return Err(ExprError::Invalid(format!(
                        "{function} requires Boolean input, got {t:?}"
                    )));
                }
            }
            match function {
                "count_where" => (
                    AggregateKind::CountWhere,
                    Some(DataType::Boolean),
                    DataType::Int64,
                    vec![DataType::Int64],
                ),
                "fraction" => (
                    AggregateKind::Fraction,
                    Some(DataType::Boolean),
                    DataType::Float64,
                    vec![DataType::Int64, DataType::Float64],
                ),
                "any" => (
                    AggregateKind::Any,
                    Some(DataType::Boolean),
                    DataType::Boolean,
                    vec![DataType::Boolean],
                ),
                _ => (
                    AggregateKind::All,
                    Some(DataType::Boolean),
                    DataType::Boolean,
                    vec![DataType::Boolean],
                ),
            }
        }
        "sum" | "product" | "min" | "max" | "mean" | "stddev_pop" => {
            let t = input_type
                .ok_or_else(|| ExprError::Invalid(format!("{function} requires an input type")))?;
            if !matches!(
                t,
                DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
            ) {
                return Err(ExprError::Invalid(format!(
                    "{function} does not support {t:?}"
                )));
            }
            let k = match function {
                "sum" => AggregateKind::Sum,
                "product" => AggregateKind::Product,
                "min" => AggregateKind::Min,
                "max" => AggregateKind::Max,
                "mean" => AggregateKind::Mean,
                _ => AggregateKind::Stddev,
            };
            let out = if matches!(k, AggregateKind::Min | AggregateKind::Max) {
                t.clone()
            } else if matches!(k, AggregateKind::Sum | AggregateKind::Product) && int_type(t) {
                DataType::Int64
            } else {
                DataType::Float64
            };
            let st = match k {
                AggregateKind::Min | AggregateKind::Max => vec![t.clone()],
                AggregateKind::Sum | AggregateKind::Product => vec![DataType::Int64, out.clone()],
                AggregateKind::Mean => vec![DataType::Float64, DataType::Int64],
                _ => vec![DataType::Float64, DataType::Float64, DataType::Int64],
            };
            (k, Some(t.clone()), out, st)
        }
        "collect" => {
            return Err(ExprError::Invalid(
                "collect is registered separately".into(),
            ));
        }
        _ => {
            return Err(ExprError::Invalid(format!(
                "unknown aggregate {function:?}"
            )));
        }
    };
    let ty = input.clone().unwrap();
    let inputs = vec![ty.clone()];
    let factory: datafusion_expr::AccumulatorFactoryFunction =
        Arc::new(move |_| Ok(Box::new(HailAccumulator::new(kind, ty.clone()))));
    Ok(Arc::new(create_udaf(
        function,
        inputs,
        Arc::new(ret),
        Volatility::Immutable,
        factory,
        Arc::new(state),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int64Array};
    #[test]
    fn int64_sum_preserves_bits_and_merges() {
        let mut a = HailAccumulator::new(AggregateKind::Sum, DataType::Int64);
        a.update_batch(&[Arc::new(Int64Array::from(vec![
            9_007_199_254_740_993_i64,
            7,
        ]))])
        .unwrap();
        let st = a.state().unwrap();
        let mut b = HailAccumulator::new(AggregateKind::Sum, DataType::Int64);
        b.update_batch(&[Arc::new(Int64Array::from(vec![3_i64]))])
            .unwrap();
        let bs = b.state().unwrap();
        let states: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                as_i64(&st[0]).unwrap(),
                as_i64(&bs[0]).unwrap(),
            ])),
            Arc::new(Int64Array::from(vec![
                as_i64(&st[1]).unwrap(),
                as_i64(&bs[1]).unwrap(),
            ])),
        ];
        let mut merged = HailAccumulator::new(AggregateKind::Sum, DataType::Int64);
        merged.merge_batch(&states).unwrap();
        assert_eq!(
            merged.evaluate().unwrap(),
            ScalarValue::Int64(Some(9_007_199_254_741_003))
        );
    }
    #[test]
    fn empty_semantics_and_boolean_counts() {
        assert!(
            matches!(HailAccumulator::new(AggregateKind::Mean,DataType::Float64).evaluate().unwrap(),ScalarValue::Float64(Some(x)) if x.is_nan())
        );
        assert!(
            matches!(HailAccumulator::new(AggregateKind::Stddev,DataType::Float64).evaluate().unwrap(),ScalarValue::Float64(Some(x)) if x.is_nan())
        );
        assert!(matches!(HailAccumulator::new(AggregateKind::Fraction,DataType::Boolean).evaluate().unwrap(),ScalarValue::Float64(Some(x)) if x.is_nan()));
        let mut all = HailAccumulator::new(AggregateKind::All, DataType::Boolean);
        all.update_batch(&[Arc::new(BooleanArray::from(vec![Some(true), None]))])
            .unwrap();
        assert_eq!(all.evaluate().unwrap(), ScalarValue::Boolean(Some(true)));
        let mut cw = HailAccumulator::new(AggregateKind::CountWhere, DataType::Boolean);
        cw.update_batch(&[Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
        ]))])
        .unwrap();
        assert_eq!(cw.evaluate().unwrap(), ScalarValue::Int64(Some(1)));
    }
    #[test]
    fn floating_states_merge() {
        let mut a = HailAccumulator::new(AggregateKind::Stddev, DataType::Float64);
        a.update_batch(&[Arc::new(Float64Array::from(vec![1., 3.]))])
            .unwrap();
        let mut b = HailAccumulator::new(AggregateKind::Stddev, DataType::Float64);
        b.update_batch(&[Arc::new(Float64Array::from(vec![5.]))])
            .unwrap();
        let sa = a.state().unwrap();
        let sb = b.state().unwrap();
        let states: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![
                num(&sa[0]).unwrap(),
                num(&sb[0]).unwrap(),
            ])),
            Arc::new(Float64Array::from(vec![
                num(&sa[1]).unwrap(),
                num(&sb[1]).unwrap(),
            ])),
            Arc::new(Int64Array::from(vec![
                as_i64(&sa[2]).unwrap(),
                as_i64(&sb[2]).unwrap(),
            ])),
        ];
        let mut m = HailAccumulator::new(AggregateKind::Stddev, DataType::Float64);
        m.merge_batch(&states).unwrap();
        assert_eq!(
            m.evaluate().unwrap(),
            ScalarValue::Float64(Some((8f64 / 3.).sqrt()))
        );
    }

    #[test]
    fn fraction_counts_null_rows_and_product_merges_empty_partials() {
        let mut frac = HailAccumulator::new(AggregateKind::Fraction, DataType::Boolean);
        frac.update_batch(&[Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
        ]))])
        .unwrap();
        assert_eq!(
            frac.evaluate().unwrap(),
            ScalarValue::Float64(Some(1. / 3.))
        );

        let empty = HailAccumulator::new(AggregateKind::Product, DataType::Int64)
            .state()
            .unwrap();
        let mut nonempty = HailAccumulator::new(AggregateKind::Product, DataType::Int64);
        nonempty
            .update_batch(&[Arc::new(Int64Array::from(vec![4_i64, 5]))])
            .unwrap();
        let nonempty = nonempty.state().unwrap();
        let states: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                as_i64(&empty[0]).unwrap(),
                as_i64(&nonempty[0]).unwrap(),
            ])),
            Arc::new(Int64Array::from(vec![
                as_i64(&empty[1]).unwrap(),
                as_i64(&nonempty[1]).unwrap(),
            ])),
        ];
        let mut merged = HailAccumulator::new(AggregateKind::Product, DataType::Int64);
        merged.merge_batch(&states).unwrap();
        assert_eq!(merged.evaluate().unwrap(), ScalarValue::Int64(Some(20)));
    }

    #[test]
    fn extrema_keep_input_integer_type_and_ignore_nan_when_finite_exists() {
        let mut i32_min = HailAccumulator::new(AggregateKind::Min, DataType::Int32);
        i32_min
            .update_batch(&[Arc::new(arrow_array::Int32Array::from(vec![5, -2, 7]))])
            .unwrap();
        assert_eq!(i32_min.evaluate().unwrap(), ScalarValue::Int32(Some(-2)));
        assert_eq!(i32_min.state().unwrap(), vec![ScalarValue::Int32(Some(-2))]);

        let mut max = HailAccumulator::new(AggregateKind::Max, DataType::Float64);
        max.update_batch(&[Arc::new(Float64Array::from(vec![f64::NAN, 2.0, 4.0]))])
            .unwrap();
        assert_eq!(max.evaluate().unwrap(), ScalarValue::Float64(Some(4.0)));
        let mut all_nan = HailAccumulator::new(AggregateKind::Min, DataType::Float64);
        all_nan
            .update_batch(&[Arc::new(Float64Array::from(vec![f64::NAN, f64::NAN]))])
            .unwrap();
        assert!(matches!(all_nan.evaluate().unwrap(), ScalarValue::Float64(Some(x)) if x.is_nan()));
    }
}
