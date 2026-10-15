//! Bounded and statistical Hail-style numeric aggregates.
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, StructArray, new_empty_array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, Volatility, expr_fn::create_udaf};
use kll_rs::KllDoubleSketch;
use nalgebra::{DMatrix, DVector};
use ndhistogram::{Hist1D, Histogram, axis::Uniform, ndhistogram};
use statrs::distribution::{ContinuousCDF, FisherSnedecor, StudentsT};

use crate::aggregate_identity::configured_udaf_name;
use crate::expressions::ExprError;

fn invalid(msg: impl Into<String>) -> ExprError {
    ExprError::Invalid(msg.into())
}

pub fn make_udaf(
    function: &str,
    input_type: &DataType,
    options: &serde_json::Value,
) -> Result<Option<Arc<AggregateUDF>>, ExprError> {
    match function {
        "hist" => histogram(input_type, options).map(Some),
        "approx_quantiles" | "approx_median" => quantiles(function, input_type, options).map(Some),
        "approx_cdf" => Err(invalid(
            "approx_cdf is unsupported: Hail-compatible retained items and compaction state are unavailable",
        )),
        "corr" => correlation(input_type).map(Some),
        "linreg" => linreg(input_type, options).map(Some),
        "downsample" => downsample(input_type, options).map(Some),
        _ => Ok(None),
    }
}

fn numeric(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    )
}
fn struct_scalar(fields: Vec<Arc<Field>>, values: Vec<ScalarValue>) -> DfResult<ScalarValue> {
    let columns = values
        .into_iter()
        .map(|v| ScalarValue::iter_to_array([v]))
        .collect::<DfResult<Vec<_>>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::new(
        fields.into(),
        columns,
        None,
    ))))
}
fn read_f64(a: &dyn Array, row: usize) -> DfResult<f64> {
    Ok(match a.data_type() {
        DataType::Int32 => a
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap()
            .value(row) as f64,
        DataType::Int64 => a.as_any().downcast_ref::<Int64Array>().unwrap().value(row) as f64,
        DataType::Float32 => a
            .as_any()
            .downcast_ref::<arrow_array::Float32Array>()
            .unwrap()
            .value(row) as f64,
        DataType::Float64 => a
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row),
        t => {
            return Err(DataFusionError::Execution(format!(
                "expected numeric value, got {t:?}"
            )));
        }
    })
}

fn histogram(t: &DataType, o: &serde_json::Value) -> Result<Arc<AggregateUDF>, ExprError> {
    if !numeric(t) {
        return Err(invalid(format!("hist requires numeric input, got {t:?}")));
    }
    let start = o["start"]
        .as_f64()
        .ok_or_else(|| invalid("hist requires numeric start"))?;
    let end = o["end"]
        .as_f64()
        .ok_or_else(|| invalid("hist requires numeric end"))?;
    let bins = o["bins"]
        .as_u64()
        .filter(|n| *n > 0 && *n <= i32::MAX as u64)
        .ok_or_else(|| invalid("hist bins must be a positive int32"))? as usize;
    if !start.is_finite() || !end.is_finite() || start >= end {
        return Err(invalid("hist requires finite start < end"));
    }
    let out = DataType::Struct(
        vec![
            Arc::new(Field::new(
                "bin_edges",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )),
            Arc::new(Field::new(
                "bin_freq",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            )),
            Arc::new(Field::new("n_smaller", DataType::Int64, true)),
            Arc::new(Field::new("n_larger", DataType::Int64, true)),
        ]
        .into(),
    );
    let st = vec![DataType::Int64; bins + 2];
    let axis = Uniform::new(bins, start, end).map_err(|e| invalid(format!("hist axis: {e}")))?;
    let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
        Ok(Box::new(HistAccumulator {
            start,
            end,
            bins,
            hist: ndhistogram!(axis.clone(); i64),
        }))
    });
    let udaf_name = configured_udaf_name("hist", t, o);
    Ok(Arc::new(create_udaf(
        &udaf_name,
        vec![t.clone()],
        Arc::new(out),
        Volatility::Immutable,
        factory,
        Arc::new(st),
    )))
}

#[derive(Debug)]
struct HistAccumulator {
    start: f64,
    end: f64,
    bins: usize,
    hist: Hist1D<Uniform<f64>, i64>,
}
impl HistAccumulator {
    fn result(&self) -> DfResult<ScalarValue> {
        let edges: ArrayRef = Arc::new(Float64Array::from(
            (0..=self.bins)
                .map(|i| self.start + (self.end - self.start) * i as f64 / self.bins as f64)
                .collect::<Vec<_>>(),
        ));
        let freqs: ArrayRef = Arc::new(Int64Array::from(
            (1..=self.bins)
                .map(|i| *self.hist.value_at_index(i).unwrap())
                .collect::<Vec<_>>(),
        ));
        let list_edges = ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Float64, true)),
            OffsetBuffer::from_lengths([edges.len()]),
            edges,
            None,
        )));
        let list_freq = ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::from_lengths([freqs.len()]),
            freqs,
            None,
        )));
        struct_scalar(
            vec![
                Arc::new(Field::new(
                    "bin_edges",
                    list_edges.data_type().clone(),
                    true,
                )),
                Arc::new(Field::new("bin_freq", list_freq.data_type().clone(), true)),
                Arc::new(Field::new("n_smaller", DataType::Int64, true)),
                Arc::new(Field::new("n_larger", DataType::Int64, true)),
            ],
            vec![
                list_edges,
                list_freq,
                ScalarValue::Int64(Some(*self.hist.value_at_index(0).unwrap())),
                ScalarValue::Int64(Some(*self.hist.value_at_index(self.bins + 1).unwrap())),
            ],
        )
    }
}
impl Accumulator for HistAccumulator {
    fn update_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let [a] = v else {
            return Err(DataFusionError::Execution("hist expects one input".into()));
        };
        for i in 0..a.len() {
            if a.is_null(i) {
                continue;
            }
            let x = read_f64(a.as_ref(), i)?;
            if x.is_nan() {
                continue;
            }
            // Hail includes the right endpoint in the final bin. Uniform
            // axes use [start,end), so adapt that one boundary only.
            let count = if x == self.end {
                self.hist.value_at_index_mut(self.bins)
            } else {
                self.hist.value_mut(&x)
            }
            .ok_or_else(|| DataFusionError::Execution("hist axis rejected value".into()))?;
            *count = count.wrapping_add(1);
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.result()
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + (self.bins + 2) * 8
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(self
            .hist
            .values()
            .map(|n| ScalarValue::Int64(Some(*n)))
            .collect())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        if states.len() != self.bins + 2 {
            return Err(DataFusionError::Execution(
                "hist state shape mismatch".into(),
            ));
        }
        for (i, a) in states.iter().enumerate() {
            let s = a
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Execution("hist state must be int64".into()))?;
            for r in 0..s.len() {
                if s.is_valid(r) {
                    let count = self.hist.value_at_index_mut(i).unwrap();
                    *count = count.wrapping_add(s.value(r));
                }
            }
        }
        Ok(())
    }
}

fn quantiles(
    name: &str,
    t: &DataType,
    o: &serde_json::Value,
) -> Result<Arc<AggregateUDF>, ExprError> {
    if !numeric(t) {
        return Err(invalid(format!("{name} requires numeric input, got {t:?}")));
    }
    let requested_k = o["k"]
        .as_i64()
        .filter(|k| (*k >= i32::MIN as i64) && (*k <= u16::MAX as i64))
        .ok_or_else(|| {
            invalid("quantile k must fit Hail's int32 parameter and the sketch's maximum k")
        })?;
    // Hail's KLL implementation has a minimum per-level capacity of eight,
    // even when its public k is smaller. DataSketches enforces k >= 8, so
    // use that same minimum capacity instead of rejecting Hail-valid inputs.
    let k = requested_k.max(8) as u16;
    let missing_quantiles = o.get("quantiles").is_some_and(serde_json::Value::is_null);
    let qs = o["quantiles"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|v| {
                    if v.is_null() {
                        Ok(None)
                    } else {
                        v.as_f64()
                            .map(Some)
                            .ok_or_else(|| invalid("quantiles must be numeric"))
                    }
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_else(|| vec![Some(0.5)]);
    if qs
        .iter()
        .flatten()
        .any(|q| !q.is_finite() || !(0.0..=1.0).contains(q))
    {
        return Err(invalid("quantiles must lie in [0, 1]"));
    }
    let result_type = if o["scalar"].as_bool().unwrap_or(false) {
        t.clone()
    } else {
        DataType::List(Arc::new(Field::new("item", t.clone(), true)))
    };
    let state_type = DataType::Binary;
    let ret = result_type.clone();
    let input = t.clone();
    let scalar = o["scalar"].as_bool().unwrap_or(false);
    let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
        Ok(Box::new(KllAccumulator::new(
            input.clone(),
            k,
            qs.clone(),
            scalar,
            missing_quantiles,
        )))
    });
    let udaf_name = configured_udaf_name(name, t, o);
    Ok(Arc::new(create_udaf(
        &udaf_name,
        vec![t.clone()],
        Arc::new(ret),
        Volatility::Immutable,
        factory,
        Arc::new(vec![state_type]),
    )))
}

#[derive(Debug)]
struct KllAccumulator {
    input_type: DataType,
    qs: Vec<Option<f64>>,
    scalar: bool,
    missing_quantiles: bool,
    sketch: KllDoubleSketch,
    empty: bool,
}
impl KllAccumulator {
    fn new(
        t: DataType,
        k: u16,
        qs: Vec<Option<f64>>,
        scalar: bool,
        missing_quantiles: bool,
    ) -> Self {
        Self {
            input_type: t,
            qs,
            scalar,
            missing_quantiles,
            sketch: KllDoubleSketch::new_with_k(k).expect("validated k"),
            empty: true,
        }
    }
    fn merge_serialized(&mut self, bytes: &[u8]) -> DfResult<()> {
        let s = KllDoubleSketch::deserialize(bytes)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.sketch
            .merge(&s)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.empty &= s.is_empty();
        Ok(())
    }
    fn cast_num(&self, x: f64) -> ScalarValue {
        match self.input_type {
            DataType::Int32 => ScalarValue::Int32(Some(x as i32)),
            DataType::Int64 => ScalarValue::Int64(Some(x as i64)),
            DataType::Float32 => ScalarValue::Float32(Some(x as f32)),
            _ => ScalarValue::Float64(Some(x)),
        }
    }
    fn list(&self, vals: Vec<ScalarValue>, ty: DataType) -> DfResult<ScalarValue> {
        let arr = if vals.is_empty() {
            new_empty_array(&ty)
        } else {
            ScalarValue::iter_to_array(vals.into_iter())?
        };
        Ok(ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(Field::new("item", ty, true)),
            OffsetBuffer::from_lengths([arr.len()]),
            arr,
            None,
        ))))
    }
    fn evaluate_value(&self) -> DfResult<ScalarValue> {
        if self.missing_quantiles {
            let dtype = if self.scalar {
                self.input_type.clone()
            } else {
                DataType::List(Arc::new(Field::new("item", self.input_type.clone(), true)))
            };
            return ScalarValue::try_from(&dtype);
        }
        if self.empty {
            return if self.scalar {
                Ok(ScalarValue::try_from(&self.input_type).unwrap_or(ScalarValue::Null))
            } else {
                self.list(
                    (0..self.qs.len())
                        .map(|_| {
                            ScalarValue::try_from(&self.input_type).unwrap_or(ScalarValue::Null)
                        })
                        .collect(),
                    self.input_type.clone(),
                )
            };
        }
        let vals = self
            .qs
            .iter()
            .map(|q| match q {
                Some(q) => {
                    // Hail requests rank floor(q * n) (zero-based), whereas
                    // DataSketches' inclusive query selects ceil(q * n) - 1.
                    let n = self.sketch.get_n() as f64;
                    let inclusive_rank = ((q * n).floor() + 1.0).min(n) / n;
                    Ok(self.cast_num(self.sketch.get_quantile(inclusive_rank)))
                }
                None => ScalarValue::try_from(&self.input_type),
            })
            .collect::<DfResult<Vec<_>>>()?;
        if self.scalar {
            Ok(vals[0].clone())
        } else {
            self.list(vals, self.input_type.clone())
        }
    }
}
impl Accumulator for KllAccumulator {
    fn update_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let [a] = v else {
            return Err(DataFusionError::Execution(
                "approximate quantile expects one input".into(),
            ));
        };
        for i in 0..a.len() {
            if a.is_null(i) {
                continue;
            }
            if let Some(ints) = a.as_any().downcast_ref::<Int64Array>() {
                let value = ints.value(i);
                if !(-9_007_199_254_740_992..=9_007_199_254_740_992).contains(&value) {
                    return Err(DataFusionError::Execution("approximate quantiles for int64 inputs outside ±2^53 cannot be represented exactly by the f64 sketch".into()));
                }
            }
            let x = read_f64(a.as_ref(), i)?;
            if !x.is_nan() {
                self.sketch.update(x);
                self.empty = false;
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.evaluate_value()
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.sketch.get_num_retained() as usize * 8
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(
            self.sketch
                .serialize()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?,
        ))])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [a] = states else {
            return Err(DataFusionError::Execution(
                "quantile state expects one binary column".into(),
            ));
        };
        let a = a
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("quantile state must be binary".into()))?;
        for i in 0..a.len() {
            if a.is_valid(i) {
                self.merge_serialized(a.value(i))?;
            }
        }
        Ok(())
    }
}

fn correlation(t: &DataType) -> Result<Arc<AggregateUDF>, ExprError> {
    let DataType::Struct(fields) = t else {
        return Err(invalid("corr input must be a struct containing x and y"));
    };
    if fields.len() != 2 || !numeric(fields[0].data_type()) || !numeric(fields[1].data_type()) {
        return Err(invalid("corr requires numeric x and y fields"));
    }
    // The planner packs both inputs into a single physical struct. The UDAF's
    // accumulator delegates to DataFusion's stable pairwise correlation kernel.
    let out = DataType::Float64;
    let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
        Ok(Box::new(CorrAccumulator {
            inner: datafusion_functions_aggregate::correlation::CorrelationAccumulator::try_new()?,
        }))
    });
    Ok(Arc::new(create_udaf(
        "corr",
        vec![t.clone()],
        Arc::new(out),
        Volatility::Immutable,
        factory,
        Arc::new(vec![
            DataType::UInt64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ]),
    )))
}
#[derive(Debug)]
struct CorrAccumulator {
    inner: datafusion_functions_aggregate::correlation::CorrelationAccumulator,
}
impl Accumulator for CorrAccumulator {
    fn update_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let [a] = v else {
            return Err(DataFusionError::Execution(
                "corr expects a struct argument".into(),
            ));
        };
        let s = a
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("corr expects struct values".into()))?;
        let [x, y] = s.columns() else {
            return Err(DataFusionError::Execution("corr requires x and y".into()));
        };
        let x = arrow_cast::cast(x.as_ref(), &DataType::Float64)?;
        let y = arrow_cast::cast(y.as_ref(), &DataType::Float64)?;
        self.inner.update_batch(&[x, y])
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let state = self.inner.state()?;
        let n = match state.first() {
            Some(ScalarValue::UInt64(Some(n))) => *n,
            _ => {
                return Err(DataFusionError::Execution(
                    "corr count state must be uint64".into(),
                ));
            }
        };
        if n == 0 {
            return Ok(ScalarValue::Float64(None));
        }
        let m2_x = match state.get(2) {
            Some(ScalarValue::Float64(Some(x))) => *x,
            _ => 0.0,
        };
        let m2_y = match state.get(4) {
            Some(ScalarValue::Float64(Some(y))) => *y,
            _ => 0.0,
        };
        if n < 2 || m2_x == 0.0 || m2_y == 0.0 {
            return Ok(ScalarValue::Float64(Some(f64::NAN)));
        }
        match self.inner.evaluate()? {
            ScalarValue::Float64(None) => Ok(ScalarValue::Float64(Some(f64::NAN))),
            value => Ok(value),
        }
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn merge_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        self.inner.merge_batch(v)
    }
}

// Hail's downsample aggregate retains at most one point in each cell of a
// dynamically bounded n_divisions by n_divisions grid. Rebinning the retained
// cell representatives when the bounds expand preserves bounded O(d^2) state.
#[derive(Clone, Debug)]
struct DownsamplePoint {
    x: f64,
    y: f64,
    label: ScalarValue,
}
fn tuple_item_type() -> DataType {
    let dtype = DataType::Struct(
        vec![
            Arc::new(Field::new("__vail_tuple_0", DataType::Float64, true)),
            Arc::new(Field::new("__vail_tuple_1", DataType::Float64, true)),
            Arc::new(Field::new(
                "__vail_tuple_2",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            )),
        ]
        .into(),
    );
    vail_ordering::tag_type(
        dtype,
        &serde_json::json!({"tuple": ["float64", "float64", {"array": "str"}]}),
    )
    .expect("the fixed downsample tuple has a nonempty struct carrier")
}
fn downsample_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", tuple_item_type(), true)))
}
fn downsample(
    input: &DataType,
    options: &serde_json::Value,
) -> Result<Arc<AggregateUDF>, ExprError> {
    let DataType::Struct(fields) = input else {
        return Err(invalid(
            "downsample expects packed x, y, and optional label fields",
        ));
    };
    if fields.len() < 2
        || fields.len() > 3
        || !numeric(fields[0].data_type())
        || !numeric(fields[1].data_type())
    {
        return Err(invalid("downsample requires numeric x and y fields"));
    }
    if fields.len() == 3 && !matches!(fields[2].data_type(), DataType::List(_) | DataType::Utf8) {
        return Err(invalid("downsample label must be string or array<string>"));
    }
    let divisions = options["n_divisions"]
        .as_u64()
        .filter(|n| (4..=i32::MAX as u64).contains(n))
        .ok_or_else(|| invalid("downsample n_divisions must be at least four"))?
        as usize;
    let output = downsample_type();
    let state = output.clone();
    let factory: datafusion_expr::AccumulatorFactoryFunction =
        Arc::new(move |_| Ok(Box::new(DownsampleAccumulator::new(divisions))));
    let udaf_name = configured_udaf_name("downsample", input, options);
    Ok(Arc::new(create_udaf(
        &udaf_name,
        vec![input.clone()],
        Arc::new(output),
        Volatility::Immutable,
        factory,
        Arc::new(vec![state]),
    )))
}
#[derive(Debug)]
struct DownsampleAccumulator {
    divisions: usize,
    points: BTreeMap<(i32, i32), DownsamplePoint>,
    bounds: Option<(f64, f64, f64, f64)>,
}
impl DownsampleAccumulator {
    fn new(divisions: usize) -> Self {
        Self {
            divisions,
            points: BTreeMap::new(),
            bounds: None,
        }
    }
    fn cell(&self, x: f64, y: f64, b: (f64, f64, f64, f64)) -> (i32, i32) {
        let (l, r, bottom, top) = b;
        let bx = if r == l {
            0
        } else {
            (((x - l) / (r - l)) * self.divisions as f64).floor() as i32
        };
        let by = if top == bottom {
            0
        } else {
            (((y - bottom) / (top - bottom)) * self.divisions as f64).floor() as i32
        };
        (
            bx.clamp(0, self.divisions as i32 - 1),
            by.clamp(0, self.divisions as i32 - 1),
        )
    }
    fn insert(&mut self, p: DownsamplePoint) {
        let bounds = match self.bounds {
            None => {
                self.bounds = Some((p.x, p.x, p.y, p.y));
                self.bounds.unwrap()
            }
            Some((l, r, b, t)) => (l.min(p.x), r.max(p.x), b.min(p.y), t.max(p.y)),
        };
        if self.bounds != Some(bounds) {
            let old = std::mem::take(&mut self.points);
            self.bounds = Some(bounds);
            for point in old.into_values() {
                let cell = self.cell(point.x, point.y, bounds);
                self.points.entry(cell).or_insert(point);
            }
        }
        let cell = self.cell(p.x, p.y, bounds);
        self.points.entry(cell).or_insert(p);
    }
    fn empty_label() -> DfResult<ScalarValue> {
        let ty = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let a = arrow_array::new_null_array(&ty, 1);
        ScalarValue::try_from_array(a.as_ref(), 0)
    }
    fn point_scalar(p: &DownsamplePoint) -> DfResult<ScalarValue> {
        let DataType::Struct(fields) = tuple_item_type() else {
            unreachable!()
        };
        struct_scalar(
            fields.to_vec(),
            vec![
                ScalarValue::Float64(Some(p.x)),
                ScalarValue::Float64(Some(p.y)),
                p.label.clone(),
            ],
        )
    }
    fn list_value(points: impl Iterator<Item = DownsamplePoint>) -> DfResult<ScalarValue> {
        let vals = points
            .map(|p| Self::point_scalar(&p))
            .collect::<DfResult<Vec<_>>>()?;
        let a = if vals.is_empty() {
            new_empty_array(&tuple_item_type())
        } else {
            ScalarValue::iter_to_array(vals.into_iter())?
        };
        Ok(ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(Field::new("item", tuple_item_type(), true)),
            OffsetBuffer::from_lengths([a.len()]),
            a,
            None,
        ))))
    }
    fn result(&self) -> DfResult<ScalarValue> {
        Self::list_value(self.points.values().cloned())
    }
    fn insert_row(&mut self, s: &StructArray, row: usize) -> DfResult<()> {
        let cols = s.columns();
        if cols[0].is_null(row) || cols[1].is_null(row) {
            return Ok(());
        }
        let x = read_f64(cols[0].as_ref(), row)?;
        let y = read_f64(cols[1].as_ref(), row)?;
        if !x.is_finite() || !y.is_finite() {
            return Ok(());
        }
        let label = if cols.len() == 2 {
            Self::empty_label()?
        } else if cols[2].data_type() == &DataType::Utf8 {
            let v = ScalarValue::try_from_array(&cols[2], row)?;
            match v {
                ScalarValue::Utf8(Some(s)) => {
                    let a: ArrayRef =
                        Arc::new(arrow_array::StringArray::from(vec![Some(s.as_str())]));
                    ScalarValue::List(Arc::new(ListArray::new(
                        Arc::new(Field::new("item", DataType::Utf8, true)),
                        OffsetBuffer::from_lengths([1]),
                        a,
                        None,
                    )))
                }
                ScalarValue::Utf8(None) => Self::empty_label()?,
                _ => Self::empty_label()?,
            }
        } else if cols[2].is_null(row) {
            Self::empty_label()?
        } else {
            ScalarValue::try_from_array(&cols[2], row)?
        };
        self.insert(DownsamplePoint { x, y, label });
        Ok(())
    }
}
impl Accumulator for DownsampleAccumulator {
    fn update_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let [a] = v else {
            return Err(DataFusionError::Execution(
                "downsample expects one packed struct argument".into(),
            ));
        };
        let s = a
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("downsample expects struct values".into()))?;
        for row in 0..s.len() {
            self.insert_row(s, row)?;
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.result()
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.points.len() * std::mem::size_of::<((i32, i32), DownsamplePoint)>()
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![self.result()?])
    }
    fn merge_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let [a] = v else {
            return Err(DataFusionError::Execution(
                "downsample state expects one list".into(),
            ));
        };
        let lists = a
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("downsample state must be a list".into()))?;
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            let ss = lists.value(row);
            let tuples = ss.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                DataFusionError::Execution("downsample list item must be tuple struct".into())
            })?;
            for i in 0..tuples.len() {
                let c = tuples.columns();
                let p = DownsamplePoint {
                    x: read_f64(c[0].as_ref(), i)?,
                    y: read_f64(c[1].as_ref(), i)?,
                    label: ScalarValue::try_from_array(&c[2], i)?,
                };
                self.insert(p);
            }
        }
        Ok(())
    }
}

fn linreg(t: &DataType, o: &serde_json::Value) -> Result<Arc<AggregateUDF>, ExprError> {
    let DataType::Struct(fields) = t else {
        return Err(invalid(
            "linreg requires a struct of y, x, and optional weight",
        ));
    };
    let p = o["n_predictors"]
        .as_u64()
        .filter(|x| *x > 0 && *x < 128)
        .ok_or_else(|| invalid("linreg requires positive n_predictors option"))?
        as usize;
    let nested = o["nested_dim"]
        .as_u64()
        .ok_or_else(|| invalid("linreg requires nested_dim option"))? as usize;
    if nested > p {
        return Err(invalid("linreg nested_dim exceeds predictor count"));
    }
    let weighted = fields.len() == 3;
    if fields.len() != 2 && !weighted {
        return Err(invalid("linreg expects y and x, plus optional weight"));
    }
    let xt = fields[1].data_type();
    let n_actual = match xt {
        DataType::List(f) if *f.data_type() == DataType::Float64 => p,
        DataType::Float64 => 1,
        _ => return Err(invalid("linreg x must be float64 or array<float64>")),
    };
    if n_actual != p {
        return Err(invalid("linreg n_predictors does not match x type"));
    }
    if *fields[0].data_type() != DataType::Float64 {
        return Err(invalid("linreg y must be float64"));
    }
    let state = vec![DataType::Int64]
        .into_iter()
        .chain((0..p * p + p + 1).map(|_| DataType::Float64))
        .chain([DataType::Boolean])
        .collect::<Vec<_>>();
    let result = linreg_type();
    let factory: datafusion_expr::AccumulatorFactoryFunction =
        Arc::new(move |_| Ok(Box::new(LinregAccumulator::new(p, nested, weighted))));
    let udaf_name = configured_udaf_name("linreg", t, o);
    Ok(Arc::new(create_udaf(
        &udaf_name,
        vec![t.clone()],
        Arc::new(result),
        Volatility::Immutable,
        factory,
        Arc::new(state),
    )))
}
fn linreg_type() -> DataType {
    let f = Arc::new(Field::new("item", DataType::Float64, true));
    DataType::Struct(
        vec![
            Arc::new(Field::new("beta", DataType::List(f.clone()), true)),
            Arc::new(Field::new(
                "standard_error",
                DataType::List(f.clone()),
                true,
            )),
            Arc::new(Field::new("t_stat", DataType::List(f.clone()), true)),
            Arc::new(Field::new("p_value", DataType::List(f), true)),
            Arc::new(Field::new(
                "multiple_standard_error",
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new("multiple_r_squared", DataType::Float64, true)),
            Arc::new(Field::new("adjusted_r_squared", DataType::Float64, true)),
            Arc::new(Field::new("f_stat", DataType::Float64, true)),
            Arc::new(Field::new("multiple_p_value", DataType::Float64, true)),
            Arc::new(Field::new("n", DataType::Int64, true)),
        ]
        .into(),
    )
}
#[derive(Debug)]
struct LinregAccumulator {
    p: usize,
    nested: usize,
    weighted: bool,
    n: i64,
    xtx: Vec<f64>,
    xty: Vec<f64>,
    yty: f64,
    has_negative_weight: bool,
}
impl LinregAccumulator {
    fn new(p: usize, nested: usize, weighted: bool) -> Self {
        Self {
            p,
            nested,
            weighted,
            n: 0,
            xtx: vec![0.; p * p],
            xty: vec![0.; p],
            yty: 0.,
            has_negative_weight: false,
        }
    }
    fn result(&self) -> DfResult<ScalarValue> {
        let DataType::Struct(fields) = linreg_type() else {
            unreachable!()
        };
        let missing = || {
            fields
                .iter()
                .take(9)
                .map(|f| ScalarValue::try_from(f.data_type()).unwrap_or(ScalarValue::Null))
                .collect::<Vec<_>>()
        };
        let list = |numbers: Vec<f64>| -> ScalarValue {
            let values: ArrayRef = Arc::new(Float64Array::from(numbers));
            ScalarValue::List(Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Float64, true)),
                OffsetBuffer::from_lengths([values.len()]),
                values,
                None,
            )))
        };
        let mut values = missing();
        if self.n as usize > self.p {
            if self.has_negative_weight {
                let nan = || vec![f64::NAN; self.p];
                values = vec![
                    list(nan()),
                    list(nan()),
                    list(nan()),
                    list(nan()),
                    ScalarValue::Float64(Some(f64::NAN)),
                    ScalarValue::Float64(Some(f64::NAN)),
                    ScalarValue::Float64(Some(f64::NAN)),
                    ScalarValue::Float64(Some(f64::NAN)),
                    ScalarValue::Float64(Some(f64::NAN)),
                ];
            } else {
                let a = DMatrix::from_row_slice(self.p, self.p, &self.xtx);
                let inverse = a.clone().try_inverse();
                let nested_inverse = if self.nested == 0 {
                    None
                } else if self.nested == self.p {
                    inverse.clone()
                } else {
                    a.view((0, 0), (self.nested, self.nested))
                        .into_owned()
                        .try_inverse()
                };
                if let Some(inv) = inverse {
                    if self.nested == 0 || nested_inverse.is_some() {
                        let xy = DVector::from_column_slice(&self.xty);
                        let beta = &inv * &xy;
                        let rss = self.yty - beta.dot(&xy);
                        let df = (self.n as usize - self.p) as f64;
                        let sig2 = rss / df;
                        let se = (0..self.p)
                            .map(|i| (sig2 * inv[(i, i)]).sqrt())
                            .collect::<Vec<_>>();
                        let t = (0..self.p).map(|i| beta[i] / se[i]).collect::<Vec<_>>();
                        let pvals = t
                            .iter()
                            .map(|x| {
                                if x.is_nan() {
                                    return f64::NAN;
                                }
                                StudentsT::new(0., 1., df)
                                    .map(|d| 2. * d.sf(x.abs()))
                                    .unwrap_or(f64::NAN)
                            })
                            .collect::<Vec<_>>();
                        let rss0 = if self.nested == 0 {
                            self.yty
                        } else {
                            let xy0 = DVector::from_column_slice(&self.xty[..self.nested]);
                            self.yty - xy0.dot(&(nested_inverse.unwrap() * xy0.clone()))
                        };
                        let d0 = (self.p - self.nested) as f64;
                        let r2 = 1. - rss / rss0;
                        let adjusted = 1. - (1. - r2) * (self.n as f64 - self.nested as f64) / df;
                        let f = (rss0 - rss) * df / (rss * d0);
                        let model_p = if f.is_nan() {
                            f64::NAN
                        } else {
                            FisherSnedecor::new(d0, df)
                                .map(|d| d.sf(f))
                                .unwrap_or(f64::NAN)
                        };
                        values = vec![
                            list(beta.iter().copied().collect()),
                            list(se),
                            list(t),
                            list(pvals),
                            ScalarValue::Float64(Some(sig2.sqrt())),
                            ScalarValue::Float64(Some(r2)),
                            ScalarValue::Float64(Some(adjusted)),
                            ScalarValue::Float64(Some(f)),
                            ScalarValue::Float64(Some(model_p)),
                        ];
                    }
                }
            }
        }
        values.push(ScalarValue::Int64(Some(self.n)));
        struct_scalar(fields.to_vec(), values)
    }
}
impl Accumulator for LinregAccumulator {
    fn update_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let [a] = v else {
            return Err(DataFusionError::Execution(
                "linreg expects one struct input".into(),
            ));
        };
        let s = a
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("linreg expects struct values".into()))?;
        let cols = s.columns();
        let ys = cols[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg y must be float64".into()))?;
        let xs_list = cols[1].as_any().downcast_ref::<ListArray>();
        let xs_scalar = cols[1].as_any().downcast_ref::<Float64Array>();
        let weights = if self.weighted {
            Some(
                cols[2]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("linreg weight must be float64".into())
                    })?,
            )
        } else {
            None
        };
        for r in 0..a.len() {
            if ys.is_null(r) || cols[1].is_null(r) {
                continue;
            }
            let x: Vec<f64> = if let Some(l) = xs_list {
                let ar = l.value(r);
                let f = ar.as_any().downcast_ref::<Float64Array>().unwrap();
                if f.null_count() > 0 || f.len() != self.p {
                    continue;
                }
                f.values().to_vec()
            } else if let Some(f) = xs_scalar {
                vec![f.value(r)]
            } else {
                continue;
            };
            if x.len() != self.p {
                continue;
            }
            let w = if let Some(w) = weights {
                if w.is_null(r) {
                    continue;
                }
                w.value(r)
            } else {
                1.
            };
            let y = ys.value(r);
            if w < 0. {
                self.has_negative_weight = true;
            }
            for i in 0..self.p {
                self.xty[i] += w * x[i] * y;
                for j in 0..self.p {
                    self.xtx[i * self.p + j] += w * x[i] * x[j];
                }
            }
            self.yty += w * y * y;
            self.n += 1;
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.result()
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + (self.xtx.capacity() + self.xty.capacity()) * 8
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let mut s = vec![ScalarValue::Int64(Some(self.n))];
        s.extend(
            self.xtx
                .iter()
                .chain(&self.xty)
                .chain([&self.yty])
                .map(|x| ScalarValue::Float64(Some(*x))),
        );
        s.push(ScalarValue::Boolean(Some(self.has_negative_weight)));
        Ok(s)
    }
    fn merge_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        if v.len() != self.xtx.len() + self.xty.len() + 3 {
            return Err(DataFusionError::Execution(
                "linreg state shape mismatch".into(),
            ));
        }
        let n = v[0].as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..n.len() {
            if n.is_valid(r) {
                self.n += n.value(r);
            }
            let neg = v
                .last()
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("linreg negative-weight state must be bool".into())
                })?;
            self.has_negative_weight |= neg.value(r);
            for (i, a) in v[1..v.len() - 1].iter().enumerate() {
                let f = a.as_any().downcast_ref::<Float64Array>().unwrap();
                if f.is_valid(r) {
                    if i < self.xtx.len() {
                        self.xtx[i] += f.value(r);
                    } else if i < self.xtx.len() + self.xty.len() {
                        self.xty[i - self.xtx.len()] += f.value(r);
                    } else {
                        self.yty += f.value(r);
                    }
                }
            }
        }
        Ok(())
    }
}
