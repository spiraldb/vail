//! Bounded and statistical Hail-style numeric aggregates.
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    new_empty_array, Array, ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray,
    StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{
    expr_fn::create_udaf,
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use kll_rs::KllDoubleSketch;
use nalgebra::{DMatrix, DVector};
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

/// Arithmetic on a validated finite interval, including spans that overflow.
#[derive(Clone, Copy, Debug)]
struct FiniteRange {
    start: f64,
    end: f64,
}
impl FiniteRange {
    fn fraction(self, value: f64) -> f64 {
        let span = self.end - self.start;
        if span.is_finite() {
            (value - self.start) / span
        } else {
            (value * 0.5 - self.start * 0.5) / (self.end * 0.5 - self.start * 0.5)
        }
    }
    fn interpolate(self, t: f64) -> f64 {
        if t == 0. {
            return self.start;
        }
        if t == 1. {
            return self.end;
        }
        let span = self.end - self.start;
        if span.is_finite() {
            self.start + span * t
        } else {
            self.start * (1. - t) + self.end * t
        }
    }
}
#[derive(Debug)]
struct HistogramConfig {
    range: FiniteRange,
    bins: usize,
    edges: Arc<Float64Array>,
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
    let range = FiniteRange { start, end };
    let edges = (0..=bins)
        .map(|i| range.interpolate(i as f64 / bins as f64))
        .collect::<Vec<_>>();
    if edges.iter().any(|edge| !edge.is_finite()) || edges.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid("hist bin edges cannot be represented distinctly"));
    }
    let config = Arc::new(HistogramConfig {
        range,
        bins,
        edges: Arc::new(Float64Array::from(edges)),
    });
    let list = |ty| DataType::List(Arc::new(Field::new("item", ty, true)));
    let fields = vec![
        Arc::new(Field::new("bin_edges", list(DataType::Float64), true)),
        Arc::new(Field::new("bin_freq", list(DataType::Int64), true)),
        Arc::new(Field::new("n_smaller", DataType::Int64, true)),
        Arc::new(Field::new("n_larger", DataType::Int64, true)),
    ];
    let output = DataType::Struct(fields.clone().into());
    let state = list(DataType::Int64);
    let udf_name = configured_udaf_name("hist", t, o);
    let scalar_config = Arc::clone(&config);
    let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
        Ok(Box::new(HistAccumulator {
            config: Arc::clone(&scalar_config),
            counts: vec![0; scalar_config.bins + 2],
        }) as Box<dyn Accumulator>)
    });
    let groups_config = Arc::clone(&config);
    let groups_factory = Arc::new(move || {
        Ok(Box::new(HistGroupsAccumulator {
            config: Arc::clone(&groups_config),
            counts: Vec::new(),
        }) as Box<dyn GroupsAccumulator>)
    });
    let groups_supported = Arc::new(|args: &datafusion_expr::function::AccumulatorArgs<'_>| {
        !args.is_distinct
            && args.order_bys.is_empty()
            && !args.is_reversed
            && args.expr_fields.len() == 1
            && numeric(args.expr_fields[0].data_type())
    });
    Ok(crate::kernel_identity::aggregate_named_with_groups(
        &udf_name,
        (udf_name.clone(), t.clone(), output.clone()),
        vec![t.clone()],
        output,
        vec![state],
        factory,
        Some(groups_factory),
        Some(groups_supported),
    ))
}
#[derive(Debug)]
struct HistAccumulator {
    config: Arc<HistogramConfig>,
    counts: Vec<i64>,
}
fn histogram_bin(config: &HistogramConfig, value: f64) -> Option<usize> {
    if value.is_nan() {
        return None;
    }
    Some(if value < config.range.start {
        0
    } else if value > config.range.end {
        config.bins + 1
    } else if value == config.range.end {
        config.bins
    } else {
        let candidate = ((config.range.fraction(value) * config.bins as f64).floor() as usize)
            .min(config.bins - 1);
        let bin = if value >= config.edges.value(candidate)
            && value < config.edges.value(candidate + 1)
        {
            candidate
        } else {
            config
                .edges
                .values()
                .partition_point(|edge| *edge <= value)
                .saturating_sub(1)
                .min(config.bins - 1)
        };
        bin + 1
    })
}

fn numeric_list(values: ArrayRef) -> ScalarValue {
    ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::from_lengths([values.len()]),
        values,
        None,
    )))
}
fn list_array(values: ArrayRef, list_width: usize) -> DfResult<ArrayRef> {
    if list_width == 0 || values.len() % list_width != 0 {
        return Err(DataFusionError::Execution(
            "hist list values do not match the configured width".into(),
        ));
    }
    let rows = values.len() / list_width;
    if values.len() > i32::MAX as usize {
        return Err(DataFusionError::Execution(
            "hist list output exceeds Arrow's offset limit".into(),
        ));
    }
    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::from_lengths(vec![list_width; rows]),
        values,
        None,
    )))
}

fn histogram_groups_output(config: &HistogramConfig, counts: Vec<i64>) -> DfResult<ArrayRef> {
    let width = config.bins + 2;
    if counts.len() % width != 0 {
        return Err(DataFusionError::Execution(
            "hist grouped state width mismatch".into(),
        ));
    }
    let groups = counts.len() / width;
    let mut edges = Vec::with_capacity(groups.saturating_mul(config.edges.len()));
    let mut frequencies = Vec::with_capacity(groups.saturating_mul(config.bins));
    let mut smaller = Vec::with_capacity(groups);
    let mut larger = Vec::with_capacity(groups);
    for group in counts.chunks_exact(width) {
        edges.extend(config.edges.values().iter().copied());
        frequencies.extend_from_slice(&group[1..=config.bins]);
        smaller.push(group[0]);
        larger.push(group[config.bins + 1]);
    }
    let edge_lists = list_array(Arc::new(Float64Array::from(edges)), config.edges.len())?;
    let frequency_lists = list_array(Arc::new(Int64Array::from(frequencies)), config.bins)?;
    let fields = vec![
        Arc::new(Field::new(
            "bin_edges",
            edge_lists.data_type().clone(),
            true,
        )),
        Arc::new(Field::new(
            "bin_freq",
            frequency_lists.data_type().clone(),
            true,
        )),
        Arc::new(Field::new("n_smaller", DataType::Int64, true)),
        Arc::new(Field::new("n_larger", DataType::Int64, true)),
    ];
    Ok(Arc::new(StructArray::new(
        fields.into(),
        vec![
            edge_lists,
            frequency_lists,
            Arc::new(Int64Array::from(smaller)),
            Arc::new(Int64Array::from(larger)),
        ],
        None,
    )))
}

impl Accumulator for HistAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let [input] = values else {
            return Err(DataFusionError::Execution("hist expects one input".into()));
        };
        let cast = arrow_cast::cast(input, &DataType::Float64)?;
        let values = cast.as_any().downcast_ref::<Float64Array>().unwrap();
        let config = &self.config;
        for row in 0..values.len() {
            if values.is_null(row) {
                continue;
            }
            if let Some(index) = histogram_bin(config, values.value(row)) {
                self.counts[index] = self.counts[index].wrapping_add(1);
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let edges = numeric_list(self.config.edges.clone());
        let counts = numeric_list(Arc::new(Int64Array::from(
            self.counts[1..=self.config.bins].to_vec(),
        )));
        struct_scalar(
            vec![
                Arc::new(Field::new("bin_edges", edges.data_type(), true)),
                Arc::new(Field::new("bin_freq", counts.data_type(), true)),
                Arc::new(Field::new("n_smaller", DataType::Int64, true)),
                Arc::new(Field::new("n_larger", DataType::Int64, true)),
            ],
            vec![
                edges,
                counts,
                ScalarValue::Int64(Some(self.counts[0])),
                ScalarValue::Int64(Some(self.counts[self.config.bins + 1])),
            ],
        )
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.counts.capacity() * 8
            + self.config.edges.get_array_memory_size()
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![numeric_list(Arc::new(Int64Array::from(
            self.counts.clone(),
        )))])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [state] = states else {
            return Err(DataFusionError::Execution(
                "hist state shape mismatch".into(),
            ));
        };
        let lists = state
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("hist state must be int64 list".into()))?;
        let values = lists
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("hist counts must be int64".into()))?;
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            let start = lists.value_offsets()[row] as usize;
            if lists.value_length(row) as usize != self.counts.len() {
                return Err(DataFusionError::Execution(
                    "hist state width mismatch".into(),
                ));
            }
            for (i, count) in self.counts.iter_mut().enumerate() {
                if values.is_null(start + i) {
                    return Err(DataFusionError::Execution(
                        "hist counts must be defined".into(),
                    ));
                }
                *count = count.wrapping_add(values.value(start + i));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct HistGroupsAccumulator {
    config: Arc<HistogramConfig>,
    // Each group owns one contiguous row of `bins + 2` counts.
    counts: Vec<i64>,
}

impl HistGroupsAccumulator {
    fn width(&self) -> usize {
        self.config.bins + 2
    }

    fn ensure_groups(&mut self, groups: usize) -> DfResult<()> {
        let count_len = groups
            .checked_mul(self.width())
            .ok_or_else(|| DataFusionError::Execution("hist grouped state size overflow".into()))?;
        if count_len > i32::MAX as usize {
            return Err(DataFusionError::Execution(
                "hist grouped state exceeds Arrow's offset limit".into(),
            ));
        }
        if count_len > self.counts.len() {
            self.counts.resize(count_len, 0);
        }
        Ok(())
    }

    fn take_counts(&mut self, emit_to: EmitTo) -> DfResult<Vec<i64>> {
        let width = self.width();
        let groups = self.counts.len() / width;
        match emit_to {
            EmitTo::All => Ok(std::mem::take(&mut self.counts)),
            EmitTo::First(n) if n <= groups => Ok(self.counts.drain(0..n * width).collect()),
            EmitTo::First(_) => Err(DataFusionError::Execution(
                "hist emitted group count exceeds accumulator state".into(),
            )),
        }
    }

    fn checked_input<'a>(&self, values: &'a [ArrayRef]) -> DfResult<&'a ArrayRef> {
        let [input] = values else {
            return Err(DataFusionError::Execution("hist expects one input".into()));
        };
        Ok(input)
    }

    fn add_row(&mut self, group: usize, value: f64) {
        if let Some(index) = histogram_bin(&self.config, value) {
            let offset = group * self.width() + index;
            self.counts[offset] = self.counts[offset].wrapping_add(1);
        }
    }
}

impl GroupsAccumulator for HistGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DfResult<()> {
        let input = self.checked_input(values)?;
        if input.len() != group_indices.len()
            || opt_filter.is_some_and(|filter| filter.len() != input.len())
            || group_indices.iter().any(|group| *group >= total_num_groups)
            || !numeric(input.data_type())
        {
            return Err(DataFusionError::Execution(
                "hist grouping input shape/type mismatch".into(),
            ));
        }
        self.ensure_groups(total_num_groups)?;
        let cast = arrow_cast::cast(input, &DataType::Float64)?;
        let values = cast.as_any().downcast_ref::<Float64Array>().unwrap();
        for (row, group) in group_indices.iter().copied().enumerate() {
            if opt_filter.is_some_and(|filter| !filter.is_valid(row) || !filter.value(row))
                || values.is_null(row)
            {
                continue;
            }
            self.add_row(group, values.value(row));
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DfResult<ArrayRef> {
        let config = Arc::clone(&self.config);
        let counts = self.take_counts(emit_to)?;
        histogram_groups_output(&config, counts)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.counts.capacity() * std::mem::size_of::<i64>()
    }

    fn state(&mut self, emit_to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        let width = self.width();
        let counts = self.take_counts(emit_to)?;
        Ok(vec![list_array(Arc::new(Int64Array::from(counts)), width)?])
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DfResult<()> {
        let [state] = states else {
            return Err(DataFusionError::Execution(
                "hist state shape mismatch".into(),
            ));
        };
        let lists = state
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("hist state must be int64 list".into()))?;
        let values = lists
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("hist counts must be int64".into()))?;
        if lists.len() != group_indices.len()
            || group_indices.iter().any(|group| *group >= total_num_groups)
        {
            return Err(DataFusionError::Execution(
                "hist grouped state shape mismatch".into(),
            ));
        }
        self.ensure_groups(total_num_groups)?;
        for (row, group) in group_indices.iter().copied().enumerate() {
            if lists.is_null(row) {
                continue;
            }
            let start = lists.value_offsets()[row] as usize;
            if lists.value_length(row) as usize != self.width() {
                return Err(DataFusionError::Execution(
                    "hist grouped state width mismatch".into(),
                ));
            }
            for offset in 0..self.width() {
                if values.is_null(start + offset) {
                    return Err(DataFusionError::Execution(
                        "hist grouped counts must be defined".into(),
                    ));
                }
                let dst = group * self.width() + offset;
                self.counts[dst] = self.counts[dst].wrapping_add(values.value(start + offset));
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> DfResult<Vec<ArrayRef>> {
        let [input] = values else {
            return Err(DataFusionError::Execution("hist expects one input".into()));
        };
        if opt_filter.is_some_and(|filter| filter.len() != input.len())
            || !numeric(input.data_type())
        {
            return Err(DataFusionError::Execution(
                "hist converter input shape/type mismatch".into(),
            ));
        }
        let state_len = input
            .len()
            .checked_mul(self.width())
            .ok_or_else(|| DataFusionError::Execution("hist grouped state size overflow".into()))?;
        if state_len > i32::MAX as usize {
            return Err(DataFusionError::Execution(
                "hist grouped state exceeds Arrow's offset limit".into(),
            ));
        }
        let cast = arrow_cast::cast(input, &DataType::Float64)?;
        let cast = cast.as_any().downcast_ref::<Float64Array>().unwrap();
        let mut counts = vec![0; state_len];
        for row in 0..cast.len() {
            if opt_filter.is_some_and(|filter| !filter.is_valid(row) || !filter.value(row))
                || cast.is_null(row)
            {
                continue;
            }
            if let Some(index) = histogram_bin(&self.config, cast.value(row)) {
                counts[row * self.width() + index] = 1;
            }
        }
        Ok(vec![list_array(
            Arc::new(Int64Array::from(counts)),
            self.width(),
        )?])
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
    Ok(Arc::new(AggregateUDF::from(CorrelationUdf {
        input: t.clone(),
        signature: Signature::exact(vec![t.clone()], Volatility::Immutable),
    })))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CorrelationUdf {
    input: DataType,
    signature: Signature,
}

fn native_correlation() -> Arc<AggregateUDF> {
    datafusion_functions_aggregate::correlation::corr_udaf()
}

impl AggregateUDFImpl for CorrelationUdf {
    fn name(&self) -> &str {
        "corr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> DfResult<DataType> {
        if args != [self.input.clone()] {
            return Err(DataFusionError::Plan(
                "corr input signature mismatch".into(),
            ));
        }
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<FieldRef>> {
        Ok([
            ("count", DataType::UInt64),
            ("sum_x", DataType::Float64),
            ("sum_y", DataType::Float64),
            ("sum_xy", DataType::Float64),
            ("sum_xx", DataType::Float64),
            ("sum_yy", DataType::Float64),
        ]
        .into_iter()
        .map(|(name, data_type)| {
            Arc::new(Field::new(
                format_state_name(args.name, name),
                data_type,
                true,
            ))
        })
        .collect())
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        let mut inner =
            datafusion_functions_aggregate::correlation::CorrelationGroupsAccumulator::new();
        // Keep the native group state at one row even before the first update,
        // so scalar and grouped partial states have the same six-field layout.
        inner.update_batch(
            &[
                Arc::new(Float64Array::new_null(0)),
                Arc::new(Float64Array::new_null(0)),
            ],
            &[],
            None,
            1,
        )?;
        Ok(Box::new(CorrAccumulator {
            inner,
            pair_count: 0,
        }))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        native_correlation().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DfResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(CorrGroupsAccumulator {
            inner: native_correlation().create_groups_accumulator(args)?,
            pair_counts: Vec::new(),
        }))
    }
}
struct CorrAccumulator {
    inner: datafusion_functions_aggregate::correlation::CorrelationGroupsAccumulator,
    pair_count: u64,
}

impl std::fmt::Debug for CorrAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrAccumulator")
            .field("native_state_size", &self.inner.size())
            .field("pair_count", &self.pair_count)
            .finish()
    }
}

const SINGLE_GROUP_CHUNK: usize = 256;
const SINGLE_GROUP_IDS: [usize; SINGLE_GROUP_CHUNK] = [0; SINGLE_GROUP_CHUNK];

fn update_single_group_in_chunks(
    inner: &mut dyn GroupsAccumulator,
    values: &[ArrayRef],
) -> DfResult<()> {
    let [x, y] = values else {
        return Err(DataFusionError::Execution(
            "corr expects two numeric arrays".into(),
        ));
    };
    if x.len() != y.len() {
        return Err(DataFusionError::Execution(
            "corr input lengths differ".into(),
        ));
    }
    for start in (0..x.len()).step_by(SINGLE_GROUP_CHUNK) {
        let len = (x.len() - start).min(SINGLE_GROUP_CHUNK);
        let chunk = [x.slice(start, len), y.slice(start, len)];
        inner.update_batch(&chunk, &SINGLE_GROUP_IDS[..len], None, 1)?;
    }
    Ok(())
}

fn merge_single_group_in_chunks(
    inner: &mut dyn GroupsAccumulator,
    states: &[ArrayRef],
) -> DfResult<()> {
    if states.len() != 6 {
        return Err(DataFusionError::Execution(
            "corr state expects six columns".into(),
        ));
    }
    let len = states[0].len();
    if states.iter().any(|state| state.len() != len) {
        return Err(DataFusionError::Execution(
            "corr state column lengths differ".into(),
        ));
    }
    let counts = states[0]
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .ok_or_else(|| DataFusionError::Execution("corr count state must be uint64".into()))?;
    if counts.null_count() != 0 {
        return Err(DataFusionError::Execution(
            "corr count state cannot contain nulls".into(),
        ));
    }
    if states
        .iter()
        .skip(1)
        .any(|state| state.data_type() != &DataType::Float64 || state.null_count() != 0)
    {
        return Err(DataFusionError::Execution(
            "corr sum state columns must be non-null float64".into(),
        ));
    }
    for start in (0..len).step_by(SINGLE_GROUP_CHUNK) {
        let n = (len - start).min(SINGLE_GROUP_CHUNK);
        let chunk = states
            .iter()
            .map(|state| state.slice(start, n))
            .collect::<Vec<_>>();
        inner.merge_batch(&chunk, &SINGLE_GROUP_IDS[..n], 1)?;
    }
    Ok(())
}

fn scalar_state_arrays(inner: &mut dyn GroupsAccumulator) -> DfResult<Vec<ArrayRef>> {
    let state = inner.state(EmitTo::First(1))?;
    if state.len() != 6 || state.iter().any(|array| array.len() != 1) {
        return Err(DataFusionError::Execution(
            "corr scalar state must contain one six-field group".into(),
        ));
    }
    // GroupsAccumulator::state emits by draining. Restore the single group so
    // repeated state() calls and a later evaluate() preserve scalar semantics.
    inner.merge_batch(&state, &[0], 1)?;
    Ok(state)
}

fn scalar_state(inner: &mut dyn GroupsAccumulator) -> DfResult<Vec<ScalarValue>> {
    let state = scalar_state_arrays(inner)?;
    state
        .iter()
        .map(|array| ScalarValue::try_from_array(array.as_ref(), 0))
        .collect()
}

fn corr_pair(values: &[ArrayRef]) -> DfResult<Vec<ArrayRef>> {
    let [value] = values else {
        return Err(DataFusionError::Execution(
            "corr expects a struct argument".into(),
        ));
    };
    let value = value
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Execution("corr expects struct values".into()))?;
    let [x, y] = value.columns() else {
        return Err(DataFusionError::Execution("corr requires x and y".into()));
    };
    let x = arrow_cast::cast(x.as_ref(), &DataType::Float64)?;
    let y = arrow_cast::cast(y.as_ref(), &DataType::Float64)?;
    let x = if value.null_count() > 0 {
        arrow_array::make_array(
            x.to_data()
                .into_builder()
                .nulls(arrow_buffer::NullBuffer::union(x.nulls(), value.nulls()))
                .build()?,
        )
    } else {
        x
    };
    Ok(vec![x, y])
}

impl Accumulator for CorrAccumulator {
    fn update_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let pairs = corr_pair(v)?;
        let [x, y] = pairs.as_slice() else {
            unreachable!("corr_pair always returns two arrays")
        };
        let x = x.as_any().downcast_ref::<Float64Array>().unwrap();
        let y = y.as_any().downcast_ref::<Float64Array>().unwrap();
        let pair_count = (0..x.len())
            .filter(|&row| x.is_valid(row) && y.is_valid(row))
            .count() as u64;
        let next_count = self
            .pair_count
            .checked_add(pair_count)
            .ok_or_else(|| DataFusionError::Execution("corr pair count exceeds uint64".into()))?;
        update_single_group_in_chunks(&mut self.inner, &pairs)?;
        self.pair_count = next_count;
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let state = scalar_state_arrays(&mut self.inner)?;
        let value = self.inner.evaluate(EmitTo::First(1));
        let restore = self.inner.merge_batch(&state, &[0], 1);
        let value = value?;
        restore?;
        let value = value
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("corr result must be float64".into()))?;
        if value.len() != 1 {
            return Err(DataFusionError::Execution(
                "corr scalar result must contain one group".into(),
            ));
        }
        if self.pair_count == 0 {
            return Ok(ScalarValue::Float64(None));
        }
        if value.is_null(0) {
            Ok(ScalarValue::Float64(Some(f64::NAN)))
        } else {
            Ok(ScalarValue::Float64(Some(value.value(0))))
        }
    }
    fn size(&self) -> usize {
        self.inner.size() + std::mem::size_of_val(&self.pair_count)
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        scalar_state(&mut self.inner)
    }
    fn merge_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let counts = v
            .first()
            .and_then(|state| state.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| DataFusionError::Execution("corr count state must be uint64".into()))?;
        if counts.null_count() != 0 || v.iter().any(|state| state.len() != counts.len()) {
            return Err(DataFusionError::Execution(
                "corr state columns must have equal lengths and defined counts".into(),
            ));
        }
        let merged_count = counts.values().iter().try_fold(0_u64, |total, count| {
            total
                .checked_add(*count)
                .ok_or_else(|| DataFusionError::Execution("corr pair count exceeds uint64".into()))
        })?;
        let next_count = self
            .pair_count
            .checked_add(merged_count)
            .ok_or_else(|| DataFusionError::Execution("corr pair count exceeds uint64".into()))?;
        merge_single_group_in_chunks(&mut self.inner, v)?;
        self.pair_count = next_count;
        Ok(())
    }
}

struct CorrGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    pair_counts: Vec<u64>,
}

impl GroupsAccumulator for CorrGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        filter: Option<&arrow_array::BooleanArray>,
        total_groups: usize,
    ) -> DfResult<()> {
        let pairs = corr_pair(values)?;
        let x = pairs[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let y = pairs[1].as_any().downcast_ref::<Float64Array>().unwrap();
        if groups.len() != x.len()
            || y.len() != x.len()
            || filter.is_some_and(|filter| filter.len() != x.len())
        {
            return Err(DataFusionError::Execution(
                "corr grouped input lengths differ".into(),
            ));
        }
        if groups.iter().any(|&group| group >= total_groups) {
            return Err(DataFusionError::Execution(
                "corr group index exceeds state".into(),
            ));
        }
        self.pair_counts.resize(total_groups, 0);
        for (row, &group) in groups.iter().enumerate() {
            if filter.is_none_or(|filter| filter.is_valid(row) && filter.value(row))
                && x.is_valid(row)
                && y.is_valid(row)
            {
                self.pair_counts[group] =
                    self.pair_counts[group].checked_add(1).ok_or_else(|| {
                        DataFusionError::Execution("corr pair count exceeds uint64".into())
                    })?;
            }
        }
        self.inner
            .update_batch(&pairs, groups, filter, total_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DfResult<ArrayRef> {
        let result = self.inner.evaluate(emit_to)?;
        let values = result.as_any().downcast_ref::<Float64Array>().unwrap();
        let counts = emit_to.take_needed(&mut self.pair_counts);
        if counts.len() != values.len() {
            return Err(DataFusionError::Execution(
                "corr emitted group count differs from native output".into(),
            ));
        }
        if values.null_count() == 0 {
            return Ok(result);
        }
        Ok(Arc::new(Float64Array::from_iter(
            values
                .iter()
                .zip(counts)
                .map(|(value, count)| (count > 0).then_some(value.unwrap_or(f64::NAN))),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        let states = self.inner.state(emit_to)?;
        let counts = emit_to.take_needed(&mut self.pair_counts);
        let native_counts = states
            .first()
            .and_then(|state| state.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| DataFusionError::Execution("corr count state must be uint64".into()))?;
        if native_counts.len() != counts.len()
            || counts
                .iter()
                .enumerate()
                .any(|(row, count)| native_counts.value(row) != *count)
        {
            return Err(DataFusionError::Execution(
                "corr wrapper and native pair counts differ".into(),
            ));
        }
        Ok(states)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        groups: &[usize],
        total_groups: usize,
    ) -> DfResult<()> {
        let counts = states
            .first()
            .and_then(|state| state.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| DataFusionError::Execution("corr count state must be uint64".into()))?;
        if counts.len() != groups.len() {
            return Err(DataFusionError::Execution(
                "corr grouped state lengths differ".into(),
            ));
        }
        if groups.iter().any(|&group| group >= total_groups) {
            return Err(DataFusionError::Execution(
                "corr group index exceeds state".into(),
            ));
        }
        self.pair_counts.resize(total_groups, 0);
        for (row, &group) in groups.iter().enumerate() {
            self.pair_counts[group] = self.pair_counts[group]
                .checked_add(if counts.is_valid(row) {
                    counts.value(row)
                } else {
                    0
                })
                .ok_or_else(|| {
                    DataFusionError::Execution("corr pair count exceeds uint64".into())
                })?;
        }
        self.inner.merge_batch(states, groups, total_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&arrow_array::BooleanArray>,
    ) -> DfResult<Vec<ArrayRef>> {
        self.inner.convert_to_state(&corr_pair(values)?, filter)
    }

    fn size(&self) -> usize {
        self.inner.size() + self.pair_counts.capacity() * std::mem::size_of::<u64>()
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
    label_size: usize,
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
    if fields.len() == 3
        && !matches!(fields[2].data_type(), DataType::Utf8)
        && !matches!(fields[2].data_type(), DataType::List(item) if item.data_type() == &DataType::Utf8)
    {
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
    let native = Arc::new(create_udaf(
        &udaf_name,
        vec![input.clone()],
        Arc::new(output.clone()),
        Volatility::Immutable,
        factory,
        Arc::new(vec![
            state,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ]),
    ));
    Ok(Arc::new(AggregateUDF::from(DownsampleUdf {
        name: udaf_name,
        input_type: input.clone(),
        output_type: output,
        signature: Signature::exact(vec![input.clone()], Volatility::Immutable),
        native,
        divisions,
    })))
}

#[derive(Debug)]
struct DownsampleUdf {
    name: String,
    input_type: DataType,
    output_type: DataType,
    signature: Signature,
    native: Arc<AggregateUDF>,
    divisions: usize,
}
impl PartialEq for DownsampleUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.input_type == other.input_type
            && self.output_type == other.output_type
            && self.divisions == other.divisions
    }
}
impl Eq for DownsampleUdf {}
impl Hash for DownsampleUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.input_type.hash(state);
        self.output_type.hash(state);
        self.divisions.hash(state);
    }
}
impl AggregateUDFImpl for DownsampleUdf {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> DfResult<DataType> {
        if args != [self.input_type.clone()] {
            return Err(DataFusionError::Plan(
                "downsample input type mismatch".into(),
            ));
        }
        Ok(self.output_type.clone())
    }
    fn is_nullable(&self) -> bool {
        self.native.is_nullable()
    }
    fn accumulator(&self, args: AccumulatorArgs) -> DfResult<Box<dyn Accumulator>> {
        self.native.inner().accumulator(args)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> DfResult<Vec<arrow_schema::FieldRef>> {
        self.native.inner().state_fields(args)
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
            && args.order_bys.is_empty()
            && !args.is_reversed
            && args.expr_fields.len() == 1
            && args.expr_fields[0].data_type() == &self.input_type
            && args.return_field.data_type() == &self.output_type
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DfResult<Box<dyn GroupsAccumulator>> {
        if !self.groups_accumulator_supported(args) {
            return Err(DataFusionError::NotImplemented(
                "downsample grouped accumulator does not support these arguments".into(),
            ));
        }
        Ok(Box::new(DownsampleGroups {
            divisions: self.divisions,
            groups: Vec::new(),
            state_bytes: 0,
        }))
    }
}
#[derive(Debug)]
struct DownsampleAccumulator {
    divisions: usize,
    points: BTreeMap<(i32, i32), DownsamplePoint>,
    bounds: Option<(f64, f64, f64, f64)>,
    label_bytes: usize,
}
impl DownsampleAccumulator {
    fn new(divisions: usize) -> Self {
        Self {
            divisions,
            points: BTreeMap::new(),
            bounds: None,
            label_bytes: 0,
        }
    }
    fn heap_size(&self) -> usize {
        self.points.len() * std::mem::size_of::<((i32, i32), DownsamplePoint)>() + self.label_bytes
    }
    fn cell(&self, x: f64, y: f64, bounds: (f64, f64, f64, f64)) -> (i32, i32) {
        let axis = |value, start, end| {
            if start == end {
                0
            } else {
                ((FiniteRange { start, end }.fraction(value) * self.divisions as f64).floor()
                    as i32)
                    .clamp(0, self.divisions as i32 - 1)
            }
        };
        (axis(x, bounds.0, bounds.1), axis(y, bounds.2, bounds.3))
    }
    fn expand_axis(mut start: f64, mut end: f64, value: f64) -> (f64, f64) {
        if start == end {
            return (start.min(value), end.max(value));
        }
        while value < start || value > end {
            let span = end - start;
            if !span.is_finite() {
                return (-f64::MAX, f64::MAX);
            }
            if value < start {
                start = (start - span).max(-f64::MAX);
            } else {
                end = (end + span).min(f64::MAX);
            }
        }
        (start, end)
    }
    fn include_bounds(&mut self, x: f64, y: f64) {
        let bounds = match self.bounds {
            None => (x, x, y, y),
            Some((l, r, b, t)) => {
                let (l, r) = Self::expand_axis(l, r, x);
                let (b, t) = Self::expand_axis(b, t, y);
                (l, r, b, t)
            }
        };
        if self.bounds != Some(bounds) {
            self.bounds = Some(bounds);
            let old = std::mem::take(&mut self.points);
            self.label_bytes = 0;
            for point in old.into_values() {
                let cell = self.cell(point.x, point.y, bounds);
                if let std::collections::btree_map::Entry::Vacant(entry) = self.points.entry(cell) {
                    self.label_bytes += point.label_size;
                    entry.insert(point);
                }
            }
        }
    }
    fn insert_lazy(
        &mut self,
        x: f64,
        y: f64,
        label: impl FnOnce() -> DfResult<ScalarValue>,
    ) -> DfResult<()> {
        self.include_bounds(x, y);
        let cell = self.cell(x, y, self.bounds.unwrap());
        if let std::collections::btree_map::Entry::Vacant(entry) = self.points.entry(cell) {
            let label = label()?;
            let label_size = label.size();
            self.label_bytes += label_size;
            entry.insert(DownsamplePoint {
                x,
                y,
                label,
                label_size,
            });
        }
        Ok(())
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
    fn add_rows(
        &mut self,
        rows: &StructArray,
        xs: &Float64Array,
        ys: &Float64Array,
        label: impl Fn(usize) -> DfResult<ScalarValue>,
    ) -> DfResult<()> {
        for row in 0..rows.len() {
            if rows.is_null(row) || xs.is_null(row) || ys.is_null(row) {
                continue;
            }
            let x = xs.value(row);
            let y = ys.value(row);
            if !x.is_finite() || !y.is_finite() {
                continue;
            }
            self.insert_lazy(x, y, || label(row))?;
        }
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
        let x = arrow_cast::cast(&s.columns()[0], &DataType::Float64)?;
        let y = arrow_cast::cast(&s.columns()[1], &DataType::Float64)?;
        let xs = x.as_any().downcast_ref::<Float64Array>().unwrap();
        let ys = y.as_any().downcast_ref::<Float64Array>().unwrap();
        match s.columns().get(2) {
            None => self.add_rows(s, xs, ys, |_| Self::empty_label()),
            Some(labels) if labels.data_type() == &DataType::Utf8 => {
                let labels = labels
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                self.add_rows(s, xs, ys, |row| {
                    if labels.is_null(row) {
                        Self::empty_label()
                    } else {
                        Ok(numeric_list(Arc::new(arrow_array::StringArray::from(
                            vec![labels.value(row)],
                        ))))
                    }
                })
            }
            Some(labels) => self.add_rows(s, xs, ys, |row| {
                if labels.is_null(row) {
                    Self::empty_label()
                } else {
                    ScalarValue::try_from_array(labels, row)
                }
            }),
        }
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.result()
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.heap_size()
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let mut state = vec![self.result()?];
        let bounds = self.bounds.map(|(l, r, b, t)| [l, r, b, t]);
        state.extend((0..4).map(|i| ScalarValue::Float64(bounds.map(|values| values[i]))));
        Ok(state)
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [points, l, r, b, t] = states else {
            return Err(DataFusionError::Execution(
                "downsample state requires points and bounds".into(),
            ));
        };
        let lists = points
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("downsample state must be list".into()))?;
        let bound_columns = [l, r, b, t];
        let bounds = bound_columns
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("downsample bounds must be float64".into())
                    })
            })
            .collect::<DfResult<Vec<_>>>()?;
        if bounds.iter().any(|array| array.len() != lists.len()) {
            return Err(DataFusionError::Execution(
                "downsample state row counts differ".into(),
            ));
        }
        let tuples = lists
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("downsample points must be tuples".into()))?;
        let x = tuples
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("downsample x state must be float64".into())
            })?;
        let y = tuples
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("downsample y state must be float64".into())
            })?;
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            if bounds.iter().all(|values| values.is_null(row)) && lists.value_length(row) == 0 {
                continue;
            }
            if bounds
                .iter()
                .any(|values| values.is_null(row) || !values.value(row).is_finite())
            {
                return Err(DataFusionError::Execution(
                    "downsample state bounds must be finite".into(),
                ));
            }
            let (l, r, b, t) = (
                bounds[0].value(row),
                bounds[1].value(row),
                bounds[2].value(row),
                bounds[3].value(row),
            );
            if l > r || b > t {
                return Err(DataFusionError::Execution(
                    "downsample state bounds are reversed".into(),
                ));
            }
            self.include_bounds(l, b);
            self.include_bounds(r, t);
            let start = lists.value_offsets()[row] as usize;
            let end = lists.value_offsets()[row + 1] as usize;
            for i in start..end {
                if tuples.is_null(i)
                    || x.is_null(i)
                    || y.is_null(i)
                    || !x.value(i).is_finite()
                    || !y.value(i).is_finite()
                    || x.value(i) < l
                    || x.value(i) > r
                    || y.value(i) < b
                    || y.value(i) > t
                {
                    return Err(DataFusionError::Execution(
                        "downsample state point lies outside bounds".into(),
                    ));
                }
                self.insert_lazy(x.value(i), y.value(i), || {
                    ScalarValue::try_from_array(tuples.column(2), i)
                })?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct DownsampleGroups {
    divisions: usize,
    groups: Vec<DownsampleAccumulator>,
    state_bytes: usize,
}
impl DownsampleGroups {
    fn ensure(&mut self, total: usize) {
        if total < self.groups.len() {
            let dropped = self.groups[total..]
                .iter()
                .map(DownsampleAccumulator::heap_size)
                .sum::<usize>();
            self.state_bytes = self.state_bytes.saturating_sub(dropped);
        }
        self.groups
            .resize_with(total, || DownsampleAccumulator::new(self.divisions));
    }
    fn emitted(&mut self, to: EmitTo) -> DfResult<Vec<DownsampleAccumulator>> {
        let count = match to {
            EmitTo::All => self.groups.len(),
            EmitTo::First(count) => count,
        };
        if count > self.groups.len() {
            return Err(DataFusionError::Execution(
                "downsample emitted groups exceed state".into(),
            ));
        }
        let emitted_bytes = self.groups[..count]
            .iter()
            .map(DownsampleAccumulator::heap_size)
            .sum::<usize>();
        self.state_bytes = self.state_bytes.saturating_sub(emitted_bytes);
        if count == self.groups.len() {
            Ok(std::mem::take(&mut self.groups))
        } else {
            Ok(self.groups.drain(..count).collect())
        }
    }
    fn row_label(labels: Option<&ArrayRef>, row: usize) -> DfResult<ScalarValue> {
        let Some(labels) = labels else {
            return DownsampleAccumulator::empty_label();
        };
        if labels.is_null(row) {
            return DownsampleAccumulator::empty_label();
        }
        if labels.data_type() == &DataType::Utf8 {
            let labels = labels
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("downsample label must be utf8".into())
                })?;
            Ok(numeric_list(Arc::new(arrow_array::StringArray::from(
                vec![labels.value(row)],
            ))))
        } else {
            ScalarValue::try_from_array(labels, row)
        }
    }
    fn state_arrays(groups: &mut [DownsampleAccumulator], state: bool) -> DfResult<Vec<ArrayRef>> {
        if groups.is_empty() {
            let fields = if state {
                vec![
                    downsample_type(),
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ]
            } else {
                vec![downsample_type()]
            };
            return Ok(fields.into_iter().map(|ty| new_empty_array(&ty)).collect());
        }
        let width = if state { 5 } else { 1 };
        let mut columns = (0..width)
            .map(|_| Vec::with_capacity(groups.len()))
            .collect::<Vec<_>>();
        for group in groups {
            let values = if state {
                group.state()?
            } else {
                vec![group.evaluate()?]
            };
            for (column, value) in columns.iter_mut().zip(values) {
                column.push(value);
            }
        }
        columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect()
    }
    fn convert_rows_to_state(
        values: &[ArrayRef],
        filter: Option<&arrow_array::BooleanArray>,
    ) -> DfResult<Vec<ArrayRef>> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "downsample requires one input".into(),
            ));
        };
        let rows = input.len();
        if filter.is_some_and(|filter| filter.len() != rows) {
            return Err(DataFusionError::Execution(
                "downsample filter row count differs".into(),
            ));
        }
        let packed = input
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("downsample expects struct values".into()))?;
        if packed.num_columns() < 2 || packed.num_columns() > 3 {
            return Err(DataFusionError::Execution(
                "downsample packed fields mismatch".into(),
            ));
        }
        let x = arrow_cast::cast(packed.column(0), &DataType::Float64)?;
        let y = arrow_cast::cast(packed.column(1), &DataType::Float64)?;
        let xs = x.as_any().downcast_ref::<Float64Array>().unwrap();
        let ys = y.as_any().downcast_ref::<Float64Array>().unwrap();
        let labels = packed.columns().get(2);
        // A no-op row selection is common when DataFusion converts a batch to
        // grouped state. Keep the original UTF-8 value buffers in that case:
        // wrapping scalar labels in a list only needs offsets and validity.
        let identity_selection = (0..rows).all(|row| {
            !packed.is_null(row)
                && !xs.is_null(row)
                && !ys.is_null(row)
                && xs.value(row).is_finite()
                && ys.value(row).is_finite()
                && filter.is_none_or(|f| f.is_valid(row) && f.value(row))
        });
        let shared_labels = if identity_selection {
            match labels {
                Some(labels) if labels.data_type() == &DataType::Utf8 => {
                    let labels = labels
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("downsample label must be utf8".into())
                        })?;
                    Some(Arc::new(ListArray::new(
                        Arc::new(Field::new("item", DataType::Utf8, true)),
                        OffsetBuffer::from_lengths(std::iter::repeat_n(1, rows)),
                        Arc::new(labels.clone()),
                        labels.nulls().cloned(),
                    )) as ArrayRef)
                }
                Some(labels) if matches!(labels.data_type(), DataType::List(_)) => {
                    let labels = labels.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                        DataFusionError::Execution("downsample label array must be a list".into())
                    })?;
                    let items = labels
                        .values()
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "downsample label list must contain strings".into(),
                            )
                        })?;
                    Some(Arc::new(ListArray::new(
                        Arc::new(Field::new("item", DataType::Utf8, true)),
                        labels.offsets().clone(),
                        Arc::new(items.clone()),
                        labels.nulls().cloned(),
                    )) as ArrayRef)
                }
                _ => None,
            }
        } else {
            None
        };
        let reuse_labels = shared_labels.is_some();
        let mut point_lengths = Vec::with_capacity(rows);
        let mut label_lengths = if reuse_labels {
            Vec::new()
        } else {
            Vec::with_capacity(rows)
        };
        let mut label_validity = if reuse_labels {
            Vec::new()
        } else {
            Vec::with_capacity(rows)
        };
        let mut label_values: Vec<Option<&str>> = Vec::new();
        let mut point_x = Vec::with_capacity(rows);
        let mut point_y = Vec::with_capacity(rows);
        let mut left = Vec::with_capacity(rows);
        let mut right = Vec::with_capacity(rows);
        let mut bottom = Vec::with_capacity(rows);
        let mut top = Vec::with_capacity(rows);
        for row in 0..rows {
            let included = !packed.is_null(row)
                && !xs.is_null(row)
                && !ys.is_null(row)
                && xs.value(row).is_finite()
                && ys.value(row).is_finite()
                && filter.is_none_or(|f| f.is_valid(row) && f.value(row));
            if !included {
                point_lengths.push(0);
                left.push(None);
                right.push(None);
                bottom.push(None);
                top.push(None);
                continue;
            }
            let (x, y) = (xs.value(row), ys.value(row));
            point_lengths.push(1);
            point_x.push(x);
            point_y.push(y);
            left.push(Some(x));
            right.push(Some(x));
            bottom.push(Some(y));
            top.push(Some(y));
            if reuse_labels {
                continue;
            }
            match labels {
                None => {
                    label_lengths.push(0);
                    label_validity.push(false);
                }
                Some(labels) if labels.is_null(row) => {
                    label_lengths.push(0);
                    label_validity.push(false);
                }
                Some(labels) if labels.data_type() == &DataType::Utf8 => {
                    let labels = labels
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("downsample label must be utf8".into())
                        })?;
                    label_values.push(Some(labels.value(row)));
                    label_lengths.push(1);
                    label_validity.push(true);
                }
                Some(labels) => {
                    let labels = labels.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                        DataFusionError::Execution("downsample label array must be a list".into())
                    })?;
                    let items = labels
                        .values()
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "downsample label list must contain strings".into(),
                            )
                        })?;
                    let start = labels.value_offsets()[row] as usize;
                    let end = labels.value_offsets()[row + 1] as usize;
                    label_values.extend(
                        (start..end)
                            .map(|index| (!items.is_null(index)).then(|| items.value(index))),
                    );
                    label_lengths.push(end - start);
                    label_validity.push(true);
                }
            }
        }
        let labels = shared_labels.unwrap_or_else(|| {
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                OffsetBuffer::from_lengths(label_lengths),
                Arc::new(arrow_array::StringArray::from(label_values)),
                Some(NullBuffer::from(label_validity)),
            ))
        });
        let DataType::Struct(fields) = tuple_item_type() else {
            unreachable!()
        };
        let tuples: ArrayRef = Arc::new(StructArray::try_new(
            fields,
            vec![
                Arc::new(Float64Array::from(point_x)),
                Arc::new(Float64Array::from(point_y)),
                labels,
            ],
            None,
        )?);
        let points = Arc::new(ListArray::new(
            Arc::new(Field::new("item", tuple_item_type(), true)),
            OffsetBuffer::from_lengths(point_lengths),
            tuples,
            None,
        ));
        Ok(vec![
            points,
            Arc::new(Float64Array::from(left)),
            Arc::new(Float64Array::from(right)),
            Arc::new(Float64Array::from(bottom)),
            Arc::new(Float64Array::from(top)),
        ])
    }
    fn update_inner(
        &mut self,
        values: &[ArrayRef],
        indices: Option<&[usize]>,
        filter: Option<&arrow_array::BooleanArray>,
        total: usize,
    ) -> DfResult<()> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "downsample requires one input".into(),
            ));
        };
        let rows = input.len();
        if indices.is_some_and(|indices| indices.len() != rows)
            || filter.is_some_and(|filter| filter.len() != rows)
        {
            return Err(DataFusionError::Execution(
                "downsample grouped row counts differ".into(),
            ));
        }
        if indices.is_some_and(|indices| indices.iter().any(|&index| index >= total)) {
            return Err(DataFusionError::Execution(
                "downsample group index exceeds state".into(),
            ));
        }
        self.ensure(total);
        let packed = input
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("downsample expects struct values".into()))?;
        if packed.num_columns() < 2 || packed.num_columns() > 3 {
            return Err(DataFusionError::Execution(
                "downsample packed fields mismatch".into(),
            ));
        }
        let x = arrow_cast::cast(packed.column(0), &DataType::Float64)?;
        let y = arrow_cast::cast(packed.column(1), &DataType::Float64)?;
        let xs = x.as_any().downcast_ref::<Float64Array>().unwrap();
        let ys = y.as_any().downcast_ref::<Float64Array>().unwrap();
        let labels = packed.columns().get(2);
        for row in 0..rows {
            if filter.is_some_and(|f| f.is_null(row) || !f.value(row))
                || packed.is_null(row)
                || xs.is_null(row)
                || ys.is_null(row)
            {
                continue;
            }
            let (x, y) = (xs.value(row), ys.value(row));
            if !x.is_finite() || !y.is_finite() {
                continue;
            }
            let group = indices.map_or(row, |indices| indices[row]);
            let before = self.groups[group].heap_size();
            let update = self.groups[group].insert_lazy(x, y, || Self::row_label(labels, row));
            let after = self.groups[group].heap_size();
            self.state_bytes = self
                .state_bytes
                .saturating_sub(before)
                .saturating_add(after);
            update?;
        }
        Ok(())
    }
}
impl GroupsAccumulator for DownsampleGroups {
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&arrow_array::BooleanArray>,
    ) -> DfResult<Vec<ArrayRef>> {
        Self::convert_rows_to_state(values, filter)
    }
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> DfResult<()> {
        self.update_inner(values, Some(group_indices), filter, total_num_groups)
    }
    fn evaluate(&mut self, emit_to: EmitTo) -> DfResult<ArrayRef> {
        let mut groups = self.emitted(emit_to)?;
        Self::state_arrays(&mut groups, false).map(|mut arrays| arrays.remove(0))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.groups.capacity() * std::mem::size_of::<DownsampleAccumulator>()
            + self.state_bytes
    }
    fn state(&mut self, emit_to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        let mut groups = self.emitted(emit_to)?;
        Self::state_arrays(&mut groups, true)
    }
    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DfResult<()> {
        if states.len() != 5
            || states
                .first()
                .is_none_or(|state| state.len() != group_indices.len())
            || states
                .iter()
                .any(|state| state.len() != group_indices.len())
        {
            return Err(DataFusionError::Execution(
                "downsample grouped state rows differ".into(),
            ));
        }
        if group_indices.iter().any(|&index| index >= total_num_groups) {
            return Err(DataFusionError::Execution(
                "downsample group index exceeds state".into(),
            ));
        }
        self.ensure(total_num_groups);
        for (row, &group) in group_indices.iter().enumerate() {
            let before = self.groups[group].heap_size();
            let one_row = states
                .iter()
                .map(|state| state.slice(row, 1))
                .collect::<Vec<_>>();
            let merge = self.groups[group].merge_batch(&one_row);
            let after = self.groups[group].heap_size();
            self.state_bytes = self
                .state_bytes
                .saturating_sub(before)
                .saturating_add(after);
            merge?;
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
        DataType::Struct(xs) if xs.iter().all(|f| f.data_type() == &DataType::Float64) => xs.len(),
        DataType::List(f) if *f.data_type() == DataType::Float64 => p,
        DataType::Float64 => 1,
        _ => return Err(invalid("linreg x must contain float64 predictors")),
    };
    if n_actual != p {
        return Err(invalid("linreg n_predictors does not match x type"));
    }
    if *fields[0].data_type() != DataType::Float64 {
        return Err(invalid("linreg y must be float64"));
    }
    let state = vec![
        DataType::Int64,
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        DataType::Boolean,
    ];
    let result = linreg_type();
    let factory: datafusion_expr::AccumulatorFactoryFunction = Arc::new(move |_| {
        Ok(Box::new(LinregAccumulator::new(p, nested, weighted)) as Box<dyn Accumulator>)
    });
    let groups_factory = Arc::new(move || {
        Ok(Box::new(LinregGroupsAccumulator::new(p, nested, weighted))
            as Box<dyn GroupsAccumulator>)
    });
    let udaf_name = configured_udaf_name("linreg", t, o);
    let grouped_input = t.clone();
    let groups_supported = Arc::new(
        move |args: &datafusion_expr::function::AccumulatorArgs<'_>| {
            !args.is_distinct
                && args.order_bys.is_empty()
                && !args.is_reversed
                && args.expr_fields.len() == 1
                && args.expr_fields[0].data_type() == &grouped_input
        },
    );
    Ok(crate::kernel_identity::aggregate_named_with_groups(
        &udaf_name,
        (udaf_name.clone(), t.clone(), result.clone()),
        vec![t.clone()],
        result,
        state,
        factory,
        Some(groups_factory),
        Some(groups_supported),
    ))
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
    // Upper triangular QR factor of the augmented weighted [X | y].
    qr: Vec<f64>,
    work: Vec<f64>,
    // Column shear relative to x[0], retained across partial QR merges.
    origins: Vec<f64>,
    origin_set: bool,
    invalid_numeric: bool,
}
impl LinregAccumulator {
    fn new(p: usize, nested: usize, weighted: bool) -> Self {
        Self {
            p,
            nested,
            weighted,
            n: 0,
            qr: vec![0.; (p + 1) * (p + 1)],
            work: vec![0.; p + 1],
            origins: vec![0.; p + 1],
            origin_set: false,
            invalid_numeric: false,
        }
    }
    fn append_work(&mut self) {
        append_linreg_work(
            self.p,
            &mut self.qr,
            &mut self.work,
            &mut self.invalid_numeric,
        );
    }
    fn accumulate_observation(&mut self, x: &[f64], y: f64, weight: f64) {
        accumulate_linreg_observation(
            self.p,
            &mut self.n,
            &mut self.qr,
            &mut self.work,
            &mut self.origins,
            &mut self.origin_set,
            &mut self.invalid_numeric,
            x,
            y,
            weight,
        );
    }
    fn result(&self) -> DfResult<ScalarValue> {
        linreg_result(
            self.p,
            self.nested,
            self.n,
            &self.qr,
            &self.origins,
            self.invalid_numeric,
        )
    }
}

#[inline]
fn linreg_hypot(a: f64, b: f64) -> f64 {
    let scale = a.abs().max(b.abs());
    // The common QR path has moderate finite values. Squaring is safe in this
    // range and lets LLVM use a hardware sqrt instead of the general-purpose
    // hypot implementation. Keep hypot for extremes, where direct squares
    // could overflow or underflow.
    if (1e-150..=1e150).contains(&scale) {
        a.mul_add(a, b * b).sqrt()
    } else {
        a.hypot(b)
    }
}

fn append_linreg_work(p: usize, qr: &mut [f64], work: &mut [f64], invalid_numeric: &mut bool) {
    let d = p + 1;
    for j in 0..d {
        let a = qr[j * d + j];
        let b = work[j];
        if b == 0. {
            continue;
        }
        let norm = linreg_hypot(a, b);
        if !norm.is_finite() {
            *invalid_numeric = true;
            return;
        }
        let c = a / norm;
        let sine = b / norm;
        qr[j * d + j] = norm;
        for k in j + 1..d {
            let old = qr[j * d + k];
            let value = work[k];
            qr[j * d + k] = c * old + sine * value;
            work[k] = c * value - sine * old;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn accumulate_linreg_observation(
    p: usize,
    n: &mut i64,
    qr: &mut [f64],
    work: &mut [f64],
    origins: &mut [f64],
    origin_set: &mut bool,
    invalid_numeric: &mut bool,
    x: &[f64],
    y: f64,
    weight: f64,
) {
    // Hail's n counts defined rows even when their numeric values make the fit invalid.
    *n += 1;
    if weight < 0.0 || !weight.is_finite() || !y.is_finite() || x.iter().any(|v| !v.is_finite()) {
        *invalid_numeric = true;
        return;
    }
    if !*origin_set && x[0] != 0.0 {
        for j in 1..p {
            let origin = x[j] / x[0];
            origins[j] = if origin.is_finite() { origin } else { 0.0 };
        }
        let origin = y / x[0];
        origins[p] = if origin.is_finite() { origin } else { 0.0 };
        *origin_set = true;
    }
    let scale = weight.sqrt();
    work[0] = scale * x[0];
    for j in 1..p {
        work[j] = scale * (-origins[j]).mul_add(x[0], x[j]);
    }
    work[p] = scale * (-origins[p]).mul_add(x[0], y);
    if work.iter().any(|value| !value.is_finite()) {
        *invalid_numeric = true;
        return;
    }
    append_linreg_work(p, qr, work, invalid_numeric);
}

fn linreg_result(
    p: usize,
    nested: usize,
    n: i64,
    qr: &[f64],
    origins: &[f64],
    invalid_numeric: bool,
) -> DfResult<ScalarValue> {
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
    if n as usize > p {
        if invalid_numeric {
            let nan = || vec![f64::NAN; p];
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
            let d = p + 1;
            let r = DMatrix::from_fn(p, p, |i, j| qr[i * d + j]);
            // Compare each diagonal to its own column norm, rather than
            // the magnitude of unrelated translated/scaled predictors.
            let full_rank = (0..p).all(|j| {
                let norm = (0..=j).map(|i| r[(i, j)]).fold(0f64, f64::hypot);
                r[(j, j)].abs() > f64::EPSILON * p as f64 * norm
            });
            if full_rank {
                let xy = DVector::from_iterator(p, (0..p).map(|i| qr[i * d + p]));
                if let Some(mut beta) = r.solve_upper_triangular(&xy) {
                    let residual = qr[p * d + p];
                    let df = (n as usize - p) as f64;
                    let sigma = residual / df.sqrt();
                    beta[0] += origins[p];
                    for j in 1..p {
                        beta[0] = (-origins[j]).mul_add(beta[j], beta[0]);
                    }
                    let transpose = r.transpose();
                    let mut se = Vec::with_capacity(p);
                    for i in 0..p {
                        let mut basis = DVector::zeros(p);
                        basis[i] = 1.;
                        if i == 0 {
                            for j in 1..p {
                                basis[j] = -origins[j];
                            }
                        }
                        let Some(column) = transpose.solve_lower_triangular(&basis) else {
                            return Err(DataFusionError::Execution(
                                "linreg triangular covariance solve failed".into(),
                            ));
                        };
                        se.push(
                            column
                                .iter()
                                .map(|value| sigma * value)
                                .fold(0f64, f64::hypot),
                        );
                    }
                    let t = (0..p).map(|i| beta[i] / se[i]).collect::<Vec<_>>();
                    let student = StudentsT::new(0., 1., df).ok();
                    let pvals = t
                        .iter()
                        .map(|x| {
                            if x.is_nan() {
                                f64::NAN
                            } else {
                                student
                                    .as_ref()
                                    .map(|dist| 2. * dist.sf(x.abs()))
                                    .unwrap_or(f64::NAN)
                            }
                        })
                        .collect::<Vec<_>>();
                    let response = |i| {
                        if i == 0 {
                            origins[p].mul_add(qr[0], qr[p])
                        } else {
                            qr[i * d + p]
                        }
                    };
                    let null_residual = (nested..=p).map(response).fold(0f64, f64::hypot);
                    let explained = (nested..p).map(response).fold(0f64, f64::hypot);
                    let d0 = (p - nested) as f64;
                    let ratio = residual / null_residual;
                    let r2 = 1. - ratio * ratio;
                    let adjusted = 1. - (1. - r2) * (n as f64 - nested as f64) / df;
                    let explained_ratio = explained / residual;
                    let f = explained_ratio * explained_ratio * df / d0;
                    let model_p = if f.is_nan() {
                        f64::NAN
                    } else {
                        FisherSnedecor::new(d0, df)
                            .map(|dist| dist.sf(f))
                            .unwrap_or(f64::NAN)
                    };
                    values = vec![
                        list(beta.iter().copied().collect()),
                        list(se),
                        list(t),
                        list(pvals),
                        ScalarValue::Float64(Some(sigma)),
                        ScalarValue::Float64(Some(r2)),
                        ScalarValue::Float64(Some(adjusted)),
                        ScalarValue::Float64(Some(f)),
                        ScalarValue::Float64(Some(model_p)),
                    ];
                }
            }
        }
    }
    values.push(ScalarValue::Int64(Some(n)));
    struct_scalar(fields.to_vec(), values)
}
impl Accumulator for LinregAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "linreg expects one struct input".into(),
            ));
        };
        let rows = input
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("linreg expects struct values".into()))?;
        let columns = rows.columns();
        let ys = columns[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg y must be float64".into()))?;
        let predictors = columns[1].as_ref();
        let lists = predictors.as_any().downcast_ref::<ListArray>();
        let scalar = predictors.as_any().downcast_ref::<Float64Array>();
        let predictor_columns = predictors
            .as_any()
            .downcast_ref::<StructArray>()
            .map(|xs| {
                if xs.columns().len() != self.p {
                    return Err(DataFusionError::Execution(format!(
                        "linreg predictor struct has {} fields, expected {}",
                        xs.columns().len(),
                        self.p
                    )));
                }
                xs.columns()
                    .iter()
                    .map(|column| {
                        column
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "linreg predictors must be float64".into(),
                                )
                            })
                    })
                    .collect::<DfResult<Vec<_>>>()
            })
            .transpose()?;
        let children = lists
            .map(|list| {
                list.values()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("linreg predictors must be float64".into())
                    })
            })
            .transpose()?;
        let weights = if self.weighted {
            Some(
                columns[2]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("linreg weight must be float64".into())
                    })?,
            )
        } else {
            None
        };
        // Pick the input representation once per batch. Struct predictors use one
        // reusable scratch vector; list predictors borrow directly from Arrow values.
        let mut scratch = vec![0.0; self.p];
        for row in 0..rows.len() {
            if rows.is_null(row)
                || ys.is_null(row)
                || predictors.is_null(row)
                || weights.is_some_and(|w| w.is_null(row))
            {
                continue;
            }
            let x: &[f64] = if let Some(xs) = &predictor_columns {
                if xs.iter().any(|column| column.is_null(row)) {
                    continue;
                }
                for (slot, column) in scratch.iter_mut().zip(xs) {
                    *slot = column.value(row);
                }
                &scratch
            } else if let (Some(list), Some(values)) = (lists, children) {
                let offsets = list.value_offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                if end - start != self.p || (start..end).any(|i| values.is_null(i)) {
                    continue;
                }
                &values.values()[start..end]
            } else if let Some(values) = scalar {
                if self.p != 1 {
                    return Err(DataFusionError::Execution(
                        "linreg scalar predictor requires exactly one predictor".into(),
                    ));
                }
                std::slice::from_ref(&values.values()[row])
            } else {
                return Err(DataFusionError::Execution(
                    "linreg x has unsupported predictor representation".into(),
                ));
            };
            self.accumulate_observation(x, ys.value(row), weights.map_or(1.0, |w| w.value(row)));
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.result()
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (self.qr.capacity() + self.work.capacity() + self.origins.capacity()) * 8
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Int64(Some(self.n)),
            numeric_list(Arc::new(Float64Array::from(self.qr.clone()))),
            if self.origin_set {
                numeric_list(Arc::new(Float64Array::from(self.origins.clone())))
            } else {
                ScalarValue::List(Arc::new(ListArray::new_null(
                    Arc::new(Field::new("item", DataType::Float64, true)),
                    1,
                )))
            },
            ScalarValue::Boolean(Some(self.invalid_numeric)),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let [n, qr, origins, invalid] = states else {
            return Err(DataFusionError::Execution(
                "linreg state shape mismatch".into(),
            ));
        };
        let counts = n
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg count state must be int64".into()))?;
        let factors = qr
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("linreg QR state must be list".into()))?;
        let values = factors
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg QR values must be float64".into()))?;
        let origins = origins
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("linreg origin state must be list".into()))?;
        let origin_values = origins
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg origins must be float64".into()))?;
        let invalid = invalid
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("linreg invalid-input state must be bool".into())
            })?;
        if factors.len() != counts.len()
            || invalid.len() != counts.len()
            || origins.len() != counts.len()
        {
            return Err(DataFusionError::Execution(
                "linreg state row counts differ".into(),
            ));
        }
        let d = self.p + 1;
        for row in 0..counts.len() {
            if counts.is_null(row) {
                continue;
            }
            if factors.is_null(row)
                || invalid.is_null(row)
                || factors.value_length(row) as usize != d * d
            {
                return Err(DataFusionError::Execution(
                    "linreg QR state dimensions differ".into(),
                ));
            }
            let start = factors.value_offsets()[row] as usize;
            if (start..start + d * d).any(|i| values.is_null(i)) {
                return Err(DataFusionError::Execution(
                    "linreg QR state must be defined".into(),
                ));
            }
            let partial_origins = if origins.is_null(row) {
                None
            } else {
                let start = origins.value_offsets()[row] as usize;
                if origins.value_length(row) as usize != d
                    || (start..start + d)
                        .any(|i| origin_values.is_null(i) || !origin_values.value(i).is_finite())
                {
                    return Err(DataFusionError::Execution(
                        "linreg origin state dimensions/values differ".into(),
                    ));
                }
                Some(&origin_values.values()[start..start + d])
            };
            if !self.origin_set {
                if let Some(origins) = partial_origins {
                    self.origins.copy_from_slice(origins);
                    self.origin_set = true;
                }
            }
            if counts.value(row) < 0 {
                return Err(DataFusionError::Execution(
                    "linreg count state must be nonnegative".into(),
                ));
            }
            self.n += counts.value(row);
            self.invalid_numeric |= invalid.value(row);
            for i in 0..d {
                self.work.fill(0.);
                self.work[i..d]
                    .copy_from_slice(&values.values()[start + i * d + i..start + (i + 1) * d]);
                if self.work[0] != 0. {
                    for j in 1..d {
                        let delta =
                            partial_origins.map_or(0., |origins| origins[j]) - self.origins[j];
                        self.work[j] = delta.mul_add(self.work[0], self.work[j]);
                    }
                }
                if self.work.iter().any(|value| !value.is_finite()) {
                    self.invalid_numeric = true;
                    continue;
                }
                self.append_work();
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct LinregGroupsAccumulator {
    p: usize,
    nested: usize,
    weighted: bool,
    n: Vec<i64>,
    // One contiguous upper-triangular QR state per group, with shared scratch.
    qr: Vec<f64>,
    origins: Vec<f64>,
    origin_set: Vec<bool>,
    invalid_numeric: Vec<bool>,
    work: Vec<f64>,
    predictor_scratch: Vec<f64>,
}

fn take_linreg_groups<T>(values: &mut Vec<T>, count: usize, width: usize, all: bool) -> Vec<T> {
    if all {
        std::mem::take(values)
    } else {
        values.drain(0..count * width).collect()
    }
}

impl LinregGroupsAccumulator {
    fn new(p: usize, nested: usize, weighted: bool) -> Self {
        Self {
            p,
            nested,
            weighted,
            n: Vec::new(),
            qr: Vec::new(),
            origins: Vec::new(),
            origin_set: Vec::new(),
            invalid_numeric: Vec::new(),
            work: vec![0.; p + 1],
            predictor_scratch: vec![0.; p],
        }
    }

    fn dimension(&self) -> usize {
        self.p + 1
    }

    fn qr_width(&self) -> usize {
        self.dimension() * self.dimension()
    }

    fn ensure_groups(&mut self, groups: usize) -> DfResult<()> {
        let qr_len = groups
            .checked_mul(self.qr_width())
            .ok_or_else(|| DataFusionError::Execution("linreg state size overflow".into()))?;
        let origins_len = groups
            .checked_mul(self.dimension())
            .ok_or_else(|| DataFusionError::Execution("linreg state size overflow".into()))?;
        if qr_len > i32::MAX as usize || origins_len > i32::MAX as usize {
            return Err(DataFusionError::Execution(
                "linreg grouped state exceeds Arrow's offset limit".into(),
            ));
        }
        if groups > self.n.len() {
            self.n.resize(groups, 0);
            self.qr.resize(qr_len, 0.);
            self.origins.resize(origins_len, 0.);
            self.origin_set.resize(groups, false);
            self.invalid_numeric.resize(groups, false);
        }
        Ok(())
    }

    fn take_state(
        &mut self,
        emit_to: EmitTo,
    ) -> DfResult<(Vec<i64>, Vec<f64>, Vec<f64>, Vec<bool>, Vec<bool>)> {
        let groups = self.n.len();
        let emitted = match emit_to {
            EmitTo::All => groups,
            EmitTo::First(n) if n <= groups => n,
            EmitTo::First(_) => {
                return Err(DataFusionError::Execution(
                    "linreg emitted group count exceeds accumulator state".into(),
                ));
            }
        };
        let all = matches!(emit_to, EmitTo::All);
        let qr_width = self.qr_width();
        let dimension = self.dimension();
        Ok((
            take_linreg_groups(&mut self.n, emitted, 1, all),
            take_linreg_groups(&mut self.qr, emitted, qr_width, all),
            take_linreg_groups(&mut self.origins, emitted, dimension, all),
            take_linreg_groups(&mut self.origin_set, emitted, 1, all),
            take_linreg_groups(&mut self.invalid_numeric, emitted, 1, all),
        ))
    }
}

fn linreg_state_list(
    values: Vec<f64>,
    width: usize,
    validity: Option<Vec<bool>>,
) -> DfResult<ArrayRef> {
    if width == 0 || values.len() % width != 0 || values.len() > i32::MAX as usize {
        return Err(DataFusionError::Execution(
            "linreg list state has invalid width or Arrow offsets".into(),
        ));
    }
    let groups = values.len() / width;
    let nulls = validity
        .filter(|valid| valid.iter().any(|valid| !valid))
        .map(NullBuffer::from);
    Ok(Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        OffsetBuffer::from_lengths(vec![width; groups]),
        Arc::new(Float64Array::from(values)),
        nulls,
    )))
}

impl GroupsAccumulator for LinregGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DfResult<()> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "linreg expects one struct input".into(),
            ));
        };
        if input.len() != group_indices.len()
            || opt_filter.is_some_and(|filter| filter.len() != input.len())
            || group_indices.iter().any(|group| *group >= total_num_groups)
        {
            return Err(DataFusionError::Execution(
                "linreg grouping shape mismatch".into(),
            ));
        }
        self.ensure_groups(total_num_groups)?;
        let rows = input
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("linreg expects struct values".into()))?;
        let columns = rows.columns();
        if columns.len() != if self.weighted { 3 } else { 2 } {
            return Err(DataFusionError::Execution(
                "linreg input struct fields differ from its configured weight mode".into(),
            ));
        }
        let ys = columns[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg y must be float64".into()))?;
        let predictors = columns[1].as_ref();
        let lists = predictors.as_any().downcast_ref::<ListArray>();
        let scalar = predictors.as_any().downcast_ref::<Float64Array>();
        let predictor_columns = predictors
            .as_any()
            .downcast_ref::<StructArray>()
            .map(|xs| {
                if xs.columns().len() != self.p {
                    return Err(DataFusionError::Execution(format!(
                        "linreg predictor struct has {} fields, expected {}",
                        xs.columns().len(),
                        self.p
                    )));
                }
                xs.columns()
                    .iter()
                    .map(|column| {
                        column
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "linreg predictors must be float64".into(),
                                )
                            })
                    })
                    .collect::<DfResult<Vec<_>>>()
            })
            .transpose()?;
        let children = lists
            .map(|list| {
                list.values()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("linreg predictors must be float64".into())
                    })
            })
            .transpose()?;
        let weights = if self.weighted {
            Some(
                columns[2]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("linreg weight must be float64".into())
                    })?,
            )
        } else {
            None
        };
        for (row, group) in group_indices.iter().copied().enumerate() {
            if opt_filter.is_some_and(|filter| !filter.is_valid(row) || !filter.value(row))
                || rows.is_null(row)
                || ys.is_null(row)
                || predictors.is_null(row)
                || weights.is_some_and(|weight| weight.is_null(row))
            {
                continue;
            }
            let x: &[f64] = if let Some(xs) = &predictor_columns {
                if xs.iter().any(|column| column.is_null(row)) {
                    continue;
                }
                for (slot, column) in self.predictor_scratch.iter_mut().zip(xs) {
                    *slot = column.value(row);
                }
                &self.predictor_scratch
            } else if let (Some(list), Some(values)) = (lists, children) {
                let offsets = list.value_offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                if end - start != self.p || (start..end).any(|index| values.is_null(index)) {
                    continue;
                }
                &values.values()[start..end]
            } else if let Some(values) = scalar {
                if self.p != 1 {
                    return Err(DataFusionError::Execution(
                        "linreg scalar predictor requires exactly one predictor".into(),
                    ));
                }
                std::slice::from_ref(&values.values()[row])
            } else {
                return Err(DataFusionError::Execution(
                    "linreg x has unsupported predictor representation".into(),
                ));
            };
            let qr_width = self.qr_width();
            let origins_width = self.dimension();
            let qr_start = group * qr_width;
            let origins_start = group * origins_width;
            accumulate_linreg_observation(
                self.p,
                &mut self.n[group],
                &mut self.qr[qr_start..qr_start + qr_width],
                &mut self.work,
                &mut self.origins[origins_start..origins_start + origins_width],
                &mut self.origin_set[group],
                &mut self.invalid_numeric[group],
                x,
                ys.value(row),
                weights.map_or(1.0, |w| w.value(row)),
            );
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DfResult<ArrayRef> {
        let (counts, qr, origins, _origin_set, invalid) = self.take_state(emit_to)?;
        let groups = counts.len();
        if groups == 0 {
            return Ok(new_empty_array(&linreg_type()));
        }
        let d = self.dimension();
        let qr_width = self.qr_width();
        let mut results = Vec::with_capacity(groups);
        for group in 0..groups {
            let origin_start = group * d;
            let qr_start = group * qr_width;
            results.push(linreg_result(
                self.p,
                self.nested,
                counts[group],
                &qr[qr_start..qr_start + qr_width],
                &origins[origin_start..origin_start + d],
                invalid[group],
            )?);
        }
        ScalarValue::iter_to_array(results)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.n.capacity() * std::mem::size_of::<i64>()
            + (self.qr.capacity() + self.origins.capacity() + self.work.capacity())
                * std::mem::size_of::<f64>()
            + (self.origin_set.capacity() + self.invalid_numeric.capacity())
                * std::mem::size_of::<bool>()
            + self.predictor_scratch.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self, emit_to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        let (counts, qr, origins, origin_set, invalid) = self.take_state(emit_to)?;
        let qr = linreg_state_list(qr, self.qr_width(), None)?;
        let origins = linreg_state_list(origins, self.dimension(), Some(origin_set))?;
        Ok(vec![
            Arc::new(Int64Array::from(counts)),
            qr,
            origins,
            Arc::new(BooleanArray::from(invalid)),
        ])
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DfResult<()> {
        let [counts, qr, origins, invalid] = states else {
            return Err(DataFusionError::Execution(
                "linreg grouped state shape mismatch".into(),
            ));
        };
        let counts = counts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg count state must be int64".into()))?;
        let qr = qr
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("linreg QR state must be list".into()))?;
        let qr_values = qr
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg QR values must be float64".into()))?;
        let origins = origins
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("linreg origin state must be list".into()))?;
        let origin_values = origins
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("linreg origins must be float64".into()))?;
        let invalid = invalid
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("linreg invalid state must be bool".into())
            })?;
        if counts.len() != group_indices.len()
            || qr.len() != group_indices.len()
            || origins.len() != group_indices.len()
            || invalid.len() != group_indices.len()
            || group_indices.iter().any(|group| *group >= total_num_groups)
        {
            return Err(DataFusionError::Execution(
                "linreg grouped state row count mismatch".into(),
            ));
        }
        self.ensure_groups(total_num_groups)?;
        let d = self.dimension();
        let qr_width = self.qr_width();
        for (row, group) in group_indices.iter().copied().enumerate() {
            if counts.is_null(row) {
                continue;
            }
            if qr.is_null(row) || invalid.is_null(row) || qr.value_length(row) as usize != qr_width
            {
                return Err(DataFusionError::Execution(
                    "linreg grouped QR state dimensions differ".into(),
                ));
            }
            let qr_start = qr.value_offsets()[row] as usize;
            if (qr_start..qr_start + qr_width).any(|index| qr_values.is_null(index)) {
                return Err(DataFusionError::Execution(
                    "linreg grouped QR state must be defined".into(),
                ));
            }
            let partial_origins = if origins.is_null(row) {
                None
            } else {
                if origins.value_length(row) as usize != d {
                    return Err(DataFusionError::Execution(
                        "linreg grouped origin state dimensions differ".into(),
                    ));
                }
                let start = origins.value_offsets()[row] as usize;
                if (start..start + d).any(|index| {
                    origin_values.is_null(index) || !origin_values.value(index).is_finite()
                }) {
                    return Err(DataFusionError::Execution(
                        "linreg grouped origin state values differ".into(),
                    ));
                }
                Some(&origin_values.values()[start..start + d])
            };
            if counts.value(row) < 0 {
                return Err(DataFusionError::Execution(
                    "linreg grouped count state must be nonnegative".into(),
                ));
            }
            let origins_start = group * d;
            if !self.origin_set[group] {
                if let Some(partial) = partial_origins {
                    self.origins[origins_start..origins_start + d].copy_from_slice(partial);
                    self.origin_set[group] = true;
                }
            }
            self.n[group] += counts.value(row);
            self.invalid_numeric[group] |= invalid.value(row);
            let qr_start_out = group * qr_width;
            for i in 0..d {
                self.work.fill(0.);
                self.work[i..d].copy_from_slice(
                    &qr_values.values()[qr_start + i * d + i..qr_start + (i + 1) * d],
                );
                if self.work[0] != 0. {
                    for j in 1..d {
                        let delta = partial_origins.map_or(0., |origin| origin[j])
                            - self.origins[origins_start + j];
                        self.work[j] = delta.mul_add(self.work[0], self.work[j]);
                    }
                }
                if self.work.iter().any(|value| !value.is_finite()) {
                    self.invalid_numeric[group] = true;
                    continue;
                }
                append_linreg_work(
                    self.p,
                    &mut self.qr[qr_start_out..qr_start_out + qr_width],
                    &mut self.work,
                    &mut self.invalid_numeric[group],
                );
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> DfResult<Vec<ArrayRef>> {
        let [input] = values else {
            return Err(DataFusionError::Execution(
                "linreg expects one struct input".into(),
            ));
        };
        if opt_filter.is_some_and(|filter| filter.len() != input.len()) {
            return Err(DataFusionError::Execution(
                "linreg converter filter length mismatch".into(),
            ));
        }
        let mut groups = Self::new(self.p, self.nested, self.weighted);
        let group_indices = (0..input.len()).collect::<Vec<_>>();
        groups.update_batch(values, &group_indices, opt_filter, input.len())?;
        groups.state(EmitTo::All)
    }
}
