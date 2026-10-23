//! Dense population variance state. This is the same Welford update/parallel
//! merge recurrence used by DataFusion 55's VarianceAccumulator; its helpers
//! are private, and its public grouped adapter requires per-entry group IDs.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, UInt64Array,
};
use datafusion_common::{DataFusionError, Result};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct DenseStddev {
    count: Vec<u64>,
    mean: Vec<f64>,
    m2: Vec<f64>,
    nonfinite: Vec<u64>,
}
impl DenseStddev {
    fn resize(&mut self, width: usize) {
        self.count.resize(width, 0);
        self.mean.resize(width, 0.);
        self.m2.resize(width, 0.);
        self.nonfinite.resize(width, 0);
    }
    fn add(&mut self, column: usize, value: f64) -> Result<()> {
        if !value.is_finite() {
            self.nonfinite[column] = self.nonfinite[column].checked_add(1).ok_or_else(|| {
                DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
            })?;
            return Ok(());
        }
        let count = self.count[column] + 1;
        let delta1 = value - self.mean[column];
        let mean = delta1 / count as f64 + self.mean[column];
        let delta2 = value - mean;
        self.count[column] = count;
        self.mean[column] = mean;
        self.m2[column] += delta1 * delta2;
        Ok(())
    }
    pub(crate) fn update(
        &mut self,
        input: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        self.resize(width);
        macro_rules! update {
            ($array:ty) => {{
                let values = input.as_any().downcast_ref::<$array>().unwrap();
                for (row, chunk) in values.values().chunks_exact(width).enumerate() {
                    for (column, value) in chunk.iter().copied().enumerate() {
                        let index = row * width + column;
                        if values.is_valid(index)
                            && filter.is_none_or(|f| f.is_valid(index) && f.value(index))
                        {
                            self.add(column, value as f64)?;
                        }
                    }
                }
            }};
        }
        match input.data_type() {
            arrow_schema::DataType::Int32 => update!(Int32Array),
            arrow_schema::DataType::Int64 => update!(Int64Array),
            arrow_schema::DataType::Float32 => update!(Float32Array),
            arrow_schema::DataType::Float64 => update!(Float64Array),
            _ => {
                return Err(DataFusionError::Execution(
                    "coordinate stddev requires numeric input".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn states(&self) -> Vec<ArrayRef> {
        vec![
            Arc::new(UInt64Array::from(self.count.clone())),
            Arc::new(Float64Array::from(self.mean.clone())),
            Arc::new(Float64Array::from(self.m2.clone())),
            Arc::new(UInt64Array::from(self.nonfinite.clone())),
        ]
    }
    pub(crate) fn merge(&mut self, states: &[ArrayRef], width: usize) -> Result<()> {
        if states.len() != 4 {
            return Err(DataFusionError::Execution(
                "coordinate stddev requires four partial states".into(),
            ));
        }
        let count = states[0].as_any().downcast_ref::<UInt64Array>();
        let mean = states[1].as_any().downcast_ref::<Float64Array>();
        let m2 = states[2].as_any().downcast_ref::<Float64Array>();
        let nonfinite = states[3].as_any().downcast_ref::<UInt64Array>();
        let (Some(count), Some(mean), Some(m2), Some(nonfinite)) = (count, mean, m2, nonfinite)
        else {
            return Err(DataFusionError::Execution(
                "coordinate stddev partial state type mismatch".into(),
            ));
        };
        self.resize(width);
        for column in 0..width {
            self.nonfinite[column] = self.nonfinite[column]
                .checked_add(nonfinite.value(column))
                .ok_or_else(|| {
                    DataFusionError::Execution("stddev_pop nonfinite count exceeds uint64".into())
                })?;
            let count2 = count.value(column);
            if count2 == 0 {
                continue;
            }
            let count1 = self.count[column];
            let total = count1 + count2;
            let mean1 = self.mean[column];
            let mean2 = mean.value(column);
            let delta = mean1 - mean2;
            self.mean[column] =
                mean1 * count1 as f64 / total as f64 + mean2 * count2 as f64 / total as f64;
            self.m2[column] +=
                m2.value(column) + delta * delta * count1 as f64 * count2 as f64 / total as f64;
            self.count[column] = total;
        }
        Ok(())
    }
    pub(crate) fn array(&self) -> ArrayRef {
        Arc::new(Float64Array::from_iter_values((0..self.count.len()).map(
            |column| {
                if self.nonfinite[column] != 0 || self.count[column] == 0 {
                    f64::NAN
                } else if self.count[column] == 1 {
                    0.
                } else {
                    (self.m2[column] / self.count[column] as f64).sqrt()
                }
            },
        )))
    }
    pub(crate) fn size(&self) -> usize {
        (self.count.capacity() + self.nonfinite.capacity()) * 8
            + (self.mean.capacity() + self.m2.capacity()) * 8
    }
}
