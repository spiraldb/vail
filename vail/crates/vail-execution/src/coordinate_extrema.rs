//! Primitive coordinate extrema without grouping IDs or per-coordinate gathers.
use arrow_array::ArrowNativeTypeOp;
use arrow_array::{Array, ArrayRef, BooleanArray, PrimitiveArray, types::*};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::AggregateUDF;
use std::sync::Arc;

pub(crate) fn native_minimum(udf: &AggregateUDF) -> Option<bool> {
    if udf
        .inner()
        .downcast_ref::<datafusion_functions_aggregate::min_max::Min>()
        .is_some()
    {
        Some(true)
    } else if udf
        .inner()
        .downcast_ref::<datafusion_functions_aggregate::min_max::Max>()
        .is_some()
    {
        Some(false)
    } else {
        None
    }
}

#[derive(Debug)]
pub(crate) struct PrimitiveState<T: ArrowPrimitiveType> {
    values: Vec<T::Native>,
    present: Vec<bool>,
}
impl<T: ArrowPrimitiveType> Default for PrimitiveState<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            present: Vec::new(),
        }
    }
}
impl<T: ArrowPrimitiveType> PrimitiveState<T> {
    fn update(
        &mut self,
        input: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
        minimum: bool,
    ) -> Result<()> {
        let input = input
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                DataFusionError::Execution("coordinate extrema input type mismatch".into())
            })?;
        if self.values.is_empty() {
            self.values.resize(width, T::default_value());
            self.present.resize(width, false);
        }
        // Coordinates validates equal-width lists, and partial-state merge
        // validates each vector width. No remainder or hidden list slots exist.
        for (row, values) in input.values().chunks_exact(width).enumerate() {
            let offset = row * width;
            for (column, value) in values.iter().copied().enumerate() {
                let index = offset + column;
                if input.is_null(index)
                    || filter.is_some_and(|f| f.is_null(index) || !f.value(index))
                {
                    continue;
                }
                let order = value.compare(self.values[column]);
                if !self.present[column]
                    || if minimum {
                        order.is_lt()
                    } else {
                        order.is_gt()
                    }
                {
                    self.values[column] = value;
                }
                self.present[column] = true;
            }
        }
        Ok(())
    }
    fn array(&self) -> ArrayRef {
        Arc::new(
            self.values
                .iter()
                .copied()
                .zip(&self.present)
                .map(|(value, present)| present.then_some(value))
                .collect::<PrimitiveArray<T>>(),
        )
    }
    fn size(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<T::Native>() + self.present.capacity()
    }
}

#[derive(Debug)]
pub(crate) enum DenseExtrema {
    Int32(PrimitiveState<Int32Type>, bool),
    Int64(PrimitiveState<Int64Type>, bool),
    Float32(PrimitiveState<Float32Type>, bool),
    Float64(PrimitiveState<Float64Type>, bool),
}
impl DenseExtrema {
    pub(crate) fn new(dtype: &DataType, minimum: bool) -> Option<Self> {
        Some(match dtype {
            DataType::Int32 => Self::Int32(Default::default(), minimum),
            DataType::Int64 => Self::Int64(Default::default(), minimum),
            DataType::Float32 => Self::Float32(Default::default(), minimum),
            DataType::Float64 => Self::Float64(Default::default(), minimum),
            _ => return None,
        })
    }
    pub(crate) fn update(
        &mut self,
        input: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        match self {
            Self::Int32(s, m) => s.update(input, filter, width, *m),
            Self::Int64(s, m) => s.update(input, filter, width, *m),
            Self::Float32(s, m) => s.update(input, filter, width, *m),
            Self::Float64(s, m) => s.update(input, filter, width, *m),
        }
    }
    pub(crate) fn array(&self) -> ArrayRef {
        match self {
            Self::Int32(s, _) => s.array(),
            Self::Int64(s, _) => s.array(),
            Self::Float32(s, _) => s.array(),
            Self::Float64(s, _) => s.array(),
        }
    }
    pub(crate) fn size(&self) -> usize {
        match self {
            Self::Int32(s, _) => s.size(),
            Self::Int64(s, _) => s.size(),
            Self::Float32(s, _) => s.size(),
            Self::Float64(s, _) => s.size(),
        }
    }
}
