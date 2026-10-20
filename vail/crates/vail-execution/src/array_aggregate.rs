//! Lift a DataFusion aggregate over array coordinates without unnesting rows.
//! Arithmetic, null handling, partial states and merges remain owned by the
//! underlying aggregate. A native GroupsAccumulator processes all coordinates
//! in one batch; other UDAFs receive one Arrow batch per coordinate.
use std::sync::{Arc, Mutex, Weak};

use arrow_array::{Array, ArrayRef, Int64Array, ListArray, UInt32Array, new_empty_array};
use arrow_buffer::OffsetBuffer;
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::{concat::concat, take::take};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, EmitTo, GroupsAccumulator};
use datafusion_physical_expr::{
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
    expressions::Column,
};

fn list_type(dtype: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(dtype, true)))
}
fn list_scalar(values: ArrayRef) -> ScalarValue {
    ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::from_lengths([values.len()]),
        values,
        None,
    )))
}
fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Debug)]
struct CoordinateIndices {
    width: usize,
    values: Vec<usize>,
}

fn coordinate_indices(width: usize, length: usize) -> Result<Arc<CoordinateIndices>> {
    if width == 0 {
        return Err(error("coordinate indices require a nonzero width"));
    }
    // Concurrent partial aggregates usually have the same shape. Reuse their
    // immutable coordinate pattern, without retaining it after those users
    // finish or accumulating a cache entry for every encountered shape.
    static LAST: Mutex<Weak<CoordinateIndices>> = Mutex::new(Weak::new());
    let mut last = LAST
        .lock()
        .map_err(|_| error("coordinate indices cache poisoned"))?;
    if let Some(indices) = last.upgrade() {
        if indices.width == width && indices.values.len() >= length {
            return Ok(indices);
        }
    }
    let mut values = Vec::with_capacity(length);
    values.extend(0..width.min(length));
    while values.len() < length {
        let copied = values.len().min(length - values.len());
        values.extend_from_within(..copied);
    }
    let indices = Arc::new(CoordinateIndices { width, values });
    *last = Arc::downgrade(&indices);
    Ok(indices)
}

/// The adapter accepts any unary UDAF with ordinary mergeable Arrow state.
/// `empty` supplies the caller's language-level identity, e.g. Hail sum's zero.
pub fn coordinate_udaf(
    inner: Arc<AggregateUDF>,
    input: DataType,
    empty: ScalarValue,
) -> Result<Arc<AggregateUDF>> {
    // Keep the expression's actual input type at this boundary. SUM owns its
    // widening in a typed adapter, so annotations and callers cannot narrow
    // or change its arithmetic contract.
    let aggregate_input = input.clone();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "element",
        aggregate_input.clone(),
        true,
    )]));
    let aggregate = Arc::new(
        AggregateExprBuilder::new(inner, vec![Arc::new(Column::new("element", 0))])
            .schema(schema)
            .alias("coordinate")
            .build()?,
    );
    let output = aggregate.field().data_type().clone();
    if empty.data_type() != output {
        return Err(error(
            "coordinate empty identity type must match aggregate output",
        ));
    }
    let state_types = aggregate
        .state_fields()?
        .iter()
        .map(|f| list_type(f.data_type().clone()))
        .chain(std::iter::once(DataType::Int64))
        .collect::<Vec<_>>();
    let name = format!("vail_coordinate_{}", aggregate.fun().name());
    let identity = (
        aggregate.fun().clone(),
        input.clone(),
        aggregate_input.clone(),
        empty.clone(),
    );
    Ok(crate::kernel_identity::aggregate_named(
        &name,
        identity,
        vec![list_type(input.clone())],
        list_type(output),
        state_types,
        Arc::new(move |_| {
            Ok(Box::new(Coordinates::new(
                aggregate.clone(),
                empty.clone(),
                input.clone(),
                aggregate_input.clone(),
            )?))
        }),
    ))
}

enum Kernels {
    Groups(std::sync::Mutex<Box<dyn GroupsAccumulator>>),
    Coordinates(Vec<Box<dyn Accumulator>>),
}
impl std::fmt::Debug for Kernels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Groups(_) => f.write_str("DataFusionGroupsAccumulator"),
            Self::Coordinates(kernels) => f
                .debug_tuple("CoordinateAccumulators")
                .field(kernels)
                .finish(),
        }
    }
}
#[derive(Debug)]
struct Coordinates {
    aggregate: Arc<AggregateFunctionExpr>,
    kernels: Kernels,
    width: Option<usize>,
    empty: ScalarValue,
    input: DataType,
    aggregate_input: DataType,
    group_indices: Option<Arc<CoordinateIndices>>,
}
impl Coordinates {
    fn new(
        aggregate: Arc<AggregateFunctionExpr>,
        empty: ScalarValue,
        input: DataType,
        aggregate_input: DataType,
    ) -> Result<Self> {
        let kernels = if aggregate.groups_accumulator_supported() {
            Kernels::Groups(std::sync::Mutex::new(
                aggregate.create_groups_accumulator()?,
            ))
        } else {
            Kernels::Coordinates(Vec::new())
        };
        Ok(Self {
            aggregate,
            kernels,
            width: None,
            empty,
            input,
            aggregate_input,
            group_indices: None,
        })
    }
    fn set_width(&mut self, width: usize) -> Result<()> {
        if let Some(old) = self.width {
            if old != width {
                return Err(error(&format!(
                    "array_agg requires equal array lengths, found {old} and {width}"
                )));
            }
        } else {
            self.width = Some(width);
            if let Kernels::Coordinates(kernels) = &mut self.kernels {
                *kernels = (0..width)
                    .map(|_| self.aggregate.create_accumulator())
                    .collect::<Result<_>>()?;
            }
        }
        Ok(())
    }
    fn states(&mut self) -> Result<Vec<ArrayRef>> {
        let width = self.width.unwrap_or(0);
        match &mut self.kernels {
            Kernels::Groups(kernel) => {
                let kernel = kernel
                    .get_mut()
                    .map_err(|_| error("coordinate accumulator state poisoned"))?;
                let state = kernel.state(EmitTo::All)?;
                // GroupsAccumulator emits state destructively, whereas the
                // scalar Accumulator interface requires a repeatable snapshot.
                if width > 0 {
                    kernel.merge_batch(&state, &(0..width).collect::<Vec<_>>(), width)?;
                }
                Ok(state)
            }
            Kernels::Coordinates(kernels) => {
                let states = kernels
                    .iter_mut()
                    .map(|k| k.state())
                    .collect::<Result<Vec<_>>>()?;
                self.aggregate
                    .state_fields()?
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        if states.is_empty() {
                            Ok(new_empty_array(f.data_type()))
                        } else {
                            ScalarValue::iter_to_array(states.iter().map(|s| s[i].clone()))
                        }
                    })
                    .collect()
            }
        }
    }
}
impl Accumulator for Coordinates {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arrays = values
            .first()
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| error("coordinate aggregate requires a list"))?;
        let mut chunks = Vec::new();
        for row in 0..arrays.len() {
            if arrays.is_valid(row) {
                self.set_width(arrays.value_length(row) as usize)?;
            }
        }
        let Some(width) = self.width else {
            return Ok(());
        };
        if width == 0 {
            return Ok(());
        }
        // For ordinary non-null arrays the child values are already contiguous
        // Arrow storage. Missing lists may hide arbitrary children: omit them.
        let flat = if arrays.null_count() == 0 {
            let offsets = arrays.value_offsets();
            arrays.values().slice(
                offsets[0] as usize,
                (offsets[arrays.len()] - offsets[0]) as usize,
            )
        } else {
            for row in 0..arrays.len() {
                if arrays.is_valid(row) {
                    chunks.push(arrays.value(row));
                }
            }
            if chunks.is_empty() {
                return Ok(());
            }
            concat(&chunks.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
        };
        let flat = if self.input == self.aggregate_input {
            flat
        } else {
            cast(flat.as_ref(), &self.aggregate_input)?
        };
        if matches!(self.kernels, Kernels::Groups(_))
            && self
                .group_indices
                .as_ref()
                .is_none_or(|indices| indices.values.len() < flat.len())
        {
            self.group_indices = Some(coordinate_indices(width, flat.len())?);
        }
        match &mut self.kernels {
            Kernels::Groups(kernel) => {
                let kernel = kernel
                    .get_mut()
                    .map_err(|_| error("coordinate accumulator state poisoned"))?;
                let groups = &self.group_indices.as_ref().unwrap().values[..flat.len()];
                kernel.update_batch(&[flat], groups, None, width)
            }
            Kernels::Coordinates(kernels) => {
                for (coordinate, kernel) in kernels.iter_mut().enumerate() {
                    let indices = UInt32Array::from_iter_values(
                        (coordinate..flat.len()).step_by(width).map(|i| i as u32),
                    );
                    kernel.update_batch(&[take(flat.as_ref(), &indices, None)?])?;
                }
                Ok(())
            }
        }
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(width) = self.width else {
            return ScalarValue::try_from(&list_type(self.aggregate.field().data_type().clone()));
        };
        if width == 0 {
            return Ok(list_scalar(new_empty_array(
                self.aggregate.field().data_type(),
            )));
        }
        let values = match &mut self.kernels {
            Kernels::Groups(_) => {
                let states = self.states()?;
                let mut copy = self.aggregate.create_groups_accumulator()?;
                copy.merge_batch(&states, &(0..width).collect::<Vec<_>>(), width)?;
                copy.evaluate(EmitTo::All)?
            }
            Kernels::Coordinates(kernels) => ScalarValue::iter_to_array(
                kernels
                    .iter_mut()
                    .map(|k| k.evaluate())
                    .collect::<Result<Vec<_>>>()?,
            )?,
        };
        // Only language-level empty identities are added; reductions themselves
        // use the underlying library's vectorized implementation.
        let values = if values.null_count() > 0 && !self.empty.is_null() {
            let mask = arrow_arith::boolean::is_null(values.as_ref())?;
            let fill = self.empty.to_scalar()?;
            arrow_select::zip::zip(&mask, &fill, &values)?
        } else {
            values
        };
        Ok(list_scalar(values))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.group_indices.as_ref().map_or(0, |indices| {
                // Conservatively charge shared storage to each user.
                indices.values.capacity() * std::mem::size_of::<usize>()
            })
            + match &self.kernels {
                Kernels::Groups(k) => k.lock().map(|k| k.size()).unwrap_or_default(),
                Kernels::Coordinates(ks) => ks.iter().map(|k| k.size()).sum(),
            }
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut states = self
            .states()?
            .into_iter()
            .map(list_scalar)
            .collect::<Vec<_>>();
        states.push(ScalarValue::Int64(self.width.map(|n| n as i64)));
        Ok(states)
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let widths = states
            .last()
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| error("coordinate aggregate has invalid width state"))?;
        let lists = states[..states.len() - 1]
            .iter()
            .map(|a| {
                a.as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| error("coordinate aggregate has invalid partial state"))
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..widths.len() {
            if widths.is_null(row) {
                continue;
            }
            let width =
                usize::try_from(widths.value(row)).map_err(|_| error("invalid array width"))?;
            self.set_width(width)?;
            if width == 0 {
                continue;
            }
            let values = lists.iter().map(|a| a.value(row)).collect::<Vec<_>>();
            if values.iter().any(|a| a.len() != width) {
                return Err(error("coordinate state length mismatch"));
            }
            match &mut self.kernels {
                Kernels::Groups(kernel) => {
                    let kernel = kernel
                        .get_mut()
                        .map_err(|_| error("coordinate accumulator state poisoned"))?;
                    kernel.merge_batch(&values, &(0..width).collect::<Vec<_>>(), width)?
                }
                Kernels::Coordinates(kernels) => {
                    for (i, kernel) in kernels.iter_mut().enumerate() {
                        kernel.merge_batch(
                            &values.iter().map(|a| a.slice(i, 1)).collect::<Vec<_>>(),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}
