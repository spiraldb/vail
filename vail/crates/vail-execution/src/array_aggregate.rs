//! Lift a DataFusion aggregate over array coordinates without unnesting rows.
//! Arithmetic, null handling, partial states and merges remain owned by the
//! underlying aggregate. A native GroupsAccumulator processes all coordinates
//! in one batch; other UDAFs receive one Arrow batch per coordinate.
use std::sync::{Arc, Mutex, Weak};

use arrow_array::{
    Array, ArrayRef, Int64Array, ListArray, StructArray, UInt32Array, new_empty_array,
};
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

/// One reducer carried by a packed coordinate callback. `value_index` and
/// `filter_index` address fields in the callback's per-coordinate struct.
#[derive(Clone)]
pub struct CoordinateReducer {
    pub name: String,
    pub aggregate: Arc<AggregateUDF>,
    pub input: DataType,
    pub value_index: usize,
    pub filter_index: Option<usize>,
    pub empty: ScalarValue,
    /// Stable description of options not otherwise visible in the UDAF name.
    pub identity: String,
}

/// Lift several reducers over the same packed array callback without first
/// unnesting its positions. The callback is mapped once to `List<Struct>`;
/// every reducer shares its implicit coordinate ids and width validation.
pub fn coordinate_bundle_udaf(
    packed: DataType,
    reducers: Vec<CoordinateReducer>,
) -> Result<Arc<AggregateUDF>> {
    let DataType::Struct(fields) = &packed else {
        return Err(error("coordinate bundle requires a struct element"));
    };
    if reducers.is_empty() {
        return Err(error("coordinate bundle requires reducers"));
    }
    let mut output_fields = Vec::with_capacity(reducers.len());
    let mut state_types = Vec::new();
    let mut physical = Vec::with_capacity(reducers.len());
    for reducer in &reducers {
        let value_field = fields
            .get(reducer.value_index)
            .ok_or_else(|| error("coordinate bundle value slot is out of bounds"))?;
        if value_field.data_type() != &reducer.input {
            return Err(error("coordinate bundle value slot type mismatch"));
        }
        if let Some(index) = reducer.filter_index {
            if fields.get(index).map(|f| f.data_type()) != Some(&DataType::Boolean) {
                return Err(error("coordinate bundle filter slot must be Boolean"));
            }
        }
        if reducer.empty.data_type() != reducer.aggregate.return_type(&[reducer.input.clone()])? {
            return Err(error("coordinate bundle empty identity type mismatch"));
        }
        let schema = Arc::new(Schema::new(vec![Field::new(
            "element",
            reducer.input.clone(),
            true,
        )]));
        let expression = Arc::new(
            AggregateExprBuilder::new(
                reducer.aggregate.clone(),
                vec![Arc::new(Column::new("element", 0))],
            )
            .schema(schema)
            .alias(&reducer.name)
            .build()?,
        );
        output_fields.push(Arc::new(Field::new(
            &reducer.name,
            expression.field().data_type().clone(),
            true,
        )));
        state_types.extend(
            expression
                .state_fields()?
                .iter()
                .map(|f| list_type(f.data_type().clone())),
        );
        physical.push((expression, reducer.clone()));
    }
    state_types.push(DataType::Int64);
    let output_fields: arrow_schema::Fields = output_fields.into();
    let output = list_type(DataType::Struct(output_fields.clone()));
    let input = list_type(packed);
    let identity = reducers
        .iter()
        .map(|r| r.identity.clone())
        .collect::<Vec<_>>();
    Ok(crate::kernel_identity::aggregate_named(
        "vail_coordinate_bundle",
        (identity, input.clone(), output.clone(), state_types.clone()),
        vec![input],
        output,
        state_types,
        Arc::new(move |_| {
            Ok(Box::new(CoordinateBundle::new(
                physical.clone(),
                output_fields.clone(),
            )?))
        }),
    ))
}

#[derive(Debug)]
struct CoordinateBundle {
    reducers: Vec<(Coordinates, usize, Option<usize>)>,
    width: Option<usize>,
    packed_fields: usize,
    output_fields: arrow_schema::Fields,
}

impl CoordinateBundle {
    fn new(
        physical: Vec<(Arc<AggregateFunctionExpr>, CoordinateReducer)>,
        output_fields: arrow_schema::Fields,
    ) -> Result<Self> {
        let packed_fields = physical
            .iter()
            .map(|(_, r)| r.value_index.max(r.filter_index.unwrap_or(0)) + 1)
            .max()
            .unwrap_or(0);
        let reducers = physical
            .into_iter()
            .map(|(aggregate, reducer)| {
                Ok((
                    Coordinates::new(
                        aggregate,
                        reducer.empty,
                        reducer.input.clone(),
                        reducer.input.clone(),
                    )?,
                    reducer.value_index,
                    reducer.filter_index,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            reducers,
            width: None,
            packed_fields,
            output_fields,
        })
    }
}

impl Accumulator for CoordinateBundle {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arrays = values
            .first()
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| error("coordinate bundle requires list input"))?;
        let mut chunks = Vec::new();
        for row in 0..arrays.len() {
            if arrays.is_valid(row) {
                let width = arrays.value_length(row) as usize;
                if self.width.is_some_and(|old| old != width) {
                    return Err(error("array_agg requires equal array lengths"));
                }
                self.width = Some(width);
            }
        }
        let Some(width) = self.width else {
            return Ok(());
        };
        if width == 0 {
            return Ok(());
        }
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
        let packed = flat
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error("coordinate bundle elements must be structs"))?;
        if packed.num_columns() < self.packed_fields {
            return Err(error("coordinate bundle struct is missing slots"));
        }
        for (kernel, value_index, filter_index) in &mut self.reducers {
            let filter = filter_index.map(|i| {
                packed
                    .column(i)
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .expect("validated filter type")
            });
            kernel.set_width(width)?;
            kernel.update_flat(packed.column(*value_index).clone(), filter, width)?;
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(width) = self.width else {
            return ScalarValue::try_from(&list_type(DataType::Struct(self.output_fields.clone())));
        };
        let fields = self.output_fields.clone();
        if width == 0 {
            return Ok(list_scalar(Arc::new(StructArray::new(
                fields.clone(),
                fields
                    .iter()
                    .map(|f| new_empty_array(f.data_type()))
                    .collect(),
                None,
            ))));
        }
        let arrays = self
            .reducers
            .iter_mut()
            .map(|(k, _, _)| k.evaluate_values())
            .collect::<Result<Vec<_>>>()?;
        let structs = Arc::new(StructArray::new(fields, arrays, None));
        Ok(list_scalar(structs))
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut states = Vec::new();
        for (kernel, _, _) in &mut self.reducers {
            states.extend(kernel.states()?.into_iter().map(list_scalar));
        }
        states.push(ScalarValue::Int64(self.width.map(|w| w as i64)));
        Ok(states)
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Err(error("coordinate bundle has invalid state"));
        }
        let widths = states
            .last()
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| error("coordinate bundle has invalid width state"))?;
        let mut offset = 0;
        for (kernel, _, _) in &mut self.reducers {
            let count = kernel.aggregate.state_fields()?.len();
            let slices = states
                .get(offset..offset + count)
                .ok_or_else(|| error("coordinate bundle has incomplete state"))?;
            let width_state = states.last().unwrap();
            let mut merged = slices.to_vec();
            merged.push(width_state.clone());
            kernel.merge_batch(&merged)?;
            offset += count;
        }
        for row in 0..widths.len() {
            if widths.is_null(row) {
                continue;
            }
            let w = usize::try_from(widths.value(row)).map_err(|_| error("invalid array width"))?;
            if self.width.is_some_and(|old| old != w) {
                return Err(error("array_agg requires equal array lengths"));
            }
            self.width = Some(w);
        }
        Ok(())
    }
    fn size(&self) -> usize {
        let mut shared_indices = std::collections::HashSet::new();
        let kernels = self
            .reducers
            .iter()
            .map(|(kernel, _, _)| {
                let mut size = kernel.size();
                if let Some(indices) = &kernel.group_indices {
                    if !shared_indices.insert(Arc::as_ptr(indices)) {
                        size = size.saturating_sub(
                            indices.values.capacity() * std::mem::size_of::<usize>(),
                        );
                    }
                }
                size
            })
            .sum::<usize>();
        std::mem::size_of::<Self>()
            + self.reducers.capacity() * std::mem::size_of::<(Coordinates, usize, Option<usize>)>()
            + kernels
    }
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

    fn update_flat(
        &mut self,
        flat: ArrayRef,
        filter: Option<&arrow_array::BooleanArray>,
        width: usize,
    ) -> Result<()> {
        let flat = if self.input == self.aggregate_input {
            flat
        } else {
            cast(flat.as_ref(), &self.aggregate_input)?
        };
        if matches!(self.kernels, Kernels::Groups(_)) {
            if self
                .group_indices
                .as_ref()
                .is_none_or(|v| v.values.len() < flat.len())
            {
                self.group_indices = Some(coordinate_indices(width, flat.len())?);
            }
            let indices = &self.group_indices.as_ref().unwrap().values[..flat.len()];
            let Kernels::Groups(kernel) = &mut self.kernels else {
                unreachable!()
            };
            return kernel
                .get_mut()
                .map_err(|_| error("coordinate accumulator state poisoned"))?
                .update_batch(&[flat], indices, filter, width);
        }
        let Kernels::Coordinates(kernels) = &mut self.kernels else {
            unreachable!()
        };
        for (coordinate, kernel) in kernels.iter_mut().enumerate() {
            let indices = UInt32Array::from_iter_values(
                (coordinate..flat.len())
                    .step_by(width)
                    .filter(|i| filter.is_none_or(|mask| mask.is_valid(*i) && mask.value(*i)))
                    .map(|i| i as u32),
            );
            if indices.is_empty() {
                continue;
            }
            kernel.update_batch(&[take(flat.as_ref(), &indices, None)?])?;
        }
        Ok(())
    }

    fn evaluate_values(&mut self) -> Result<ArrayRef> {
        if self.width == Some(0) {
            return Ok(new_empty_array(self.aggregate.field().data_type()));
        }
        let mut values = match &mut self.kernels {
            Kernels::Groups(_) => {
                let width = self.width.unwrap_or(0);
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
        if values.null_count() > 0 && !self.empty.is_null() {
            let mask = arrow_arith::boolean::is_null(values.as_ref())?;
            let fill = self.empty.to_scalar()?;
            values = arrow_select::zip::zip(&mask, &fill, &values)?;
        }
        Ok(values)
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
        self.update_flat(flat, None, width)
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
        let values = self.evaluate_values()?;
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
