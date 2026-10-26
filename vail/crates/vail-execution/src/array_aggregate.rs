//! Lift a DataFusion aggregate over array coordinates without unnesting rows.
//! Arithmetic, null handling, partial states and merges remain owned by the
//! underlying aggregate. SUM uses reusable typed Arrow vectors; other native
//! reducers use GroupsAccumulator or one Arrow batch per coordinate.
use std::sync::{Arc, Mutex, Weak};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    StructArray, UInt8Array, UInt32Array, UInt64Array, new_empty_array,
};
use arrow_buffer::{BooleanBuffer, OffsetBuffer};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;
use arrow_data::transform::MutableArrayData;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, EmitTo, GroupsAccumulator};
use datafusion_physical_expr::{
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
    expressions::Column,
};

/// Callers validate the width of every defined list before using this helper.
/// Keep one prepared coordinate kernel update per batch, preserving value order.
fn coordinate_values(arrays: &ListArray, width: usize) -> Result<Option<ArrayRef>> {
    let defined = arrays.len() - arrays.null_count();
    if defined == 0 {
        return Ok(None);
    }
    let selected = defined.checked_mul(width)
        .ok_or_else(|| error("coordinate child count overflow"))?;
    let offsets = arrays.value_offsets();
    let start = offsets[0] as usize;
    let length = (offsets[arrays.len()] - offsets[0]) as usize;
    // Equal valid widths and monotonic offsets prove that missing lists hide
    // no payload. Their bitmap can be fragmented without requiring compaction.
    if length == selected {
        return Ok(Some(arrays.values().slice(start, length)));
    }
    // Prepare Arrow's range-copy machinery once against the original child.
    // Coalesced defined spans omit arbitrary hidden children without per-list
    // ArrayRef slices or buffers/indices proportional to the hidden payload.
    let data = arrays.values().to_data();
    let mut output = MutableArrayData::new(vec![&data], false, selected);
    let mut run: Option<(usize, usize)> = None;
    for row in 0..arrays.len() {
        if arrays.is_null(row) {
            continue;
        }
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        match run {
            Some((first, last)) if last == start => run = Some((first, end)),
            Some((first, last)) => {
                output.extend(0, first, last);
                run = Some((start, end));
            }
            None => run = Some((start, end)),
        }
    }
    if let Some((first, last)) = run {
        output.extend(0, first, last);
    }
    Ok(Some(arrow_array::make_array(output.freeze())))
}

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
        .map(|r| {
            Ok((
                r.aggregate.clone(),
                r.value_index,
                r.filter_index,
                r.input.clone(),
                r.aggregate.return_type(std::slice::from_ref(&r.input))?,
                r.empty.clone(),
                r.identity.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
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
        let Some(flat) = coordinate_values(arrays, width)? else {
            return Ok(());
        };
        let packed = flat
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error("coordinate bundle elements must be structs"))?;
        if packed.num_columns() < self.packed_fields {
            return Err(error("coordinate bundle struct is missing slots"));
        }
        let parent_filter = packed
            .nulls()
            .map(|nulls| BooleanArray::new(nulls.inner().clone(), None));
        for (kernel, value_index, filter_index) in &mut self.reducers {
            let filter = filter_index.map(|i| {
                packed
                    .column(i)
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .expect("validated filter type")
            });
            let combined_filter = match (parent_filter.as_ref(), filter) {
                (Some(parent), Some(filter)) => Some(arrow_arith::boolean::and(parent, filter)?),
                (Some(parent), None) => Some(parent.clone()),
                (None, Some(_)) => None,
                (None, None) => None,
            };
            kernel.set_width(width)?;
            kernel.update_flat(
                packed.column(*value_index).clone(),
                combined_filter.as_ref().or(filter),
                width,
            )?;
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

/// Reduce a direct integer-list field across owners without materializing entry
/// structs. The full input type and resolved field path participate in semantic
/// identity; no schema lookup is performed in the owner traversal.
pub fn owning_coordinate_sum_udaf(input: DataType, names: &[String]) -> Result<Arc<AggregateUDF>> {
    let DataType::List(outer_field) = &input else {
        return Err(error("owning coordinate sum requires outer lists"));
    };
    let mut dtype = outer_field.data_type();
    let mut path = Vec::with_capacity(names.len());
    for name in names {
        let DataType::Struct(fields) = dtype else {
            return Err(error("owning coordinate path requires ordinary structs"));
        };
        let index = fields
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| error("owning coordinate field is absent"))?;
        path.push(index);
        dtype = fields[index].data_type();
    }
    let DataType::List(element) = dtype else {
        return Err(error("owning coordinate field requires a list"));
    };
    if path.is_empty() || !matches!(element.data_type(), DataType::Int32 | DataType::Int64) {
        return Err(error("owning coordinate sum requires direct integer lists"));
    }
    let primitive = element.data_type().clone();
    let inner = crate::aggregates::aggregate_udf("sum", Some(&primitive))
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "element",
        primitive.clone(),
        true,
    )]));
    let aggregate = Arc::new(
        AggregateExprBuilder::new(inner, vec![Arc::new(Column::new("element", 0))])
            .schema(schema)
            .alias("coordinate")
            .build()?,
    );
    let state_types = aggregate
        .state_fields()?
        .iter()
        .map(|field| list_type(field.data_type().clone()))
        .chain(std::iter::once(DataType::Int64))
        .collect();
    let output = list_type(aggregate.field().data_type().clone());
    let empty = ScalarValue::Int64(Some(0));
    let identity = (
        input.clone(),
        path.clone(),
        aggregate.fun().clone(),
        empty.clone(),
    );
    Ok(crate::kernel_identity::aggregate_named(
        "vail_owning_coordinate_sum",
        identity,
        vec![input],
        output,
        state_types,
        Arc::new(move |_| {
            Ok(Box::new(OwningCoordinates {
                coordinates: Coordinates::new(
                    aggregate.clone(),
                    empty.clone(),
                    primitive.clone(),
                    primitive.clone(),
                )?,
                path: path.clone(),
                runs: Vec::new(),
                owners: Vec::new(),
                validity: arrow_buffer::BooleanBufferBuilder::new(0),
            }))
        }),
    ))
}

/// A selected owner domain is never represented as a fabricated contiguous
/// ListArray. Borrow fields first, then copy only observed primitive spans.
#[derive(Debug)]
struct OwningCoordinates {
    coordinates: Coordinates,
    path: Vec<usize>,
    runs: Vec<(usize, usize)>,
    owners: Vec<(usize, usize)>,
    validity: arrow_buffer::BooleanBufferBuilder,
}
impl OwningCoordinates {
    fn update_selected<const MASKED: bool, const PACKED: bool>(
        coordinates: &mut Coordinates,
        runs: &mut Vec<(usize, usize)>,
        owners: &[(usize, usize)],
        arrays: &ListArray,
        validity: &[u8],
        validity_offset: usize,
    ) -> Result<()> {
        let selected = owners.iter().flat_map(|&(start, end)| start..end)
            .enumerate().filter_map(|(packed, owner)| {
                let index = if PACKED { packed } else { owner };
                (!MASKED || arrow_buffer::bit_util::get_bit(validity, validity_offset + index))
                    .then_some(owner)
            });
        let offsets = arrays.value_offsets();
        runs.clear();
        let mut count = 0usize;
        // Validate and coalesce only observed owners before touching primitive
        // backing data. Missing owners never establish a coordinate width.
        for owner in selected {
            let start = offsets[owner] as usize;
            let end = offsets[owner + 1] as usize;
            coordinates.set_width(end - start)?;
            count = count.checked_add(1).ok_or_else(|| error("coordinate owner count overflow"))?;
            if let Some(last) = runs.last_mut().filter(|last| last.1 == start) {
                last.1 = end;
            } else {
                runs.push((start, end));
            }
        }
        let Some(width) = coordinates.width else { return Ok(()); };
        if width == 0 || count == 0 { return Ok(()); }
        let length = count.checked_mul(width).ok_or_else(|| error("coordinate child count overflow"))?;
        let flat = if runs.len() == 1 {
            arrays.values().slice(runs[0].0, length)
        } else {
            let data = arrays.values().to_data();
            let mut output = MutableArrayData::new(vec![&data], false, length);
            for &(start, end) in runs.iter() {
                output.extend(0, start, end);
            }
            arrow_array::make_array(output.freeze())
        };
        coordinates.update_flat(flat, None, width)
    }
}
impl Accumulator for OwningCoordinates {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let outer = values
            .first()
            .and_then(|value| value.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| error("owning coordinate sum requires outer lists"))?;
        let mut value = outer.values().as_ref();
        let mut ancestors = Vec::with_capacity(self.path.len());
        for &index in &self.path {
            let structure = value
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| error("owning coordinate input changed type"))?;
            if let Some(nulls) = structure.nulls() {
                ancestors.push(nulls);
            }
            value = structure.column(index).as_ref();
        }
        let arrays = value
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| error("owning coordinate input changed list type"))?;
        // Build only the selected owner domain. All-valid outer lists need no
        // row traversal; nullable outer gaps are skipped through Arrow's bitmap
        // index iterator, without touching their child envelope.
        self.owners.clear();
        let offsets = outer.value_offsets();
        if outer.null_count() == 0 {
            if offsets[0] != offsets[outer.len()] {
                self.owners.push((offsets[0] as usize, offsets[outer.len()] as usize));
            }
        } else {
            for row in outer.nulls().expect("nullable outer list").valid_indices() {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                if start == end { continue; }
                if let Some(last) = self.owners.last_mut().filter(|last| last.1 == start) {
                    last.1 = end;
                } else { self.owners.push((start, end)); }
            }
        }
        // Nullability is bound once per batch. Width/span traversal consumes
        // one bitmap, never a dynamic fold across Struct layers per entry.
        if let Some(nulls) = arrays.nulls() { ancestors.push(nulls); }
        ancestors.retain(|nulls| nulls.null_count() != 0);
        match ancestors.as_slice() {
            [] => Self::update_selected::<false, false>(
                &mut self.coordinates, &mut self.runs, &self.owners, arrays, &[], 0),
            [single] => Self::update_selected::<true, false>(
                &mut self.coordinates, &mut self.runs, &self.owners, arrays,
                single.inner().values(), single.inner().offset()),
            [first, rest @ ..] => {
                self.validity.truncate(0);
                for &(start, end) in &self.owners {
                    self.validity.append_packed_range(
                        first.inner().offset() + start..first.inner().offset() + end,
                        first.inner().values());
                }
                for mask in rest {
                    let mut packed = 0;
                    for &(start, end) in &self.owners {
                        arrow_buffer::bit_util::apply_bitwise_binary_op(
                            self.validity.as_slice_mut(), packed, mask.inner().values(),
                            mask.inner().offset() + start, end - start, |left, right| left & right);
                        packed += end - start;
                    }
                }
                Self::update_selected::<true, true>(
                    &mut self.coordinates, &mut self.runs, &self.owners, arrays,
                    self.validity.as_slice(), 0)
            }
        }
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.coordinates.evaluate()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.coordinates.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.coordinates.merge_batch(states)
    }
    fn size(&self) -> usize {
        self.coordinates.size() + self.path.capacity() * std::mem::size_of::<usize>()
            + (self.runs.capacity() + self.owners.capacity()) * std::mem::size_of::<(usize, usize)>()
            + self.validity.capacity().div_ceil(8)
    }
}

/// Reusable typed numeric vectors, with a compile-time arithmetic operation.
#[derive(Debug)]
struct DenseNumeric {
    values: ArrayRef,
}
impl DenseNumeric {
    fn new(dtype: &DataType) -> Self {
        Self {
            values: new_empty_array(dtype),
        }
    }
    /// Increment coordinate counters directly from intersected packed masks.
    /// Missing masks mean all ones. No Boolean/UInt8/filter output is built.
    fn update_masks(
        &mut self,
        len: usize,
        width: usize,
        masks: [Option<&BooleanBuffer>; 4],
    ) -> Result<()> {
        if width == 0 { return Ok(()); }
        let mut inputs = [crate::packed_count::BitSlice { data: &[], offset: 0 }; 4];
        let mut count = 0;
        for mask in masks.into_iter().flatten() {
            if mask.len() != len { return Err(error("coordinate bitmap length mismatch")); }
            inputs[count] = crate::packed_count::BitSlice { data: mask.values(), offset: mask.offset() };
            count += 1;
        }
        let empty = new_empty_array(self.values.data_type());
        let old = std::mem::replace(&mut self.values, empty);
        macro_rules! update {
            ($array:ty, $zero:expr) => {{
                let values = if old.is_empty() { <$array>::from(vec![$zero; width]) }
                    else { old.as_any().downcast_ref::<$array>().unwrap().clone() };
                drop(old);
                let mut builder = values.into_builder().unwrap_or_else(|shared|
                    <$array>::from(shared.values().to_vec()).into_builder().unwrap());
                crate::packed_count::accumulate(&inputs[..count], len / width, builder.values_slice_mut());
                self.values = Arc::new(builder.finish());
            }};
        }
        match old.data_type() {
            DataType::Int64 => update!(Int64Array, 0_i64),
            DataType::UInt64 => update!(UInt64Array, 0_u64),
            _ => return Err(error("packed counts require64-bit integer state")),
        }
        Ok(())
    }
    fn count_valid(&mut self, flat: &ArrayRef, filter: Option<&BooleanArray>, width: usize) -> Result<()> {
        let validity = flat.logical_nulls();
        self.update_masks(flat.len(), width, [
            validity.as_ref().map(|n| n.inner()),
            filter.map(|f| f.values()),
            filter.and_then(|f| f.nulls()).map(|n| n.inner()), None,
        ])
    }
    fn count_rows(&mut self, len: usize, filter: Option<&BooleanArray>, width: usize) -> Result<()> {
        self.update_masks(len, width, [filter.map(|f| f.values()),
            filter.and_then(|f| f.nulls()).map(|n| n.inner()), None, None])
    }
    fn update(
        &mut self,
        flat: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        self.update_inner::<false>(flat, filter, width)
    }
    fn product(
        &mut self,
        flat: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        self.update_inner::<true>(flat, filter, width)
    }
    fn update_inner<const PRODUCT: bool>(
        &mut self,
        flat: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        if width == 0 {
            return Ok(());
        }
        if !PRODUCT && matches!(self.values.data_type(), DataType::Int64 | DataType::UInt64) {
            if let Some(values) = flat.as_any().downcast_ref::<BooleanArray>() {
                return self.update_masks(values.len(), width, [Some(values.values()),
                    values.nulls().map(|n| n.inner()), filter.map(|f| f.values()),
                    filter.and_then(|f| f.nulls()).map(|n| n.inner())]);
            }
        }
        // Arrow's mixed primitive kernel reads narrow values directly. Boolean
        // storage is bit-packed, so use its native one-byte cast, not Int64.
        let mut input = if flat.data_type() == &DataType::Boolean {
            cast(flat.as_ref(), &DataType::UInt8)?
        } else {
            Arc::clone(flat)
        };
        if let Some(filter) = filter {
            let zero = ScalarValue::try_from(input.data_type())?.to_scalar()?;
            input = arrow_select::zip::zip(filter, &input, &zero)?;
        }
        if input.null_count() != 0 {
            let mask = arrow_arith::boolean::is_null(input.as_ref())?;
            let zero = match input.data_type() {
                DataType::Int32 => ScalarValue::Int32(Some(if PRODUCT { 1 } else { 0 })),
                DataType::Int64 => ScalarValue::Int64(Some(if PRODUCT { 1 } else { 0 })),
                DataType::UInt8 => ScalarValue::UInt8(Some(if PRODUCT { 1 } else { 0 })),
                DataType::UInt64 => ScalarValue::UInt64(Some(if PRODUCT { 1 } else { 0 })),
                DataType::Float32 => ScalarValue::Float32(Some(if PRODUCT { 1.0 } else { 0.0 })),
                DataType::Float64 => ScalarValue::Float64(Some(if PRODUCT { 1.0 } else { 0.0 })),
                _ => unreachable!(),
            }
            .to_scalar()?;
            input = arrow_select::zip::zip(&mask, &zero, &input)?;
        }
        macro_rules! add {
            ($array:ty, $input_array:ty, $zero:expr, $op:expr) => {{
                // Move the state owner into Arrow's writable primitive buffer.
                let empty = new_empty_array(self.values.data_type());
                let old = std::mem::replace(&mut self.values, empty);
                let values = if old.is_empty() {
                    <$array>::from(vec![if PRODUCT { 1 as _ } else { $zero }; width])
                } else {
                    old.as_any().downcast_ref::<$array>().unwrap().clone()
                };
                drop(old);
                // Acquire the writable Arrow state once per batch. Rebuilding
                // a PrimitiveArray/builder for every short row costs more than
                // its arithmetic and prevents a tight contiguous inner loop.
                let mut builder = values.into_builder().unwrap_or_else(|shared| {
                    <$array>::from(shared.values().to_vec())
                        .into_builder()
                        .unwrap()
                });
                let output = builder.values_slice_mut();
                let input = input.as_any().downcast_ref::<$input_array>().unwrap();
                let op = $op;
                // Coordinates checks equal list widths and flattens only whole
                // defined lists (and validates partial-state widths), so the
                // flat length is divisible by width. Numeric state has no nulls;
                // input null/filter slots were replaced by the identity above.
                for row in input.values().chunks_exact(width) {
                    for (sum, value) in output.iter_mut().zip(row.iter()) {
                        *sum = op(*sum, *value);
                    }
                }
                self.values = Arc::new(builder.finish());
            }};
        }
        macro_rules! integer_op {
            ($a:expr, $b:expr) => {
                if PRODUCT {
                    $a.wrapping_mul($b)
                } else {
                    $a.wrapping_add($b)
                }
            };
        }
        macro_rules! float_op {
            ($a:expr, $b:expr) => {
                if PRODUCT { $a * $b } else { $a + $b }
            };
        }
        match input.data_type() {
            DataType::Int32 if self.values.data_type() == &DataType::Float64 => add!(
                Float64Array,
                Int32Array,
                0.0f64,
                |a: f64, b: i32| float_op!(a, f64::from(b))
            ),
            DataType::Int64 if self.values.data_type() == &DataType::Float64 => add!(
                Float64Array,
                Int64Array,
                0.0f64,
                |a: f64, b: i64| float_op!(a, b as f64)
            ),
            DataType::Int32 => add!(Int64Array, Int32Array, 0i64, |a: i64, b: i32| integer_op!(
                a,
                i64::from(b)
            )),
            DataType::UInt8 if self.values.data_type() == &DataType::UInt64 => {
                add!(UInt64Array, UInt8Array, 0u64, |a: u64, b: u8| integer_op!(
                    a,
                    u64::from(b)
                ))
            }
            DataType::UInt64 => add!(
                UInt64Array,
                UInt64Array,
                0u64,
                |a: u64, b: u64| integer_op!(a, b)
            ),
            DataType::UInt8 => add!(Int64Array, UInt8Array, 0i64, |a: i64, b: u8| integer_op!(
                a,
                i64::from(b)
            )),
            DataType::Int64 => add!(Int64Array, Int64Array, 0i64, |a: i64, b: i64| integer_op!(
                a, b
            )),
            DataType::Float32 => add!(
                Float64Array,
                Float32Array,
                0.0f64,
                |a: f64, b: f32| float_op!(a, f64::from(b))
            ),
            DataType::Float64 => add!(
                Float64Array,
                Float64Array,
                0.0f64,
                |a: f64, b: f64| float_op!(a, b)
            ),
            _ => unreachable!(),
        }
        Ok(())
    }
}

/// Bit-packed Boolean state uses Arrow's offset-aware in-place bitmap operators.
#[derive(Debug)]
struct DenseBool {
    values: BooleanBuffer,
    all: bool,
}
impl DenseBool {
    fn new(all: bool) -> Self {
        Self {
            values: BooleanBuffer::new_unset(0),
            all,
        }
    }
    fn array(&self) -> ArrayRef {
        Arc::new(BooleanArray::new(self.values.clone(), None))
    }
    fn update(
        &mut self,
        flat: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        if width == 0 {
            return Ok(());
        }
        let identity = BooleanArray::new_scalar(self.all);
        let mut input = Arc::clone(flat);
        if let Some(filter) = filter {
            input = arrow_select::zip::zip(filter, &input, &identity)?;
        }
        if input.null_count() != 0 {
            let missing = arrow_arith::boolean::is_null(input.as_ref())?;
            input = arrow_select::zip::zip(&missing, &identity, &input)?;
        }
        let input = input.as_any().downcast_ref::<BooleanArray>().unwrap();
        if self.values.is_empty() {
            self.values = if self.all {
                BooleanBuffer::new_set(width)
            } else {
                BooleanBuffer::new_unset(width)
            };
        }
        for offset in (0..input.len()).step_by(width) {
            let row = input.values().slice(offset, width);
            // These native operators reuse a uniquely owned buffer and copy
            // only when an earlier state snapshot still owns the allocation.
            if self.all {
                self.values &= &row;
            } else {
                self.values |= &row;
            }
        }
        Ok(())
    }
}

/// Native product state is (defined count, widened product).
#[derive(Debug)]
struct DenseProduct {
    count: DenseNumeric,
    values: DenseNumeric,
}
impl DenseProduct {
    fn new(dtype: &DataType) -> Self {
        Self {
            count: DenseNumeric::new(&DataType::Int64),
            values: DenseNumeric::new(dtype),
        }
    }
    fn update(
        &mut self,
        flat: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        self.count.count_valid(flat, filter, width)?;
        self.values.product(flat, filter, width)
    }
    fn merge(&mut self, states: &[ArrayRef], width: usize) -> Result<()> {
        self.count.update(&states[0], None, width)?;
        self.values.product(&states[1], None, width)
    }
}

/// Two vector states preserve native AVG/fraction wire types at emission.
#[derive(Debug)]
struct DenseRatio {
    count: DenseNumeric,
    sum: DenseNumeric,
    fraction: bool,
}
impl DenseRatio {
    fn new(fraction: bool) -> Self {
        Self {
            count: DenseNumeric::new(if fraction {
                &DataType::Int64
            } else {
                &DataType::UInt64
            }),
            sum: DenseNumeric::new(if fraction {
                &DataType::Int64
            } else {
                &DataType::Float64
            }),
            fraction,
        }
    }
    fn update(
        &mut self,
        flat: &ArrayRef,
        filter: Option<&BooleanArray>,
        width: usize,
    ) -> Result<()> {
        if self.fraction {
            // Missing predicates still contribute to fraction's denominator.
            self.count.count_rows(flat.len(), filter, width)?;
        } else {
            self.count.count_valid(flat, filter, width)?;
        }
        self.sum.update(flat, filter, width)
    }
    fn states(&self) -> Result<Vec<ArrayRef>> {
        Ok(vec![
            Arc::clone(&self.count.values),
            Arc::clone(&self.sum.values),
        ])
    }
    fn merge(&mut self, values: &[ArrayRef], width: usize) -> Result<()> {
        self.count.update(&values[0], None, width)?;
        self.sum.update(&values[1], None, width)
    }
    fn evaluate(&self) -> Result<ArrayRef> {
        if self.fraction {
            let count = self
                .count
                .values
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sum = self
                .sum
                .values
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            Ok(Arc::new(Float64Array::from_iter((0..count.len()).map(
                |i| (count.value(i) != 0).then(|| sum.value(i) as f64 / count.value(i) as f64),
            ))))
        } else {
            let count = self
                .count
                .values
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let sum = self
                .sum
                .values
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            Ok(Arc::new(arrow_arith::arity::binary::<
                _,
                _,
                _,
                arrow_array::types::Float64Type,
            >(
                sum,
                count,
                |sum: f64, count: u64| {
                    if count == 0 {
                        f64::NAN
                    } else {
                        sum / count as f64
                    }
                },
            )?))
        }
    }
}

/// DataFusion grouped state emission consumes its buffers. Keep that immutable
/// snapshot until another input arrives; read-only state/evaluate calls must not
/// repeatedly reconstruct and re-emit every group's state.
struct GroupKernel {
    kernel: Box<dyn GroupsAccumulator>,
    snapshot: Option<Vec<ArrayRef>>,
}
impl GroupKernel {
    fn snapshot(&mut self) -> Result<Vec<ArrayRef>> {
        if self.snapshot.is_none() {
            self.snapshot = Some(self.kernel.state(EmitTo::All)?);
        }
        Ok(self.snapshot.as_ref().unwrap().clone())
    }
    fn restore(&mut self, width: usize) -> Result<()> {
        if let Some(snapshot) = self.snapshot.take() {
            // A snapshot can precede the first defined input list: it then
            // contains zero groups, although this update just established width.
            let groups = snapshot.first().map_or(0, |state| state.len());
            if groups > 0 {
                if groups != width { return Err(error("coordinate snapshot width mismatch")); }
                self.kernel.merge_batch(&snapshot, &(0..width).collect::<Vec<_>>(), width)?;
            }
        }
        Ok(())
    }
    fn size(&self) -> usize {
        self.kernel.size() + self.snapshot.as_ref().map_or(0, |states|
            states.iter().map(|array| array.get_array_memory_size()).sum::<usize>())
    }
}

enum Kernels {
    Sum(DenseNumeric),
    Count(DenseNumeric),
    Ratio(DenseRatio),
    Product(DenseProduct),
    Bool(DenseBool),
    Extrema(crate::coordinate_extrema::DenseExtrema),
    Stddev(crate::coordinate_stddev::DenseStddev),
    Collect(crate::coordinate_collect::CoordinateCollect),
    Histogram(crate::advanced_numeric_aggregates::CoordinateHistogram),
    Correlation(crate::advanced_numeric_aggregates::CoordinateCorrelation),
    Quantiles(crate::advanced_numeric_aggregates::CoordinateQuantiles),
    Linreg(crate::advanced_numeric_aggregates::CoordinateLinreg),
    OrderedTake(crate::coordinate_take::CoordinateOrderedTake),
    Groups(std::sync::Mutex<GroupKernel>),
    Coordinates(Vec<Box<dyn Accumulator>>),
}
impl std::fmt::Debug for Kernels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sum(sum) | Self::Count(sum) => sum.fmt(f),
            Self::Ratio(ratio) => ratio.fmt(f),
            Self::Product(product) => product.fmt(f),
            Self::Bool(boolean) => boolean.fmt(f),
            Self::Extrema(extrema) => extrema.fmt(f),
            Self::Stddev(stddev) => stddev.fmt(f),
            Self::Collect(collect) => collect.fmt(f),
            Self::Histogram(histogram) => histogram.fmt(f),
            Self::Correlation(correlation) => correlation.fmt(f),
            Self::Quantiles(quantiles) => quantiles.fmt(f),
            Self::Linreg(linreg) => linreg.fmt(f),
            Self::OrderedTake(take) => take.fmt(f),
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
        let native = crate::reducer::native_reducer(aggregate.fun());
        let kernels = if let Some(dtype) = crate::aggregates::coordinate_sum_type(native) {
            Kernels::Sum(DenseNumeric::new(&dtype))
        } else if crate::aggregates::coordinate_count(native) {
            Kernels::Count(DenseNumeric::new(&DataType::Int64))
        } else if crate::native_count_where::coordinate_count_where(native) {
            // Boolean true counts are the numeric SUM of defined true bits.
            Kernels::Sum(DenseNumeric::new(&DataType::Int64))
        } else if crate::native_mean::coordinate_mean(native) {
            Kernels::Ratio(DenseRatio::new(false))
        } else if crate::fraction::coordinate_fraction(native) {
            Kernels::Ratio(DenseRatio::new(true))
        } else if let Some(dtype) = crate::aggregates::coordinate_product_type(native) {
            Kernels::Product(DenseProduct::new(&dtype))
        } else if let Some(all) = crate::native_bool::coordinate_bool(native) {
            Kernels::Bool(DenseBool::new(all))
        } else if let Some(extrema) = crate::coordinate_extrema::native_minimum(native)
            .and_then(|minimum| crate::coordinate_extrema::DenseExtrema::new(&aggregate_input, minimum))
        {
            Kernels::Extrema(extrema)
        } else if crate::native_stddev::coordinate_stddev(native) {
            Kernels::Stddev(Default::default())
        } else if native.inner().downcast_ref::<datafusion_functions_aggregate::array_agg::ArrayAgg>().is_some()
            && aggregate.groups_accumulator_supported()
        {
            Kernels::Collect(crate::coordinate_collect::CoordinateCollect::new(aggregate_input.clone()))
        } else if let Some(take) = crate::advanced_collection_aggregates::coordinate_take(native) {
            Kernels::Collect(take)
        } else if let Some(histogram) = crate::advanced_numeric_aggregates::coordinate_histogram(native) {
            Kernels::Histogram(histogram)
        } else if let Some(correlation) = crate::advanced_numeric_aggregates::coordinate_correlation(native) {
            Kernels::Correlation(correlation)
        } else if let Some(quantiles) = crate::advanced_numeric_aggregates::coordinate_quantiles(native) {
            Kernels::Quantiles(quantiles)
        } else if let Some(linreg) = crate::advanced_numeric_aggregates::coordinate_linreg(native) {
            Kernels::Linreg(linreg)
        } else if let Some(take) = crate::advanced_collection_aggregates::coordinate_ordered_take(native) {
            Kernels::OrderedTake(take)
        } else if aggregate.groups_accumulator_supported() {
            Kernels::Groups(std::sync::Mutex::new(GroupKernel {
                kernel: aggregate.create_groups_accumulator()?,
                snapshot: None,
            }))
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
            Kernels::Sum(sum) | Kernels::Count(sum) => Ok(vec![Arc::clone(&sum.values)]),
            Kernels::Ratio(ratio) => ratio.states(),
            Kernels::Bool(boolean) => Ok(vec![boolean.array()]),
            Kernels::Extrema(extrema) => Ok(vec![extrema.array()]),
            Kernels::Stddev(stddev) => Ok(stddev.states()),
            Kernels::Collect(collect) => Ok(vec![collect.array(width)?]),
            Kernels::Histogram(histogram) => histogram.states(),
            Kernels::Correlation(correlation) => Ok(correlation.states()),
            Kernels::Quantiles(quantiles) => quantiles.states(),
            Kernels::Linreg(linreg) => linreg.states(),
            Kernels::OrderedTake(take) => take.states(width),
            Kernels::Product(product) => Ok(vec![
                Arc::clone(&product.count.values),
                Arc::clone(&product.values.values),
            ]),
            Kernels::Groups(kernel) => {
                let kernel = kernel
                    .get_mut()
                    .map_err(|_| error("coordinate accumulator state poisoned"))?;
                kernel.snapshot()
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
        if let Kernels::OrderedTake(take) = &mut self.kernels {
            let flat = if flat.data_type() == &self.aggregate_input { flat } else { cast(&flat, &self.aggregate_input)? };
            return take.update(&flat, filter, width);
        }
        if let Kernels::Linreg(linreg) = &mut self.kernels {
            return linreg.update(&flat, filter, width);
        }
        if let Kernels::Quantiles(quantiles) = &mut self.kernels {
            return quantiles.update(&flat, filter, width);
        }
        if let Kernels::Correlation(correlation) = &mut self.kernels {
            return correlation.update(&flat, filter, width);
        }
        if let Kernels::Histogram(histogram) = &mut self.kernels {
            return histogram.update(&flat, filter, width);
        }
        if let Kernels::Collect(collect) = &mut self.kernels {
            let flat = if flat.data_type() == &self.aggregate_input { flat } else { cast(&flat, &self.aggregate_input)? };
            return collect.update(&flat, filter, width);
        }
        if let Kernels::Stddev(stddev) = &mut self.kernels {
            return stddev.update(&flat, filter, width);
        }
        if let Kernels::Extrema(extrema) = &mut self.kernels {
            return extrema.update(&flat, filter, width);
        }
        if let Kernels::Sum(sum) = &mut self.kernels {
            return sum.update(&flat, filter, width);
        }
        if let Kernels::Bool(boolean) = &mut self.kernels {
            return boolean.update(&flat, filter, width);
        }
        if let Kernels::Product(product) = &mut self.kernels {
            return product.update(&flat, filter, width);
        }
        if let Kernels::Ratio(ratio) = &mut self.kernels {
            return ratio.update(&flat, filter, width);
        }
        if let Kernels::Count(count) = &mut self.kernels {
            return count.count_valid(&flat, filter, width);
        }
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
            let kernel = kernel.get_mut().map_err(|_| error("coordinate accumulator state poisoned"))?;
            kernel.restore(width)?;
            return kernel.kernel.update_batch(&[flat], indices, filter, width);
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
            Kernels::Sum(sum) | Kernels::Count(sum) => Arc::clone(&sum.values),
            Kernels::Ratio(ratio) => ratio.evaluate()?,
            Kernels::Product(product) => Arc::clone(&product.values.values),
            Kernels::Bool(boolean) => boolean.array(),
            Kernels::Extrema(extrema) => extrema.array(),
            Kernels::Stddev(stddev) => stddev.array(),
            Kernels::Collect(collect) => collect.array(self.width.unwrap_or(0))?,
            Kernels::Histogram(histogram) => histogram.array()?,
            Kernels::Correlation(correlation) => correlation.array()?,
            Kernels::Quantiles(quantiles) => quantiles.array()?,
            Kernels::Linreg(linreg) => linreg.array()?,
            Kernels::OrderedTake(take) => take.array(self.width.unwrap_or(0))?,
            Kernels::Groups(_) => {
                let width = self.width.unwrap_or(0);
                let states = self.states()?;
                if crate::reducer::native_reducer(self.aggregate.fun()).inner()
                    .downcast_ref::<datafusion_functions_aggregate::array_agg::ArrayAgg>().is_some()
                {
                    // Native ARRAY_AGG's sole ListArray state is its result.
                    // Retain its native order/null semantics without a second
                    // merge/counting-sort/interleave of the collected values.
                    states.into_iter().next().ok_or_else(|| error("collect snapshot is missing"))?
                } else {
                    let mut copy = self.aggregate.create_groups_accumulator()?;
                    copy.merge_batch(&states, &(0..width).collect::<Vec<_>>(), width)?;
                    copy.evaluate(EmitTo::All)?
                }
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
        let Some(flat) = coordinate_values(arrays, width)? else {
            return Ok(());
        };
        if matches!(
            self.kernels,
            Kernels::Count(_) | Kernels::Ratio(_) | Kernels::Product(_) | Kernels::Bool(_) | Kernels::Extrema(_) | Kernels::Stddev(_) | Kernels::Collect(_) | Kernels::Histogram(_) | Kernels::Correlation(_) | Kernels::Quantiles(_) | Kernels::Linreg(_) | Kernels::OrderedTake(_)
        ) {
            return self.update_flat(flat, None, width);
        }
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
                Kernels::Sum(sum) | Kernels::Count(sum) => sum.values.get_array_memory_size(),
                Kernels::Ratio(ratio) => {
                    ratio.count.values.get_array_memory_size()
                        + ratio.sum.values.get_array_memory_size()
                }
                Kernels::Product(product) => {
                    product.count.values.get_array_memory_size()
                        + product.values.values.get_array_memory_size()
                }
                Kernels::Bool(boolean) => boolean.values.inner().capacity(),
                Kernels::Extrema(extrema) => extrema.size(),
                Kernels::Stddev(stddev) => stddev.size(),
                Kernels::Collect(collect) => collect.size(),
                Kernels::Histogram(histogram) => histogram.size(),
                Kernels::Correlation(correlation) => correlation.size(),
                Kernels::Quantiles(quantiles) => quantiles.size(),
                Kernels::Linreg(linreg) => linreg.size(),
                Kernels::OrderedTake(take) => take.size(),
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
                Kernels::Sum(sum) | Kernels::Count(sum) => sum.update(&values[0], None, width)?,
                Kernels::Ratio(ratio) => ratio.merge(&values, width)?,
                Kernels::Product(product) => product.merge(&values, width)?,
                Kernels::Bool(boolean) => boolean.update(&values[0], None, width)?,
                Kernels::Extrema(extrema) => extrema.update(&values[0], None, width)?,
                Kernels::Stddev(stddev) => stddev.merge(&values, width)?,
                Kernels::Collect(collect) => collect.merge(&values, width)?,
                Kernels::Histogram(histogram) => histogram.merge(&values, width)?,
                Kernels::Correlation(correlation) => correlation.merge(&values, width)?,
                Kernels::Quantiles(quantiles) => quantiles.merge(&values, width)?,
                Kernels::Linreg(linreg) => linreg.merge(&values, width)?,
                Kernels::OrderedTake(take) => take.merge(&values, width)?,
                Kernels::Groups(kernel) => {
                    let kernel = kernel
                        .get_mut()
                        .map_err(|_| error("coordinate accumulator state poisoned"))?;
                    kernel.restore(width)?;
                    kernel.kernel.merge_batch(&values, &(0..width).collect::<Vec<_>>(), width)?
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_buffer::NullBuffer;
    use arrow_schema::Fields;
    use datafusion_physical_expr::expressions::Column;

    fn reducer(value_index: usize, aggregate: Arc<AggregateUDF>) -> CoordinateReducer {
        CoordinateReducer {
            name: "count_value".into(),
            aggregate,
            input: DataType::Int64,
            value_index,
            filter_index: None,
            empty: ScalarValue::Int64(Some(0)),
            identity: "same-caller-identity".into(),
        }
    }

    fn bundle(value_index: usize, aggregate: Arc<AggregateUDF>) -> Arc<AggregateUDF> {
        coordinate_bundle_udaf(
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("left", DataType::Int64, true)),
                Arc::new(Field::new("right", DataType::Int64, true)),
            ])),
            vec![reducer(value_index, aggregate)],
        )
        .unwrap()
    }

    #[test]
    fn public_coordinate_mean_merge_preserves_full_uint64_count_domain() {
        let mean = crate::reducer::configured_udaf(
            "mean",
            Some(&DataType::Float64),
            &serde_json::json!({}),
        )
        .unwrap();
        let aggregate = coordinate_udaf(
            mean,
            DataType::Float64,
            ScalarValue::Float64(Some(f64::NAN)),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "xs",
            DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
            true,
        )]));
        let expression = AggregateExprBuilder::new(aggregate, vec![Arc::new(Column::new("xs", 0))])
            .schema(schema)
            .alias("means")
            .build()
            .unwrap();
        let mut accumulator = expression.create_accumulator().unwrap();
        let count = 1u64 << 63;
        let state_list = |values: ArrayRef| -> ArrayRef {
            Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(values.data_type().clone(), true)),
                OffsetBuffer::from_lengths([1]),
                values,
                None,
            ))
        };
        accumulator
            .merge_batch(&[
                state_list(Arc::new(UInt64Array::from(vec![count]))),
                state_list(Arc::new(Float64Array::from(vec![count as f64 * 2.0]))),
                Arc::new(Int64Array::from(vec![1])),
            ])
            .unwrap();
        let ScalarValue::List(result) = accumulator.evaluate().unwrap() else {
            panic!("list result");
        };
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
        let state = accumulator.state().unwrap();
        let ScalarValue::List(counts) = &state[0] else {
            panic!("list state");
        };
        assert_eq!(
            counts
                .values()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            count
        );
    }

    #[test]
    fn bundle_identity_uses_semantic_reducer_and_selector() {
        let first = crate::reducer::configured_udaf(
            "count",
            Some(&DataType::Int64),
            &serde_json::json!({}),
        )
        .unwrap();
        let second = crate::reducer::configured_udaf(
            "count",
            Some(&DataType::Int64),
            &serde_json::json!({}),
        )
        .unwrap();
        assert_eq!(bundle(0, first.clone()), bundle(0, second.clone()));
        assert_ne!(bundle(0, first), bundle(1, second));
    }

    #[test]
    fn nullable_packed_struct_masks_every_reducer() {
        let count = crate::reducer::configured_udaf(
            "count",
            Some(&DataType::Int64),
            &serde_json::json!({}),
        )
        .unwrap();
        let bundle = bundle(1, count);
        let packed_type = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("left", DataType::Int64, true)),
            Arc::new(Field::new("right", DataType::Int64, true)),
        ]));
        let packed_list_type =
            DataType::List(Arc::new(Field::new_list_field(packed_type.clone(), true)));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "packed",
            packed_list_type,
            true,
        )]));
        let expression =
            AggregateExprBuilder::new(bundle, vec![Arc::new(Column::new("packed", 0))])
                .schema(input_schema)
                .alias("counts")
                .build()
                .unwrap();
        let mut accumulator = expression.create_accumulator().unwrap();

        let fields = match packed_type {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        };
        let left = Int64Array::new(
            vec![100, 0].into(),
            Some(NullBuffer::from(vec![true, false])),
        );
        let right = Int64Array::from(vec![Some(100), Some(10)]);
        let packed = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(left), Arc::new(right)],
            Some(NullBuffer::from(vec![false, true])),
        ));
        let input = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(packed.data_type().clone(), true)),
            OffsetBuffer::from_lengths([2]),
            packed,
            None,
        ));
        accumulator.update_batch(&[input]).unwrap();
        let ScalarValue::List(result) = accumulator.evaluate().unwrap() else {
            panic!("coordinate bundle did not return a list")
        };
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        let result = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let counts = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[0, 1]);
    }
}
