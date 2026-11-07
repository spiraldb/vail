// Write prepared output columns into Arrow's final builder tree.
use arrow_array::{
    StringArray,
    builder::{ArrayBuilder, ListBuilder, StringBuilder, StructBuilder, make_builder},
};

pub(super) fn supported(dtype: &DataType) -> bool {
    supported_at(dtype, 0)
}
fn supported_at(dtype: &DataType, depth: usize) -> bool {
    if depth == MAX_DEPTH {
        return false;
    }
    Kind::of(dtype).is_some()
        || match dtype {
            DataType::Struct(fields) => fields
                .iter()
                .all(|field| supported_at(field.data_type(), depth + 1)),
            DataType::Utf8 => true,
            DataType::List(field) => Kind::of(field.data_type()).is_some(),
            _ => false,
        }
}
fn make_output_builder(dtype: &DataType) -> Box<dyn ArrayBuilder> {
    match dtype {
        DataType::Struct(fields) => Box::new(StructBuilder::new(
            fields.clone(),
            fields
                .iter()
                .map(|field| make_output_builder(field.data_type()))
                .collect(),
        )),
        DataType::List(field) => {
            macro_rules! list {
                ($builder:ty) => {
                    Box::new(
                        ListBuilder::new(<$builder>::with_capacity(0)).with_field(field.clone()),
                    )
                };
            }
            match Kind::of(field.data_type()).unwrap() {
                Kind::I32 => list!(Int32Builder),
                Kind::I64 => list!(Int64Builder),
                Kind::F32 => list!(Float32Builder),
                Kind::F64 => list!(Float64Builder),
                Kind::Bool => list!(BooleanBuilder),
            }
        }
        _ => make_builder(dtype, 0),
    }
}
struct OutputBlock<'a> {
    reads: &'a [BoundRead],
    workspace: &'a [Buffer],
    coordinates: &'a [Vec<usize>],
    span: Option<SpanInput>,
    count: usize,
    entries: &'a [(usize, usize)],
    active: u64,
}
impl OutputBlock<'_> {
    fn index(&self, read: usize, lane: usize) -> usize {
        let read = &self.reads[read];
        if read.scalar && read.coordinate == 0 {
            return 0;
        }
        match self.span {
            Some(span) if span.coordinate == read.coordinate => span.index(self.coordinates, lane),
            _ => self.coordinates[read.coordinate][lane],
        }
    }
    fn append(
        &self,
        builder: &mut dyn ArrayBuilder,
        column: &OutputColumn,
        parents: Option<&[u8]>,
        scratch: &mut [u8],
    ) -> Result<()> {
        let parent_valid = |i: usize| parents.is_none_or(|valid| valid[i] != 0);
        match column {
            OutputColumn::Scalar { kind, registers } => {
                macro_rules! numeric {
                    ($builder:ty, $getter:ident) => {{
                        let out = builder.as_any_mut().downcast_mut::<$builder>().unwrap();
                        let mut columns = [&[][..]; MAX_LEAVES];
                        for (leaf, &register) in registers.iter().enumerate() {
                            if self.active & (1 << leaf) != 0 {
                                columns[leaf] =
                                    self.workspace[register].$getter(self.reads, self.count);
                            }
                        }
                        let leaf = self.entries[0].1;
                        let source = &self.workspace[registers[leaf]];
                        if self.active.count_ones() == 1
                            && source.all_valid
                            && parents.is_none_or(|p| p.iter().all(|&v| v != 0))
                            && self.entries.windows(2).all(|p| p[0].0 + 1 == p[1].0)
                        {
                            if source.scalar {
                                out.append_value_n(columns[leaf][0], self.entries.len());
                            } else {
                                let first = self.entries[0].0;
                                out.append_slice(&columns[leaf][first..first + self.entries.len()]);
                            }
                        } else {
                            for (i, &(lane, leaf)) in self.entries.iter().enumerate() {
                                let source = &self.workspace[registers[leaf]];
                                out.append_option(
                                    (parent_valid(i) && source.valid(lane)).then(|| {
                                        columns[leaf][if source.scalar { 0 } else { lane }]
                                    }),
                                );
                            }
                        }
                    }};
                }
                match kind {
                    Kind::I32 => numeric!(Int32Builder, i32),
                    Kind::I64 => numeric!(Int64Builder, i64),
                    Kind::F32 => numeric!(Float32Builder, f32),
                    Kind::F64 => numeric!(Float64Builder, f64),
                    Kind::Bool => {
                        let out = builder
                            .as_any_mut()
                            .downcast_mut::<BooleanBuilder>()
                            .unwrap();
                        for (i, &(lane, leaf)) in self.entries.iter().enumerate() {
                            let source = &self.workspace[registers[leaf]];
                            out.append_option(
                                (parent_valid(i) && source.valid(lane))
                                    .then(|| source.true_at(lane)),
                            );
                        }
                    }
                }
            }
            OutputColumn::Struct { valid, fields } => {
                let out = builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .unwrap();
                let (mask, remaining) = scratch.split_at_mut(self.entries.len());
                for (i, &(lane, leaf)) in self.entries.iter().enumerate() {
                    mask[i] =
                        u8::from(parent_valid(i) && self.workspace[valid[leaf]].true_at(lane));
                }
                for (child, column) in out.field_builders_mut().iter_mut().zip(fields) {
                    self.append(child.as_mut(), column, Some(mask), remaining)?;
                }
                for &valid in mask.iter() {
                    out.append(valid != 0);
                }
            }
            OutputColumn::Borrowed {
                dtype,
                reads,
                valid,
            } => match dtype {
                DataType::Utf8 => {
                    let out = builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap();
                    let first = self.reads[reads[0]]
                        .values
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let mut arrays = [first; MAX_LEAVES];
                    for (leaf, &read) in reads.iter().enumerate() {
                        arrays[leaf] = self.reads[read]
                            .values
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                    }
                    for (i, &(lane, leaf)) in self.entries.iter().enumerate() {
                        if parent_valid(i) && self.workspace[valid[leaf]].true_at(lane) {
                            let value = arrays[leaf].value(self.index(reads[leaf], lane));
                            if out
                                .values_slice()
                                .len()
                                .checked_add(value.len())
                                .is_none_or(|n| n > i32::MAX as usize)
                            {
                                return Err(DataFusionError::Execution(
                                    "flatmap string exceeds Utf8 capacity".into(),
                                ));
                            }
                            out.append_value(value);
                        } else {
                            out.append_null();
                        }
                    }
                }
                DataType::List(field) => {
                    let first = self.reads[reads[0]]
                        .values
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();
                    let mut lists = [first; MAX_LEAVES];
                    for (leaf, &read) in reads.iter().enumerate() {
                        lists[leaf] = self.reads[read]
                            .values
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap();
                    }
                    macro_rules! lists {
                        ($builder:ty,$array:ty,$copy:ident) => {{
                            let out = builder
                                .as_any_mut()
                                .downcast_mut::<ListBuilder<$builder>>()
                                .unwrap();
                            let first =
                                lists[0].values().as_any().downcast_ref::<$array>().unwrap();
                            let mut arrays = [first; MAX_LEAVES];
                            for (leaf, list) in lists.iter().enumerate().take(reads.len()) {
                                arrays[leaf] =
                                    list.values().as_any().downcast_ref::<$array>().unwrap();
                            }
                            let mut valid_bits = [false; MAX_BLOCK];
                            for (i, &(lane, leaf)) in self.entries.iter().enumerate() {
                                if parent_valid(i) && self.workspace[valid[leaf]].true_at(lane) {
                                    let index = self.index(reads[leaf], lane);
                                    let offsets = lists[leaf].value_offsets();
                                    let start = offsets[index] as usize;
                                    let stop = offsets[index + 1] as usize;
                                    if out
                                        .values_ref()
                                        .len()
                                        .checked_add(stop - start)
                                        .is_none_or(|n| n > i32::MAX as usize)
                                    {
                                        return Err(DataFusionError::Execution(
                                            "flatmap child exceeds List32 capacity".into(),
                                        ));
                                    }
                                    $copy!(
                                        out.values(),
                                        arrays[leaf],
                                        start,
                                        stop,
                                        &mut valid_bits
                                    );
                                    out.append(true);
                                } else {
                                    out.append(false);
                                }
                            }
                        }};
                    }
                    macro_rules! numeric_span {
                        ($out:expr,$array:expr,$start:expr,$stop:expr,$mask:expr) => {{
                            let out = $out;
                            let array = $array;
                            if array.null_count() == 0 {
                                out.append_slice(&array.values()[$start..$stop]);
                            } else {
                                // Copy final values in spans; the bounded bit scratch avoids
                                // constructing an Arrow slice/header for each child list.
                                let mask = $mask;
                                for first in ($start..$stop).step_by(MAX_BLOCK) {
                                    let stop = (first + MAX_BLOCK).min($stop);
                                    for (i, valid) in mask[..stop - first].iter_mut().enumerate() {
                                        *valid = array.is_valid(first + i);
                                    }
                                    out.append_values(
                                        &array.values()[first..stop],
                                        &mask[..stop - first],
                                    );
                                }
                            }
                        }};
                    }
                    macro_rules! boolean_span {
                        ($out:expr,$array:expr,$start:expr,$stop:expr,$mask:expr) => {{
                            let _ = $mask;
                            let out = $out;
                            let array = $array;
                            for index in $start..$stop {
                                out.append_option(
                                    array.is_valid(index).then(|| array.value(index)),
                                );
                            }
                        }};
                    }
                    match Kind::of(field.data_type()).unwrap() {
                        Kind::I32 => lists!(Int32Builder, Int32Array, numeric_span),
                        Kind::I64 => lists!(Int64Builder, Int64Array, numeric_span),
                        Kind::F32 => lists!(Float32Builder, Float32Array, numeric_span),
                        Kind::F64 => lists!(Float64Builder, Float64Array, numeric_span),
                        Kind::Bool => lists!(BooleanBuilder, BooleanArray, boolean_span),
                    }
                }
                _ => unreachable!(),
            },
        }
        Ok(())
    }
}
#[allow(clippy::too_many_arguments)]
fn append_nested(
    out: &mut dyn ArrayBuilder,
    program: &ListFlatMap,
    reads: &[BoundRead],
    workspace: &[Buffer],
    coordinates: &[Vec<usize>],
    span: Option<SpanInput>,
    count: usize,
    entries: &[(usize, usize)],
    scratch: &mut [u8],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let active = entries
        .iter()
        .fold(0u64, |mask, &(_, leaf)| mask | (1 << leaf));
    OutputBlock {
        reads,
        workspace,
        coordinates,
        span,
        count,
        entries,
        active,
    }
    .append(out, program.output.as_ref().unwrap(), None, scratch)
}
