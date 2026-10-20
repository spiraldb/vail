//! Hail-compatible genomic and ndarray aggregates. All states grow only with
//! the number of alleles or the shape of one ndarray, never with row count.
use crate::aggregate_identity::configured_udaf_name;
use crate::expressions::ExprError;
use arrow_array::{
    builder::{Float64Builder, Int32Builder, ListBuilder},
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, ListArray, StructArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_expr::{
    expr_fn::create_udaf,
    function::{AccumulatorArgs, StateFieldsArgs},
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

fn bad(s: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(s.into())
}
fn invalid(s: impl Into<String>) -> ExprError {
    ExprError::Invalid(s.into())
}
fn field(name: &str, ty: DataType) -> Arc<Field> {
    Arc::new(Field::new(name, ty, true))
}
fn list_type(ty: DataType) -> DataType {
    DataType::List(field("item", ty))
}
fn struct_type(fields: Vec<(&str, DataType)>) -> DataType {
    DataType::Struct(
        fields
            .into_iter()
            .map(|(n, t)| field(n, t))
            .collect::<Vec<_>>()
            .into(),
    )
}
fn list(value: ArrayRef, item_type: DataType) -> ScalarValue {
    ScalarValue::List(Arc::new(ListArray::new(
        field("item", item_type),
        OffsetBuffer::from_lengths([value.len()]),
        value,
        None,
    )))
}
fn call_input(t: &DataType) -> bool {
    vail_ordering::same_layout(t, &crate::calls::storage_type())
}

fn packed<'a>(t: &'a DataType, name: &str) -> Result<&'a DataType, ExprError> {
    let DataType::Struct(fs) = t else {
        return Err(invalid("expected packed struct input"));
    };
    fs.iter()
        .find(|f| f.name() == name)
        .map(|f| f.data_type())
        .ok_or_else(|| invalid(format!("missing {name}")))
}
fn packed_col<'a>(s: &'a StructArray, name: &str) -> DfResult<&'a ArrayRef> {
    s.column_by_name(name)
        .ok_or_else(|| bad(format!("missing {name}")))
}
fn scalar_struct(names: &[&str], v: Vec<ScalarValue>) -> DfResult<ScalarValue> {
    let fields = names
        .iter()
        .zip(v.iter())
        .map(|(name, value)| field(name, value.data_type()))
        .collect::<Vec<_>>();
    let columns = v
        .into_iter()
        .map(|value| value.to_array_of_size(1))
        .collect::<DfResult<Vec<_>>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::new(
        fields.into(),
        columns,
        None,
    ))))
}

pub fn make_udaf(
    function: &str,
    input_type: &DataType,
    options: &serde_json::Value,
) -> Result<Option<Arc<AggregateUDF>>, ExprError> {
    let (output, state, kind) = match function {
        "hardy_weinberg_test" => {
            if !call_input(input_type) {
                return Err(invalid("hardy_weinberg_test requires a Call"));
            }
            (
                struct_type(vec![
                    ("het_freq_hwe", DataType::Float64),
                    ("p_value", DataType::Float64),
                ]),
                vec![DataType::Int64; 3],
                Kind::Hwe(options["one_sided"].as_bool().unwrap_or(false)),
            )
        }
        "inbreeding" => {
            if !call_input(packed(input_type, "__vail_genomic_call")?)
                || packed(input_type, "__vail_genomic_prior")? != &DataType::Float64
            {
                return Err(invalid("inbreeding requires Call and float64 prior"));
            }
            (
                struct_type(vec![
                    ("f_stat", DataType::Float64),
                    ("n_called", DataType::Int64),
                    ("expected_homs", DataType::Float64),
                    ("observed_homs", DataType::Int64),
                ]),
                vec![DataType::Int64, DataType::Float64, DataType::Int64],
                Kind::Inbreeding,
            )
        }
        "call_stats" => {
            if !call_input(packed(input_type, "__vail_genomic_call")?) {
                return Err(invalid("call_stats requires Call"));
            }
            let alleles = packed(input_type, "__vail_genomic_alleles")?;
            if !matches!(alleles, DataType::Int32 | DataType::List(_)) {
                return Err(invalid(
                    "call_stats requires int32 allele count or allele array",
                ));
            }
            (
                struct_type(vec![
                    ("AC", list_type(DataType::Int32)),
                    ("AF", list_type(DataType::Float64)),
                    ("AN", DataType::Int32),
                    ("homozygote_count", list_type(DataType::Int32)),
                ]),
                vec![
                    DataType::Int64,
                    list_type(DataType::Int64),
                    list_type(DataType::Int64),
                ],
                Kind::CallStats(
                    options
                        .get("n_alleles")
                        .and_then(serde_json::Value::as_u64)
                        .filter(|n| *n <= i32::MAX as u64)
                        .ok_or_else(|| {
                            invalid("call_stats requires a nonnegative int32 n_alleles initializer")
                        })? as usize,
                ),
            )
        }
        "info_score" => {
            if !matches!(input_type,DataType::List(f) if matches!(f.data_type(),DataType::Float64|DataType::Float32))
            {
                return Err(invalid("info_score requires array<float>"));
            }
            (
                struct_type(vec![
                    ("score", DataType::Float64),
                    ("n_included", DataType::Int32),
                ]),
                vec![
                    DataType::Int64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ],
                Kind::Info,
            )
        }
        "ndarray_sum" => {
            let data = packed(input_type, "__vail_ndarray_data")?;
            let shape = packed(input_type, "__vail_ndarray_shape")?;
            if !matches!(data,DataType::List(f) if matches!(f.data_type(),DataType::Int32|DataType::Int64|DataType::Float32|DataType::Float64))
                || !matches!(shape,DataType::List(f) if f.data_type()==&DataType::Int64)
            {
                return Err(invalid("ndarray_sum requires numeric ndarray struct"));
            }
            (
                input_type.clone(),
                vec![input_type.clone(), DataType::Boolean],
                Kind::Ndarray,
            )
        }
        _ => return Ok(None),
    };
    let ty = input_type.clone();
    let udaf_name = configured_udaf_name(function, input_type, options);
    let grouped_output = output.clone();
    let factory: datafusion_expr::AccumulatorFactoryFunction =
        Arc::new(move |_| Ok(Box::new(GenomicAccumulator::new(kind, ty.clone()))));
    let native = Arc::new(create_udaf(
        &udaf_name,
        vec![input_type.clone()],
        Arc::new(output),
        Volatility::Immutable,
        factory,
        Arc::new(state),
    ));
    if let Kind::CallStats(n_alleles) = kind {
        Ok(Some(Arc::new(AggregateUDF::from(CallStatsUdf {
            name: udaf_name,
            input_type: input_type.clone(),
            output_type: grouped_output,
            signature: Signature::exact(vec![input_type.clone()], Volatility::Immutable),
            native,
            n_alleles,
        }))))
    } else {
        Ok(Some(native))
    }
}

/// The scalar call_stats accumulator is retained for list/scan contexts; this
/// adapter adds a flat, per-group allele counter for relational group-bys.
#[derive(Debug)]
struct CallStatsUdf {
    name: String,
    input_type: DataType,
    output_type: DataType,
    signature: Signature,
    native: Arc<AggregateUDF>,
    n_alleles: usize,
}
impl PartialEq for CallStatsUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.input_type == other.input_type
            && self.output_type == other.output_type
            && self.n_alleles == other.n_alleles
    }
}
impl Eq for CallStatsUdf {}
impl Hash for CallStatsUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.input_type.hash(state);
        self.output_type.hash(state);
        self.n_alleles.hash(state);
    }
}
impl AggregateUDFImpl for CallStatsUdf {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> DfResult<DataType> {
        if args != [self.input_type.clone()] {
            return Err(DataFusionError::Plan(
                "call_stats input type mismatch".into(),
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
                "call_stats grouped accumulator does not support these arguments".into(),
            ));
        }
        Ok(Box::new(CallStatsGroups::new(self.n_alleles)))
    }
}

#[derive(Debug)]
struct CallStatsGroups {
    n_alleles: usize,
    allele_number: Vec<i64>,
    allele_counts: Vec<i64>,
    homozygote_counts: Vec<i64>,
}
impl CallStatsGroups {
    fn new(n_alleles: usize) -> Self {
        Self {
            n_alleles,
            allele_number: Vec::new(),
            allele_counts: Vec::new(),
            homozygote_counts: Vec::new(),
        }
    }
    fn ensure(&mut self, total: usize) -> DfResult<()> {
        let cells = total
            .checked_mul(self.n_alleles)
            .filter(|cells| *cells <= i32::MAX as usize)
            .ok_or_else(|| bad("call_stats grouped state size overflow"))?;
        self.allele_number.resize(total, 0);
        self.allele_counts.resize(cells, 0);
        self.homozygote_counts.resize(cells, 0);
        Ok(())
    }
    fn group(&self, index: usize) -> DfResult<usize> {
        if index >= self.allele_number.len() {
            return Err(bad("call_stats group index exceeds state"));
        }
        index
            .checked_mul(self.n_alleles)
            .ok_or_else(|| bad("call_stats group state offset overflow"))
    }
    fn enabled(filter: Option<&arrow_array::BooleanArray>, row: usize) -> bool {
        filter.is_none_or(|f| f.is_valid(row) && f.value(row))
    }
    fn take_prefix<T: Copy + Default>(values: &mut Vec<T>, count: usize) -> DfResult<Vec<T>> {
        if count > values.len() {
            return Err(bad("call_stats emitted groups exceed state"));
        }
        if count == values.len() {
            Ok(std::mem::take(values))
        } else {
            Ok(values.drain(..count).collect())
        }
    }
    fn emit(&mut self, to: EmitTo) -> DfResult<(Vec<i64>, Vec<i64>, Vec<i64>)> {
        let groups = match to {
            EmitTo::All => self.allele_number.len(),
            EmitTo::First(n) => n,
        };
        let cells = groups
            .checked_mul(self.n_alleles)
            .ok_or_else(|| bad("call_stats grouped state size overflow"))?;
        let allele_number = Self::take_prefix(&mut self.allele_number, groups)?;
        let allele_counts = Self::take_prefix(&mut self.allele_counts, cells)?;
        let homozygote_counts = Self::take_prefix(&mut self.homozygote_counts, cells)?;
        Ok((allele_number, allele_counts, homozygote_counts))
    }
    fn list_i64(values: Vec<i64>, rows: usize, width: usize) -> DfResult<ArrayRef> {
        let cells = rows
            .checked_mul(width)
            .filter(|cells| *cells <= i32::MAX as usize)
            .ok_or_else(|| bad("call_stats grouped state exceeds Arrow list limit"))?;
        if values.len() != cells {
            return Err(bad("call_stats grouped state has wrong size"));
        }
        Ok(Arc::new(ListArray::new(
            field("item", DataType::Int64),
            OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
            Arc::new(Int64Array::from(values)),
            None,
        )))
    }
    fn merge_list(
        list: &ListArray,
        row: usize,
        width: usize,
        into: &mut [i64],
        label: &str,
    ) -> DfResult<()> {
        if list.is_null(row) || list.value_length(row) as usize != width || into.len() != width {
            return Err(bad(format!(
                "call_stats {label} state has wrong allele count"
            )));
        }
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| bad(format!("call_stats {label} state must contain int64")))?;
        let offset = list.value_offsets()[row] as usize;
        for (i, value) in into.iter_mut().enumerate() {
            let source = offset + i;
            if values.is_null(source) {
                return Err(bad("call_stats grouped state must be defined"));
            }
            if values.value(source) < 0 {
                return Err(bad("call_stats grouped state must be nonnegative"));
            }
            *value = value
                .checked_add(values.value(source))
                .ok_or_else(|| bad("call_stats grouped state overflow"))?;
        }
        Ok(())
    }
}
impl GroupsAccumulator for CallStatsGroups {
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&arrow_array::BooleanArray>,
    ) -> DfResult<Vec<ArrayRef>> {
        let rows = values.first().map_or(0, |a| a.len());
        let indices = (0..rows).collect::<Vec<_>>();
        let mut groups = Self::new(self.n_alleles);
        groups.update_batch(values, &indices, filter, rows)?;
        groups.state(EmitTo::All)
    }
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        indices: &[usize],
        filter: Option<&arrow_array::BooleanArray>,
        total: usize,
    ) -> DfResult<()> {
        let [input] = values else {
            return Err(bad("call_stats requires one input"));
        };
        if input.len() != indices.len() || filter.is_some_and(|f| f.len() != indices.len()) {
            return Err(bad("call_stats grouped row counts differ"));
        }
        if indices.iter().any(|&index| index >= total) {
            return Err(bad("call_stats group index exceeds state"));
        }
        self.ensure(total)?;
        let packed = input
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| bad("call_stats requires packed struct"))?;
        let calls = packed_col(packed, "__vail_genomic_call")?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| bad("call_stats requires Call"))?;
        let decoded = crate::calls::CallRows::from_struct(calls).map_err(|e| bad(e.to_string()))?;
        let allele_counts = packed_col(packed, "__vail_genomic_alleles")?;
        for row in 0..packed.len() {
            if !Self::enabled(filter, row) || packed.is_null(row) {
                continue;
            }
            let n = match allele_counts.data_type() {
                DataType::Int32 => {
                    let a = allele_counts.as_any().downcast_ref::<Int32Array>().unwrap();
                    if a.is_null(row) {
                        continue;
                    }
                    a.value(row) as usize
                }
                DataType::List(_) => {
                    let a = allele_counts.as_any().downcast_ref::<ListArray>().unwrap();
                    if a.is_null(row) {
                        continue;
                    }
                    a.value_length(row) as usize
                }
                _ => return Err(bad("invalid call_stats allele count type")),
            };
            if n > i32::MAX as usize {
                return Err(bad("call_stats allele count exceeds int32"));
            }
            if n != self.n_alleles {
                return Err(bad("call_stats allele count differs from initializer"));
            }
            let Some(call) = decoded.get(row).map_err(|e| bad(e.to_string()))? else {
                continue;
            };
            let offset = self.group(indices[row])?;
            for &allele in call.alleles() {
                let allele = allele as usize;
                if allele >= n {
                    return Err(bad("call_stats allele index out of range"));
                }
                let count = &mut self.allele_counts[offset + allele];
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| bad("call_stats count overflow"))?;
                self.allele_number[indices[row]] = self.allele_number[indices[row]]
                    .checked_add(1)
                    .ok_or_else(|| bad("call_stats AN overflow"))?;
            }
            if call.alleles().len() == 2 && call.alleles()[0] == call.alleles()[1] {
                let allele = call.alleles()[0] as usize;
                let count = &mut self.homozygote_counts[offset + allele];
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| bad("call_stats homozygote count overflow"))?;
            }
        }
        Ok(())
    }
    fn evaluate(&mut self, to: EmitTo) -> DfResult<ArrayRef> {
        let (an, ac, hom) = self.emit(to)?;
        let rows = an.len();
        let mut ac_list = ListBuilder::new(Int32Builder::new());
        let mut af_list = ListBuilder::new(Float64Builder::new());
        let mut hom_list = ListBuilder::new(Int32Builder::new());
        for row in 0..rows {
            let start = row * self.n_alleles;
            for value in &ac[start..start + self.n_alleles] {
                ac_list.values().append_value(
                    i32::try_from(*value).map_err(|_| bad("call_stats AC overflow"))?,
                );
            }
            ac_list.append(true);
            for value in &hom[start..start + self.n_alleles] {
                hom_list.values().append_value(
                    i32::try_from(*value)
                        .map_err(|_| bad("call_stats homozygote count overflow"))?,
                );
            }
            hom_list.append(true);
            if an[row] == 0 {
                af_list.append(false);
            } else {
                for value in &ac[start..start + self.n_alleles] {
                    af_list
                        .values()
                        .append_value(*value as f64 / an[row] as f64);
                }
                af_list.append(true);
            }
        }
        let output = struct_type(vec![
            ("AC", list_type(DataType::Int32)),
            ("AF", list_type(DataType::Float64)),
            ("AN", DataType::Int32),
            ("homozygote_count", list_type(DataType::Int32)),
        ]);
        let fields = match output {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        };
        let an = an
            .into_iter()
            .map(|value| i32::try_from(value).map_err(|_| bad("call_stats AN overflow")))
            .collect::<DfResult<Vec<_>>>()?;
        Ok(Arc::new(StructArray::try_new(
            fields,
            vec![
                Arc::new(ac_list.finish()),
                Arc::new(af_list.finish()),
                Arc::new(Int32Array::from(an)),
                Arc::new(hom_list.finish()),
            ],
            None,
        )?))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.allele_number.capacity() * 8
            + (self.allele_counts.capacity() + self.homozygote_counts.capacity()) * 8
    }
    fn state(&mut self, to: EmitTo) -> DfResult<Vec<ArrayRef>> {
        let (an, ac, hom) = self.emit(to)?;
        let rows = an.len();
        Ok(vec![
            Arc::new(Int64Array::from(an)),
            Self::list_i64(ac, rows, self.n_alleles)?,
            Self::list_i64(hom, rows, self.n_alleles)?,
        ])
    }
    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        indices: &[usize],
        total: usize,
    ) -> DfResult<()> {
        let [an, ac, hom] = states else {
            return Err(bad("call_stats state shape mismatch"));
        };
        if an.len() != indices.len() || ac.len() != indices.len() || hom.len() != indices.len() {
            return Err(bad("call_stats grouped state rows differ"));
        }
        let an = an
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| bad("call_stats AN state must be int64"))?;
        let ac = ac
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| bad("call_stats AC state must be a list"))?;
        let hom = hom
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| bad("call_stats homozygote state must be a list"))?;
        self.ensure(total)?;
        for row in 0..indices.len() {
            let group = indices[row];
            let offset = self.group(group)?;
            if an.is_null(row) {
                return Err(bad("call_stats AN state must be defined"));
            }
            if an.value(row) < 0 {
                return Err(bad("call_stats AN state must be nonnegative"));
            }
            self.allele_number[group] = self.allele_number[group]
                .checked_add(an.value(row))
                .ok_or_else(|| bad("call_stats AN state overflow"))?;
            Self::merge_list(
                ac,
                row,
                self.n_alleles,
                &mut self.allele_counts[offset..offset + self.n_alleles],
                "AC",
            )?;
            Self::merge_list(
                hom,
                row,
                self.n_alleles,
                &mut self.homozygote_counts[offset..offset + self.n_alleles],
                "homozygote",
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Hwe(bool),
    Inbreeding,
    CallStats(usize),
    Info,
    Ndarray,
}
#[derive(Debug)]
struct GenomicAccumulator {
    kind: Kind,
    input_type: DataType,
    ints: [i64; 3],
    floats: [f64; 3],
    ac: Vec<i64>,
    hom: Vec<i64>,
    shape: Option<Vec<i64>>,
    ndarray_data: Option<ArrayRef>,
}
impl GenomicAccumulator {
    fn new(kind: Kind, input_type: DataType) -> Self {
        let n = match kind {
            Kind::CallStats(n) => n,
            _ => 0,
        };
        Self {
            kind,
            input_type,
            ints: [0; 3],
            floats: [0.; 3],
            ac: vec![0; n],
            hom: vec![0; n],
            shape: None,
            ndarray_data: None,
        }
    }
    fn update_call_stats(&mut self, s: &StructArray) -> DfResult<()> {
        let calls = packed_col(s, "__vail_genomic_call")?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| bad("call_stats requires Call"))?;
        let allele_counts = packed_col(s, "__vail_genomic_alleles")?;
        let decoded = crate::calls::CallRows::from_struct(calls).map_err(|e| bad(e.to_string()))?;
        for row in 0..s.len() {
            if s.is_null(row) {
                continue;
            }
            let n = match allele_counts.data_type() {
                DataType::Int32 => {
                    let a = allele_counts.as_any().downcast_ref::<Int32Array>().unwrap();
                    if a.is_null(row) {
                        continue;
                    }
                    a.value(row) as usize
                }
                DataType::List(_) => {
                    let a = allele_counts.as_any().downcast_ref::<ListArray>().unwrap();
                    if a.is_null(row) {
                        continue;
                    }
                    a.value_length(row) as usize
                }
                _ => return Err(bad("invalid allele count type")),
            };
            if n > i32::MAX as usize {
                return Err(bad("call_stats allele count exceeds int32"));
            }
            if n != self.ac.len() {
                return Err(bad("call_stats allele count differs from initializer"));
            }
            let Some(call) = decoded.get(row).map_err(|e| bad(e.to_string()))? else {
                continue;
            };
            self.tally_call(call.alleles(), n)?;
        }
        Ok(())
    }
    // Both adapters validate n == self.ac.len() before calling this shared tally.
    fn tally_call(&mut self, call: &[u16], n: usize) -> DfResult<()> {
        for &allele in call {
            let index = allele as usize;
            if index >= n {
                return Err(bad("call_stats allele index out of range"));
            }
            self.ac[index] = self.ac[index]
                .checked_add(1)
                .ok_or_else(|| bad("call_stats count overflow"))?;
            self.ints[0] = self.ints[0]
                .checked_add(1)
                .ok_or_else(|| bad("call_stats AN overflow"))?;
        }
        if call.len() == 2 && call[0] == call[1] {
            let count = &mut self.hom[call[0] as usize];
            *count = count
                .checked_add(1)
                .ok_or_else(|| bad("call_stats homozygote count overflow"))?;
        }
        Ok(())
    }

    fn update_fixed_calls(&mut self, calls: &ArrayRef, n: usize) -> DfResult<()> {
        if n != self.ac.len() {
            return Err(bad("call_stats allele count differs from initializer"));
        }
        let rows = crate::calls::CallRows::new(calls).map_err(|e| bad(e.to_string()))?;
        for row in 0..calls.len() {
            if let Some(call) = rows.get(row).map_err(|e| bad(e.to_string()))? {
                self.tally_call(call.alleles(), n)?;
            }
        }
        Ok(())
    }

    fn update_ndarray(&mut self, s: &StructArray) -> DfResult<()> {
        let datas = packed_col(s, "__vail_ndarray_data")?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| bad("ndarray data must be list"))?;
        let shapes = packed_col(s, "__vail_ndarray_shape")?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| bad("ndarray shape must be list"))?;
        for row in 0..s.len() {
            if s.is_null(row) {
                continue;
            }
            if datas.is_null(row) || shapes.is_null(row) {
                return Err(bad("ndarray missing data or shape"));
            }
            let shape_arr = shapes.value(row);
            let shape_arr = shape_arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| bad("ndarray shape must be int64"))?;
            let mut shape = Vec::with_capacity(shape_arr.len());
            let mut len = 1usize;
            for i in 0..shape_arr.len() {
                if shape_arr.is_null(i) || shape_arr.value(i) < 0 {
                    return Err(bad("invalid ndarray dimension"));
                }
                let d = shape_arr.value(i);
                len = len
                    .checked_mul(d as usize)
                    .ok_or_else(|| bad("ndarray shape overflow"))?;
                shape.push(d);
            }
            let data = datas.value(row);
            if data.len() != len {
                return Err(bad("ndarray data length does not match shape"));
            }
            if let Some(existing) = &self.shape {
                if *existing != shape {
                    return Err(bad("ndarray_sum requires equal shapes"));
                }
            } else {
                self.shape = Some(shape);
            }
            // Hail's aggregate treats a missing element as zero. Arrow's
            // arithmetic propagates nulls, so fill them before using its
            // typed wrapping-add kernel.
            let data = zero_filled(data)?;
            self.ndarray_data = Some(match self.ndarray_data.take() {
                Some(total) => arrow_arith::numeric::add_wrapping(&total.as_ref(), &data.as_ref())?,
                None => data,
            });
        }
        Ok(())
    }
    fn ndarray_scalar(&self) -> DfResult<ScalarValue> {
        let Some(shape) = &self.shape else {
            return ScalarValue::try_from(&self.input_type);
        };
        let DataType::Struct(fields) = &self.input_type else {
            unreachable!()
        };
        let DataType::List(item) = fields
            .iter()
            .find(|f| f.name() == "__vail_ndarray_data")
            .unwrap()
            .data_type()
        else {
            unreachable!()
        };
        let data = self
            .ndarray_data
            .as_ref()
            .ok_or_else(|| bad("ndarray state missing data"))?
            .clone();
        // Preserve the exact item field names required by the ndarray codec.
        let data_list = ScalarValue::List(Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths([data.len()]),
            data,
            None,
        )));
        let shape_item = match fields
            .iter()
            .find(|f| f.name() == "__vail_ndarray_shape")
            .unwrap()
            .data_type()
        {
            DataType::List(f) => f.clone(),
            _ => unreachable!(),
        };
        let shape_data: ArrayRef = Arc::new(Int64Array::from(shape.clone()));
        let shape_list = ScalarValue::List(Arc::new(ListArray::new(
            shape_item,
            OffsetBuffer::from_lengths([shape.len()]),
            shape_data,
            None,
        )));
        let value = scalar_struct(
            &["__vail_ndarray_data", "__vail_ndarray_shape"],
            vec![data_list, shape_list],
        )?;
        let array = arrow_cast::cast(&value.to_array_of_size(1)?, &self.input_type)?;
        Ok(ScalarValue::Struct(Arc::new(
            array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| bad("ndarray metadata restoration must produce struct"))?
                .clone(),
        )))
    }
    fn result(&self) -> DfResult<ScalarValue> {
        match self.kind {
            Kind::Hwe(one_sided) => {
                let (freq, p) = hwe(self.ints[0], self.ints[1], self.ints[2], one_sided)?;
                scalar_struct(
                    &["het_freq_hwe", "p_value"],
                    vec![
                        ScalarValue::Float64(Some(freq)),
                        ScalarValue::Float64(Some(p)),
                    ],
                )
            }
            Kind::Inbreeding => {
                let n = self.ints[0];
                let e = self.floats[0];
                let obs = self.ints[1];
                scalar_struct(
                    &["f_stat", "n_called", "expected_homs", "observed_homs"],
                    vec![
                        ScalarValue::Float64(Some((obs as f64 - e) / (n as f64 - e))),
                        ScalarValue::Int64(Some(n)),
                        ScalarValue::Float64(Some(e)),
                        ScalarValue::Int64(Some(obs)),
                    ],
                )
            }
            Kind::CallStats(_) => {
                let an = i32::try_from(self.ints[0]).map_err(|_| bad("call_stats AN overflow"))?;
                let ac = self
                    .ac
                    .iter()
                    .map(|x| i32::try_from(*x).map_err(|_| bad("call_stats AC overflow")))
                    .collect::<DfResult<Vec<_>>>()?;
                let hom = self
                    .hom
                    .iter()
                    .map(|x| {
                        i32::try_from(*x).map_err(|_| bad("call_stats homozygote count overflow"))
                    })
                    .collect::<DfResult<Vec<_>>>()?;
                let af = self
                    .ac
                    .iter()
                    .map(|x| *x as f64 / an as f64)
                    .collect::<Vec<_>>();
                scalar_struct(
                    &["AC", "AF", "AN", "homozygote_count"],
                    vec![
                        list(Arc::new(Int32Array::from(ac)), DataType::Int32),
                        if an == 0 {
                            ScalarValue::try_from(&list_type(DataType::Float64))?
                        } else {
                            list(Arc::new(Float64Array::from(af)), DataType::Float64)
                        },
                        ScalarValue::Int32(Some(an)),
                        list(Arc::new(Int32Array::from(hom)), DataType::Int32),
                    ],
                )
            }
            Kind::Info => {
                let n = self.ints[0];
                let theta = if self.floats[2] != 0. {
                    self.floats[1] / self.floats[2]
                } else {
                    f64::NAN
                };
                let score = if n == 0 {
                    None
                } else if theta == 0. || theta == 1. {
                    Some(1.)
                } else {
                    Some(1. - (self.floats[0] / n as f64) / (2. * theta * (1. - theta)))
                };
                scalar_struct(
                    &["score", "n_included"],
                    vec![
                        ScalarValue::Float64(score),
                        ScalarValue::Int32(Some(
                            i32::try_from(n).map_err(|_| bad("info_score count overflow"))?,
                        )),
                    ],
                )
            }
            Kind::Ndarray => self.ndarray_scalar(),
        }
    }
}
fn number(a: &dyn Array, i: usize) -> DfResult<f64> {
    Ok(match a.data_type() {
        DataType::Int32 => a.as_any().downcast_ref::<Int32Array>().unwrap().value(i) as f64,
        DataType::Int64 => a.as_any().downcast_ref::<Int64Array>().unwrap().value(i) as f64,
        DataType::Float32 => a.as_any().downcast_ref::<Float32Array>().unwrap().value(i) as f64,
        DataType::Float64 => a.as_any().downcast_ref::<Float64Array>().unwrap().value(i),
        t => return Err(bad(format!("expected numeric input, got {t:?}"))),
    })
}
fn list_i64(v: &[i64]) -> ScalarValue {
    list(Arc::new(Int64Array::from(v.to_vec())), DataType::Int64)
}
fn zero_filled(data: ArrayRef) -> DfResult<ArrayRef> {
    if data.null_count() == 0 {
        return Ok(data);
    }
    let len = data.len();
    let zero: ArrayRef = match data.data_type() {
        DataType::Int32 => Arc::new(Int32Array::from(vec![0; len])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![0; len])),
        DataType::Float32 => Arc::new(Float32Array::from(vec![0.; len])),
        DataType::Float64 => Arc::new(Float64Array::from(vec![0.; len])),
        _ => return Err(bad("unsupported ndarray element type")),
    };
    let mask =
        arrow_array::BooleanArray::from((0..len).map(|i| data.is_valid(i)).collect::<Vec<_>>());
    Ok(arrow_select::zip::zip(
        &mask,
        &data.as_ref(),
        &zero.as_ref(),
    )?)
}
fn read_list_i64(a: &dyn Array, row: usize) -> DfResult<Vec<i64>> {
    let l = a
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| bad("state must be a list"))?;
    if l.is_null(row) {
        return Ok(vec![]);
    }
    let value = l.value(row);
    let ints = value
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| bad("state list must contain int64"))?;
    Ok((0..ints.len()).map(|i| ints.value(i)).collect())
}
fn int_col(a: &dyn Array, row: usize) -> DfResult<i64> {
    Ok(a.as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| bad("state must be int64"))?
        .value(row))
}
fn float_col(a: &dyn Array, row: usize) -> DfResult<f64> {
    Ok(a.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| bad("state must be float64"))?
        .value(row))
}
impl Accumulator for GenomicAccumulator {
    fn update_batch(&mut self, v: &[ArrayRef]) -> DfResult<()> {
        let [a] = v else {
            return Err(bad("genomic aggregate expects one argument"));
        };
        match self.kind {
            Kind::Hwe(_) => {
                let s = a
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| bad("HWE requires Call"))?;
                let calls =
                    crate::calls::CallRows::from_struct(s).map_err(|e| bad(e.to_string()))?;
                for i in 0..s.len() {
                    let Some(call) = calls.get(i).map_err(|e| bad(e.to_string()))? else {
                        continue;
                    };
                    let call = call.alleles();
                    if call.len() != 2 {
                        continue;
                    }
                    let n = call.iter().filter(|&&x| x > 0).count();
                    self.ints[n] = self.ints[n]
                        .checked_add(1)
                        .ok_or_else(|| bad("HWE count overflow"))?;
                }
            }
            Kind::Inbreeding => {
                let s = a
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| bad("inbreeding requires packed struct"))?;
                let calls = packed_col(s, "__vail_genomic_call")?
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| bad("inbreeding requires Call"))?;
                let priors = packed_col(s, "__vail_genomic_prior")?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| bad("inbreeding prior must be float64"))?;
                let decoded =
                    crate::calls::CallRows::from_struct(calls).map_err(|e| bad(e.to_string()))?;
                for i in 0..s.len() {
                    if s.is_null(i) || priors.is_null(i) {
                        continue;
                    }
                    let Some(call) = decoded.get(i).map_err(|e| bad(e.to_string()))? else {
                        continue;
                    };
                    let call = call.alleles();
                    if call.len() != 2 || call.iter().any(|&x| x > 1) {
                        return Err(bad(
                            "'inbreeding' does not support non-diploid or multiallelic genotypes",
                        ));
                    }
                    let p = priors.value(i);
                    self.ints[0] += 1;
                    self.floats[0] += 1. - 2. * p * (1. - p);
                    if call[0] == call[1] {
                        self.ints[1] += 1;
                    }
                }
            }
            Kind::CallStats(_) => self.update_call_stats(
                a.as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| bad("call_stats requires packed struct"))?,
            )?,
            Kind::Info => {
                let l = a
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| bad("info_score requires array"))?;
                for i in 0..l.len() {
                    if l.is_null(i) {
                        continue;
                    }
                    let gp = l.value(i);
                    if gp.len() != 3 {
                        return Err(bad(format!(
                            "'info_score': expected 'gp' to have length 3, found {}",
                            gp.len()
                        )));
                    }
                    if (0..3).any(|j| gp.is_null(j)) {
                        return Err(bad("'info_score': genotype probabilities must be defined"));
                    }
                    let g0 = number(gp.as_ref(), 0)?;
                    let g1 = number(gp.as_ref(), 1)?;
                    let g2 = number(gp.as_ref(), 2)?;
                    let mean = g1 + 2. * g2;
                    self.floats[0] += g1 + 4. * g2 - mean * mean;
                    self.floats[1] += mean;
                    self.floats[2] += g0 + g1 + g2;
                    self.ints[0] += 1;
                }
            }
            Kind::Ndarray => self.update_ndarray(
                a.as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| bad("ndarray_sum requires ndarray struct"))?,
            )?,
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        self.result()
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + (self.ac.capacity()
                + self.hom.capacity()
                + self.shape.as_ref().map_or(0, Vec::capacity))
                * 8
            + self
                .ndarray_data
                .as_ref()
                .map_or(0, |data| data.get_array_memory_size())
    }
    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(match self.kind {
            Kind::Hwe(_) => self
                .ints
                .iter()
                .map(|x| ScalarValue::Int64(Some(*x)))
                .collect(),
            Kind::Inbreeding => vec![
                ScalarValue::Int64(Some(self.ints[0])),
                ScalarValue::Float64(Some(self.floats[0])),
                ScalarValue::Int64(Some(self.ints[1])),
            ],
            Kind::CallStats(_) => vec![
                ScalarValue::Int64(Some(self.ints[0])),
                list_i64(&self.ac),
                list_i64(&self.hom),
            ],
            Kind::Info => vec![
                ScalarValue::Int64(Some(self.ints[0])),
                ScalarValue::Float64(Some(self.floats[0])),
                ScalarValue::Float64(Some(self.floats[1])),
                ScalarValue::Float64(Some(self.floats[2])),
            ],
            Kind::Ndarray => vec![
                self.ndarray_scalar()?,
                ScalarValue::Boolean(Some(self.shape.is_some())),
            ],
        })
    }
    fn merge_batch(&mut self, s: &[ArrayRef]) -> DfResult<()> {
        match self.kind {
            Kind::Hwe(_) => {
                if s.len() != 3 {
                    return Err(bad("HWE state shape mismatch"));
                }
                for (j, a) in s.iter().enumerate() {
                    for i in 0..a.len() {
                        self.ints[j] = self.ints[j]
                            .checked_add(int_col(a.as_ref(), i)?)
                            .ok_or_else(|| bad("HWE state count overflow"))?;
                    }
                }
            }
            Kind::Inbreeding => {
                if s.len() != 3 {
                    return Err(bad("inbreeding state shape mismatch"));
                }
                for i in 0..s[0].len() {
                    self.ints[0] = self.ints[0]
                        .checked_add(int_col(s[0].as_ref(), i)?)
                        .ok_or_else(|| bad("inbreeding called count overflow"))?;
                    self.floats[0] += float_col(s[1].as_ref(), i)?;
                    self.ints[1] = self.ints[1]
                        .checked_add(int_col(s[2].as_ref(), i)?)
                        .ok_or_else(|| bad("inbreeding homozygote count overflow"))?;
                }
            }
            Kind::CallStats(_) => {
                if s.len() != 3 {
                    return Err(bad("call_stats state shape mismatch"));
                }
                for i in 0..s[0].len() {
                    let ac = read_list_i64(s[1].as_ref(), i)?;
                    let hom = read_list_i64(s[2].as_ref(), i)?;
                    if ac.len() != hom.len() {
                        return Err(bad("call_stats state arrays differ in length"));
                    }
                    if self.ac.len() != ac.len() {
                        return Err(bad("call_stats allele count differs between partitions"));
                    }
                    self.ints[0] = self.ints[0]
                        .checked_add(int_col(s[0].as_ref(), i)?)
                        .ok_or_else(|| bad("call_stats AN overflow"))?;
                    for j in 0..ac.len() {
                        self.ac[j] = self.ac[j]
                            .checked_add(ac[j])
                            .ok_or_else(|| bad("call_stats AC overflow"))?;
                        self.hom[j] = self.hom[j]
                            .checked_add(hom[j])
                            .ok_or_else(|| bad("call_stats homozygote count overflow"))?;
                    }
                }
            }
            Kind::Info => {
                if s.len() != 4 {
                    return Err(bad("info_score state shape mismatch"));
                }
                for i in 0..s[0].len() {
                    self.ints[0] = self.ints[0]
                        .checked_add(int_col(s[0].as_ref(), i)?)
                        .ok_or_else(|| bad("info_score count overflow"))?;
                    for j in 0..3 {
                        self.floats[j] += float_col(s[j + 1].as_ref(), i)?
                    }
                }
            }
            Kind::Ndarray => {
                if s.len() != 2 {
                    return Err(bad("ndarray state shape mismatch"));
                }
                let flags = s[1]
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .ok_or_else(|| bad("ndarray state flag must be bool"))?;
                let a = s[0]
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| bad("ndarray state must be struct"))?;
                for i in 0..a.len() {
                    if !flags.value(i) {
                        continue;
                    }
                    let row = arrow_select::take::take(a, &Int32Array::from(vec![i as i32]), None)?;
                    self.update_ndarray(row.as_any().downcast_ref::<StructArray>().unwrap())?
                }
            }
        }
        Ok(())
    }
}

// Levene-Haldane conditional distribution; aggregate states remain three counters.
pub(crate) fn hwe(hom_ref: i64, het: i64, hom_var: i64, one_sided: bool) -> DfResult<(f64, f64)> {
    let (frequency, two, one) = hwe_both(hom_ref, het, hom_var)?;
    Ok((frequency, if one_sided { one } else { two }))
}

pub(crate) fn hwe_both(hom_ref: i64, het: i64, hom_var: i64) -> DfResult<(f64, f64, f64)> {
    if hom_ref < 0 || het < 0 || hom_var < 0 {
        return Err(bad("hardy_weinberg_test requires nonnegative counts"));
    }
    // Hail's LeveneHaldane recurrence, including normalization and summation
    // order. Sharing this kernel keeps scalar and aggregate tests identical.
    let n = hom_ref
        .checked_add(het)
        .and_then(|n| n.checked_add(hom_var))
        .ok_or_else(|| bad("hardy_weinberg_test count overflow"))?;
    if n == 0 {
        return Ok((f64::NAN, 0.5, 0.5));
    }
    let minor = het
        .checked_add(
            hom_ref
                .min(hom_var)
                .checked_mul(2)
                .ok_or_else(|| bad("hardy_weinberg_test count overflow"))?,
        )
        .ok_or_else(|| bad("hardy_weinberg_test count overflow"))?;
    let twice_n = n
        .checked_mul(2)
        .ok_or_else(|| bad("hardy_weinberg_test count overflow"))?;
    let major = twice_n
        .checked_sub(minor)
        .ok_or_else(|| bad("hardy_weinberg_test count overflow"))?;
    let denominator = twice_n
        .checked_sub(1)
        .ok_or_else(|| bad("hardy_weinberg_test count overflow"))?;
    let center_denominator = twice_n
        .checked_add(3)
        .ok_or_else(|| bad("hardy_weinberg_test count overflow"))?;
    let freq = (minor as f64 * major as f64) / denominator as f64 / n as f64;
    let parity = minor % 2;
    let center = (minor as f64 + 1.) * (major as f64 + 1.) / center_denominator as f64;
    let mode = 2 * ((center - parity as f64) / 2.).round() as i64 + parity;
    let mut right = vec![1.];
    let mut h = mode;
    while h < minor {
        let p = right.last().unwrap() * (minor - h) as f64 * (major - h) as f64
            / ((h as f64 + 2.) * (h + 1) as f64);
        right.push(p);
        if p == 0. {
            break;
        }
        h += 2;
    }
    let mut left = vec![1.];
    h = mode;
    while h > parity {
        let p = left.last().unwrap() * h as f64 * (h - 1) as f64
            / (((minor - h) as f64 + 2.) * ((major - h) as f64 + 2.));
        left.push(p);
        if p == 0. {
            break;
        }
        h -= 2;
    }
    let norm = right
        .iter()
        .copied()
        .take_while(|p| *p > 1e-16)
        .sum::<f64>()
        + left.iter().copied().take_while(|p| *p > 1e-16).sum::<f64>()
        - 1.;
    let observed = if het >= mode {
        right
            .get(((het - mode) / 2) as usize)
            .copied()
            .unwrap_or(0.)
    } else {
        left.get(((mode - het) / 2) as usize).copied().unwrap_or(0.)
    };
    let one = {
        let tail = if het >= mode {
            let start = ((het - mode) / 2 + 1) as usize;
            let cutoff = right.get(start).copied().unwrap_or(0.) * 1e-16;
            right
                .iter()
                .skip(start)
                .copied()
                .take_while(|p| *p > cutoff)
                .sum::<f64>()
        } else {
            left.iter()
                .skip(1)
                .take(((mode - het + 1) / 2 - 1) as usize)
                .copied()
                .take_while(|p| *p > 1e-16)
                .sum::<f64>()
                + right
                    .iter()
                    .copied()
                    .take_while(|p| *p > 1e-16)
                    .sum::<f64>()
        };
        tail / norm + 0.5 * (observed / norm)
    };
    let two = if observed <= f64::MIN_POSITIVE {
        0.
    } else {
        let mid_tail = |weights: &[f64]| {
            let p0 = (observed / norm) * norm;
            let equal =
                |p: f64| (p - p0).abs() <= f64::MIN_POSITIVE + 1e-12 * p.abs().max(p0.abs());
            let mut iter = weights
                .iter()
                .copied()
                .skip_while(|p| *p > p0 && !equal(*p))
                .peekable();
            let mut eq = 0.;
            while iter.peek().is_some_and(|p| equal(*p)) {
                eq += iter.next().unwrap();
            }
            0.5 * eq + iter.take_while(|p| *p > p0 * 0.5e-16).sum::<f64>()
        };
        (mid_tail(&left[1..]) + mid_tail(&right)) / norm
    };
    Ok((freq, two, one))
}

/// Per-owning-row adapter for the same checked CallStats reducer used by tables.
/// It scans calls once and never constructs one predicate array per allele.
pub(crate) fn list_call_stats() -> Result<
    (
        Arc<datafusion_expr::ScalarUDF>,
        Vec<crate::expressions::Type>,
        crate::expressions::Type,
    ),
    ExprError,
> {
    crate::list_aggregate::legacy_call_stats()
}
