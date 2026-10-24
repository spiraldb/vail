//! Call kernels over two UInt16 allele columns, a UInt8 ploidy, and a Boolean
//! phase column. Validity belongs to the parent; unused alleles are zero.
use crate::expressions::{ExprError, PrimitiveType, Type};
use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, StructArray, UInt8Array, UInt16Array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Field};
use std::sync::Arc;
pub use vail_ordering::call::Call;

pub const FIRST: &str = "__vail_call_allele0";
pub const SECOND: &str = "__vail_call_allele1";
pub const PLOIDY: &str = "__vail_call_ploidy";
pub const PHASED: &str = "__vail_call_phased";

pub fn storage_type() -> DataType {
    DataType::Struct(
        vec![
            Field::new(FIRST, DataType::UInt16, true),
            Field::new(SECOND, DataType::UInt16, true),
            Field::new(PLOIDY, DataType::UInt8, true),
            Field::new(PHASED, DataType::Boolean, true),
        ]
        .into(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CallAccessor {
    Alleles,
    Phased,
    Property(CallProperty),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CallProperty {
    Ploidy,
    NAltAlleles,
    UnphasedDiploidGtIndex,
    IsHaploid,
    IsDiploid,
    IsHomRef,
    IsNonRef,
    IsHet,
    IsHetRef,
    IsHetNonRef,
    IsHomVar,
}
impl CallAccessor {
    pub(crate) fn for_field(name: &str) -> Option<Self> {
        use CallProperty::*;
        Some(match name {
            "__vail_call_alleles" => Self::Alleles,
            PHASED => Self::Phased,
            PLOIDY => Self::Property(Ploidy),
            "__vail_call_n_alt_alleles" => Self::Property(NAltAlleles),
            "__vail_call_unphased_diploid_gt_index" => Self::Property(UnphasedDiploidGtIndex),
            "__vail_call_is_haploid" => Self::Property(IsHaploid),
            "__vail_call_is_diploid" => Self::Property(IsDiploid),
            "__vail_call_is_hom_ref" => Self::Property(IsHomRef),
            "__vail_call_is_non_ref" => Self::Property(IsNonRef),
            "__vail_call_is_het" => Self::Property(IsHet),
            "__vail_call_is_het_ref" => Self::Property(IsHetRef),
            "__vail_call_is_het_non_ref" => Self::Property(IsHetNonRef),
            "__vail_call_is_hom_var" => Self::Property(IsHomVar),
            _ => return None,
        })
    }
    pub(crate) fn result_type(self) -> Type {
        match self {
            Self::Alleles => Type::Array {
                array: Box::new(Type::Primitive(PrimitiveType::Int32)),
            },
            Self::Property(CallProperty::Ploidy | CallProperty::NAltAlleles | CallProperty::UnphasedDiploidGtIndex) => {
                Type::Primitive(PrimitiveType::Int32)
            }
            _ => Type::Primitive(PrimitiveType::Bool),
        }
    }
}
fn down<T: Array + 'static>(array: &dyn Array) -> Result<&T, ExprError> {
    array.as_any().downcast_ref().ok_or_else(|| ExprError::Invalid("invalid Call Arrow storage; expected two UInt16 alleles, UInt8 ploidy and Boolean phase".into()))
}

pub struct CallRows<'a> {
    parent: &'a StructArray,
    first: &'a UInt16Array,
    second: &'a UInt16Array,
    ploidy: &'a UInt8Array,
    phased: &'a BooleanArray,
}
impl<'a> CallRows<'a> {
    pub fn new(value: &'a ArrayRef) -> Result<Self, ExprError> {
        Self::from_struct(down(value.as_ref())?)
    }
    pub fn from_struct(parent: &'a StructArray) -> Result<Self, ExprError> {
        let field = |name| {
            parent.column_by_name(name).ok_or_else(|| {
                ExprError::Invalid(format!(
                    "Call field {name} is absent; legacy packed Calls require rewriting"
                ))
            })
        };
        Ok(Self {
            parent,
            first: down(field(FIRST)?.as_ref())?,
            second: down(field(SECOND)?.as_ref())?,
            ploidy: down(field(PLOIDY)?.as_ref())?,
            phased: down(field(PHASED)?.as_ref())?,
        })
    }
    pub fn get(&self, row: usize) -> Result<Option<Call>, ExprError> {
        if self.parent.is_null(row) {
            return Ok(None);
        }
        Ok(Some(Call::from_storage(
            self.first.value(row),
            self.second.value(row),
            self.ploidy.value(row),
            self.phased.value(row),
        )))
    }
}

pub(crate) fn access(value: &ArrayRef, accessor: CallAccessor) -> Result<ArrayRef, ExprError> {
    let rows = CallRows::new(value)?;
    let nulls = rows.parent.nulls().cloned();
    let a = rows.first.values();
    let b = rows.second.values();
    let n = rows.ploidy.values();
    use CallProperty::*;
    Ok(match accessor {
        CallAccessor::Phased => Arc::new(BooleanArray::new(rows.phased.values().clone(), nulls)),
        CallAccessor::Alleles => {
            let mut out = ListBuilder::with_capacity(
                Int32Builder::with_capacity(value.len().saturating_mul(2)),
                value.len(),
            );
            for i in 0..value.len() {
                if rows.parent.is_null(i) {
                    out.append(false);
                    continue;
                }
                if n[i] > 0 {
                    out.values().append_value(i32::from(a[i]));
                }
                if n[i] == 2 {
                    out.values().append_value(i32::from(b[i]));
                }
                out.append(true);
            }
            Arc::new(out.finish())
        }
        CallAccessor::Property(Ploidy) => Arc::new(Int32Array::new(
            n.iter().map(|&x| i32::from(x)).collect(),
            nulls,
        )),
        CallAccessor::Property(NAltAlleles) => Arc::new(Int32Array::new(
            (0..value.len())
                .map(|i| i32::from(a[i] > 0) + i32::from(b[i] > 0))
                .collect(),
            nulls,
        )),
        CallAccessor::Property(UnphasedDiploidGtIndex) => {
            let mut out = Int32Builder::with_capacity(value.len());
            for i in 0..value.len() {
                if rows.parent.is_null(i) {
                    out.append_null();
                    continue;
                }
                if n[i] != 2 {
                    return Err(ExprError::Invalid("unphased_diploid_gt_index only supports ploidy == 2".into()));
                }
                let high = i64::from(a[i].max(b[i]));
                let low = i64::from(a[i].min(b[i]));
                // Full UInt16 alleles require a widened intermediate. The
                // public result is int32, with explicit final wrapping.
                out.append_value((high * (high + 1) / 2 + low) as i32);
            }
            Arc::new(out.finish())
        }
        CallAccessor::Property(IsHaploid) => boolean(value.len(), nulls, |i| n[i] == 1),
        CallAccessor::Property(IsDiploid) => boolean(value.len(), nulls, |i| n[i] == 2),
        CallAccessor::Property(IsHomRef) => boolean(value.len(), nulls, |i| {
            (n[i] > 0) & (a[i] == 0) & (b[i] == 0)
        }),
        CallAccessor::Property(IsNonRef) => {
            boolean(value.len(), nulls, |i| (a[i] > 0) | (b[i] > 0))
        }
        CallAccessor::Property(IsHet) => {
            boolean(value.len(), nulls, |i| (n[i] == 2) & (a[i] != b[i]))
        }
        CallAccessor::Property(IsHetRef) => boolean(value.len(), nulls, |i| {
            (n[i] == 2) & (a[i] != b[i]) & ((a[i] == 0) | (b[i] == 0))
        }),
        CallAccessor::Property(IsHetNonRef) => boolean(value.len(), nulls, |i| {
            (n[i] == 2) & (a[i] != b[i]) & (a[i] > 0) & (b[i] > 0)
        }),
        CallAccessor::Property(IsHomVar) => boolean(value.len(), nulls, |i| {
            (n[i] > 0) & (a[i] > 0) & ((n[i] == 1) | (a[i] == b[i]))
        }),
    })
}

// Select the index strategy during lowering; function addresses are not part of
// semantic expression identity. Actual validity and ploidy remain data-dependent.
#[derive(Debug, Clone)]
pub(crate) struct PreparedIndex {
    constant: Option<i32>,
    kernel: fn(&[ArrayRef], Option<i32>) -> Result<ArrayRef, ExprError>,
}
impl PartialEq for PreparedIndex {
    fn eq(&self, other: &Self) -> bool { self.constant == other.constant }
}
impl Eq for PreparedIndex {}
impl std::hash::Hash for PreparedIndex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.constant, state);
    }
}
impl PreparedIndex {
    pub(crate) fn new(constant: Option<i32>) -> Self {
        let kernel: fn(&[ArrayRef], Option<i32>) -> Result<ArrayRef, ExprError> = match constant {
            Some(0) => call_index::<0, false>,
            Some(1) => call_index::<1, false>,
            Some(-1) => call_index::<-1, false>,
            Some(-2) => call_index::<-2, false>,
            Some(_) => invalid_call_index,
            None => call_index::<0, true>,
        };
        Self { constant, kernel }
    }
    pub(crate) fn is_constant(&self) -> bool { self.constant.is_some() }
    pub(crate) fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExprError> {
        (self.kernel)(args, self.constant)
    }
}

// Every defined supported/unchecked Call exposes at most two alleles. An
// impossible constant index needs only the first defined row for its error;
// missing Calls still yield missing without raising an out-of-bounds error.
fn invalid_call_index(args: &[ArrayRef], requested: Option<i32>) -> Result<ArrayRef, ExprError> {
    let rows = CallRows::new(&args[0])?;
    if let Some(row) = (0..rows.parent.len()).find(|&row| rows.parent.is_valid(row)) {
        let n = rows.ploidy.value(row);
        let len = if n == 2 { 2 } else { usize::from(n > 0) };
        return Err(ExprError::Invalid(format!(
            "array index out of bounds: index={}, length={len}", requested.unwrap()
        )));
    }
    Ok(arrow_array::new_null_array(&DataType::Int32, rows.parent.len()))
}

fn call_index<const INDEX: i32, const DYNAMIC: bool>(args: &[ArrayRef], _: Option<i32>) -> Result<ArrayRef, ExprError> {
    let rows = CallRows::new(&args[0])?;
    let indices = if DYNAMIC { Some(down::<Int32Array>(args[1].as_ref())?) } else { None };
    let nulls = if let Some(indices) = indices {
        NullBuffer::union(rows.parent.logical_nulls().as_ref(), indices.logical_nulls().as_ref())
    } else { rows.parent.logical_nulls() };
    // Only actual bitmap presence is bound per batch. The nullable check is
    // compiled out of the dense loop; index/configuration selection is per query.
    if nulls.as_ref().is_some_and(|n| n.null_count() != 0) {
        call_index_values::<INDEX, DYNAMIC, true>(&rows, indices, nulls)
    } else {
        call_index_values::<INDEX, DYNAMIC, false>(&rows, indices, None)
    }
}

fn call_index_values<const INDEX: i32, const DYNAMIC: bool, const NULLABLE: bool>(
    rows: &CallRows<'_>, indices: Option<&Int32Array>, nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ExprError> {
    let a = rows.first.values();
    let b = rows.second.values();
    let n = rows.ploidy.values();
    let mut values = Vec::with_capacity(rows.parent.len());
    for row in 0..rows.parent.len() {
        if NULLABLE && nulls.as_ref().unwrap().is_null(row) {
            values.push(0);
            continue;
        }
        let requested = if DYNAMIC { indices.unwrap().value(row) } else { INDEX };
        // Match the existing allele-list semantics even for unchecked storage:
        // ploidy > 2 exposes just its first allele, rather than reading beyond it.
        let len = if n[row] == 2 { 2_i64 } else { i64::from(n[row] > 0) };
        let normalized = if requested < 0 { len + i64::from(requested) } else { i64::from(requested) };
        if normalized < 0 || normalized >= len {
            return Err(ExprError::Invalid(format!(
                "array index out of bounds: index={requested}, length={len}"
            )));
        }
        values.push(i32::from(if normalized == 0 { a[row] } else { b[row] }));
    }
    Ok(Arc::new(Int32Array::new(values.into(), nulls)))
}

fn boolean(len: usize, nulls: Option<NullBuffer>, f: impl FnMut(usize) -> bool) -> ArrayRef {
    Arc::new(BooleanArray::new(
        BooleanBuffer::collect_bool(len, f),
        nulls,
    ))
}

pub(crate) fn literal_call(value: &serde_json::Value) -> Result<Call, ExprError> {
    let integer = |name: &str| {
        value[name]
            .as_u64()
            .ok_or_else(|| ExprError::Invalid(format!("Call {name} must be present and unsigned")))
    };
    let allele = |name| {
        u16::try_from(integer(name)?)
            .map_err(|_| ExprError::Invalid("Call allele index must be between 0 and 65535".into()))
    };
    Call::from_fields(
        allele(FIRST)?,
        allele(SECOND)?,
        u8::try_from(integer(PLOIDY)?)
            .map_err(|_| ExprError::Invalid("invalid Call ploidy".into()))?,
        value[PHASED]
            .as_bool()
            .ok_or_else(|| ExprError::Invalid("Call phase must be present".into()))?,
    )
    .map_err(ExprError::Invalid)
}
pub(crate) fn literal_array(
    value: &serde_json::Value,
    dtype: &DataType,
    len: usize,
) -> Result<ArrayRef, ExprError> {
    let c = literal_call(value)?;
    make_array(
        dtype,
        vec![c.first(); len],
        vec![c.second(); len],
        vec![c.ploidy(); len],
        vec![c.phased(); len],
        None,
    )
}
fn make_array(
    dtype: &DataType,
    a: Vec<u16>,
    b: Vec<u16>,
    n: Vec<u8>,
    p: Vec<bool>,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ExprError> {
    let DataType::Struct(fields) = dtype else {
        return Err(ExprError::Invalid("Call storage must be a struct".into()));
    };
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(UInt16Array::from(a)),
            Arc::new(UInt16Array::from(b)),
            Arc::new(UInt8Array::from(n)),
            Arc::new(BooleanArray::from(p)),
        ],
        nulls,
    )?))
}
pub(crate) fn construct(value: &ArrayRef, dtype: &DataType) -> Result<ArrayRef, ExprError> {
    let parent = down::<StructArray>(value.as_ref())?;
    let field = |name| {
        parent
            .column_by_name(name)
            .ok_or_else(|| ExprError::Invalid(format!("Call input field {name} absent")))
    };
    let alleles = down::<ListArray>(field("__vail_call_alleles")?.as_ref())?;
    let phase = down::<BooleanArray>(field(PHASED)?.as_ref())?;
    let values = down::<Int32Array>(alleles.values().as_ref())?;
    let mut first = Vec::with_capacity(value.len());
    let mut second = Vec::with_capacity(value.len());
    let mut ploidy = Vec::with_capacity(value.len());
    let mut phased = Vec::with_capacity(value.len());
    let mut valid = Vec::with_capacity(value.len());
    for row in 0..value.len() {
        let start = alleles.value_offsets()[row] as usize;
        let end = alleles.value_offsets()[row + 1] as usize;
        let present = !parent.is_null(row)
            && !alleles.is_null(row)
            && !phase.is_null(row)
            && !(start..end).any(|i| values.is_null(i));
        let c = if present {
            Call::new(&values.values()[start..end], phase.value(row)).map_err(ExprError::Invalid)?
        } else {
            Call::new(&[], false).unwrap()
        };
        first.push(c.first());
        second.push(c.second());
        ploidy.push(c.ploidy());
        phased.push(c.phased());
        valid.push(present);
    }
    make_array(
        dtype,
        first,
        second,
        ploidy,
        phased,
        Some(NullBuffer::from(valid)),
    )
}
