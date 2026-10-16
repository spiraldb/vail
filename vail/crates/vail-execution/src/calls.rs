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
            Self::Property(CallProperty::Ploidy | CallProperty::NAltAlleles) => {
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
        if self.first.is_null(row)
            || self.second.is_null(row)
            || self.ploidy.is_null(row)
            || self.phased.is_null(row)
        {
            return Err(ExprError::Invalid("present Call has missing fields".into()));
        }
        Call::from_fields(
            self.first.value(row),
            self.second.value(row),
            self.ploidy.value(row),
            self.phased.value(row),
        )
        .map(Some)
        .map_err(ExprError::Invalid)
    }
    fn validate(&self) -> Result<(), ExprError> {
        for row in 0..self.parent.len() {
            self.get(row)?;
        }
        Ok(())
    }
}

pub(crate) fn access(value: &ArrayRef, accessor: CallAccessor) -> Result<ArrayRef, ExprError> {
    let rows = CallRows::new(value)?;
    rows.validate()?;
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
