//! Checked Arrow adapters for packed Calls. Each input expression is evaluated
//! once by the physical caller; each present row is packed or decoded once.
use crate::expressions::ExprError;
use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, StructArray};
use arrow_schema::DataType;
use std::sync::Arc;
use vail_ordering::call::{DecodedCall, decode};
pub use vail_ordering::call::{pack, unpack};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CallAccessor {
    Alleles,
    Phased,
    Word,
    Property(CallProperty),
}

impl CallAccessor {
    pub(crate) fn for_field(name: &str) -> Option<Self> {
        match name {
            "__vail_call_alleles" => Some(Self::Alleles),
            "__vail_call_phased" => Some(Self::Phased),
            "__vail_call_word" => Some(Self::Word),
            _ => CallProperty::for_name(name).map(Self::Property),
        }
    }
}

/// One output buffer per batch; no decoded allele arrays or per-Call allocation.
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
impl CallProperty {
    fn for_name(name: &str) -> Option<Self> {
        Some(match name {
            "__vail_call_ploidy" => Self::Ploidy,
            "__vail_call_n_alt_alleles" => Self::NAltAlleles,
            "__vail_call_is_haploid" => Self::IsHaploid,
            "__vail_call_is_diploid" => Self::IsDiploid,
            "__vail_call_is_hom_ref" => Self::IsHomRef,
            "__vail_call_is_non_ref" => Self::IsNonRef,
            "__vail_call_is_het" => Self::IsHet,
            "__vail_call_is_het_ref" => Self::IsHetRef,
            "__vail_call_is_het_non_ref" => Self::IsHetNonRef,
            "__vail_call_is_hom_var" => Self::IsHomVar,
            _ => return None,
        })
    }
    pub(crate) fn is_integer(self) -> bool {
        matches!(self, Self::Ploidy | Self::NAltAlleles)
    }
    fn flag(self) -> u16 {
        match self {
            Self::IsHaploid => 1 << 4,
            Self::IsDiploid => 1 << 5,
            Self::IsHomRef => 1 << 6,
            Self::IsNonRef => 1 << 7,
            Self::IsHet => 1 << 8,
            Self::IsHetRef => 1 << 9,
            Self::IsHetNonRef => 1 << 10,
            Self::IsHomVar => 1 << 11,
            _ => unreachable!(),
        }
    }
}
impl CallAccessor {
    pub(crate) fn result_type(self) -> crate::expressions::Type {
        use crate::expressions::{PrimitiveType, Type};
        match self {
            Self::Alleles => Type::Array {
                array: Box::new(Type::Primitive(PrimitiveType::Int32)),
            },
            Self::Word => Type::Primitive(PrimitiveType::Int32),
            Self::Property(p) if p.is_integer() => Type::Primitive(PrimitiveType::Int32),
            _ => Type::Primitive(PrimitiveType::Bool),
        }
    }
}

// 512 bytes, shared across queries. Covers both phase states and every ploidy
// for common small alleles, without a square root or allele reconstruction.
static SMALL_PROPERTIES: std::sync::LazyLock<[u16; 256]> =
    std::sync::LazyLock::new(|| std::array::from_fn(|word| property_bits(word as u32)));

fn property_bits(word: u32) -> u16 {
    let ploidy = (word >> 1) & 3;
    let repr = word >> 3;
    let (first, second) = if ploidy == 2 {
        let k = ((8 * u64::from(repr) + 1).isqrt() - 1) / 2;
        let j = u64::from(repr) - k * (k + 1) / 2;
        (
            j as u32,
            if word & 1 != 0 {
                (k - j) as u32
            } else {
                k as u32
            },
        )
    } else {
        (repr, 0)
    };
    let het = (ploidy == 2) & (first != second);
    let non_ref = (ploidy > 0) & (repr > 0);
    let n_alt = u16::from((ploidy > 0) & (first > 0)) + u16::from((ploidy == 2) & (second > 0));
    ploidy as u16
        | (n_alt << 2)
        | (u16::from(ploidy == 1) << 4)
        | (u16::from(ploidy == 2) << 5)
        | (u16::from((ploidy > 0) & (repr == 0)) << 6)
        | (u16::from(non_ref) << 7)
        | (u16::from(het) << 8)
        | (u16::from(het & ((first == 0) | (second == 0))) << 9)
        | (u16::from(het & (first > 0) & (second > 0)) << 10)
        | (u16::from(non_ref & !het) << 11)
}

fn properties(word: i32, small: &[u16; 256]) -> u16 {
    small
        .get(word as u32 as usize)
        .copied()
        .unwrap_or_else(|| property_bits(word as u32))
}

fn down<T: Array + 'static>(array: &dyn Array) -> Result<&T, ExprError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ExprError::Invalid("invalid Call Arrow storage".into()))
}

/// A row adapter that applies parent validity before checking the packed word.
/// Successful present rows are canonical according to the shared Call codec.
pub(crate) struct CallRows<'a> {
    parent: &'a StructArray,
    words: &'a Int32Array,
}
impl<'a> CallRows<'a> {
    pub(crate) fn new(value: &'a ArrayRef) -> Result<Self, ExprError> {
        let parent = down::<StructArray>(value.as_ref())?;
        Self::from_struct(parent)
    }
    pub(crate) fn from_struct(parent: &'a StructArray) -> Result<Self, ExprError> {
        let words = parent
            .column_by_name("__vail_call_word")
            .ok_or_else(|| ExprError::Invalid("Call word is absent".into()))?;
        Ok(Self {
            parent,
            words: down(words.as_ref())?,
        })
    }
    fn validate(&self) -> Result<(), ExprError> {
        for (row, &word) in self.words.values().iter().enumerate() {
            if self.parent.is_null(row) {
                continue;
            }
            if self.words.is_null(row) {
                return Err(ExprError::Invalid("present Call has missing word".into()));
            }
            let word = word as u32;
            let ploidy = (word >> 1) & 3;
            // All 29-bit triangular payloads are canonical for diploids;
            // haploids use the payload directly. Only empty/ploidy-3 are invalid.
            if ploidy == 3 || (ploidy == 0 && word >> 3 != 0) {
                return Err(ExprError::Invalid(
                    "invalid packed Call ploidy or allele representation".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn get(&self, row: usize) -> Result<Option<DecodedCall>, ExprError> {
        if self.parent.is_null(row) {
            return Ok(None);
        }
        if self.words.is_null(row) {
            return Err(ExprError::Invalid("present Call has missing word".into()));
        }
        decode(self.words.value(row))
            .map(Some)
            .map_err(ExprError::Invalid)
    }
}

pub(crate) fn access(value: &ArrayRef, accessor: CallAccessor) -> Result<ArrayRef, ExprError> {
    let rows = CallRows::new(value)?;
    match accessor {
        CallAccessor::Alleles => {
            let mut out = ListBuilder::with_capacity(
                Int32Builder::with_capacity(value.len().saturating_mul(2)),
                value.len(),
            );
            for row in 0..value.len() {
                match rows.get(row)? {
                    Some(call) => {
                        out.values().append_slice(call.alleles());
                        out.append(true);
                    }
                    None => out.append(false),
                }
            }
            Ok(Arc::new(out.finish()))
        }
        CallAccessor::Phased | CallAccessor::Word | CallAccessor::Property(_) => {
            rows.validate()?;
            let nulls = arrow_buffer::NullBuffer::union(rows.words.nulls(), rows.parent.nulls());
            let words = rows.words.values();
            let small = &*SMALL_PROPERTIES;
            match accessor {
                CallAccessor::Word => Ok(Arc::new(Int32Array::new(words.clone(), nulls))),
                CallAccessor::Phased => Ok(Arc::new(BooleanArray::new(
                    arrow_buffer::BooleanBuffer::collect_bool(words.len(), |i| words[i] & 1 != 0),
                    nulls,
                ))),
                CallAccessor::Property(CallProperty::Ploidy) => Ok(Arc::new(Int32Array::new(
                    words
                        .iter()
                        .map(|w| ((*w as u32 >> 1) & 3) as i32)
                        .collect(),
                    nulls,
                ))),
                CallAccessor::Property(CallProperty::NAltAlleles) => Ok(Arc::new(Int32Array::new(
                    words
                        .iter()
                        .map(|w| ((properties(*w, small) >> 2) & 3) as i32)
                        .collect(),
                    nulls,
                ))),
                CallAccessor::Property(property) => {
                    let flag = property.flag();
                    Ok(Arc::new(BooleanArray::new(
                        arrow_buffer::BooleanBuffer::collect_bool(words.len(), |i| {
                            properties(words[i], small) & flag != 0
                        }),
                        nulls,
                    )))
                }
                _ => unreachable!(),
            }
        }
    }
}

pub(crate) fn construct(value: &ArrayRef, dtype: &DataType) -> Result<ArrayRef, ExprError> {
    let parent = down::<StructArray>(value.as_ref())?;
    let field = |name: &str| {
        parent
            .column_by_name(name)
            .ok_or_else(|| ExprError::Invalid(format!("Call field {name:?} is absent")))
    };
    let alleles = down::<ListArray>(field("__vail_call_alleles")?.as_ref())?;
    let phased = down::<BooleanArray>(field("__vail_call_phased")?.as_ref())?;
    let values = down::<Int32Array>(alleles.values().as_ref())?;
    let mut words = Vec::with_capacity(value.len());
    for row in 0..value.len() {
        if parent.is_null(row) || alleles.is_null(row) || phased.is_null(row) {
            words.push(None);
            continue;
        }
        let start = alleles.value_offsets()[row] as usize;
        let end = alleles.value_offsets()[row + 1] as usize;
        if (start..end).any(|index| values.is_null(index)) {
            words.push(None);
            continue;
        }
        words.push(Some(
            pack(&values.values()[start..end], phased.value(row)).map_err(ExprError::Invalid)?,
        ));
    }
    let words = Int32Array::from(words);
    let DataType::Struct(fields) = dtype else {
        return Err(ExprError::Invalid(
            "Call output requires struct storage".into(),
        ));
    };
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![Arc::new(words.clone())],
        words.nulls().cloned(),
    )?))
}
