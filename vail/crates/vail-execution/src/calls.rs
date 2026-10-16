//! Checked Arrow adapters for packed Calls. Each input expression is evaluated
//! once by the physical caller; each present row is packed or decoded once.
use crate::expressions::ExprError;
use arrow_array::builder::{BooleanBuilder, Int32Builder, ListBuilder};
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, StructArray};
use arrow_schema::DataType;
use std::sync::Arc;
pub use vail_ordering::call::{pack, unpack};
use vail_ordering::call::{DecodedCall, decode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CallAccessor {
    Alleles,
    Phased,
    Word,
}

impl CallAccessor {
    pub(crate) fn for_field(name: &str) -> Option<Self> {
        match name {
            "__vail_call_alleles" => Some(Self::Alleles),
            "__vail_call_phased" => Some(Self::Phased),
            "__vail_call_word" => Some(Self::Word),
            _ => None,
        }
    }
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
                Int32Builder::with_capacity(value.len().saturating_mul(2)), value.len());
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
        CallAccessor::Phased => {
            let mut values = BooleanBuilder::with_capacity(value.len());
            for row in 0..value.len() {
                values.append_option(rows.get(row)?.map(|call| call.phased()));
            }
            Ok(Arc::new(values.finish()))
        }
        CallAccessor::Word => {
            for row in 0..value.len() {
                rows.get(row)?;
            }
            Ok(Arc::new(Int32Array::new(
                rows.words.values().clone(),
                arrow_buffer::NullBuffer::union(rows.words.nulls(), rows.parent.nulls()),
            )))
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
