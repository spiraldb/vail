//! Optional consumer endpoint owned by an exact registered native kernel.
use arrow_array::{ArrayRef, Int32Array, ListArray};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarUDF};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceDomain {
    Scalar,
    OriginalRow,
    OriginalEntry,
}

#[derive(Debug, Clone)]
pub struct Argument {
    pub value: ColumnarValue,
    pub domain: SourceDomain,
}

/// Parent and selector rows are in the selected outer-row domain. Argument
/// arrays retain their original domains; scalar arguments own one value.
/// `entry_validity` covers both cell and entry-value ancestor validity.
pub struct Request<'a> {
    pub arguments: &'a [Argument],
    pub entries: &'a ListArray,
    pub entry_validity: Option<&'a NullBuffer>,
    pub parents: &'a Int32Array,
    pub selectors: &'a Int32Array,
    /// Requested ordinals in the ordinary native Struct result.
    pub fields: u64,
    pub max_scratch_bytes: usize,
}

pub trait SelectedListProducer: Send + Sync {
    /// Flat-lane bundle type after selecting one element from each field list.
    /// Fields occur in ascending requested ordinal order and retain their names
    /// and metadata from the ordinary native Struct result.
    fn selected_type(&self, fields: u64) -> Option<DataType>;
    /// `None` requests the original eager route before native evaluation.
    fn evaluate_selected(&self, request: &Request<'_>) -> Result<Option<ArrayRef>>;
}

pub fn capability(function: &ScalarUDF) -> Option<&dyn SelectedListProducer> {
    crate::variant_kernels::selected_list_producer(function)
}
