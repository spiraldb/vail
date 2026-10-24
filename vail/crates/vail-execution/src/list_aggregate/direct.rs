//! Primitive segmented reductions using Arrow batch kernels and one output builder.
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
};
use arrow_schema::DataType;
use datafusion_common::{Result, exec_datafusion_err};

type Evaluate = fn(&ListArray) -> Result<ArrayRef>;

/// Immutable descriptor selected by DataFusion when the argument type is known.
#[derive(Debug, Clone)]
pub(super) struct Prepared {
    identity: (DataType, String),
    evaluate: Evaluate,
}
impl PartialEq for Prepared {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}
impl Eq for Prepared {}
impl std::hash::Hash for Prepared {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.identity, state);
    }
}
impl Prepared {
    pub(super) fn new(input: &DataType, function: &str) -> Option<Self> {
        let DataType::List(field) = input else {
            return None;
        };
        macro_rules! choose {
            ($kernel:ident) => {
                match function {
                    "sum" => $kernel::<0> as Evaluate,
                    "product" => $kernel::<1> as Evaluate,
                    "min" => $kernel::<2> as Evaluate,
                    "max" => $kernel::<3> as Evaluate,
                    _ => return None,
                }
            };
        }
        let evaluate = match field.data_type() {
            DataType::Int32 if matches!(function, "sum" | "product") => choose!(int32_wide),
            DataType::Int32 => choose!(int32),
            DataType::Int64 => choose!(int64),
            _ => return None,
        };
        Some(Self {
            identity: (input.clone(), function.to_owned()),
            evaluate,
        })
    }
    pub(super) fn evaluate(&self, lists: &ListArray) -> Result<ArrayRef> {
        (self.evaluate)(lists)
    }
}

// OP is chosen once per query. Nullable/dense traversal is chosen once per
// batch from logical validity. Borrowed spans require no per-list Arrow header,
// refcount changes, bitmap slicing, or null-count computation. Integer wrapping
// matches Arrow and the dense loop leaves vectorization to LLVM.
macro_rules! integer_kernel {
    ($name:ident, $typed:ty, $builder:ty, $native:ty) => {
        fn $name<const OP: u8>(lists: &ListArray) -> Result<ArrayRef> {
            let values = lists
                .values()
                .as_any()
                .downcast_ref::<$typed>()
                .ok_or_else(|| exec_datafusion_err!("prepared list reducer input type changed"))?;
            let logical_nulls = values.logical_nulls();
            let nulls = logical_nulls.as_ref().filter(|n| n.null_count() != 0);
            fn run<const OP: u8, const NULLS: bool>(
                lists: &ListArray,
                values: &$typed,
                nulls: Option<&arrow_buffer::NullBuffer>,
            ) -> ArrayRef {
                let offsets = lists.value_offsets();
                let raw = values.values();
                let mut output = <$builder>::with_capacity(lists.len());
                for row in 0..lists.len() {
                    if lists.is_null(row) {
                        output.append_null();
                        continue;
                    }
                    let start = offsets[row] as usize;
                    let end = offsets[row + 1] as usize;
                    let mut value: $native = if OP == 1 { 1 } else { 0 };
                    let mut present = OP < 2;
                    for (relative, source) in raw[start..end].iter().enumerate() {
                        if NULLS && !nulls.unwrap().is_valid(start + relative) {
                            continue;
                        }
                        let next = *source as $native;
                        if OP == 0 {
                            value = value.wrapping_add(next);
                        } else if OP == 1 {
                            value = value.wrapping_mul(next);
                        } else if !present || (OP == 2 && next < value) || (OP == 3 && next > value)
                        {
                            value = next;
                        }
                        present = true;
                    }
                    if present {
                        output.append_value(value);
                    } else {
                        output.append_null();
                    }
                }
                Arc::new(output.finish())
            }
            Ok(if nulls.is_some() {
                run::<OP, true>(lists, values, nulls)
            } else {
                run::<OP, false>(lists, values, None)
            })
        }
    };
}
integer_kernel!(int32_wide, Int32Array, Int64Builder, i64);
integer_kernel!(int32, Int32Array, Int32Builder, i32);
integer_kernel!(int64, Int64Array, Int64Builder, i64);

/// Return None for reducers that still need the general accumulator adapter.
/// Child values are cast once for the batch, never once per list. Sliced list
/// offsets are rebased onto the referenced child range before reduction.
pub(super) fn reduce(lists: &ListArray, function: &str) -> Result<Option<ArrayRef>> {
    let offsets = lists.value_offsets();
    let base = offsets[0] as usize;
    let end = offsets[lists.len()] as usize;
    let source = lists.values().slice(base, end - base);
    let input = source.data_type();
    let boolean = matches!(function, "any" | "all" | "fraction" | "count_where");
    let numeric = matches!(function, "sum" | "product" | "mean" | "min" | "max");
    if !boolean && !numeric {
        return Ok(None);
    }
    if boolean {
        let values = source
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| exec_datafusion_err!("{function} requires Boolean input"))?;
        macro_rules! boolean {
            ($builder:ty, $value:expr) => {{
                let mut output = <$builder>::with_capacity(lists.len());
                for row in 0..lists.len() {
                    if lists.is_null(row) {
                        output.append_null();
                        continue;
                    }
                    let start = offsets[row] as usize - base;
                    let len = (offsets[row + 1] - offsets[row]) as usize;
                    let values = values.slice(start, len);
                    output.append_option(($value)(&values));
                }
                Arc::new(output.finish()) as ArrayRef
            }};
        }
        return Ok(Some(match function {
            "any" => boolean!(BooleanBuilder, |v: &BooleanArray| Some(v.true_count() != 0)),
            "all" => boolean!(BooleanBuilder, |v: &BooleanArray| Some(
                v.true_count() == v.len() - v.null_count()
            )),
            "count_where" => boolean!(Int64Builder, |v: &BooleanArray| Some(v.true_count() as i64)),
            _ => boolean!(Float64Builder, |v: &BooleanArray| (!v.is_empty())
                .then(|| v.true_count() as f64 / v.len() as f64)),
        }));
    }

    if !matches!(
        input,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    ) {
        return Ok(None);
    }
    let output_type = if function == "mean" {
        DataType::Float64
    } else if matches!(function, "min" | "max") {
        input.clone()
    } else if matches!(input, DataType::Int32 | DataType::Int64) {
        DataType::Int64
    } else {
        DataType::Float64
    };
    let source = arrow_cast::cast(&source, &output_type)?;
    macro_rules! numeric {
        ($array:ty, $builder:ty, $reduce:expr) => {{
            let values = source.as_any().downcast_ref::<$array>().unwrap();
            let mut output = <$builder>::with_capacity(lists.len());
            for row in 0..lists.len() {
                if lists.is_null(row) {
                    output.append_null();
                    continue;
                }
                let start = offsets[row] as usize - base;
                let len = (offsets[row + 1] - offsets[row]) as usize;
                let values = values.slice(start, len);
                let value = ($reduce)(&values);
                output.append_option(value);
            }
            Arc::new(output.finish()) as ArrayRef
        }};
    }
    let result = if function == "mean" {
        let values = source.as_any().downcast_ref::<Float64Array>().unwrap();
        let mut output = Float64Builder::with_capacity(lists.len());
        for row in 0..lists.len() {
            if lists.is_null(row) {
                output.append_null();
                continue;
            }
            let start = offsets[row] as usize - base;
            let len = (offsets[row + 1] - offsets[row]) as usize;
            let values = values.slice(start, len);
            let count = len - values.null_count();
            output.append_value(arrow_arith::aggregate::sum(&values).unwrap_or(0.0) / count as f64);
        }
        Arc::new(output.finish()) as ArrayRef
    } else {
        macro_rules! dispatch {
            ($array:ty, $builder:ty, $zero:expr, $one:expr) => {
                match function {
                    "sum" => numeric!($array, $builder, |v: &$array| Some(
                        arrow_arith::aggregate::sum(v).unwrap_or($zero)
                    )),
                    "product" => numeric!($array, $builder, |v: &$array| Some(
                        arrow_arith::aggregate::product(v).unwrap_or($one)
                    )),
                    "min" => numeric!($array, $builder, arrow_arith::aggregate::min),
                    "max" => numeric!($array, $builder, arrow_arith::aggregate::max),
                    _ => unreachable!(),
                }
            };
        }
        match output_type {
            DataType::Int32 => dispatch!(Int32Array, Int32Builder, 0, 1),
            DataType::Int64 => dispatch!(Int64Array, Int64Builder, 0, 1),
            DataType::Float32 => dispatch!(Float32Array, Float32Builder, 0.0, 1.0),
            DataType::Float64 => dispatch!(Float64Array, Float64Builder, 0.0, 1.0),
            _ => unreachable!(),
        }
    };
    Ok(Some(result))
}
