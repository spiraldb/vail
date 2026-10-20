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
