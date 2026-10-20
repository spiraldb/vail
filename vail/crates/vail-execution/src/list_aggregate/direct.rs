//! Primitive segmented reductions using Arrow batch kernels and one output builder.
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, ListArray,
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
        ($array:ty, $builder:ty, $zero:expr, $one:expr) => {{
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
                let value = match function {
                    "sum" => Some(arrow_arith::aggregate::sum(&values).unwrap_or($zero)),
                    "product" => Some(arrow_arith::aggregate::product(&values).unwrap_or($one)),
                    "min" => arrow_arith::aggregate::min(&values),
                    "max" => arrow_arith::aggregate::max(&values),
                    _ => unreachable!("mean uses the Float64 path"),
                };
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
        match output_type {
            DataType::Int32 => numeric!(Int32Array, Int32Builder, 0, 1),
            DataType::Int64 => numeric!(Int64Array, Int64Builder, 0, 1),
            DataType::Float32 => numeric!(Float32Array, Float32Builder, 0.0, 1.0),
            DataType::Float64 => numeric!(Float64Array, Float64Builder, 0.0, 1.0),
            _ => unreachable!(),
        }
    };
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::{Float32Type, Int32Type, Int64Type};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::Field;

    #[test]
    fn integer_lists_keep_precision_missingness_and_sliced_offsets() {
        let values = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(99)]),
            Some(vec![Some(9_007_199_254_740_993), None, Some(1)]),
            Some(vec![]),
            Some(vec![None]),
            None,
        ])
        .slice(1, 4);
        let sum = reduce(&values, "sum").unwrap().unwrap();
        assert_eq!(
            sum.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![Some(9_007_199_254_740_994), Some(0), Some(0), None])
        );
        let product = reduce(&values, "product").unwrap().unwrap();
        assert_eq!(
            product.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![Some(9_007_199_254_740_993), Some(1), Some(1), None])
        );
    }

    #[test]
    fn extrema_preserve_narrow_types_and_empty_nulls() {
        let ints = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(4), None, Some(-2)]),
            Some(vec![]),
            None,
        ]);
        assert_eq!(
            reduce(&ints, "min")
                .unwrap()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(-2), None, None])
        );
        let floats = ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some(vec![Some(f32::NEG_INFINITY)]),
            Some(vec![Some(f32::INFINITY)]),
        ]);
        for op in ["min", "max"] {
            assert_eq!(
                reduce(&floats, op)
                    .unwrap()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap(),
                &Float32Array::from(vec![f32::NEG_INFINITY, f32::INFINITY])
            );
        }
    }

    #[test]
    fn boolean_lists_ignore_missing_payloads_and_preserve_identities() {
        let child = Arc::new(BooleanArray::new(
            vec![true, true, false, true].into(),
            Some(NullBuffer::from(vec![true, false, true, false])),
        )) as ArrayRef;
        let lists = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::from_lengths([1, 2, 0, 1, 0]),
            child,
            Some(NullBuffer::from(vec![true, true, true, true, false])),
        )
        .slice(1, 4);
        assert_eq!(
            reduce(&lists, "any")
                .unwrap()
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![Some(false), Some(false), Some(false), None])
        );
        assert_eq!(
            reduce(&lists, "all")
                .unwrap()
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(vec![Some(false), Some(true), Some(true), None])
        );
        assert_eq!(
            reduce(&lists, "count_where")
                .unwrap()
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![Some(0), Some(0), Some(0), None])
        );
        assert_eq!(
            reduce(&lists, "fraction")
                .unwrap()
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &Float64Array::from(vec![Some(0.0), None, Some(0.0), None])
        );
    }

    #[test]
    fn mean_widens_and_empty_is_nan_not_missing() {
        let lists = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None, Some(3)]),
            Some(vec![]),
            Some(vec![None]),
            None,
        ]);
        let mean = reduce(&lists, "mean").unwrap().unwrap();
        let mean = mean.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(mean.value(0), 2.0);
        assert!(mean.value(1).is_nan());
        assert!(mean.value(2).is_nan());
        assert!(mean.is_null(3));
    }
}
