//! Primitive list reductions through the public expression boundary.
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch,
};
use arrow_schema::{DataType, Schema};
use datafusion_common::Result;
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType, Type, lower};

fn reduce(lists: &ListArray, function: &str) -> Result<Option<ArrayRef>> {
    let input = match lists.value_type() {
        DataType::Int32 => PrimitiveType::Int32,
        DataType::Int64 => PrimitiveType::Int64,
        DataType::Float32 => PrimitiveType::Float32,
        DataType::Float64 => PrimitiveType::Float64,
        DataType::Boolean => PrimitiveType::Bool,
        _ => unreachable!(),
    };
    let output = match function {
        "any" | "all" => PrimitiveType::Bool,
        "count_where" => PrimitiveType::Int64,
        "mean" | "fraction" => PrimitiveType::Float64,
        "sum" | "product" => match input {
            PrimitiveType::Int32 | PrimitiveType::Int64 => PrimitiveType::Int64,
            _ => PrimitiveType::Float64,
        },
        _ => input,
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "values",
        lists.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(lists.clone())])?;
    let expr = Expr::NestedFunction {
        function: format!("__vail_list_reduce_{function}"),
        args: vec![Expr::Column {
            name: "values".into(),
        }],
        dtype: Type::Primitive(output),
    };
    Ok(Some(
        lower(&expr, &schema)
            .unwrap()
            .evaluate(&batch)?
            .into_array(lists.len())?,
    ))
}

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

#[test]
fn sum_and_product_keep_integer_wrapping_semantics() {
    let lists =
        ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(i64::MAX), Some(2)])]);
    assert_eq!(
        reduce(&lists, "sum")
            .unwrap()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN + 1
    );
    assert_eq!(
        reduce(&lists, "product")
            .unwrap()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -2
    );
}

#[test]
fn float_extrema_match_native_datafusion_on_nan_signed_zero_and_infinities() {
    use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
    let values = Arc::new(Float64Array::from(vec![
        Some(f64::NAN),
        Some(-0.0),
        None,
        Some(0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ])) as ArrayRef;
    let lists = ListArray::new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        OffsetBuffer::from_lengths([values.len()]),
        values.clone(),
        None,
    );
    for function in ["min", "max"] {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let udf = if function == "min" {
            datafusion_functions_aggregate::min_max::min_udaf()
        } else {
            datafusion_functions_aggregate::min_max::max_udaf()
        };
        let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("x", 0))])
            .schema(schema)
            .alias("native")
            .build()
            .unwrap();
        let mut accumulator = aggregate.create_accumulator().unwrap();
        accumulator.update_batch(&[values.clone()]).unwrap();
        let native = accumulator.evaluate().unwrap();
        let direct = reduce(&lists, function).unwrap().unwrap();
        let actual = datafusion_common::ScalarValue::try_from_array(&direct, 0).unwrap();
        match (native, actual) {
            (
                datafusion_common::ScalarValue::Float64(Some(a)),
                datafusion_common::ScalarValue::Float64(Some(b)),
            ) => assert_eq!(a.to_bits(), b.to_bits()),
            other => panic!("unexpected extrema {other:?}"),
        }
    }
}
