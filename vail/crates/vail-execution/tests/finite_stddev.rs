//! Public aggregate state/merge/retract contracts across numeric input types.
use arrow_array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use datafusion_expr::EmitTo;
use datafusion_physical_expr::{
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
    expressions::Column,
};
use std::sync::Arc;

fn aggregate(dtype: &DataType) -> AggregateFunctionExpr {
    let udf = vail_execution::aggregates::aggregate_udf("stddev_pop", Some(dtype)).unwrap();
    AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("x", 0))])
        .schema(Arc::new(Schema::new(vec![Field::new(
            "x",
            dtype.clone(),
            true,
        )])))
        .alias("sd")
        .build()
        .unwrap()
}
fn value(result: ScalarValue) -> f64 {
    let ScalarValue::Float64(Some(x)) = result else {
        panic!("expected defined population deviation")
    };
    x
}
fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= 1e-12 * expected.abs().max(1.),
        "{actual} != {expected}"
    );
}

#[test]
fn integer_partial_merge_and_retraction_keep_native_population_math() {
    for dtype in [DataType::Int32, DataType::Int64] {
        let original: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(999),
            Some(1),
            None,
            Some(2),
            Some(4),
            Some(8),
            Some(999),
        ]));
        let input = arrow_cast::cast(&original.slice(1, 5), &dtype).unwrap();
        let expression = aggregate(&dtype);
        let mut first = expression.create_accumulator().unwrap();
        first.update_batch(&[input.slice(0, 3)]).unwrap();
        let states = first
            .state()
            .unwrap()
            .into_iter()
            .map(|x| x.to_array_of_size(1).unwrap())
            .collect::<Vec<_>>();
        let mut merged = expression.create_accumulator().unwrap();
        merged.merge_batch(&states).unwrap();
        merged.update_batch(&[input.slice(3, 2)]).unwrap();
        close(value(merged.evaluate().unwrap()), (7.1875f64).sqrt());
        assert!(merged.supports_retract_batch());
        merged.retract_batch(&[input.slice(0, 2)]).unwrap();
        close(value(merged.evaluate().unwrap()), (56f64 / 9.).sqrt());
        let mut empty = expression.create_accumulator().unwrap();
        assert!(value(empty.evaluate().unwrap()).is_nan());
    }
}

#[test]
fn integer_grouped_state_conversion_filter_and_partial_emission_match_updates() {
    for dtype in [DataType::Int32, DataType::Int64] {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(2),
            None,
            Some(4),
            Some(8),
            Some(12),
        ]));
        let input = arrow_cast::cast(&input, &dtype).unwrap();
        let expression = aggregate(&dtype);
        let groups = [0, 1, 1, 2, 2, 2];
        let filter = BooleanArray::from(vec![true, true, true, true, false, true]);
        let mut direct = expression.create_groups_accumulator().unwrap();
        direct
            .update_batch(&[input.clone()], &groups, Some(&filter), 4)
            .unwrap();
        let mut converted = expression.create_groups_accumulator().unwrap();
        let states = converted.convert_to_state(&[input], Some(&filter)).unwrap();
        converted.merge_batch(&states, &groups, 4).unwrap();
        let first = direct.state(EmitTo::First(2)).unwrap();
        let rest = direct.state(EmitTo::All).unwrap();
        let mut restored = expression.create_groups_accumulator().unwrap();
        restored.merge_batch(&first, &[0, 1], 4).unwrap();
        restored.merge_batch(&rest, &[2, 3], 4).unwrap();
        for actual in [
            restored.evaluate(EmitTo::All).unwrap(),
            converted.evaluate(EmitTo::All).unwrap(),
        ] {
            let array = actual.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(array.len(), 4);
            assert_eq!(
                [array.value(0), array.value(1), array.value(2)],
                [0., 0., 4.]
            );
            assert!(array.value(3).is_nan());
        }
    }
}

#[test]
fn floating_nonfinite_retraction_still_restores_finite_state() {
    for dtype in [DataType::Float32, DataType::Float64] {
        let input: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(f32::INFINITY),
            Some(f32::NAN),
            Some(1.),
            None,
            Some(5.),
        ]));
        let input = arrow_cast::cast(&input, &dtype).unwrap();
        let mut accumulator = aggregate(&dtype).create_accumulator().unwrap();
        accumulator.update_batch(&[input.clone()]).unwrap();
        assert!(value(accumulator.evaluate().unwrap()).is_nan());
        accumulator.retract_batch(&[input.slice(0, 2)]).unwrap();
        close(value(accumulator.evaluate().unwrap()), 2.);
    }
}
