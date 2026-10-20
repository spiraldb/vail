//! Checked public aggregate construction contracts.
use arrow_array::{BooleanArray, Float32Array, Int32Array, ListArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use datafusion_expr::EmitTo;
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
use std::sync::Arc;
use vail_execution::{
    aggregates::aggregate_udf,
    array_aggregate::coordinate_udaf,
    expressions::{Expr, PrimitiveType, Type},
    fold_aggregate::{FoldProgram, fold_udaf},
};

#[test]
fn fold_factory_checks_initial_sequence_and_combine_types() {
    let schema = Arc::new(Schema::empty());
    let state_schema = Schema::new(vec![Field::new("state", DataType::Int32, true)]);
    let program = |value: Expr| {
        FoldProgram::lower(&value, &state_schema, &Schema::empty(), &Default::default()).unwrap()
    };
    let valid = || {
        program(Expr::Column {
            name: "state".into(),
        })
    };
    let wrong = || {
        program(Expr::RuntimeLiteral {
            value: ScalarValue::Int64(Some(0)),
        })
    };
    assert!(
        fold_udaf(
            schema.clone(),
            ScalarValue::Int32(Some(0)),
            DataType::Int32,
            valid(),
            valid()
        )
        .is_ok()
    );
    assert!(
        fold_udaf(
            schema.clone(),
            ScalarValue::Int64(Some(0)),
            DataType::Int32,
            valid(),
            valid()
        )
        .is_err()
    );
    for sequence in [true, false] {
        assert!(
            fold_udaf(
                schema.clone(),
                ScalarValue::Int32(Some(0)),
                DataType::Int32,
                if sequence { wrong() } else { valid() },
                if sequence { valid() } else { wrong() }
            )
            .is_err()
        );
    }
}
#[test]
fn coordinate_factory_checks_empty_identity_against_executable_output() {
    let sum = datafusion_functions_aggregate::sum::sum_udaf();
    coordinate_udaf(sum.clone(), DataType::Int64, ScalarValue::Int64(Some(0))).unwrap();
    assert!(coordinate_udaf(sum, DataType::Int64, ScalarValue::Float64(Some(0.))).is_err());
}

#[test]
fn count_where_scan_factory_requires_boolean_elements_and_supported_options() {
    let bool_list = DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true)));
    let int_list = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
    assert!(
        vail_execution::array_aggregate_scan::make_udf(
            "count_where",
            &bool_list,
            &serde_json::json!({"__vail_scan": true}),
        )
        .is_ok()
    );
    assert!(
        vail_execution::array_aggregate_scan::make_udf(
            "count_where",
            &int_list,
            &serde_json::json!({"__vail_scan": true}),
        )
        .is_err()
    );
    assert!(
        vail_execution::array_aggregate_scan::make_udf(
            "count_where",
            &bool_list,
            &serde_json::json!({"unexpected": true}),
        )
        .is_err()
    );
}

#[test]
fn count_where_list_expression_typing_rejects_non_boolean_elements() {
    let expression = |element| Expr::NestedFunction {
        function: "__vail_list_reduce_count_where".into(),
        args: vec![Expr::Literal {
            dtype: Type::Array {
                array: Box::new(element),
            },
            value: serde_json::json!([]),
        }],
        dtype: Type::Primitive(PrimitiveType::Int64),
    };
    assert!(
        expression(Type::Primitive(PrimitiveType::Bool))
            .dtype(&Schema::empty())
            .is_ok()
    );
    assert!(
        expression(Type::Primitive(PrimitiveType::Int32))
            .dtype(&Schema::empty())
            .is_err()
    );
}

#[test]
fn coordinate_sum_widens_float32_before_native_grouped_kernel() {
    let sum = coordinate_udaf(
        aggregate_udf("sum", Some(&DataType::Float32)).unwrap(),
        DataType::Float32,
        ScalarValue::Float64(Some(0.0)),
    )
    .unwrap();
    let list_type = DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true)));
    let schema = Arc::new(Schema::new(vec![Field::new("value", list_type, true)]));
    let aggregate = AggregateExprBuilder::new(sum, vec![Arc::new(Column::new("value", 0))])
        .schema(schema)
        .alias("sum")
        .build()
        .unwrap();
    let values: arrow_array::ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float32, true)),
        OffsetBuffer::from_lengths([1, 1, 1]),
        Arc::new(Float32Array::from(vec![16_777_216.0, 1.0, -16_777_216.0])),
        None,
    ));
    let mut accumulator = aggregate.create_accumulator().unwrap();
    accumulator.update_batch(&[values]).unwrap();
    let ScalarValue::List(values) = accumulator.evaluate().unwrap() else {
        panic!("coordinate sum must return a list");
    };
    let sums = values
        .values()
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert_eq!(sums.value(0), 1.0);
}

#[test]
fn public_sum_factory_validates_types_and_preserves_widened_native_state() {
    assert!(aggregate_udf("sum", None).is_err());
    assert!(aggregate_udf("sum", Some(&DataType::Utf8)).is_err());

    let sum = aggregate_udf("sum", Some(&DataType::Int32)).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let aggregate = AggregateExprBuilder::new(sum, vec![Arc::new(Column::new("value", 0))])
        .schema(schema)
        .alias("sum")
        .build()
        .unwrap();
    let mut accumulator = aggregate.create_accumulator().unwrap();
    assert_eq!(
        accumulator.evaluate().unwrap(),
        ScalarValue::Int64(Some(0)),
        "empty SUM has a typed zero identity"
    );
    accumulator
        .update_batch(&[Arc::new(Int32Array::from(vec![i32::MAX]))])
        .unwrap();
    accumulator
        .update_batch(&[Arc::new(Int32Array::from(vec![1]))])
        .unwrap();
    assert_eq!(
        accumulator.evaluate().unwrap(),
        ScalarValue::Int64(Some(2_147_483_648))
    );
    let partial = accumulator.state().unwrap();
    assert_eq!(partial.len(), 1, "native SUM state has one value field");
    let mut merged = aggregate.create_accumulator().unwrap();
    let arrays = partial
        .iter()
        .map(|value| ScalarValue::iter_to_array([value.clone()]).unwrap())
        .collect::<Vec<_>>();
    merged.merge_batch(&arrays).unwrap();
    assert_eq!(
        merged.evaluate().unwrap(),
        ScalarValue::Int64(Some(2_147_483_648))
    );

    let bool_sum = aggregate_udf("sum", Some(&DataType::Boolean)).unwrap();
    let bool_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Boolean,
        true,
    )]));
    let bool_aggregate =
        AggregateExprBuilder::new(bool_sum, vec![Arc::new(Column::new("value", 0))])
            .schema(bool_schema)
            .alias("sum")
            .build()
            .unwrap();
    assert!(bool_aggregate.groups_accumulator_supported());
    let mut groups = bool_aggregate.create_groups_accumulator().unwrap();
    groups
        .update_batch(
            &[Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
            ]))],
            &[0, 0, 0],
            None,
            2,
        )
        .unwrap();
    let values = groups.evaluate(EmitTo::All).unwrap();
    let values = values
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[1, 0]);
}

#[test]
fn coordinate_sum_uses_actual_int32_input_not_frontend_result_annotation() {
    let sum = coordinate_udaf(
        aggregate_udf("sum", Some(&DataType::Int32)).unwrap(),
        DataType::Int32,
        ScalarValue::Int64(Some(0)),
    )
    .unwrap();
    let list_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
    let schema = Arc::new(Schema::new(vec![Field::new("value", list_type, true)]));
    let aggregate = AggregateExprBuilder::new(sum, vec![Arc::new(Column::new("value", 0))])
        .schema(schema)
        .alias("sum")
        .build()
        .unwrap();
    let values: arrow_array::ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int32, true)),
        OffsetBuffer::from_lengths([1, 1]),
        Arc::new(Int32Array::from(vec![16_777_217, 1])),
        None,
    ));
    let mut accumulator = aggregate.create_accumulator().unwrap();
    accumulator.update_batch(&[values]).unwrap();
    let ScalarValue::List(values) = accumulator.evaluate().unwrap() else {
        panic!("coordinate sum must return a list");
    };
    let sums = values
        .values()
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(sums.value(0), 16_777_218);
}

#[test]
fn coordinate_sum_widens_boolean_input_to_int64() {
    let sum = coordinate_udaf(
        aggregate_udf("sum", Some(&DataType::Boolean)).unwrap(),
        DataType::Boolean,
        ScalarValue::Int64(Some(0)),
    )
    .unwrap();
    let list_type = DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true)));
    let schema = Arc::new(Schema::new(vec![Field::new("value", list_type, true)]));
    let aggregate = AggregateExprBuilder::new(sum, vec![Arc::new(Column::new("value", 0))])
        .schema(schema)
        .alias("sum")
        .build()
        .unwrap();
    let values: arrow_array::ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Boolean, true)),
        OffsetBuffer::from_lengths([3]),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        None,
    ));
    let mut accumulator = aggregate.create_accumulator().unwrap();
    accumulator.update_batch(&[values]).unwrap();
    let ScalarValue::List(values) = accumulator.evaluate().unwrap() else {
        panic!("coordinate sum must return a list");
    };
    let sums = values
        .values()
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(sums.value(0), 1);
}
