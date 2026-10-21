//! Checked public aggregate construction contracts.
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, ListArray, StringArray,
    StructArray, UInt8Array, UInt16Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
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

#[test]
fn grouped_call_stats_uses_current_call_fields_and_roundtrips_group_state() {
    let call_type = vail_execution::calls::storage_type();
    let packed_type = DataType::Struct(
        vec![
            Arc::new(Field::new("__vail_genomic_call", call_type.clone(), true)),
            Arc::new(Field::new("__vail_genomic_alleles", DataType::Int32, true)),
        ]
        .into(),
    );
    let udf = vail_execution::advanced_genomic_aggregates::make_udaf(
        "call_stats",
        &packed_type,
        &serde_json::json!({"n_alleles": 2}),
    )
    .unwrap()
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "packed",
        packed_type.clone(),
        true,
    )]));
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("packed", 0))])
        .schema(schema)
        .alias("stats")
        .build()
        .unwrap();
    assert!(aggregate.groups_accumulator_supported());

    let DataType::Struct(call_fields) = call_type else {
        unreachable!()
    };
    let calls = Arc::new(StructArray::new(
        call_fields,
        vec![
            Arc::new(UInt16Array::from(vec![Some(0), Some(1), Some(0), Some(0)])),
            Arc::new(UInt16Array::from(vec![Some(1), Some(0), Some(0), None])),
            Arc::new(UInt8Array::from(vec![Some(2), Some(1), Some(0), None])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(false),
                None,
            ])),
        ],
        Some(NullBuffer::from(vec![true, true, true, false])),
    ));
    let packed_fields = match &packed_type {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    };
    let packed: arrow_array::ArrayRef = Arc::new(StructArray::new(
        packed_fields,
        vec![calls, Arc::new(Int32Array::from(vec![2, 2, 2, 2]))],
        None,
    ));
    let mut grouped = aggregate.create_groups_accumulator().unwrap();
    grouped
        .update_batch(&[packed.clone()], &[0, 0, 1, 1], None, 2)
        .unwrap();
    let group_state = grouped.state(EmitTo::All).unwrap();

    // A grouped partition state is also a valid scalar partial state: it has
    // native-compatible Int64 AN plus fixed-width Int64 AC and homozygote lists.
    let mut scalar = aggregate.create_accumulator().unwrap();
    scalar.merge_batch(&group_state).unwrap();
    let ScalarValue::Struct(result) = scalar.evaluate().unwrap() else {
        panic!("call_stats must return a struct");
    };
    assert_eq!(
        result
            .column_by_name("AN")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        3
    );
    let ac = result
        .column_by_name("AC")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let ac = ac.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ac.values(), &[1, 2]);

    let mut scalar_source = aggregate.create_accumulator().unwrap();
    scalar_source.update_batch(&[packed.clone()]).unwrap();
    let scalar_state = scalar_source.state().unwrap();
    let scalar_state = scalar_state
        .iter()
        .map(|value| value.to_array_of_size(1).unwrap())
        .collect::<Vec<_>>();
    let mut grouped_from_scalar = aggregate.create_groups_accumulator().unwrap();
    grouped_from_scalar
        .merge_batch(&scalar_state, &[0], 1)
        .unwrap();
    let from_scalar = grouped_from_scalar.evaluate(EmitTo::All).unwrap();
    let from_scalar = from_scalar.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        from_scalar
            .column_by_name("AN")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        3
    );

    // `state(EmitTo::All)` drains the original groups. Evaluate a fresh
    // accumulator after merging that state to verify grouped state roundtrips.
    let mut evaluated_groups = aggregate.create_groups_accumulator().unwrap();
    evaluated_groups
        .merge_batch(&group_state, &[0, 1], 2)
        .unwrap();
    let evaluated = evaluated_groups.evaluate(EmitTo::All).unwrap();
    let evaluated = evaluated.as_any().downcast_ref::<StructArray>().unwrap();
    let an = evaluated
        .column_by_name("AN")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(an.values(), &[3, 0]);

    // Null parent Calls can contain hidden null children; the group reader
    // ignores those hidden physical values exactly as the scalar reader does.
    let bad_calls: arrow_array::ArrayRef = Arc::new(StructArray::new(
        match vail_execution::calls::storage_type() {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        },
        vec![
            Arc::new(UInt16Array::from(vec![Some(0)])),
            Arc::new(UInt16Array::from(vec![Some(0)])),
            Arc::new(UInt8Array::from(vec![None])),
            Arc::new(BooleanArray::from(vec![Some(false)])),
        ],
        Some(NullBuffer::from(vec![false])),
    ));
    let hidden = StructArray::new(
        match &packed_type {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![bad_calls, Arc::new(Int32Array::from(vec![2]))],
        None,
    );
    let mut hidden_groups = aggregate.create_groups_accumulator().unwrap();
    hidden_groups
        .update_batch(&[Arc::new(hidden)], &[0], None, 1)
        .unwrap();
    let empty = hidden_groups.evaluate(EmitTo::All).unwrap();
    let empty = empty.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        empty
            .column_by_name("AN")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        0
    );

    let malformed_call: arrow_array::ArrayRef = Arc::new(StructArray::new(
        match vail_execution::calls::storage_type() {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        },
        vec![
            Arc::new(UInt16Array::from(vec![Some(0)])),
            Arc::new(UInt16Array::from(vec![Some(0)])),
            Arc::new(UInt8Array::from(vec![None])),
            Arc::new(BooleanArray::from(vec![Some(false)])),
        ],
        None,
    ));
    let malformed = StructArray::new(
        match &packed_type {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![malformed_call, Arc::new(Int32Array::from(vec![2]))],
        None,
    );
    let mut malformed_groups = aggregate.create_groups_accumulator().unwrap();
    // Storage is trusted: child null bits are not a second Call validity mask.
    malformed_groups
        .update_batch(&[Arc::new(malformed)], &[0], None, 1)
        .unwrap();
    assert_eq!(malformed_groups.evaluate(EmitTo::All).unwrap().len(), 1);
}

#[test]
fn grouped_downsample_roundtrips_bounded_group_states() {
    let input_type = DataType::Struct(
        vec![
            Arc::new(Field::new("x", DataType::Float64, true)),
            Arc::new(Field::new("y", DataType::Float64, true)),
            Arc::new(Field::new("label", DataType::Utf8, true)),
        ]
        .into(),
    );
    let udf = vail_execution::advanced_numeric_aggregates::make_udaf(
        "downsample",
        &input_type,
        &serde_json::json!({"n_divisions": 4}),
    )
    .unwrap()
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "packed",
        input_type.clone(),
        true,
    )]));
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("packed", 0))])
        .schema(schema)
        .alias("points")
        .build()
        .unwrap();
    assert!(aggregate.groups_accumulator_supported());
    let values: arrow_array::ArrayRef = Arc::new(StructArray::new(
        match &input_type {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(Float64Array::from(vec![
                Some(0.),
                Some(1.),
                Some(100.),
                None,
            ])),
            Arc::new(Float64Array::from(vec![0., 1., 100., 101.])),
            Arc::new(StringArray::from(vec!["a0", "a1", "b0", "b1"])),
        ],
        None,
    ));
    let mut grouped = aggregate.create_groups_accumulator().unwrap();
    grouped
        .update_batch(&[values.clone()], &[0, 0, 1, 1], None, 2)
        .unwrap();
    let states = grouped.state(EmitTo::All).unwrap();
    let mut merged = aggregate.create_groups_accumulator().unwrap();
    merged.merge_batch(&states, &[1, 0], 2).unwrap();
    let result = merged.evaluate(EmitTo::All).unwrap();
    let result = result.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(result.len(), 2);
    assert!((1..=16).contains(&result.value_length(0)));
    assert!((1..=2).contains(&result.value_length(1)));
    for group in 0..result.len() {
        let points = result.value(group);
        let points = points.as_any().downcast_ref::<StructArray>().unwrap();
        let xs = points
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ys = points
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let labels = points
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..points.len() {
            let label = labels.value(row);
            let label = label.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(label.len(), 1);
            let source_x = match label.value(0) {
                "a0" => 0.0,
                "a1" => 1.0,
                "b0" => 100.0,
                "b1" => 101.0,
                label => panic!("unexpected downsample label {label}"),
            };
            assert_eq!(xs.value(row), source_x);
            assert_eq!(ys.value(row), source_x);
        }
    }

    // Direct state conversion emits one bounded state per row, honoring null
    // filters and null coordinates without building a per-row accumulator.
    let filter = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
    let converted = grouped
        .convert_to_state(&[values.clone()], Some(&filter))
        .unwrap();
    let converted_points = converted[0].as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(converted_points.len(), 4);
    assert_eq!(converted_points.value_length(0), 1);
    assert_eq!(converted_points.value_length(1), 0);
    assert_eq!(converted_points.value_length(2), 1);
    assert_eq!(converted_points.value_length(3), 0);
    let converted_first = converted_points.value(0);
    let converted_first = converted_first
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        converted_first
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.0
    );
    let converted_label = converted_first
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        converted_label
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a0"
    );

    // Identity conversion wraps UTF-8 labels in one-element lists while
    // reusing the source string values buffer.
    // The fourth source point has missing x, so use the fully retained prefix
    // to exercise identity conversion rather than its legitimate gather path.
    let identity_values = values.slice(0, 3);
    let converted_all = grouped.convert_to_state(&[identity_values], None).unwrap();
    let converted_all_points = converted_all[0]
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let converted_all_row = converted_all_points.value(0);
    let converted_all_row = converted_all_row
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let converted_all_labels = converted_all_row
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let converted_all_values = converted_all_labels
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let source_labels = values
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        converted_all_values.values().as_ptr(),
        source_labels.values().as_ptr()
    );

    let mut partial = aggregate.create_groups_accumulator().unwrap();
    partial
        .update_batch(&[values.clone()], &[0, 0, 1, 1], Some(&filter), 2)
        .unwrap();
    assert_eq!(partial.evaluate(EmitTo::First(1)).unwrap().len(), 1);
    assert_eq!(partial.evaluate(EmitTo::All).unwrap().len(), 1);
    let mut empty = aggregate.create_groups_accumulator().unwrap();
    assert_eq!(empty.evaluate(EmitTo::All).unwrap().len(), 0);

    let xs = (0..40).map(|x| x as f64).collect::<Vec<_>>();
    let labels = (0..40).map(|x| format!("p{x}")).collect::<Vec<_>>();
    let many: arrow_array::ArrayRef = Arc::new(StructArray::new(
        match &input_type {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(Float64Array::from(xs.clone())),
            Arc::new(Float64Array::from(xs)),
            Arc::new(StringArray::from(labels)),
        ],
        None,
    ));
    let mut bounded = aggregate.create_groups_accumulator().unwrap();
    bounded
        .update_batch(&[many], &vec![0; 40], None, 1)
        .unwrap();
    let bounded = bounded.evaluate(EmitTo::All).unwrap();
    let bounded = bounded.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(bounded.value_length(0) <= 16);
}
