//! Checked public aggregate construction contracts.
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use std::sync::Arc;
use vail_execution::{
    array_aggregate::coordinate_udaf,
    expressions::Expr,
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
