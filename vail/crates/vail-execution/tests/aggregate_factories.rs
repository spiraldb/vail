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
