//! Checked public aggregate construction contracts.
use arrow_schema::{DataType, Schema};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::{Column, Literal};
use std::sync::Arc;
use vail_execution::{array_aggregate::coordinate_udaf, fold_aggregate::fold_udaf};

#[test]
fn fold_factory_checks_initial_sequence_and_combine_types() {
    let schema = Arc::new(Schema::empty());
    assert!(
        fold_udaf(
            schema.clone(),
            ScalarValue::Int32(Some(0)),
            DataType::Int32,
            Arc::new(Column::new("state", 0)),
            Arc::new(Column::new("left", 0))
        )
        .is_ok()
    );
    assert!(
        fold_udaf(
            schema.clone(),
            ScalarValue::Int64(Some(0)),
            DataType::Int32,
            Arc::new(Column::new("state", 0)),
            Arc::new(Column::new("left", 0))
        )
        .is_err()
    );
    for sequence in [true, false] {
        let wrong = Arc::new(Literal::new(ScalarValue::Int64(Some(0))));
        let valid = Arc::new(Column::new("state", 0));
        assert!(
            fold_udaf(
                schema.clone(),
                ScalarValue::Int32(Some(0)),
                DataType::Int32,
                if sequence {
                    wrong.clone()
                } else {
                    valid.clone()
                },
                if sequence { valid } else { wrong }
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
