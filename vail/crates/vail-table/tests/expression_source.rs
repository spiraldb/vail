use std::sync::Arc;

use arrow_array::{Array, Int32Array, ListArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::ScalarValue;
use vail_table::{ScalarIR, TableIR};

#[tokio::test]
async fn expression_source_preserves_ordered_arrow_rows_across_actions() {
    let fields: Fields = vec![Arc::new(Field::new("x", DataType::Int32, true))].into();
    let structs = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40]))],
        None,
    )
    .unwrap();
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(fields), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 4])),
        Arc::new(structs),
        None,
    );
    let expression = ScalarIR::RuntimeLiteral {
        value: ScalarValue::List(Arc::new(list)),
    };
    let table = TableIR::parallelize_expression(expression, Default::default(), 3).unwrap();
    // Diagnostics prepare no action; repeated real actions preserve every row.
    table.physical_plan().unwrap();
    for _ in 0..2 {
        let partitions = table.collect_partitions().await.unwrap();
        let values = partitions
            .iter()
            .flatten()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![10, 20, 30, 40]);
    }
}

#[tokio::test]
async fn expression_source_plan_does_not_evaluate_invalid_array() {
    let fields: Fields = vec![Arc::new(Field::new("x", DataType::Int32, true))].into();
    let structs = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![1]))],
        None,
    )
    .unwrap();
    let missing_list = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(fields), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(structs),
        Some(NullBuffer::from(vec![false])),
    );
    let table = TableIR::parallelize_expression(
        ScalarIR::RuntimeLiteral {
            value: ScalarValue::List(Arc::new(missing_list)),
        },
        Default::default(),
        2,
    )
    .unwrap();
    // Invalid source values must fail at execution, never while rendering a plan.
    table.physical_plan().unwrap();
    let error = table.collect_partitions().await.unwrap_err();
    assert!(error.to_string().contains("missing array"), "{error}");
}
