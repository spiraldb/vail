use std::sync::Arc;

use arrow_array::{Array, Int32Array, ListArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::ScalarValue;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::execution_plan::collect;
use vail_table::{ScalarIR, TableIR, partitioned};

#[tokio::test]
async fn expression_source_evaluates_once_and_splits_ordered_arrow_rows() {
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
    // Construction leaves the array expression as an unevaluated IR node.
    assert!(matches!(
        table.node(),
        vail_table::TableNode::Expression { .. }
    ));
    assert_eq!(
        table
            .physical_plan()
            .unwrap()
            .matches("ExpressionSourceExec")
            .count(),
        1
    );

    let prepared = partitioned::prepare(table).await.unwrap();
    assert_eq!(prepared.parts.len(), 3);
    assert_eq!(prepared.counts, vec![None, None, None]);
    let context = Arc::new(TaskContext::default());
    let mut values = Vec::new();
    for part in &prepared.parts {
        let batches = collect(part.clone(), context.clone()).await.unwrap();
        values.extend(batches.iter().flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        }));
    }
    assert_eq!(values, vec![10, 20, 30, 40]);
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
    assert!(
        table
            .physical_plan()
            .unwrap()
            .contains("ExpressionSourceExec")
    );
    let error = table.collect_partitions().await.unwrap_err();
    assert!(error.to_string().contains("missing array"), "{error}");
}
