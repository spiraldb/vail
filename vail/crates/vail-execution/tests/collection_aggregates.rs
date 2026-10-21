//! Public collection UDAFs executed on Arrow batches, including invalid packing.
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{Field, Schema, SortOptions};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::AggregateUDF;
use datafusion_physical_expr::{
    PhysicalSortExpr, aggregate::AggregateExprBuilder, expressions::Column,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    execution_plan::{ExecutionPlan, collect},
};
use std::sync::Arc;

async fn execute(
    udf: Arc<AggregateUDF>,
    batches: Vec<ArrayRef>,
) -> Result<ArrayRef, DataFusionError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        batches[0].data_type().clone(),
        true,
    )]));
    let batches = batches
        .into_iter()
        .map(|array| RecordBatch::try_new(schema.clone(), vec![array]).unwrap())
        .collect::<Vec<_>>();
    let source = MemorySourceConfig::try_new_exec(&[batches], schema.clone(), None)?;
    let aggregate = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("value", 0))])
        .schema(schema)
        .alias("result")
        .build()?;
    let plan = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![]),
        vec![Arc::new(aggregate)],
        vec![None],
        source.clone(),
        source.schema(),
    )?);
    Ok(collect(plan, Arc::new(TaskContext::default())).await?[0]
        .column(0)
        .clone())
}
fn packed(values: Vec<Option<i64>>, keys: Vec<Option<i64>>, valid: Vec<bool>) -> ArrayRef {
    Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("value", arrow_schema::DataType::Int64, true)),
            Arc::new(Field::new("ordering", arrow_schema::DataType::Int64, true)),
        ]
        .into(),
        vec![
            Arc::new(Int64Array::from(values)),
            Arc::new(Int64Array::from(keys)),
        ],
        Some(NullBuffer::from(valid)),
    ))
}
#[tokio::test]
async fn ordered_take_preserves_key_order_across_slices_batches_and_missing_values() {
    let values = packed(
        vec![Some(999), Some(10), None, Some(30), Some(40)],
        vec![Some(-1), Some(3), Some(1), None, Some(2)],
        vec![true; 5],
    )
    .slice(1, 4);
    let udf = vail_execution::advanced_collection_aggregates::make_udaf(
        "take",
        values.data_type(),
        &serde_json::json!({"n":3,"ordered":true}),
    )
    .unwrap()
    .unwrap();
    let result = execute(udf, vec![values.slice(0, 2), values.slice(2, 2)])
        .await
        .unwrap();
    let values = result
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(40), Some(10)]
    );
}
#[tokio::test]
async fn ordered_take_rejects_hidden_children_under_null_packed_parent() {
    let input = packed(
        vec![Some(99), Some(1)],
        vec![Some(-99), Some(1)],
        vec![false, true],
    );
    let udf = vail_execution::advanced_collection_aggregates::make_udaf(
        "take",
        input.data_type(),
        &serde_json::json!({"n":1,"ordered":true}),
    )
    .unwrap()
    .unwrap();
    assert!(
        execute(udf, vec![input])
            .await
            .unwrap_err()
            .to_string()
            .contains("packed records must be defined")
    );
}
#[test]
fn ordered_take_factory_rejects_noncanonical_field_order() {
    let input = arrow_schema::DataType::Struct(
        vec![
            Arc::new(Field::new("ordering", arrow_schema::DataType::Int64, true)),
            Arc::new(Field::new("value", arrow_schema::DataType::Int64, true)),
        ]
        .into(),
    );
    assert!(
        vail_execution::advanced_collection_aggregates::make_udaf(
            "take",
            &input,
            &serde_json::json!({"n":1,"ordered":true})
        )
        .is_err()
    );
}
#[tokio::test]
async fn array_agg_preserves_nullable_struct_values_and_input_order() {
    let input = packed(
        vec![Some(999), Some(1), Some(2), Some(3)],
        vec![Some(999), None, Some(2), Some(3)],
        vec![true, true, false, true],
    )
    .slice(1, 3);
    let udf = datafusion_functions_aggregate::array_agg::array_agg_udaf();
    let result = execute(udf, vec![input.slice(0, 1), input.slice(1, 2)])
        .await
        .unwrap();
    let result = result
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    assert_eq!(result.as_ref(), input.as_ref());
}

#[tokio::test]
async fn native_ordered_array_agg_preserves_zero_field_struct_values() {
    let value: ArrayRef = Arc::new(StructArray::new_empty_fields(3, None));
    let ordering: ArrayRef = Arc::new(Int64Array::from(vec![2, 0, 1]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", value.data_type().clone(), true),
        Field::new("ordering", ordering.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![value, ordering]).unwrap();
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let aggregate = AggregateExprBuilder::new(
        datafusion_functions_aggregate::array_agg::array_agg_udaf(),
        vec![Arc::new(Column::new("value", 0))],
    )
    .order_by(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("ordering", 1)),
        SortOptions::default(),
    )])
    .schema(schema)
    .alias("result")
    .build()
    .unwrap();
    let plan = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![]),
            vec![Arc::new(aggregate)],
            vec![None],
            source.clone(),
            source.schema(),
        )
        .unwrap(),
    );
    let result = collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap();
    let values = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    assert_eq!(values.len(), 3);
    assert_eq!(values.null_count(), 0);

    let compacted =
        ScalarValue::Struct(Arc::new(StructArray::new_empty_fields(1, None))).compacted();
    let ScalarValue::Struct(compacted) = compacted else {
        panic!("expected zero-field struct scalar")
    };
    assert_eq!(compacted.len(), 1);
}

#[tokio::test]
async fn set_retains_one_null_across_repeated_batch_values() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(2),
        None,
        Some(2),
        Some(3),
        None,
    ]));
    let udf = vail_execution::reducer::configured_udaf(
        "collect_as_set",
        Some(input.data_type()),
        &serde_json::json!({}),
    )
    .unwrap();
    let result = execute(udf, vec![input.slice(0, 2), input.slice(2, 3)])
        .await
        .unwrap();
    let result = result
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(
        result.iter().collect::<std::collections::HashSet<_>>(),
        [Some(2), Some(3), None].into_iter().collect()
    );
}

#[tokio::test]
async fn unordered_take_preserves_first_values_across_large_later_batches() {
    let input: ArrayRef = Arc::new(Int64Array::from_iter_values(0..65536));
    let udf = vail_execution::reducer::configured_udaf(
        "take",
        Some(input.data_type()),
        &serde_json::json!({"n":3}),
    )
    .unwrap();
    let result = execute(udf, vec![input.slice(0, 2), input.slice(2, 65534), input])
        .await
        .unwrap();
    let result = result
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    assert_eq!(result.as_ref(), &Int64Array::from(vec![0, 1, 2]));
}
