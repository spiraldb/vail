//! Public collection UDAFs executed on Arrow batches, including invalid packing.
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{Field, Schema};
use datafusion_common::DataFusionError;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_expr::AggregateUDF;
use datafusion_physical_expr::{aggregate::AggregateExprBuilder, expressions::Column};
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
async fn collect_preserves_nullable_struct_values_and_input_order() {
    let input = packed(
        vec![Some(999), Some(1), Some(2), Some(3)],
        vec![Some(999), None, Some(2), Some(3)],
        vec![true, true, false, true],
    )
    .slice(1, 3);
    let udf = vail_execution::collect_aggregate::collect_udf(input.data_type());
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
