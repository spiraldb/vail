//! Public numeric UDAFs executed on sliced and nullable Arrow batches.
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
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
use vail_execution::{
    scan_scopes::{self, Transform},
    scan_state::ScanState,
};

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
fn sum() -> ScanState {
    ScanState::new(
        vail_execution::aggregates::aggregate_udf("sum", Some(&DataType::Int64)).unwrap(),
        vec![DataType::Int64],
    )
    .unwrap()
}
fn filtered(valid: Vec<bool>) -> ArrayRef {
    Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("predicate", DataType::Boolean, true)),
            Arc::new(Field::new("value", DataType::Int64, true)),
        ]
        .into(),
        vec![
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
            Arc::new(Int64Array::from(vec![Some(2), Some(99), Some(88), None])),
        ],
        Some(NullBuffer::from(valid)),
    ))
}
#[tokio::test]
async fn filtered_scope_ignores_missing_predicates_and_values() {
    let value = filtered(vec![true; 4]);
    let udf =
        scan_scopes::transformed(sum(), value.data_type().clone(), Transform::Filter).unwrap();
    let result = execute(udf, vec![value.slice(0, 2), value.slice(2, 2)])
        .await
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}
#[tokio::test]
async fn filtered_scope_rejects_hidden_children_under_null_parent() {
    let value = filtered(vec![false, true, true, true]);
    let udf =
        scan_scopes::transformed(sum(), value.data_type().clone(), Transform::Filter).unwrap();
    assert!(
        execute(udf, vec![value])
            .await
            .unwrap_err()
            .to_string()
            .contains("parents must be defined")
    );
}
#[tokio::test]
async fn explode_handles_sliced_spans_and_missing_lists() {
    for valid in [vec![true, true, true, true], vec![true, true, false, true]] {
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::from_lengths([1, 2, 1, 1]),
            Arc::new(Int64Array::from(vec![
                Some(999),
                Some(2),
                None,
                Some(3),
                Some(999),
            ])),
            Some(NullBuffer::from(valid.clone())),
        ));
        let array = array.slice(1, 2);
        let udf =
            scan_scopes::transformed(sum(), array.data_type().clone(), Transform::Explode).unwrap();
        let result = execute(udf, vec![array]).await.unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            if valid[2] { 5 } else { 2 }
        );
    }
}
#[test]
fn scan_factories_reject_inconsistent_contracts() {
    let udf = vail_execution::aggregates::aggregate_udf("sum", Some(&DataType::Int64)).unwrap();
    assert!(ScanState::new(udf, vec![]).is_err());
    assert!(scan_scopes::transformed(sum(), DataType::Int64, Transform::Filter).is_err());
    assert!(
        scan_scopes::transformed(
            sum(),
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            Transform::Explode
        )
        .is_err()
    );
    assert!(
        scan_scopes::indexed(
            sum(),
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            true
        )
        .is_err()
    );
    let spec = scan_scopes::Composition {
        children: vec![sum()],
        schema: Arc::new(Schema::new(vec![Field::new("out", DataType::Int64, true)])),
        projection: Arc::new(Column::new("out", 0)),
    };
    assert!(scan_scopes::composed(spec, DataType::Struct(Vec::<Field>::new().into())).is_err());
}

#[tokio::test]
async fn public_snapshot_finalization_preserves_values_and_rejects_wrong_schema() {
    let descriptor = sum();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), None, Some(3)]));
    let snapshots = execute(descriptor.state_udaf(), vec![values])
        .await
        .unwrap();
    let result = descriptor
        .finish_states(snapshots.as_any().downcast_ref::<StructArray>().unwrap())
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
    let wrong = StructArray::new_empty_fields(1, None);
    assert!(descriptor.finish_states(&wrong).is_err());
    let udf = vail_execution::aggregates::aggregate_udf("sum", Some(&DataType::Int64)).unwrap();
    assert!(ScanState::new(udf, vec![DataType::Utf8]).is_err());
}
#[tokio::test]
async fn composition_and_keyed_scopes_reject_missing_packed_parents() {
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("value", DataType::Int64, true))].into(),
        vec![Arc::new(Int64Array::from(vec![99]))],
        Some(NullBuffer::from(vec![false])),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new("out", DataType::Int64, true)]));
    let udf = scan_scopes::composed(
        scan_scopes::Composition {
            children: vec![sum()],
            schema,
            projection: Arc::new(Column::new("out", 0)),
        },
        values.data_type().clone(),
    )
    .unwrap();
    assert!(
        execute(udf, vec![values])
            .await
            .unwrap_err()
            .to_string()
            .contains("parents must be defined")
    );
    let fields = vec![
        Arc::new(Field::new("key", DataType::Int64, true)),
        Arc::new(Field::new("value", DataType::Int64, true)),
    ];
    let input: ArrayRef = Arc::new(StructArray::new(
        fields.clone().into(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![99])),
        ],
        Some(NullBuffer::from(vec![false])),
    ));
    let output = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(fields.into()),
        true,
    )));
    let udf = scan_scopes::indexed(
        sum(),
        input.data_type().clone(),
        DataType::Int64,
        output,
        false,
    )
    .unwrap();
    assert!(
        execute(udf, vec![input])
            .await
            .unwrap_err()
            .to_string()
            .contains("parents must be defined")
    );
}

#[tokio::test]
async fn real_datafusion_sum_uses_generic_snapshot_and_finalization() {
    let datafusion_expr::Expr::AggregateFunction(call) =
        datafusion_functions_aggregate::expr_fn::sum(datafusion_expr::col("value"))
    else {
        panic!("sum must be an aggregate");
    };
    assert!(ScanState::new(call.func.clone(), vec![DataType::Utf8]).is_err());
    let descriptor = ScanState::new(call.func, vec![DataType::Int64]).unwrap();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), None, Some(3)]));
    let snapshots = execute(descriptor.state_udaf(), vec![values])
        .await
        .unwrap();
    let result = descriptor
        .finish_states(snapshots.as_any().downcast_ref::<StructArray>().unwrap())
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
}
