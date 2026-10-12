use arrow_array::Int32Array;
use datafusion_execution::{TaskContext, config::SessionConfig, runtime_env::RuntimeEnvBuilder};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties, execution_plan::collect};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{PrimitiveType, Type};
use vail_table::{ScalarIR, TableIR, plan};

fn find<'a>(plan: &'a dyn ExecutionPlan, name: &str) -> Option<&'a dyn ExecutionPlan> {
    if plan.name() == name {
        return Some(plan);
    }
    plan.children()
        .into_iter()
        .find_map(|child| find(child.as_ref(), name))
}

#[test]
fn explode_lowers_to_datafusion_unnest() {
    let source = TableIR::range(3, 2).unwrap();
    let table = source
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "items".into(),
                ScalarIR::Literal {
                    dtype: Type::Array {
                        array: Box::new(Type::Primitive(PrimitiveType::Int32)),
                    },
                    value: json!([1, 2]),
                },
            ),
        ])
        .unwrap();
    let exploded = table.explode(vec!["items".into()]).unwrap();
    let physical = plan::lower(&exploded).unwrap().rows;
    assert_eq!(physical.name(), "UnnestExec");
    assert!(find(physical.as_ref(), "ProjectionExec").is_some());
    assert!(find(physical.as_ref(), "RangeExec").is_some());
}

#[tokio::test]
async fn singleton_globals_and_rows_are_both_datafusion_plans() {
    let source = TableIR::range(8, 3).unwrap();
    let table = source
        .map_globals(vec![(
            "g".into(),
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: json!(5),
            },
        )])
        .unwrap();
    let mapped = table
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("x".into(), ScalarIR::Global { name: "g".into() }),
        ])
        .unwrap();
    let physical = plan::lower(&mapped).unwrap().rows;
    assert_eq!(physical.name(), "ProjectionExec");
    let join = find(physical.as_ref(), "CrossJoinExec").unwrap();
    let singleton = collect(join.children()[0].clone(), Arc::new(TaskContext::default()))
        .await
        .unwrap();
    assert_eq!(singleton.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(find(physical.as_ref(), "RangeExec").is_some());
    assert_eq!(plan::count_plan(&mapped).unwrap().name(), "AggregateExec");
    assert_eq!(mapped.count().await.unwrap(), 8);
    let dropped = table.map_globals(vec![]).unwrap();
    assert_eq!(dropped.globals().evaluate().await.unwrap().num_rows(), 1);
}

#[tokio::test]
async fn viceberg_plan_advertises_certified_order_without_sorting() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table");
    let table = TableIR::range(12, 3)
        .unwrap()
        .map_globals(vec![(
            "g".into(),
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: json!(7),
            },
        )])
        .unwrap();
    table.write(path.to_str().unwrap(), false).await.unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();
    let plan = plan::lower(&read).unwrap().rows;
    assert_eq!(plan.name(), "VicebergScanExec");
    assert!(plan.output_ordering().is_some());
    assert!(find(plan.as_ref(), "SortExec").is_none());
    let globals = read.globals().lower().unwrap();
    assert_eq!(globals.name(), "GlobalsScanExec");
    assert_eq!(read.count().await.unwrap(), 12);
}

#[tokio::test]
async fn sort_spills_directly_from_a_generated_tableir_source() {
    let source = TableIR::range(1_000_000, 4).unwrap();
    let sorted = source.order_by(vec![("idx".into(), true)]).unwrap();
    let plan = plan::lower(&sorted).unwrap().rows;
    assert!(find(plan.as_ref(), "RangeExec").is_some());
    let config = SessionConfig::new();
    let reserve = config.options().execution.sort_spill_reservation_bytes;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(reserve + 8 * 1024 * 1024, 1.0)
        .build_arc()
        .unwrap();
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime.clone()),
    );
    let batches = collect(plan.clone(), context).await.unwrap();
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        1_000_000
    );
    let first = batches
        .first()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let last = batches
        .last()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(first.value(0), 999_999);
    assert_eq!(last.value(last.len() - 1), 0);
    let sort = find(plan.as_ref(), "SortExec").unwrap();
    assert!(sort.metrics().unwrap().spill_count().unwrap_or(0) > 0);
    assert_eq!(runtime.memory_pool.reserved(), 0);
}

#[tokio::test]
async fn captured_globals_share_ir_without_retaining_table_rows() {
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use vail_table::globals::GlobalBindings;
    let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let source = TableIR::parallelize(
        RecordBatch::try_new(schema, vec![values.clone()]).unwrap(),
        2,
    )
    .unwrap()
    .map_globals(vec![(
        "g".into(),
        ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: json!(7),
        },
    )])
    .unwrap();
    let captured = source.globals().clone();
    drop(source);
    assert_eq!(
        Arc::strong_count(&values),
        1,
        "a globals reference must not retain table row buffers"
    );
    let dtype = Type::from_arrow(&DataType::Struct(captured.schema().fields().clone())).unwrap();
    let expression = ScalarIR::GetField {
        value: Box::new(ScalarIR::ExternalGlobal {
            reference: "other".into(),
            dtype,
        }),
        name: "g".into(),
    };
    let destination = TableIR::range(3, 2).unwrap();
    let fields = vec![
        ("idx".into(), ScalarIR::Column { name: "idx".into() }),
        ("value".into(), expression),
    ];
    assert!(
        destination.map_rows(fields.clone()).is_err(),
        "unbound references must fail before execution"
    );
    let table = destination
        .map_rows_bound(
            fields,
            GlobalBindings::from([("other".into(), captured.clone())]),
        )
        .unwrap();
    let vail_table::TableNode::MapRows { bindings, .. } = table.node() else {
        panic!("expected MapRows")
    };
    assert!(Arc::ptr_eq(&bindings["other"], &captured));
    let plan = plan::lower(&table).unwrap().rows;
    assert!(find(plan.as_ref(), "CrossJoinExec").is_some());
    assert!(find(plan.as_ref(), "ParallelizeExec").is_none());
    let output = table.collect_partitions().await.unwrap();
    let results = output
        .iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(results, vec![7, 7, 7]);
}
