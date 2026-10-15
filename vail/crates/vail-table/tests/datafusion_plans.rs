use arrow_array::Int32Array;
use datafusion_execution::{TaskContext, config::SessionConfig, runtime_env::RuntimeEnvBuilder};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties, execution_plan::collect};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
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
async fn nested_row_maps_without_global_references_do_not_join_globals() {
    let source = TableIR::range(16, 4)
        .unwrap()
        .with_keys(vec!["idx".into()])
        .unwrap()
        .map_globals(
            ["g0", "g1", "g2"]
                .into_iter()
                .map(|name| {
                    (
                        name.into(),
                        ScalarIR::Literal {
                            dtype: Type::Primitive(PrimitiveType::Int32),
                            value: json!(5),
                        },
                    )
                })
                .collect(),
        )
        .unwrap();
    let mut mapped = source;
    for i in 0..20 {
        mapped = mapped
            .key_by(vec![])
            .unwrap()
            .map_rows(vec![
                ("idx".into(), ScalarIR::Column { name: "idx".into() }),
                (
                    format!("v{i}"),
                    ScalarIR::Column { name: "idx".into() },
                ),
            ])
            .unwrap();
    }

    let plan = mapped.logical_plan().unwrap();
    assert!(
        !plan.contains("Cross Join"),
        "unused singleton globals should not add cross joins: {plan}"
    );
    assert!(
        !plan.contains("VailPartitionOperation: KeyBy"),
        "empty-key metadata nodes should lower as passthroughs: {plan}"
    );
    let batches = mapped
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let mut values = Vec::new();
    for batch in &batches {
        let idx = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let annotated = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.push((idx.value(row), annotated.value(row)));
        }
    }
    assert_eq!(values.len(), 16);
    assert!(values.iter().all(|(idx, value)| idx == value));
    assert_eq!(mapped.globals().evaluate().await.unwrap().num_rows(), 1);

    let predicate = ScalarIR::Binary {
        operator: BinaryOperator::Ge,
        left: Box::new(ScalarIR::Column { name: "idx".into() }),
        right: Box::new(ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: json!(8),
        }),
    };
    let kept = mapped.filter(predicate.clone(), true).unwrap();
    let removed = mapped.filter(predicate, false).unwrap();
    assert!(!kept.logical_plan().unwrap().contains("Cross Join"));
    assert!(!removed.logical_plan().unwrap().contains("Cross Join"));
    let filtered_values = |partitions: Vec<Vec<arrow_array::RecordBatch>>| {
        partitions
            .into_iter()
            .flatten()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>()
    };
    let mut kept_values = filtered_values(kept.collect_partitions().await.unwrap());
    let mut removed_values = filtered_values(removed.collect_partitions().await.unwrap());
    kept_values.sort_unstable();
    removed_values.sort_unstable();
    assert_eq!(kept_values, (8..16).collect::<Vec<_>>());
    assert_eq!(removed_values, (0..8).collect::<Vec<_>>());

    let direct_global = TableIR::range(2, 2)
        .unwrap()
        .map_globals(vec![(
            "g".into(),
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: json!(5),
            },
        )])
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("x".into(), ScalarIR::Global { name: "g".into() }),
        ])
        .unwrap();
    assert!(direct_global.logical_plan().unwrap().contains("Cross Join"));
    let rows = direct_global
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(rows.iter().all(|batch| {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .iter()
            .all(|value| *value == 5)
    }));
}

#[test]
fn empty_keyby_logical_plan_size_is_independent_of_partition_count() {
    let mut line_counts = Vec::new();
    for partitions in [1, 4, 16] {
        let table = TableIR::range(64, partitions)
            .unwrap()
            .with_keys(vec!["idx".into()])
            .unwrap()
            .key_by(vec![])
            .unwrap();
        let plan = table.logical_plan().unwrap();
        assert!(!plan.contains("VailPartitionOperation: KeyBy"));
        line_counts.push(plan.lines().count());
    }
    assert!(
        line_counts.iter().all(|&count| count == line_counts[0]),
        "passthrough plan depth should not scale with partitions: {line_counts:?}"
    );
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
            dtype: dtype.clone(),
        }),
        name: "g".into(),
    };
    let destination = TableIR::range(3, 2)
        .unwrap()
        .map_globals(
            ["own0", "own1", "own2"]
                .into_iter()
                .map(|name| {
                    (
                        name.into(),
                        ScalarIR::Literal {
                            dtype: Type::Primitive(PrimitiveType::Int32),
                            value: json!(11),
                        },
                    )
                })
                .collect(),
        )
        .unwrap();
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

    let external_g = ScalarIR::GetField {
        value: Box::new(ScalarIR::ExternalGlobal {
            reference: "other".into(),
            dtype,
        }),
        name: "g".into(),
    };
    let predicate = ScalarIR::Binary {
        operator: BinaryOperator::Gt,
        left: Box::new(ScalarIR::Column { name: "idx".into() }),
        right: Box::new(external_g),
    };
    let keep = table
        .filter_bound(
            predicate.clone(),
            true,
            GlobalBindings::from([("other".into(), captured.clone())]),
        )
        .unwrap();
    let drop = table
        .filter_bound(
            predicate,
            false,
            GlobalBindings::from([("other".into(), captured)]),
        )
        .unwrap();
    let keep_rows = keep
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(keep_rows, 0);
    let mut drop_rows = drop
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    drop_rows.sort_unstable();
    assert_eq!(drop_rows, vec![0, 1, 2]);
}
