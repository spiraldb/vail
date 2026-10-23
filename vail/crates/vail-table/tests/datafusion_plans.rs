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

#[tokio::test]
async fn explode_lowers_to_datafusion_unnest() {
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
    let physical = plan::physical(&exploded).await.unwrap();
    assert!(find(physical.as_ref(), "UnnestExec").is_some());
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
    let physical = plan::physical(&mapped).await.unwrap();
    assert!(find(physical.as_ref(), "ProjectionExec").is_some());
    let join = find(physical.as_ref(), "CrossJoinExec").unwrap();
    let singleton = collect(join.children()[0].clone(), Arc::new(TaskContext::default()))
        .await
        .unwrap();
    assert_eq!(singleton.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(find(physical.as_ref(), "RangeExec").is_some());
    let count = vail_table::globals::GlobalsIR::aggregate(
        mapped.clone(),
        ScalarIR::Aggregate {
            function: "count".into(),
            value: None,
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap();
    assert!(count.physical_plan().unwrap().contains("AggregateExec"));
    assert_eq!(mapped.count().await.unwrap(), 8);
    let dropped = table.map_globals(vec![]).unwrap();
    assert_eq!(dropped.globals().evaluate().await.unwrap().num_rows(), 1);
}

#[tokio::test]
async fn nested_unkey_and_row_maps_preserve_rows_globals_and_filters() {
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
    let source_rows = source
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .map(|partition| {
            partition
                .into_iter()
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
        })
        .collect::<Vec<_>>();
    let mut mapped = source;
    for i in 0..20 {
        mapped = mapped
            .key_by(vec![])
            .unwrap()
            .map_rows(vec![
                ("idx".into(), ScalarIR::Column { name: "idx".into() }),
                (format!("v{i}"), ScalarIR::Column { name: "idx".into() }),
            ])
            .unwrap();
    }

    let mapped_rows = mapped
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .map(|partition| {
            partition
                .into_iter()
                .flat_map(|batch| {
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
                    (0..batch.num_rows())
                        .map(|row| (idx.value(row), annotated.value(row)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let expected_mapped_rows = source_rows
        .iter()
        .map(|partition| partition.iter().map(|&idx| (idx, idx)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(mapped_rows, expected_mapped_rows);
    assert_eq!(mapped.globals().evaluate().await.unwrap().num_rows(), 1);

    let predicate = ScalarIR::Binary {
        operator: BinaryOperator::Ge,
        left: Arc::new(ScalarIR::Column { name: "idx".into() }),
        right: Arc::new(ScalarIR::Literal {
            dtype: Type::Primitive(PrimitiveType::Int32),
            value: json!(8),
        }),
    };
    let kept = mapped.filter(predicate.clone(), true).unwrap();
    let removed = mapped.filter(predicate, false).unwrap();
    let filtered_values = |partitions: Vec<Vec<arrow_array::RecordBatch>>| {
        partitions
            .into_iter()
            .map(|partition| {
                partition
                    .into_iter()
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
            })
            .collect::<Vec<_>>()
    };
    let expected_kept = source_rows
        .iter()
        .map(|partition| partition.iter().copied().filter(|&idx| idx >= 8).collect())
        .collect::<Vec<Vec<_>>>();
    let expected_removed = source_rows
        .iter()
        .map(|partition| partition.iter().copied().filter(|&idx| idx < 8).collect())
        .collect::<Vec<Vec<_>>>();
    assert_eq!(
        filtered_values(kept.collect_partitions().await.unwrap()),
        expected_kept
    );
    assert_eq!(
        filtered_values(removed.collect_partitions().await.unwrap()),
        expected_removed
    );

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

#[tokio::test]
async fn repeated_empty_keyby_and_row_maps_preserve_results_across_partition_counts() {
    for partitions in [1, 4, 16] {
        let mut table = TableIR::range(64, partitions)
            .unwrap()
            .with_keys(vec!["idx".into()])
            .unwrap();
        let source_rows = table
            .collect_partitions()
            .await
            .unwrap()
            .into_iter()
            .map(|partition| {
                partition
                    .into_iter()
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
            })
            .collect::<Vec<_>>();
        for i in 0..20 {
            table = table
                .key_by(vec![])
                .unwrap()
                .map_rows(vec![
                    ("idx".into(), ScalarIR::Column { name: "idx".into() }),
                    (format!("v{i}"), ScalarIR::Column { name: "idx".into() }),
                ])
                .unwrap();
        }
        let values = table
            .collect_partitions()
            .await
            .unwrap()
            .into_iter()
            .map(|partition| {
                partition
                    .into_iter()
                    .flat_map(|batch| {
                        let idx = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        let mapped = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        (0..batch.num_rows())
                            .map(|row| (idx.value(row), mapped.value(row)))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let expected = source_rows
            .iter()
            .map(|partition| partition.iter().map(|&idx| (idx, idx)).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            values, expected,
            "repeated key and map operations should preserve row order and partition membership with {partitions} partitions"
        );
    }
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
    let plan = plan::physical(&read).await.unwrap();
    assert!(find(plan.as_ref(), "VicebergScanExec").is_some());
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
    let plan = plan::physical_with_context(&sorted, context.clone())
        .await
        .unwrap();
    assert!(find(plan.as_ref(), "RangeExec").is_some());
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
        value: Arc::new(ScalarIR::ExternalGlobal {
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
    let plan = plan::physical(&table).await.unwrap();
    // External singleton globals are now scalar subqueries; correctness does
    // not require a row cross join. Check the source lifetime and values below.
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
        value: Arc::new(ScalarIR::ExternalGlobal {
            reference: "other".into(),
            dtype,
        }),
        name: "g".into(),
    };
    let predicate = ScalarIR::Binary {
        operator: BinaryOperator::Gt,
        left: Arc::new(ScalarIR::Column { name: "idx".into() }),
        right: Arc::new(external_g),
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

#[tokio::test]
async fn sort_unkey_and_bounded_rows_report_their_actual_execution_layout() {
    let source = TableIR::range(24, 3).unwrap();
    let unkeyed = source.key_by(vec![]).unwrap();
    let sorted = unkeyed.key_by(vec!["idx".into()]).unwrap();
    let descending = source.order_by(vec![("idx".into(), true)]).unwrap();
    for table in [
        unkeyed,
        sorted,
        descending,
        source.head(5),
        source.tail(5),
        source.head(0),
        source.tail(0),
    ] {
        let physical = plan::physical(&table).await.unwrap();
        let declared = table.n_partitions().await.unwrap();
        assert_eq!(declared, physical.output_partitioning().partition_count());
        assert_eq!(table.collect_partitions().await.unwrap().len(), declared);
        let batches = collect(physical, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(rows, table.count().await.unwrap());
    }
    assert_eq!(
        source.key_by(vec![]).unwrap().n_partitions().await.unwrap(),
        source.n_partitions().await.unwrap()
    );
    let tail = collect(
        plan::physical(&source.tail(5)).await.unwrap(),
        Arc::new(TaskContext::default()),
    )
    .await
    .unwrap();
    assert_eq!(
        tail.iter()
            .flat_map(|batch| batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec())
            .collect::<Vec<_>>(),
        vec![19, 20, 21, 22, 23]
    );
}

#[tokio::test]
async fn configured_histogram_repeated_actions_and_empty_validation_use_real_sources() {
    use arrow_array::{Array, Int64Array, ListArray, StructArray};
    let source = TableIR::range(24, 3).unwrap();
    let expression = ScalarIR::Aggregate {
        function: "hist".into(),
        value: Some(Arc::new(ScalarIR::Column { name: "idx".into() })),
        options: json!({}),
        result_type: None,
        init: vec![
            (
                "start".into(),
                ScalarIR::RuntimeLiteral {
                    value: datafusion_common::ScalarValue::Float64(Some(0.0)),
                },
            ),
            (
                "end".into(),
                ScalarIR::RuntimeLiteral {
                    value: datafusion_common::ScalarValue::Float64(Some(24.0)),
                },
            ),
            (
                "bins".into(),
                ScalarIR::RuntimeLiteral {
                    value: datafusion_common::ScalarValue::Int32(Some(3)),
                },
            ),
        ],
    };
    let total = vail_table::globals::GlobalsIR::aggregate(
        source.clone(),
        expression.clone(),
        Default::default(),
    )
    .unwrap();
    for _ in 0..2 {
        let batch = total.evaluate().await.unwrap();
        let result = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let frequencies = result
            .column_by_name("bin_freq")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            frequencies
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[8, 8, 8]
        );
        assert!(!result.is_null(0));
    }
    let mut invalid = expression;
    if let ScalarIR::Aggregate { init, .. } = &mut invalid {
        init[1].1 = ScalarIR::RuntimeLiteral {
            value: datafusion_common::ScalarValue::Float64(Some(0.0)),
        };
    }
    let total =
        vail_table::globals::GlobalsIR::aggregate(source.head(0), invalid, Default::default())
            .unwrap();
    // Public native diagnostics must not evaluate invalid singleton parameters.
    assert!(!total.physical_plan().unwrap().is_empty());
    assert!(
        total
            .evaluate()
            .await
            .unwrap_err()
            .to_string()
            .contains("finite start < end")
    );
}

#[tokio::test]
async fn total_aggregation_splits_only_underparallel_sources_and_repeats_safely() {
    use arrow_array::Int64Array;
    use vail_table::globals::GlobalsIR;

    let workers = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(32);
    let rows = 1_000_000;
    for partitions in [1, workers] {
        let source = TableIR::range(rows, partitions).unwrap();
        let total = GlobalsIR::aggregate(
            source,
            ScalarIR::Aggregate {
                function: "sum".into(),
                value: Some(Arc::new(ScalarIR::Column { name: "idx".into() })),
                options: json!({}),
                result_type: None,
                init: vec![],
            },
            Default::default(),
        )
        .unwrap();
        let physical = total.physical_plan().unwrap();
        assert!(
            !physical.contains("RoundRobin"),
            "a total sum must not combine input partitions: {physical}"
        );
        assert_eq!(
            physical.contains("VailSplitPartitionsExec"),
            partitions < workers,
            "split only when more execution parallelism is available: {physical}"
        );
        // Repeated actions must get fresh channels and aggregate state.
        for _ in 0..2 {
            let batch = total.evaluate().await.unwrap();
            let sums = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(sums.value(0), 499_999_500_000);
        }
    }
}

#[test]
fn total_collect_keeps_source_order_without_split_or_exchange() {
    let total = vail_table::globals::GlobalsIR::aggregate(
        TableIR::range(65_536, 1).unwrap(),
        ScalarIR::Aggregate {
            function: "collect".into(),
            value: Some(Arc::new(ScalarIR::Column { name: "idx".into() })),
            options: json!({}),
            result_type: None,
            init: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let physical = total.physical_plan().unwrap();
    assert!(!physical.contains("VailSplitPartitionsExec"), "{physical}");
    assert!(!physical.contains("RoundRobin"), "{physical}");
}
