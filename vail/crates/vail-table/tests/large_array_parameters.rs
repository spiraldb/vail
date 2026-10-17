use arrow_array::{Array, Int32Array, Int64Array, ListArray};
use datafusion_common::ScalarValue;
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_table::{ScalarIR, TableIR};

fn repeat(length: i64) -> ScalarIR {
    ScalarIR::NestedFunction {
        function: "array_repeat".into(),
        args: vec![
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: json!(7),
            },
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(length),
            },
        ],
        dtype: Type::Array {
            array: Box::new(Type::Primitive(PrimitiveType::Int32)),
        },
    }
}

fn literal_i32(value: i32) -> ScalarIR {
    ScalarIR::Literal {
        dtype: Type::Primitive(PrimitiveType::Int32),
        value: json!(value),
    }
}

fn range(start: i32, stop: i32, step: i32) -> ScalarIR {
    ScalarIR::StreamRange {
        start: Arc::new(literal_i32(start)),
        stop: Arc::new(literal_i32(stop)),
        step: Arc::new(literal_i32(step)),
    }
}

#[tokio::test]
async fn generated_arrays_preserve_values_and_source_partition_membership() {
    let source = TableIR::range(7, 5).unwrap();
    let expected = source.collect_partitions().await.unwrap();
    let table = source
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("items".into(), repeat(16_384)),
        ])
        .unwrap();
    let actual = table.collect_partitions().await.unwrap();
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(&expected) {
        let keys = |batches: &[arrow_array::RecordBatch]| {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name("idx")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(keys(actual), keys(expected));
        for batch in actual {
            let lists = batch
                .column_by_name("items")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(lists.null_count(), 0);
            for row in 0..lists.len() {
                let values = lists.value(row);
                let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(values.len(), 16_384);
                assert!(values.values().iter().all(|&value| value == 7));
            }
        }
    }
}

#[tokio::test]
async fn live_wide_repeat_is_split_into_single_row_batches() {
    // Each row is just over the 64 MiB advice target, so the public action
    // should emit two one-row batches rather than materialize both lists in
    // one batch.
    let source = TableIR::range(2, 1).unwrap();
    let table = source
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "items".into(),
                repeat_value(ScalarIR::Column { name: "idx".into() }, 16_300_000),
            ),
        ])
        .unwrap()
        .filter(
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Bool),
                value: json!(true),
            },
            true,
        )
        .unwrap();

    let partitions = table.collect_partitions().await.unwrap();
    assert_eq!(partitions.len(), 1);
    let batches = &partitions[0];
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [1, 1]
    );
    for (expected_idx, batch) in batches.iter().enumerate() {
        let indices = batch
            .column_by_name("idx")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(indices.values(), &[expected_idx as i32]);
        let items = batch
            .column_by_name("items")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = items.value(0);
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.len(), 16_300_000);
        assert_eq!(values.value(0), expected_idx as i32);
        assert_eq!(values.value(values.len() - 1), expected_idx as i32);
    }
}

#[tokio::test]
async fn dropped_or_unselected_wide_repeat_does_not_change_batches() {
    let source = TableIR::range(8, 1)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("items".into(), repeat(50_000_000)),
        ])
        .unwrap();
    let projected = source
        .map_rows(vec![(
            "idx".into(),
            ScalarIR::Column { name: "idx".into() },
        )])
        .unwrap();
    let batches = projected.collect_partitions().await.unwrap();
    assert_eq!(
        batches[0]
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [8]
    );
    let values = batches[0][0]
        .column_by_name("idx")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.values(), &(0..8).collect::<Vec<_>>());

    let conditionally_selected = TableIR::range(8, 1)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "items".into(),
                ScalarIR::IfElse {
                    condition: Arc::new(ScalarIR::Literal {
                        dtype: Type::Primitive(PrimitiveType::Bool),
                        value: json!(true),
                    }),
                    consequent: Arc::new(repeat(1)),
                    alternate: Arc::new(repeat(50_000_000)),
                },
            ),
        ])
        .unwrap();
    let batches = conditionally_selected.collect_partitions().await.unwrap();
    assert_eq!(
        batches[0]
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [8]
    );
    let items = batches[0][0]
        .column_by_name("items")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(items.value_length(0), 1);
    assert_eq!(items.value_length(7), 1);
}

#[tokio::test]
async fn closed_wide_array_length_reduction_keeps_normal_batches() {
    let table = TableIR::range(100, 1)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "length".into(),
                ScalarIR::ArrayLength {
                    value: Arc::new(repeat(1_000_000)),
                },
            ),
        ])
        .unwrap();
    let batches = table.collect_partitions().await.unwrap();
    assert_eq!(
        batches[0]
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [100]
    );
    let lengths = batches[0][0]
        .column_by_name("length")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(lengths.values().iter().all(|&length| length == 1_000_000));
}

#[tokio::test]
async fn ordinary_range_keeps_its_requested_batch_size() {
    let batches = TableIR::range_batched(10, 1, 3)
        .unwrap()
        .collect_partitions()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [3, 3, 3, 1]
    );
}

#[tokio::test]
async fn large_stream_range_preserves_exact_values() {
    let table = TableIR::range(1, 1)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("items".into(), range(2, 20_002, 1)),
        ])
        .unwrap();
    let partitions = table.collect_partitions().await.unwrap();
    let values = partitions
        .into_iter()
        .flatten()
        .flat_map(|batch| {
            let lists = batch
                .column_by_name("items")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let values = lists.value(0);
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (2..20_002).collect::<Vec<_>>());
}

fn erroring_repeat() -> ScalarIR {
    let mut expression = repeat(16_384);
    if let ScalarIR::NestedFunction { args, .. } = &mut expression {
        args[0] = ScalarIR::ArrayLength {
            value: Arc::new(range(0, 20_000, 0)),
        };
    }
    expression
}

#[tokio::test]
async fn unselected_erroring_branch_with_large_repeat_is_not_evaluated_eagerly() {
    for select_error in [false, true] {
        let table = TableIR::range(1, 1)
            .unwrap()
            .map_rows(vec![
                ("idx".into(), ScalarIR::Column { name: "idx".into() }),
                (
                    "length".into(),
                    ScalarIR::IfElse {
                        condition: Arc::new(ScalarIR::Binary {
                            operator: BinaryOperator::Eq,
                            left: Arc::new(ScalarIR::Column { name: "idx".into() }),
                            right: Arc::new(literal_i32(if select_error { 1 } else { 0 })),
                        }),
                        consequent: Arc::new(ScalarIR::ArrayLength {
                            value: Arc::new(repeat(16_384)),
                        }),
                        alternate: Arc::new(ScalarIR::ArrayLength {
                            value: Arc::new(erroring_repeat()),
                        }),
                    },
                ),
            ])
            .unwrap();
        let result = table.collect_partitions().await;
        if select_error {
            let error = result.unwrap_err().to_string().to_lowercase();
            assert!(error.contains("step must not be zero"), "{error}");
        } else {
            let batches = result.unwrap();
            let lengths = batches[0][0]
                .column_by_name("length")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(lengths.values().as_ref(), &[16_384]);
        }
    }
}

#[tokio::test]
async fn arrow_nested_literal_preserves_nulls_and_empty_input() {
    let values = Int32Array::from_iter((0..20_000).map(|i| (i % 11 != 0).then_some(i)));
    let list = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([Some(
        values.iter().collect::<Vec<_>>(),
    )]);
    let literal = ScalarIR::RuntimeLiteral {
        value: ScalarValue::List(Arc::new(list)),
    };
    for rows in [0, 3] {
        let table = TableIR::range(rows, 4)
            .unwrap()
            .map_rows(vec![
                ("idx".into(), ScalarIR::Column { name: "idx".into() }),
                ("items".into(), literal.clone()),
            ])
            .unwrap();
        let partitions = table.collect_partitions().await.unwrap();
        assert_eq!(
            partitions
                .iter()
                .flatten()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            rows
        );
        for batch in partitions.into_iter().flatten() {
            let lists = batch
                .column_by_name("items")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..lists.len() {
                let actual = lists.value(row);
                assert_eq!(
                    actual.as_any().downcast_ref::<Int32Array>().unwrap(),
                    &values
                );
            }
        }
    }
}

#[tokio::test]
async fn unused_array_branch_does_not_change_row_values() {
    let table = TableIR::range(4, 3)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "items".into(),
                ScalarIR::IfElse {
                    condition: Arc::new(ScalarIR::Binary {
                        operator: BinaryOperator::Lt,
                        left: Arc::new(ScalarIR::Column { name: "idx".into() }),
                        right: Arc::new(ScalarIR::Literal {
                            dtype: Type::Primitive(PrimitiveType::Int32),
                            value: json!(0),
                        }),
                    }),
                    consequent: Arc::new(repeat(16_384)),
                    alternate: Arc::new(repeat(0)),
                },
            ),
        ])
        .unwrap();
    let partitions = table.collect_partitions().await.unwrap();
    for batch in partitions.into_iter().flatten() {
        let lists = batch
            .column_by_name("items")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(lists.null_count(), 0);
        assert!((0..lists.len()).all(|row| lists.value_length(row) == 0));
    }
}

#[tokio::test]
async fn small_result_consumer_streams_many_batches_across_actions() {
    let rows = 140_000;
    let table = TableIR::range(rows, 1)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "length".into(),
                ScalarIR::ArrayLength {
                    value: Arc::new(repeat(20_000)),
                },
            ),
        ])
        .unwrap();
    // Each public action owns an independent execution scope. A physical
    // consumer is deliberately not replayable inside one action.
    for _ in 0..3 {
        let mut seen = 0;
        for batch in table
            .collect_partitions()
            .await
            .unwrap()
            .into_iter()
            .flatten()
        {
            let indices = batch
                .column_by_name("idx")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let lengths = batch
                .column_by_name("length")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(lengths.null_count(), 0);
            assert!(lengths.values().iter().all(|&value| value == 20_000));
            assert!(
                indices
                    .values()
                    .iter()
                    .copied()
                    .eq((seen..seen + batch.num_rows()).map(|i| i as i32))
            );
            seen += batch.num_rows();
        }
        assert_eq!(seen, rows);
    }
}

#[tokio::test]
async fn empty_input_does_not_evaluate_erroring_closed_generator() {
    let table = TableIR::range(0, 3)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("items".into(), erroring_repeat()),
        ])
        .unwrap();
    let batches = table.collect_partitions().await.unwrap();
    assert!(batches.iter().flatten().all(|batch| batch.num_rows() == 0));
}

fn repeat_value(value: ScalarIR, length: i64) -> ScalarIR {
    ScalarIR::NestedFunction {
        function: "array_repeat".into(),
        args: vec![
            value,
            ScalarIR::Literal {
                dtype: Type::Primitive(PrimitiveType::Int64),
                value: json!(length),
            },
        ],
        dtype: Type::Array {
            array: Box::new(Type::Primitive(PrimitiveType::Int32)),
        },
    }
}

fn collected_i32_rows(
    partitions: Vec<Vec<arrow_array::RecordBatch>>,
    field: &str,
) -> Vec<(usize, Vec<Option<i32>>)> {
    partitions
        .into_iter()
        .enumerate()
        .map(|(partition, batches)| {
            let values = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name(field)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                })
                .collect();
            (partition, values)
        })
        .collect()
}

#[tokio::test]
async fn large_closed_repeat_survives_inner_and_left_joins() {
    use vail_table::TableJoinKind;

    let left = TableIR::range(4, 2)
        .unwrap()
        .with_keys(vec!["idx".into()])
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("left_items".into(), repeat(16_384)),
        ])
        .unwrap();
    let right = TableIR::range(3, 3)
        .unwrap()
        .with_keys(vec!["idx".into()])
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            ("right_items".into(), repeat_value(literal_i32(9), 16_384)),
        ])
        .unwrap();

    for (kind, expected_keys) in [
        (TableJoinKind::Inner, vec![0, 1, 2]),
        (TableJoinKind::Left, vec![0, 1, 2, 3]),
    ] {
        let joined = left.join(right.clone(), kind, 1).unwrap();
        let partitions = joined.collect_partitions().await.unwrap();
        let mut keys = Vec::new();
        for batch in partitions.iter().flatten() {
            let key = batch
                .column_by_name("idx")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            keys.extend(key.values().iter().copied());
            let left_items = batch
                .column_by_name("left_items")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let right_items = batch
                .column_by_name("right_items")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(left_items.value_length(row), 16_384);
                let left_values = left_items.value(row);
                assert!(
                    left_values
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .all(|&value| value == 7)
                );
                if kind == TableJoinKind::Left && key.value(row) == 3 {
                    assert!(right_items.is_null(row));
                } else {
                    assert_eq!(right_items.value_length(row), 16_384);
                    let right_values = right_items.value(row);
                    assert!(
                        right_values
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                            .iter()
                            .all(|&value| value == 9)
                    );
                }
            }
        }
        assert_eq!(keys, expected_keys);
    }
}

#[tokio::test]
async fn large_globals_repeat_and_runtime_literal_work_with_and_without_rows() {
    let values = Int32Array::from_iter((0..20_000).map(Some));
    let nested = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([Some(
        values.iter().collect::<Vec<_>>(),
    )]);
    let source = TableIR::range(3, 2).unwrap();
    let source_rows = source.collect_partitions().await.unwrap();
    let table = source
        .map_globals(vec![
            ("repeated".into(), repeat(16_384)),
            (
                "nested".into(),
                ScalarIR::RuntimeLiteral {
                    value: ScalarValue::List(Arc::new(nested)),
                },
            ),
        ])
        .unwrap();
    let mapped = table
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "repeat_len".into(),
                ScalarIR::ArrayLength {
                    value: Arc::new(ScalarIR::Global {
                        name: "repeated".into(),
                    }),
                },
            ),
            (
                "nested_len".into(),
                ScalarIR::ArrayLength {
                    value: Arc::new(ScalarIR::Global {
                        name: "nested".into(),
                    }),
                },
            ),
        ])
        .unwrap();
    let partitions = mapped.collect_partitions().await.unwrap();
    let expected_rows = collected_i32_rows(source_rows, "idx");
    let actual_rows = collected_i32_rows(partitions.clone(), "idx");
    assert_eq!(actual_rows, expected_rows);
    for batch in partitions.into_iter().flatten() {
        let repeat_len = batch
            .column_by_name("repeat_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(repeat_len.values().iter().all(|&length| length == 16_384));
        let nested_len = batch
            .column_by_name("nested_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(nested_len.values().iter().all(|&length| length == 20_000));
    }

    let empty = TableIR::range(0, 4)
        .unwrap()
        .map_globals(vec![
            ("repeated".into(), repeat(16_384)),
            (
                "nested".into(),
                ScalarIR::RuntimeLiteral {
                    value: ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<
                        arrow_array::types::Int32Type,
                        _,
                        _,
                    >([Some(
                        values.iter().collect::<Vec<_>>(),
                    )]))),
                },
            ),
        ])
        .unwrap();
    let empty_rows = empty
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "repeat_len".into(),
                ScalarIR::ArrayLength {
                    value: Arc::new(ScalarIR::Global {
                        name: "repeated".into(),
                    }),
                },
            ),
        ])
        .unwrap()
        .collect_partitions()
        .await
        .unwrap();
    assert!(
        empty_rows
            .iter()
            .flatten()
            .all(|batch| batch.num_rows() == 0)
    );

    let globals = empty.globals().evaluate().await.unwrap();
    assert_eq!(globals.num_rows(), 1);
    let repeated = globals
        .column_by_name("repeated")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(repeated.value_length(0), 16_384);
    let nested = globals
        .column_by_name("nested")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(nested.value_length(0), 20_000);
}

#[tokio::test]
async fn column_dependent_large_repeat_is_evaluated_per_row() {
    let table = TableIR::range(3, 2)
        .unwrap()
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "items".into(),
                repeat_value(ScalarIR::Column { name: "idx".into() }, 16_384),
            ),
        ])
        .unwrap();
    let partitions = table.collect_partitions().await.unwrap();
    let mut actual = Vec::new();
    for batch in partitions.into_iter().flatten() {
        let indices = batch
            .column_by_name("idx")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let items = batch
            .column_by_name("items")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let value = indices.value(row);
            let elements = items.value(row);
            let elements = elements.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(elements.len(), 16_384);
            assert!(elements.values().iter().all(|&element| element == value));
            actual.push((value, elements.value(0)));
        }
    }
    assert_eq!(actual, vec![(0, 0), (1, 1), (2, 2)]);
}

#[tokio::test]
async fn large_repeat_after_empty_row_projection_preserves_cardinality() {
    for rows in [0, 1, 5] {
        let table = TableIR::range(rows, 3)
            .unwrap()
            .key_by(vec![])
            .unwrap()
            .map_rows(vec![])
            .unwrap()
            .map_rows(vec![(
                "length".into(),
                ScalarIR::ArrayLength {
                    value: Arc::new(repeat(16_384)),
                },
            )])
            .unwrap();
        assert_eq!(table.schema().fields().len(), 1);
        assert_eq!(table.schema().field(0).name(), "length");
        assert_eq!(
            table.schema().field(0).data_type(),
            &arrow_schema::DataType::Int32
        );

        let partitions = table.collect_partitions().await.unwrap();
        assert_eq!(
            partitions
                .iter()
                .flatten()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            rows as usize
        );
        for batch in partitions.into_iter().flatten() {
            let lengths = batch
                .column_by_name("length")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert!(lengths.values().iter().all(|&length| length == 16_384));
        }
    }
}

#[tokio::test]
async fn grouped_aggregate_then_large_repeat_preserves_aggregate_row() {
    let source = TableIR::range(5, 3).unwrap();
    let grouped = source
        .grouped_aggregate_bound(
            vec![],
            vec![(
                "n".into(),
                ScalarIR::Aggregate {
                    result_type: None,
                    function: "count".into(),
                    value: None,
                    options: json!({}),
                    init: vec![],
                },
            )],
            vail_table::globals::GlobalBindings::new(),
        )
        .unwrap();
    let mapped = grouped
        .map_rows(vec![
            ("n".into(), ScalarIR::Column { name: "n".into() }),
            (
                "length".into(),
                ScalarIR::ArrayLength {
                    value: Arc::new(repeat(16_384)),
                },
            ),
        ])
        .unwrap();
    let batches = mapped
        .collect_partitions()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let row = &batches[0];
    assert_eq!(
        row.column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[5]
    );
    assert_eq!(
        row.column_by_name("length")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[16_384]
    );
}

#[tokio::test]
async fn large_repeat_after_constant_projection_preserves_empty_and_nonempty_rows() {
    for rows in [0, 1, 5] {
        let table = TableIR::range(rows, 3)
            .unwrap()
            .key_by(vec![])
            .unwrap()
            .map_rows(vec![("constant".into(), literal_i32(11))])
            .unwrap()
            .map_rows(vec![
                (
                    "constant".into(),
                    ScalarIR::Column {
                        name: "constant".into(),
                    },
                ),
                (
                    "length".into(),
                    ScalarIR::ArrayLength {
                        value: Arc::new(repeat(16_384)),
                    },
                ),
            ])
            .unwrap();
        assert_eq!(table.schema().fields().len(), 2);
        let partitions = table.collect_partitions().await.unwrap();
        let batches = partitions.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            rows as usize
        );
        for batch in batches {
            let constant = batch
                .column_by_name("constant")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let length = batch
                .column_by_name("length")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert!(constant.values().iter().all(|&value| value == 11));
            assert!(length.values().iter().all(|&value| value == 16_384));
        }
    }
}

#[tokio::test]
async fn large_repeat_after_semantic_partition_map_preserves_partition_order() {
    let source = TableIR::range(3, 5)
        .unwrap()
        .with_keys(vec!["idx".into()])
        .unwrap();
    let baseline = collected_i32_rows(source.collect_partitions().await.unwrap(), "idx");
    let mapped_partitions = source
        .map_partitions(
            "rows".into(),
            ScalarIR::Column {
                name: "rows".into(),
            },
            vail_table::globals::GlobalBindings::new(),
        )
        .unwrap();
    let output = mapped_partitions
        .map_rows(vec![
            ("idx".into(), ScalarIR::Column { name: "idx".into() }),
            (
                "length".into(),
                ScalarIR::ArrayLength {
                    value: Arc::new(repeat(16_384)),
                },
            ),
        ])
        .unwrap();
    let partitions = output.collect_partitions().await.unwrap();
    assert_eq!(collected_i32_rows(partitions.clone(), "idx"), baseline);
    assert_eq!(partitions.len(), baseline.len());
    for partition in partitions {
        for batch in partition {
            let lengths = batch
                .column_by_name("length")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert!(lengths.values().iter().all(|&length| length == 16_384));
        }
    }
}
