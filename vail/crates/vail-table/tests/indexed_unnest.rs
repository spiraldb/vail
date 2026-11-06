//! Public row expansion must not copy entry fields discarded by its consumer.
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use vail_execution::expressions::{
    ArrayReduction, BinaryOperator, Expr, PrimitiveType, Type, UnaryOperator,
};
use vail_table::{TableIR, TableRef, partitioned};

struct Counter;
static COUNT: AtomicBool = AtomicBool::new(false);
static BYTES: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            BYTES.fetch_add(size, Ordering::Relaxed);
        }
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Counter = Counter;
fn int() -> Type {
    Type::Primitive(PrimitiveType::Int32)
}
fn array(value: Type) -> Type {
    Type::Array {
        array: Box::new(value),
    }
}
fn col(name: &str) -> Expr {
    Expr::Column { name: name.into() }
}
fn field(value: Expr, name: &str) -> Expr {
    Expr::GetField {
        value: Arc::new(value),
        name: name.into(),
    }
}
fn list(values: ArrayRef, offsets: Vec<i32>, nulls: Option<NullBuffer>) -> ListArray {
    ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        nulls,
    )
}
fn structure(fields: Vec<(&str, ArrayRef)>, nulls: Option<NullBuffer>) -> StructArray {
    StructArray::new(
        fields
            .iter()
            .map(|(name, value)| Field::new(*name, value.data_type().clone(), true))
            .collect::<Vec<_>>()
            .into(),
        fields.into_iter().map(|(_, value)| value).collect(),
        nulls,
    )
}

fn source_fixture(width: usize) -> (TableRef, Type) {
    let n = 32;
    let payload = list(
        Arc::new(Int32Array::from(vec![17; n * width])),
        (0..=n).map(|i| (i * width) as i32).collect(),
        None,
    );
    let values = structure(
        vec![
            (
                "x",
                Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>())),
            ),
            ("payload", Arc::new(payload)),
        ],
        Some(NullBuffer::from(
            (0..n).map(|i| i % 5 != 0).collect::<Vec<_>>(),
        )),
    );
    let slots = structure(vec![("value", Arc::new(values))], None);
    let entry_type = Type::from_arrow(slots.data_type()).unwrap();
    let cells = list(
        Arc::new(slots),
        vec![0, 8, 16, 24, 32],
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let choices = list(
        Arc::new(Int32Array::from(vec![2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1])),
        vec![0, 3, 6, 9, 12],
        None,
    );
    let batch = RecordBatch::try_from_iter([
        ("cells", Arc::new(cells) as ArrayRef),
        ("choice", Arc::new(choices) as ArrayRef),
        (
            "id",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
    ])
    .unwrap();
    let source = TableIR::parallelize(batch, 1).unwrap();
    (source, entry_type)
}

fn selected_entries(source: TableRef, entry_type: Type, predicate: Option<Expr>) -> TableRef {
    selected_entries_with_metadata(source, entry_type, predicate, &[])
}

fn selected_entries_with_metadata(
    source: TableRef, entry_type: Type, predicate: Option<Expr>, metadata: &[(String, Expr)],
) -> TableRef {
    let expanded = source.explode(vec!["choice".into()]).unwrap();
    let expanded = match predicate {
        Some(predicate) => expanded.filter(predicate, true).unwrap(),
        None => expanded,
    };
    let mut expanded_fields = vec![
            ("cells".into(), col("cells")),
            (
                "rows".into(),
                Expr::Struct {
                    fields: vec![(
                        "__vail_aligned_row".into(),
                        Expr::Struct {
                            fields: vec![("index".into(), col("choice"))],
                        },
                    )],
                },
            ),
        ];
    expanded_fields.extend(metadata.iter().map(|(name, _)| (name.clone(), col(name))));
    let expanded = expanded.map_rows(expanded_fields).unwrap();
    let input = Expr::AlignedMapInput {
        axis: "entry".into(),
        name: None,
        dtype: entry_type,
    };
    let value = field(input, "value");
    let result_type = Type::Struct {
        r#struct: vec![
            ("x".into(), int()),
            ("payload".into(), array(int())),
            ("selected".into(), int()),
        ],
    };
    let aligned = Expr::AlignedMatrixMap {
        entries: Arc::new(col("cells")),
        rows: Arc::new(col("rows")),
        columns: Arc::new(Expr::Literal {
            dtype: array(int()),
            value: serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7]),
        }),
        globals: Arc::new(Expr::Struct { fields: vec![] }),
        body: Arc::new(Expr::IfElse {
            condition: Arc::new(Expr::AlignedMapInput {
                axis: "entry_present".into(),
                name: None,
                dtype: Type::Primitive(PrimitiveType::Bool),
            }),
            consequent: Arc::new(Expr::Struct {
                fields: vec![
                    ("x".into(), field(value.clone(), "x")),
                    ("payload".into(), field(value.clone(), "payload")),
                    (
                        "selected".into(),
                        Expr::ArrayReduce {
                            operation: ArrayReduction::Sum,
                            filter_missing: true,
                            value: Arc::new(Expr::Array {
                                element_type: int(),
                                elements: vec![
                                    field(value, "x"),
                                    Expr::AlignedMapInput {
                                        axis: "row".into(),
                                        name: Some("index".into()),
                                        dtype: int(),
                                    },
                                ],
                            }),
                        },
                    ),
                ],
            }),
            alternate: Arc::new(Expr::Literal {
                dtype: result_type.clone(),
                value: serde_json::Value::Null,
            }),
        }),
        dtype: array(Type::Struct {
            r#struct: vec![("value".into(), result_type)],
        }),
        preserve_cell: true,
    };
    let mut updated_fields = vec![("cells".into(), aligned)];
    updated_fields.extend_from_slice(metadata);
    let updated = expanded.map_rows(updated_fields).unwrap();
    let cell_type = Type::Struct {
        r#struct: vec![(
            "value".into(),
            Type::Struct {
                r#struct: vec![
                    ("x".into(), int()),
                    ("payload".into(), array(int())),
                    ("selected".into(), int()),
                ],
            },
        )],
    };
    let value = field(
        Expr::LambdaVariable {
            name: "cell".into(),
            dtype: Some(cell_type),
        },
        "value",
    );
    let selected_type = Type::Struct {
        r#struct: vec![("selected".into(), int())],
    };
    let selected = Expr::ArrayMap {
        value: Arc::new(col("cells")),
        parameter: "cell".into(),
        body: Arc::new(Expr::Struct {
            fields: vec![(
                "value".into(),
                Expr::IfElse {
                    condition: Arc::new(Expr::Unary {
                        operator: UnaryOperator::Not,
                        value: Arc::new(Expr::IsMissing {
                            value: Arc::new(value.clone()),
                        }),
                    }),
                    consequent: Arc::new(Expr::Struct {
                        fields: vec![("selected".into(), field(value, "selected"))],
                    }),
                    alternate: Arc::new(Expr::Literal {
                        dtype: selected_type,
                        value: serde_json::Value::Null,
                    }),
                },
            )],
        }),
    };
    let mut selected_fields = vec![("cells".into(), selected)];
    selected_fields.extend_from_slice(metadata);
    updated.map_rows(selected_fields).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn expanded_rows_read_source_entries_without_copying_discarded_payload() {
    let mut costs = Vec::new();
    for width in [2, 4096] {
        let (source, entry_type) = source_fixture(width);
        let selected = selected_entries(source.clone(), entry_type, None);
        BYTES.store(0, Ordering::Relaxed);
        COUNT.store(true, Ordering::Relaxed);
        let result = selected.collect_partitions().await;
        COUNT.store(false, Ordering::Relaxed);
        costs.push(BYTES.load(Ordering::Relaxed));
        let result = result.unwrap();
        let mut actual = Vec::new();
        for batch in result.iter().flatten() {
            let lists = batch
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in lists.iter() {
                actual.push(row.map(|row| {
                    let slots = row.as_any().downcast_ref::<StructArray>().unwrap();
                    assert_eq!(slots.null_count(), 0);
                    let values = slots
                        .column(0)
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap();
                    let selected = values
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    (0..values.len())
                        .map(|i| {
                            (!values.is_null(i)).then(|| {
                                assert!(selected.is_valid(i));
                                selected.value(i)
                            })
                        })
                        .collect::<Vec<_>>()
                }));
            }
        }
        let expected = (0..4)
            .flat_map(|r| {
                [2, 0, 1].map(move |choice| {
                    (r != 1).then(|| {
                        (r * 8..r * 8 + 8)
                            .map(|i| (i % 5 != 0).then_some(i + choice))
                            .collect::<Vec<_>>()
                    })
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        let original = source.collect_partitions().await.unwrap();
        let mut observed = 0;
        for batch in original.iter().flatten() {
            let lists = batch
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in lists.iter() {
                let parent = observed;
                observed += 1;
                if parent == 1 {
                    assert!(row.is_none());
                    continue;
                }
                let row = row.unwrap();
                let slots = row.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(slots.null_count(), 0);
                let entries = slots
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let x = entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let payload = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                for i in 0..entries.len() {
                    let index = parent * 8 + i;
                    assert_eq!(entries.is_valid(i), index % 5 != 0);
                    if entries.is_null(i) {
                        continue;
                    }
                    assert!(x.is_valid(i));
                    assert_eq!(x.value(i), index as i32);
                    assert!(payload.is_valid(i));
                    let values = payload.value(i);
                    let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(values.len(), width);
                    assert!(values.iter().all(|v| v == Some(17)));
                }
            }
        }
        assert_eq!(observed, 4);
    }
    eprintln!("expanded entry allocation small/large unused payload: {costs:?}");
    assert!(
        costs[1] <= costs[0] + 256 * 1024,
        "expansion copied discarded payload: {costs:?}"
    );
}

fn mapped_source(source: &TableRef, cell_type: &Type) -> TableRef {
    let Type::Struct { r#struct: fields } = cell_type else {
        unreachable!()
    };
    let value_type = fields[0].1.clone();
    let value = field(
        Expr::AlignedMapInput {
            axis: "entry".into(),
            name: None,
            dtype: cell_type.clone(),
        },
        "value",
    );
    let body = Expr::Struct {
        fields: vec![
            ("x".into(), field(value.clone(), "x")),
            (
                "payload".into(),
                Expr::ArrayMap {
                    value: Arc::new(field(value, "payload")),
                    parameter: "v".into(),
                    body: Arc::new(Expr::Binary {
                        operator: BinaryOperator::Add,
                        left: Arc::new(Expr::LambdaVariable {
                            name: "v".into(),
                            dtype: Some(int()),
                        }),
                        right: Arc::new(Expr::Literal {
                            dtype: int(),
                            value: serde_json::json!(1),
                        }),
                    }),
                },
            ),
        ],
    };
    let map = Expr::AlignedMatrixMap {
        entries: Arc::new(col("cells")),
        rows: Arc::new(Expr::Struct { fields: vec![] }),
        columns: Arc::new(Expr::Literal {
            dtype: array(int()),
            value: serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7]),
        }),
        globals: Arc::new(Expr::Struct { fields: vec![] }),
        preserve_cell: true,
        dtype: array(cell_type.clone()),
        body: Arc::new(Expr::IfElse {
            condition: Arc::new(Expr::AlignedMapInput {
                axis: "entry_present".into(),
                name: None,
                dtype: Type::Primitive(PrimitiveType::Bool),
            }),
            consequent: Arc::new(body),
            alternate: Arc::new(Expr::Literal {
                dtype: value_type,
                value: serde_json::Value::Null,
            }),
        }),
    };
    source
        .map_rows(vec![
            ("cells".into(), map),
            ("choice".into(), col("choice")),
            ("id".into(), col("id")),
        ])
        .unwrap()
}
fn compare(left: Expr, operator: BinaryOperator, right: i32) -> Expr {
    Expr::Binary {
        operator,
        left: Arc::new(left),
        right: Arc::new(Expr::Literal {
            dtype: int(),
            value: serde_json::json!(right),
        }),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn filtered_entry_maps_skip_unused_batch_materialization() {
    let mut costs = Vec::new();
    for width in [2, 16384] {
        let (source, entry_type) = source_fixture(width);
        let producer = mapped_source(&source, &entry_type);
        let none = selected_entries(
            producer.clone(),
            entry_type.clone(),
            Some(compare(col("choice"), BinaryOperator::Gt, 3)),
        );
        BYTES.store(0, Ordering::Relaxed);
        COUNT.store(true, Ordering::Relaxed);
        let result = none.collect_partitions().await;
        COUNT.store(false, Ordering::Relaxed);
        costs.push(BYTES.load(Ordering::Relaxed));
        assert_eq!(
            result
                .unwrap()
                .iter()
                .flatten()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        for one in [true, false] {
            let predicate = if one {
                Expr::Binary {
                    operator: BinaryOperator::And,
                    left: Arc::new(compare(col("id"), BinaryOperator::Eq, 0)),
                    right: Arc::new(compare(col("choice"), BinaryOperator::Eq, 2)),
                }
            } else {
                compare(col("choice"), BinaryOperator::Ge, 0)
            };
            let query = selected_entries(producer.clone(), entry_type.clone(), Some(predicate));
            let batches = query.collect_partitions().await.unwrap();
            let mut actual = Vec::new();
            for batch in batches.iter().flatten() {
                let lists = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                for row in lists.iter() {
                    actual.push(row.map(|row| {
                        let cells = row.as_any().downcast_ref::<StructArray>().unwrap();
                        assert_eq!(cells.null_count(), 0);
                        let values = cells
                            .column(0)
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .unwrap();
                        let numbers = values
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        (0..values.len())
                            .map(|i| {
                                if values.is_null(i) {
                                    None
                                } else {
                                    assert!(numbers.is_valid(i));
                                    Some(numbers.value(i))
                                }
                            })
                            .collect::<Vec<_>>()
                    }));
                }
            }
            let expected = if one {
                vec![Some(
                    (0..8)
                        .map(|i| (i % 5 != 0).then_some(i + 2))
                        .collect::<Vec<_>>(),
                )]
            } else {
                (0..4)
                    .flat_map(|r| {
                        [2, 0, 1].map(move |choice| {
                            (r != 1).then(|| {
                                (r * 8..r * 8 + 8)
                                    .map(|i| (i % 5 != 0).then_some(i + choice))
                                    .collect::<Vec<_>>()
                            })
                        })
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(actual, expected);
        }
        assert_eq!(
            source
                .collect_partitions()
                .await
                .unwrap()
                .iter()
                .flatten()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );
    }
    eprintln!("no-survivor producer allocation small/large: {costs:?}");
    assert!(
        costs[1] <= costs[0] + 256 * 1024,
        "filtered-out producer materialized its payload: {costs:?}"
    );
}


#[tokio::test(flavor = "current_thread")]
async fn wide_computed_metadata_preserves_values_and_reports_preparation_cost() {
    let mut costs = Vec::new();
    for kind in ["computed", "forwarded", "literal"] {
        for width in [8, 256] {
            let (source, entry_type) = source_fixture(2);
            let names = (0..width).map(|i| format!("metadata_{i}")).collect::<Vec<_>>();
            let mut fields = source.schema().fields().iter()
                .map(|field| (field.name().clone(), col(field.name())))
                .collect::<Vec<_>>();
            fields.extend(names.iter().map(|name| (name.clone(), col("id"))));
            let source = source.map_rows(fields).unwrap();
            let metadata = names.iter().enumerate().map(|(i, name)| {
                let value = Expr::Literal { dtype: int(), value: serde_json::json!(i as i32) };
                let expression = match kind {
                    "computed" => Expr::Binary {
                        operator: BinaryOperator::Add,
                        left: Arc::new(col(name)), right: Arc::new(value),
                    },
                    "literal" => value,
                    _ => col(name),
                };
                (name.clone(), expression)
            }).collect::<Vec<_>>();
            let query = selected_entries_with_metadata(source, entry_type, None, &metadata);
            // Fixture buffers and expression construction are outside this counter.
            BYTES.store(0, Ordering::Relaxed);
            COUNT.store(true, Ordering::Relaxed);
            let prepared = partitioned::prepare(query.clone()).await;
            COUNT.store(false, Ordering::Relaxed);
            costs.push((kind, width, BYTES.load(Ordering::Relaxed)));
            let _ = prepared.unwrap();
            let batches = query.collect_partitions().await.unwrap();
            let mut actual_entries = Vec::new();
            let mut row_number = 0;
            for batch in batches.iter().flatten() {
                assert_eq!(batch.num_columns(), width + 1);
                for (index, name) in names.iter().enumerate() {
                    assert_eq!(batch.schema().field(index + 1).name(), name);
                    assert_eq!(batch.schema().field(index + 1).data_type(), &DataType::Int32);
                    let values = batch.column(index + 1).as_any().downcast_ref::<Int32Array>().unwrap();
                    for row in 0..batch.num_rows() {
                        let source_row = (row_number + row) / 3;
                        let expected = match kind {
                            "computed" => source_row as i32 + 2 * index as i32,
                            "literal" => index as i32,
                            _ => source_row as i32,
                        };
                        assert!(values.is_valid(row));
                        assert_eq!(values.value(row), expected);
                    }
                }
                let lists = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
                for row in lists.iter() {
                    actual_entries.push(row.map(|row| {
                        let cells = row.as_any().downcast_ref::<StructArray>().unwrap();
                        let entries = cells.column(0).as_any().downcast_ref::<StructArray>().unwrap();
                        let values = entries.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                        (0..entries.len()).map(|i| (!entries.is_null(i)).then(|| values.value(i))).collect::<Vec<_>>()
                    }));
                }
                row_number += batch.num_rows();
            }
            let expected = (0..4).flat_map(|row| [2, 0, 1].map(move |choice| {
                (row != 1).then(|| (row * 8..row * 8 + 8)
                    .map(|i| (i % 5 != 0).then_some(i + choice)).collect::<Vec<_>>())
            })).collect::<Vec<_>>();
            assert_eq!(row_number, 12);
            assert_eq!(actual_entries, expected);
        }
    }
    eprintln!("metadata preparation allocated bytes (fixtures excluded): {costs:?}");
}
