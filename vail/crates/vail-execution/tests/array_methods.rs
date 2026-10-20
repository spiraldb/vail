//! Array callbacks through the public native expression boundary.
use arrow_array::{
    Array, ArrayRef, BooleanArray, LargeStringArray, ListArray, RecordBatch, StringArray,
    StringViewArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{Field, Schema};
use std::sync::Arc;
use vail_execution::expressions::{Expr, lower};

fn column(name: &str) -> Arc<Expr> {
    Arc::new(Expr::Column { name: name.into() })
}

#[test]
fn fold_and_scan_accept_alternate_string_storage() {
    for view in [false, true] {
        let values: ArrayRef = if view {
            Arc::new(StringViewArray::from(vec![
                Some("a"),
                None,
                Some("b"),
                Some("c"),
            ]))
        } else {
            Arc::new(LargeStringArray::from(vec![
                Some("a"),
                None,
                Some("b"),
                Some("c"),
            ]))
        };
        let zero: ArrayRef = if view {
            Arc::new(StringViewArray::from(vec!["z0", "z1", "z2", "z3"]))
        } else {
            Arc::new(LargeStringArray::from(vec!["z0", "z1", "z2", "z3"]))
        };
        let lists: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0i32, 3, 3, 3, 4].into()),
            values,
            Some(NullBuffer::from(vec![true, true, false, true])),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", lists.data_type().clone(), true),
            Field::new("zero", zero.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![lists, zero]).unwrap();
        for scan in [false, true] {
            let expression = Expr::ArrayFold {
                value: column("a"),
                zero: column("zero"),
                accumulator: "acc".into(),
                parameter: "item".into(),
                body: Arc::new(Expr::IfElse {
                    condition: Arc::new(Expr::IsMissing {
                        value: Arc::new(Expr::LambdaVariable {
                            dtype: None,
                            name: "item".into(),
                        }),
                    }),
                    consequent: Arc::new(Expr::LambdaVariable {
                        dtype: None,
                        name: "acc".into(),
                    }),
                    alternate: Arc::new(Expr::LambdaVariable {
                        dtype: None,
                        name: "item".into(),
                    }),
                }),
                scan,
            };
            let result = lower(&expression, &schema)
                .unwrap()
                .evaluate(&batch)
                .unwrap()
                .into_array(4)
                .unwrap();
            if scan {
                let lists = result.as_any().downcast_ref::<ListArray>().unwrap();
                assert!(lists.is_null(2));
                for (row, expected) in [
                    (0, vec!["z0", "a", "a", "b"]),
                    (1, vec!["z1"]),
                    (3, vec!["z3", "c"]),
                ] {
                    let values = lists.value(row);
                    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(
                        values.iter().collect::<Vec<_>>(),
                        expected.into_iter().map(Some).collect::<Vec<_>>()
                    );
                }
            } else {
                let values = result.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    vec![Some("b"), Some("z1"), None, Some("c")]
                );
            }
        }
    }
}

#[test]
fn list_any_uses_native_reducer_empty_and_missing_identity() {
    use arrow_schema::DataType;
    use vail_execution::expressions::{PrimitiveType, Type};

    let values = Arc::new(BooleanArray::from(vec![
        None,
        None,
        Some(false),
        None,
        Some(true),
        None,
    ])) as ArrayRef;
    let list = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Boolean, true)),
        OffsetBuffer::new(vec![0i32, 0, 2, 4, 6, 6].into()),
        values,
        Some(NullBuffer::from(vec![true, true, true, true, false])),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "values",
        list.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![list]).unwrap();
    let expression = Expr::NestedFunction {
        function: "__vail_list_reduce_any".into(),
        args: vec![Expr::Column {
            name: "values".into(),
        }],
        dtype: Type::Primitive(PrimitiveType::Bool),
    };
    let result = lower(&expression, &schema)
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(5)
        .unwrap();
    let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        result.iter().collect::<Vec<_>>(),
        vec![Some(false), Some(false), Some(false), Some(true), None]
    );
}

#[test]
fn any_array_aggregate_scan_is_exclusive_for_empty_and_missing_lists() {
    use arrow_schema::DataType;
    use vail_execution::expressions::{PrimitiveType, Type};

    let offsets = OffsetBuffer::new(vec![0i32, 3, 3, 5, 5].into());
    let field = Arc::new(Field::new_list_field(DataType::Boolean, true));
    let values = Arc::new(ListArray::new(
        field.clone(),
        offsets.clone(),
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            None,
            None,
        ])),
        Some(NullBuffer::from(vec![true, true, true, false])),
    ));
    let predicates = Arc::new(ListArray::new(
        field,
        offsets,
        Arc::new(BooleanArray::from(vec![true, true, false, true, true])),
        Some(NullBuffer::from(vec![true, true, true, false])),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("values", values.data_type().clone(), true),
        Field::new("predicate", predicates.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values, predicates]).unwrap();
    let array_bool = Type::Array {
        array: Box::new(Type::Primitive(PrimitiveType::Bool)),
    };
    let expression = Expr::ArrayAggregateScan {
        function: "any".into(),
        value: column("values"),
        predicate: column("predicate"),
        options: serde_json::Value::Null,
        dtype: array_bool,
    };
    let result = lower(&expression, &schema)
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(4)
        .unwrap();
    let result = result.as_any().downcast_ref::<ListArray>().unwrap();
    let first = result.value(0);
    assert_eq!(
        first
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false), Some(true), Some(true)]
    );
    assert_eq!(result.value(1).len(), 0);
    assert_eq!(
        result
            .value(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false), Some(false)]
    );
    assert!(result.is_null(3));
}

#[test]
fn ndarray_constructor_checks_numeric_input_and_declared_rank() {
    use vail_execution::expressions::{PrimitiveType as P, Type};
    for (element, rank, valid) in [
        (P::Int32, 1, true),
        (P::Float64, 1, true),
        (P::Str, 1, false),
        (P::Int32, 2, false),
    ] {
        let element = Type::Primitive(element);
        let schema = Schema::new(vec![Field::new(
            "a",
            Type::Array {
                array: Box::new(element.clone()),
            }
            .to_arrow()
            .unwrap(),
            true,
        )]);
        let expression = Expr::NestedFunction {
            function: "__vail_ndarray_from_array".into(),
            args: vec![Expr::Column { name: "a".into() }],
            dtype: Type::Ndarray {
                ndarray: Box::new((element, rank)),
            },
        };
        assert_eq!(lower(&expression, &schema).is_ok(), valid);
    }
}

#[test]
fn list_scan_builds_primitive_and_collection_prefixes_with_masks() {
    use arrow_array::{Int64Array, types::Int64Type};
    use arrow_schema::DataType;
    use vail_execution::expressions::{PrimitiveType, Type};
    let values = Arc::new(
        ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(99)]),
            Some(vec![Some(2), None, Some(4), Some(8)]),
            Some(vec![]),
            None,
        ])
        .slice(1, 3),
    );
    let predicates = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Boolean, true)),
        OffsetBuffer::from_lengths([4, 0, 0]),
        Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            None,
            Some(true),
        ])),
        Some(NullBuffer::from(vec![true, true, false])),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("values", values.data_type().clone(), true),
        Field::new("predicate", predicates.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values, predicates]).unwrap();
    let integer = Type::Primitive(PrimitiveType::Int64);
    for function in ["sum", "collect"] {
        let element = if function == "collect" {
            Type::Array {
                array: Box::new(integer.clone()),
            }
        } else {
            integer.clone()
        };
        let expr = Expr::ArrayAggregateScan {
            function: function.into(),
            value: column("values"),
            predicate: column("predicate"),
            options: serde_json::Value::Null,
            dtype: Type::Array {
                array: Box::new(element),
            },
        };
        let result = lower(&expr, &schema)
            .unwrap()
            .evaluate(&batch)
            .unwrap()
            .into_array(3)
            .unwrap();
        let lists = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value(1).len(), 0);
        assert!(lists.is_null(2));
        let prefixes = lists.value(0);
        if function == "sum" {
            assert_eq!(
                prefixes.as_any().downcast_ref::<Int64Array>().unwrap(),
                &Int64Array::from(vec![0, 2, 2, 2])
            );
        } else {
            let prefixes = prefixes.as_any().downcast_ref::<ListArray>().unwrap();
            let expected = [
                vec![],
                vec![Some(2)],
                vec![Some(2), None],
                vec![Some(2), None],
            ];
            for (row, expected) in expected.into_iter().enumerate() {
                assert_eq!(
                    prefixes
                        .value(row)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &Int64Array::from(expected)
                );
            }
        }
    }
}
