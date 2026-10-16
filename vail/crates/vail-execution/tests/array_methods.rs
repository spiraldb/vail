//! Array callbacks through the public native expression boundary.
use arrow_array::{
    Array, ArrayRef, LargeStringArray, ListArray, RecordBatch, StringArray, StringViewArray,
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
                            name: "item".into(),
                        }),
                    }),
                    consequent: Arc::new(Expr::LambdaVariable { name: "acc".into() }),
                    alternate: Arc::new(Expr::LambdaVariable {
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
