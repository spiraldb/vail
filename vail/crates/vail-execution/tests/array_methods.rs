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
