//! Public native expression executions over sliced, nullable Arrow batches.
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StructArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use std::sync::Arc;
use vail_execution::expressions::{Expr, Type, lower};

fn lists(rows: Vec<Vec<Option<i32>>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        rows.into_iter().map(Some),
    ))
}
fn list_wrap(values: ArrayRef, lengths: Vec<usize>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::from_lengths(lengths),
        values,
        None,
    ))
}
fn execute(
    name: &str,
    inputs: Vec<(ArrayRef, bool)>,
    output: &DataType,
) -> Result<ArrayRef, String> {
    let rows = inputs
        .iter()
        .find(|(_, scalar)| !scalar)
        .map(|(a, _)| a.len())
        .unwrap_or(1);
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    let mut args = Vec::new();
    for (i, (array, scalar)) in inputs.into_iter().enumerate() {
        if scalar {
            args.push(Expr::RuntimeLiteral {
                value: ScalarValue::try_from_array(&array, 0).unwrap(),
            });
        } else {
            let name = format!("a{i}");
            fields.push(Field::new(&name, array.data_type().clone(), true));
            columns.push(array);
            args.push(Expr::Column { name });
        }
    }
    // All-scalar executions still evaluate against a public one-row batch.
    if columns.is_empty() {
        fields.push(Field::new("unused", DataType::Int32, false));
        columns.push(Arc::new(Int32Array::from(vec![0; rows])));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let expression = Expr::NestedFunction {
        function: name.into(),
        args,
        dtype: Type::from_arrow(output).unwrap(),
    };
    lower(&expression, &schema)
        .map_err(|e| e.to_string())?
        .evaluate(&batch)
        .map_err(|e| e.to_string())?
        .into_array(rows)
        .map_err(|e| e.to_string())
}
#[test]
fn gather_preserves_scalar_owners_slices_repetition_and_empty_lists() {
    let source = lists(vec![vec![Some(99)], vec![Some(7), None, Some(8)], vec![]]).slice(1, 2);
    let positions = lists(vec![
        vec![Some(99)],
        vec![Some(2), Some(0), Some(2)],
        vec![],
    ])
    .slice(1, 2);
    let expected = lists(vec![vec![Some(8), Some(7), Some(8)], vec![]]);
    assert_eq!(
        execute(
            "__vail_list_take",
            vec![(source.clone(), false), (positions, false)],
            expected.data_type()
        )
        .unwrap()
        .as_ref(),
        expected.as_ref()
    );
    let expected = lists(vec![vec![Some(8)], vec![None, Some(7)]]);
    let positions = lists(vec![vec![Some(2)], vec![Some(1), Some(0)]]);
    assert_eq!(
        execute(
            "__vail_list_take",
            vec![(source.slice(0, 1), true), (positions, false)],
            expected.data_type()
        )
        .unwrap()
        .as_ref(),
        expected.as_ref()
    );
    for bad in [vec![Some(-1)], vec![Some(3)], vec![None]] {
        assert!(
            execute(
                "__vail_list_take",
                vec![(source.slice(0, 1), true), (lists(vec![bad]), true)],
                source.data_type()
            )
            .is_err()
        );
    }
}
#[test]
fn grouped_gather_preserves_empty_groups_and_scalar_positions() {
    let source = lists(vec![
        vec![Some(7), Some(2), Some(8)],
        vec![Some(4), Some(5), Some(6)],
    ]);
    let positions = list_wrap(
        lists(vec![vec![Some(2), Some(0)], vec![], vec![Some(1)]]),
        vec![3],
    );
    let expected = list_wrap(
        lists(vec![
            vec![Some(8), Some(7)],
            vec![],
            vec![Some(2)],
            vec![Some(6), Some(4)],
            vec![],
            vec![Some(5)],
        ]),
        vec![3, 3],
    );
    assert_eq!(
        execute(
            "__vail_list_take_groups",
            vec![(source, false), (positions, true)],
            expected.data_type()
        )
        .unwrap()
        .as_ref(),
        expected.as_ref()
    );
}
fn records(ids: Vec<Option<i64>>, cells: bool) -> ArrayRef {
    let length = ids.len();
    let mut fields = vec![Arc::new(Field::new(
        "__vail_matrix_col_id",
        DataType::Int64,
        true,
    ))];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids))];
    if cells {
        fields.push(Arc::new(Field::new("present", DataType::Boolean, true)));
        arrays.push(Arc::new(BooleanArray::from(vec![true; length])));
    }
    fields.push(Arc::new(Field::new(
        if cells { "value" } else { "__vail_matrix_col" },
        DataType::Struct(Vec::<Field>::new().into()),
        true,
    )));
    arrays.push(Arc::new(StructArray::new_empty_fields(length, None)));
    Arc::new(StructArray::new(fields.into(), arrays, None))
}
#[test]
fn alignment_compares_exact_positional_ids_against_shared_columns() {
    let cells = list_wrap(
        records(vec![Some(7), Some(2), Some(7), Some(2)], true),
        vec![2, 2],
    );
    let columns = list_wrap(records(vec![Some(7), Some(2)], false), vec![2]);
    let expected: ArrayRef = Arc::new(BooleanArray::from(vec![true, true]));
    assert_eq!(
        execute(
            "__vail_matrix_aligned",
            vec![(cells.clone(), false), (columns.clone(), true)],
            &DataType::Boolean
        )
        .unwrap()
        .as_ref(),
        expected.as_ref()
    );
    for bad in [vec![Some(2), Some(7)], vec![Some(7)], vec![None, Some(2)]] {
        assert!(
            execute(
                "__vail_matrix_aligned",
                vec![
                    (cells.clone(), false),
                    (
                        list_wrap(records(bad.clone(), false), vec![bad.len()]),
                        true
                    )
                ],
                &DataType::Boolean
            )
            .is_err()
        );
    }
    assert!(
        execute(
            "__vail_matrix_columns",
            vec![(columns, true)],
            cells.data_type()
        )
        .is_err()
    ); // declared output cannot hide a wrong schema
    let duplicates = list_wrap(records(vec![Some(7), Some(7)], false), vec![2]);
    assert!(
        execute(
            "__vail_matrix_columns",
            vec![(duplicates.clone(), true)],
            duplicates.data_type()
        )
        .is_err()
    );
}

#[test]
fn alignment_fast_path_handles_sliced_and_nullable_arrays() {
    use arrow_buffer::NullBuffer;

    // Both lists are sliced after construction so their offsets are nonzero.
    // The aligned row must compare the sliced child spans rather than assume
    // either list starts at child offset zero.
    let cells = list_wrap(records(vec![Some(99), Some(7), Some(2)], true), vec![1, 2]).slice(1, 1);
    let columns =
        list_wrap(records(vec![Some(88), Some(7), Some(2)], false), vec![1, 2]).slice(1, 1);
    let expected: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    assert_eq!(
        execute(
            "__vail_matrix_aligned",
            vec![(cells.clone(), false), (columns.clone(), true)],
            &DataType::Boolean
        )
        .unwrap()
        .as_ref(),
        expected.as_ref()
    );

    let reordered =
        list_wrap(records(vec![Some(88), Some(2), Some(7)], false), vec![1, 2]).slice(1, 1);
    assert!(
        execute(
            "__vail_matrix_aligned",
            vec![(cells.clone(), false), (reordered, true)],
            &DataType::Boolean
        )
        .unwrap_err()
        .contains("identity differs")
    );

    // An explicitly present, all-valid identity bitmap selects the nullable
    // fallback; it must still accept the valid row and preserve null errors.
    let valid = records(vec![Some(7), Some(2)], true);
    let valid = valid.as_any().downcast_ref::<StructArray>().unwrap();
    let mut nullable_columns = valid.columns().to_vec();
    nullable_columns[0] = Arc::new(Int64Array::new(
        vec![7, 2].into(),
        Some(NullBuffer::from(vec![true, true])),
    ));
    let nullable_records = Arc::new(StructArray::new(
        valid.fields().clone(),
        nullable_columns.clone(),
        None,
    ));
    let nullable_cells = list_wrap(nullable_records.clone(), vec![2]);
    let aligned_columns = list_wrap(records(vec![Some(7), Some(2)], false), vec![2]);
    assert_eq!(
        execute(
            "__vail_matrix_aligned",
            vec![
                (nullable_cells.clone(), false),
                (aligned_columns.clone(), true)
            ],
            &DataType::Boolean
        )
        .unwrap()
        .as_ref(),
        Arc::new(BooleanArray::from(vec![true])).as_ref()
    );

    nullable_columns[1] = Arc::new(BooleanArray::from(vec![Some(true), None]));
    let malformed_records = Arc::new(StructArray::new(
        valid.fields().clone(),
        nullable_columns,
        None,
    ));
    let malformed_cells = list_wrap(malformed_records, vec![2]);
    assert!(
        execute(
            "__vail_matrix_aligned",
            vec![(malformed_cells, false), (aligned_columns, true)],
            &DataType::Boolean
        )
        .unwrap_err()
        .contains("presence must not be missing")
    );

    let DataType::List(entry_type) = cells.data_type() else {
        panic!("test cells are a list");
    };
    let missing_cells: ArrayRef = Arc::new(ListArray::new_null(entry_type.clone(), 1));
    assert!(
        execute(
            "__vail_matrix_aligned",
            vec![(missing_cells, false), (columns.clone(), true)],
            &DataType::Boolean
        )
        .unwrap_err()
        .contains("entry arrays must not be missing")
    );
    let missing_columns: ArrayRef = Arc::new(ListArray::new_null(
        Arc::new(Field::new("item", entry_type.data_type().clone(), true)),
        1,
    ));
    assert!(
        execute(
            "__vail_matrix_aligned",
            vec![(cells.clone(), false), (missing_columns, true)],
            &DataType::Boolean
        )
        .unwrap_err()
        .contains("entry array length differs")
    );
}

fn context_records(contexts: Vec<i32>, values: ArrayRef) -> ArrayRef {
    let context: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("c", DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(contexts))],
        None,
    ));
    Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("context", context.data_type().clone(), true)),
            Arc::new(Field::new("value", values.data_type().clone(), true)),
        ]
        .into(),
        vec![context, values],
        None,
    ))
}
#[test]
fn transpose_preserves_context_nullable_values_and_skips_missing_arrays() {
    let arrays: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(99)]),
        Some(vec![Some(1), None]),
        None,
        Some(vec![Some(3), Some(4)]),
    ]));
    let input = list_wrap(context_records(vec![99, 10, 20, 30], arrays), vec![1, 3]).slice(1, 1);
    let items = context_records(
        vec![10, 30, 10, 30],
        Arc::new(Int32Array::from(vec![Some(1), Some(3), None, Some(4)])),
    );
    let expected = list_wrap(list_wrap(items, vec![2, 2]), vec![2]);
    assert_eq!(
        execute(
            "__vail_matrix_transpose",
            vec![(input, true)],
            expected.data_type()
        )
        .unwrap()
        .as_ref(),
        expected.as_ref()
    );
    let ragged = list_wrap(
        context_records(
            vec![1, 2],
            lists(vec![vec![Some(1)], vec![Some(2), Some(3)]]),
        ),
        vec![2],
    );
    assert!(
        execute(
            "__vail_matrix_transpose",
            vec![(ragged, true)],
            expected.data_type()
        )
        .unwrap_err()
        .contains("equal array lengths")
    );
}

#[test]
fn dense_scatter_rejects_duplicate_and_out_of_range_positions() {
    for bad in [
        vec![Some(1), Some(1)],
        vec![Some(-1)],
        vec![Some(2)],
        vec![None],
    ] {
        let input = list_wrap(records(bad.clone(), true), vec![bad.len()]);
        let widths: ArrayRef = Arc::new(Int32Array::from(vec![2]));
        assert!(
            execute(
                "__vail_dense_cells",
                vec![(input.clone(), true), (widths, true)],
                input.data_type()
            )
            .is_err()
        );
    }
}

#[test]
fn empty_batch_does_not_validate_unused_missing_scalar_gather_owner() {
    let empty = lists(vec![]);
    let missing: ArrayRef = Arc::new(ListArray::new_null(
        Arc::new(Field::new("item", DataType::Int32, true)),
        1,
    ));
    assert_eq!(
        execute(
            "__vail_list_take",
            vec![(missing, true), (empty.clone(), false)],
            empty.data_type()
        )
        .unwrap()
        .len(),
        0
    );
}

#[test]
fn alignment_rejects_missing_entry_records_and_presence() {
    use arrow_buffer::NullBuffer;
    let valid = records(vec![Some(7)], true);
    let valid = valid.as_any().downcast_ref::<StructArray>().unwrap();
    let columns = list_wrap(records(vec![Some(7)], false), vec![1]);
    let mut missing_presence = valid.columns().to_vec();
    missing_presence[1] = Arc::new(BooleanArray::from(vec![None]));
    for malformed in [
        StructArray::new(
            valid.fields().clone(),
            valid.columns().to_vec(),
            Some(NullBuffer::from(vec![false])),
        ),
        StructArray::new(valid.fields().clone(), missing_presence, None),
    ] {
        let cells = list_wrap(Arc::new(malformed), vec![1]);
        assert!(
            execute(
                "__vail_matrix_aligned",
                vec![(cells, false), (columns.clone(), true)],
                &DataType::Boolean
            )
            .is_err()
        );
    }
}
