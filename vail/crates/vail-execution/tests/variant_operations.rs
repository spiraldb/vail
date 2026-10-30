use arrow_array::{
    Array, ArrayRef, Int32Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    StructArray,
};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{Expr, project_batch};

#[test]
fn normalization_accepts_shared_scalars_and_arrow_string_storage() {
    let text = "é".repeat(8192);
    let reference = format!("{text}A");
    let alternate = format!("{text}T");
    let expr: Expr = serde_json::from_value(json!({
        "op":"string_function", "function":"variant_min_rep", "args":[
            {"op":"literal","dtype":"str","value":reference},
            {"op":"column","name":"alternate"}
        ]
    }))
    .unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![alternate.as_str(); 64])),
        Arc::new(LargeStringArray::from(vec![alternate.as_str(); 64])),
        Arc::new(StringViewArray::from(vec![alternate.as_str(); 64])),
    ];
    for array in arrays {
        let source = RecordBatch::try_from_iter([("alternate", array)]).unwrap();
        let result = project_batch(&source, &[("variant".into(), expr.clone())]).unwrap();
        let variant = result
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let shift = variant
            .column_by_name("shift")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(shift, &Int32Array::from(vec![8192; 64]));
        assert_eq!(
            project_batch(&source.slice(0, 0), &[("variant".into(), expr.clone())])
                .unwrap()
                .num_rows(),
            0
        );
    }
}

#[test]
fn entry_preparation_reuses_scalar_likelihoods_for_batch_rows() {
    let n = 128;
    let pl = (0..n * (n + 1) / 2).collect::<Vec<i32>>();
    let expr: Expr = serde_json::from_value(json!({
        "op":"string_function","function":"variant_entry", "args":[
            {"op":"column","name":"n"},
            {"op":"literal","dtype":{"array":"int32"},"value":[0,1]},
            {"op":"literal","dtype":{"array":"int32"},"value":null},
            {"op":"literal","dtype":{"array":"int32"},"value":null},
            {"op":"literal","dtype":{"array":"int32"},"value":pl}
        ]
    }))
    .unwrap();
    let source =
        RecordBatch::try_from_iter([("n", Arc::new(Int32Array::from(vec![n; 256])) as ArrayRef)])
            .unwrap();
    let result = project_batch(&source, &[("entry".into(), expr.clone())]).unwrap();
    assert_eq!(result.num_rows(), 256);
    let prepared = result.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    let pl = prepared.column_by_name("pl").unwrap().as_any().downcast_ref::<arrow_array::ListArray>().unwrap();
    let alleles = pl.value(0);
    let alleles = alleles.as_any().downcast_ref::<arrow_array::ListArray>().unwrap();
    for a in 1..n {
        let values = alleles.value((a - 1) as usize);
        assert_eq!(values.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![0, a * (a + 1) / 2, a * (a + 1) / 2 + a]));
    }
    let first = datafusion_common::ScalarValue::try_from_array(result.column(0), 0).unwrap();
    for row in 1..256 {
        assert_eq!(
            datafusion_common::ScalarValue::try_from_array(result.column(0), row).unwrap(),
            first
        );
    }
    assert_eq!(
        project_batch(&source.slice(0, 0), &[("entry".into(), expr)])
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn entry_preparation_reads_sliced_nullable_lists_and_resets_workspace() {
    use arrow_array::ListArray;
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field};

    // The child has its own offset, the lists skip a child prefix, and the
    // source batch then skips a parent row. Null parents hide arbitrary values.
    fn lists(rows: &[Option<Vec<Option<i32>>>]) -> ArrayRef {
        let mut values = vec![Some(-1); 5];
        let mut offsets = vec![2];
        for row in rows {
            values.extend(row.as_deref().unwrap_or(&[Some(999)]));
            offsets.push((values.len() - 3) as i32);
        }
        let len = values.len() - 3;
        let child = Arc::new(Int32Array::from(values).slice(3, len));
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            child,
            Some(NullBuffer::from(
                rows.iter().map(Option::is_some).collect::<Vec<_>>(),
            )),
        ))
    }
    fn nested(value: &ArrayRef) -> Vec<Vec<Option<Vec<Option<i32>>>>> {
        let outer = value.as_any().downcast_ref::<ListArray>().unwrap();
        (0..outer.len())
            .map(|i| {
                assert!(outer.is_valid(i));
                let row = outer.value(i);
                let inner = row.as_any().downcast_ref::<ListArray>().unwrap();
                (0..inner.len())
                    .map(|j| {
                        if inner.is_null(j) {
                            return None;
                        }
                        let values = inner.value(j);
                        Some(
                            values
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .iter()
                                .collect(),
                        )
                    })
                    .collect()
            })
            .collect()
    }
    let gt = lists(&[
        Some(vec![Some(0), Some(1)]),
        Some(vec![Some(0), Some(1)]),
        Some(vec![Some(2)]),
        None,
        Some(vec![Some(1), Some(2)]),
    ]);
    let ad = lists(&[
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(5), None]),
        None,
        Some(vec![Some(4), Some(6)]),
        Some(vec![Some(5), Some(7), Some(11)]),
    ]);
    let pl = lists(&[
        Some(vec![Some(9), Some(3), Some(7)]),
        Some(vec![Some(9), None, Some(3)]),
        Some(vec![Some(7), Some(3), Some(9)]),
        Some(vec![Some(8), Some(2), Some(6)]),
        Some(vec![
            Some(40),
            Some(30),
            Some(20),
            Some(10),
            Some(0),
            Some(50),
        ]),
    ]);
    let source = RecordBatch::try_from_iter([
        (
            "n",
            Arc::new(Int32Array::from(vec![2, 2, 3, 2, 3])) as ArrayRef,
        ),
        ("gt", gt),
        ("ad", ad),
        ("pl", pl),
    ])
    .unwrap()
    .slice(1, 4);
    let expr: Expr = serde_json::from_value(json!({
        "op":"string_function", "function":"variant_entry", "args":[
            {"op":"column","name":"n"}, {"op":"column","name":"gt"},
            {"op":"literal","dtype":{"array":"int32"},"value":null},
            {"op":"column","name":"ad"}, {"op":"column","name":"pl"}
        ]
    }))
    .unwrap();
    let result = project_batch(&source, &[("entry".into(), expr.clone())]).unwrap();
    let result = result
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(result.null_count(), 0);
    assert_eq!(
        nested(result.column_by_name("ad").unwrap()),
        vec![
            vec![Some(vec![None, None])],
            vec![None, None],
            vec![Some(vec![Some(4), Some(6)])],
            vec![
                Some(vec![Some(16), Some(7)]),
                Some(vec![Some(12), Some(11)])
            ],
        ]
    );
    assert_eq!(
        nested(result.column_by_name("pl").unwrap()),
        vec![
            vec![Some(vec![Some(9), None, Some(3)])],
            vec![Some(vec![Some(7), Some(3)]), Some(vec![Some(3), Some(9)])],
            vec![None],
            vec![
                Some(vec![Some(10), Some(0), Some(20)]),
                Some(vec![Some(20), Some(0), Some(50)])
            ],
        ]
    );
    assert_eq!(
        project_batch(&source.slice(0, 0), &[("entry".into(), expr)])
            .unwrap()
            .num_rows(),
        0
    );
}
