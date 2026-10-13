//! Audit DataFusion's native behavior on Vail's physical type representations.
//! This deliberately bypasses Vail's canonical key expressions.
use datafusion::{
    arrow::{array::*, datatypes::*, record_batch::RecordBatch},
    datasource::MemTable,
    prelude::SessionContext,
};
use futures::FutureExt;
use std::sync::Arc;

fn list(values: Vec<Vec<Option<i32>>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        values.into_iter().map(Some),
    ))
}
fn structure(fields: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let definitions = fields
        .iter()
        .map(|(n, a)| Arc::new(Field::new(*n, a.data_type().clone(), true)))
        .collect::<Vec<_>>();
    Arc::new(StructArray::new(
        definitions.into(),
        fields.into_iter().map(|(_, a)| a).collect(),
        None,
    ))
}
fn dict(reversed: bool) -> ArrayRef {
    use datafusion::arrow::buffer::OffsetBuffer;
    let keys = if reversed {
        vec![1, 2, 2, 1]
    } else {
        vec![1, 2, 1, 2]
    };
    let values = if reversed {
        vec![10, 20, 20, 10]
    } else {
        vec![10, 20, 10, 20]
    };
    let entries = structure(vec![
        ("key", Arc::new(Int32Array::from(keys))),
        ("value", Arc::new(Int32Array::from(values))),
    ]);
    Arc::new(ListArray::new(
        Arc::new(Field::new(
            "__vail_dict_entry",
            entries.data_type().clone(),
            true,
        )),
        OffsetBuffer::new(vec![0, 2, 4].into()),
        entries,
        None,
    ))
}
fn float_view(values: ArrayRef) -> ArrayRef {
    Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "value",
            values.data_type().clone(),
            true,
        ))]
        .into(),
        vec![values.clone()],
        values.nulls().cloned(),
    ))
}
fn nullable_member_map() -> ArrayRef {
    use datafusion::arrow::buffer::OffsetBuffer;
    // Arrow Map forbids null keys; a present struct can contain a missing
    // logical key. This also allows sets to use Map<wrapped-key, Null>.
    let keys = structure(vec![(
        "value",
        Arc::new(Int32Array::from(vec![None, Some(1), None, Some(1)])),
    )]);
    let entries = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("key", keys.data_type().clone(), false)),
            Arc::new(Field::new("value", DataType::Null, true)),
        ]
        .into(),
        vec![keys, Arc::new(NullArray::new(4))],
        None,
    ));
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 2, 4].into()),
        entries
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone(),
        None,
        true,
    ))
}
fn verify(name: &str, results: &[Option<bool>], groups: usize, ids: &[i32], matches: i64) {
    let lt = [false, true, true, true, false, false];
    let gt = [false, true, false, false, true, true];
    let equal = [true, false, false, true, false, true];
    let (expected, expected_groups, expected_ids, expected_matches) = match name {
        "float32_signed_zero" | "float64_signed_zero" => {
            (equal.map(Some).to_vec(), 1, vec![0, 1], 4)
        }
        "float64_signed_zero_reversed" => (equal.map(Some).to_vec(), 1, vec![1, 0], 4),
        "array_outer_missing" | "float_view_outer_missing" => (vec![None; 6], 2, vec![0, 1], 1),
        "empty_tuple"
        | "empty_struct_tagged"
        | "set_sorted"
        | "dict_sorted"
        | "set_map_missing_member" => (equal.map(Some).to_vec(), 1, vec![0, 1], 4),
        "call_packed" | "ndarray_shape_first" => (gt.map(Some).to_vec(), 2, vec![1, 0], 2),
        _ => (lt.map(Some).to_vec(), 2, vec![0, 1], 2),
    };
    assert_eq!(results, expected, "{name}: comparisons");
    assert_eq!(groups, expected_groups, "{name}: grouping");
    assert_eq!(ids, expected_ids, "{name}: sorting");
    assert_eq!(matches, expected_matches, "{name}: join");
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let zero32: ArrayRef = Arc::new(Float32Array::from(vec![-0.0, 0.0]));
    let zero64: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 0.0]));
    let calls = structure(vec![
        (
            "__vail_call_alleles",
            list(vec![vec![Some(0), Some(2)], vec![Some(1), Some(1)]]),
        ),
        (
            "__vail_call_phased",
            Arc::new(BooleanArray::from(vec![false, false])),
        ),
    ]);
    let data = list(vec![vec![Some(0), Some(0)], vec![Some(1)]]);
    let shape: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(2)]),
        Some(vec![Some(1)]),
    ]));
    let cases: Vec<(&str, ArrayRef)> = vec![
        ("bool", Arc::new(BooleanArray::from(vec![false, true]))),
        (
            "int32",
            Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX])),
        ),
        (
            "int64",
            Arc::new(Int64Array::from(vec![1i64 << 53, (1i64 << 53) + 1])),
        ),
        (
            "float_array_signed_zero",
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([
                Some(vec![Some(-0.0)]),
                Some(vec![Some(0.0)]),
            ])),
        ),
        ("float32_signed_zero", zero32.clone()),
        ("float64_signed_zero", zero64.clone()),
        (
            "float64_signaling_nan",
            Arc::new(Float64Array::from(vec![
                f64::from_bits(0x7ff0000000000001),
                f64::from_bits(0x7ff8000000000001),
            ])),
        ),
        (
            "float32_nan_payload",
            Arc::new(Float32Array::from(vec![
                f32::from_bits(0x7fc00001),
                f32::from_bits(0x7fc00002),
            ])),
        ),
        (
            "float_view_outer_missing",
            float_view(Arc::new(Float64Array::from(vec![None, Some(1.0)]))),
        ),
        (
            "float64_signed_zero_reversed",
            Arc::new(Float64Array::from(vec![0.0, -0.0])),
        ),
        (
            "float64_nan_payload",
            Arc::new(Float64Array::from(vec![
                f64::from_bits(0x7ff8000000000001),
                f64::from_bits(0x7ff8000000000002),
            ])),
        ),
        (
            "float32_nan_sign",
            Arc::new(Float32Array::from(vec![
                f32::from_bits(0xffc00001),
                f32::from_bits(0x7fc00001),
            ])),
        ),
        ("string", Arc::new(StringArray::from(vec!["a", "β"]))),
        (
            "array",
            list(vec![vec![Some(1), Some(2)], vec![Some(1), Some(3)]]),
        ),
        (
            "array_nested_missing",
            list(vec![vec![None], vec![Some(1)]]),
        ),
        (
            "array_outer_missing",
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                None,
                Some(vec![Some(1)]),
            ])),
        ),
        (
            "struct",
            structure(vec![
                ("a", Arc::new(Int32Array::from(vec![1, 2]))),
                ("b", Arc::new(Int32Array::from(vec![100, 0]))),
            ]),
        ),
        (
            "struct_nested_missing",
            structure(vec![("a", Arc::new(Int32Array::from(vec![None, Some(1)])))]),
        ),
        (
            "tuple",
            structure(vec![
                ("__vail_tuple_0", Arc::new(Int32Array::from(vec![1, 2]))),
                ("__vail_tuple_1", Arc::new(Int32Array::from(vec![100, 0]))),
            ]),
        ),
        (
            "empty_struct",
            Arc::new(StructArray::new_empty_fields(2, None)),
        ),
        (
            "empty_tuple",
            structure(vec![(
                "__vail_tuple_empty",
                Arc::new(BooleanArray::from(vec![true, true])),
            )]),
        ),
        (
            "set_raw",
            list(vec![vec![Some(1), Some(2)], vec![Some(2), Some(1)]]),
        ),
        (
            "set_sorted",
            list(vec![vec![Some(1), Some(2)], vec![Some(1), Some(2)]]),
        ),
        ("set_map_missing_member", nullable_member_map()),
        (
            "empty_struct_tagged",
            structure(vec![(
                "__vail_struct_empty",
                Arc::new(BooleanArray::from(vec![true, true])),
            )]),
        ),
        ("dict_raw", dict(true)),
        ("dict_sorted", dict(false)),
        (
            "interval",
            structure(vec![
                (
                    "__vail_interval_start",
                    Arc::new(Int32Array::from(vec![1, 1])),
                ),
                (
                    "__vail_interval_start_exclusive",
                    Arc::new(BooleanArray::from(vec![false, true])),
                ),
                (
                    "__vail_interval_end",
                    Arc::new(Int32Array::from(vec![2, 2])),
                ),
                (
                    "__vail_interval_end_inclusive",
                    Arc::new(BooleanArray::from(vec![false, false])),
                ),
            ]),
        ),
        ("call_raw", calls),
        (
            "call_packed",
            Arc::new(Int32Array::from(vec![(3 << 3) | 4, (2 << 3) | 4])),
        ),
        (
            "ndarray_raw",
            structure(vec![
                ("__vail_ndarray_data", data.clone()),
                ("__vail_ndarray_shape", shape.clone()),
            ]),
        ),
        (
            "ndarray_shape_first",
            structure(vec![
                ("__vail_ndarray_shape", shape),
                ("__vail_ndarray_data", data),
            ]),
        ),
        (
            "locus",
            structure(vec![
                (
                    "__vail_locus_ordinal",
                    Arc::new(Int32Array::from(vec![1, 2])),
                ),
                (
                    "__vail_locus_contig",
                    Arc::new(StringArray::from(vec!["z", "a"])),
                ),
                (
                    "__vail_locus_position",
                    Arc::new(Int32Array::from(vec![100, 1])),
                ),
            ]),
        ),
        ("float32_wrapped", structure(vec![("value", zero32)])),
        ("float64_wrapped", structure(vec![("value", zero64)])),
    ];
    for (name, array) in cases {
        let outcome = std::panic::AssertUnwindSafe(async {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", array.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0, 1])), array],
        )?;
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
        let comparisons=ctx.sql("SELECT a.v=b.v AS eq, a.v<>b.v AS ne, a.v<b.v AS lt, a.v<=b.v AS le, a.v>b.v AS gt, a.v>=b.v AS ge FROM t a CROSS JOIN t b WHERE a.id=0 AND b.id=1").await?.collect().await?;
        let results = comparisons[0]
            .columns()
            .iter()
            .map(|a| {
                a.as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let groups = ctx
            .sql("SELECT count(*) FROM t GROUP BY v")
            .await?
            .collect()
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        let order = ctx
            .sql("SELECT id FROM t ORDER BY v ASC NULLS FIRST")
            .await?
            .collect()
            .await?;
        let ids = order
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        let joined = ctx
            .sql("SELECT count(*) FROM t a JOIN t b ON a.v=b.v")
            .await?
            .collect()
            .await?;
        let matches = joined[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        verify(name, &results, groups, &ids, matches);
        println!(
            "{name}\teq,ne,lt,le,gt,ge={results:?}\tgroups={groups}\torder={ids:?}\tjoin_matches={matches}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
        }).catch_unwind().await;
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(error)) if name == "empty_struct" => {
                assert!(error.to_string().contains("struct array with no fields"));
                println!("{name}\tUNSUPPORTED: {error}");
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(format!("unexpected panic auditing {name}").into()),
        }
    }
    Ok(())
}
