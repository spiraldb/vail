//! Differential audit against the Vortex version used by the storage reader.
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    StringArray, StructArray,
};
use arrow_schema::{DataType, Field};
use std::{cmp::Ordering, sync::Arc};
use vortex::array::scalar_fn::fns::operators::Operator;
use vortex::{
    VortexSessionDefault,
    array::{VortexSessionExecute, builtins::ArrayBuiltins},
    arrow::ArrowSessionExt,
    session::VortexSession,
};

const OPS: [Operator; 6] = [
    Operator::Eq,
    Operator::NotEq,
    Operator::Lt,
    Operator::Lte,
    Operator::Gt,
    Operator::Gte,
];

fn comparisons(left: ArrayRef, right: ArrayRef, op: Operator) -> Vec<Option<bool>> {
    let session = VortexSession::default();
    let lhs = session.arrow().from_arrow_array(left, true).unwrap();
    let rhs = session.arrow().from_arrow_array(right, true).unwrap();
    let result = lhs.binary(rhs, op).unwrap();
    let mut ctx = session.create_execution_ctx();
    (0..result.len())
        .map(|i| {
            result
                .execute_scalar(i, &mut ctx)
                .unwrap()
                .as_bool()
                .value()
        })
        .collect()
}

fn apply(order: Ordering, op: Operator) -> bool {
    match op {
        Operator::Eq => order.is_eq(),
        Operator::NotEq => order.is_ne(),
        Operator::Lt => order.is_lt(),
        Operator::Lte => order.is_le(),
        Operator::Gt => order.is_gt(),
        Operator::Gte => order.is_ge(),
        _ => unreachable!(),
    }
}

fn agree(left: ArrayRef, right: ArrayRef) {
    assert_eq!(left.len(), right.len());
    for op in OPS {
        let expected: Vec<_> = (0..left.len())
            .map(|i| {
                if left.is_null(i) || right.is_null(i) {
                    None
                } else {
                    Some(apply(
                        vail_ordering::encode_value(left.as_ref(), i)
                            .unwrap()
                            .cmp(&vail_ordering::encode_value(right.as_ref(), i).unwrap()),
                        op,
                    ))
                }
            })
            .collect();
        assert_eq!(
            comparisons(left.clone(), right.clone(), op),
            expected,
            "{op:?}"
        );
    }
}

#[test]
fn floats_all_six_operators_agree_including_nan_bits() {
    let bits64 = [
        0xffffffffffffffff,
        0xfff8000000000002,
        0xfff8000000000001,
        0xfff0000000000001,
        0xfff0000000000000,
        0xbff0000000000000,
        0x8000000000000001,
        0x8000000000000000,
        0,
        1,
        0x3ff0000000000000,
        0x7ff0000000000000,
        0x7ff0000000000001,
        0x7ff8000000000001,
        0x7ff8000000000002,
        0x7fffffffffffffff,
    ];
    let bits32 = [
        0xffffffff, 0xffc00002, 0xffc00001, 0xff800001, 0xff800000, 0xbf800000, 0x80000001,
        0x80000000, 0, 1, 0x3f800000, 0x7f800000, 0x7f800001, 0x7fc00001, 0x7fc00002, 0x7fffffff,
    ];
    let left64: Vec<_> = bits64
        .iter()
        .flat_map(|a| bits64.iter().map(move |_| f64::from_bits(*a)))
        .collect();
    let right64: Vec<_> = bits64
        .iter()
        .flat_map(|_| bits64.iter().map(|b| f64::from_bits(*b)))
        .collect();
    let left32: Vec<_> = bits32
        .iter()
        .flat_map(|a| bits32.iter().map(move |_| f32::from_bits(*a)))
        .collect();
    let right32: Vec<_> = bits32
        .iter()
        .flat_map(|_| bits32.iter().map(|b| f32::from_bits(*b)))
        .collect();
    agree(
        Arc::new(Float64Array::from(left64)),
        Arc::new(Float64Array::from(right64)),
    );
    agree(
        Arc::new(Float32Array::from(left32)),
        Arc::new(Float32Array::from(right32)),
    );
}

#[test]
fn ordinary_scalars_and_outer_nulls_agree() {
    agree(
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(0),
            Some(i32::MAX),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(i32::MIN),
        ])),
    );
    agree(
        Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
        Arc::new(Int64Array::from(vec![0, 0, i64::MIN])),
    );
    agree(
        Arc::new(BooleanArray::from(vec![Some(false), Some(true), None])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
    );
    agree(
        Arc::new(StringArray::from(vec![
            Some(""),
            Some("a\0b"),
            Some("é"),
            Some("😀"),
            None,
        ])),
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("a"),
            Some("z"),
            Some("é"),
            None,
        ])),
    );
    agree(
        Arc::new(Float64Array::from(vec![None, Some(-0.0), Some(f64::NAN)])),
        Arc::new(Float64Array::from(vec![Some(0.0), None, None])),
    );
}

fn struct_rows(name: &str, values: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(StructArray::new(
        vec![Arc::new(Field::new(name, DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(values))],
        None,
    ))
}

#[test]
fn present_nested_values_agree() {
    agree(
        struct_rows("a", vec![Some(1), Some(2)]),
        struct_rows("a", vec![Some(2), Some(2)]),
    );
    agree(
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(2)]),
        ])),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![Some(1), Some(3)]),
            Some(vec![Some(1)]),
        ])),
    );
}

#[test]
fn nested_nulls_demonstrate_missing_first_vs_missing_last() {
    let left = struct_rows("a", vec![None]);
    let right = struct_rows("a", vec![Some(1)]);
    assert_eq!(
        comparisons(left.clone(), right.clone(), Operator::Lt),
        vec![Some(true)]
    );
    assert!(
        vail_ordering::encode_value(left.as_ref(), 0).unwrap()
            > vail_ordering::encode_value(right.as_ref(), 0).unwrap()
    );
    let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![None]),
    ]));
    let right: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1)]),
    ]));
    assert_eq!(
        comparisons(left.clone(), right.clone(), Operator::Lt),
        vec![Some(true)]
    );
    assert!(
        vail_ordering::encode_value(left.as_ref(), 0).unwrap()
            > vail_ordering::encode_value(right.as_ref(), 0).unwrap()
    );
}

#[test]
fn set_physical_order_is_not_logical_set_equality() {
    let make = |values| {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(values)]);
        Arc::new(ListArray::new(
            Arc::new(Field::new("__vail_set_item", DataType::Int32, true)),
            list.offsets().clone(),
            list.values().clone(),
            None,
        )) as ArrayRef
    };
    let left = make(vec![Some(1), Some(2)]);
    let right = make(vec![Some(2), Some(1)]);
    assert_eq!(
        comparisons(left.clone(), right.clone(), Operator::Eq),
        vec![Some(false)]
    );
    assert_eq!(
        vail_ordering::encode_value(left.as_ref(), 0).unwrap(),
        vail_ordering::encode_value(right.as_ref(), 0).unwrap()
    );
}

fn fields(columns: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let definitions = columns
        .iter()
        .map(|(name, array)| Arc::new(Field::new(*name, array.data_type().clone(), true)))
        .collect::<Vec<_>>();
    Arc::new(StructArray::new(
        definitions.into(),
        columns.into_iter().map(|(_, array)| array).collect(),
        None,
    ))
}

#[test]
fn interval_inclusivity_order_matches_physical_struct_order() {
    let make = |inclusive: bool| {
        fields(vec![
            ("__vail_interval_start", Arc::new(Int32Array::from(vec![0]))),
            (
                "__vail_interval_start_exclusive",
                Arc::new(BooleanArray::from(vec![!inclusive])),
            ),
            ("__vail_interval_end", Arc::new(Int32Array::from(vec![1]))),
            (
                "__vail_interval_end_inclusive",
                Arc::new(BooleanArray::from(vec![false])),
            ),
        ])
    };
    let left = make(true);
    let right = make(false);
    assert_eq!(
        comparisons(left.clone(), right.clone(), Operator::Lt),
        vec![Some(true)]
    );
    assert!(
        vail_ordering::encode_value(left.as_ref(), 0).unwrap()
            < vail_ordering::encode_value(right.as_ref(), 0).unwrap()
    );
}

#[test]
fn interval_struct_order_matches_hail_endpoint_order() {
    let values = [
        (1, 100, false, true),
        (1, 2, true, false),
        (1, 100, false, false),
        (0, 3, true, true),
        (1, 2, false, true),
    ];
    let make = |(start, end, exclusive, inclusive)| {
        fields(vec![
            (
                "__vail_interval_start",
                Arc::new(Int32Array::from(vec![start])),
            ),
            (
                "__vail_interval_start_exclusive",
                Arc::new(BooleanArray::from(vec![exclusive])),
            ),
            ("__vail_interval_end", Arc::new(Int32Array::from(vec![end]))),
            (
                "__vail_interval_end_inclusive",
                Arc::new(BooleanArray::from(vec![inclusive])),
            ),
        ])
    };
    for left in values {
        for right in values {
            // Hail compares the complete left endpoint before the right endpoint.
            let expected =
                (left.0, left.2, left.1, left.3).cmp(&(right.0, right.2, right.1, right.3));
            for op in OPS {
                assert_eq!(
                    comparisons(make(left), make(right), op),
                    vec![Some(apply(expected, op))]
                );
            }
            assert_eq!(
                vail_ordering::encode_value(make(left).as_ref(), 0)
                    .unwrap()
                    .cmp(&vail_ordering::encode_value(make(right).as_ref(), 0).unwrap()),
                expected
            );
        }
    }
}

#[test]
fn call_order_differs_from_physical_allele_order() {
    let make = |values| {
        fields(vec![
            (
                "__vail_call_alleles",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(values),
                ])),
            ),
            (
                "__vail_call_phased",
                Arc::new(BooleanArray::from(vec![false])),
            ),
        ])
    };
    let left = make(vec![Some(0), Some(2)]);
    let right = make(vec![Some(1), Some(1)]);
    assert_eq!(
        comparisons(left.clone(), right.clone(), Operator::Lt),
        vec![Some(true)]
    );
    assert!(
        vail_ordering::encode_value(left.as_ref(), 0).unwrap()
            > vail_ordering::encode_value(right.as_ref(), 0).unwrap()
    );
}

#[test]
fn ndarray_order_differs_from_physical_data_first_order() {
    use arrow_array::types::Int64Type;
    let make = |data, size| {
        fields(vec![
            (
                "__vail_ndarray_data",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(data),
                ])),
            ),
            (
                "__vail_ndarray_shape",
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(size)]),
                ])),
            ),
        ])
    };
    let left = make(vec![Some(0), Some(0)], 2);
    let right = make(vec![Some(1)], 1);
    assert_eq!(
        comparisons(left.clone(), right.clone(), Operator::Lt),
        vec![Some(true)]
    );
    assert!(
        vail_ordering::encode_value(left.as_ref(), 0).unwrap()
            > vail_ordering::encode_value(right.as_ref(), 0).unwrap()
    );
}

#[test]
fn dictionary_entry_order_is_not_logical_dictionary_equality() {
    let make = |keys| {
        let entries = fields(vec![
            ("key", Arc::new(Int32Array::from(keys))),
            ("value", Arc::new(Int32Array::from(vec![10, 10]))),
        ]);
        Arc::new(ListArray::new(
            Arc::new(Field::new(
                "__vail_dict_entry",
                entries.data_type().clone(),
                false,
            )),
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(0), Some(0)])])
                .offsets()
                .clone(),
            entries,
            None,
        )) as ArrayRef
    };
    let left = make(vec![1, 2]);
    let right = make(vec![2, 1]);
    assert_eq!(
        comparisons(left.clone(), right.clone(), Operator::Eq),
        vec![Some(false)]
    );
    assert_eq!(
        vail_ordering::encode_value(left.as_ref(), 0).unwrap(),
        vail_ordering::encode_value(right.as_ref(), 0).unwrap()
    );
}

#[tokio::test]
async fn iceberg_metrics_pruning_drops_nan_rows_that_vortex_matches() {
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use futures::TryStreamExt;
    use iceberg::{Catalog, CatalogBuilder, MemoryCatalogBuilder, NamespaceIdent, TableCreation};
    use iceberg::{
        arrow::arrow_schema_to_schema_auto_assign_ids, expr::Reference, io::LocalFsStorageFactory,
        spec::Datum,
    };
    use serde_json::json;
    use std::collections::HashMap;
    use vail_storage::{VailSemantics, write_initial_table};

    let dir = tempfile::tempdir().unwrap();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "audit",
            HashMap::from([(
                "warehouse".into(),
                format!("file://{}", dir.path().join("warehouse").display()),
            )]),
        )
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("audit".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("nan_rows".into())
                .location(format!("file://{}", dir.path().join("table").display()))
                .schema(arrow_schema_to_schema_auto_assign_ids(&schema).unwrap())
                .build(),
        )
        .await
        .unwrap();
    let values: ArrayRef = Arc::new(Float64Array::from(vec![f64::from_bits(0x7ff8000000000001)]));
    let batch = RecordBatch::try_new(schema, vec![values.clone()]).unwrap();
    let table = write_initial_table(
        &catalog,
        &table,
        vec![batch],
        VailSemantics {
            row_type: json!({"x":"float64"}),
            global_type: json!({}),
            globals: json!({}),
            key_field_ids: vec![],
            comparator_version: "arrival-order-v1".into(),
            reference_genomes: json!({}),
        },
    )
    .await
    .unwrap();
    let scan = table
        .scan()
        .with_filter(Reference::new("x").greater_than(Datum::double(f64::INFINITY)))
        .build()
        .unwrap();
    let tasks = scan
        .plan_files()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    // This records a CURRENT mismatch, not desired behavior: a residual filter
    // cannot recover the matching row once metrics pruning discards its file.
    assert!(
        tasks.is_empty(),
        "re-audit when Iceberg NaN metrics semantics change"
    );
    assert_eq!(
        comparisons(
            values,
            Arc::new(Float64Array::from(vec![f64::INFINITY])),
            Operator::Gt
        ),
        vec![Some(true)]
    );
}

#[test]
fn floating_constant_folding_and_compression_preserve_comparisons() {
    use vortex::array::{IntoArray, arrays::ConstantArray};
    use vortex::compressor::BtrBlocksCompressor;
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let bits = [
        0xfff8000000000001,
        0xfff0000000000001,
        0xfff0000000000000,
        0x8000000000000000,
        0,
        0x7ff0000000000000,
        0x7ff0000000000001,
        0x7ff8000000000001,
        0x7ff8000000000002,
    ];
    for a in bits {
        for b in bits {
            let lhs = f64::from_bits(a);
            let rhs = f64::from_bits(b);
            let left = session
                .arrow()
                .from_arrow_array(
                    Arc::new(Float64Array::from(vec![lhs; 128])) as ArrayRef,
                    false,
                )
                .unwrap();
            let right = session
                .arrow()
                .from_arrow_array(
                    Arc::new(Float64Array::from(vec![rhs; 128])) as ArrayRef,
                    false,
                )
                .unwrap();
            let lc =
                ConstantArray::new(left.execute_scalar(0, &mut ctx).unwrap(), 128).into_array();
            let rc =
                ConstantArray::new(right.execute_scalar(0, &mut ctx).unwrap(), 128).into_array();
            let compressed = BtrBlocksCompressor::default()
                .compress(&left, &mut ctx)
                .unwrap();
            for op in OPS {
                let expected = Some(apply(lhs.total_cmp(&rhs), op));
                for (l, r) in [
                    (left.clone(), rc.clone()),
                    (lc.clone(), right.clone()),
                    (lc.clone(), rc.clone()),
                    (compressed.clone(), rc.clone()),
                ] {
                    let result = l.binary(r, op).unwrap();
                    assert_eq!(
                        result
                            .execute_scalar(0, &mut ctx)
                            .unwrap()
                            .as_bool()
                            .value(),
                        expected,
                        "bits {a:016x}, {b:016x}, op {op:?}"
                    );
                }
            }
        }
    }
}
