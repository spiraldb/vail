use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use vail_execution::expressions::{Expr, Type, project_batch};
use vail_execution::reference::{ParInterval, ReferenceDefinition};

fn reference() -> ReferenceDefinition {
    ReferenceDefinition {
        name: "public_reference".into(),
        contigs: vec!["a".into(), "b".into(), "x".into(), "y".into()],
        lengths: [
            ("a".into(), 2),
            ("b".into(), 3),
            ("x".into(), 5),
            ("y".into(), 4),
        ]
        .into(),
        x_contigs: vec!["x".into()],
        y_contigs: vec!["y".into()],
        mt_contigs: vec![],
        par: vec![ParInterval {
            contig: "x".into(),
            start: 2,
            end: 5,
        }],
    }
}

#[test]
fn imported_parent_masked_loci_keep_all_extracted_fields_missing() {
    let dtype = Type::Locus { locus: reference() }.to_arrow().unwrap();
    let DataType::Struct(fields) = &dtype else {
        panic!("expected locus storage struct")
    };
    let ordinals = Arc::new(Int32Array::from(vec![-1, 2])) as ArrayRef;
    let contigs = Arc::new(StringArray::from(vec!["hidden", "x"]));
    let positions = Arc::new(Int32Array::from(vec![-1, 2]));
    let loci = StructArray::try_new(
        fields.clone(),
        vec![ordinals, contigs.clone(), positions.clone()],
        Some(NullBuffer::from(vec![false, true])),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("locus", dtype, true)])),
        vec![Arc::new(loci)],
    )
    .unwrap();
    let projections = ["contig", "position", "is_x", "in_x_par", "global_position"]
        .into_iter()
        .map(|operation| {
            (
                operation.to_string(),
                Expr::LocusOp {
                    operation: operation.into(),
                    value: Arc::new(Expr::Column {
                        name: "locus".into(),
                    }),
                },
            )
        })
        .collect::<Vec<_>>();
    let result = project_batch(&batch, &projections).unwrap();
    for column in result.columns() {
        assert!(column.is_null(0));
        assert!(!column.is_null(1));
    }
    let actual_contigs = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let actual_positions = result
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(actual_contigs.value(1), "x");
    assert_eq!(actual_positions.value(1), 2);
    assert!(
        result
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(1)
    );
    assert!(
        result
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(1)
    );
    assert_eq!(
        result
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1),
        6
    );
    // Validity changes must share the imported Arrow data buffers.
    assert_eq!(actual_contigs.values().as_ptr(), contigs.values().as_ptr());
    assert_eq!(
        actual_positions.values().as_ptr(),
        positions.values().as_ptr()
    );
}

fn column(name: &str) -> Arc<Expr> {
    Arc::new(Expr::Column { name: name.into() })
}

fn operation(name: &str, value: Expr) -> Expr {
    Expr::LocusOp {
        operation: name.into(),
        value: Arc::new(value),
    }
}

#[test]
fn global_coordinates_round_trip_at_every_contig_boundary_and_repeat() {
    let contigs = (0..256).map(|i| format!("c{i}")).collect::<Vec<_>>();
    let reference = ReferenceDefinition {
        name: "many".into(),
        lengths: contigs.iter().map(|c| (c.clone(), 7)).collect(),
        x_contigs: contigs[..128].to_vec(),
        y_contigs: contigs[128..].to_vec(),
        mt_contigs: vec![],
        par: contigs
            .iter()
            .flat_map(|c| {
                [(1, 4), (2, 3), (3, 6)].map(|(start, end)| ParInterval {
                    contig: c.clone(),
                    start,
                    end,
                })
            })
            .collect(),
        contigs,
    };
    let coordinates = (0..256)
        .flat_map(|i| [Some(i * 7), Some(i * 7 + 6)])
        .chain([None])
        .collect::<Vec<Option<i64>>>();
    let batch = RecordBatch::try_from_iter(vec![(
        "global",
        Arc::new(Int64Array::from(coordinates.clone())) as ArrayRef,
    )])
    .unwrap();
    let locus = Expr::LocusFromGlobalPosition {
        reference,
        position: column("global"),
    };
    let plan = vail_execution::expressions::lower(
        &operation("global_position", locus.clone()),
        batch.schema().as_ref(),
    )
    .unwrap();
    for _ in 0..3 {
        let actual = plan
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        assert_eq!(
            actual.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(coordinates.clone())
        );
    }
    let result = project_batch(
        &batch,
        &[
            ("contig".into(), operation("contig", locus.clone())),
            ("position".into(), operation("position", locus.clone())),
            ("x".into(), operation("is_x", locus.clone())),
            ("y".into(), operation("is_y", locus.clone())),
            ("x_par".into(), operation("in_x_par", locus.clone())),
            ("y_par".into(), operation("in_y_par", locus)),
        ],
    )
    .unwrap();
    let names = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let positions = result
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    for i in 0..256 {
        for endpoint in 0..2 {
            let row = i * 2 + endpoint;
            assert_eq!(names.value(row), format!("c{i}"));
            assert_eq!(positions.value(row), if endpoint == 0 { 1 } else { 7 });
            for (col, expected) in [
                (2, i < 128),
                (3, i >= 128),
                (4, i < 128 && endpoint == 0),
                (5, i >= 128 && endpoint == 0),
            ] {
                assert_eq!(
                    result
                        .column(col)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row),
                    expected
                );
            }
        }
    }
    assert!(result.columns().iter().all(|c| c.is_null(512)));
    let empty = batch.slice(0, 0);
    assert_eq!(
        plan.evaluate(&empty).unwrap().into_array(0).unwrap().len(),
        0
    );
}

#[test]
fn locus_validation_and_parse_reject_invalid_coordinates_and_metadata() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "contig",
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("unknown"),
                Some("x"),
                Some("x"),
                None,
            ])) as ArrayRef,
        ),
        (
            "position",
            Arc::new(Int32Array::from(vec![
                Some(2),
                Some(1),
                Some(0),
                Some(6),
                Some(1),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let valid = Expr::LocusValid {
        reference: reference(),
        contig: column("contig"),
        position: column("position"),
    };
    let result = project_batch(&batch, &[("valid".into(), valid)]).unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap(),
        &BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None
        ])
    );
    let create = Expr::Locus {
        reference: reference(),
        contig: column("contig"),
        position: column("position"),
    };
    for row in 1..4 {
        assert!(project_batch(&batch.slice(row, 1), &[("locus".into(), create.clone())]).is_err());
    }
    for value in [-1, 14] {
        let batch = RecordBatch::try_from_iter(vec![(
            "global",
            Arc::new(Int64Array::from(vec![value])) as ArrayRef,
        )])
        .unwrap();
        assert!(
            project_batch(
                &batch,
                &[(
                    "locus".into(),
                    Expr::LocusFromGlobalPosition {
                        reference: reference(),
                        position: column("global")
                    }
                )]
            )
            .is_err()
        );
    }
    for value in ["a:3", "x:0", "unknown:1", "a:no", "a"] {
        let batch = RecordBatch::try_from_iter(vec![(
            "text",
            Arc::new(StringArray::from(vec![value])) as ArrayRef,
        )])
        .unwrap();
        assert!(
            project_batch(
                &batch,
                &[(
                    "locus".into(),
                    Expr::ParseLocus {
                        reference: reference(),
                        value: column("text")
                    }
                )]
            )
            .is_err()
        );
    }
    let mut invalid = Vec::new();
    let mut r = reference();
    r.y_contigs.push("x".into());
    invalid.push(r);
    let mut r = reference();
    r.x_contigs.push("x".into());
    invalid.push(r);
    let mut r = reference();
    r.mt_contigs.push("unknown".into());
    invalid.push(r);
    let mut r = reference();
    r.lengths.insert("a".into(), 0);
    invalid.push(r);
    let mut r = reference();
    r.par[0].end = 6;
    invalid.push(r);
    let mut r = reference();
    r.par[0].contig = "a".into();
    invalid.push(r);
    let mut r = reference();
    r.par.push(r.par[0].clone());
    invalid.push(r);
    for reference in invalid {
        assert!(reference.validate().is_err());
        assert!((Type::Locus { locus: reference }).to_arrow().is_err());
    }
}

#[test]
fn overlapping_nested_par_intervals_use_half_open_coverage() {
    let mut reference = reference();
    reference.par = vec![
        ParInterval {
            contig: "x".into(),
            start: 1,
            end: 3,
        },
        ParInterval {
            contig: "x".into(),
            start: 2,
            end: 5,
        },
        ParInterval {
            contig: "x".into(),
            start: 2,
            end: 3,
        },
        ParInterval {
            contig: "y".into(),
            start: 2,
            end: 4,
        },
    ];
    let batch = RecordBatch::try_from_iter(vec![
        (
            "contig",
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("x"),
                Some("x"),
                Some("x"),
                Some("x"),
                Some("y"),
                Some("y"),
                Some("y"),
                Some("y"),
                None,
            ])) as ArrayRef,
        ),
        (
            "position",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 1])) as ArrayRef,
        ),
    ])
    .unwrap();
    let locus = Expr::Locus {
        reference,
        contig: column("contig"),
        position: column("position"),
    };
    let result = project_batch(
        &batch,
        &[
            ("x".into(), operation("in_x_par", locus.clone())),
            ("y".into(), operation("in_y_par", locus)),
        ],
    )
    .unwrap();
    for (col, expected) in [
        vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            None,
        ],
        vec![
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(false),
            None,
        ],
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            result
                .column(col)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(expected)
        );
    }
}
