//! Public singleton construction, binding and legacy/logical demand behavior.
use arrow_array::{Int32Array, RecordBatch, StringViewArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use vail_execution::expressions::{Expr, PrimitiveType, Type};
use vail_table::globals::GlobalsIR;

fn integer() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("g", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )
    .unwrap()
}
#[test]
fn singleton_schemas_reject_ambiguous_or_unsupported_fields() {
    for names in [vec!["g", "g"], vec!["g", ""]] {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(
                names
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, false))
                    .collect::<Vec<_>>(),
            )),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        )
        .unwrap();
        assert!(GlobalsIR::arrow_literal(batch).is_err());
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("g", DataType::UInt64, false)])),
        vec![Arc::new(arrow_array::UInt64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(GlobalsIR::arrow_literal(batch).is_err());
    let globals = GlobalsIR::arrow_literal(integer()).unwrap();
    assert!(
        globals
            .map(vec![("bad".into(), Expr::Column { name: "idx".into() })])
            .is_err()
    );
}
#[test]
fn binding_declarations_are_checked_even_when_the_reference_is_shared() {
    let target = GlobalsIR::arrow_literal(integer()).unwrap();
    let valid = Expr::ExternalGlobal {
        reference: "shared".into(),
        dtype: Type::Struct {
            r#struct: vec![("g".into(), Type::Primitive(PrimitiveType::Int32))],
        },
    };
    let invalid = Expr::ExternalGlobal {
        reference: "shared".into(),
        dtype: Type::Primitive(PrimitiveType::Int32),
    };
    let expression = Expr::Struct {
        fields: vec![
            ("valid".into(), valid.clone()),
            ("again".into(), valid),
            ("invalid".into(), invalid),
        ],
    };
    assert!(
        GlobalsIR::empty()
            .map_bound(
                vec![("result".into(), expression)],
                [("shared".into(), target)].into()
            )
            .unwrap_err()
            .to_string()
            .contains("type mismatch")
    );
}
#[tokio::test]
async fn empty_projection_does_not_read_a_discarded_singleton_in_either_lowering_path() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let moved = directory.path().join("moved");
    let source = vail_table::TableIR::range(0, 1)
        .unwrap()
        .map_globals(vec![(
            "g".into(),
            Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: serde_json::json!(7),
            },
        )])
        .unwrap();
    source.write(path.to_str().unwrap(), false).await.unwrap();
    let source = vail_table::TableIR::read(path.to_str().unwrap())
        .await
        .unwrap();
    let discarded = source.globals().map(vec![]).unwrap();
    std::fs::rename(&path, &moved).unwrap();
    let logical = discarded.evaluate().await.unwrap();
    let legacy = datafusion_physical_plan::collect(
        discarded.lower().unwrap(),
        Arc::new(datafusion_execution::TaskContext::default()),
    )
    .await
    .unwrap();
    assert_eq!(logical.num_rows(), 1);
    assert_eq!(logical.num_columns(), 0);
    assert_eq!(
        legacy.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert!(source.globals().evaluate().await.is_err());
    std::fs::rename(&moved, &path).unwrap();
    assert_eq!(
        source
            .globals()
            .evaluate()
            .await
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );
}
#[tokio::test]
async fn literal_globals_keep_compatible_string_storage_and_metadata() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("g", DataType::Utf8View, false)],
        [("owner".into(), "test".into())].into(),
    ));
    let globals = GlobalsIR::arrow_literal(
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringViewArray::from(vec!["β"]))],
        )
        .unwrap(),
    )
    .unwrap();
    let result = globals.evaluate().await.unwrap();
    assert_eq!(result.schema(), schema);
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        "β"
    );
}

#[tokio::test]
async fn stored_globals_reject_misadvertised_names_and_logical_types() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    vail_table::TableIR::range(0, 1)
        .unwrap()
        .map_globals(vec![(
            "g".into(),
            Expr::Literal {
                dtype: Type::Primitive(PrimitiveType::Int32),
                value: serde_json::json!(7),
            },
        )])
        .unwrap()
        .write(path.to_str().unwrap(), false)
        .await
        .unwrap();
    for advertised in [
        serde_json::json!([["h", "int32"]]),
        serde_json::json!([["g", "str"]]),
    ] {
        let mut source = vail_storage::local_table::open_local(path.to_str().unwrap())
            .await
            .unwrap();
        source.semantics.global_type = advertised;
        let globals = GlobalsIR::read(Arc::new(source)).unwrap();
        assert!(
            globals
                .evaluate()
                .await
                .unwrap_err()
                .to_string()
                .contains("globals schema")
        );
    }
}
