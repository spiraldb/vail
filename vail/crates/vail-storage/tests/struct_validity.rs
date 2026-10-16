//! Parent struct validity is independent of the validity of its fields.
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use vortex::array::VortexSessionExecute;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::dtype::Nullability;
use vortex::expr::{fill_null, get_item, is_not_null, is_null, lit, pack, root, select};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::{VortexSessionDefault, arrow::ArrowSessionExt, session::VortexSession};

fn batch() -> RecordBatch {
    let fields = vec![Arc::new(Field::new("x", DataType::Int32, true))].into();
    let structure = StructArray::new(
        fields,
        vec![Arc::new(Int32Array::from(vec![Some(1), None, None]))],
        Some([true, true, false].into_iter().collect()),
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "s",
            structure.data_type().clone(),
            true,
        )])),
        vec![Arc::new(structure)],
    )
    .unwrap()
}

fn parent_validity(batch: &RecordBatch) -> Vec<bool> {
    let structure = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    (0..structure.len())
        .map(|i| structure.is_valid(i))
        .collect()
}

#[tokio::test]
async fn vortex_roundtrip_retains_struct_parent_validity() {
    let input = batch();
    let session = VortexSession::default();
    let array = session
        .arrow()
        .from_arrow_record_batch(input.clone(), input.schema_ref())
        .unwrap();
    let field = session.arrow().to_arrow_field("", array.dtype()).unwrap();
    let mut ctx = session.create_execution_ctx();
    let exported = session
        .arrow()
        .execute_arrow(array.clone(), Some(&field), &mut ctx)
        .unwrap();
    assert_eq!(
        parent_validity(&RecordBatch::from(exported.as_struct())),
        [true, true, false],
        "Arrow conversion"
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("struct.vortex");
    let stream = ArrayStreamAdapter::new(array.dtype().clone(), futures::stream::iter([Ok(array)]));
    session
        .write_options()
        .write(tokio::fs::File::create(&path).await.unwrap(), stream)
        .await
        .unwrap();
    let file = session.open_options().open_path(&path).await.unwrap();
    for (name, projection) in [("root", root()), ("select", select(["s"], root()))] {
        let chunks = file
            .scan()
            .unwrap()
            .with_projection(projection.bind(file.dtype()).unwrap())
            .into_array_stream()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut validity = Vec::new();
        for chunk in chunks {
            let exported = session
                .arrow()
                .execute_arrow(chunk, Some(&field), &mut ctx)
                .unwrap();
            validity.extend(parent_validity(&RecordBatch::from(exported.as_struct())));
        }
        assert_eq!(validity, [true, true, false], "file projection {name}");
    }
    let chunks = file
        .scan()
        .unwrap()
        .with_projection(get_item("s", root()).bind(file.dtype()).unwrap())
        .into_array_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut validity = Vec::new();
    for chunk in chunks {
        let field = session.arrow().to_arrow_field("", chunk.dtype()).unwrap();
        let exported = session
            .arrow()
            .execute_arrow(chunk, Some(&field), &mut ctx)
            .unwrap();
        validity.extend((0..exported.len()).map(|i| exported.is_valid(i)));
    }
    assert_eq!(
        validity,
        [true, true, false],
        "direct struct field projection"
    );

    for (name, filter, expected) in [
        ("is_null", is_null(get_item("s", root())), vec![false]),
        (
            "is_not_null",
            is_not_null(get_item("s", root())),
            vec![true, true],
        ),
    ] {
        let chunks = file
            .scan()
            .unwrap()
            .with_some_filter(Some(filter.bind(file.dtype()).unwrap()))
            .into_array_stream()
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut validity = Vec::new();
        for chunk in chunks {
            let exported = session
                .arrow()
                .execute_arrow(chunk, Some(&field), &mut ctx)
                .unwrap();
            validity.extend(parent_validity(&RecordBatch::from(exported.as_struct())));
        }
        assert_eq!(validity, expected, "non-strict file filter {name}");
    }
    let projected = pack(
        [
            ("missing", is_null(get_item("s", root()))),
            ("present", is_not_null(get_item("s", root()))),
            (
                "filled",
                fill_null(get_item("x", get_item("s", root())), lit(42i32)),
            ),
        ],
        Nullability::NonNullable,
    );
    let chunks = file
        .scan()
        .unwrap()
        .with_projection(projected.bind(file.dtype()).unwrap())
        .into_array_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut missing = Vec::new();
    let mut present = Vec::new();
    let mut filled = Vec::new();
    for chunk in chunks {
        let field = session.arrow().to_arrow_field("", chunk.dtype()).unwrap();
        let exported = session
            .arrow()
            .execute_arrow(chunk, Some(&field), &mut ctx)
            .unwrap();
        let exported = exported.as_struct();
        let boolean = |name| {
            exported
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };
        missing.extend(boolean("missing"));
        present.extend(boolean("present"));
        filled.extend(
            exported
                .column_by_name("filled")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter(),
        );
    }
    assert_eq!(
        missing,
        [Some(false), Some(false), Some(true)],
        "non-strict is_null"
    );
    assert_eq!(
        present,
        [Some(true), Some(true), Some(false)],
        "non-strict is_not_null"
    );
    assert_eq!(
        filled,
        [Some(1), Some(42), Some(42)],
        "non-strict fill_null"
    );
}
