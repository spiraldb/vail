//! Public source-field presence checks. Expectations use independent logical masks.
use arrow_array::{Array, BooleanArray, Int32Array, StructArray as ArrowStruct};
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::{BoolArray, DictArray, PrimitiveArray, StructArray, VarBinArray};
use vortex::array::builtins::ArrayBuiltins;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray, MaskFuture};
use vortex::dtype::{DType, FieldNames, Nullability, PType};
use vortex::error::VortexResult;
use vortex::expr::{Expression, fill_null, get_item, is_not_null, is_null, lit, pack, root};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::{VortexSessionDefault, arrow::ArrowSessionExt, session::VortexSession};

fn field(path: &[&str]) -> Expression {
    path.iter()
        .fold(root(), |value, name| get_item(*name, value))
}
fn fixture() -> VortexResult<ArrayRef> {
    let n = 14;
    let number = DictArray::try_new(
        PrimitiveArray::from_option_iter((0..n).map(|i| (i % 6 != 4).then_some((i % 4) as u8)))
            .into_array(),
        PrimitiveArray::from_option_iter([Some(f64::NAN), Some(0.0), None, Some(2.0)]).into_array(),
    )?
    .into_array();
    let optional_empty = StructArray::try_new(
        FieldNames::default(),
        Vec::<ArrayRef>::new(),
        n,
        Validity::from_iter((0..n).map(|i| i % 3 != 0)),
    )?
    .into_array();
    let deep = StructArray::try_new(
        FieldNames::from(["number", "flag", "empty"]),
        vec![
            number,
            BoolArray::from_iter((0..n).map(|i| (i % 4 != 1).then_some(i % 2 == 0))).into_array(),
            optional_empty,
        ],
        n,
        Validity::from_iter((0..n).map(|i| i % 11 != 3)),
    )?
    .into_array();
    let branch = StructArray::try_new(
        FieldNames::from(["deep"]),
        vec![deep],
        n,
        Validity::from_iter((0..n).map(|i| i % 7 != 2)),
    )?
    .into_array();
    let required = StructArray::try_new(
        FieldNames::from(["always", "empty"]),
        vec![
            BoolArray::from_iter(std::iter::repeat_n(false, n)).into_array(),
            StructArray::try_new(
                FieldNames::default(),
                Vec::<ArrayRef>::new(),
                n,
                Validity::NonNullable,
            )?
            .into_array(),
        ],
        n,
        Validity::NonNullable,
    )?
    .into_array();
    Ok(StructArray::try_new(
        FieldNames::from(["branch", "required", "payload", "text"]),
        vec![
            branch,
            required,
            PrimitiveArray::from_iter((0..n).map(|i| i as i32 * 31)).into_array(),
            VarBinArray::from_iter(
                (0..n).map(|i| Some(if i == 0 { "bad" } else { "12" })),
                DType::Utf8(Nullability::Nullable),
            )
            .into_array(),
        ],
        n,
        Validity::from_iter((0..n).map(|i| i % 5 != 1)),
    )?
    .into_array())
}
fn oracle(i: usize, path: &[&str]) -> bool {
    let mut valid = i % 5 != 1;
    if path.first() == Some(&"branch") {
        valid &= i % 7 != 2;
    }
    if path.get(1) == Some(&"deep") {
        valid &= i % 11 != 3;
    }
    match path.last().copied() {
        Some("number") => valid &= i % 6 != 4 && i % 4 != 2,
        Some("flag") => valid &= i % 4 != 1,
        Some("empty") if path.first() == Some(&"branch") => valid &= i % 3 != 0,
        _ => {}
    }
    valid
}
fn bool_values(array: ArrayRef, session: &VortexSession) -> VortexResult<Vec<Option<bool>>> {
    assert_eq!(array.dtype(), &DType::Bool(Nullability::NonNullable));
    let field = session.arrow().to_arrow_field("", array.dtype())?;
    let mut ctx = session.create_execution_ctx();
    let materialized = session
        .arrow()
        .execute_arrow(array, Some(&field), &mut ctx)?;
    Ok(materialized
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect())
}

#[tokio::test]
async fn nested_struct_presence_uses_all_logical_ancestors_and_reuses_row_domains()
-> VortexResult<()> {
    let input = fixture()?;
    let session = VortexSession::default();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("presence.vortex");
    session
        .write_options()
        .write(
            tokio::fs::File::create(&path).await.unwrap(),
            ArrayStreamAdapter::new(input.dtype().clone(), futures::stream::iter([Ok(input)])),
        )
        .await?;
    let file = session
        .open_options()
        .open_path(&path)
        .await?
        .with_caching();
    let reader = file.layout_reader()?;
    let paths: &[&[&str]] = &[
        &[],
        &["branch"],
        &["branch", "deep"],
        &["branch", "deep", "number"],
        &["branch", "deep", "flag"],
        &["branch", "deep", "empty"],
        &["required"],
        &["required", "always"],
        &["required", "empty"],
    ];
    for names in paths {
        for missing in [false, true] {
            let expression = if missing {
                is_null(field(names))
            } else {
                is_not_null(field(names))
            }
            .bind(file.dtype())?;
            let full = reader
                .projection_evaluation(&(0..14), &expression, MaskFuture::new_true(14))?
                .await?;
            assert_eq!(
                bool_values(full, &session)?,
                (0..14)
                    .map(|i| Some(oracle(i, names) != missing))
                    .collect::<Vec<_>>(),
                "path={names:?}, missing={missing}"
            );
            // Same prepared request, different range/mask: no cached data/masks.
            let selected = [true, false, true, false, true, false, true, true, false];
            let bounded = reader
                .projection_evaluation(
                    &(3..12),
                    &expression,
                    MaskFuture::ready(vortex::mask::Mask::from_iter(selected)),
                )?
                .await?;
            assert_eq!(
                bool_values(bounded, &session)?,
                selected
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, keep)| keep.then_some(Some(oracle(i + 3, names) != missing)))
                    .collect::<Vec<_>>()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn struct_presence_raw_and_unknown_consumers_keep_original_values() -> VortexResult<()> {
    let input = fixture()?;
    let session = VortexSession::default();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("raw-sibling.vortex");
    session
        .write_options()
        .write(
            tokio::fs::File::create(&path).await.unwrap(),
            ArrayStreamAdapter::new(
                input.dtype().clone(),
                futures::stream::iter([Ok(input.clone())]),
            ),
        )
        .await?;
    let file = session.open_options().open_path(&path).await?;
    let reader = file.layout_reader()?;
    let number = field(&["branch", "deep", "number"]);
    let expression = pack(
        [
            ("raw", root()),
            ("defined", is_not_null(number.clone())),
            ("missing", is_null(number.clone())),
            ("filled", is_not_null(fill_null(number, lit(0.0f64)))),
        ],
        Nullability::NonNullable,
    )
    .bind(file.dtype())?;
    let actual = reader
        .projection_evaluation(&(0..14), &expression, MaskFuture::new_true(14))?
        .await?;
    assert_eq!(
        bool_values(actual.get_item("defined")?, &session)?,
        (0..14)
            .map(|i| Some(oracle(i, &["branch", "deep", "number"])))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        bool_values(actual.get_item("missing")?, &session)?,
        (0..14)
            .map(|i| Some(!oracle(i, &["branch", "deep", "number"])))
            .collect::<Vec<_>>()
    );
    // FillNull is not a source field path. Parent absence cannot be pushed
    // through it: filling yields a PRESENT value even for an absent parent.
    assert_eq!(
        bool_values(actual.get_item("filled")?, &session)?,
        vec![Some(true); 14]
    );
    let raw = actual.get_item("raw")?;
    assert_eq!(raw.dtype(), input.dtype());
    let arrow_field = session.arrow().to_arrow_field("", raw.dtype())?;
    let mut ctx = session.create_execution_ctx();
    let raw = session
        .arrow()
        .execute_arrow(raw, Some(&arrow_field), &mut ctx)?;
    let raw = raw.as_any().downcast_ref::<ArrowStruct>().unwrap();
    let payload = raw
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    for i in 0..14 {
        assert_eq!(raw.is_valid(i), i % 5 != 1);
        if raw.is_valid(i) {
            assert_eq!(payload.value(i), i as i32 * 31);
        }
    }
    // Unknown cast computation remains the original consumer. Presence may
    // legitimately read only its logical validity, but requesting the cast value
    // must still retain its conversion error on the valid first row "bad".
    let casted = vortex::expr::cast(
        field(&["text"]),
        DType::Primitive(PType::I64, Nullability::Nullable),
    );
    let cast_presence = is_not_null(casted.clone()).bind(file.dtype())?;
    let actual = reader
        .projection_evaluation(&(0..14), &cast_presence, MaskFuture::new_true(14))?
        .await?;
    assert_eq!(
        bool_values(actual, &session)?,
        (0..14).map(|i| Some(i % 5 != 1)).collect::<Vec<_>>()
    );
    let values = pack(
        [("defined", is_not_null(casted.clone())), ("value", casted)],
        Nullability::NonNullable,
    )
    .bind(file.dtype())?;
    let error = match reader.projection_evaluation(&(0..14), &values, MaskFuture::new_true(14)) {
        Err(_) => true,
        Ok(future) => match future.await {
            Err(_) => true,
            Ok(array) => {
                let result_field = session.arrow().to_arrow_field("", array.dtype())?;
                session
                    .arrow()
                    .execute_arrow(array, Some(&result_field), &mut ctx)
                    .is_err()
            }
        },
    };
    assert!(error, "requested fallible cast value must still error");
    Ok(())
}

#[tokio::test]
async fn struct_presence_required_empty_and_dictionary_rows_are_logical() -> VortexResult<()> {
    let session = VortexSession::default();
    let dictionary = StructArray::try_new(
        FieldNames::from(["value"]),
        vec![PrimitiveArray::from_iter([12i32, 34, 56]).into_array()],
        3,
        Validity::from_iter([true, false, true]),
    )?
    .into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_option_iter([Some(0u8), Some(1), None, Some(2)]).into_array(),
        dictionary,
    )?
    .into_array();
    let directory = tempfile::tempdir().unwrap();
    for (case, input, expected) in [
        (
            "required-empty",
            StructArray::try_new(
                FieldNames::default(),
                Vec::<ArrayRef>::new(),
                5,
                Validity::NonNullable,
            )?
            .into_array(),
            vec![true; 5],
        ),
        (
            "zero-rows",
            StructArray::try_new(
                FieldNames::default(),
                Vec::<ArrayRef>::new(),
                0,
                Validity::NonNullable,
            )?
            .into_array(),
            vec![],
        ),
        ("dictionary", dictionary, vec![true, false, false, true]),
    ] {
        let path = directory.path().join(format!("{case}.vortex"));
        session
            .write_options()
            .write(
                tokio::fs::File::create(&path).await.unwrap(),
                ArrayStreamAdapter::new(input.dtype().clone(), futures::stream::iter([Ok(input)])),
            )
            .await?;
        let file = session.open_options().open_path(&path).await?;
        let reader = file.layout_reader()?;
        for missing in [false, true] {
            let expression = if missing {
                is_null(root())
            } else {
                is_not_null(root())
            }
            .bind(file.dtype())?;
            let actual = reader
                .projection_evaluation(
                    &(0..expected.len() as u64),
                    &expression,
                    MaskFuture::new_true(expected.len()),
                )?
                .await?;
            assert_eq!(
                bool_values(actual, &session)?,
                expected
                    .iter()
                    .map(|valid| Some(*valid != missing))
                    .collect::<Vec<_>>(),
                "{case}"
            );
        }
    }
    Ok(())
}
