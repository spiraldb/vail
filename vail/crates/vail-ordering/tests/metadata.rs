use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use serde_json::json;
use vail_ordering::{
    TYPE_DESCRIPTOR, TYPE_VERSION, has_type_metadata, same_layout, tag_type, type_metadata,
    without_type_metadata,
};

#[test]
fn supported_carriers_roundtrip_and_preserve_layout_and_unrelated_metadata() {
    let field = Arc::new(
        Field::new("element", DataType::Int32, false)
            .with_metadata(HashMap::from([("application".into(), "preserved".into())])),
    );
    for physical in [
        DataType::Struct(vec![field.clone()].into()),
        DataType::List(field.clone()),
        DataType::LargeList(field.clone()),
    ] {
        let descriptor = json!({"custom": ["int32"]});
        let tagged = tag_type(physical.clone(), &descriptor).unwrap();
        assert_eq!(type_metadata(&tagged).unwrap(), Some(descriptor));
        assert!(has_type_metadata(&tagged));
        assert!(same_layout(&physical, &tagged));
        let clean = without_type_metadata(&tagged);
        assert!(!has_type_metadata(&clean));
        assert_eq!(type_metadata(&clean).unwrap(), None);
        assert!(same_layout(&physical, &clean));
        let child = match &clean {
            DataType::Struct(fields) => &fields[0],
            DataType::List(field) | DataType::LargeList(field) => field,
            _ => panic!("carrier changed"),
        };
        assert_eq!(child.metadata().get("application").unwrap(), "preserved");
        assert!(!child.is_nullable());
    }
}

#[test]
fn types_without_a_carrier_are_rejected() {
    for physical in [
        DataType::Struct(arrow_schema::Fields::empty()),
        DataType::Int32,
        DataType::Utf8,
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 2),
    ] {
        assert!(tag_type(physical, &json!("custom")).is_err());
    }
}

#[test]
fn nested_large_list_metadata_is_detected_and_removed() {
    let leaf = DataType::Struct(vec![Arc::new(Field::new("x", DataType::Int32, true))].into());
    let tagged_leaf = tag_type(leaf, &json!({"tuple": ["int32"]})).unwrap();
    let inner = DataType::LargeList(Arc::new(Field::new("custom", tagged_leaf, false)));
    let tagged_inner = tag_type(inner, &json!({"array": "custom"})).unwrap();
    let outer = DataType::Struct(vec![Arc::new(Field::new("nested", tagged_inner, true))].into());
    assert!(has_type_metadata(&outer));
    let cleaned = without_type_metadata(&outer);
    assert!(!has_type_metadata(&cleaned));
    assert!(same_layout(&outer, &cleaned));
    let expected = DataType::Struct(
        vec![Arc::new(Field::new(
            "nested",
            DataType::LargeList(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Arc::new(Field::new("x", DataType::Int32, true))].into()),
                false,
            ))),
            true,
        ))]
        .into(),
    );
    assert_eq!(cleaned, expected);
}

#[test]
fn malformed_descriptors_are_detected_rejected_and_removable() {
    for metadata in [
        HashMap::from([(TYPE_DESCRIPTOR.into(), "null".into())]),
        HashMap::from([(TYPE_VERSION.into(), "1".into())]),
        HashMap::from([
            (TYPE_VERSION.into(), "2".into()),
            (TYPE_DESCRIPTOR.into(), "null".into()),
        ]),
        HashMap::from([
            (TYPE_VERSION.into(), "1".into()),
            (TYPE_DESCRIPTOR.into(), "{".into()),
        ]),
    ] {
        let field = Arc::new(Field::new("item", DataType::Int32, true).with_metadata(metadata));
        for dtype in [
            DataType::Struct(vec![field.clone()].into()),
            DataType::List(field.clone()),
            DataType::LargeList(field.clone()),
        ] {
            assert!(has_type_metadata(&dtype));
            assert!(type_metadata(&dtype).is_err());
            let clean = without_type_metadata(&dtype);
            assert!(!has_type_metadata(&clean));
            assert_eq!(type_metadata(&clean).unwrap(), None);
        }
    }
}

#[test]
fn layout_comparison_preserves_offset_width_and_element_type() {
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let list = DataType::List(field.clone());
    let large = DataType::LargeList(field);
    assert!(!same_layout(&list, &large));
    assert!(!same_layout(
        &large,
        &DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
    ));
}
