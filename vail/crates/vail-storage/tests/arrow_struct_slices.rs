//! Arrow struct offsets apply once at every nesting level, including C Data imports.
use std::sync::Arc;

use arrow_array::ffi::{from_ffi, to_ffi};
use arrow_array::{Array, Int32Array, StructArray};
use arrow_schema::Field;

fn nested_array() -> StructArray {
    // The primitive child starts at a different buffer offset from either struct.
    let values = Int32Array::from(vec![
        Some(10),
        Some(11),
        Some(12),
        None,
        Some(14),
        None,
        Some(16),
        Some(17),
    ])
    .slice(1, 6);
    let inner = StructArray::new(
        vec![Arc::new(Field::new("x", values.data_type().clone(), true))].into(),
        vec![Arc::new(values)],
        Some([true, true, false, true, true, false].into_iter().collect()),
    );
    StructArray::new(
        vec![Arc::new(Field::new(
            "child",
            inner.data_type().clone(),
            true,
        ))]
        .into(),
        vec![Arc::new(inner)],
        Some([true, false, true, true, false, true].into_iter().collect()),
    )
}

#[test]
fn nested_array_data_slices_match_concrete_slices() {
    let original = nested_array();
    for (offset, length) in [(0, 6), (1, 4), (2, 2), (6, 0)] {
        let sliced = original.to_data().slice(offset, length);
        sliced.validate_full().unwrap();
        let actual = StructArray::from(sliced.clone());
        assert_eq!(
            actual,
            original.slice(offset, length),
            "slice {offset}:{length}"
        );
        if length > 0 {
            let repeated = sliced.slice(1, length - 1);
            repeated.validate_full().unwrap();
            assert_eq!(
                StructArray::from(repeated),
                original.slice(offset + 1, length - 1),
                "repeated slice {offset}:{length}"
            );
        }
    }
}

#[test]
fn nested_c_data_imports_apply_parent_and_child_offsets_once() {
    let original = nested_array();
    for (offset, length) in [(0, 6), (1, 4), (2, 2), (6, 0)] {
        // A C producer may retain unsliced children and express a slice on the parent.
        let raw = original
            .to_data()
            .into_builder()
            .offset(offset)
            .len(length)
            .nulls(original.nulls().map(|nulls| nulls.slice(offset, length)))
            .build()
            .unwrap();
        let (array, schema) = to_ffi(&raw).unwrap();
        // SAFETY: Both values were exported above; ownership transfers to the importer.
        let imported = unsafe { from_ffi(array, &schema) }.unwrap();
        let actual = StructArray::from(imported);
        assert_eq!(
            actual,
            original.slice(offset, length),
            "C Data slice {offset}:{length}"
        );
    }
}
