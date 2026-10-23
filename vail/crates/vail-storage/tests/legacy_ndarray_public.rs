//! NDarray decoding through public component readers and exported Arrow batches.
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StructArray,
};
use arrow_schema::{DataType, Schema};
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use vail_storage::legacy::{Component, Parser};
use vortex::{
    VortexSessionDefault, array::VortexSessionExecute, arrow::ArrowSessionExt,
    session::VortexSession,
};

fn check(batch: &RecordBatch, shapes: &[Option<[usize; 2]>], primitive: &str) {
    let arrays = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let data = arrays
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let dimensions = arrays
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let nulls = arrays.logical_nulls();
    for (row, shape) in shapes.iter().enumerate() {
        let Some(shape) = shape else {
            assert!(nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)));
            continue;
        };
        assert!(nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row)));
        let actual_shape = dimensions.value(row);
        assert_eq!(
            actual_shape
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[shape[0] as i64, shape[1] as i64]
        );
        let expected = (0..shape[0])
            .flat_map(|r| (0..shape[1]).map(move |c| (r + c * shape[0]) as i64))
            .collect::<Vec<_>>();
        let values = data.value(row);
        match primitive {
            "Boolean" => assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|i| Some(i % 2 != 0))
                    .collect::<Vec<_>>()
            ),
            "Int32" => assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(i64::from))
                    .collect::<Vec<_>>(),
                expected.into_iter().map(Some).collect::<Vec<_>>()
            ),
            "Int64" => assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected.into_iter().map(Some).collect::<Vec<_>>()
            ),
            "Float32" => assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|v| Some(v as f32))
                    .collect::<Vec<_>>()
            ),
            "Float64" => assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|v| Some(v as f64))
                    .collect::<Vec<_>>()
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn ndarray_public_readers_preserve_types_transposition_missing_shapes_and_batches() {
    let shapes = [
        Some([2usize, 3]),
        Some([2, 3]),
        Some([3, 2]),
        Some([0, 4]),
        None,
        Some([1, 1]),
    ];
    for (primitive, encoded) in [
        ("Boolean", "EBoolean"),
        ("Int32", "EInt32"),
        ("Int64", "EInt64"),
        ("Float32", "EFloat32"),
        ("Float64", "EFloat64"),
    ] {
        let refs = HashMap::new();
        let (dtype, _) = Parser::new(&format!("Struct{{array:NDArray[{primitive},2]}}"), &refs)
            .virtual_type()
            .unwrap();
        let encoding = Parser::new(
            &format!("+EBaseStruct{{array:ENDArrayColumnMajor[+{encoded},2]}}"),
            &refs,
        )
        .encoded()
        .unwrap();
        let DataType::Struct(fields) = dtype.to_arrow().unwrap() else {
            unreachable!()
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("part-0");
        let mut bytes = vec![];
        for shape in shapes {
            bytes.push(1);
            let Some(shape) = shape else {
                bytes.push(1);
                continue;
            };
            bytes.push(0);
            for dimension in shape {
                bytes.extend_from_slice(&(dimension as i64).to_le_bytes());
            }
            for value in 0..shape[0] * shape[1] {
                match primitive {
                    "Boolean" => bytes.push((value % 2) as u8),
                    "Int32" => bytes.extend_from_slice(&(value as i32).to_le_bytes()),
                    "Int64" => bytes.extend_from_slice(&(value as i64).to_le_bytes()),
                    "Float32" => bytes.extend_from_slice(&(value as f32).to_le_bytes()),
                    "Float64" => bytes.extend_from_slice(&(value as f64).to_le_bytes()),
                    _ => unreachable!(),
                }
            }
        }
        bytes.push(0);
        std::fs::write(&path, bytes).unwrap();
        let component = Component {
            schema: Arc::new(Schema::new(fields)),
            dtype,
            encoding,
            buffer: json!({"name":"StreamBufferSpec"}),
            paths: vec![path],
            int_key_bounds: vec![None],
            index: None,
        };
        for batch_size in [1, 2, 4, 16] {
            let mut reader = component.reader(0, &[0]).unwrap();
            let mut position = 0;
            while let Some(batch) = reader.next_batch(batch_size).unwrap() {
                check(
                    &batch,
                    &shapes[position..position + batch.num_rows()],
                    primitive,
                );
                position += batch.num_rows();
            }
            assert_eq!(position, shapes.len());
            let session = VortexSession::default();
            let mut reader = component.reader(0, &[0]).unwrap();
            let mut position = 0;
            while let Some(array) = reader.next_vortex_batch(batch_size, &session).unwrap() {
                let field = session.arrow().to_arrow_field("", array.dtype()).unwrap();
                let mut context = session.create_execution_ctx();
                let exported = session
                    .arrow()
                    .execute_arrow(array, Some(&field), &mut context)
                    .unwrap();
                let batch =
                    RecordBatch::from(exported.as_any().downcast_ref::<StructArray>().unwrap());
                check(
                    &batch,
                    &shapes[position..position + batch.num_rows()],
                    primitive,
                );
                position += batch.num_rows();
            }
            assert_eq!(position, shapes.len());
        }
    }
}
