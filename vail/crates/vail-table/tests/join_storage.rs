//! Public joins over logically equal keys in alternate Arrow storage.
use arrow_array::{
    ArrayRef, Int32Array, LargeStringArray, RecordBatch, StringArray, StringViewArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{Field, Schema};
use std::sync::Arc;
use vail_table::{TableIR, TableJoinKind, TableRef};
fn keys(mode: usize, nested: bool) -> ArrayRef {
    let values = vec![Some("a"), None, Some("b")];
    let keys: ArrayRef = match mode {
        0 => Arc::new(StringArray::from(values)),
        1 => Arc::new(LargeStringArray::from(values)),
        _ => Arc::new(StringViewArray::from(values)),
    };
    if !nested {
        return keys;
    }
    Arc::new(StructArray::new(
        vec![Arc::new(Field::new("name", keys.data_type().clone(), true))].into(),
        vec![keys],
        Some(NullBuffer::from(vec![true, true, false])),
    ))
}
fn table(mode: usize, nested: bool, value: &str) -> TableRef {
    let keys = keys(mode, nested);
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", keys.data_type().clone(), true),
        Field::new(value, arrow_schema::DataType::Int32, true),
    ]));
    TableIR::parallelize(
        RecordBatch::try_new(
            schema,
            vec![keys, Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap(),
        3,
    )
    .unwrap()
    .key_by(vec!["key".into()])
    .unwrap()
}
#[tokio::test]
async fn equality_and_membership_joins_normalize_physical_key_storage() {
    for nested in [false, true] {
        for mode in [1, 2] {
            let left = table(0, nested, "left");
            let right = table(mode, nested, "right");
            let expected = 2;
            let joined = left.join(right.clone(), TableJoinKind::Inner, 1).unwrap();
            assert_eq!(joined.count().await.unwrap(), expected);
            let batches = joined.collect_partitions().await.unwrap();
            let mut pairs = Vec::new();
            for batch in batches.iter().flatten() {
                let left = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let right = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                pairs.extend((0..batch.num_rows()).map(|i| (left.value(i), right.value(i))));
            }
            assert_eq!(
                pairs,
                if nested {
                    vec![(1, 1), (2, 2)]
                } else {
                    vec![(1, 1), (3, 3)]
                }
            );
            assert_eq!(
                left.join(right.clone(), TableJoinKind::Semi, 1)
                    .unwrap()
                    .count()
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(
                left.join(right, TableJoinKind::Anti, 1)
                    .unwrap()
                    .count()
                    .await
                    .unwrap(),
                1
            );
        }
    }
}
#[tokio::test]
async fn zip_join_normalizes_physical_key_storage_and_retains_missing_occurrences() {
    for nested in [false, true] {
        let tables = (0..3).map(|mode| table(mode, nested, "value")).collect();
        let joined = TableIR::multi_way_zip_join(tables, "data".into(), "globals".into()).unwrap();
        // Two present keys share a row; each missing parent retains its row.
        assert_eq!(joined.count().await.unwrap(), 5);
        assert_eq!(
            joined
                .collect_partitions()
                .await
                .unwrap()
                .iter()
                .flatten()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            5
        );
    }
}
