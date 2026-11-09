use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use vail_table::TableIR;

#[tokio::test]
async fn explode_keeps_nullable_parents_and_children_but_not_hidden_list_values() {
    let values = Arc::new(Int32Array::from(vec![Some(0), None, Some(12), Some(13), Some(14), Some(15), Some(16), Some(17)])) as ArrayRef;
    let items = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![1i32, 3, 5, 5, 8].into()), values,
        Some(NullBuffer::from(vec![true, false, true, true])),
    )) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("parent", DataType::Int32, true), Field::new("items", items.data_type().clone(), true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![Some(100), Some(200), Some(300), None])), items]).unwrap();
    let source = TableIR::parallelize(batch, 1).unwrap();
    let exploded = source.explode(vec!["items".into()]).unwrap();
    let mut actual = Vec::new();
    for partition in exploded.collect_partitions().await.unwrap() {
        for batch in partition {
            let parents = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let values = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            actual.extend(parents.iter().zip(values.iter()));
        }
    }
    assert_eq!(actual, vec![(Some(100), None), (Some(100), Some(12)), (None, Some(15)), (None, Some(16)), (None, Some(17))]);
}

#[tokio::test]
async fn explode_splits_long_lists_without_changing_order() {
    let values = Arc::new(Int32Array::from_iter_values(0..30_000)) as ArrayRef;
    let items = Arc::new(ListArray::new(Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0i32, 20_000, 30_000].into()), values, None)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("parent", DataType::Int32, false), Field::new("items", items.data_type().clone(), true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![7, 8])), items]).unwrap();
    let exploded = TableIR::parallelize(batch, 1).unwrap().explode(vec!["items".into()]).unwrap();
    let mut actual = Vec::new();
    for partition in exploded.collect_partitions().await.unwrap() {
        for batch in partition {
            let parents = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let values = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            actual.extend(parents.iter().zip(values.iter()));
        }
    }
    assert_eq!(actual, (0..30_000).map(|value| (Some(if value < 20_000 { 7 } else { 8 }), Some(value))).collect::<Vec<_>>());
}
