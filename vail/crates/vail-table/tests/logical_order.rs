use arrow_array::Int32Array;
use vail_table::TableIR;

#[tokio::test]
async fn optimized_planner_preserves_order_by_result() {
    let table = TableIR::range(10, 2)
        .unwrap()
        .order_by(vec![("idx".into(), true)])
        .unwrap();
    let batches = table.collect_partitions().await.unwrap();
    let values = batches
        .into_iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (0..10).rev().collect::<Vec<_>>());
}
