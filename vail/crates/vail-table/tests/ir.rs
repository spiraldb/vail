use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;
use std::sync::Arc;
use vail_execution::expressions::{BinaryOperator, PrimitiveType, Type};
use vail_table::{ScalarIR, TableIR, TableNode};

fn column(name: &str) -> ScalarIR {
    ScalarIR::Column { name: name.into() }
}
fn int(value: i32) -> ScalarIR {
    ScalarIR::Literal {
        dtype: Type::Primitive(PrimitiveType::Int32),
        value: json!(value),
    }
}
fn greater(left: ScalarIR, right: ScalarIR) -> ScalarIR {
    ScalarIR::Binary {
        operator: BinaryOperator::Gt,
        left: Arc::new(left),
        right: Arc::new(right),
    }
}

#[tokio::test]
async fn branches_share_a_typed_child_and_execute_independently() {
    let source = TableIR::range(10, 3).unwrap();
    let left = source.head(2);
    let right = source.filter(greater(column("idx"), int(4)), true).unwrap();
    let TableNode::Head { child: a, .. } = left.node() else {
        panic!("expected Head")
    };
    let TableNode::Filter { child: b, .. } = right.node() else {
        panic!("expected Filter")
    };
    assert!(Arc::ptr_eq(a, b));
    assert!(Arc::ptr_eq(a, &source));
    assert_eq!(left.count().await.unwrap(), 2);
    assert_eq!(right.count().await.unwrap(), 5);
    assert_eq!(source.count().await.unwrap(), 10);
    assert!(right.known_count().is_none());
    assert_eq!(right.known_n_partitions(), Some(3));
}

#[tokio::test]
async fn range_is_lazy_and_head_stops_batch_generation() {
    let source = TableIR::range(i32::MAX as usize, 1).unwrap();
    let batches = source.head(3).collect_partitions().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0][0].num_rows(), 3);
    assert_eq!(
        batches[0][0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[0, 1, 2]
    );
}

#[tokio::test]
async fn parallelize_keeps_shared_input_buffers_without_prepartitioning() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let source = TableIR::parallelize(
        RecordBatch::try_new(schema, vec![values.clone()]).unwrap(),
        5,
    )
    .unwrap();
    let TableNode::Parallelize { rows, n_partitions } = source.node() else {
        panic!("expected Parallelize")
    };
    assert_eq!(*n_partitions, 5);
    assert_eq!(rows.num_rows(), 3);
    assert_eq!(
        rows.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ptr(),
        values.values().as_ptr()
    );
    let partitions = source.collect_partitions().await.unwrap();
    assert_eq!(partitions.len(), 5);
    assert_eq!(
        partitions
            .iter()
            .map(|p| p.iter().map(RecordBatch::num_rows).sum::<usize>())
            .collect::<Vec<_>>(),
        vec![0, 1, 0, 1, 1]
    );
}

#[test]
fn constructors_typecheck_scalar_ir_without_execution() {
    let source = TableIR::range(10, 3).unwrap();
    assert!(source.filter(column("idx"), true).is_err());
    assert!(source.map_rows(vec![("idx".into(), int(7))]).is_err());
    assert!(
        source
            .filter(greater(column("absent"), int(0)), true)
            .is_err()
    );
    assert!(
        source
            .map_rows(vec![("idx".into(), column("absent"))])
            .is_err()
    );
    assert!(source.key_by(vec!["idx".into(), "idx".into()]).is_err());
}

#[tokio::test]
async fn write_streams_transformed_rows_into_rolled_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table");
    let source = TableIR::range(20, 2).unwrap();
    let table = source.filter(greater(column("idx"), int(3)), true).unwrap();
    table
        .write_with_options(
            path.to_str().unwrap(),
            false,
            vail_storage::local_table::WriteOptions {
                max_buffer_bytes: usize::MAX,
                max_rows_per_file: 3,
            },
        )
        .await
        .unwrap();
    let read = TableIR::read(path.to_str().unwrap()).await.unwrap();
    assert_eq!(read.known_count(), Some(16));
    assert_eq!(read.known_n_partitions(), Some(2));
    let collected = read.collect_partitions().await.unwrap();
    let values = collected
        .iter()
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
    assert_eq!(values, (4..20).collect::<Vec<_>>());
}
